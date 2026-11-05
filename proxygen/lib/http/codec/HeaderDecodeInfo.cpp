/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <proxygen/lib/http/codec/HeaderDecodeInfo.h>

#include <folly/Conv.h>
#include <proxygen/lib/http/codec/CodecUtil.h>

using std::string;

namespace proxygen {

bool HeaderDecodeInfo::onHeader(const HPACKHeaderName& name,
                                const folly::fbstring& value) {
  // Refuse decoding other headers if an error is already found
  if (decodeError != HPACK::DecodeError::NONE || !error.empty()) {
    VLOG(4) << "Ignoring header=" << name << " value=" << value
            << " due to parser error=" << error.combine();
    return true;
  }
  DVLOG(5) << "Processing header=" << name << " value=" << value;
  auto headerCode = name.getHeaderCode();
  folly::StringPiece nameSp(name.get());
  folly::StringPiece valueSp(value);
  auto& headers = msg->getHeaders();

  if (nameSp.startsWith(':')) {
    pseudoHeaderSeen_ = true;
    if (firstPseudoHeader_.empty()) {
      firstPseudoHeader_ = nameSp.str();
    }
    if (regularHeaderSeen_) {
      error.set("illegal-pseudo-header", folly::to<string>("name=", nameSp));
      return false;
    }
    if (isRequest_) {
      bool ok = false;
      switch (headerCode) {
        case HTTP_HEADER_COLON_METHOD:
          ok = verifier.setMethod(valueSp);
          if (verifier.hasValidationError()) {
            proxygenError = kErrorHeaderContentValidation;
          }
          break;
        case HTTP_HEADER_COLON_SCHEME:
          ok = verifier.setScheme(valueSp);
          if (verifier.hasValidationError()) {
            proxygenError = kErrorHeaderContentValidation;
          }
          break;
        case HTTP_HEADER_COLON_AUTHORITY:
          ok = verifier.setAuthority(valueSp, validate_, strictValidation_);
          if (verifier.hasValidationError()) {
            proxygenError = kErrorHeaderContentValidation;
          }
          break;
        case HTTP_HEADER_COLON_PATH:
          ok = verifier.setPath(valueSp, strictValidation_, allowEmptyPath_);
          if (verifier.hasValidationError()) {
            proxygenError = kErrorHeaderContentValidation;
          }
          break;
        case HTTP_HEADER_COLON_PROTOCOL:
          ok = verifier.setUpgradeProtocol(valueSp, strictValidation_);
          if (verifier.hasValidationError()) {
            proxygenError = kErrorHeaderContentValidation;
          }
          break;
        default:
          error.set("invalid-request-pseudo-header",
                    folly::to<string>("name=", nameSp));
          return false;
      }
      if (!ok) {
        return false;
      }
    } else {
      if (headerCode == HTTP_HEADER_COLON_STATUS) {
        if (hasStatus_) {
          error.set("duplicate-status",
                    folly::to<string>(
                        "existing=", msg->getStatusCode(), " new=", valueSp));
          return false;
        }
        hasStatus_ = true;
        int32_t code = -1;
        folly::tryTo<int32_t>(valueSp).then(
            [&code](int32_t num) { code = num; });
        if (code >= 100 && code <= 999) {
          msg->setStatusCode(code);
          msg->setStatusMessage(HTTPMessage::getDefaultReason(code));
        } else {
          error.set("malformed-status", folly::to<string>("code=", valueSp));
          return false;
        }
      } else {
        error.set("invalid-response-pseudo-header",
                  folly::to<string>("name=", nameSp));
        return false;
      }
    }
  } else {
    regularHeaderSeen_ = true;
    switch (headerCode) {
      case HTTP_HEADER_CONNECTION:
        error.set("connection-header-forbidden",
                  folly::to<string>("value=", valueSp));
        return false;
      case HTTP_HEADER_CONTENT_LENGTH: {
        const auto cl = headers.getSingleOrNullptr(HTTP_HEADER_CONTENT_LENGTH);
        if (cl) {
          bool ok = *cl == valueSp;
          if (!ok) {
            error.set("multiple-content-length",
                      folly::to<string>("existing=", *cl, " new=", valueSp));
          }
          return ok; // skips adding if already present and equal
        }
        break;
      }
      case HTTP_HEADER_HOST: {
        if (verifier.hasAuthority()) { // HTTP_HEADER_HOST already present
          bool ok = headers.getSingleOrEmpty(HTTP_HEADER_HOST) == valueSp;
          if (!ok) {
            error.set(
                "authority-host-mismatch",
                folly::to<string>("existing=",
                                  headers.getSingleOrEmpty(HTTP_HEADER_HOST),
                                  " new=",
                                  valueSp));
          }
          return ok; // skips adding if already present and equal
        }
        break;
      }
      default:
        // no op
        break;
    }
    bool nameOk = !validate_ || headerCode != HTTP_HEADER_OTHER ||
                  CodecUtil::validateHeaderName(
                      nameSp,
                      strictValidation_ ? CodecUtil::HEADER_NAME_STRICT
                                        : CodecUtil::HEADER_NAME_STRICT_COMPAT);
    auto valueError =
        validate_
            ? CodecUtil::validateHeaderValueDetail(
                  valueSp,
                  strictValidation_ ? CodecUtil::CtlEscapeMode::STRICT
                                    : CodecUtil::CtlEscapeMode::STRICT_COMPAT)
            : CodecUtil::HeaderValueError::None;
    if (!nameOk || valueError != CodecUtil::HeaderValueError::None) {
      proxygenError =
          nameSp.empty() ? kErrorParseHeader : kErrorHeaderContentValidation;
      std::string reason =
          nameSp.empty() ? "empty-name" : (!nameOk ? "invalid-name-char" : "");
      if (valueError != CodecUtil::HeaderValueError::None) {
        if (!reason.empty()) {
          reason += ",";
        }
        reason += CodecUtil::describeHeaderValueError(valueError);
      }
      error.set(isRequestTrailers_ ? "invalid-trailer" : "invalid-header",
                folly::to<string>("name=", nameSp, " reason=", reason));
      headerErrorValue = valueSp;
      return false;
    }

    // Add the (name, value) pair to headers
    headerCode == HTTP_HEADER_OTHER ? headers.add(nameSp, valueSp)
                                    : headers.add(headerCode, valueSp);
  }
  return true;
}

void HeaderDecodeInfo::onHeadersComplete(HTTPHeaderSize decodedSize) {
  HTTPHeaders& headers = msg->getHeaders();

  if (isRequest_ && !isRequestTrailers_) {
    auto combinedCookie = headers.combine(HTTP_HEADER_COOKIE, "; ");
    if (!combinedCookie.empty()) {
      headers.set(HTTP_HEADER_COOKIE, combinedCookie);
    }
    if (!verifier.validate()) {
      // verifier.error embeds the offending method/path/scheme/authority
      error.set("malformed-request", verifier.error);
      return;
    }
  }

  if (isTrailers()) {
    if (pseudoHeaderSeen_) {
      error.set("pseudo-header-in-trailers",
                folly::to<string>("name=", firstPseudoHeader_));
      return;
    } else if (error.empty() && missingEndStream_) {
      error.set("trailers-without-eos");
      return;
    }
  }

  msg->setHTTPVersion(1, 1);
  msg->setIngressHeaderSize(decodedSize);
}

bool HeaderDecodeInfo::hasStatus() const {
  return hasStatus_;
}
} // namespace proxygen
