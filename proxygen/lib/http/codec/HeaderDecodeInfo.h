/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Conv.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/lib/http/codec/HTTPRequestVerifier.h>
#include <proxygen/lib/http/codec/compress/HPACKConstants.h>
#include <proxygen/lib/http/codec/compress/HPACKHeaderName.h>

#include <memory>

namespace proxygen {

class HTTPMessage;

/**
 * Why a codec rejected a field section. `context` is a bounded token drawn from
 * a fixed vocabulary; `detail` may embed peer-supplied bytes, so anything that
 * leaves the process must use `context` on its own.
 */
struct FieldSectionError {
  std::string context;
  std::string detail;

  void set(std::string errorContext, std::string errorDetail = {}) {
    context = std::move(errorContext);
    detail = std::move(errorDetail);
  }

  void clear() {
    context.clear();
    detail.clear();
  }

  [[nodiscard]] bool empty() const {
    return context.empty() && detail.empty();
  }

  [[nodiscard]] std::string combine() const {
    if (context.empty()) {
      return detail;
    }
    if (detail.empty()) {
      return context;
    }
    return folly::to<std::string>(context, " ", detail);
  }
};

class HeaderDecodeInfo {
 public:
  void init(bool isRequestIn,
            bool isRequestTrailers,
            bool validate,
            bool strictValidation,
            bool allowEmptyPath,
            bool missingEndStream = false) {
    CHECK(!msg);
    msg = std::make_unique<HTTPMessage>();
    isRequest_ = isRequestIn;
    isRequestTrailers_ = isRequestTrailers;
    validate_ = validate;
    hasStatus_ = false;
    regularHeaderSeen_ = false;
    pseudoHeaderSeen_ = false;
    error.clear();
    headerErrorValue.clear();
    firstPseudoHeader_.clear();
    decodeError = HPACK::DecodeError::NONE;
    strictValidation_ = strictValidation;
    allowEmptyPath_ = allowEmptyPath;
    missingEndStream_ = missingEndStream;
    verifier.reset(msg.get());
  }

  bool onHeader(const HPACKHeaderName& name, const folly::fbstring& value);

  void onHeadersComplete(HTTPHeaderSize decodedSize);

  [[nodiscard]] bool hasStatus() const;

  // Change this to a map of decoded header blocks when we decide
  // to concurrently decode partial header blocks
  std::unique_ptr<HTTPMessage> msg;
  HTTPRequestVerifier verifier;
  FieldSectionError error;
  std::string headerErrorValue;
  folly::Optional<ProxygenError> proxygenError;
  HPACK::DecodeError decodeError{HPACK::DecodeError::NONE};

 private:
  [[nodiscard]] bool isTrailers() const {
    return isRequestTrailers_ || isResponseTrailers();
  }

  [[nodiscard]] bool isResponseTrailers() const {
    return !isRequest_ && !hasStatus_;
  }

  bool isRequest_{false};
  bool isRequestTrailers_{false};
  bool validate_{true};
  bool hasStatus_{false};
  bool regularHeaderSeen_{false};
  bool pseudoHeaderSeen_{false};
  // Default to false for now to match existing behavior
  bool strictValidation_{false};
  bool allowEmptyPath_{false};
  bool missingEndStream_{false};
  std::string firstPseudoHeader_;
};

} // namespace proxygen
