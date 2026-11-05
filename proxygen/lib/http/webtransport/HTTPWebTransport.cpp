/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <proxygen/lib/http/webtransport/HTTPWebTransport.h>

#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/HeaderConstants.h>
#include <proxygen/lib/http/structuredheaders/StructuredHeadersDecoder.h>
#include <proxygen/lib/http/structuredheaders/StructuredHeadersEncoder.h>

namespace proxygen {

using WTProtocolError = HTTPWebTransport::WTProtocolError;

/*static*/ bool HTTPWebTransport::isConnectMessage(const HTTPMessage& msg) {
  // webtransport-h3 is the draft-15+ token tied to SETTINGS_WT_ENABLED;
  // webtransport is the draft-14 token browsers still send
  return msg.isRequest() && msg.getMethod() == proxygen::HTTPMethod::CONNECT &&
         msg.getUpgradeProtocol() &&
         (*msg.getUpgradeProtocol() == headers::kWebTransportH3 ||
          *msg.getUpgradeProtocol() == headers::kWebTransport);
}

/*static*/ HTTPMessage HTTPWebTransport::makeConnectRequest(
    const std::string& authority,
    std::string path,
    const std::vector<std::string>& protocols) {
  HTTPMessage request;
  request.setHTTPVersion(1, 1);
  request.setSecure(true);
  request.getHeaders().set(HTTP_HEADER_HOST, authority);
  request.getHeaders().add(headers::kSecWebTransportHttp3Draft02,
                           headers::kSecWebTransportHttp3Draft02Value);
  request.setURL(std::move(path));
  request.setMethod(HTTPMethod::CONNECT);
  request.setUpgradeProtocol(std::string{headers::kWebTransport});
  setWTAvailableProtocols(request, protocols);
  return request;
}

/*static*/ void HTTPWebTransport::setWTAvailableProtocols(
    HTTPMessage& msg, const std::vector<std::string>& protocols) {
  std::vector<StructuredHeaders::StructuredHeaderItem> items;
  items.reserve(protocols.size());
  for (const auto& protocol : protocols) {
    items.emplace_back(StructuredHeaderItem::Type::STRING, protocol);
  }

  if (!items.empty()) {
    StructuredHeadersEncoder encoder;
    encoder.encodeList(items);
    msg.getHeaders().set(headers::kWTAvailableProtocols, encoder.get());
  }
}

/*static*/ void HTTPWebTransport::setWTProtocol(HTTPMessage& msg,
                                                std::string protocol) {
  StructuredHeadersEncoder encoder;
  StructuredHeaderItem item(StructuredHeaderItem::Type::STRING, protocol);
  encoder.encodeItem(item);
  msg.getHeaders().set(headers::kWTProtocol, encoder.get());
}

/*static*/ folly::Expected<std::vector<std::string>, WTProtocolError>
HTTPWebTransport::getWTAvailableProtocols(const HTTPMessage& msg) {
  auto header = msg.getHeaders().combine(headers::kWTAvailableProtocols);
  if (header.empty()) {
    return folly::makeUnexpected(WTProtocolError::HeaderMissing);
  }

  StructuredHeadersDecoder decoder(header);
  std::vector<StructuredHeaderItem> list;

  if (decoder.decodeList(list) != DecodeError::OK) {
    return folly::makeUnexpected(WTProtocolError::ParseFailed);
  }

  std::vector<std::string> protocols;
  for (const auto& item : list) {
    if (item.tag != StructuredHeaders::StructuredHeaderItem::Type::STRING) {
      return folly::makeUnexpected(WTProtocolError::ParseFailed);
    }
    protocols.emplace_back(item.get<std::string>());
  }

  if (protocols.empty()) {
    return folly::makeUnexpected(WTProtocolError::EmptyList);
  }

  return protocols;
}

/*static*/ folly::Expected<std::string, WTProtocolError>
HTTPWebTransport::getWTProtocol(const HTTPMessage& msg) {
  auto header = msg.getHeaders().getSingleOrEmpty(headers::kWTProtocol);
  if (header.empty()) {
    return folly::makeUnexpected(WTProtocolError::HeaderMissing);
  }

  StructuredHeadersDecoder decoder(header);
  StructuredHeaderItem item;

  if (decoder.decodeItem(item) != DecodeError::OK) {
    return folly::makeUnexpected(WTProtocolError::ParseFailed);
  }
  if (item.tag != StructuredHeaders::StructuredHeaderItem::Type::STRING) {
    return folly::makeUnexpected(WTProtocolError::ParseFailed);
  }

  return item.get<std::string>();
}

/*static*/ folly::Optional<std::string> HTTPWebTransport::negotiateWTProtocol(
    const std::vector<std::string>& wtAvailableProtocols,
    const std::vector<std::string>& supportedProtocols) {
  for (const auto& protocol : supportedProtocols) {
    if (std::find(wtAvailableProtocols.begin(),
                  wtAvailableProtocols.end(),
                  protocol) != wtAvailableProtocols.end()) {
      return protocol;
    }
  }

  return folly::none;
}

}; // namespace proxygen
