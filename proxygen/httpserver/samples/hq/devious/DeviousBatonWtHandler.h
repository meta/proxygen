/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/async/EventBase.h>
#include <proxygen/httpserver/samples/hq/devious/DeviousBaton.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/webtransport/WebTransport.h>

namespace devious {

/**
 * Server side of the devious baton protocol, for the WtStreamManager based
 * WebTransport sessions in proxygen/lib and proxygen::coro, over h2 and h3.
 */
class DeviousBatonWtHandler : public proxygen::WebTransportHandler {
 public:
  explicit DeviousBatonWtHandler(folly::EventBase* evb);
  ~DeviousBatonWtHandler() noexcept override;
  DeviousBatonWtHandler(const DeviousBatonWtHandler&) = delete;
  DeviousBatonWtHandler& operator=(const DeviousBatonWtHandler&) = delete;
  DeviousBatonWtHandler(DeviousBatonWtHandler&&) = delete;
  DeviousBatonWtHandler& operator=(DeviousBatonWtHandler&&) = delete;

  // Validates the CONNECT request and returns the response to send. The
  // handler serves the session only if it accompanies a 2xx response.
  proxygen::HTTPMessage onRequest(const proxygen::HTTPMessage& request);

  void onWebTransportSession(
      std::shared_ptr<proxygen::WebTransport> wt) noexcept override;
  void onNewUniStream(
      proxygen::WebTransport::StreamReadHandle* readHandle) noexcept override;
  void onNewBidiStream(
      proxygen::WebTransport::BidiStreamHandle bidiHandle) noexcept override;
  void onDatagrams(
      std::vector<std::unique_ptr<folly::IOBuf>> datagrams) noexcept override;
  void onSessionEnd(folly::Optional<uint32_t> error) noexcept override;
  void onSessionDrain() noexcept override;

 private:
  struct State;
  // Pending reads hold a reference, since they can complete after the
  // session has destroyed this handler.
  std::shared_ptr<State> state_;
  bool startImmediately_{false};
};

} // namespace devious
