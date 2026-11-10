/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Function.h>
#include <proxygen/lib/http/webtransport/WebTransport.h>

#include <memory>
#include <utility>
#include <vector>

namespace proxygen {

/**
 * Shares ownership of a handler with the WebTransport, which owns its handler
 * uniquely. The handler is given up front or created from the WebTransport when
 * the session starts, and is released when the session ends.
 */
class SharedWebTransportHandler final : public WebTransportHandler {
 public:
  using Factory = folly::Function<std::shared_ptr<WebTransportHandler>(
      std::shared_ptr<WebTransport>)>;

  explicit SharedWebTransportHandler(
      std::shared_ptr<WebTransportHandler> handler)
      : handler_(std::move(handler)) {
  }

  // factory runs in onWebTransportSession. If it returns null, the session's
  // events are dropped.
  explicit SharedWebTransportHandler(Factory factory)
      : factory_(std::move(factory)) {
  }

  void onWebTransportSession(
      std::shared_ptr<WebTransport> wt) noexcept override {
    if (factory_) {
      handler_ = std::exchange(factory_, nullptr)(wt);
    }
    if (handler_) {
      handler_->onWebTransportSession(std::move(wt));
    }
  }

  void onNewUniStream(
      WebTransport::StreamReadHandle* readHandle) noexcept override {
    if (handler_) {
      handler_->onNewUniStream(readHandle);
    }
  }

  void onNewBidiStream(
      WebTransport::BidiStreamHandle bidiHandle) noexcept override {
    if (handler_) {
      handler_->onNewBidiStream(bidiHandle);
    }
  }

  void onDatagrams(
      std::vector<std::unique_ptr<folly::IOBuf>> datagrams) noexcept override {
    if (handler_) {
      handler_->onDatagrams(std::move(datagrams));
    }
  }

  void onSessionEnd(folly::Optional<uint32_t> error) noexcept override {
    if (auto handler = std::exchange(handler_, nullptr)) {
      handler->onSessionEnd(error);
    }
  }

  void onSessionDrain() noexcept override {
    if (handler_) {
      handler_->onSessionDrain();
    }
  }

 private:
  Factory factory_;
  std::shared_ptr<WebTransportHandler> handler_;
};

} // namespace proxygen
