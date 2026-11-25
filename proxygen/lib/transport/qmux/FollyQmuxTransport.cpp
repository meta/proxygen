/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <proxygen/lib/transport/qmux/FollyQmuxTransport.h>

#include <proxygen/lib/transport/qmux/FollyQmuxDeliveryObserver.h>

#include <folly/coro/Error.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/coro/Transport.h>

#include <memory>
#include <utility>

namespace proxygen::qmux {

FollyQmuxTransport::FollyQmuxTransport(
    std::unique_ptr<folly::coro::TransportIf> transport)
    : transport_(std::move(transport)) {
}

FollyQmuxTransport::~FollyQmuxTransport() = default;

folly::coro::Task<size_t> FollyQmuxTransport::read(
    folly::IOBufQueue& readBuf,
    size_t minReadSize,
    size_t newAllocationSize,
    std::chrono::milliseconds timeout) {
  return transport_->read(readBuf, minReadSize, newAllocationSize, timeout);
}

folly::coro::Task<folly::Unit> FollyQmuxTransport::write(
    folly::IOBufQueue& writeBuf, std::chrono::milliseconds timeout) {
  return transport_->write(writeBuf, timeout);
}

folly::coro::Task<folly::Unit> FollyQmuxTransport::write(
    folly::IOBufQueue& writeBuf,
    std::chrono::milliseconds timeout,
    DeliveryCallback callback) {
  if (!callback) {
    co_return co_await transport_->write(writeBuf, timeout);
  }
  if (!deliveryObserver_) {
    deliveryObserver_ =
        std::make_unique<DeliveryObserver>(transport_->getEventBase());
    if (auto* transport = transport_->getTransport()) {
      if (auto* socket =
              transport->getUnderlyingTransport<folly::AsyncSocket>()) {
        socket->addLifecycleObserver(deliveryObserver_.get());
      }
    }
  }
  auto flags = deliveryObserver_->registerWrite(std::move(callback));
  auto result = co_await folly::coro::co_awaitTry(
      transport_->write(writeBuf, timeout, flags));
  deliveryObserver_->onWriteComplete(!result.hasException());
  co_return std::move(result).value();
}

void FollyQmuxTransport::shutdownWrite() {
  transport_->shutdownWrite();
}

folly::SocketAddress FollyQmuxTransport::getLocalAddress() const noexcept {
  return transport_->getLocalAddress();
}

folly::SocketAddress FollyQmuxTransport::getPeerAddress() const noexcept {
  return transport_->getPeerAddress();
}

folly::AsyncTransport* FollyQmuxTransport::getUnderlyingTransport()
    const noexcept {
  return transport_->getTransport();
}

} // namespace proxygen::qmux
