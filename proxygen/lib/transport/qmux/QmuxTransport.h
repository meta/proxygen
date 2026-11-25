/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <folly/Function.h>
#include <folly/SocketAddress.h>
#include <folly/coro/Task.h>
#include <folly/io/IOBufQueue.h>

namespace folly {
class AsyncTransport;
}

namespace proxygen::qmux {

class QmuxTransport {
 public:
  enum class DeliveryResult : std::uint8_t { Acknowledged, Cancelled };
  using DeliveryCallback = folly::Function<void(DeliveryResult)>;

  virtual ~QmuxTransport();

  virtual folly::coro::Task<size_t> read(folly::IOBufQueue& readBuf,
                                         size_t minReadSize,
                                         size_t newAllocationSize,
                                         std::chrono::milliseconds timeout) = 0;

  virtual folly::coro::Task<folly::Unit> write(
      folly::IOBufQueue& writeBuf, std::chrono::milliseconds timeout) = 0;

  virtual folly::coro::Task<folly::Unit> write(
      folly::IOBufQueue& writeBuf,
      std::chrono::milliseconds timeout,
      DeliveryCallback callback);

  virtual void shutdownWrite() = 0;

  [[nodiscard]] virtual folly::SocketAddress getLocalAddress()
      const noexcept = 0;
  [[nodiscard]] virtual folly::SocketAddress getPeerAddress()
      const noexcept = 0;
  [[nodiscard]] virtual folly::AsyncTransport* getUnderlyingTransport()
      const noexcept = 0;
};

} // namespace proxygen::qmux
