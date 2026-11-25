/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <proxygen/lib/transport/qmux/FollyQmuxTransport.h>

#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBase.h>

#include <algorithm>
#include <cstdint>
#include <list>
#include <optional>
#include <utility>

namespace proxygen::qmux {

class FollyQmuxTransport::DeliveryObserver
    : public folly::AsyncSocket::LegacyLifecycleObserver {
 public:
  explicit DeliveryObserver(folly::EventBase* evb)
      : LegacyLifecycleObserver(config()), evb_(evb) {
  }

  DeliveryObserver(const DeliveryObserver&) = delete;
  DeliveryObserver& operator=(const DeliveryObserver&) = delete;
  DeliveryObserver(DeliveryObserver&&) = delete;
  DeliveryObserver& operator=(DeliveryObserver&&) = delete;

  ~DeliveryObserver() override {
    if (socket_) {
      socket_->removeLifecycleObserver(this);
    }
    unavailable();
    if (currentWrite_) {
      onWriteComplete(false);
    }
  }

  // QmuxSession serializes local writes.
  folly::WriteFlags registerWrite(DeliveryCallback callback) {
    currentWrite_.emplace(PendingDelivery{
        .callback = std::move(callback),
        .rawOffset = std::nullopt,
        .cancelled = !enabled_,
    });
    return enabled_ ? folly::WriteFlags::TIMESTAMP_WRITE |
                          folly::WriteFlags::TIMESTAMP_ACK
                    : folly::WriteFlags::NONE;
  }

  void onWriteComplete(bool success) {
    auto event = std::move(*currentWrite_);
    currentWrite_.reset();
    if (!success || event.cancelled || !event.rawOffset) {
      complete(std::move(event.callback), DeliveryResult::Cancelled);
    } else if (ackedOffset_ && *ackedOffset_ >= *event.rawOffset) {
      complete(std::move(event.callback), DeliveryResult::Acknowledged);
    } else {
      pending_.push_back(std::move(event));
    }
  }

  void observerAttach(folly::AsyncSocket* socket) noexcept override {
    socket_ = socket;
  }

  void observerDetach(folly::AsyncSocket*) noexcept override {
    socket_ = nullptr;
    unavailable();
  }

  void destroy(folly::AsyncSocket* sock) noexcept override {
    observerDetach(sock);
  }

  void close(folly::AsyncSocket*) noexcept override {
    unavailable();
  }

  void byteEventsEnabled(folly::AsyncSocket*) noexcept override {
    enabled_ = true;
  }

  void byteEventsUnavailable(
      folly::AsyncSocket*,
      const folly::AsyncSocketException&) noexcept override {
    unavailable();
  }

  void byteEvent(folly::AsyncSocket*,
                 const folly::AsyncSocketObserverInterface::ByteEvent&
                     event) noexcept override {
    using Type = folly::AsyncSocketObserverInterface::ByteEvent::Type;
    if (event.type == Type::WRITE && currentWrite_) {
      currentWrite_->rawOffset = event.offset;
    } else if (event.type == Type::ACK) {
      ackedOffset_ = std::max<uint64_t>(ackedOffset_.value_or(0), event.offset);
      while (!pending_.empty() &&
             *pending_.front().rawOffset <= *ackedOffset_) {
        completeFront(DeliveryResult::Acknowledged);
      }
    }
  }

 private:
  struct PendingDelivery {
    DeliveryCallback callback;
    std::optional<uint64_t> rawOffset;
    bool cancelled;
  };

  static Config config() {
    Config result;
    result.byteEvents = true;
    return result;
  }

  void unavailable() {
    enabled_ = false;
    if (currentWrite_) {
      currentWrite_->cancelled = true;
    }
    while (!pending_.empty()) {
      completeFront(DeliveryResult::Cancelled);
    }
  }

  void completeFront(DeliveryResult result) {
    auto callback = std::move(pending_.front().callback);
    pending_.pop_front();
    complete(std::move(callback), result);
  }

  void complete(DeliveryCallback callback, DeliveryResult result) {
    if (callback) {
      evb_->runInLoop([callback = std::move(callback), result]() mutable {
        callback(result);
      });
    }
  }

  folly::EventBase* evb_;
  folly::AsyncSocket* socket_{nullptr};
  std::list<PendingDelivery> pending_;
  std::optional<PendingDelivery> currentWrite_;
  std::optional<uint64_t> ackedOffset_;
  bool enabled_{false};
};

} // namespace proxygen::qmux
