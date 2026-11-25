/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/Utility.h>
#include <folly/coro/Error.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/DelayedDestruction.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/coro/Transport.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <proxygen/lib/http/coro/transport/test/TestCoroTransport.h>
#include <proxygen/lib/transport/qmux/FollyQmuxTransport.h>

#include <list>
#include <string>
#include <vector>

using proxygen::coro::test::TestCoroTransport;
using proxygen::qmux::FollyQmuxTransport;
using proxygen::qmux::QmuxTransport;

namespace {

using SocketByteEvent = folly::AsyncSocketObserverInterface::ByteEvent;
using DeliveryResult = QmuxTransport::DeliveryResult;

class ControlledTransport : public TestCoroTransport {
 public:
  ControlledTransport(folly::EventBase* evb, State* state)
      : TestCoroTransport(evb, state) {
    ON_CALL(mockAsyncSocket_, addLifecycleObserver(testing::_))
        .WillByDefault(
            [this](folly::AsyncSocket::LegacyLifecycleObserver* observer) {
              observers_.push_back(observer);
              observer->observerAttach(&mockAsyncSocket_);
              if (trackingSupported) {
                observer->byteEventsEnabled(&mockAsyncSocket_);
              } else {
                observer->byteEventsUnavailable(
                    &mockAsyncSocket_,
                    folly::AsyncSocketException(
                        folly::AsyncSocketException::NOT_SUPPORTED,
                        "No timestamps"));
              }
            });
  }

  using TestCoroTransport::write;
  folly::coro::Task<folly::Unit> write(folly::IOBufQueue& buf,
                                       std::chrono::milliseconds timeout,
                                       folly::WriteFlags flags,
                                       WriteInfo* info) noexcept override {
    writeFlags = flags;
    rawOffset += buf.chainLength() + tlsOverhead;
    if (folly::isSet(flags, folly::WriteFlags::TIMESTAMP_WRITE)) {
      fireByteEvents(SocketByteEvent::WRITE, rawOffset);
    }
    if (failWrite) {
      fireByteEvents(SocketByteEvent::ACK, rawOffset);
      co_yield folly::coro::co_error(folly::AsyncSocketException(
          folly::AsyncSocketException::NETWORK_ERROR, "Write failed"));
    }
    co_return co_await TestCoroTransport::write(
        buf, timeout, folly::WriteFlags::NONE, info);
  }

  folly::AsyncTransport* getTransport() const override {
    return exposeSocket ? &mockAsyncSocket_ : nullptr;
  }

  void unavailable() {
    for (auto* observer : observers_) {
      observer->byteEventsUnavailable(
          &mockAsyncSocket_,
          folly::AsyncSocketException(
              folly::AsyncSocketException::NETWORK_ERROR, "Socket closed"));
    }
  }

  bool trackingSupported{true};
  bool exposeSocket{true};
  bool failWrite{false};
  uint64_t rawOffset{1000};
  uint64_t tlsOverhead{17};
  folly::WriteFlags writeFlags{folly::WriteFlags::NONE};
};

class FollyQmuxTransportTest : public testing::Test {
 protected:
  void SetUp() override {
    auto transport = std::make_unique<ControlledTransport>(&evb_, &state_);
    socket_ = transport.get();
    transport_ = std::make_unique<FollyQmuxTransport>(std::move(transport));
  }

  void TearDown() override {
    transport_.reset();
    drain();
  }

  void drain(int milliseconds = 5) {
    bool done = false;
    evb_.runAfterDelay([&] { done = true; }, milliseconds);
    while (!done) {
      evb_.loopOnce();
    }
  }

  void write(const std::string& data = "error",
             std::chrono::milliseconds timeout = std::chrono::seconds(1),
             QmuxTransport::DeliveryCallback callback = nullptr) {
    writes_.emplace_back(folly::IOBufQueue::cacheChainLength());
    writes_.back().append(folly::IOBuf::copyBuffer(data));
    if (!callback) {
      callback = [this](DeliveryResult result) {
        delivered_.push_back(result);
      };
    }
    folly::coro::co_withExecutor(
        &evb_, transport_->write(writes_.back(), timeout, std::move(callback)))
        .start([this](const folly::Try<folly::Unit>& result) {
          writesSucceeded_.push_back(!result.hasException());
        });
    drain();
  }

  folly::EventBase evb_;
  TestCoroTransport::State state_;
  ControlledTransport* socket_{nullptr};
  std::unique_ptr<FollyQmuxTransport> transport_;
  std::list<folly::IOBufQueue> writes_;
  std::vector<DeliveryResult> delivered_;
  std::vector<bool> writesSucceeded_;
};

TEST_F(FollyQmuxTransportTest, DeliveryWaitsForRawTcpAck) {
  write();
  EXPECT_EQ(writesSucceeded_, std::vector<bool>{true});
  EXPECT_TRUE(delivered_.empty());
  EXPECT_TRUE(
      folly::isSet(socket_->writeFlags, folly::WriteFlags::TIMESTAMP_WRITE));
  EXPECT_TRUE(
      folly::isSet(socket_->writeFlags, folly::WriteFlags::TIMESTAMP_ACK));

  socket_->fireByteEvents(SocketByteEvent::ACK, state_.writeOffset);
  drain();
  EXPECT_TRUE(delivered_.empty());

  socket_->fireByteEvents(SocketByteEvent::ACK, socket_->rawOffset);
  drain();
  EXPECT_EQ(delivered_,
            std::vector<DeliveryResult>{DeliveryResult::Acknowledged});

  socket_->fireByteEvents(SocketByteEvent::ACK, socket_->rawOffset);
  socket_->unavailable();
  drain();
  EXPECT_EQ(delivered_,
            std::vector<DeliveryResult>{DeliveryResult::Acknowledged});
}

TEST_F(FollyQmuxTransportTest, EarlyAckWaitsForWriteCompletion) {
  socket_->pauseWrites();
  write();
  socket_->fireByteEvents(SocketByteEvent::ACK, socket_->rawOffset);
  drain();
  EXPECT_TRUE(writesSucceeded_.empty());
  EXPECT_TRUE(delivered_.empty());

  socket_->resumeWrites();
  drain();
  EXPECT_EQ(writesSucceeded_, std::vector<bool>{true});
  EXPECT_EQ(delivered_,
            std::vector<DeliveryResult>{DeliveryResult::Acknowledged});
}

TEST_F(FollyQmuxTransportTest, AckOnlyCompletesCoveredWrites) {
  write("first");
  auto firstOffset = socket_->rawOffset;
  write("second");

  socket_->fireByteEvents(SocketByteEvent::ACK, firstOffset);
  drain();
  EXPECT_EQ(delivered_,
            std::vector<DeliveryResult>{DeliveryResult::Acknowledged});

  socket_->fireByteEvents(SocketByteEvent::ACK, socket_->rawOffset);
  drain();
  EXPECT_EQ(delivered_,
            (std::vector<DeliveryResult>{DeliveryResult::Acknowledged,
                                         DeliveryResult::Acknowledged}));
}

TEST_F(FollyQmuxTransportTest, ManyWritesWaitForAcknowledgment) {
  constexpr size_t kWrites = 100;
  for (size_t i = 0; i < kWrites; ++i) {
    write();
  }
  EXPECT_EQ(writesSucceeded_, std::vector<bool>(kWrites, true));
  EXPECT_TRUE(delivered_.empty());

  socket_->fireByteEvents(SocketByteEvent::ACK, socket_->rawOffset);
  drain();
  EXPECT_EQ(delivered_,
            std::vector<DeliveryResult>(kWrites, DeliveryResult::Acknowledged));
}

TEST_F(FollyQmuxTransportTest, UnsupportedTrackingWaitsForWriteCompletion) {
  socket_->trackingSupported = false;
  socket_->pauseWrites();
  write();
  EXPECT_EQ(socket_->writeFlags, folly::WriteFlags::NONE);
  EXPECT_TRUE(delivered_.empty());

  socket_->resumeWrites();
  drain();
  EXPECT_EQ(writesSucceeded_, std::vector<bool>{true});
  EXPECT_EQ(delivered_, std::vector<DeliveryResult>{DeliveryResult::Cancelled});
}

TEST_F(FollyQmuxTransportTest, TransportWithoutAsyncSocketCancelsDelivery) {
  socket_->exposeSocket = false;
  write();
  EXPECT_EQ(writesSucceeded_, std::vector<bool>{true});
  EXPECT_EQ(delivered_, std::vector<DeliveryResult>{DeliveryResult::Cancelled});
}

TEST_F(FollyQmuxTransportTest, WriteFailureWinsOverEarlyAck) {
  socket_->failWrite = true;
  write();
  EXPECT_EQ(writesSucceeded_, std::vector<bool>{false});
  EXPECT_EQ(delivered_, std::vector<DeliveryResult>{DeliveryResult::Cancelled});
}

TEST_F(FollyQmuxTransportTest, UnavailableTrackingCancelsPendingDeliveryOnce) {
  write();
  socket_->unavailable();
  socket_->fireByteEvents(SocketByteEvent::ACK, socket_->rawOffset);
  drain();
  EXPECT_EQ(delivered_, std::vector<DeliveryResult>{DeliveryResult::Cancelled});
}

TEST_F(FollyQmuxTransportTest, TrackingLostWhileWriteIsInFlight) {
  write("pending");
  socket_->pauseWrites();
  write("in flight");
  ASSERT_EQ(writesSucceeded_, std::vector<bool>{true});
  EXPECT_TRUE(delivered_.empty());

  socket_->unavailable();
  socket_->fireByteEvents(SocketByteEvent::ACK, socket_->rawOffset);
  drain();
  EXPECT_EQ(writesSucceeded_, std::vector<bool>{true});
  EXPECT_EQ(delivered_, std::vector<DeliveryResult>{DeliveryResult::Cancelled});

  socket_->resumeWrites();
  drain();
  EXPECT_EQ(writesSucceeded_, (std::vector<bool>{true, true}));
  EXPECT_EQ(delivered_,
            (std::vector<DeliveryResult>{DeliveryResult::Cancelled,
                                         DeliveryResult::Cancelled}));
}

TEST_F(FollyQmuxTransportTest, MissingAckRemainsPendingUntilTeardown) {
  write("error", std::chrono::milliseconds(20));
  drain(100);
  EXPECT_TRUE(delivered_.empty());

  transport_.reset();
  drain();
  EXPECT_EQ(delivered_, std::vector<DeliveryResult>{DeliveryResult::Cancelled});
}

TEST_F(FollyQmuxTransportTest, DestructionCancelsPendingDelivery) {
  write();
  EXPECT_TRUE(delivered_.empty());
  transport_.reset();
  drain();
  EXPECT_EQ(delivered_, std::vector<DeliveryResult>{DeliveryResult::Cancelled});
}

TEST_F(FollyQmuxTransportTest, DestructionDetachesObserverFromGuardedSocket) {
  transport_.reset();
  auto socket = folly::AsyncSocket::UniquePtr(new folly::AsyncSocket(&evb_));
  auto* underlyingSocket = socket.get();
  auto guard = std::make_unique<folly::DelayedDestruction::DestructorGuard>(
      underlyingSocket);
  transport_ = std::make_unique<FollyQmuxTransport>(
      std::make_unique<folly::coro::Transport>(&evb_, std::move(socket)));

  write();
  ASSERT_EQ(writesSucceeded_, std::vector<bool>{false});
  ASSERT_EQ(underlyingSocket->getLifecycleObservers().size(), 1);

  transport_.reset();
  EXPECT_TRUE(underlyingSocket->getDestroyPending());
  EXPECT_TRUE(underlyingSocket->getLifecycleObservers().empty());
  guard.reset();
  drain();
  EXPECT_EQ(delivered_, std::vector<DeliveryResult>{DeliveryResult::Cancelled});
}

TEST_F(FollyQmuxTransportTest, DeliveryCallbackCanDestroyTransport) {
  write("error", std::chrono::seconds(1), [this](DeliveryResult result) {
    delivered_.push_back(result);
    transport_.reset();
  });
  auto firstOffset = socket_->rawOffset;
  write("pending");
  socket_->fireByteEvents(SocketByteEvent::ACK, firstOffset);
  drain();
  EXPECT_EQ(delivered_,
            (std::vector<DeliveryResult>{DeliveryResult::Acknowledged,
                                         DeliveryResult::Cancelled}));
  EXPECT_EQ(transport_, nullptr);
}

} // namespace
