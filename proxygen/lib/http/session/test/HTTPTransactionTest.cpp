/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <chrono>
#include <cstdint>
#include <folly/Optional.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <memory>
#include <proxygen/lib/http/session/ByteEvents.h>
#include <proxygen/lib/http/session/HTTP2PriorityQueue.h>
#include <proxygen/lib/http/session/HTTPTransaction.h>
#include <proxygen/lib/http/session/test/HTTPTransactionMocks.h>
#include <proxygen/lib/http/session/test/MockHTTPTransactionObserver.h>
#include <string>

using namespace proxygen;
using namespace testing;
using namespace std::chrono_literals;
using TxnBytesEvent = HTTPTransactionObserverInterface::TxnBytesEvent;
using BodyByteStreamRange = TxnBytesEvent::BodyByteStreamRange;

TEST(BodyByteStreamRangeTest, BuilderRequiresAllFields) {
  EXPECT_DEATH(BodyByteStreamRange::Builder()
                   .setBodyOffset(2)
                   .setStreamOffset(3)
                   .setLength(4)
                   .build(),
               "Check failed");
  EXPECT_DEATH(BodyByteStreamRange::Builder()
                   .setStreamId(1)
                   .setStreamOffset(3)
                   .setLength(4)
                   .build(),
               "Check failed");
  EXPECT_DEATH(BodyByteStreamRange::Builder()
                   .setStreamId(1)
                   .setBodyOffset(2)
                   .setLength(4)
                   .build(),
               "Check failed");
  EXPECT_DEATH(BodyByteStreamRange::Builder()
                   .setStreamId(1)
                   .setBodyOffset(2)
                   .setStreamOffset(3)
                   .build(),
               "Check failed");
}

class HTTPTransactionObservabilityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    txn_ = std::make_unique<HTTPTransaction>(TransportDirection::DOWNSTREAM,
                                             HTTPCodec::StreamID(1),
                                             0,
                                             transport_,
                                             txnEgressQueue_,
                                             &evb_.timer());
  }

  void TearDown() override {
    if (txn_) {
      EXPECT_CALL(transport_, detach(txn_.get())).WillOnce([this] {
        txn_.reset();
      });
      EXPECT_CALL(transport_, sendAbort(txn_.get(), _));
      txn_->sendAbort();
    }
  }

  std::unique_ptr<MockHTTPTransactionObserver> addMockTxnObserver(
      MockHTTPTransactionObserver::EventSet eventSet) {
    auto observer =
        std::make_unique<NiceMock<MockHTTPTransactionObserver>>(eventSet);
    EXPECT_CALL(*observer, attached(Eq(txn_->getObserverAccessor()))).Times(1);
    txn_->addObserver(observer.get());
    return std::move(observer);
  }

  std::shared_ptr<MockHTTPTransactionObserver> addMockTxnObserverShared(
      MockHTTPTransactionObserver::EventSet eventSet) {
    auto observer =
        std::make_shared<NiceMock<MockHTTPTransactionObserver>>(eventSet);
    EXPECT_CALL(*observer, attached(Eq(txn_->getObserverAccessor()))).Times(1);
    txn_->addObserver(observer);
    return observer;
  }

  folly::EventBase evb_;
  HTTP2PriorityQueue txnEgressQueue_;
  StrictMock<MockHTTPTransactionTransport> transport_;
  std::unique_ptr<HTTPTransaction> txn_;
};

TEST_F(HTTPTransactionObservabilityTest, ObserverAttachedDetached) {
  MockHTTPTransactionObserver::EventSet eventSet;
  auto observer = addMockTxnObserver(eventSet);
  EXPECT_CALL(*observer, detached(Eq(txn_->getObserverAccessor()))).Times(1);
  txn_->removeObserver(observer.get());
}

TEST_F(HTTPTransactionObservabilityTest, ObserverAttachedDestroyed) {
  MockHTTPTransactionObserver::EventSet eventSet;
  auto observer = addMockTxnObserver(eventSet);
  EXPECT_CALL(*observer, destroyed(txn_->getObserverAccessor(), _));
  txn_ = nullptr;
}

TEST_F(HTTPTransactionObservabilityTest, ObserverAttachedDetachedSharedPtr) {
  MockHTTPTransactionObserver::EventSet eventSet;
  auto observer = addMockTxnObserverShared(eventSet);
  EXPECT_CALL(*observer, detached(Eq(txn_->getObserverAccessor()))).Times(1);
  txn_->removeObserver(observer.get());
}

TEST_F(HTTPTransactionObservabilityTest, ObserverAttachedDestroyedSharedPtr) {
  MockHTTPTransactionObserver::EventSet eventSet;
  auto observer = addMockTxnObserverShared(eventSet);
  EXPECT_CALL(*observer, destroyed(txn_->getObserverAccessor(), _));
  txn_ = nullptr;
}

TEST_F(HTTPTransactionObservabilityTest, ObserverAccessorReturnsHTTPPriority) {
  const HTTPPriority initialPriority(1 /* urgency */,
                                     false /* incremental */,
                                     7 /* orderId */,
                                     true /* paused */,
                                     1000 /* requiredBps */);
  const HTTPPriority updatedPriority(5 /* urgency */, true /* incremental */);
  EXPECT_CALL(transport_, getHTTPPriority())
      .WillOnce(Return(initialPriority))
      .WillOnce(Return(updatedPriority))
      .WillOnce(Return(folly::none));

  const auto* accessor = txn_->getObserverAccessor();
  EXPECT_EQ(accessor->getHTTPPriority(), initialPriority);
  EXPECT_EQ(accessor->getHTTPPriority(), updatedPriority);
  EXPECT_EQ(accessor->getHTTPPriority(), folly::none);
}

TEST_F(HTTPTransactionObservabilityTest,
       OnPriorityChange_NotifiesOnlyTxnPriorityObservers) {
  using TxnPriorityEvent = HTTPTransactionObserverInterface::TxnPriorityEvent;
  auto bytesObserver = addMockTxnObserver(
      MockHTTPTransactionObserver::EventSetBuilder()
          .enable(MockHTTPTransactionObserver::Events::TxnBytes)
          .build());
  auto priorityObserver = addMockTxnObserver(
      MockHTTPTransactionObserver::EventSetBuilder()
          .enable(MockHTTPTransactionObserver::Events::TxnPriority)
          .build());
  EXPECT_CALL(*bytesObserver, onPriorityEvent(_, _)).Times(0);
  EXPECT_CALL(
      *priorityObserver,
      onPriorityEvent(
          Eq(txn_->getObserverAccessor()),
          AllOf(Field(&TxnPriorityEvent::source,
                      TxnPriorityEvent::Source::PRIORITY_UPDATE),
                Field(&TxnPriorityEvent::priority, HTTPPriority(2, false)),
                Field(&TxnPriorityEvent::applied, false))));
  EXPECT_TRUE(txn_->hasPriorityEventObservers());
  txn_->onPriorityChange(HTTPPriority(2, false),
                         TxnPriorityEvent::Source::PRIORITY_UPDATE,
                         false /* applied */);
}

TEST(HTTPTransactionObserverAccessorTest, DefaultHTTPPriorityIsNone) {
  class TestAccessor : public HTTPTransactionObserverAccessor {
   public:
    bool addObserver(HTTPTransactionObserverContainer::Observer*) override {
      return false;
    }
    bool addObserver(
        std::shared_ptr<HTTPTransactionObserverContainer::Observer>) override {
      return false;
    }
    [[nodiscard]] uint64_t getTxnId() const override {
      return 0;
    }
  };

  const TestAccessor accessor;
  EXPECT_EQ(accessor.getHTTPPriority(), folly::none);
}

TEST_F(HTTPTransactionObservabilityTest,
       OnBytesEvent_ObserverNotSubscribedToTxnBytesEvent) {
  auto observer =
      addMockTxnObserver(MockHTTPTransactionObserver::EventSetBuilder()
                             .build() /* no events set */);
  EXPECT_CALL(*observer, onBytesEvent(_, _)).Times(0);
  txn_->onEgressHeaderFirstByte();
}

TEST_F(HTTPTransactionObservabilityTest,
       OnBytesEvent_ObserverSubscribedToTxnBytesEvent) {
  auto observer = addMockTxnObserver(
      MockHTTPTransactionObserver::EventSetBuilder()
          .enable(MockHTTPTransactionObserver::Events::TxnBytes)
          .build());
  InSequence s;
  EXPECT_CALL(*observer, onBytesEvent(_, _))
      .WillOnce(Invoke(
          [&](HTTPTransactionObserverAccessor* observerAccessor,
              const proxygen::HTTPTransactionObserverInterface::TxnBytesEvent&
                  event) {
            EXPECT_THAT(observerAccessor, Eq(txn_->getObserverAccessor()));
            EXPECT_THAT(event.type,
                        Eq(proxygen::HTTPTransactionObserverInterface::
                               TxnBytesEvent::Type::FIRST_HEADER_BYTE_WRITE));
          }));
  EXPECT_CALL(*observer, onBytesEvent(_, _))
      .WillOnce(Invoke(
          [&](HTTPTransactionObserverAccessor* observerAccessor,
              const proxygen::HTTPTransactionObserverInterface::TxnBytesEvent&
                  event) {
            EXPECT_THAT(observerAccessor, Eq(txn_->getObserverAccessor()));
            EXPECT_THAT(event.type,
                        Eq(proxygen::HTTPTransactionObserverInterface::
                               TxnBytesEvent::Type::FIRST_BODY_BYTE_WRITE));
          }));
  EXPECT_CALL(*observer, onBytesEvent(_, _))
      .WillOnce(Invoke(
          [&](HTTPTransactionObserverAccessor* observerAccessor,
              const proxygen::HTTPTransactionObserverInterface::TxnBytesEvent&
                  event) {
            EXPECT_THAT(observerAccessor, Eq(txn_->getObserverAccessor()));
            EXPECT_THAT(event.type,
                        Eq(proxygen::HTTPTransactionObserverInterface::
                               TxnBytesEvent::Type::LAST_BODY_BYTE_WRITE));
          }));
  EXPECT_CALL(*observer, onBytesEvent(_, _))
      .WillOnce(Invoke(
          [&](HTTPTransactionObserverAccessor* observerAccessor,
              const proxygen::HTTPTransactionObserverInterface::TxnBytesEvent&
                  event) {
            EXPECT_THAT(observerAccessor, Eq(txn_->getObserverAccessor()));
            EXPECT_THAT(event.type,
                        Eq(proxygen::HTTPTransactionObserverInterface::
                               TxnBytesEvent::Type::FIRST_BODY_BYTE_ACK));
          }));
  EXPECT_CALL(*observer, onBytesEvent(_, _))
      .WillOnce(Invoke(
          [&](HTTPTransactionObserverAccessor* observerAccessor,
              const proxygen::HTTPTransactionObserverInterface::TxnBytesEvent&
                  event) {
            EXPECT_THAT(observerAccessor, Eq(txn_->getObserverAccessor()));
            EXPECT_THAT(event.type,
                        Eq(proxygen::HTTPTransactionObserverInterface::
                               TxnBytesEvent::Type::LAST_BODY_BYTE_ACK));
          }));
  txn_->onEgressHeaderFirstByte();
  txn_->onEgressBodyFirstByte();
  txn_->onEgressBodyLastByte();
  txn_->onEgressTrackedByteEventAck(
      {0 /* byteOffset */, ByteEvent::FIRST_BYTE});
  txn_->onEgressLastByteAck(42ms /* latency */);

  // Currently, observers are not notified of egress bytes acked for non-first
  // byte
  EXPECT_CALL(*observer, onBytesEvent(_, _)).Times(0);
  txn_->onEgressTrackedByteEventAck(
      {2 /* byteOffset */, ByteEvent::TRACKED_BYTE});
  txn_->onEgressTrackedByteEventAck({3 /* byteOffset */, ByteEvent::LAST_BYTE});
}

TEST_F(HTTPTransactionObservabilityTest,
       BodyBytesGeneratedIncludesBodyStreamRange) {
  HTTPMessage response;
  response.setStatusCode(200);
  EXPECT_CALL(transport_, sendHeaders(txn_.get(), _, _, false))
      .WillOnce(
          [](HTTPTransaction*, const HTTPMessage&, HTTPHeaderSize* size, bool) {
            size->compressed = 1;
          });
  txn_->sendHeaders(response);

  auto observer = addMockTxnObserver(
      MockHTTPTransactionObserver::EventSetBuilder()
          .enable(MockHTTPTransactionObserver::Events::TxnBytes)
          .build());

  constexpr size_t kBodyLength = 100;
  constexpr size_t kEncodedLength = 102;
  EXPECT_CALL(transport_, notifyEgressBodyBuffered(kBodyLength));
  EXPECT_CALL(transport_, notifyPendingEgress());
  txn_->sendBody(folly::IOBuf::copyBuffer(std::string(kBodyLength, 'a')));

  EXPECT_CALL(transport_,
              notifyEgressBodyBuffered(-static_cast<int64_t>(kBodyLength)));
  EXPECT_CALL(transport_, sendBody(txn_.get(), _, false, false))
      .WillOnce(Return(HTTPTransaction::Transport::SendBodyResult{
          .encodedSize = kEncodedLength,
          .bodyByteStreamRange = BodyByteStreamRange::Builder()
                                     .setStreamId(1)
                                     .setBodyOffset(0)
                                     .setStreamOffset(2)
                                     .setLength(kBodyLength)
                                     .build()}));
  EXPECT_CALL(*observer, onBytesEvent(_, _))
      .WillOnce([&](HTTPTransactionObserverAccessor* observerAccessor,
                    const TxnBytesEvent& event) {
        EXPECT_THAT(observerAccessor, Eq(txn_->getObserverAccessor()));
        EXPECT_THAT(event.type, Eq(TxnBytesEvent::Type::BODY_BYTES_GENERATED));
        ASSERT_TRUE(event.maybeNumBytes.has_value());
        EXPECT_THAT(event.maybeNumBytes.value(), Eq(kEncodedLength));
        EXPECT_THAT(event.maybeNumBodyBytes, Optional(Eq(kBodyLength)));
        ASSERT_TRUE(event.maybeBodyByteStreamRange.has_value());
        const auto& range = *event.maybeBodyByteStreamRange;
        EXPECT_THAT(range.streamId, Eq(1));
        EXPECT_THAT(range.bodyOffset, Eq(0));
        EXPECT_THAT(range.streamOffset, Eq(2));
        EXPECT_THAT(range.length, Eq(kBodyLength));
      });
  txn_->onWriteReady(kBodyLength, 1.0);

  EXPECT_CALL(*observer, detached(Eq(txn_->getObserverAccessor()))).Times(1);
  txn_->removeObserver(observer.get());
}

TEST_F(HTTPTransactionObservabilityTest,
       ChunkedFlushEmitsOneBodyBytesGeneratedEventPerWrite) {
  HTTPMessage response;
  response.setStatusCode(200);
  EXPECT_CALL(transport_, sendHeaders(txn_.get(), _, _, false))
      .WillOnce(
          [](HTTPTransaction*, const HTTPMessage&, HTTPHeaderSize* size, bool) {
            size->compressed = 1;
          });
  txn_->sendHeaders(response);

  auto observer = addMockTxnObserver(
      MockHTTPTransactionObserver::EventSetBuilder()
          .enable(MockHTTPTransactionObserver::Events::TxnBytes)
          .build());

  constexpr size_t kBodyLength = 100;
  constexpr size_t kChunkHeaderLength = 4;
  constexpr size_t kChunkTerminatorLength = 2;
  constexpr size_t kEOMLength = 5;
  EXPECT_CALL(transport_, notifyEgressBodyBuffered(kBodyLength));
  EXPECT_CALL(transport_, notifyPendingEgress()).Times(AnyNumber());
  txn_->sendChunkHeader(kBodyLength);
  txn_->sendBody(folly::IOBuf::copyBuffer(std::string(kBodyLength, 'a')));
  txn_->sendChunkTerminator();
  txn_->sendEOM();

  EXPECT_CALL(transport_,
              notifyEgressBodyBuffered(-static_cast<int64_t>(kBodyLength)));
  EXPECT_CALL(transport_, sendChunkHeader(txn_.get(), kBodyLength))
      .WillOnce(Return(kChunkHeaderLength));
  EXPECT_CALL(transport_, sendBody(txn_.get(), _, false, false))
      .WillOnce(Return(HTTPTransaction::Transport::SendBodyResult{
          .encodedSize = kBodyLength, .bodyByteStreamRange = folly::none}));
  EXPECT_CALL(transport_, sendChunkTerminator(txn_.get()))
      .WillOnce(Return(kChunkTerminatorLength));
  EXPECT_CALL(transport_, sendEOM(txn_.get(), _)).WillOnce(Return(kEOMLength));
  {
    InSequence sequence;
    // The body write carries its chunk framing.
    EXPECT_CALL(*observer, onBytesEvent(_, _))
        .WillOnce(
            [](HTTPTransactionObserverAccessor*, const TxnBytesEvent& event) {
              EXPECT_THAT(event.type,
                          Eq(TxnBytesEvent::Type::BODY_BYTES_GENERATED));
              EXPECT_THAT(event.maybeNumBytes,
                          Optional(Eq(kChunkHeaderLength + kBodyLength +
                                      kChunkTerminatorLength)));
              EXPECT_THAT(event.maybeNumBodyBytes, Optional(Eq(kBodyLength)));
              EXPECT_FALSE(event.maybeBodyByteStreamRange.has_value());
            });
    // The EOM write is its own event with no body bytes.
    EXPECT_CALL(*observer, onBytesEvent(_, _))
        .WillOnce(
            [](HTTPTransactionObserverAccessor*, const TxnBytesEvent& event) {
              EXPECT_THAT(event.type,
                          Eq(TxnBytesEvent::Type::BODY_BYTES_GENERATED));
              EXPECT_THAT(event.maybeNumBytes, Optional(Eq(kEOMLength)));
              EXPECT_THAT(event.maybeNumBodyBytes, Optional(Eq(0)));
              EXPECT_FALSE(event.maybeBodyByteStreamRange.has_value());
            });
  }
  txn_->onWriteReady(1000, 1.0);

  EXPECT_CALL(*observer, detached(Eq(txn_->getObserverAccessor()))).Times(1);
  txn_->removeObserver(observer.get());
}

TEST_F(HTTPTransactionObservabilityTest,
       ObserverAttachedByHeadersEOMCallbackReceivesZeroBodyBytesGenerated) {
  auto observer = std::make_unique<NiceMock<MockHTTPTransactionObserver>>(
      MockHTTPTransactionObserver::EventSetBuilder()
          .enable(MockHTTPTransactionObserver::Events::TxnBytes)
          .build());
  NiceMock<MockHTTPTransactionTransportCallback> transportCallback;
  txn_->setTransportCallback(&transportCallback);
  HTTPMessage response;
  response.setStatusCode(200);
  EXPECT_CALL(transport_, sendHeaders(txn_.get(), _, _, true))
      .WillOnce(
          [](HTTPTransaction*, const HTTPMessage&, HTTPHeaderSize* size, bool) {
            size->compressed = 1;
          });

  EXPECT_CALL(transportCallback, bodyBytesGenerated(0)).WillOnce([&] {
    EXPECT_CALL(*observer, attached(Eq(txn_->getObserverAccessor())));
    txn_->addObserver(observer.get());
  });
  EXPECT_CALL(*observer, onBytesEvent(_, _))
      .WillOnce([](HTTPTransactionObserverAccessor*,
                   const TxnBytesEvent& event) {
        EXPECT_THAT(event.type, Eq(TxnBytesEvent::Type::BODY_BYTES_GENERATED));
        EXPECT_THAT(event.maybeNumBodyBytes, Optional(Eq(0)));
        EXPECT_NE(event.timestamp, SteadyClock::time_point{});
      });
  txn_->sendHeadersWithEOM(response);

  EXPECT_CALL(*observer, detached(Eq(txn_->getObserverAccessor()))).Times(1);
  txn_->removeObserver(observer.get());
  txn_->setTransportCallback(nullptr);
}

TEST_F(HTTPTransactionObservabilityTest,
       HeadersWithEOMGeneratedEventsShareTimestamp) {
  auto observer = addMockTxnObserver(
      MockHTTPTransactionObserver::EventSetBuilder()
          .enable(MockHTTPTransactionObserver::Events::TxnBytes)
          .build());
  HTTPMessage response;
  response.setStatusCode(200);
  EXPECT_CALL(transport_, sendHeaders(txn_.get(), _, _, true))
      .WillOnce(
          [](HTTPTransaction*, const HTTPMessage&, HTTPHeaderSize* size, bool) {
            size->compressed = 1;
          });

  folly::Optional<TimePoint> headerTimestamp;
  InSequence sequence;
  EXPECT_CALL(*observer, onBytesEvent(_, _))
      .WillOnce(
          [&](HTTPTransactionObserverAccessor*, const TxnBytesEvent& event) {
            EXPECT_THAT(event.type,
                        Eq(TxnBytesEvent::Type::HEADER_BYTES_GENERATED));
            headerTimestamp = event.timestamp;
          });
  EXPECT_CALL(*observer, onBytesEvent(_, _))
      .WillOnce([&](HTTPTransactionObserverAccessor*,
                    const TxnBytesEvent& event) {
        EXPECT_THAT(event.type, Eq(TxnBytesEvent::Type::BODY_BYTES_GENERATED));
        EXPECT_THAT(event.maybeNumBodyBytes, Optional(Eq(0)));
        ASSERT_TRUE(headerTimestamp.has_value());
        EXPECT_EQ(event.timestamp, *headerTimestamp);
      });
  txn_->sendHeadersWithEOM(response);

  EXPECT_CALL(*observer, detached(Eq(txn_->getObserverAccessor()))).Times(1);
  txn_->removeObserver(observer.get());
}

TEST_F(HTTPTransactionObservabilityTest,
       ObserverAttachedByTransportCallbackReceivesBodyBytesGenerated) {
  HTTPMessage response;
  response.setStatusCode(200);
  EXPECT_CALL(transport_, sendHeaders(txn_.get(), _, _, false))
      .WillOnce(
          [](HTTPTransaction*, const HTTPMessage&, HTTPHeaderSize* size, bool) {
            size->compressed = 1;
          });
  txn_->sendHeaders(response);

  auto observer = std::make_unique<NiceMock<MockHTTPTransactionObserver>>(
      MockHTTPTransactionObserver::EventSetBuilder()
          .enable(MockHTTPTransactionObserver::Events::TxnBytes)
          .build());
  NiceMock<MockHTTPTransactionTransportCallback> transportCallback;
  txn_->setTransportCallback(&transportCallback);

  constexpr size_t kBodyLength = 100;
  constexpr size_t kEncodedLength = 102;
  EXPECT_CALL(transport_, notifyEgressBodyBuffered(kBodyLength));
  EXPECT_CALL(transport_, notifyPendingEgress());
  txn_->sendBody(folly::IOBuf::copyBuffer(std::string(kBodyLength, 'a')));

  EXPECT_CALL(transport_,
              notifyEgressBodyBuffered(-static_cast<int64_t>(kBodyLength)));
  EXPECT_CALL(transport_, sendBody(txn_.get(), _, false, false))
      .WillOnce(Return(HTTPTransaction::Transport::SendBodyResult{
          .encodedSize = kEncodedLength,
          .bodyByteStreamRange = BodyByteStreamRange::Builder()
                                     .setStreamId(1)
                                     .setBodyOffset(0)
                                     .setStreamOffset(2)
                                     .setLength(kBodyLength)
                                     .build()}));
  EXPECT_CALL(transportCallback, bodyBytesGenerated(kEncodedLength))
      .WillOnce([&] {
        EXPECT_CALL(*observer, attached(Eq(txn_->getObserverAccessor())));
        txn_->addObserver(observer.get());
      });
  EXPECT_CALL(*observer, onBytesEvent(_, _))
      .WillOnce([&](HTTPTransactionObserverAccessor* observerAccessor,
                    const TxnBytesEvent& event) {
        EXPECT_THAT(observerAccessor, Eq(txn_->getObserverAccessor()));
        EXPECT_THAT(event.type, Eq(TxnBytesEvent::Type::BODY_BYTES_GENERATED));
        EXPECT_THAT(event.maybeNumBytes, Optional(Eq(kEncodedLength)));
        EXPECT_THAT(event.maybeNumBodyBytes, Optional(Eq(kBodyLength)));
        ASSERT_TRUE(event.maybeBodyByteStreamRange.has_value());
        const auto& range = *event.maybeBodyByteStreamRange;
        EXPECT_THAT(range.streamId, Eq(1));
        EXPECT_THAT(range.bodyOffset, Eq(0));
        EXPECT_THAT(range.streamOffset, Eq(2));
        EXPECT_THAT(range.length, Eq(kBodyLength));
      });
  txn_->onWriteReady(kBodyLength, 1.0);

  EXPECT_CALL(*observer, detached(Eq(txn_->getObserverAccessor())));
  txn_->removeObserver(observer.get());
  txn_->setTransportCallback(nullptr);
}
