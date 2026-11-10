/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/Singleton.h>
#include <folly/io/async/EventBase.h>
#include <proxygen/lib/http/webtransport/QuicWebTransport.h>
#include <proxygen/lib/http/webtransport/test/FakeSharedWebTransport.h>
#include <proxygen/lib/http/webtransport/test/Mocks.h>

namespace proxygen::test {

namespace {
std::unique_ptr<folly::IOBuf> makeBuf(size_t len) {
  auto buf = folly::IOBuf::create(len);
  buf->append(len);
  return buf;
}
} // namespace

TEST(WebTransportTest, ErrorCodes) {
  EXPECT_EQ(WebTransport::toHTTPErrorCode(0), WebTransport::kFirstErrorCode);
  EXPECT_EQ(WebTransport::toHTTPErrorCode(std::numeric_limits<uint32_t>::max()),
            WebTransport::kLastErrorCode);
  EXPECT_EQ(WebTransport::toApplicationErrorCode(0).error(),
            WebTransport::ErrorCode::GENERIC_ERROR);
  EXPECT_EQ(WebTransport::toApplicationErrorCode(WebTransport::kFirstErrorCode)
                .value(),
            0);
  EXPECT_EQ(WebTransport::toApplicationErrorCode(WebTransport::kLastErrorCode)
                .value(),
            std::numeric_limits<uint32_t>::max());
}

TEST(WebTransportTest, AwaitNextRead) {
  folly::SingletonVault::singleton()->registrationComplete();
  folly::EventBase evb;
  MockStreamReadHandle readHandle(0);
  EXPECT_CALL(readHandle, readStreamData()).WillOnce(testing::Invoke([] {
    return folly::makeFuture(
        WebTransport::StreamData({.data = nullptr, .fin = true}));
  }));

  readHandle.awaitNextRead(
      &evb,
      [&](WebTransport::StreamReadHandle* handle,
          uint64_t,
          folly::Try<WebTransport::StreamData> streamData) {
        EXPECT_EQ(handle, nullptr);
        EXPECT_EQ(streamData.value().data, nullptr);
        EXPECT_TRUE(streamData.value().fin);
      });
  evb.loopOnce();
}

TEST(WebTransportTest, AwaitNextReadTimeout) {
  folly::SingletonVault::singleton()->registrationComplete();
  folly::EventBase evb;
  MockStreamReadHandle readHandle(0);
  auto [promise, future] =
      folly::makePromiseContract<WebTransport::StreamData>();
  EXPECT_CALL(readHandle, readStreamData())
      .WillOnce(testing::Invoke([&, &readFuture = future] { //
        return std::move(readFuture);
      }));

  readHandle.awaitNextRead(
      &evb,
      [&, &readPromise = promise](
          WebTransport::StreamReadHandle* handle,
          uint64_t,
          folly::Try<WebTransport::StreamData> streamData) {
        EXPECT_EQ(handle, nullptr);
        EXPECT_NE(streamData.tryGetExceptionObject<folly::FutureException>(),
                  nullptr);
        // The promise core holds a keep alive to the event base, kill it here
        auto deadPromise = std::move(readPromise);
      },
      std::chrono::milliseconds(100));
  evb.loop();
}

class FakeReliableResetTest : public testing::Test {
 protected:
  void SetUp() override {
    client_->setPeerHandler(&serverHandler_);
    server_->setPeerHandler(&clientHandler_);
    EXPECT_CALL(serverHandler_, onNewUniStream(testing::_))
        .WillOnce([this](auto* handle) { readHandle_ = handle; });
    writeHandle_ = client_->createUniStream().value();
    auto* fake = CHECK_NOTNULL(dynamic_cast<FakeStreamHandle*>(writeHandle_));
    fake->setImmediateDelivery(false);
  }

  void deliver() {
    static_cast<FakeStreamHandle*>(writeHandle_)->deliverInflightData();
  }

  uint64_t readLen() {
    auto fut = readHandle_->readStreamData();
    EXPECT_TRUE(fut.isReady());
    return std::move(fut).get().data->computeChainDataLength();
  }

  void expectReadError() {
    auto fut = readHandle_->readStreamData();
    ASSERT_TRUE(fut.isReady());
    EXPECT_THROW(std::move(fut).get(), WebTransport::Exception);
  }

  std::pair<std::unique_ptr<FakeSharedWebTransport>,
            std::unique_ptr<FakeSharedWebTransport>>
      transports_{FakeSharedWebTransport::makeSharedWebTransport()};
  FakeSharedWebTransport* client_{transports_.first.get()};
  FakeSharedWebTransport* server_{transports_.second.get()};
  MockWebTransportHandler clientHandler_;
  MockWebTransportHandler serverHandler_;
  WebTransport::StreamWriteHandle* writeHandle_{nullptr};
  WebTransport::StreamReadHandle* readHandle_{nullptr};
};

TEST_F(FakeReliableResetTest, LastCheckpointWins) {
  writeHandle_->writeStreamData(makeBuf(50), false, nullptr, true);
  writeHandle_->writeStreamData(makeBuf(50), false, nullptr, true);
  writeHandle_->writeStreamData(makeBuf(50), false, nullptr);
  writeHandle_->resetStream(42);
  deliver();
  EXPECT_EQ(readLen(), 100);
  expectReadError();
}

TEST_F(FakeReliableResetTest, NoCheckpoint) {
  writeHandle_->writeStreamData(makeBuf(100), false, nullptr);
  writeHandle_->resetStream(42);
  deliver();
  expectReadError();
}

TEST_F(FakeReliableResetTest, PendingRead) {
  // 60 bytes fulfill a pending read. The next 40 reach the checkpoint, and the
  // reset drops the 50 after it while a second read is pending.
  auto readFut = readHandle_->readStreamData();
  writeHandle_->writeStreamData(makeBuf(60), false, nullptr);
  deliver();
  EXPECT_EQ(std::move(readFut).get().data->computeChainDataLength(), 60);

  readFut = readHandle_->readStreamData();
  writeHandle_->writeStreamData(makeBuf(40), false, nullptr, true);
  writeHandle_->writeStreamData(makeBuf(50), false, nullptr);
  writeHandle_->resetStream(42);
  EXPECT_FALSE(readFut.isReady());
  deliver();
  EXPECT_EQ(std::move(readFut).get().data->computeChainDataLength(), 40);
  expectReadError();
}

} // namespace proxygen::test
