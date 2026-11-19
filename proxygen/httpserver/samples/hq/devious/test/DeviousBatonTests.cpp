/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/MoveWrapper.h>
#include <folly/futures/Future.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <proxygen/httpserver/samples/hq/devious/DeviousBaton.h>
#include <proxygen/httpserver/samples/hq/devious/DeviousBatonWtHandler.h>
#include <proxygen/lib/http/webtransport/test/Mocks.h>

using devious::DeviousBaton;
using devious::DeviousBatonWtHandler;
using proxygen::test::MockWebTransport;
using testing::_;

struct Message {
  uint64_t id;
  std::unique_ptr<folly::IOBuf> message;
};
folly::SemiFuture<Message> expectSendMessage(MockWebTransport& wt) {
  auto [promise, future] = folly::makePromiseContract<Message>();

  EXPECT_CALL(wt, writeStreamData(_, _, _, _, _))
      .WillOnce([&wt, promise = folly::MoveWrapper(std::move(promise))](
                    uint64_t id,
                    std::unique_ptr<folly::IOBuf> data,
                    bool eof,
                    proxygen::WebTransport::ByteEventCallback*
                    /* deliveryCallback */,
                    bool /* checkpoint */) mutable
                    -> folly::Expected<proxygen::WebTransport::FCState,
                                       proxygen::WebTransport::ErrorCode> {
        Message m;
        m.id = id;
        m.message = std::move(data);
        promise->setValue(std::move(m));
        EXPECT_TRUE(eof);
        wt.cleanupStream(id);
        return proxygen::WebTransport::FCState::UNBLOCKED;
      })
      .RetiresOnSaturation();

  return std::move(future);
}

class DeviousBatonTest : public testing::TestWithParam<uint8_t> {};

TEST_P(DeviousBatonTest, Basic) {
  MockWebTransport mockClientWt;
  MockWebTransport mockServerWt;
  mockServerWt.nextUniStreamId_++;
  mockServerWt.nextBidiStreamId_++;
  DeviousBaton client(&mockClientWt, DeviousBaton::Mode::CLIENT, [](auto) {});
  DeviousBaton server(&mockServerWt, DeviousBaton::Mode::SERVER, [](auto) {});

  auto req = client.makeRequest(0, 1, {GetParam()});
  auto nextMessageFuture = expectSendMessage(mockServerWt);
  auto res = server.onRequest(req);
  EXPECT_FALSE(res.hasError());
  server.start();
  DeviousBaton* nextActor = &client;
  MockWebTransport* nextActorWt = &mockClientWt;
  for (uint8_t baton = GetParam(); baton != 0; baton++) {
    EXPECT_TRUE(nextMessageFuture.isReady());
    auto nextMessage = std::move(nextMessageFuture).get();
    nextMessageFuture = expectSendMessage(*nextActorWt);
    DeviousBaton::BatonMessageState state;
    nextActor->onStreamData(
        nextMessage.id, state, std::move(nextMessage.message), true);
    nextActorWt->cleanupReadHandle(nextMessage.id);
    EXPECT_EQ(state.state, DeviousBaton::BatonMessageState::DONE);
    EXPECT_EQ(state.baton, baton);
    if (nextActor == &client) {
      nextActor = &server;
      nextActorWt = &mockServerWt;
    } else {
      nextActor = &client;
      nextActorWt = &mockClientWt;
    }
  }
  EXPECT_FALSE(res.hasError());
  EXPECT_TRUE(nextMessageFuture.isReady());
  auto nextMessage = std::move(nextMessageFuture).get();
  EXPECT_CALL(*nextActorWt, closeSession(folly::Optional<uint32_t>()));
  DeviousBaton::BatonMessageState state;
  nextActor->onStreamData(
      nextMessage.id, state, std::move(nextMessage.message), true);
  nextActorWt->cleanupReadHandle(nextMessage.id);
  if (nextActor == &client) {
    mockServerWt.cleanupReadHandle(nextMessage.id);
  } else {
    mockClientWt.cleanupReadHandle(nextMessage.id);
  }
  EXPECT_EQ(state.state, DeviousBaton::BatonMessageState::DONE);
  EXPECT_EQ(state.baton, 0);
}

INSTANTIATE_TEST_SUITE_P(DeviousBatonTest,
                         DeviousBatonTest,
                         testing::Values(250, 1, 255));

namespace {
proxygen::HTTPMessage makeBatonRequest(bool immediate) {
  DeviousBaton client(
      /*inWt=*/nullptr, DeviousBaton::Mode::CLIENT, [](auto) {});
  auto req = client.makeRequest(/*version=*/0, /*count=*/1, {1});
  if (immediate) {
    req.setQueryParam("immediate", "1");
  }
  return req;
}
} // namespace

TEST(DeviousBatonWtHandlerTest, RejectsNonConnect) {
  folly::EventBase evb;
  DeviousBatonWtHandler handler(&evb);
  proxygen::HTTPMessage req;
  req.setMethod(proxygen::HTTPMethod::GET);
  req.setURL("/webtransport/devious-baton");
  EXPECT_EQ(handler.onRequest(req).getStatusCode(), 400);
}

TEST(DeviousBatonWtHandlerTest, ImmediateStartsWithoutStreamCredit) {
  folly::EventBase evb;
  DeviousBatonWtHandler handler(&evb);
  ASSERT_EQ(
      handler.onRequest(makeBatonRequest(/*immediate=*/true)).getStatusCode(),
      200);
  auto wt = std::make_shared<testing::NiceMock<MockWebTransport>>();
  EXPECT_CALL(*wt, awaitUniStreamCredit()).Times(0);
  auto sent = expectSendMessage(*wt);
  handler.onWebTransportSession(wt);
  EXPECT_TRUE(sent.isReady());
}

TEST(DeviousBatonWtHandlerTest, StartsOnStreamCredit) {
  folly::EventBase evb;
  DeviousBatonWtHandler handler(&evb);
  ASSERT_EQ(
      handler.onRequest(makeBatonRequest(/*immediate=*/false)).getStatusCode(),
      200);
  auto wt = std::make_shared<testing::NiceMock<MockWebTransport>>();
  auto [creditPromise, creditFuture] =
      folly::makePromiseContract<folly::Unit>();
  EXPECT_CALL(*wt, awaitUniStreamCredit())
      .WillOnce(testing::Return(testing::ByMove(std::move(creditFuture))));
  auto sent = expectSendMessage(*wt);
  handler.onWebTransportSession(wt);
  evb.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_FALSE(sent.isReady());
  creditPromise.setValue();
  evb.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_TRUE(sent.isReady());
}
