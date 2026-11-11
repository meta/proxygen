/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "proxygen/lib/http/coro/HTTPStreamSource.h"
#include "proxygen/lib/http/coro/client/test/HTTPClientTestsCommon.h"
#include "proxygen/lib/http/coro/test/HTTPCoroSessionTests.h"
#include "proxygen/lib/http/coro/test/Mocks.h"
#include "proxygen/lib/http/coro/util/test/TestHelpers.h"
#include <proxygen/lib/http/codec/HQFramer.h>
#include <proxygen/lib/http/codec/HTTP2Codec.h>
#include <proxygen/lib/http/codec/webtransport/WebTransportCapsuleCodec.h>
#include <proxygen/lib/http/coro/util/CoroWtSession.h>
#include <proxygen/lib/http/coro/util/H3CoroWtSession.h>
#include <proxygen/lib/http/webtransport/WtStreamManager.h>
#include <proxygen/lib/http/webtransport/test/Mocks.h>
#include <quic/folly_utils/Utils.h>

using namespace proxygen;
using namespace proxygen::test;
using namespace testing;
using namespace proxygen::detail;

using folly::coro::blockingWait;
using folly::coro::co_awaitTry;
using folly::coro::co_reschedule_on_current_executor;

namespace proxygen::coro::test {

struct WtCapsuleCodecCallback : public WebTransportCapsuleCodec::Callback {
  ~WtCapsuleCodecCallback() override = default;
  void onResetStream(WTResetStreamCapsule capsule) noexcept override {
    rst.emplace(capsule);
    baton.post();
  }
  void onStopSending(WTStopSendingCapsule capsule) noexcept override {
    ss.emplace(capsule);
    baton.post();
  }
  void onStream(WTStreamCapsule capsule) noexcept override {
    XLOG(DBG4)
        << __func__ << "; id=" << capsule.streamId << "; len="
        << (capsule.streamData ? capsule.streamData->computeChainDataLength()
                               : 0);
    stream.emplace(std::move(capsule));
    baton.post();
  }
  void onMaxData(WTMaxDataCapsule capsule) noexcept override {
    md.emplace(capsule);
    baton.post();
  }
  void onMaxStreamData(WTMaxStreamDataCapsule capsule) noexcept override {
    msd.emplace(capsule);
    baton.post();
  }
  void onMaxStreamsBidi(WTMaxStreamsCapsule capsule) noexcept override {
    bidiMaxStreams.emplace(capsule);
    baton.post();
  }
  void onMaxStreamsUni(WTMaxStreamsCapsule capsule) noexcept override {
    uniMaxStreams.emplace(capsule);
    baton.post();
  }
  void onDatagram(DatagramCapsule dgram) noexcept override {
    dgrams.push_back(std::move(dgram.httpDatagramPayload));
    baton.post();
  }

  void onDataBlocked(WTDataBlockedCapsule) noexcept override {
  }
  void onStreamDataBlocked(WTStreamDataBlockedCapsule) noexcept override {
  }
  void onPadding(PaddingCapsule) noexcept override {
  }
  void onStreamsBlockedBidi(WTStreamsBlockedCapsule) noexcept override {
  }
  void onStreamsBlockedUni(WTStreamsBlockedCapsule) noexcept override {
  }
  void onCloseSession(CloseWebTransportSessionCapsule) noexcept override {
  }
  void onDrainSession(DrainWebTransportSessionCapsule) noexcept override {
  }
  void onConnectionError(
      WebTransportCapsuleCodec::ErrorCode) noexcept override {
    XLOG(FATAL) << "conn error";
  }
  void onCapsule(uint64_t capsuleType,
                 uint64_t capsuleLength) noexcept override {
    XLOG(DBG4) << __func__ << "; capsuleType=" << capsuleType
               << "; capsuleLength=" << capsuleLength;
  }

  folly::coro::Task<void> waitForEvent() {
    co_await baton;
    baton.reset();
  }

  std::optional<CloseWebTransportSessionCapsule> close;
  std::optional<DrainWebTransportSessionCapsule> drain;
  std::optional<WTResetStreamCapsule> rst;
  std::optional<WTStopSendingCapsule> ss;
  std::optional<WTStreamCapsule> stream;
  std::optional<WTMaxDataCapsule> md;
  std::optional<WTMaxStreamDataCapsule> msd;
  std::optional<WTMaxStreamsCapsule> bidiMaxStreams;
  std::optional<WTMaxStreamsCapsule> uniMaxStreams;
  std::vector<std::unique_ptr<folly::IOBuf>> dgrams;

 private:
  folly::coro::Baton baton;
};

class HttpWtUpstreamSessionTest : public HTTPCoroSessionTest {
 public:
  HttpWtUpstreamSessionTest()
      : HTTPCoroSessionTest(TransportDirection::UPSTREAM),
        serverCodec_(peerCodec_.get()) {
  }

  void SetUp() override {
    // need to set wt settings before HTTPCoroSession is constructed
    initSelfCodec_ = enableCodecWtSettings;
    HTTPCoroSessionTest::setUp();
  }

 protected:
  // TODO(@damlaj): derive from HTTPUpstreamSessionTest to reduce code
  // duplication
  HTTPMessage makeResponse(uint16_t statusCode) {
    HTTPMessage resp;
    resp.setHTTPVersion(1, 1);
    resp.setStatusCode(statusCode);
    return resp;
  }

  void deliverRespHeaders(HTTPCodec::StreamID id,
                          const HTTPMessage& resp,
                          bool eom = true) {
    serverCodec_->generateHeader(writeBuf_, id, resp, eom);
    // @lint-ignore CLANGTIDY
    transport_->addReadEvent(id, writeBuf_.move(), /*eof=*/false);
  }

  void deliverRstStream(HTTPCodec::StreamID id) {
    serverCodec_->generateRstStream(writeBuf_, id, ErrorCode::CANCEL);
    // @lint-ignore CLANGTIDY
    transport_->addReadEvent(id, writeBuf_.move(), /*eof=*/false);
  }

  HTTPCodec* serverCodec_{nullptr};
};

using H2WtUpstreamSessionTest = HttpWtUpstreamSessionTest;

CO_TEST_P_X(H2WtUpstreamSessionTest, Simple) {
  // valid wt req
  HTTPMessage msg;
  msg.setMethod(HTTPMethod::CONNECT);
  msg.setUpgradeProtocol("webtransport");

  auto reservation = session_->reserveRequest();
  auto fut =
      co_withExecutor(&evb_,
                      session_->sendWtReq(
                          std::move(*reservation), msg, DummyWtHandler::make()))
          .start();

  MockLifecycleObserver obs;
  session_->addLifecycleObserver(&obs);
  folly::coro::Baton waitForHeaders;
  EXPECT_CALL(obs, onIngressMessage(_, _)).WillOnce([&]() {
    waitForHeaders.post();
    session_->removeLifecycleObserver(&obs);
  });

  // serialize non-final 1xx continue
  deliverRespHeaders(/*id=*/1, makeResponse(100), /*eom=*/false);
  co_await waitForHeaders;     // wait for session to parse 1xx resp headers
  co_await rescheduleN(2);     // for good measure
  EXPECT_FALSE(fut.isReady()); // fut only resolves once final headers are rx'd

  // serialize final 2xx
  deliverRespHeaders(/*id=*/1, makeResponse(200), /*eom=*/true);
  auto res = co_await co_awaitTry(std::move(fut));

  EXPECT_TRUE(res->resp->is2xxResponse());
  EXPECT_NE(res->wt, nullptr);
  res->wt->closeSession(/*error=*/folly::none);
}

CO_TEST_P_X(H2WtUpstreamSessionTest, Non2xxResp) {
  // valid wt req
  HTTPMessage msg;
  msg.setMethod(HTTPMethod::CONNECT);
  msg.setUpgradeProtocol("webtransport");

  auto reservation = session_->reserveRequest();
  auto fut =
      co_withExecutor(&evb_,
                      session_->sendWtReq(
                          std::move(*reservation), msg, DummyWtHandler::make()))
          .start();

  // server rejecting CONNECT (i.e. sending 5xx) => res->wt == nullptr
  deliverRespHeaders(/*id=*/1, makeResponse(500), /*eom=*/false);
  auto res = co_await co_awaitTry(std::move(fut));
  EXPECT_TRUE(res->resp->is5xxResponse());
  EXPECT_EQ(res->wt, nullptr);
}

CO_TEST_P_X(H2WtUpstreamSessionTest, WtUpgradeReqRstErr) {
  // receiving a rst_stream when waiting for 2xx should propagate the
  // ::readHeaderEvent error
  HTTPMessage msg;
  msg.setMethod(HTTPMethod::CONNECT);
  msg.setUpgradeProtocol("webtransport");

  auto reservation = session_->reserveRequest();
  evb_.runAfterDelay([this]() { deliverRstStream(/*id=*/1); },
                     /*milliseconds=*/50);
  auto res = co_await co_awaitTry(session_->sendWtReq(
      std::move(*reservation), msg, DummyWtHandler::make()));
  auto* ex = res.tryGetExceptionObject<HTTPError>();
  EXPECT_TRUE(ex && ex->code == HTTPErrorCode::CANCEL);
}

CO_TEST_P_X(H2WtUpstreamSessionTest, SendInvalidWtReq) {
  HTTPMessage msg;
  msg.setMethod(HTTPMethod::GET);

  {
    // invalid reservation
    HTTPCoroSession::RequestReservation reservation;
    auto res = co_await co_awaitTry(session_->sendWtReq(
        std::move(reservation), msg, DummyWtHandler::make()));
    auto* ex = res.tryGetExceptionObject<HTTPError>();
    EXPECT_TRUE(ex && ex->code == HTTPErrorCode::INTERNAL_ERROR);
  }

  {
    // invalid msg
    auto reservation = session_->reserveRequest();
    auto res = co_await co_awaitTry(session_->sendWtReq(
        std::move(*reservation), msg, DummyWtHandler::make()));
    auto* ex = res.tryGetExceptionObject<HTTPError>();
    EXPECT_TRUE(ex && ex->code == HTTPErrorCode::INTERNAL_ERROR);
  }

  {
    setCodecWtSettings(*codec_, /*enabled=*/false);
    // unsupported webtransport (settings not set)
    msg.setMethod(HTTPMethod::CONNECT);
    msg.setUpgradeProtocol("webtransport");
    auto reservation = session_->reserveRequest();
    auto res = co_await co_awaitTry(session_->sendWtReq(
        std::move(*reservation), msg, DummyWtHandler::make()));
    auto* ex = res.tryGetExceptionObject<HTTPError>();
    EXPECT_TRUE(ex && ex->code == HTTPErrorCode::INTERNAL_ERROR);
  }
}

INSTANTIATE_TEST_SUITE_P(
    HttpWtUpstreamSessionTest,
    H2WtUpstreamSessionTest,
    Values(TestParams({.codecProtocol = CodecProtocol::HTTP_2})),
    paramsToTestName);

// unit tests where a wt session is already successfully established
class WtTest : public H2WtUpstreamSessionTest {
 public:
  WtTest() = default;

  void SetUp() override {
    H2WtUpstreamSessionTest::SetUp();
    constexpr auto kTimeout = std::chrono::milliseconds(50);
    session_->setConnectionReadTimeout(kTimeout);
    session_->setWriteTimeout(kTimeout);
    loopN(2);
    establishWtSession();

    /**
     * Every HTTPCoroSession upstream http/2 transport write is piped into the
     * peer http/2 codec, which subsequently pipes every http/2 body chunk into
     * WebTransportCapsuleCodec.
     */
    EXPECT_CALL(lifecycleObs_, onWrite(_, _)).WillRepeatedly([this]() {
      peerCodec_->onIngress(*coalesceWrites());
    });
    EXPECT_CALL(callbacks_, onBody(1, _, _))
        .WillRepeatedly([this](auto id, auto buf, auto padding) {
          XLOG(DBG5) << "::onBody len=" << buf->computeChainDataLength();
          wtCodec.onIngress(buf->clone(), false);
        });
    EXPECT_CALL(callbacks_, onMessageComplete(1, _)).WillRepeatedly([this]() {
      XLOG(DBG5) << "::onMessageComplete";
      wtCodec.onIngress(nullptr, true);
    });
    coroTp = CHECK_NOTNULL(dynamic_cast<TestCoroTransport*>(transport_));
  }

  void TearDown() override {
    wt->closeSession(folly::none);
  }

  void establishWtSession() {
    HTTPMessage msg;
    msg.setMethod(HTTPMethod::CONNECT);
    msg.setUpgradeProtocol("webtransport");

    auto handler = DummyWtHandler::make();
    wtHandlerCtx = handler->ctx;

    // send/serialize CONNECT request
    auto reservation = session_->reserveRequest();
    auto fut =
        co_withExecutor(&evb_,
                        session_->sendWtReq(
                            std::move(*reservation), msg, std::move(handler)))
            .start();
    evb_.loopOnce(); // serialize request

    // release 1mb of http/2 conn- and stream-level fc
    constexpr uint32_t kInitWindowSize = 1ul << 20; // 1MiB
    session_->onWindowUpdate(0, kInitWindowSize);
    session_->onWindowUpdate(1, kInitWindowSize);

    // serialize final 2xx
    deliverRespHeaders(/*id=*/1, makeResponse(200), /*eom=*/false);
    auto res = folly::coro::blockingWait(std::move(fut), &evb_);
    wt = std::move(res.wt);
  }

  std::unique_ptr<folly::IOBuf> coalesceWrites() {
    folly::IOBuf buf;
    auto writeEvents = std::move(coroTp->state_->writeEvents);
    for (auto& ev : writeEvents) {
      buf.appendToChain(ev.move());
    }
    return buf.pop();
  }

  void deliverWtData(std::unique_ptr<folly::IOBuf> wtData, bool eom = false) {
    peerCodec_->generateBody(writeBuf_,
                             1,
                             std::move(wtData),
                             /*padding=*/folly::none,
                             /*eom=*/eom);
    // @lint-ignore CLANGTIDY
    transport_->addReadEvent(1, writeBuf_.move(), /*eof=*/false);
  }

  void grantMaxData(uint64_t streamId, uint64_t offset) {
    if (streamId == kInvalidVarint) {
      writeWTMaxData(wtBuf, {offset});
    } else {
      writeWTMaxStreamData(wtBuf,
                           {.streamId = streamId, .maximumStreamData = offset});
    }
    deliverWtData(wtBuf.move());
  }

  std::shared_ptr<WebTransport> wt;
  // wtCodec & wtCodecCb are used for parsing capsules; simulating a peer
  // receiving data from client
  WtCapsuleCodecCallback wtCodecCb;
  WebTransportCapsuleCodec wtCodec{&wtCodecCb, CodecVersion::H2};
  TestCoroTransport* coroTp{nullptr};
  std::shared_ptr<const DummyWtHandler::Ctx> wtHandlerCtx;
  // wtBuf contains the serialize wt capsules; simulating a peer sending data
  // to client
  folly::IOBufQueue wtBuf{folly::IOBufQueue::cacheChainLength()};
};

/**
 * Tests the following egress related behaviours:
 *  - defaults to zero wt_initial_max_streams_uni/bidi; cannot create uni/bidi
 *    streams until we receive a max_streams capsule
 *
 *  - dequeues are blocked until connection- and stream-level flow
 *    control credit is received from peer
 *
 *  - writing only fin=true even if blocked on flow control should still go thru
 *
 *  - backpressure signal is irrelevant of peer fc credit and is resolved after
 *    the http/2 loop has dequeued from WtStreamManager
 */
CO_TEST_P_X(WtTest, SimpleUniEgress) {
  XCHECK(wt);

  // no available uni/bidi streams
  auto createStream = wt->createUniStream();
  EXPECT_TRUE(createStream.hasError());

  // asynchronously advertise max_streams
  evb_.runInLoop([&]() {
    writeWTMaxStreams(wtBuf, /*capsule=*/{1}, /*isBidi=*/false);
    deliverWtData(wtBuf.move());
  });

  co_await wt->awaitUniStreamCredit();

  // next awaitUniStreamCredit should be synchronously available
  wt->awaitUniStreamCredit().isReady();

  // peer advertised uni credit => ::createUniStream now yields handle
  createStream = wt->createUniStream();
  CHECK(createStream.hasValue());
  auto* wh = createStream.value();
  auto id = wh->getID();

  // fill up egress buffer => writes blocked
  constexpr uint16_t kBufLen = 65'535;
  auto writeRes =
      wt->writeStreamData(id, makeBuf(kBufLen), /*fin=*/false, nullptr);
  EXPECT_EQ(writeRes.value(), WebTransport::FCState::BLOCKED);

  // awaitWritable is only resolved after the data has been dequeued from
  // WtStreamManager
  auto awaitWritable = wt->awaitWritable(id);
  EXPECT_FALSE(awaitWritable->isReady());

  // asynchronously advertise max_data (default peer value when not present in
  // http settings is assumed to be 0)
  evb_.runInLoop([&]() {
    // grant conn- and stream-level fc
    grantMaxData(kInvalidVarint, kBufLen);
    grantMaxData(id, kBufLen);
  });

  // wait for wt capsule codec callback to fire
  co_await wtCodecCb.waitForEvent();

  // validate we've rx'd a wt_stream capsule with expected values
  auto streamEvent = std::exchange(wtCodecCb.stream, {});
  XCHECK(streamEvent.has_value());
  EXPECT_EQ(streamEvent->streamId, id);
  EXPECT_EQ(streamEvent->streamData->computeChainDataLength(), kBufLen);

  // when WtStreamManager dequeued the buffered data, awaitWritable is
  // resolved
  EXPECT_TRUE(awaitWritable->isReady() && awaitWritable->value() == kBufLen);

  // blocked on both connection- and stream-level fc; buffered data will not be
  // dequeued from WtStreamManager
  wt->writeStreamData(
      id, makeBuf(1), /*fin=*/false, /*deliveryCallback=*/nullptr);

  co_await rescheduleN(4);
  EXPECT_FALSE(wtCodecCb.stream.has_value()); // no data has been written

  // release 1 byte of stream-level wt fc; buffered data will still not be
  // dequeued from WtStreamManager as we're still blocked on connection-level fc
  grantMaxData(id, kBufLen + 1);
  co_await rescheduleN(5);
  EXPECT_FALSE(wtCodecCb.stream.has_value()); // no data has been written

  // release 1 byte of connection-level wt fc; buffered data will be dequeued
  // from WtStreamManager
  grantMaxData(kInvalidVarint, kBufLen + 1);
  co_await wtCodecCb.waitForEvent(); // ouch, this needs 5 loops otherwise
                                     // (i.e. co_await rescheduleN(5))
  streamEvent = std::exchange(wtCodecCb.stream, {});
  XCHECK(streamEvent.has_value());
  EXPECT_EQ(streamEvent->streamId, id);
  EXPECT_EQ(streamEvent->streamData->computeChainDataLength(), 1);

  // blocked on both connection- and stream-level fc; however if just fin=true,
  // this should be dequeued regardless of peer fc credit
  wt->writeStreamData(id, nullptr, /*fin=*/true, /*deliveryCallback=*/nullptr);

  co_await wtCodecCb.waitForEvent();
  streamEvent = std::exchange(wtCodecCb.stream, {});
  XCHECK(streamEvent.has_value());
  EXPECT_EQ(streamEvent->streamId, id);
  EXPECT_EQ(streamEvent->streamData->computeChainDataLength(), 0);
  EXPECT_EQ(streamEvent->fin, true);

  co_return;
}

/**
 * A write's delivery callback fires once the write loop serializes its data.
 */
CO_TEST_P_X(WtTest, DeliveryCallbackFires) {
  XCHECK(wt);
  struct DeliveryCallback : public WebTransport::ByteEventCallback {
    void onByteEvent(quic::StreamId id, uint64_t offset) noexcept override {
      delivered.emplace_back(id, offset);
    }
    void onByteEventCanceled(quic::StreamId, uint64_t) noexcept override {
      ++canceled;
    }
    std::vector<std::pair<quic::StreamId, uint64_t>> delivered;
    size_t canceled{0};
  } deliveryCb;

  evb_.runInLoop([&]() {
    writeWTMaxStreams(wtBuf, /*capsule=*/{1}, /*isBidi=*/false);
    deliverWtData(wtBuf.move());
  });
  co_await wt->awaitUniStreamCredit();
  auto id = wt->createUniStream().value()->getID();

  constexpr uint16_t kBufLen = 100;
  grantMaxData(kInvalidVarint, kBufLen);
  grantMaxData(id, kBufLen);
  wt->writeStreamData(id, makeBuf(kBufLen), /*fin=*/false, &deliveryCb);
  co_await wtCodecCb.waitForEvent();

  const std::vector<std::pair<quic::StreamId, uint64_t>> expected{
      {id, kBufLen - 1}};
  EXPECT_EQ(deliveryCb.delivered, expected);
  EXPECT_EQ(deliveryCb.canceled, 0);
}

/**
 * Tests the following ingress related behaviours:
 *  - Receiving data on a new stream id implicitly creates the data
 *
 *  - When bytes read have exceeded half the conn- and stream-level flow control
 *    advertised to the peer, the endpoint sends a max_data & max_stream_data
 *    frame respectively
 */
CO_TEST_P_X(WtTest, SimpleUniIngress) {
  XCHECK(wt);
  constexpr uint16_t kBufLen = 65'535;
  constexpr uint16_t kIngressId = 3;

  // send kBufLen / 2 bytes of data, ensure client is able to read
  writeWTStream(wtBuf,
                WTStreamCapsule{.streamId = kIngressId,
                                .streamData = makeBuf(kBufLen / 2 + 1),
                                .fin = false});
  deliverWtData(wtBuf.move());

  // wait until we get a peer stream
  while (wtHandlerCtx->peerStreams.empty()) {
    co_await folly::coro::co_reschedule_on_current_executor;
  }
  // ingress only => writeHandle == nullptr
  auto handle = wtHandlerCtx->peerStreams.at(0);
  EXPECT_FALSE(handle.writeHandle);
  EXPECT_TRUE(handle.readHandle);

  auto read = wt->readStreamData(kIngressId);
  EXPECT_TRUE(read->isReady());
  EXPECT_EQ(read->value().fin, false);
  EXPECT_EQ(read->value().data->computeChainDataLength(), kBufLen / 2 + 1);

  // consuming half of advertised rwnd issues MaxData & MaxStreamData to peer
  co_await wtCodecCb.waitForEvent();
  EXPECT_TRUE(wtCodecCb.md.has_value() && wtCodecCb.msd.has_value());

  // when receiving a rst_stream, read should resolve an exc
  read = wt->readStreamData(kIngressId);
  EXPECT_TRUE(read.hasValue());

  // asynchronously deliver reset stream
  evb_.runInLoop([&]() {
    writeWTResetStream(wtBuf,
                       {/*streamId=*/.streamId = kIngressId,
                        .appProtocolErrorCode = 0,
                        .reliableSize = 0});
    deliverWtData(wtBuf.move());
  });

  // expect read resolves with exception due to rst above
  auto readRes = co_await co_awaitTry(std::move(read).value());
  EXPECT_TRUE(readRes.hasException());

  co_return;
}

CO_TEST_P_X(WtTest, SimpleBidiEcho) {
  XCHECK(wt);
  constexpr uint16_t kBufLen = 65'535;

  // no available bidi streams
  auto createStream = wt->createBidiStream();
  EXPECT_TRUE(createStream.hasError());

  // asynchronously advertise max_streams
  evb_.runInLoop([&]() {
    writeWTMaxStreams(wtBuf, /*capsule=*/{1}, /*isBidi=*/true);
    deliverWtData(wtBuf.move());
  });

  co_await wt->awaitBidiStreamCredit();

  // next awaitBidiStreamCredit should be synchronously available
  EXPECT_TRUE(wt->awaitBidiStreamCredit().isReady());

  // peer advertised bidi credit => ::createBidiStream now yields handle
  createStream = wt->createBidiStream();
  CHECK(createStream.hasValue());
  auto handle = createStream.value();
  auto id = handle.readHandle->getID();

  // asynchronously advertise MaxData & MaxStreamData
  evb_.runInLoop([&]() {
    grantMaxData(kInvalidVarint, kBufLen);
    grantMaxData(id, kBufLen);
  });

  // asynchronously deliver varius wt frames to exercise codepaths
  evb_.runInLoop([&]() {
    writeWTDataBlocked(wtBuf, {kBufLen});
    writeWTStreamDataBlocked(
        wtBuf, {/*streamId=*/.streamId = 0, .maximumStreamData = kBufLen});
    writeWTStreamsBlocked(wtBuf, {10}, /*isBidi=*/false);
    writeWTStreamsBlocked(wtBuf, {10}, /*isBidi=*/true);
    writePadding(wtBuf, {10});
    writeDrainWebTransportSession(wtBuf);
    deliverWtData(wtBuf.move());
  });

  /**
   * in a loop – write one byte, wait for peer codec to parse byte, send the
   * byte back, and finally read the byte.
   */
  for (uint8_t idx = 0; idx < std::numeric_limits<uint8_t>::max(); idx++) {
    // write idx to stream
    auto buf = folly::IOBuf::copyBuffer(&idx, sizeof(idx));
    wt->writeStreamData(
        id, buf->clone(), /*fin=*/false, /*deliveryCallback=*/nullptr);

    // wait for peer codec to receive event
    co_await wtCodecCb.waitForEvent();

    // validate we've rx'd a wt_stream capsule with val idx
    auto streamEvent = std::exchange(wtCodecCb.stream, {});
    XCHECK(streamEvent.has_value());
    EXPECT_EQ(streamEvent->streamId, id);
    EXPECT_EQ(streamEvent->streamData->length(), 1);
    EXPECT_EQ(*streamEvent->streamData->data(), idx);

    // send the same byte back to client
    writeWTStream(wtBuf,
                  WTStreamCapsule{.streamId = id,
                                  .streamData = buf->clone(),
                                  .fin = false});
    deliverWtData(wtBuf.move());

    // expect to client to rx same byte
    auto read = co_await wt->readStreamData(id).value();
    EXPECT_EQ(*read.data->data(), idx);
  }

  // deliver both stop_sending & rst_stream, which should bidirectionally reset
  // the stream (tbd – should we wait for app to specifically invoke
  // ::resetStream before reaping state)
  evb_.runInLoop([&]() {
    writeWTStopSending(wtBuf, {.streamId = id});
    writeWTResetStream(
        wtBuf, {.streamId = id, .appProtocolErrorCode = 0, .reliableSize = 0});
    writeCloseWebTransportSession(
        wtBuf,
        {.applicationErrorCode = 0, .applicationErrorMessage = "close wt"});
    deliverWtData(wtBuf.move());
  });

  // stream is reset, ::read will return an exception
  auto read = co_await co_awaitTry(wt->readStreamData(id).value());
  EXPECT_TRUE(read.hasException());

  wt->closeSession();
}

CO_TEST_P_X(WtTest, TestReadTimeout) {
  // wait until WebTransportHandler::onSessionEnd is invoked; read timeout
  // should kick in
  while (!wtHandlerCtx->err.has_value()) {
    co_await rescheduleN(1);
  }
  wt->closeSession();
}

TEST_P(WtTest, TestErrConditions) {
  XCHECK(wt);

  // default no uni credit
  auto uniRes = wt->createUniStream();
  EXPECT_TRUE(uniRes.hasError());
  // default no bidi credit
  auto bidiRes = wt->createBidiStream();
  EXPECT_TRUE(bidiRes.hasError());

  // advertise one uni&bidi stream credit to client
  writeWTMaxStreams(wtBuf, /*capsule=*/{1}, /*isBidi=*/false);
  writeWTMaxStreams(wtBuf, /*capsule=*/{1}, /*isBidi=*/true);
  deliverWtData(wtBuf.move());
  loopN(2);

  /**
   * stream 0 doesn't exist; all ops expected to fail (e.g. write, read, reset,
   * stop_sending)
   */
  constexpr uint64_t streamId = 0;
  auto read = wt->readStreamData(streamId);
  auto write = wt->writeStreamData(
      streamId, /*data=*/nullptr, /*fin=*/false, /*deliveryCallback=*/nullptr);
  auto reset = wt->resetStream(streamId, /*error=*/0);
  auto ss = wt->stopSending(streamId, /*error=*/0);
  auto await = wt->awaitWritable(streamId);

  EXPECT_TRUE(read.hasError() && write.hasError() && reset.hasError() &&
              ss.hasError() && await.hasError());

  // quic transport info is defaulted
  std::ignore = wt->getTransportInfo();

  // local & peer addr sanity checks
  const auto& localAddr = wt->getLocalAddress();
  const auto& peerAddr = wt->getPeerAddress();
  EXPECT_EQ(localAddr.getIPAddress(), peerAddr.getIPAddress());
}

CO_TEST_P_X(WtTest, PeerBidiAndTransportEom) {
  // Deliver a peer-initiated bidi stream, followed by a transport eom (http/2
  // stream eom). This should trigger shutdown of WebTransport
  XCHECK(wt);

  constexpr uint16_t kBufLen = 65'535;
  constexpr uint16_t kIngressId = 1;

  // deliver a peer-initiated bidi stream of len=kBufLen & eom=false
  writeWTStream(wtBuf,
                WTStreamCapsule{.streamId = kIngressId,
                                .streamData = makeBuf(kBufLen),
                                .fin = false});
  deliverWtData(wtBuf.move());

  // wait until client receives stream
  while (wtHandlerCtx->peerStreams.empty()) {
    co_await rescheduleN(1);
  }

  // ensure it's recognized as bidi stream
  auto& handle = wtHandlerCtx->peerStreams.at(0);
  EXPECT_TRUE(handle.readHandle && handle.writeHandle);

  // data should be available synchronously
  auto read = wt->readStreamData(kIngressId);
  EXPECT_TRUE(read->isReady() && read->value().data &&
              read->value().fin == false);

  // next read will resolve with an error after http/2 stream eom is parsed
  read = wt->readStreamData(kIngressId);

  // deliver http/2 stream eom
  deliverWtData(/*wtData=*/nullptr, /*eom=*/true);

  // wait until WebTransportHandler::onSessionEnd is invoked
  while (!wtHandlerCtx->err.has_value()) {
    co_await rescheduleN(1);
  }

  // read should have an exception now
  EXPECT_TRUE(read->hasException());
}

CO_TEST_P_X(WtTest, MaxStreamsBidiUni) {
  // open 5 of each peer-initiated uni streams and peer-initiated bidi streams;
  // close them to verify that advertising MaxStreamsBidi/MaxStreamsUni is
  // working as expected (when 50% of the limit is reached)
  XCHECK(wt);
  uint64_t nextServerBidi = 0x01, nextServerUni = 0x03;

  // send 1 byte of data on the first five server bidi and server uni streams
  for (uint8_t idx = 0; idx < 5; idx++) {
    writeWTStream(wtBuf,
                  WTStreamCapsule{.streamId = nextServerBidi,
                                  .streamData = makeBuf(1),
                                  .fin = false});
    writeWTStream(wtBuf,
                  WTStreamCapsule{.streamId = nextServerUni,
                                  .streamData = makeBuf(1),
                                  .fin = true});
    // reset the ingress direction for bidi streams
    writeWTResetStream(
        wtBuf, {.streamId = nextServerBidi, .appProtocolErrorCode = 0x00});
    nextServerBidi += 4;
    nextServerUni += 4;
  }
  deliverWtData(wtBuf.move());

  // wait until client receives stream
  while (wtHandlerCtx->peerStreams.empty()) {
    co_await rescheduleN(1);
  }

  // terminate egress for each stream (via above, ingress closed either by
  // rst_stream for bidi or eom for uni)
  for (auto& handle : wtHandlerCtx->peerStreams) {
    // read handle is unconditional for peer-initiated streams
    const bool bidi = quic::isBidirectionalStream(handle.readHandle->getID());
    auto res = co_await co_awaitTry(handle.readHandle->readStreamData());
    EXPECT_EQ(res.hasException(), bidi);
    if (handle.writeHandle) {
      handle.writeHandle->resetStream(0);
    }
  }

  // wait for peer codec to receive event
  co_await wtCodecCb.waitForEvent();

  EXPECT_EQ(wtCodecCb.bidiMaxStreams->maximumStreams, 15);
  EXPECT_EQ(wtCodecCb.uniMaxStreams->maximumStreams, 15);

  wt->closeSession();
}

// A datagram too large to ever fit in a write chunk is rejected up front
// rather than left for the write loop to rediscover on every pass.
CO_TEST_P_X(WtTest, SendDatagramExceedsMaxWriteSize) {
  XCHECK(wt);
  auto res = wt->sendDatagram(makeBuf(65'535));
  EXPECT_TRUE(res.hasError());
  co_await rescheduleN(2);
  EXPECT_TRUE(wtCodecCb.dgrams.empty());

  wt->closeSession();
}

CO_TEST_P_X(WtTest, Datagrams) {
  XCHECK(wt);
  // tx datagram to peer
  wt->sendDatagram(folly::IOBuf::copyBuffer("datagram1"));
  // wait for peer codec to receive event
  co_await wtCodecCb.waitForEvent();
  CHECK_EQ(wtCodecCb.dgrams.size(), 1);
  EXPECT_EQ(wtCodecCb.dgrams.front()->toString(), "datagram1");

  // rx datagram from peer
  writeDatagram(wtBuf, {folly::IOBuf::copyBuffer("datagram2")});
  deliverWtData(wtBuf.move());
  // wait until client receives datagram
  while (wtHandlerCtx->dgrams.empty()) {
    co_await rescheduleN(1);
  }

  wt->closeSession();
}

INSTANTIATE_TEST_SUITE_P(
    WtTest,
    WtTest,
    Values(TestParams({.codecProtocol = CodecProtocol::HTTP_2})),
    paramsToTestName);

class H3CoroWtSessionTest : public Test {
 protected:
  detail::EgressSourcePtr makeEgressSource() {
    detail::EgressSourcePtr egressSource{new detail::EgressSource(&evb_)};
    egressSource_ = egressSource.get();

    HTTPMessage headers;
    headers.setURL("/");
    egressSource->validateHeadersAndSkip(headers);
    return egressSource;
  }

  void startSession(std::unique_ptr<WebTransportHandler> handler) {
    session_ = detail::H3CoroWtSession::make(&evb_,
                                             socketDriver_.getSocket(),
                                             std::move(handler),
                                             WtStreamManager::WtConfig{},
                                             /*connectStreamId=*/0);

    session_->start(
        session_, HTTPSourceHolder(&ingressSource_), makeEgressSource());
  }

  void TearDown() override {
    if (handlerCtx_) {
      handlerCtx_->wtSession.reset();
    }
    session_.reset();
    socketDriver_.closeImpl({});
  }

  folly::EventBase evb_;
  quic::MockQuicSocketDriver socketDriver_{
      &evb_,
      nullptr,
      nullptr,
      quic::MockQuicSocketDriver::TransportEnum::CLIENT,
      "h3"};
  NiceMock<MockHTTPSource> ingressSource_;
  HTTPStreamSource* egressSource_{nullptr};
  std::shared_ptr<DummyWtHandler::Ctx> handlerCtx_;
  detail::H3CoroWtSession::Ptr session_;
};

CO_TEST_F(H3CoroWtSessionTest, SendsDatagramOverQuic) {
  constexpr std::string_view kPayload = "datagram";
  EXPECT_CALL(ingressSource_, readBodyEvent(_))
      .WillOnce(Return(folly::coro::makeTask<HTTPBodyEvent>(
          HTTPBodyEvent{nullptr, /*inEOM=*/true})));

  auto handler = DummyWtHandler::make();
  handlerCtx_ = handler->ctx;
  startSession(std::move(handler));

  CO_ASSERT_NE(handlerCtx_->wtSession, nullptr);
  auto result =
      handlerCtx_->wtSession->sendDatagram(folly::IOBuf::copyBuffer(kPayload));
  EXPECT_TRUE(result.hasValue());

  CO_ASSERT_EQ(socketDriver_.outDatagrams_.size(), 1);
  auto datagram = socketDriver_.outDatagrams_.front().move();
  folly::io::Cursor cursor(datagram.get());
  auto quarterStreamId = quic::follyutils::decodeQuicInteger(cursor);
  CO_ASSERT_TRUE(quarterStreamId.has_value());
  EXPECT_EQ(quarterStreamId->first, 0);
  EXPECT_EQ(cursor.readFixedString(cursor.totalLength()), kPayload);

  evb_.loop();
  auto egressResult =
      co_await folly::coro::co_awaitTry(egressSource_->readBodyEvent());
  CO_ASSERT_TRUE(egressResult.hasValue());
  EXPECT_TRUE(egressResult->eom);
  auto* body = asBodyEv(*egressResult);
  CO_ASSERT_TRUE(body && !body->empty());

  egressSource_ = nullptr;
}

/**
 * H3 variant of WtTest. Stream data flows through native QUIC streams (via
 * MockQuicSocketDriver), while the CONNECT stream carries only session-level
 * capsules (MaxData, MaxStreams, Drain, Close).
 */
class H3WtTest : public HttpWtUpstreamSessionTest {
 public:
  void SetUp() override {
    initSelfCodec_ = enableH3CodecWtSettings;
    HTTPCoroSessionTest::setUp();
    constexpr auto kTimeout = std::chrono::milliseconds(50);
    session_->setConnectionReadTimeout(kTimeout);
    session_->setWriteTimeout(kTimeout);
    loopN(2);

    muxTp = CHECK_NOTNULL(muxTransport_.get());
  }

  void TearDown() override {
    if (wt) {
      wt->closeSession(folly::none);
    }
    HTTPCoroSessionTest::TearDown();
  }

 protected:
  void deliverRespHeaders(HTTPCodec::StreamID id,
                          const HTTPMessage& resp,
                          bool eom = true) {
    multiCodec_->addCodec(id);
    serverCodec_->generateHeader(writeBuf_, id, resp, eom);
    flushQPACKEncoder();
    transport_->addReadEvent(id, writeBuf_.move(), isHQ() && eom);
  }

  folly::coro::Task<void> establishWtSession() {
    HTTPMessage msg;
    msg.setMethod(HTTPMethod::CONNECT);
    msg.setUpgradeProtocol("webtransport");

    auto handler = DummyWtHandler::make();
    wtHandlerCtx = handler->ctx;

    auto reservation = session_->reserveRequest();
    auto fut =
        co_withExecutor(&evb_,
                        session_->sendWtReq(
                            std::move(*reservation), msg, std::move(handler)))
            .start();
    co_await rescheduleN(1);

    // eom=false: CONNECT stream stays open for capsules
    connectStreamId_ = 0;
    deliverRespHeaders(connectStreamId_, makeResponse(200), /*eom=*/false);
    auto res = co_await co_awaitTry(std::move(fut));
    XCHECK(res.hasValue());
    wt = std::move(res->wt);
    EXPECT_EQ(session_->numOutgoingStreams(), 1);
  }

  /**
   * Create a peer-initiated WT stream by writing the WT stream preface on a
   * new QUIC stream. This triggers HQ(Uni|Bidi)StreamDispatcher ->
   * dispatch(Uni|Bidi)WTStream -> H3WtSession::acquireIngressStream.
   */
  quic::StreamId openPeerUniStream(quic::StreamId connectStreamId) {
    auto id = muxTp->nextUnidirectionalStreamId_;
    muxTp->nextUnidirectionalStreamId_ += 4;
    folly::IOBufQueue prefaceBuf{folly::IOBufQueue::cacheChainLength()};
    hq::writeWTStreamPreface(
        prefaceBuf, hq::WebTransportStreamType::UNI, connectStreamId);
    muxTp->socketDriver_.addReadEvent(id, prefaceBuf.move());
    return id;
  }

  void deliverStreamData(quic::StreamId id,
                         std::unique_ptr<folly::IOBuf> data,
                         bool eof = false) {
    muxTp->socketDriver_.addReadEvent(id, std::move(data), eof);
  }

  // bounded, so a regression fails the test rather than hanging the suite
  folly::coro::Task<void> waitFor(folly::Function<bool()> pred,
                                  size_t maxLoops = 100) {
    for (size_t i = 0; i < maxLoops && !pred(); i++) {
      co_await rescheduleN(1);
    }
    EXPECT_TRUE(pred());
  }

  std::unique_ptr<folly::IOBuf> makeQuicDatagram(quic::StreamId connectStreamId,
                                                 std::string_view payload) {
    folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
    folly::io::QueueAppender appender(&queue, /*growth=*/8);
    auto res = quic::encodeQuicInteger(
        connectStreamId / 4, [&appender](auto val) { appender.writeBE(val); });
    XCHECK(res);
    queue.append(folly::IOBuf::copyBuffer(payload));
    return queue.move();
  }

  void deliverQuicDatagram(quic::StreamId connectStreamId,
                           std::string_view payload) {
    muxTp->socketDriver_.addDatagram(
        makeQuicDatagram(connectStreamId, payload));
    muxTp->socketDriver_.addDatagramsAvailableReadEvent();
  }

  std::shared_ptr<WebTransport> wt;
  std::shared_ptr<const DummyWtHandler::Ctx> wtHandlerCtx;
  TestCoroMultiplexTransport* muxTp{nullptr};
  quic::StreamId connectStreamId_{0};
};

CO_TEST_P_X(H3WtTest, RoutesPeerStreams) {
  co_await establishWtSession();
  XCHECK_NE(wt, nullptr);

  auto invalidBidiId = muxTp->nextBidirectionalStreamId_;
  muxTp->nextBidirectionalStreamId_ += 4;
  folly::IOBufQueue prefaceBuf{folly::IOBufQueue::cacheChainLength()};
  hq::writeWTStreamPreface(prefaceBuf,
                           hq::WebTransportStreamType::BIDI,
                           /*wtSessionId=*/9999);
  muxTp->socketDriver_.addReadEvent(invalidBidiId, prefaceBuf.move());
  co_await rescheduleN(2);

  EXPECT_TRUE(wtHandlerCtx->peerStreams.empty());
  EXPECT_EQ(muxTp->socketDriver_.streams_[invalidBidiId].writeState,
            quic::MockQuicSocketDriver::StateEnum::ERROR);

  constexpr uint16_t kBufLen = 100;
  auto peerUniId = openPeerUniStream(connectStreamId_);
  deliverStreamData(peerUniId, makeBuf(kBufLen), /*eof=*/false);

  co_await waitFor([&] { return !wtHandlerCtx->peerStreams.empty(); });
  XCHECK(!wtHandlerCtx->peerStreams.empty());

  // ingress only => writeHandle == nullptr
  auto handle = wtHandlerCtx->peerStreams.at(0);
  EXPECT_FALSE(handle.writeHandle);
  EXPECT_TRUE(handle.readHandle);

  auto read = wt->readStreamData(handle.readHandle->getID());
  EXPECT_TRUE(read->isReady());
  EXPECT_EQ(read->value().fin, false);
  EXPECT_EQ(read->value().data->computeChainDataLength(), kBufLen);
}

CO_TEST_P_X(H3WtTest, Datagrams) {
  co_await establishWtSession();
  XCHECK_NE(wt, nullptr);
  auto& socketDriver = muxTp->socketDriver_;

  // tx datagram => quic datagram prefixed with the quarter stream id
  wt->sendDatagram(folly::IOBuf::copyBuffer("datagram1"));
  co_await rescheduleN(2);

  XCHECK_EQ(socketDriver.outDatagrams_.size(), 1u);
  auto egressDatagram = socketDriver.outDatagrams_.front().move();
  folly::io::Cursor cursor(egressDatagram.get());
  auto quarterStreamId = quic::follyutils::decodeQuicInteger(cursor);
  XCHECK(quarterStreamId.has_value());
  EXPECT_EQ(quarterStreamId->first, connectStreamId_ / 4);
  EXPECT_EQ(cursor.readFixedString(cursor.totalLength()), "datagram1");

  // rx quic datagram destined for this connect stream
  deliverQuicDatagram(connectStreamId_, "datagram2");
  co_await waitFor([&] { return !wtHandlerCtx->dgrams.empty(); });
  XCHECK_EQ(wtHandlerCtx->dgrams.size(), 1u);
  EXPECT_EQ(wtHandlerCtx->dgrams.back()->toString(), "datagram2");
}

CO_TEST_P_X(H3WtTest, PeerStreamBeforeConnectResponse) {
  constexpr quic::StreamId kConnectStreamId = 0;
  HTTPMessage msg;
  msg.setMethod(HTTPMethod::CONNECT);
  msg.setUpgradeProtocol("webtransport");

  auto handler = DummyWtHandler::make();
  auto handlerCtx = handler->ctx;
  auto reservation = session_->reserveRequest();
  auto fut =
      co_withExecutor(
          &evb_,
          session_->sendWtReq(std::move(*reservation), msg, std::move(handler)))
          .start();
  co_await rescheduleN(1);

  auto uniId = openPeerUniStream(kConnectStreamId);
  co_await rescheduleN(2);
  EXPECT_TRUE(handlerCtx->peerStreams.empty());
  EXPECT_FALSE(muxTp->socketDriver_.streams_[uniId].error.has_value());

  deliverRespHeaders(kConnectStreamId, makeResponse(200), /*eom=*/false);
  auto res = co_await co_awaitTry(std::move(fut));
  CO_ASSERT_TRUE(res.hasValue());
  CO_ASSERT_NE(res->wt, nullptr);
  co_await rescheduleN(2);

  EXPECT_EQ(handlerCtx->peerStreams.size(), 1);
  EXPECT_FALSE(muxTp->socketDriver_.streams_[uniId].error.has_value());
  res->wt->closeSession(folly::none);
}

CO_TEST_P_X(H3WtTest, ShortPeerStreamBeforeConnectResponse) {
  // like mvfst, discard a finished stream with no read or peek callback
  muxTp->socketDriver_.setReapClosedStreams(true);
  constexpr quic::StreamId kConnectStreamId = 0;
  HTTPMessage msg;
  msg.setMethod(HTTPMethod::CONNECT);
  msg.setUpgradeProtocol("webtransport");

  auto handler = DummyWtHandler::make();
  auto handlerCtx = handler->ctx;
  auto reservation = session_->reserveRequest();
  auto fut =
      co_withExecutor(
          &evb_,
          session_->sendWtReq(std::move(*reservation), msg, std::move(handler)))
          .start();
  co_await rescheduleN(1);

  // the whole stream, data and FIN, arrives while it is held
  auto uniId = openPeerUniStream(kConnectStreamId);
  deliverStreamData(uniId, folly::IOBuf::copyBuffer("hello"), /*eof=*/true);
  co_await rescheduleN(2);

  deliverRespHeaders(kConnectStreamId, makeResponse(200), /*eom=*/false);
  auto res = co_await co_awaitTry(std::move(fut));
  CO_ASSERT_TRUE(res.hasValue());
  CO_ASSERT_NE(res->wt, nullptr);
  co_await rescheduleN(2);
  CO_ASSERT_EQ(handlerCtx->peerStreams.size(), 1u);

  auto read = res->wt->readStreamData(uniId);
  CO_ASSERT_TRUE(read.hasValue());
  auto data = co_await co_awaitTry(folly::coro::timeout(
      std::move(read.value()), std::chrono::milliseconds(500)));
  CO_ASSERT_TRUE(data.hasValue());
  EXPECT_EQ(data->data->toString(), "hello");
  EXPECT_TRUE(data->fin);
  res->wt->closeSession(folly::none);
}

CO_TEST_P_X(H3WtTest, HeldStreamResetBeforeConnectResponse) {
  muxTp->socketDriver_.setReapClosedStreams(true);
  constexpr quic::StreamId kConnectStreamId = 0;
  HTTPMessage msg;
  msg.setMethod(HTTPMethod::CONNECT);
  msg.setUpgradeProtocol("webtransport");

  // a reset frees the read handle once delivered, so read as soon as it is
  auto handler = std::make_unique<NiceMock<MockWebTransportHandler>>();
  folly::Optional<folly::SemiFuture<WebTransport::StreamData>> read;
  EXPECT_CALL(*handler, onNewUniStream(_))
      .WillOnce([&](WebTransport::StreamReadHandle* readHandle) {
        read.emplace(readHandle->readStreamData());
      });
  auto reservation = session_->reserveRequest();
  auto fut =
      co_withExecutor(
          &evb_,
          session_->sendWtReq(std::move(*reservation), msg, std::move(handler)))
          .start();
  co_await rescheduleN(1);

  auto uniId = openPeerUniStream(kConnectStreamId);
  co_await rescheduleN(2);
  muxTp->socketDriver_.addReadError(uniId, WebTransport::toHTTPErrorCode(77));
  co_await rescheduleN(2);

  deliverRespHeaders(kConnectStreamId, makeResponse(200), /*eom=*/false);
  auto res = co_await co_awaitTry(std::move(fut));
  CO_ASSERT_TRUE(res.hasValue());
  CO_ASSERT_NE(res->wt, nullptr);
  co_await rescheduleN(2);

  CO_ASSERT_TRUE(read.has_value());
  auto data = co_await co_awaitTry(
      folly::coro::timeout(std::move(*read), std::chrono::milliseconds(500)));
  EXPECT_TRUE(data.hasException());
  res->wt->closeSession(folly::none);
}

CO_TEST_P_X(H3WtTest, DatagramBeforeConnectResponse) {
  constexpr quic::StreamId kConnectStreamId = 0;
  HTTPMessage msg;
  msg.setMethod(HTTPMethod::CONNECT);
  msg.setUpgradeProtocol("webtransport");

  auto handler = DummyWtHandler::make();
  auto handlerCtx = handler->ctx;
  auto reservation = session_->reserveRequest();
  auto fut =
      co_withExecutor(
          &evb_,
          session_->sendWtReq(std::move(*reservation), msg, std::move(handler)))
          .start();
  co_await rescheduleN(1);

  deliverQuicDatagram(kConnectStreamId, "datagram1");
  co_await rescheduleN(2);
  EXPECT_TRUE(handlerCtx->dgrams.empty());

  deliverRespHeaders(kConnectStreamId, makeResponse(200), /*eom=*/false);
  auto res = co_await co_awaitTry(std::move(fut));
  CO_ASSERT_TRUE(res.hasValue());
  CO_ASSERT_NE(res->wt, nullptr);
  co_await rescheduleN(2);

  EXPECT_EQ(handlerCtx->dgrams.size(), 1u);
  CO_ASSERT_TRUE(!handlerCtx->dgrams.empty());
  EXPECT_EQ(handlerCtx->dgrams.back()->toString(), "datagram1");
  res->wt->closeSession(folly::none);
}

INSTANTIATE_TEST_SUITE_P(
    H3WtTest,
    H3WtTest,
    Values(TestParams({.codecProtocol = CodecProtocol::HQ})),
    paramsToTestName);

class HttpStreamTransport : public Test {
 public:
  void SetUp() override {
    // precondition is that headers have already been produced via
    // egress source
    detail::EgressSourcePtr egress{new detail::EgressSource(&evb)};
    egressSource = egress.get();
    HTTPMessage msg;
    msg.setURL("/");
    egress->validateHeadersAndSkip(msg);

    transport = detail::makeHttpSourceTransport(
        &evb, std::move(egress), &ingressSource);
    EXPECT_EQ(transport->getEventBase(), &evb);
    EXPECT_EQ(transport->getTransport(), nullptr);
    EXPECT_EQ(transport->getPeerCertificate(), nullptr);
  }

  void TearDown() override {
    transport->close();
    // consume if egress source not done
    while (!egressSource->sourceComplete()) {
      folly::coro::blockingWait(egressSource->readBodyEvent(), &evb);
    }
  }

  folly::EventBase evb;
  MockHTTPSource ingressSource;
  HTTPStreamSource* egressSource{nullptr};
  std::unique_ptr<folly::coro::TransportIf> transport;
  folly::CancellationSource cs_;
};

CO_TEST_F(HttpStreamTransport, ReadAfterIngressComplete) {
  // after reading a terminal ingress event (i.e. eom or exception), a
  // subsequent ::read should yield an exception
  EXPECT_CALL(ingressSource, readBodyEvent(_))
      .WillOnce(Return(folly::coro::makeTask<HTTPBodyEvent>(
          HTTPBodyEvent{nullptr, /*inEOM=*/true})));

  folly::IOBufQueue ingress{folly::IOBufQueue::cacheChainLength()};
  auto readRes = co_await co_awaitTry(
      transport->read(ingress,
                      /*minReadSize=*/0,
                      /*newAllocationSize=*/0,
                      /*timeout=*/std::chrono::milliseconds(0)));
  EXPECT_TRUE(readRes.hasValue() && *readRes == 0);

  // reading again after ingress complete should yield an error
  readRes = co_await co_awaitTry(
      transport->read(ingress,
                      /*minReadSize=*/0,
                      /*newAllocationSize=*/0,
                      /*timeout=*/std::chrono::milliseconds(0)));

  auto* ex = readRes.tryGetExceptionObject<folly::AsyncSocketException>();
  EXPECT_TRUE(ex &&
              ex->getType() == folly::AsyncSocketException::INTERNAL_ERROR);
}

CO_TEST_F(HttpStreamTransport, SimpleRead) {
  // simple read 100 bytes
  EXPECT_CALL(ingressSource, readBodyEvent(_))
      .WillOnce(Return(folly::coro::makeTask<HTTPBodyEvent>(
          HTTPBodyEvent{makeBuf(100), /*inEOM=*/false})));

  folly::IOBufQueue ingress{folly::IOBufQueue::cacheChainLength()};
  auto readRes =
      co_await transport->read(ingress,
                               /*minReadSize=*/0,
                               /*newAllocationSize=*/0,
                               /*timeout=*/std::chrono::milliseconds(0));
  EXPECT_EQ(readRes, 100);
}

CO_TEST_F(HttpStreamTransport, DeferredEom) {
  /**
   * Yield Padding event (test non-body event get ignored), subsequently yield
   * 100-byte bytes + eom in the same HTTPBodyEvent. Ensure that the EOM will be
   * deferred until the next Transport::read.
   */
  EXPECT_CALL(ingressSource, readBodyEvent(_))
      .WillOnce(Return(folly::coro::makeTask<HTTPBodyEvent>(
          HTTPBodyEvent{HTTPBodyEvent::Padding{}, 10})))
      .WillOnce(Return(folly::coro::makeTask<HTTPBodyEvent>(
          HTTPBodyEvent{makeBuf(100), /*inEOM=*/true})));

  folly::IOBufQueue ingress{folly::IOBufQueue::cacheChainLength()};
  auto readRes =
      co_await transport->read(ingress,
                               /*minReadSize=*/0,
                               /*newAllocationSize=*/0,
                               /*timeout=*/std::chrono::milliseconds(0));
  // first read yields 100 bytes
  EXPECT_EQ(readRes, 100);
  // second read yields 0 immediately (doesn't invoke ::readBodyEvent)
  readRes = co_await transport->read(ingress,
                                     /*minReadSize=*/0,
                                     /*newAllocationSize=*/0,
                                     /*timeout=*/std::chrono::milliseconds(0));
  EXPECT_EQ(readRes, 0);

  // read after deferred EOM is an error
  auto readResTry = co_await co_awaitTry(
      transport->read(ingress,
                      /*minReadSize=*/0,
                      /*newAllocationSize=*/0,
                      /*timeout=*/std::chrono::milliseconds(0)));
  auto* ex = readResTry.tryGetExceptionObject<folly::AsyncSocketException>();
  EXPECT_TRUE(ex &&
              ex->getType() == folly::AsyncSocketException::INTERNAL_ERROR);

  // close transport
  transport->close();
  // reading the egressSource should yield an empty body + eom
  auto egressBodyEvent = co_await egressSource->readBodyEvent();
  EXPECT_TRUE(egressBodyEvent.eventType == HTTPBodyEvent::BODY &&
              egressBodyEvent.event.body.empty());
  EXPECT_TRUE(egressBodyEvent.eom);
}

CO_TEST_F(HttpStreamTransport, WriteBackpressure) {
  /**
   * Writes 64KB to the egressSource, then write again to ensure that the
   * ::write coroutine suspends. After consuming from the egress source, it
   * should unblock the write accordingly.
   */
  folly::IOBufQueue egress{folly::IOBufQueue::cacheChainLength()};
  // first 64KB writes resolves immediately
  egress.append(makeBuf(65'535));
  folly::coro::Transport::WriteInfo info{};
  auto writeRes = co_withExecutor(&evb,
                                  transport->write(egress,
                                                   std::chrono::milliseconds(0),
                                                   folly::WriteFlags::NONE,
                                                   &info))
                      .startInlineUnsafe();
  EXPECT_TRUE(writeRes.isReady());
  EXPECT_EQ(info.bytesWritten, 65'535);

  // next 100 byte write will suspend
  egress.append(makeBuf(100));
  writeRes =
      co_withExecutor(&evb, transport->write(egress)).startInlineUnsafe();
  EXPECT_FALSE(writeRes.isReady());

  // consuming from egressSource will unblock write
  co_await egressSource->readBodyEvent();
  // one evb loop
  evb.loopOnce();
  EXPECT_TRUE(writeRes.isReady());

  // close transport
  EXPECT_CALL(ingressSource, stopReading(_));
  transport->closeWithReset();
  // next egress read should yield an error
  auto ev = co_await co_awaitTry(egressSource->readBodyEvent());
  EXPECT_TRUE(ev.hasException());
}

CO_TEST_F(HttpStreamTransport, WriteTimeout) {
  /**
   * Writes 64KB to the egressSource, then write again to ensure that the
   * ::write coroutine suspends. Ensure the timeout works as expected and the
   * error yielded is AsyncSocketException::TIMED_OUT
   */
  folly::IOBufQueue egress{folly::IOBufQueue::cacheChainLength()};
  // first 64KB writes resolves immediately
  egress.append(makeBuf(65'535));
  folly::coro::Transport::WriteInfo info{};
  auto writeRes = co_withExecutor(&evb,
                                  transport->write(egress,
                                                   std::chrono::milliseconds(0),
                                                   folly::WriteFlags::NONE,
                                                   &info))
                      .startInlineUnsafe();
  EXPECT_TRUE(writeRes.isReady());
  EXPECT_EQ(info.bytesWritten, 65'535);

  // next 100 byte write will suspend
  egress.append(makeBuf(100));
  writeRes = co_withExecutor(
                 &evb, transport->write(egress, std::chrono::milliseconds(10)))
                 .startInlineUnsafe();
  EXPECT_FALSE(writeRes.isReady());

  auto res = blockingWait(co_awaitTry(std::move(writeRes)), &evb);
  auto* ex = res.tryGetExceptionObject<folly::AsyncSocketException>();
  EXPECT_TRUE(ex && ex->getType() == folly::AsyncSocketException::TIMED_OUT);
  co_return;
}

CO_TEST_F(HttpStreamTransport, WriteCancellation) {
  // write cancellation should omit write & yield the appropriate exception
  cs_.requestCancellation();
  folly::IOBufQueue egress{folly::IOBufQueue::cacheChainLength()};
  egress.append(makeBuf(65'535));
  auto writeRes = co_await co_awaitTry(
      co_withCancellation(cs_.getToken(), transport->write(egress)));
  auto* ex = writeRes.tryGetExceptionObject<folly::AsyncSocketException>();
  EXPECT_TRUE(ex && ex->getType() == folly::AsyncSocketException::CANCELED);
}

CO_TEST_F(HttpStreamTransport, EgressConsumerStopReading) {
  // consumer of the egress source invoking ::stopReading should trigger a write
  // error on next write
  egressSource->stopReading(HTTPErrorCode::NO_ERROR);

  folly::IOBufQueue egress{folly::IOBufQueue::cacheChainLength()};
  egress.append(makeBuf(65'535));
  auto writeRes = co_await co_awaitTry(transport->write(egress));
  auto* ex = writeRes.tryGetExceptionObject<folly::AsyncSocketException>();
  EXPECT_TRUE(ex && ex->getType() == folly::AsyncSocketException::NOT_OPEN);
}

CO_TEST_F(HttpStreamTransport, Fatals) {
  std::array<uint8_t, 10> ingress{};
  EXPECT_DEATH(
      co_await transport->read(folly::MutableByteRange{ingress},
                               /*timeout=*/std::chrono::milliseconds(0)),
      "not implemented");
  EXPECT_DEATH(co_await transport->write(
                   folly::ByteRange{ingress.data(), ingress.size()}),
               "not implemented");
  EXPECT_DEATH(transport->getPeerAddress(), "not implemented");
  EXPECT_DEATH(transport->getLocalAddress(), "not implemented");
}

} // namespace proxygen::coro::test
