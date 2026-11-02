/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <proxygen/lib/http/experimental/llhttp/HTTP1xLlCodec.h>

#include <folly/base64.h>
#include <folly/portability/GTest.h>
#include <list>
#include <proxygen/lib/http/HTTPHeaderSize.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/codec/test/MockHTTPCodec.h>
#include <proxygen/lib/http/codec/test/TestUtils.h>

#include <string_view>

namespace {
// Not in the common header list, so addressed by name.
constexpr std::string_view kExpect{"Expect"};
} // namespace

using namespace proxygen;
using namespace std;
using namespace testing;

class HTTP1xLlCodecCallback : public HTTPCodec::Callback {
 public:
  HTTP1xLlCodecCallback() = default;

  void onMessageBegin(HTTPCodec::StreamID /*stream*/,
                      HTTPMessage* /*msg*/) override {
  }
  void onPushMessageBegin(HTTPCodec::StreamID /*stream*/,
                          HTTPCodec::StreamID /*assocStream*/,
                          HTTPMessage* /*msg*/) override {
  }
  void onHeadersComplete(HTTPCodec::StreamID /*stream*/,
                         std::unique_ptr<HTTPMessage> msg) override {
    headersComplete++;
    headerSize = msg->getIngressHeaderSize();
    msg_ = std::move(msg);
  }
  void onBody(HTTPCodec::StreamID /*stream*/,
              std::unique_ptr<folly::IOBuf> chain,
              uint16_t /*padding*/) override {
    bodyLen += chain->computeChainDataLength();
  }
  void onChunkHeader(HTTPCodec::StreamID /*stream*/,
                     size_t /*length*/) override {
  }
  void onChunkComplete(HTTPCodec::StreamID /*stream*/) override {
  }
  void onTrailersComplete(HTTPCodec::StreamID /*stream*/,
                          std::unique_ptr<HTTPHeaders> trailers) override {
    trailersComplete++;
    trailers_ = std::move(trailers);
  }
  void onMessageComplete(HTTPCodec::StreamID /*stream*/,
                         bool /*upgrade*/) override {
    ++messageComplete;
  }
  void onError(HTTPCodec::StreamID /*stream*/,
               const HTTPException& error,
               bool /*newTxn*/) override {
    ++errors;
    LOG(ERROR) << "parse error " << error;
  }

  uint32_t headersComplete{0};
  uint32_t trailersComplete{0};
  uint32_t messageComplete{0};
  uint32_t errors{0};
  uint32_t bodyLen{0};
  HTTPHeaderSize headerSize;
  std::unique_ptr<HTTPMessage> msg_;
  std::unique_ptr<HTTPHeaders> trailers_;
};

unique_ptr<folly::IOBuf> getSimpleRequestData() {
  string req("GET /yeah HTTP/1.1\r\nHost: www.facebook.com\r\n\r\n");
  return folly::IOBuf::copyBuffer(req);
}

unique_ptr<folly::IOBuf> getSimpleRequestDataQueue() {
  string req1("GET /yeah HTTP/1.1\r\nHost: www.facebook.com\r\n\r\n");
  auto buf1 = folly::IOBuf::copyBuffer(req1);
  string req2("GET /yeah2 HTTP/1.1\r\nHost: www.facebook.com\r\n\r\n");
  auto buf2 = folly::IOBuf::copyBuffer(req2);
  buf1->appendToChain(std::move(buf2));
  return buf1;
}

TEST(HTTP1xLlCodecTest, TestSimpleHeaders) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto buffer = getSimpleRequestData();
  codec.onIngress(*buffer);
  // Call it twice so we catch that we reset compressed size
  codec.onIngress(*buffer);
  EXPECT_EQ(callbacks.headersComplete, 2);
  // "/yeah" + "Host: www.facebook.com"
  EXPECT_EQ(callbacks.headerSize.uncompressed, 5 + 4 + 2 + 16);
  EXPECT_EQ(callbacks.headerSize.compressed, callbacks.headerSize.uncompressed);
}

TEST(HTTP1xLlCodecTest, TestBareLfRequiresLenientLineEndings) {
  const std::string req = "GET /yeah HTTP/1.1\nHost: www.facebook.com\n\n";

  HTTP1xLlCodec strict(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback strictCallbacks;
  strict.setCallback(&strictCallbacks);
  strict.onIngress(*folly::IOBuf::copyBuffer(req));
  EXPECT_EQ(strictCallbacks.headersComplete, 0);

  HTTP1xLlCodec lenient(
      TransportDirection::DOWNSTREAM,
      /*force1_1=*/false,
      facebook::detail::LlhttpParserOptions{.lenientOptionalLfAfterCr = true,
                                            .lenientOptionalCrBeforeLf = true});
  HTTP1xLlCodecCallback lenientCallbacks;
  lenient.setCallback(&lenientCallbacks);
  lenient.onIngress(*folly::IOBuf::copyBuffer(req));
  EXPECT_EQ(lenientCallbacks.headersComplete, 1);
}

TEST(HTTP1xLlCodecTest, TestSimpleHeadersQueue) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto buffer = getSimpleRequestDataQueue();
  codec.onIngress(*buffer);
  EXPECT_EQ(callbacks.headersComplete, 2);
  EXPECT_EQ(callbacks.headerSize.compressed, callbacks.headerSize.uncompressed);
}

TEST(HTTP1xLlCodecTest, TestSimpleHeadersQueueWithPause) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto buffer = getSimpleRequestDataQueue();

  EXPECT_CALL(callbacks, onMessageComplete(_, _))
      .Times(1)
      .WillOnce(InvokeWithoutArgs([&] { codec.setParserPaused(true); }));
  size_t bytesParsed = codec.onIngress(*buffer);
  codec.setParserPaused(false);
  buffer->trimStart(bytesParsed);
  EXPECT_CALL(callbacks, onMessageComplete(_, _)).Times(1);
  codec.onIngress(*buffer);
}

TEST(HTTP1xLlCodecTest, TestPauseInBodyDefersMessageComplete) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  const std::string post =
      "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 2\r\n\r\nhi";
  const std::string get = "GET /2 HTTP/1.1\r\nHost: a\r\n\r\n";
  auto buffer = folly::IOBuf::copyBuffer(post + get);

  EXPECT_CALL(callbacks, onBody(1, _, _)).WillOnce(InvokeWithoutArgs([&] {
    codec.setParserPaused(true);
  }));
  EXPECT_CALL(callbacks, onMessageComplete(_, _)).Times(0);
  EXPECT_EQ(codec.onIngress(*buffer), post.size());
  Mock::VerifyAndClearExpectations(&callbacks);

  EXPECT_CALL(callbacks, onMessageComplete(1, false));
  EXPECT_CALL(callbacks, onMessageComplete(2, false));
  codec.setParserPaused(false);
  buffer->trimStart(post.size());
  EXPECT_EQ(codec.onIngress(*buffer), get.size());
}

TEST(HTTP1xLlCodecTest, PipelinedRequestAfterPausedUpgradeRequest) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  const std::string upgradeReq =
      "GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: "
      "h2c\r\n\r\n";
  const std::string get = "GET /2 HTTP/1.1\r\nHost: a\r\n\r\n";
  auto buffer = folly::IOBuf::copyBuffer(upgradeReq + get);

  EXPECT_CALL(callbacks, onMessageComplete(1, false))
      .WillOnce(InvokeWithoutArgs([&] { codec.setParserPaused(true); }));
  EXPECT_EQ(codec.onIngress(*buffer), upgradeReq.size());
  Mock::VerifyAndClearExpectations(&callbacks);

  EXPECT_CALL(callbacks, onHeadersComplete(2, _));
  EXPECT_CALL(callbacks, onMessageComplete(2, false));
  codec.setParserPaused(false);
  buffer->trimStart(upgradeReq.size());
  EXPECT_EQ(codec.onIngress(*buffer), get.size());
}

TEST(HTTP1xLlCodecTest, EOFAfterUpgradeRequestIsNotAnError) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto buffer = folly::IOBuf::copyBuffer(
      "GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: "
      "h2c\r\n\r\n");
  codec.onIngress(*buffer);
  codec.onIngressEOF();
  EXPECT_EQ(callbacks.messageComplete, 1);
  EXPECT_EQ(callbacks.errors, 0);
}

// A URL that fails strict parsing is logged, and the query is where a URL
// carries its credentials, so the log has to stop short of it.
TEST(HTTP1xLlCodecTest, DISABLED_TestInvalidURLLogOmitsQuery) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM,
                      /*force1_1=*/true);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  CapturingLogSink logs;

  // A fragment ahead of the query passes http_parser but fails ParseURL.
  auto buffer = folly::IOBuf::copyBuffer(
      "GET /v1/noise#frag?auth_token=TEST_ONLY_NOT_A_TOKEN HTTP/1.1\r\n\r\n");
  codec.onIngress(*buffer);

  EXPECT_EQ(callbacks.streamErrors, 1);
  EXPECT_THAT(logs.messages, Contains(HasSubstr("Invalid URL: /v1/noise")));
  EXPECT_THAT(logs.messages, Each(Not(HasSubstr("TEST_ONLY"))));
}

TEST(HTTP1xLlCodecTest, TestUnderscoreAllowedInHost) {
  // RFC 3986 allows _ in a reg-name
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM,
                      /*force1_1=*/true);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto buffer = folly::IOBuf::copyBuffer(
      "CONNECT face_book.facebook.com:443 HTTP/1.1\r\n\r\n");

  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onHeadersComplete(1, _));
  EXPECT_CALL(callbacks, onMessageComplete(1, _));
  codec.onIngress(*buffer);
}

unique_ptr<folly::IOBuf> getChunkedRequest1st() {
  string req("GET /aha HTTP/1.1\r\n");
  return folly::IOBuf::copyBuffer(req);
}

unique_ptr<folly::IOBuf> getChunkedRequest2nd() {
  string req("Host: m.facebook.com\r\nAccept-Encoding: meflate\r\n\r\n");
  return folly::IOBuf::copyBuffer(req);
}

TEST(HTTP1xLlCodecTest, TestChunkedHeaders) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  // test a sequence of requests to make sure we're resetting the size counter
  for (int i = 0; i < 3; i++) {
    callbacks.headersComplete = 0;
    auto buffer1 = getChunkedRequest1st();
    codec.onIngress(*buffer1);
    EXPECT_EQ(callbacks.headersComplete, 0);

    auto buffer2 = getChunkedRequest2nd();
    codec.onIngress(*buffer2);
    EXPECT_EQ(callbacks.headersComplete, 1);
    // "/aha" + "Host: m.facebook.com" + "Accept-Encoding: meflate"
    EXPECT_EQ(callbacks.headerSize.uncompressed, 4 + 4 + 2 + 14 + 15 + 2 + 7);
    EXPECT_EQ(callbacks.headerSize.uncompressed,
              callbacks.headerSize.compressed);
  }
}

TEST(HTTP1xLlCodecTest, TestRejectsIdenticalContentLengthHeaders) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);

  // llhttp rejects a repeated Content-Length even when the values match
  auto reqBuf = folly::IOBuf::copyBuffer(
      "POST /www.facebook.com HTTP/1.1\r\nHost: www.facebook.com\r\n"
      "Content-Length: 5\r\nContent-Length: 5\r\n\r\n");
  codec.onIngress(*reqBuf);

  EXPECT_EQ(callbacks.messageBegin, 1);
  EXPECT_EQ(callbacks.headersComplete, 0);
}

// Pipelining puts the tail of one message's body and the next message's header
// block in the same read. Only the header block counts against the cap.
TEST(HTTP1xLlCodecTest, TestHeaderBlockSizeCapIgnoresPipelinedBody) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);

  const size_t kBodyLen = 100 * 1024;
  codec.onIngress(*folly::IOBuf::copyBuffer(folly::to<std::string>(
      "POST /upload HTTP/1.1\r\nHost: www.facebook.com\r\nContent-Length: ",
      kBodyLen,
      "\r\n\r\n")));
  ASSERT_EQ(callbacks.headersComplete, 1);

  codec.onIngress(*folly::IOBuf::copyBuffer(
      std::string(kBodyLen, 'x') +
      "GET /next HTTP/1.1\r\nHost: www.facebook.com\r\n\r\n"));

  EXPECT_EQ(callbacks.streamErrors, 0);
  EXPECT_EQ(callbacks.headersComplete, 2);
  EXPECT_EQ(callbacks.messageComplete, 2);
}

// A header block under the cap is unaffected.
TEST(HTTP1xLlCodecTest, TestHeaderBlockUnderSizeCap) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);

  std::string req = "GET /url HTTP/1.1\r\nHost: www.facebook.com\r\n";
  const std::string filler(1024, 'x');
  for (int i = 0; i < 60; i++) {
    req += folly::to<std::string>("X-Pad-", i, ": ", filler, "\r\n");
  }
  req += "\r\n";

  auto reqBuf = folly::IOBuf::copyBuffer(req);
  codec.onIngress(*reqBuf);

  EXPECT_EQ(callbacks.streamErrors, 0);
  EXPECT_EQ(callbacks.headersComplete, 1);
}

TEST(HTTP1xLlCodecTest, TestReasonPhraseCountsTowardHeaderCap) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);

  codec.onIngress(*folly::IOBuf::copyBuffer(
      "HTTP/1.1 200 " + std::string(81 * 1024, 'x') + "\r\n\r\n"));

  EXPECT_EQ(callbacks.messageBegin, 1);
  EXPECT_EQ(callbacks.headersComplete, 0);
}

TEST(HTTP1xLlCodecTest, TestHeaderSeparatorsCountTowardHeaderCap) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);

  std::string req = "GET / HTTP/1.1\r\n";
  for (int i = 0; i < 28000; i++) {
    req += "a:\r\n";
  }
  codec.onIngress(*folly::IOBuf::copyBuffer(req + "\r\n"));

  EXPECT_EQ(callbacks.messageBegin, 1);
  EXPECT_EQ(callbacks.headersComplete, 0);
}

TEST(HTTP1xLlCodecTest, TestTrailersHaveTheirOwnHeaderBudget) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);

  const std::string filler(60 * 1024, 'x');
  const std::string trailerFiller(30 * 1024, 'y');
  codec.onIngress(*folly::IOBuf::copyBuffer(
      "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nX-Pad: " + filler +
      "\r\n\r\n0\r\nX-Trailer: " + trailerFiller + "\r\n\r\n"));

  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(callbacks.trailers, 1);
  EXPECT_EQ(callbacks.messageComplete, 1);
}

TEST(HTTP1xLlCodecTest, TestNonHttpProtocolIsRejected) {
  HTTP1xLlCodec downstream(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback downstreamCallbacks;
  downstream.setCallback(&downstreamCallbacks);
  downstream.onIngress(*folly::IOBuf::copyBuffer("GET / RTSP/1.0\r\n\r\n"));
  EXPECT_EQ(downstreamCallbacks.headersComplete, 0);

  HTTP1xLlCodec upstream(TransportDirection::UPSTREAM);
  FakeHTTPCodecCallback upstreamCallbacks;
  upstream.setCallback(&upstreamCallbacks);
  upstream.onIngress(*folly::IOBuf::copyBuffer("ICE/1.0 200 OK\r\n\r\n"));
  EXPECT_EQ(upstreamCallbacks.headersComplete, 0);
}

TEST(HTTP1xLlCodecTest, TestProtocolSplitAcrossReads) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);

  codec.onIngress(*folly::IOBuf::copyBuffer("GET / HT"));
  codec.onIngress(*folly::IOBuf::copyBuffer("TP/1.1\r\n\r\n"));

  EXPECT_EQ(callbacks.headersComplete, 1);
}

TEST(HTTP1xLlCodecTest, TestContentLengthAboveInt64MaxIsRejected) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);

  codec.onIngress(*folly::IOBuf::copyBuffer(
      "POST / HTTP/1.1\r\nContent-Length: 9223372036854775808\r\n\r\n"));

  EXPECT_EQ(callbacks.messageBegin, 1);
  EXPECT_EQ(callbacks.headersComplete, 0);
}

TEST(HTTP1xLlCodecTest, TestTransferEncodingChunkedThenTabIsRejected) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);

  codec.onIngress(*folly::IOBuf::copyBuffer(
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\t\r\n\r\n"
      "3\r\nabc\r\n0\r\n\r\n"));

  EXPECT_EQ(callbacks.headersComplete, 0);
  EXPECT_EQ(callbacks.bodyLength, 0);
}

TEST(HTTP1xLlCodecTest, TestTransferEncodingChunkedThenSpace) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);

  codec.onIngress(*folly::IOBuf::copyBuffer(
      "POST / HTTP/1.1\r\nTransfer-Encoding: chunked \r\n\r\n"
      "3\r\nabc\r\n0\r\n\r\n"));

  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(callbacks.bodyLength, 3);
  EXPECT_EQ(callbacks.messageComplete, 1);
}

TEST(HTTP1xLlCodecTest, TestCorrectTransferEncodingHeader) {
  HTTP1xLlCodec downstream(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  downstream.setCallback(&callbacks);
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());

  // Generate a POST request with folded
  auto reqBuf = folly::IOBuf::copyBuffer(
      "POST /www.facebook.com HTTP/1.1\r\nHost: www.facebook.com\r\n"
      "Transfer-Encoding: chunked\r\n\r\n");
  downstream.onIngress(*reqBuf);

  // Check that the request fails before the codec finishes parsing the headers
  EXPECT_EQ(callbacks.streamErrors, 0);
  EXPECT_EQ(callbacks.messageBegin, 1);
  EXPECT_EQ(callbacks.headersComplete, 1);
}

TEST(HTTP1xLlCodecTest, TestChainedBody) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);

  folly::IOBufQueue bodyQueue;
  ON_CALL(callbacks, onBody(1, _, _))
      .WillByDefault(
          Invoke([&bodyQueue](HTTPCodec::StreamID,
                              std::shared_ptr<folly::IOBuf> buf,
                              uint16_t) { bodyQueue.append(buf->clone()); }));

  folly::IOBufQueue reqQueue;
  reqQueue.append(folly::IOBuf::copyBuffer(
      "POST /test.php HTTP/1.1\r\nHost: www.test.com\r\n"
      "Content-Length: 10\r\n\r\nabcde"));
  reqQueue.append(folly::IOBuf::copyBuffer("fghij"));
  codec.onIngress(*reqQueue.front());
  EXPECT_TRUE(folly::IOBufEqualTo()(*bodyQueue.front(),
                                    *folly::IOBuf::copyBuffer("abcdefghij")));
}

TEST(HTTP1xLlCodecTest, TestIgnoreUpstreamUpgrade) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());

  auto reqBuf = folly::IOBuf::copyBuffer(
      "HTTP/1.1 200 OK\r\n"
      "Connection: close\r\n"
      "Upgrade: h2,h2c\r\n"
      "\r\n"
      "<!DOCTYPE html>");
  codec.onIngress(*reqBuf);

  EXPECT_EQ(callbacks.streamErrors, 0);
  EXPECT_EQ(callbacks.messageBegin, 1);
  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(callbacks.bodyLength, 15);
}

TEST(HTTP1xLlCodecTest, TestHeaderValueWhiteSpaces) {
  HTTP1xLlCodecCallback callbacks;
  auto buf = folly::IOBuf::copyBuffer(
      "GET /status.php HTTP/1.1\r\nHost: www.facebook.com  \r\n"
      "X-FB-HEADER: yay \r\n\r\n");
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  codec.setCallback(&callbacks);

  codec.onIngress(*buf);
  const auto& headers = callbacks.msg_->getHeaders();
  EXPECT_EQ(headers.getSingleOrEmpty(HTTP_HEADER_HOST), "www.facebook.com");
  EXPECT_EQ(headers.getSingleOrEmpty("X-FB-HEADER"), "yay");
}

TEST(HTTP1xLlCodecTest, UpgradeAccepted) {
  EXPECT_TRUE(serverAcceptedUpgrade("  Websocket,,,", ",,,websocket  "));
  EXPECT_FALSE(serverAcceptedUpgrade("h2c, ,websocket", " ,not-advertised"));
  EXPECT_FALSE(serverAcceptedUpgrade(",test1,test2,", ",,Test3,Test4"));
  EXPECT_FALSE(serverAcceptedUpgrade("websocket", ""));
  EXPECT_FALSE(serverAcceptedUpgrade("", "websocket"));
  // A match on a non-first client protocol must still be accepted.
  EXPECT_TRUE(serverAcceptedUpgrade("test1,websocket", "websocket"));
}

TEST(HTTP1xLlCodecTest, ExtraCRLF) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto requests = folly::to<std::string>(
      "\r"
      "GET /echo HTTP/1.1\r\n\r\n",
      "\n"
      "GET /echo HTTP/1.1\r\n\r\n",
      "\r\n"
      "GET /echo HTTP/1.1\r\n\r\n",
      "\r\n\r\n"
      "GET /echo HTTP/1.1\r\n\r\n",
      "\r\n\r\n");
  EXPECT_CALL(callbacks, onMessageBegin(_, _)).Times(4);
  EXPECT_CALL(callbacks, onHeadersComplete(_, _)).Times(4);
  EXPECT_CALL(callbacks, onMessageComplete(_, _)).Times(4);
  codec.onIngress(*folly::IOBuf::wrapBuffer(requests.data(), requests.size()));
}

TEST(HTTP1xLlCodecTest, AbsoluteURLNoPath) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM, true);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto buffer = folly::IOBuf::copyBuffer(
      string("GET http://www.foo.com HTTP/1.0\r\n\r\n"));
  codec.onIngress(*buffer);
  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(callbacks.msg_->getPathAsStringPiece(), string("/"));
}
