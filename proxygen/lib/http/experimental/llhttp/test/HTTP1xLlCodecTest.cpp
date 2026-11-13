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

  EXPECT_CALL(callbacks, onHeadersComplete(_, _)).Times(0);
  codec.setParserPaused(false);
  buffer->trimStart(upgradeReq.size());
  EXPECT_EQ(codec.onIngress(*buffer), 0);
  EXPECT_FALSE(codec.isReusable());
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

TEST(HTTP1xLlCodecTest, TestPauseInBodyAtEndOfInputCompletesMessage) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  const std::string post =
      "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 2\r\n\r\nhi";

  EXPECT_CALL(callbacks, onBody(1, _, _)).WillOnce(InvokeWithoutArgs([&] {
    codec.setParserPaused(true);
  }));
  EXPECT_CALL(callbacks, onMessageComplete(1, false));
  EXPECT_EQ(codec.onIngress(*folly::IOBuf::copyBuffer(post)), post.size());
  EXPECT_TRUE(codec.isParserPaused());
}

TEST(HTTP1xLlCodecTest, TestPausedUpgradeRequestCompletesOnEOF) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  const std::string req =
      "GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: "
      "h2c\r\n\r\n";

  EXPECT_CALL(callbacks, onHeadersComplete(1, _))
      .WillOnce(InvokeWithoutArgs([&] { codec.setParserPaused(true); }));
  EXPECT_CALL(callbacks, onMessageComplete(_, _)).Times(0);
  EXPECT_EQ(codec.onIngress(*folly::IOBuf::copyBuffer(req)), req.size());
  Mock::VerifyAndClearExpectations(&callbacks);

  EXPECT_CALL(callbacks, onMessageComplete(1, false));
  EXPECT_CALL(callbacks, onError(_, _, _)).Times(0);
  codec.setParserPaused(false);
  codec.onIngressEOF();
}

TEST(HTTP1xLlCodecTest, TestPausedUpgradeRequestCompletesOnEOFWhilePaused) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  const std::string req =
      "GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: "
      "h2c\r\n\r\n";

  EXPECT_CALL(callbacks, onHeadersComplete(1, _))
      .WillOnce(InvokeWithoutArgs([&] { codec.setParserPaused(true); }));
  EXPECT_CALL(callbacks, onMessageComplete(_, _)).Times(0);
  EXPECT_EQ(codec.onIngress(*folly::IOBuf::copyBuffer(req)), req.size());
  Mock::VerifyAndClearExpectations(&callbacks);

  EXPECT_CALL(callbacks, onMessageComplete(1, false));
  EXPECT_CALL(callbacks, onError(_, _, _)).Times(0);
  codec.onIngressEOF();
  EXPECT_TRUE(codec.isParserPaused());
}

TEST(HTTP1xLlCodecTest, TestPausedUpgradeRequestCompletesOnNextIngress) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  const std::string req =
      "GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: "
      "h2c\r\n\r\n";
  const std::string get = "GET /2 HTTP/1.1\r\nHost: a\r\n\r\n";

  EXPECT_CALL(callbacks, onHeadersComplete(1, _))
      .WillOnce(InvokeWithoutArgs([&] { codec.setParserPaused(true); }));
  EXPECT_CALL(callbacks, onMessageComplete(_, _)).Times(0);
  EXPECT_EQ(codec.onIngress(*folly::IOBuf::copyBuffer(req)), req.size());
  Mock::VerifyAndClearExpectations(&callbacks);

  EXPECT_CALL(callbacks, onMessageComplete(1, false));
  EXPECT_CALL(callbacks, onHeadersComplete(_, _)).Times(0);
  codec.setParserPaused(false);
  EXPECT_EQ(codec.onIngress(*folly::IOBuf::copyBuffer(get)), 0);
  EXPECT_FALSE(codec.isReusable());
}

// HTTPCoroSession pauses on a WebSocket request and resumes just before writing
// the response
TEST(HTTP1xLlCodecTest, TestPausedWebsocketRequestCompletesWithFirstFrame) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  const std::string req =
      "GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: "
      "websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";

  EXPECT_CALL(callbacks, onHeadersComplete(1, _))
      .WillOnce(InvokeWithoutArgs([&] { codec.setParserPaused(true); }));
  EXPECT_CALL(callbacks, onMessageComplete(_, _)).Times(0);
  EXPECT_EQ(codec.onIngress(*folly::IOBuf::copyBuffer(req)), req.size());

  codec.setParserPaused(false);
  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  resp.setStatusCode(101);
  resp.setEgressWebsocketUpgrade();
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  codec.generateHeader(writeBuf, 1, resp);
  Mock::VerifyAndClearExpectations(&callbacks);

  EXPECT_CALL(callbacks, onMessageComplete(1, true));
  EXPECT_CALL(callbacks, onBody(1, _, _));
  EXPECT_EQ(codec.onIngress(*folly::IOBuf::copyBuffer("frame")), 5);
}

TEST(HTTP1xLlCodecTest, TestPauseInPartialBodyAtEndOfInput) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  const std::string head =
      "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 4\r\n\r\nhi";

  EXPECT_CALL(callbacks, onBody(1, _, _)).WillOnce(InvokeWithoutArgs([&] {
    codec.setParserPaused(true);
  }));
  EXPECT_CALL(callbacks, onMessageComplete(_, _)).Times(0);
  EXPECT_EQ(codec.onIngress(*folly::IOBuf::copyBuffer(head)), head.size());
  Mock::VerifyAndClearExpectations(&callbacks);

  EXPECT_CALL(callbacks, onBody(1, _, _));
  EXPECT_CALL(callbacks, onMessageComplete(1, false));
  codec.setParserPaused(false);
  EXPECT_EQ(codec.onIngress(*folly::IOBuf::copyBuffer("!!")), 2);
}

TEST(HTTP1xLlCodecTest, Test09Req) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  // llhttp needs the byte after the request line to know it is HTTP/0.9
  codec.onIngress(*folly::IOBuf::copyBuffer(string("GET /yeah\r\n")));
  EXPECT_EQ(callbacks.headersComplete, 0);
  codec.onIngress(*folly::IOBuf::copyBuffer(string("\r\n")));
  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(callbacks.messageComplete, 1);
  EXPECT_EQ(callbacks.errors, 0);
  // "/yeah"
  EXPECT_EQ(callbacks.headerSize.uncompressed, 5);
  EXPECT_EQ(callbacks.headerSize.compressed, callbacks.headerSize.uncompressed);
  ASSERT_NE(callbacks.msg_, nullptr);
  EXPECT_EQ(callbacks.msg_->getHTTPVersion(), HTTPMessage::kHTTPVersion09);
}

TEST(HTTP1xLlCodecTest, Test09ReqSplitAtLineEnding) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  codec.onIngress(*folly::IOBuf::copyBuffer(string("GET /yeah\r")));
  EXPECT_EQ(callbacks.headersComplete, 0);
  codec.onIngress(*folly::IOBuf::copyBuffer(string("\n")));
  EXPECT_EQ(callbacks.headersComplete, 0);
  codec.onIngress(*folly::IOBuf::copyBuffer(string("\r\n")));
  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(callbacks.messageComplete, 1);
  EXPECT_EQ(callbacks.errors, 0);
}

TEST(HTTP1xLlCodecTest, Test09ReqUrlFoldedAcrossReadsRejected) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  codec.onIngress(*folly::IOBuf::copyBuffer(string("GET /a\n")));
  codec.onIngress(
      *folly::IOBuf::copyBuffer(string("\tb HTTP/1.1\r\nHost: a\r\n\r\n")));
  EXPECT_EQ(callbacks.headersComplete, 0);
  EXPECT_EQ(callbacks.errors, 1);
}

// HTTP/0.9 closes the connection after the response, so nothing may follow
TEST(HTTP1xLlCodecTest, Test09ReqFollowedByMoreDataRejected) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto buffer = folly::IOBuf::copyBuffer(string("GET /yeah\r\nGET /two\r\n"));
  codec.onIngress(*buffer);
  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(callbacks.messageComplete, 1);
  EXPECT_EQ(callbacks.errors, 1);
  ASSERT_NE(callbacks.msg_, nullptr);
  EXPECT_EQ(callbacks.msg_->getURL(), "/yeah");
  EXPECT_EQ(callbacks.msg_->getHTTPVersion(), HTTPMessage::kHTTPVersion09);
  EXPECT_EQ(callbacks.msg_->getHeaders().size(), 0);
}

TEST(HTTP1xLlCodecTest, TestVersionlessConnectRejected) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  EXPECT_CALL(callbacks, onHeadersComplete(_, _)).Times(0);
  EXPECT_CALL(callbacks, onMessageComplete(_, _)).Times(0);
  EXPECT_CALL(callbacks, onError(_, _, _));
  codec.onIngress(*folly::IOBuf::copyBuffer("CONNECT host:443\r\n\r\n"));
}

TEST(HTTP1xLlCodecTest, TestVersionlessPostRejected) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  codec.onIngress(*folly::IOBuf::copyBuffer("POST /a\r\n\r\n"));
  EXPECT_EQ(callbacks.headersComplete, 0);
  EXPECT_EQ(callbacks.errors, 1);
}

TEST(HTTP1xLlCodecTest, TestNonHttp1xVersionRejected) {
  for (const char* version : {"HTTP/0.9", "HTTP/2.0"}) {
    HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
    HTTP1xLlCodecCallback callbacks;
    codec.setCallback(&callbacks);
    codec.onIngress(*folly::IOBuf::copyBuffer(fmt::format(
        "POST /a {}\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello", version)));
    EXPECT_EQ(callbacks.headersComplete, 0) << version;
    EXPECT_EQ(callbacks.bodyLen, 0) << version;
    EXPECT_EQ(callbacks.errors, 1) << version;
  }
}

TEST(HTTP1xLlCodecTest, TestNonHttp1xResponseVersionRejected) {
  for (const char* version : {"HTTP/0.9", "HTTP/2.0"}) {
    HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
    HTTP1xLlCodecCallback callbacks;
    codec.setCallback(&callbacks);
    folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
    codec.generateHeader(writeBuf, codec.createStream(), getGetRequest(), true);
    codec.onIngress(*folly::IOBuf::copyBuffer(
        fmt::format("{} 200 OK\r\nContent-Length: 0\r\n\r\n", version)));
    EXPECT_EQ(callbacks.headersComplete, 0) << version;
    EXPECT_EQ(callbacks.errors, 1) << version;
  }
}

TEST(HTTP1xLlCodecTest, Test09ReqPauseInHeadersCompletesMessage) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  const std::string req = "GET /a\r\n";
  const std::string trailingCrlf = "\r\n";
  auto buffer = folly::IOBuf::copyBuffer(req + trailingCrlf);

  EXPECT_CALL(callbacks, onHeadersComplete(1, _))
      .WillOnce(InvokeWithoutArgs([&] { codec.setParserPaused(true); }));
  EXPECT_CALL(callbacks, onMessageComplete(1, false));
  EXPECT_EQ(codec.onIngress(*buffer), req.size());
  Mock::VerifyAndClearExpectations(&callbacks);

  EXPECT_CALL(callbacks, onMessageComplete(_, _)).Times(0);
  EXPECT_CALL(callbacks, onError(_, _, _)).Times(0);
  codec.setParserPaused(false);
  buffer->trimStart(req.size());
  EXPECT_EQ(codec.onIngress(*buffer), trailingCrlf.size());
}

TEST(HTTP1xLlCodecTest, Test09Resp) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  HTTP1xLlCodecCallback callbacks;
  HTTPMessage req;
  auto id = codec.createStream();
  req.setHTTPVersion(0, 9);
  req.setMethod(HTTPMethod::GET);
  req.setURL("/");
  codec.setCallback(&callbacks);
  folly::IOBufQueue buf;
  codec.generateHeader(buf, id, req, true);
  auto buffer =
      folly::IOBuf::copyBuffer(string("iamtheverymodelofamodernmajorgeneral"));
  codec.onIngress(*buffer);
  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(callbacks.bodyLen, buffer->computeChainDataLength());
  EXPECT_EQ(callbacks.messageComplete, 0);
  codec.onIngressEOF();
  EXPECT_EQ(callbacks.messageComplete, 1);
}

TEST(HTTP1xLlCodecTest, TestResponseSplit) {
  HTTP1xLlCodec downcodec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodec upcodec(TransportDirection::UPSTREAM);

  HTTP1xLlCodecCallback callbacks;
  downcodec.setCallback(&callbacks);
  auto id = upcodec.createStream();

  HTTPMessage req;
  req.setHTTPVersion(1, 1);
  req.setMethod("GET");
  req.setURL("/foo");
  req.getHeaders().set("InjectHeader", "Value\r\nx: new");
  folly::IOBufQueue buf;
  upcodec.generateHeader(buf, id, req);
  downcodec.onIngress(*buf.front());
  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(callbacks.messageComplete, 1);
  // make sure that the header is not sent on the wire.
  // Otherwise it would be parsed as two different headers with HTTP <= 1.1
  EXPECT_EQ(callbacks.msg_->getHeaders().getSingleOrEmpty("InjectHeader"), "");
  EXPECT_EQ(callbacks.msg_->getHeaders().getSingleOrEmpty("x"), "");
}

TEST(HTTP1xLlCodecTest, TestO9NoVersion) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  HTTPMessage req;
  auto id = codec.createStream();
  req.setHTTPVersion(0, 9);
  req.setMethod(HTTPMethod::GET);
  req.setURL("/yeah");
  folly::IOBufQueue buf;
  codec.generateHeader(buf, id, req, true);
  EXPECT_TRUE(folly::IOBufEqualTo()(
      *buf.front(), *folly::IOBuf::copyBuffer("GET /yeah\r\n")));
}

TEST(HTTP1xLlCodecTest, TestKeepalive09_10) {
  HTTP1xLlCodec codec1(TransportDirection::DOWNSTREAM, true);
  HTTP1xLlCodecCallback callbacks1;
  codec1.setCallback(&callbacks1);
  auto buffer = folly::IOBuf::copyBuffer(string("GET /yeah\r\n\r\n"));
  codec1.onIngress(*buffer);
  EXPECT_EQ(callbacks1.headersComplete, 1);
  EXPECT_EQ(callbacks1.messageComplete, 1);
  ASSERT_NE(callbacks1.msg_, nullptr);
  EXPECT_EQ(callbacks1.msg_->getHTTPVersion(), HTTPMessage::kHTTPVersion09);
  HTTPCodec::StreamID id = 1;
  HTTPMessage resp;
  resp.setHTTPVersion(0, 9);
  resp.setStatusCode(200);
  resp.getHeaders().set(HTTP_HEADER_CONTENT_LENGTH, "0");
  resp.getHeaders().set(HTTP_HEADER_DATE, "");
  folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
  codec1.generateHeader(buf, id, resp, true);
  // Even if forced to HTTP/1.1, HTTP/0.9 has no headers
  EXPECT_EQ(buf.chainLength(), 0);
  EXPECT_FALSE(codec1.isReusable());

  HTTP1xLlCodec codec2(TransportDirection::DOWNSTREAM, true);
  HTTP1xLlCodecCallback callbacks2;
  codec2.setCallback(&callbacks2);
  buffer = folly::IOBuf::copyBuffer(string("GET /yeah HTTP/1.0\r\n\r\n"));
  codec2.onIngress(*buffer);
  EXPECT_EQ(callbacks2.headersComplete, 1);
  EXPECT_EQ(callbacks2.messageComplete, 1);
  ASSERT_NE(callbacks2.msg_, nullptr);
  EXPECT_EQ(callbacks2.msg_->getHTTPVersion(), HTTPMessage::kHTTPVersion10);
  resp.setHTTPVersion(1, 0);
  codec2.generateHeader(buf, id, resp, true);

  EXPECT_TRUE(folly::IOBufEqualTo()(
      *buf.front(),
      *folly::IOBuf::copyBuffer("HTTP/1.1 200 \r\n"
                                "Date: \r\n"
                                "Connection: close\r\n"
                                "Content-Length: 0\r\n\r\n")));
  EXPECT_FALSE(codec2.isReusable());
  buf.reset();

  HTTP1xLlCodec codec3(TransportDirection::DOWNSTREAM, true);
  HTTP1xLlCodecCallback callbacks3;
  codec3.setCallback(&callbacks3);
  buffer =
      folly::IOBuf::copyBuffer(string("GET /yeah HTTP/1.0\r\n"
                                      "Connection: keep-alive\r\n\r\n"));
  codec3.onIngress(*buffer);
  EXPECT_EQ(callbacks3.headersComplete, 1);
  EXPECT_EQ(callbacks3.messageComplete, 1);
  ASSERT_NE(callbacks3.msg_, nullptr);
  EXPECT_EQ(callbacks3.msg_->getHTTPVersion(), HTTPMessage::kHTTPVersion10);
  codec3.generateHeader(buf, id, resp, true);
  EXPECT_TRUE(folly::IOBufEqualTo()(
      *buf.front(),
      *folly::IOBuf::copyBuffer("HTTP/1.1 200 \r\n"
                                "Date: \r\n"
                                "Connection: keep-alive\r\n"
                                "Content-Length: 0\r\n\r\n")));
  EXPECT_TRUE(codec3.isReusable());
  buf.reset();

  HTTP1xLlCodec codec4(TransportDirection::DOWNSTREAM, true);
  HTTP1xLlCodecCallback callbacks4;
  codec4.setCallback(&callbacks4);
  buffer = folly::IOBuf::copyBuffer(string("GET /yeah HTTP/1.0\r\n\r\n"));
  codec4.onIngress(*buffer);
  EXPECT_EQ(callbacks4.headersComplete, 1);
  EXPECT_EQ(callbacks4.messageComplete, 1);
  ASSERT_NE(callbacks4.msg_, nullptr);
  EXPECT_EQ(callbacks4.msg_->getHTTPVersion(), HTTPMessage::kHTTPVersion10);
  resp.getHeaders().set(HTTP_HEADER_TRANSFER_ENCODING, "chunked");
  resp.getHeaders().remove(HTTP_HEADER_CONTENT_LENGTH);
  resp.setIsChunked(true);
  codec4.generateHeader(buf, id, resp, true);
  EXPECT_TRUE(folly::IOBufEqualTo()(
      *buf.front(),
      *folly::IOBuf::copyBuffer("HTTP/1.1 200 \r\n"
                                "Date: \r\n"
                                "Connection: close\r\n\r\n")));
  EXPECT_FALSE(codec4.isReusable());
  buf.reset();
}

TEST(HTTP1xLlCodecTest, TestBadHeaders) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto buffer = folly::IOBuf::copyBuffer(
      string("GET /yeah HTTP/1.1\nUser-Agent: Mozilla/5.0 Version/4.0 "
             "\x10i\xC7n tho\xA1iSafari/534.30]"));
  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onError(1, _, _))
      .WillOnce(Invoke(
          [&](HTTPCodec::StreamID, std::shared_ptr<HTTPException> error, bool) {
            EXPECT_EQ(error->getHttpStatusCode(), 400);
            EXPECT_EQ(error->getProxygenError(), kErrorParseHeader);
          }));
  codec.onIngress(*buffer);
}

TEST(HTTP1xLlCodecTest, TestHighAsciiUA) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM,
                      /*force1_1=*/true);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto buffer = folly::IOBuf::copyBuffer(
      string("GET /yeah HTTP/1.1\r\nUser-Agent: ꪶ𝛸ꫂ_𝐹𝛩𝑅𝐶𝛯_𝑉2\r\n\r\n"));
  // RFC 9110 allows obs-text (0x80-0xFF) in field values
  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onHeadersComplete(1, _))
      .WillOnce(Invoke([](HTTPCodec::StreamID, std::shared_ptr<HTTPMessage> m) {
        EXPECT_EQ(m->getHeaders().getSingleOrEmpty(HTTP_HEADER_USER_AGENT),
                  "ꪶ𝛸ꫂ_𝐹𝛩𝑅𝐶𝛯_𝑉2");
      }));
  EXPECT_CALL(callbacks, onMessageComplete(1, _));
  codec.onIngress(*buffer);
}

TEST(HTTP1xLlCodecTest, TestBadURL) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM,
                      /*force1_1=*/true);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto buffer = folly::IOBuf::copyBuffer(string("GET /Ñoño HTTP/1.1\r\n\r\n"));
  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onError(1, _, _))
      .WillOnce(Invoke(
          [&](HTTPCodec::StreamID, std::shared_ptr<HTTPException> error, bool) {
            EXPECT_EQ(error->getHttpStatusCode(), 400);
            EXPECT_EQ(error->getProxygenError(), kErrorParseHeader);
            EXPECT_THAT(error->what(), HasSubstr("[Context]=invalid-url"));
          }));
  codec.onIngress(*buffer);
}

// A URL that fails strict parsing is logged, and the query is where a URL
// carries its credentials, so the log has to stop short of it.
TEST(HTTP1xLlCodecTest, TestInvalidURLLogOmitsQuery) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM,
                      /*force1_1=*/true);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  CapturingLogSink logs;

  // A fragment ahead of the query passes llhttp but fails ParseURL.
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

TEST(HTTP1xLlCodecTest, TestConnectRequiresAuthorityForm) {
  for (const auto& [target, valid] :
       std::vector<std::pair<std::string, bool>>{{"a.com:443", true},
                                                 {"[::1]:443", true},
                                                 {"a.com", false},
                                                 {"/x", false},
                                                 {"*", false},
                                                 {"a.com:443/x", false},
                                                 {"a.com:443?q", false},
                                                 {"http://a.com:443", false},
                                                 {":443", false},
                                                 {"a.com:0", false},
                                                 {"x[::1]y:443", false},
                                                 {"a[b]:443", false}}) {
    SCOPED_TRACE(target);
    HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
    HTTP1xLlCodecCallback callbacks;
    codec.setCallback(&callbacks);
    codec.onIngress(*folly::IOBuf::copyBuffer(
        "CONNECT " + target + " HTTP/1.1\r\nHost: a.com:443\r\n\r\n"));
    EXPECT_EQ(callbacks.headersComplete, valid ? 1 : 0);
    EXPECT_EQ(callbacks.errors, valid ? 0 : 1);
  }
}

TEST(HTTP1xLlCodecTest, TestHeadRequestChunkedResponse) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto txnID = codec.createStream();

  // Generate a HEAD request
  auto reqBuf = folly::IOBuf::copyBuffer(
      "HEAD /www.facebook.com HTTP/1.1\r\nHost: www.facebook.com\r\n\r\n");
  codec.onIngress(*reqBuf);
  EXPECT_EQ(callbacks.headersComplete, 1);

  // Generate chunked response with no body
  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  resp.setStatusCode(200);
  resp.setIsChunked(true);
  resp.getHeaders().set(HTTP_HEADER_TRANSFER_ENCODING, "chunked");
  folly::IOBufQueue respBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateHeader(respBuf, txnID, resp, true);
  auto respStr = respBuf.move()->moveToFbString();
  EXPECT_TRUE(respStr.find("0\r\n") == string::npos);
}

TEST(HTTP1xLlCodecTest, TestGetRequestChunkedResponse) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto txnID = codec.createStream();

  // Generate a GET request
  auto reqBuf = folly::IOBuf::copyBuffer(
      "GET /www.facebook.com HTTP/1.1\r\nHost: www.facebook.com\r\n\r\n");
  codec.onIngress(*reqBuf);
  EXPECT_EQ(callbacks.headersComplete, 1);

  // Generate chunked response with body
  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  resp.setStatusCode(200);
  resp.setIsChunked(true);
  resp.getHeaders().set(HTTP_HEADER_TRANSFER_ENCODING, "chunked");
  folly::IOBufQueue respBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateHeader(respBuf, txnID, resp, false);

  auto headerFromBuf = respBuf.split(respBuf.chainLength());

  string resp1("Hello");
  auto body1 = folly::IOBuf::copyBuffer(resp1);

  string resp2;
  auto body2 = folly::IOBuf::copyBuffer(resp2);

  codec.generateBody(
      respBuf, txnID, std::move(body1), HTTPCodec::NoPadding, false);

  auto bodyFromBuf = respBuf.split(respBuf.chainLength());
  ASSERT_EQ("5\r\nHello\r\n", bodyFromBuf->moveToFbString());

  codec.generateBody(
      respBuf, txnID, std::move(body2), HTTPCodec::NoPadding, true);

  bodyFromBuf = respBuf.split(respBuf.chainLength());
  ASSERT_EQ("0\r\n\r\n", bodyFromBuf->moveToFbString());
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

TEST(HTTP1xLlCodecTest, TestChunkedUpstream) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);

  auto txnID = codec.createStream();

  HTTPMessage msg;
  msg.setHTTPVersion(1, 1);
  msg.setURL("https://www.facebook.com/");
  msg.getHeaders().set(HTTP_HEADER_HOST, "www.facebook.com");
  msg.getHeaders().set(HTTP_HEADER_TRANSFER_ENCODING, "chunked");
  msg.setIsChunked(true);

  HTTPHeaderSize size;

  folly::IOBufQueue buf(folly::IOBufQueue::cacheChainLength());
  codec.generateHeader(buf, txnID, msg, false, &size);
  auto headerFromBuf = buf.split(buf.chainLength());

  string req1("Hello");
  auto body1 = folly::IOBuf::copyBuffer(req1);

  string req2("World");
  auto body2 = folly::IOBuf::copyBuffer(req2);

  codec.generateBody(buf, txnID, std::move(body1), HTTPCodec::NoPadding, false);

  auto bodyFromBuf = buf.split(buf.chainLength());
  ASSERT_EQ("5\r\nHello\r\n", bodyFromBuf->moveToFbString());

  codec.generateBody(buf, txnID, std::move(body2), HTTPCodec::NoPadding, true);
  LOG(WARNING) << "len chain" << buf.chainLength();

  auto eomFromBuf = buf.split(buf.chainLength());
  ASSERT_EQ("5\r\nWorld\r\n0\r\n\r\n", eomFromBuf->moveToFbString());
}

TEST(HTTP1xLlCodecTest, TestBadPost100) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());

  InSequence enforceOrder;
  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onHeadersComplete(1, _))
      .WillOnce(InvokeWithoutArgs([&] {
        HTTPMessage cont;
        cont.setStatusCode(100);
        cont.setStatusMessage("Continue");
        codec.generateHeader(writeBuf, 1, cont);
      }));

  EXPECT_CALL(callbacks, onBody(1, _, _));
  EXPECT_CALL(callbacks, onMessageComplete(1, _));
  EXPECT_CALL(callbacks, onMessageBegin(2, _)).WillOnce(InvokeWithoutArgs([&] {
    // simulate HTTPSession's aversion to pipelining
    codec.setParserPaused(true);

    // Trigger the response to the POST
    HTTPMessage resp;
    resp.setStatusCode(200);
    resp.setStatusMessage("OK");
    codec.generateHeader(writeBuf, 1, resp);
    codec.generateEOM(writeBuf, 1);
    codec.setParserPaused(false);
  }));
  EXPECT_CALL(callbacks, onError(2, _, _)).WillOnce(InvokeWithoutArgs([&] {
    HTTPMessage resp;
    resp.setStatusCode(400);
    resp.setStatusMessage("Bad");
    codec.generateHeader(writeBuf, 2, resp);
    codec.generateEOM(writeBuf, 2);
  }));
  // Generate a POST request with a bad content-length
  auto reqBuf = folly::IOBuf::copyBuffer(
      "POST /www.facebook.com HTTP/1.1\r\nHost: www.facebook.com\r\n"
      "Expect: 100-Continue\r\nContent-Length: 5\r\n\r\nabcdefghij");
  codec.onIngress(*reqBuf);
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

  EXPECT_EQ(callbacks.streamErrors, 1);
  EXPECT_EQ(callbacks.messageBegin, 1);
  EXPECT_EQ(callbacks.headersComplete, 0);
}

TEST(HTTP1xLlCodecTest, TestInvalidContentLengthHeaders) {
  // llhttp rejects a folded value as a header token error rather than letting
  // the codec see the joined "1 2"
  const std::vector<std::pair<std::string, std::string>> cases = {
      {"5\r\nContent-Length: 6", "unexpected-content-length"},
      {"1 2", "invalid-content-length"},
      {"1\r\n 2", "invalid-header-token"},
      {"9223372036854775808", "content-length-exceeds-max-limit"},
  };
  for (const auto& [contentLength, context] : cases) {
    SCOPED_TRACE(contentLength);
    HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
    FakeHTTPCodecCallback callbacks;
    codec.setCallback(&callbacks);
    auto reqBuf = folly::IOBuf::copyBuffer(folly::to<std::string>(
        "POST /www.facebook.com HTTP/1.1\r\nHost: www.facebook.com\r\n"
        "Content-Length: ",
        contentLength,
        "\r\n\r\n"));
    codec.onIngress(*reqBuf);

    EXPECT_EQ(callbacks.streamErrors, 1);
    EXPECT_EQ(callbacks.messageBegin, 1);
    EXPECT_EQ(callbacks.headersComplete, 0);
    EXPECT_EQ(callbacks.lastParseError->getHttpStatusCode(), 400);
    EXPECT_THAT(callbacks.lastParseError->what(),
                HasSubstr("[Context]=" + context));
    EXPECT_EQ(callbacks.lastParseError->getContext(), context);
  }
}

TEST(HTTP1xLlCodecTest, TestNonHttpProtocolErrorContext) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  codec.onIngress(*folly::IOBuf::copyBuffer("GET / RTSP/1.0\r\n\r\n"));

  EXPECT_EQ(callbacks.streamErrors, 1);
  ASSERT_NE(callbacks.lastParseError, nullptr);
  EXPECT_EQ(callbacks.lastParseError->getHttpStatusCode(), 400);
  EXPECT_EQ(callbacks.lastParseError->getContext(), "invalid-protocol");
}

TEST(HTTP1xLlCodecTest, TestUrlUserInfoIsRejected) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  codec.onIngress(*folly::IOBuf::copyBuffer(
      "GET http://a.com:80@evil.com/ HTTP/1.1\r\nHost: a.com\r\n\r\n"));

  EXPECT_EQ(callbacks.headersComplete, 0);
  ASSERT_NE(callbacks.lastParseError, nullptr);
  EXPECT_EQ(callbacks.lastParseError->getHttpStatusCode(), 400);
  EXPECT_EQ(callbacks.lastParseError->getContext(), "url-userinfo");
}

TEST(HTTP1xLlCodecTest, TestAtSignInPathIsNotUserInfo) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  codec.onIngress(*folly::IOBuf::copyBuffer(
      "GET http://a.com/x@y HTTP/1.1\r\nHost: a.com\r\n\r\n"));

  EXPECT_EQ(callbacks.errors, 0);
  ASSERT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(callbacks.msg_->getHeaders().getSingleOrEmpty(HTTP_HEADER_HOST),
            "a.com");
}

// The 80KiB cap is enforced inside http_parser and has no llhttp equivalent,
// so the codec counts URL, header name and value bytes as llhttp hands them
// over.
TEST(HTTP1xLlCodecTest, TestHeaderBlockOverSizeCap) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);

  codec.onIngress(*folly::IOBuf::copyBuffer(
      "GET /url HTTP/1.1\r\nHost: www.facebook.com\r\n"));

  const std::string filler(1024, 'x');
  for (int i = 0; i < 100 && callbacks.streamErrors == 0; i++) {
    codec.onIngress(*folly::IOBuf::copyBuffer(
        folly::to<std::string>("X-Pad-", i, ": ", filler, "\r\n")));
  }

  EXPECT_EQ(callbacks.streamErrors, 1);
  EXPECT_EQ(callbacks.headersComplete, 0);
  ASSERT_NE(callbacks.lastParseError, nullptr);
  EXPECT_EQ(callbacks.lastParseError->getHttpStatusCode(), 400);
  EXPECT_EQ(callbacks.lastParseError->getProxygenError(), kErrorParseHeader);
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

TEST(HTTP1xLlCodecTest, TestFoldedTransferEncodingHeader) {
  HTTP1xLlCodec downstream(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  downstream.setCallback(&callbacks);
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());

  // Generate a POST request with folded
  auto reqBuf = folly::IOBuf::copyBuffer(
      "POST /www.facebook.com HTTP/1.1\r\nHost: www.facebook.com\r\n"
      "Transfer-Encoding: \r\n chunked\r\nContent-Length: 8\r\n\r\n");
  downstream.onIngress(*reqBuf);

  // Check that the request fails before the codec finishes parsing the headers
  EXPECT_EQ(callbacks.streamErrors, 1);
  EXPECT_EQ(callbacks.messageBegin, 1);
  EXPECT_EQ(callbacks.headersComplete, 0);
  EXPECT_EQ(callbacks.lastParseError->getHttpStatusCode(), 400);
  // llhttp rejects the folded value before onHeadersComplete runs
  EXPECT_THAT(callbacks.lastParseError->what(),
              HasSubstr("[Context]=invalid-header-token"));
}

TEST(HTTP1xLlCodecTest, TestBadTransferEncodingHeader) {
  HTTP1xLlCodec downstream(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  downstream.setCallback(&callbacks);
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());

  auto reqBuf = folly::IOBuf::copyBuffer(
      "POST /www.facebook.com HTTP/1.1\r\n"
      "Host: www.facebook.com\r\n"
      "Transfer-Encoding: chunked, zorg\r\n"
      "Transfer-Encoding: chunked, zorg\r\n"
      "\r\n");
  downstream.onIngress(*reqBuf);

  // Check that the request fails before the codec finishes parsing the headers
  EXPECT_EQ(callbacks.streamErrors, 1);
  EXPECT_EQ(callbacks.messageBegin, 1);
  EXPECT_EQ(callbacks.headersComplete, 0);
  EXPECT_EQ(callbacks.lastParseError->getHttpStatusCode(), 400);
  EXPECT_THAT(callbacks.lastParseError->what(),
              HasSubstr("[Context]=invalid-transfer-encoding"));

  // A request rejected by one of onHeadersComplete()'s validation gates never
  // reaches the code that stamps the version, method and URL onto the message,
  // so the partial message handed to onError() has to be backfilled for it to
  // be worth logging or attributing to a request handler.
  auto* partialMsg = callbacks.lastParseError->getPartialMsg();
  ASSERT_NE(partialMsg, nullptr);
  EXPECT_EQ(partialMsg->getMethodString(), "POST");
  EXPECT_EQ(partialMsg->getURL(), "/www.facebook.com");
  EXPECT_EQ(partialMsg->getHTTPVersion(),
            std::make_pair(uint8_t(1), uint8_t(1)));
  // Headers were already present without the backfill; assert they survive it.
  EXPECT_EQ(partialMsg->getHeaders().getSingleOrEmpty(HTTP_HEADER_HOST),
            "www.facebook.com");
}

// url_ accumulates per message, so a rejection on a pipelined request must
// report that request's URL rather than one carried over from its predecessor.
TEST(HTTP1xLlCodecTest, TestPartialMsgBackfilledForPipelinedRequest) {
  HTTP1xLlCodec downstream(TransportDirection::DOWNSTREAM);
  FakeHTTPCodecCallback callbacks;
  downstream.setCallback(&callbacks);

  auto reqBuf = folly::IOBuf::copyBuffer(
      "GET /first HTTP/1.1\r\n"
      "Host: www.facebook.com\r\n"
      "\r\n"
      "POST /second HTTP/1.1\r\n"
      "Host: www.facebook.com\r\n"
      "Transfer-Encoding: chunked, zorg\r\n"
      "Transfer-Encoding: chunked, zorg\r\n"
      "\r\n");
  downstream.onIngress(*reqBuf);

  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(callbacks.streamErrors, 1);
  ASSERT_NE(callbacks.lastParseError, nullptr);
  auto* partialMsg = callbacks.lastParseError->getPartialMsg();
  ASSERT_NE(partialMsg, nullptr);
  EXPECT_EQ(partialMsg->getMethodString(), "POST");
  EXPECT_EQ(partialMsg->getURL(), "/second");
}

// Header name/value rejections fire before onHeadersComplete() is entered at
// all, so they take the same backfill path.
TEST(HTTP1xLlCodecTest, TestPartialMsgBackfilledOnHeaderError) {
  HTTP1xLlCodec downstream(TransportDirection::DOWNSTREAM,
                           /*force1_1=*/true);
  FakeHTTPCodecCallback callbacks;
  downstream.setCallback(&callbacks);

  auto reqBuf = folly::IOBuf::copyBuffer(
      string("GET /yeah?id=7 HTTP/1.1\r\nBad Name: x\r\n\r\n"));
  downstream.onIngress(*reqBuf);

  EXPECT_EQ(callbacks.streamErrors, 1);
  ASSERT_NE(callbacks.lastParseError, nullptr);
  EXPECT_EQ(callbacks.lastParseError->getProxygenError(), kErrorParseHeader);
  auto* partialMsg = callbacks.lastParseError->getPartialMsg();
  ASSERT_NE(partialMsg, nullptr);
  EXPECT_EQ(partialMsg->getMethodString(), "GET");
  EXPECT_EQ(partialMsg->getURL(), "/yeah?id=7");
  // The backfill parses the URL rather than just storing it, so a rule matching
  // on path or query has something to match against.
  EXPECT_EQ(partialMsg->getPathAsStringPiece(), "/yeah");
  EXPECT_EQ(partialMsg->getQueryStringAsStringPiece(), "id=7");
}

// When the request line itself never parsed, url_ is empty and parser_.method
// still holds its default of 0 (HTTP_DELETE).  Backfilling here would invent a
// method the client never sent, so the partial message must be left as-is.
TEST(HTTP1xLlCodecTest, TestPartialMsgNotBackfilledWhenRequestLineUnparsed) {
  HTTP1xLlCodec downstream(TransportDirection::DOWNSTREAM,
                           /*force1_1=*/true);
  FakeHTTPCodecCallback callbacks;
  downstream.setCallback(&callbacks);

  auto reqBuf = folly::IOBuf::copyBuffer(string("GET /Ñoño HTTP/1.1\r\n\r\n"));
  downstream.onIngress(*reqBuf);

  EXPECT_EQ(callbacks.streamErrors, 1);
  ASSERT_NE(callbacks.lastParseError, nullptr);
  auto* partialMsg = callbacks.lastParseError->getPartialMsg();
  ASSERT_EQ(partialMsg, nullptr);
}

// Upstream responses have no method or URL to recover; the backfill must not
// invent a request shape on them.
TEST(HTTP1xLlCodecTest, TestPartialMsgNotBackfilledUpstream) {
  HTTP1xLlCodec upstream(TransportDirection::UPSTREAM);
  FakeHTTPCodecCallback callbacks;
  upstream.setCallback(&callbacks);

  // Prime the codec with a request so that it expects a response.
  HTTPMessage req = getGetRequest("/foo");
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  upstream.generateHeader(writeBuf, upstream.createStream(), req);

  auto respBuf = folly::IOBuf::copyBuffer(
      "HTTP/1.1 200 OK\r\n"
      "Content-Length: 5\r\n"
      "Content-Length: 9\r\n"
      "\r\n");
  upstream.onIngress(*respBuf);

  EXPECT_EQ(callbacks.streamErrors, 1);
  ASSERT_NE(callbacks.lastParseError, nullptr);
  auto* partialMsg = callbacks.lastParseError->getPartialMsg();
  ASSERT_EQ(partialMsg, nullptr);
}

TEST(HTTP1xLlCodecTest, TestParseErrorOriginIdentifiesRejectingBranch) {
  auto parseError = [](const std::string& request) {
    HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
    FakeHTTPCodecCallback callbacks;
    codec.setCallback(&callbacks);
    auto reqBuf = folly::IOBuf::copyBuffer(request);
    codec.onIngress(*reqBuf);
    EXPECT_EQ(callbacks.streamErrors, 1);
    return callbacks.lastParseError
               ? std::string(callbacks.lastParseError->what())
               : std::string();
  };

  auto contentLenError = parseError(
      "POST /www.facebook.com HTTP/1.1\r\nHost: www.facebook.com\r\n"
      "Content-Length: 5\r\nContent-Length: 6\r\n\r\n");
  auto transferEncError = parseError(
      "POST /www.facebook.com HTTP/1.1\r\nHost: www.facebook.com\r\n"
      "Transfer-Encoding: chunked, zorg\r\n"
      "Transfer-Encoding: chunked, zorg\r\n\r\n");

  // http_parser reports both as the same generic callback failure; the context
  // is the only thing that separates them.
  EXPECT_THAT(contentLenError,
              HasSubstr("[Context]=unexpected-content-length"));
  EXPECT_THAT(transferEncError,
              HasSubstr("[Context]=invalid-transfer-encoding"));
}

TEST(HTTP1xLlCodecTest, TestMalformedChunkDelimiter) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);

  auto buf = folly::IOBuf::copyBuffer(
      "GET /hello HTTP/1.1\r\n"
      "Host: localhost:8080\r\n"
      "Transfer-Encoding: chunked\r\n"
      "\r\n"
      "47;"
      "\rX" // after a carriage return, "X" isn't a line feed character
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      "AA\r\n"
      "0\r\n"
      "\r\n"
      "GET /admin HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Transfer-Encoding: chunked\r\n"
      "\r\n"
      "0\r\n"
      "\r\n");

  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onHeadersComplete(1, _));
  EXPECT_CALL(callbacks, onError(1, _, _))
      .WillOnce(Invoke(
          [&](HTTPCodec::StreamID, std::shared_ptr<HTTPException> error, bool) {
            EXPECT_EQ(error->getHttpStatusCode(), 400);
            EXPECT_EQ(error->getProxygenError(), kErrorParseBody);
          }));

  codec.onIngress(*buf);
}

TEST(HTTP1xLlCodecTest, Test1xxConnectionHeader) {
  HTTP1xLlCodec upstream(TransportDirection::UPSTREAM);
  HTTP1xLlCodec downstream(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  upstream.setCallback(&callbacks);
  HTTPMessage resp;
  resp.setStatusCode(100);
  resp.setHTTPVersion(1, 1);
  resp.getHeaders().add(HTTP_HEADER_CONNECTION, "Upgrade");
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  auto streamID = downstream.createStream();
  downstream.generateHeader(writeBuf, streamID, resp);
  upstream.onIngress(*writeBuf.front());
  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(
      callbacks.msg_->getHeaders().getSingleOrEmpty(HTTP_HEADER_CONNECTION),
      "Upgrade");
  resp.setStatusCode(200);
  resp.getHeaders().remove(HTTP_HEADER_CONNECTION);
  resp.getHeaders().add(HTTP_HEADER_CONTENT_LENGTH, "0");
  writeBuf.reset();
  downstream.generateHeader(writeBuf, streamID, resp);
  upstream.onIngress(*writeBuf.front());
  EXPECT_EQ(callbacks.headersComplete, 2);
  EXPECT_EQ(
      callbacks.msg_->getHeaders().getSingleOrEmpty(HTTP_HEADER_CONNECTION),
      "keep-alive");
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

TEST(HTTP1xLlCodecTest, TestMalformedContentLength) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  EXPECT_CALL(callbacks, onError(_, _, _));

  folly::IOBufQueue reqQueue;
  // header name with a space is considered invalid
  reqQueue.append(folly::IOBuf::copyBuffer(
      "POST /test.php HTTP/1.1\r\nHost: www.test.com\r\n"
      "Content-Length : 10\r\n\r\nabcdefghij"));
  codec.onIngress(*reqQueue.front());
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

TEST(HTTP1xLlCodecTest, WebsocketUpgrade) {
  HTTP1xLlCodec upstreamCodec(TransportDirection::UPSTREAM);
  HTTP1xLlCodec downstreamCodec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback downStreamCallbacks;
  HTTP1xLlCodecCallback upstreamCallbacks;
  downstreamCodec.setCallback(&downStreamCallbacks);
  upstreamCodec.setCallback(&upstreamCallbacks);

  HTTPMessage req;
  req.setHTTPVersion(1, 1);
  req.setURL("/websocket");
  req.setEgressWebsocketUpgrade();
  folly::IOBufQueue buf;
  auto streamID = upstreamCodec.createStream();
  upstreamCodec.generateHeader(buf, streamID, req);

  downstreamCodec.onIngress(*buf.front());
  EXPECT_EQ(downStreamCallbacks.headersComplete, 1);
  EXPECT_TRUE(downStreamCallbacks.msg_->isIngressWebsocketUpgrade());
  auto& headers = downStreamCallbacks.msg_->getHeaders();
  auto ws_key_header = headers.getSingleOrEmpty(HTTP_HEADER_SEC_WEBSOCKET_KEY);
  EXPECT_NE(ws_key_header, empty_string);

  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  resp.setStatusCode(101);
  resp.setEgressWebsocketUpgrade();
  buf.reset();
  downstreamCodec.generateHeader(buf, streamID, resp);
  upstreamCodec.onIngress(*buf.front());
  EXPECT_EQ(upstreamCallbacks.headersComplete, 1);
  headers = upstreamCallbacks.msg_->getHeaders();
  auto ws_accept_header =
      headers.getSingleOrEmpty(HTTP_HEADER_SEC_WEBSOCKET_ACCEPT);
  EXPECT_NE(ws_accept_header, empty_string);
}

TEST(HTTP1xLlCodecTest, WebsocketConnectRejectionBody) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);

  HTTPMessage req;
  req.setHTTPVersion(2, 0);
  req.setMethod(HTTPMethod::CONNECT);
  req.setURL("/websocket");
  req.setEgressWebsocketUpgrade();
  folly::IOBufQueue writeBuf;
  codec.generateHeader(writeBuf, codec.createStream(), req);

  HTTP1xLlCodec peer(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback peerCallbacks;
  peer.setCallback(&peerCallbacks);
  peer.onIngress(*writeBuf.front());
  ASSERT_NE(peerCallbacks.msg_, nullptr);
  EXPECT_EQ(peerCallbacks.msg_->getMethod(), HTTPMethod::GET);

  const string body = "Unauthorized";
  auto response = folly::IOBuf::copyBuffer(
      "HTTP/1.1 401 Unauthorized\r\nContent-Length: " +
      folly::to<std::string>(body.size()) + "\r\n\r\n" + body);
  codec.onIngress(*response);

  ASSERT_NE(callbacks.msg_, nullptr);
  EXPECT_EQ(callbacks.msg_->getStatusCode(), 401);
  EXPECT_EQ(callbacks.bodyLen, body.size());
  EXPECT_EQ(callbacks.messageComplete, 1);
  EXPECT_EQ(callbacks.errors, 0);
}

TEST(HTTP1xLlCodecTest, WebsocketConnectNonUpgrade200Body) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);

  HTTPMessage req;
  req.setHTTPVersion(2, 0);
  req.setMethod(HTTPMethod::CONNECT);
  req.setURL("/websocket");
  req.setEgressWebsocketUpgrade();
  folly::IOBufQueue writeBuf;
  codec.generateHeader(writeBuf, codec.createStream(), req);

  const string body = "ordinary response";
  auto response = folly::IOBuf::copyBuffer(
      "HTTP/1.1 200 OK\r\nContent-Length: " +
      folly::to<std::string>(body.size()) + "\r\n\r\n" + body);
  codec.onIngress(*response);

  ASSERT_NE(callbacks.msg_, nullptr);
  EXPECT_EQ(callbacks.msg_->getStatusCode(), 200);
  EXPECT_FALSE(callbacks.msg_->isIngressWebsocketUpgrade());
  EXPECT_EQ(callbacks.bodyLen, body.size());
  EXPECT_EQ(callbacks.messageComplete, 1);
  EXPECT_EQ(callbacks.errors, 0);
}

TEST(HTTP1xLlCodecTest, WebsocketConnectUpgrade) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  HTTP1xLlCodec peer(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  HTTP1xLlCodecCallback peerCallbacks;
  codec.setCallback(&callbacks);
  peer.setCallback(&peerCallbacks);

  HTTPMessage req;
  req.setHTTPVersion(2, 0);
  req.setMethod(HTTPMethod::CONNECT);
  req.setURL("/websocket");
  req.setEgressWebsocketUpgrade();
  folly::IOBufQueue writeBuf;
  const auto streamID = codec.createStream();
  codec.generateHeader(writeBuf, streamID, req);
  peer.onIngress(*writeBuf.front());

  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  resp.setStatusCode(101);
  resp.setEgressWebsocketUpgrade();
  writeBuf.reset();
  peer.generateHeader(writeBuf, streamID, resp);
  codec.onIngress(*writeBuf.front());
  auto data = folly::IOBuf::copyBuffer("upgraded-protocol-data");
  codec.onIngress(*data);

  ASSERT_NE(callbacks.msg_, nullptr);
  EXPECT_EQ(callbacks.msg_->getStatusCode(), 101);
  EXPECT_TRUE(callbacks.msg_->isIngressWebsocketUpgrade());
  EXPECT_EQ(callbacks.bodyLen, data->length());
  EXPECT_EQ(callbacks.errors, 0);
}

TEST(HTTP1xLlCodecTest, DeclinedUpgradeResponseIsChunked) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto req = folly::IOBuf::copyBuffer(
      "GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: "
      "h2c\r\n\r\n");
  codec.onIngress(*req);
  ASSERT_EQ(callbacks.headersComplete, 1);

  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  resp.setStatusCode(200);
  resp.setIsChunked(true);
  resp.getHeaders().add(HTTP_HEADER_TRANSFER_ENCODING, "chunked");
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateHeader(writeBuf, 1, resp);
  writeBuf.reset();
  codec.generateBody(
      writeBuf, 1, folly::IOBuf::copyBuffer("hello world"), folly::none, true);
  EXPECT_EQ(writeBuf.move()->moveToFbString(), "b\r\nhello world\r\n0\r\n\r\n");
}

TEST(HTTP1xLlCodecTest, InterimResponseKeepsConnectTunnel) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto req = folly::IOBuf::copyBuffer(
      "CONNECT a:443 HTTP/1.1\r\nHost: a:443\r\nExpect: 100-continue\r\n\r\n");
  codec.onIngress(*req);

  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  HTTPMessage cont;
  cont.setHTTPVersion(1, 1);
  cont.setStatusCode(100);
  codec.generateHeader(writeBuf, 1, cont);
  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  resp.setStatusCode(200);
  codec.generateHeader(writeBuf, 1, resp);

  auto tunnelBytes = folly::IOBuf::copyBuffer("CLIENTHELLO");
  EXPECT_EQ(codec.onIngress(*tunnelBytes), tunnelBytes->length());
  EXPECT_EQ(callbacks.bodyLen, tunnelBytes->length());
  EXPECT_EQ(callbacks.errors, 0);
}

TEST(HTTP1xLlCodecTest, UpstreamUpgradeHeaderKeepsConnectionOpen) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  HTTPMessage req;
  req.setHTTPVersion(1, 1);
  req.setURL("/");
  req.getHeaders().add(HTTP_HEADER_UPGRADE, "h2c");
  req.getHeaders().add(HTTP_HEADER_CONNECTION, "Upgrade");
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateHeader(writeBuf, codec.createStream(), req, true);
  EXPECT_TRUE(codec.isReusable());
  EXPECT_FALSE(codec.closeOnEgressComplete());
}

TEST(HTTP1xLlCodecTest, UpstreamCustomUpgradeAccepted) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  HTTPMessage req;
  req.setHTTPVersion(1, 1);
  req.setURL("/");
  req.getHeaders().add(HTTP_HEADER_UPGRADE, "foo");
  req.getHeaders().add(HTTP_HEADER_CONNECTION, "Upgrade");
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateHeader(writeBuf, codec.createStream(), req, true);

  auto resp = folly::IOBuf::copyBuffer(
      "HTTP/1.1 101 Switching Protocols\r\nUpgrade: foo\r\n"
      "Connection: Upgrade\r\n\r\n");
  codec.onIngress(*resp);
  auto data = folly::IOBuf::copyBuffer("foo-data");
  EXPECT_EQ(codec.onIngress(*data), data->length());
  EXPECT_EQ(callbacks.errors, 0);
  EXPECT_EQ(callbacks.bodyLen, data->length());
}

// A rejected (non-101) response to an HTTP/1.1 Upgrade request closes the
// connection so trailing upgraded-protocol bytes are never reparsed as a
// pipelined request.
TEST(HTTP1xLlCodecTest, Http11UpgradeRequestRejectHasConnectionClose) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto txnID = codec.createStream();

  auto reqBuf =
      folly::IOBuf::copyBuffer(string("GET /foo HTTP/1.1\r\n"
                                      "Host: localhost\r\n"
                                      "Upgrade: blarf\r\n"
                                      "Connection: upgrade\r\n"
                                      "\r\n"));
  codec.onIngress(*reqBuf);
  EXPECT_EQ(callbacks.headersComplete, 1);

  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  resp.setStatusCode(200);
  resp.setStatusMessage("OK");
  resp.getHeaders().set(HTTP_HEADER_CONTENT_LENGTH, "0");
  folly::IOBufQueue respBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateHeader(respBuf, txnID, resp, true);

  auto respStr = respBuf.move()->moveToFbString();
  EXPECT_NE(respStr.find("Connection: close"), string::npos);
  EXPECT_FALSE(codec.isReusable());
}

// A genuine websocket upgrade accept is a real protocol switch: it must keep
// its Connection: upgrade token and must NOT be forced to close.
TEST(HTTP1xLlCodecTest, Http11WebsocketUpgradeAcceptKeepsConnectionUpgrade) {
  HTTP1xLlCodec upstreamCodec(TransportDirection::UPSTREAM);
  HTTP1xLlCodec downstreamCodec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback downstreamCallbacks;
  downstreamCodec.setCallback(&downstreamCallbacks);

  HTTPMessage req;
  req.setHTTPVersion(1, 1);
  req.setURL("/websocket");
  req.setEgressWebsocketUpgrade();
  folly::IOBufQueue buf;
  auto streamID = upstreamCodec.createStream();
  upstreamCodec.generateHeader(buf, streamID, req);
  downstreamCodec.onIngress(*buf.front());
  ASSERT_TRUE(downstreamCallbacks.msg_->isIngressWebsocketUpgrade());

  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  resp.setStatusCode(101);
  resp.setEgressWebsocketUpgrade();
  buf.reset();
  downstreamCodec.generateHeader(buf, streamID, resp);

  auto respStr = buf.move()->moveToFbString();
  EXPECT_NE(respStr.find("Connection: Upgrade"), string::npos);
  EXPECT_EQ(respStr.find("close"), string::npos);
}

// A normal (non-upgrade) request is unaffected: its response keeps the default
// keep-alive and the connection stays reusable.
TEST(HTTP1xLlCodecTest, Http11NonUpgradeRequestNoForcedClose) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto txnID = codec.createStream();

  auto reqBuf =
      folly::IOBuf::copyBuffer(string("GET /foo HTTP/1.1\r\n"
                                      "Host: localhost\r\n"
                                      "\r\n"));
  codec.onIngress(*reqBuf);
  EXPECT_EQ(callbacks.headersComplete, 1);

  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  resp.setStatusCode(200);
  resp.setStatusMessage("OK");
  resp.getHeaders().set(HTTP_HEADER_CONTENT_LENGTH, "0");
  folly::IOBufQueue respBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateHeader(respBuf, txnID, resp, true);

  auto respStr = respBuf.move()->moveToFbString();
  EXPECT_NE(respStr.find("Connection: keep-alive"), string::npos);
  EXPECT_TRUE(codec.isReusable());
}

TEST(HTTP1xLlCodecTest, WebsocketUpgradeKeyError) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);

  HTTPMessage req;
  req.setHTTPVersion(1, 1);
  req.setURL("/websocket");
  req.setEgressWebsocketUpgrade();
  folly::IOBufQueue buf;
  auto streamID = codec.createStream();
  codec.generateHeader(buf, streamID, req);

  auto resp = folly::IOBuf::copyBuffer(
      "HTTP/1.1 200 OK\r\n"
      "Connection: upgrade\r\n"
      "Upgrade: websocket\r\n"
      "\r\n");
  codec.onIngress(*resp);
  EXPECT_EQ(callbacks.headersComplete, 0);
  EXPECT_EQ(callbacks.errors, 1);
}

TEST(HTTP1xLlCodecTest, WebsocketUpgradeHeaderSet) {
  HTTP1xLlCodec upstreamCodec(TransportDirection::UPSTREAM);
  HTTPMessage req;
  req.setMethod(HTTPMethod::GET);
  req.setURL("/websocket");
  req.setEgressWebsocketUpgrade();
  req.getHeaders().add(proxygen::HTTP_HEADER_UPGRADE, "Websocket");

  folly::IOBufQueue buf;
  upstreamCodec.generateHeader(buf, upstreamCodec.createStream(), req);

  HTTP1xLlCodec downstreamCodec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  downstreamCodec.setCallback(&callbacks);
  downstreamCodec.onIngress(*buf.front());
  auto headers = callbacks.msg_->getHeaders();
  EXPECT_EQ(headers.getSingleOrEmpty(HTTP_HEADER_SEC_WEBSOCKET_KEY),
            empty_string);
}

TEST(HTTP1xLlCodecTest, WebsocketConnectionHeader) {
  HTTP1xLlCodec upstreamCodec(TransportDirection::UPSTREAM);
  HTTPMessage req;
  req.setMethod(HTTPMethod::GET);
  req.setURL("/websocket");
  req.setEgressWebsocketUpgrade();
  req.getHeaders().add(proxygen::HTTP_HEADER_CONNECTION, "upgrade, keep-alive");
  req.getHeaders().add(proxygen::HTTP_HEADER_SEC_WEBSOCKET_KEY,
                       "key should change");
  req.getHeaders().add(proxygen::HTTP_HEADER_SEC_WEBSOCKET_ACCEPT,
                       "this should not be found");

  folly::IOBufQueue buf;
  upstreamCodec.generateHeader(buf, upstreamCodec.createStream(), req);
  HTTP1xLlCodec downstreamCodec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  downstreamCodec.setCallback(&callbacks);
  downstreamCodec.onIngress(*buf.front());
  auto headers = callbacks.msg_->getHeaders();
  const auto& ws_sec_key =
      headers.getSingleOrEmpty(HTTP_HEADER_SEC_WEBSOCKET_KEY);
  EXPECT_NE(ws_sec_key, empty_string);
  EXPECT_NE(ws_sec_key, "key should change");

  EXPECT_NO_THROW(folly::base64Decode(ws_sec_key));
  EXPECT_EQ(16, folly::base64Decode(ws_sec_key).length());

  EXPECT_EQ(headers.getSingleOrEmpty(HTTP_HEADER_SEC_WEBSOCKET_ACCEPT),
            empty_string);
  EXPECT_EQ(headers.getSingleOrEmpty(HTTP_HEADER_CONNECTION),
            "upgrade, keep-alive");
}

TEST(HTTP1xLlCodecTest, WebsocketUpgradeSecondTxn) {
  /**
   * RFC9110:
   *  For example, a CONNECT request or a request with the Upgrade header field
   *  can occur at any time, not just in the first message on a connection.
   */
  HTTP1xLlCodec upstream{TransportDirection::UPSTREAM};
  HTTP1xLlCodec downstream{TransportDirection::DOWNSTREAM};
  HTTP1xLlCodecCallback upstreamCbs;
  HTTP1xLlCodecCallback downstreamCbs;
  folly::IOBufQueue buf;

  downstream.setCallback(&downstreamCbs);
  upstream.setCallback(&upstreamCbs);

  // generate req/res for simple `GET /` request
  {
    HTTPMessage req;
    req.setHTTPVersion(1, 1);
    req.setMethod(HTTPMethod::GET);
    req.setURL("/");
    auto streamId = upstream.createStream();

    // parse downstream req
    upstream.generateHeader(buf, streamId, req, /*eom=*/true);
    downstream.onIngress(*buf.front());
    buf.reset();

    // send downstream resp
    HTTPMessage resp;
    resp.setHTTPVersion(1, 1);
    resp.setStatusCode(200);
    buf.reset();
    downstream.generateHeader(buf, streamId, resp, /*eom=*/true);
    upstream.onIngress(*buf.front());
    buf.reset();

    // both codecs should be reusable
    EXPECT_TRUE(upstream.isReusable() && downstream.isReusable());
  }

  // reusing the same codec, generate req/res for ws request
  {
    HTTPMessage req;
    req.setHTTPVersion(1, 1);
    req.setURL("/websocket");
    req.setEgressWebsocketUpgrade();
    auto streamID = upstream.createStream();
    upstream.generateHeader(buf, streamID, req);

    downstream.onIngress(*buf.front());
    EXPECT_EQ(downstreamCbs.headersComplete, 2);
    auto& msg = downstreamCbs.msg_;
    EXPECT_TRUE(msg->isIngressWebsocketUpgrade());
    auto ws_key_header =
        msg->getHeaders().getSingleOrEmpty(HTTP_HEADER_SEC_WEBSOCKET_KEY);
    EXPECT_NE(ws_key_header, empty_string);

    HTTPMessage resp;
    resp.setHTTPVersion(1, 1);
    resp.setStatusCode(101);
    resp.setEgressWebsocketUpgrade();
    buf.reset();
    downstream.generateHeader(buf, streamID, resp);
    upstream.onIngress(*buf.front());
    auto ws_accept_header = upstreamCbs.msg_->getHeaders().getSingleOrEmpty(
        HTTP_HEADER_SEC_WEBSOCKET_ACCEPT);
    EXPECT_NE(ws_accept_header, empty_string);

    // both codecs should no longer be reusable
    EXPECT_TRUE(!upstream.isReusable() && !downstream.isReusable());
  }
}

TEST(HTTP1xLlCodecTest, TrailersAndEomAreNotGeneratedWhenNonChunked) {
  // Verify that generateTrailes and generateEom result in 0 bytes
  // generated when message is not chunked.
  // HTTP2 codec handles all BODY as regular non-chunked body, thus
  // HTTPTransation SM transitions allow trailers after regular body. Which
  // is not allowed in HTTP1.
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);

  auto txnID = codec.createStream();

  HTTPMessage msg;
  msg.setHTTPVersion(1, 1);
  msg.setURL("https://www.facebook.com/");
  msg.getHeaders().set(HTTP_HEADER_HOST, "www.facebook.com");
  msg.setIsChunked(false);

  folly::IOBufQueue buf;
  codec.generateHeader(buf, txnID, msg);

  HTTPHeaders trailers;
  trailers.add("X-Test-Trailer", "test");
  EXPECT_EQ(0, codec.generateTrailers(buf, txnID, trailers));
  EXPECT_EQ(0, codec.generateEOM(buf, txnID));
}

TEST(HTTP1xLlCodecTest, TestChunkResponseSerialization) {
  // When codec is used for response serialization, it never gets
  // to process request. Verify we can still serialize chunked response
  // when mayChunkEgress=true.
  folly::IOBufQueue blob;

  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  resp.setStatusCode(200);
  resp.setIsChunked(true);
  resp.getHeaders().set(HTTP_HEADER_TRANSFER_ENCODING, "chunked");
  resp.getHeaders().set("X-Custom-Header", "mac&cheese");

  string bodyStr("pizza");
  auto body = folly::IOBuf::copyBuffer(bodyStr);

  HTTPHeaders trailers;
  trailers.add("X-Test-Trailer", "chicken kyiv");

  // serialize
  HTTP1xLlCodec downCodec = HTTP1xLlCodec::makeResponseCodec(
      /*mayChunkEgress=*/true);
  HTTP1xLlCodecCallback downCallbacks;
  downCodec.setCallback(&downCallbacks);
  auto downStream = downCodec.createStream();

  downCodec.generateHeader(blob, downStream, resp);
  downCodec.generateBody(
      blob, downStream, body->clone(), HTTPCodec::NoPadding, false);
  downCodec.generateTrailers(blob, downStream, trailers);

  std::string tmp;
  blob.appendToString(tmp);
  VLOG(2) << "serializeMessage blob: " << tmp;

  // deserialize
  HTTP1xLlCodec upCodec(TransportDirection::UPSTREAM);
  HTTP1xLlCodecCallback upCallbacks;
  upCodec.setCallback(&upCallbacks);

  auto tmpBuf = blob.front()->clone();
  while (tmpBuf) {
    auto next = tmpBuf->pop();
    upCodec.onIngress(*tmpBuf);
    tmpBuf = std::move(next);
  }
  upCodec.onIngressEOF();

  EXPECT_EQ(upCallbacks.headersComplete, 1);
  EXPECT_EQ(upCallbacks.trailersComplete, 1);
  EXPECT_EQ(upCallbacks.messageComplete, 1);
  EXPECT_EQ(upCallbacks.errors, 0);

  EXPECT_EQ(resp.getStatusCode(), upCallbacks.msg_->getStatusCode());
  EXPECT_TRUE(
      upCallbacks.msg_->getHeaders().exists(HTTP_HEADER_TRANSFER_ENCODING));
  EXPECT_TRUE(upCallbacks.msg_->getHeaders().exists("X-Custom-Header"));
  EXPECT_TRUE(upCallbacks.trailers_->exists("X-Test-Trailer"));
}

TEST(HTTP1xLlCodecTest, TestGenerateEmptyBodyWithEOM) {
  HTTP1xLlCodec upstreamCodec(TransportDirection::UPSTREAM);
  HTTPMessage req;
  req.setMethod(HTTPMethod::POST);
  req.setURL("/");
  req.setHTTPVersion(1, 1);
  req.getHeaders().set(HTTP_HEADER_TRANSFER_ENCODING, "chunked");
  req.setIsChunked(true);
  folly::IOBufQueue buf;
  auto stream = upstreamCodec.createStream();
  upstreamCodec.generateHeader(buf, stream, req);
  EXPECT_GT(upstreamCodec.generateBody(buf, stream, nullptr, folly::none, true),
            0);
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

TEST(HTTP1xLlCodecTest, DownstreamGoaway) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  EXPECT_TRUE(codec.isReusable());
  EXPECT_FALSE(codec.isWaitingToDrain());
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateGoaway(writeBuf, HTTPCodec::MaxStreamID, ErrorCode::NO_ERROR);
  EXPECT_TRUE(codec.isReusable());
  EXPECT_TRUE(codec.isWaitingToDrain());
  auto buf = folly::IOBuf::copyBuffer("GET / HTTP/1.1\r\n\r\n");
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  codec.onIngress(*buf);
  HTTPMessage resp;
  resp.setStatusCode(200);
  codec.generateHeader(writeBuf, 1, resp, true);
  EXPECT_FALSE(codec.isReusable());
  EXPECT_FALSE(codec.isWaitingToDrain());
}

TEST(HTTP1xLlCodecTest, DownstreamImmediateGoaway) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateImmediateGoaway(writeBuf, ErrorCode::NO_ERROR, nullptr);
  EXPECT_FALSE(codec.isReusable());
  EXPECT_FALSE(codec.isWaitingToDrain());
}

TEST(HTTP1xLlCodecTest, DownstreamErrorGoaway) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateGoaway(
      writeBuf, HTTPCodec::MaxStreamID, ErrorCode::PROTOCOL_ERROR);
  EXPECT_FALSE(codec.isReusable());
  EXPECT_FALSE(codec.isWaitingToDrain());
}

TEST(HTTP1xLlCodecTest, GenerateRst) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateRstStream(writeBuf, 1, ErrorCode::INTERNAL_ERROR);
  EXPECT_FALSE(codec.isReusable());
  EXPECT_FALSE(codec.isWaitingToDrain());
}

TEST(HTTP1xLlCodecTest, UpstreamGoaway) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);

  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateGoaway(writeBuf, 0, ErrorCode::NO_ERROR);
  EXPECT_FALSE(codec.isReusable());
  EXPECT_FALSE(codec.isWaitingToDrain());
}

TEST(HTTP1xLlCodecTest, CloseOnEgressCompleteUpstreamConnect) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);

  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  EXPECT_FALSE(codec.closeOnEgressComplete());
  HTTPMessage req;
  req.setMethod(HTTPMethod::CONNECT);
  req.setURL("/");
  auto id = codec.createStream();
  codec.generateHeader(writeBuf, id, req, false);
  EXPECT_FALSE(codec.closeOnEgressComplete());
  codec.generateEOM(writeBuf, id);
  EXPECT_TRUE(codec.closeOnEgressComplete());
  auto ingress = folly::IOBuf::copyBuffer(std::string("HTTP/1.1 200 OK\r\r\n"));
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  codec.onIngress(*ingress);
  EXPECT_TRUE(codec.closeOnEgressComplete());
}

TEST(HTTP1xLlCodecTest, CloseOnEgressCompleteDownstreamResponse) {
  // 1.0 response with no content-length/no chunking
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);

  EXPECT_FALSE(codec.closeOnEgressComplete());
  auto ingress =
      folly::IOBuf::copyBuffer(std::string("GET / HTTP/1.0\r\n\r\n"));
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  codec.onIngress(*ingress);
  EXPECT_FALSE(codec.closeOnEgressComplete());

  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  HTTPMessage resp;
  resp.setStatusCode(200);
  auto body = makeBuf(100);
  auto id = 1;
  codec.generateHeader(writeBuf, id, resp, false);
  EXPECT_FALSE(codec.closeOnEgressComplete());
  codec.generateBody(writeBuf, id, makeBuf(100), folly::none, false);
  EXPECT_FALSE(codec.closeOnEgressComplete());
  codec.generateEOM(writeBuf, id);
  EXPECT_TRUE(codec.closeOnEgressComplete());
}

TEST(HTTP1xLlCodecTest, GenerateExtraHeaders) {
  HTTP1xLlCodec upstream(TransportDirection::UPSTREAM);
  HTTP1xLlCodec downstream(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  upstream.setCallback(&callbacks);

  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  resp.setStatusCode(200);
  resp.getHeaders().set(HTTP_HEADER_CONTENT_LENGTH, "1000");

  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  auto id = downstream.createStream();
  HTTPHeaders extraHeaders;
  extraHeaders.add(HTTP_HEADER_PRIORITY, "u=1");
  downstream.generateHeader(writeBuf,
                            id,
                            resp,
                            false,
                            nullptr /* HTTPHeaderSize */,
                            std::move(extraHeaders));

  upstream.onIngress(*writeBuf.front());
  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(
      "u=1",
      callbacks.msg_->getHeaders().getSingleOrEmpty(HTTP_HEADER_PRIORITY));
  EXPECT_EQ("1000",
            callbacks.msg_->getHeaders().getSingleOrEmpty(
                HTTP_HEADER_CONTENT_LENGTH));
}

TEST(HTTP1xLlCodecTest, UpstreamConnectTunnelBytes) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  HTTPMessage req;
  req.setMethod(HTTPMethod::CONNECT);
  req.setURL("a:443");
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateHeader(writeBuf, codec.createStream(), req, false);

  auto resp = folly::IOBuf::copyBuffer("HTTP/1.1 200 OK\r\n\r\nSERVERHELLO");
  EXPECT_EQ(codec.onIngress(*resp), resp->length());
  EXPECT_EQ(callbacks.errors, 0);
  EXPECT_EQ(callbacks.bodyLen, 11);
}

TEST(HTTP1xLlCodecTest, UpstreamResponseReadUntilEOFIsNotReusable) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  HTTPMessage req = getGetRequest();
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateHeader(writeBuf, codec.createStream(), req, true);

  auto resp = folly::IOBuf::copyBuffer("HTTP/1.1 200 OK\r\n\r\nbody");
  codec.onIngress(*resp);
  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_FALSE(codec.isReusable());
}

TEST(HTTP1xLlCodecTest, UpstreamHeadResponseWithoutLengthIsReusable) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  HTTPMessage req = getGetRequest();
  req.setMethod(HTTPMethod::HEAD);
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateHeader(writeBuf, codec.createStream(), req, true);

  auto resp = folly::IOBuf::copyBuffer("HTTP/1.1 200 OK\r\n\r\n");
  codec.onIngress(*resp);
  EXPECT_EQ(callbacks.messageComplete, 1);
  EXPECT_TRUE(codec.isReusable());
}

TEST(HTTP1xLlCodecTest, UpgradeWithoutConnectionUpgradeIsIgnored) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto req = folly::IOBuf::copyBuffer(
      "GET / HTTP/1.1\r\nHost: a\r\nUpgrade: websocket\r\n"
      "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n");
  codec.onIngress(*req);
  ASSERT_NE(callbacks.msg_, nullptr);
  EXPECT_FALSE(callbacks.msg_->isIngressWebsocketUpgrade());
  EXPECT_EQ(callbacks.messageComplete, 1);
  EXPECT_EQ(callbacks.errors, 0);

  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  resp.setStatusCode(200);
  resp.setIsChunked(true);
  resp.getHeaders().add(HTTP_HEADER_TRANSFER_ENCODING, "chunked");
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  codec.generateHeader(writeBuf, 1, resp);
  writeBuf.reset();
  codec.generateBody(
      writeBuf, 1, folly::IOBuf::copyBuffer("hi"), folly::none, true);
  EXPECT_EQ(writeBuf.move()->moveToFbString(), "2\r\nhi\r\n0\r\n\r\n");
}

TEST(HTTP1xLlCodecTest, WebsocketUpgradeDuplicate) {
  HTTP1xLlCodec downstreamCodec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback downStreamCallbacks;
  downstreamCodec.setCallback(&downStreamCallbacks);

  auto reqBuf = folly::IOBuf::copyBuffer(
      "GET / HTTP/1.1\r\nConnection: Upgrade\r\nUpgrade:websocket\r\n\r\n");
  EXPECT_CALL(downStreamCallbacks, onError(_, _, _)).Times(0);

  EXPECT_EQ(downstreamCodec.onIngress(*reqBuf), reqBuf->length());
  EXPECT_EQ(downstreamCodec.onIngress(*reqBuf), 0);
  EXPECT_FALSE(downstreamCodec.isReusable());
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

TEST(HTTP1xLlCodecTest, HugeURL) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto request = folly::to<std::string>("GET /echo?q=",
                                        std::string(84 * 1024, 'a'),
                                        " HTTP/1.1\r\n"
                                        "Host: foo\r\n\r\n");
  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onError(1, _, _))
      .WillOnce(Invoke(
          [&](HTTPCodec::StreamID, std::shared_ptr<HTTPException> error, bool) {
            EXPECT_EQ(error->getHttpStatusCode(), 400);
          }));
  folly::IOBufQueue input{folly::IOBufQueue::cacheChainLength()};
  input.append(folly::IOBuf::wrapBuffer(request.data(), request.size()));
  while (input.chainLength() > 0) {
    auto head = input.splitAtMost(4096);
    codec.onIngress(*head);
  }
}

TEST(HTTP1xLlCodecTest, UTF8Chars) {
  // Non-ascii characters should be pct-encoded according to RFC3986. S220852
  // was caused partly due to UTF-8 characters found in the URL.
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM,
                      /*force1_1=*/true);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto utf8_string = std::string("Ñoño");
  auto badRequest =
      folly::to<std::string>("GET /echo?dl=", utf8_string, " HTTP/1.1\r\n\r\n");
  std::string uriEscapedStr;
  folly::uriEscape(utf8_string, uriEscapedStr);
  auto goodRequest = folly::to<std::string>(
      "GET /echo?dl=", uriEscapedStr, " HTTP/1.1\r\n\r\n");
  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onHeadersComplete(1, _));
  EXPECT_CALL(callbacks, onMessageComplete(1, _));
  codec.onIngress(
      *folly::IOBuf::wrapBuffer(goodRequest.data(), goodRequest.size()));

  EXPECT_CALL(callbacks, onMessageBegin(2, _));
  EXPECT_CALL(callbacks, onError(2, _, _));

  codec.onIngress(
      *folly::IOBuf::wrapBuffer(badRequest.data(), badRequest.size()));
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

TEST(HTTP1xLlCodecTest, ContentLengthLast) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  std::string request = "GET /echo HTTP/1.1\r\n\r\n";
  codec.onIngress(*folly::IOBuf::wrapBuffer(request.data(), request.size()));
  HTTPCodec::StreamID id = 1;
  HTTPMessage resp;
  resp.setStatusCode(200);
  resp.getHeaders().set(HTTP_HEADER_CONTENT_LENGTH, "100");
  resp.getHeaders().set("a", "b");
  resp.getHeaders().set("z", "x");
  folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
  codec.generateHeader(buf, id, resp, true);
  auto length = buf.chainLength();
  std::string clHeader = "Content-Length: 100";
  auto expectedPos = length - clHeader.size() - 4 /* \r\n\r\n */;
  EXPECT_EQ(buf.move()->moveToFbString().find(clHeader), expectedPos);
}

TEST(HTTP1xLlCodecTest, HugeChunkLength) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto request = folly::to<std::string>(
      "POST /echo HTTP/1.1\r\n"
      "Transfer-Encoding: chunked\r\n"
      "\r\n"
      "ffffffffffffffff\r\n");
  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onError(1, _, _))
      .WillOnce(Invoke(
          [&](HTTPCodec::StreamID, std::shared_ptr<HTTPException> error, bool) {
            // TODO: It would probably be nicer to return a 413 code.
            EXPECT_EQ(error->getHttpStatusCode(), 400);
          }));
  codec.onIngress(*folly::IOBuf::wrapBuffer(request.data(), request.size()));
}

TEST(HTTP1xLlCodecTest, HugeContentLength) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto request = folly::to<std::string>(
      "POST /echo HTTP/1.1\r\n"
      "Content-Length: ffffffffffffffff\r\n");
  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onError(1, _, _))
      .WillOnce(Invoke(
          [&](HTTPCodec::StreamID, std::shared_ptr<HTTPException> error, bool) {
            // TODO: It would probably be nicer to return a 413 code.
            EXPECT_EQ(error->getHttpStatusCode(), 400);
          }));
  codec.onIngress(*folly::IOBuf::wrapBuffer(request.data(), request.size()));
}

TEST(HTTP1xLlCodecTest, Dechunk) {
  // Send a 1.0 request and a 1.1 chunked response.  The codec should dechunk
  HTTP1xLlCodec upCodec(TransportDirection::UPSTREAM, false);
  folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
  HTTPMessage req;
  req.setHTTPVersion(1, 0);
  req.setURL("/");
  req.setMethod(HTTPMethod::GET);
  HTTPCodec::StreamID id = upCodec.createStream();
  upCodec.generateHeader(buf, id, req, false);

  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  codec.onIngress(*buf.front());
  buf.reset();

  HTTPMessage resp;
  resp.setStatusCode(200);
  resp.setHTTPVersion(1, 1);
  resp.setIsChunked(true);
  resp.getHeaders().add(HTTP_HEADER_TRANSFER_ENCODING, "chunked");
  std::string body(10, 'a');
  codec.generateHeader(buf, id, resp, false);
  codec.generateChunkHeader(buf, id, 1);
  codec.generateBody(buf,
                     id,
                     folly::IOBuf::wrapBuffer(body.data(), body.size()),
                     folly::none,
                     false);
  codec.generateChunkTerminator(buf, id);
  codec.generateEOM(buf, id);

  StrictMock<MockHTTPCodecCallback> upCallbacks;
  upCodec.setCallback(&upCallbacks);
  // No chunk header callbacks
  EXPECT_CALL(upCallbacks, onMessageBegin(1, _));
  EXPECT_CALL(upCallbacks, onHeadersComplete(1, _))
      .WillOnce(Invoke([](HTTPCodec::StreamID,
                          std::shared_ptr<HTTPMessage> resp) {
        EXPECT_FALSE(resp->getHeaders().exists(HTTP_HEADER_TRANSFER_ENCODING));
        EXPECT_EQ(resp->getHeaders().getSingleOrEmpty(HTTP_HEADER_CONNECTION),
                  "close");
      }));
  EXPECT_CALL(upCallbacks, onBody(1, _, _));
  upCodec.onIngress(*buf.front());
  EXPECT_CALL(upCallbacks, onMessageComplete(1, _));
  upCodec.onIngressEOF();
}

TEST(HTTP1xLlCodecTest, Chunkify) {
  // Send a 1.1 request and a 1.0 non-chunked response.  The codec should chunk
  HTTP1xLlCodec upCodec(TransportDirection::UPSTREAM, false);
  folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
  HTTPMessage req;
  req.setHTTPVersion(1, 1);
  req.setURL("/");
  req.setMethod(HTTPMethod::GET);
  HTTPCodec::StreamID id = upCodec.createStream();
  upCodec.generateHeader(buf, id, req, false);

  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM, /*force11*/ true);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  codec.onIngress(*buf.front());
  buf.reset();

  HTTPMessage resp;
  resp.setStatusCode(200);
  resp.setHTTPVersion(1, 0);
  resp.setIsChunked(false);
  std::string body(10, 'a');
  codec.generateHeader(buf, id, resp, false);
  codec.generateBody(buf,
                     id,
                     folly::IOBuf::wrapBuffer(body.data(), body.size()),
                     folly::none,
                     false);
  codec.generateEOM(buf, id);

  StrictMock<MockHTTPCodecCallback> upCallbacks;
  upCodec.setCallback(&upCallbacks);
  // No chunk header callbacks
  EXPECT_CALL(upCallbacks, onMessageBegin(1, _));
  EXPECT_CALL(upCallbacks, onHeadersComplete(1, _))
      .WillOnce(Invoke([](HTTPCodec::StreamID,
                          std::shared_ptr<HTTPMessage> resp) {
        EXPECT_TRUE(resp->getHeaders().exists(HTTP_HEADER_TRANSFER_ENCODING));
        EXPECT_EQ(resp->getHeaders().getSingleOrEmpty(HTTP_HEADER_CONNECTION),
                  "keep-alive");
      }));
  EXPECT_CALL(upCallbacks, onChunkHeader(1, _));
  EXPECT_CALL(upCallbacks, onBody(1, _, _));
  EXPECT_CALL(upCallbacks, onChunkComplete(1));
  EXPECT_CALL(upCallbacks, onMessageComplete(1, _));
  upCodec.onIngress(*buf.front());
}

TEST(HTTP1xLlCodecTest, ChunkifyConnClose) {
  // Send a 1.1 request with Connection: close and a 1.1 non-chunked response.
  // The codec should chunk.
  HTTP1xLlCodec upCodec(TransportDirection::UPSTREAM, false);
  folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
  HTTPMessage req;
  req.setHTTPVersion(1, 1);
  req.setURL("/");
  req.setMethod(HTTPMethod::GET);
  req.getHeaders().add(HTTP_HEADER_CONNECTION, "close");
  HTTPCodec::StreamID id = upCodec.createStream();
  upCodec.generateHeader(buf, id, req, false);

  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM, false);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  codec.onIngress(*buf.front());
  buf.reset();

  HTTPMessage resp;
  resp.setStatusCode(200);
  resp.setHTTPVersion(1, 1);
  resp.setIsChunked(false);
  std::string body(10, 'a');
  codec.generateHeader(buf, id, resp, false);
  codec.generateBody(buf,
                     id,
                     folly::IOBuf::wrapBuffer(body.data(), body.size()),
                     folly::none,
                     false);
  codec.generateEOM(buf, id);

  StrictMock<MockHTTPCodecCallback> upCallbacks;
  upCodec.setCallback(&upCallbacks);
  // No chunk header callbacks
  EXPECT_CALL(upCallbacks, onMessageBegin(1, _));
  EXPECT_CALL(upCallbacks, onHeadersComplete(1, _))
      .WillOnce(Invoke([](HTTPCodec::StreamID,
                          std::shared_ptr<HTTPMessage> resp) {
        EXPECT_EQ(
            resp->getHeaders().getSingleOrEmpty(HTTP_HEADER_TRANSFER_ENCODING),
            "chunked");
        EXPECT_EQ(resp->getHeaders().getSingleOrEmpty(HTTP_HEADER_CONNECTION),
                  "close");
      }));
  EXPECT_CALL(upCallbacks, onChunkHeader(1, _));
  EXPECT_CALL(upCallbacks, onBody(1, _, _));
  EXPECT_CALL(upCallbacks, onChunkComplete(1));
  EXPECT_CALL(upCallbacks, onMessageComplete(1, _));
  upCodec.onIngress(*buf.front());
}

TEST(HTTP1xLlCodecTest, Chunkify100) {
  // Send a 1.1 request and a 1.0 non-chunked response.  The codec should chunk
  HTTP1xLlCodec upCodec(TransportDirection::UPSTREAM, false);
  folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
  HTTPMessage req;
  req.setHTTPVersion(1, 1);
  req.setURL("/");
  req.setMethod(HTTPMethod::GET);
  req.getHeaders().add(kExpect, "100-continue");
  HTTPCodec::StreamID id = upCodec.createStream();
  upCodec.generateHeader(buf, id, req, false);

  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM, /*force11*/ true);
  HTTP1xLlCodecCallback callbacks;
  codec.setCallback(&callbacks);
  codec.onIngress(*buf.front());
  buf.reset();

  HTTPMessage resp;
  resp.setStatusCode(100);
  resp.setIsChunked(false);
  codec.generateHeader(buf, id, resp, false);

  resp.setStatusCode(200);
  resp.setHTTPVersion(1, 0);
  resp.setIsChunked(false);
  std::string body(10, 'a');
  codec.generateHeader(buf, id, resp, false);
  codec.generateBody(buf,
                     id,
                     folly::IOBuf::wrapBuffer(body.data(), body.size()),
                     folly::none,
                     false);
  codec.generateEOM(buf, id);

  StrictMock<MockHTTPCodecCallback> upCallbacks;
  upCodec.setCallback(&upCallbacks);
  // No chunk header callbacks
  EXPECT_CALL(upCallbacks, onMessageBegin(1, _)).Times(2);
  EXPECT_CALL(upCallbacks, onHeadersComplete(1, _))
      .WillOnce(
          Invoke([](HTTPCodec::StreamID, std::shared_ptr<HTTPMessage> resp) {
            EXPECT_EQ(resp->getStatusCode(), 100);
            EXPECT_FALSE(resp->getHeaders().exists(HTTP_HEADER_CONNECTION));
          }))
      .WillOnce(Invoke([](HTTPCodec::StreamID,
                          std::shared_ptr<HTTPMessage> resp) {
        EXPECT_TRUE(resp->getHeaders().exists(HTTP_HEADER_TRANSFER_ENCODING));
        EXPECT_EQ(resp->getHeaders().getSingleOrEmpty(HTTP_HEADER_CONNECTION),
                  "keep-alive");
      }));
  EXPECT_CALL(upCallbacks, onChunkHeader(1, _));
  EXPECT_CALL(upCallbacks, onBody(1, _, _));
  EXPECT_CALL(upCallbacks, onChunkComplete(1));
  EXPECT_CALL(upCallbacks, onMessageComplete(1, _));
  upCodec.onIngress(*buf.front());
}

TEST(HTTP1xLlCodecTest, TrailersNonChunked) {
  HTTP1xLlCodec codec(TransportDirection::UPSTREAM);
  StrictMock<MockHTTPCodecCallback> callbacks;
  codec.setCallback(&callbacks);
  HTTPMessage req;
  auto id = codec.createStream();
  req.setHTTPVersion(1, 1);
  req.setMethod(HTTPMethod::GET);
  req.setURL("/");
  codec.setCallback(&callbacks);
  folly::IOBufQueue buf;
  codec.generateHeader(buf, id, req, true);

  auto response = folly::to<std::string>(
      "HTTP/1.1 200 Ok\r\n"
      "Content-Length: 10\r\n"
      "\r\n"
      "aaaaaaaaaa0\r\n"
      "Trailer-1: trailer\r\n\r\n");
  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onHeadersComplete(1, _));
  EXPECT_CALL(callbacks, onBody(1, _, _));
  EXPECT_CALL(callbacks, onMessageComplete(1, _));
  // Trailers interpreted as another message, and error
  EXPECT_CALL(callbacks, onMessageBegin(2, _));
  EXPECT_CALL(callbacks, onError(2, _, _));

  codec.onIngress(*folly::IOBuf::wrapBuffer(response.data(), response.size()));
}

TEST(HTTP1xLlCodecTest, HeaderCtls) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM,
                      /*force1_1=*/true);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto buffer =
      folly::IOBuf::copyBuffer(string("GET / HTTP/1.1\r\n"
                                      "Foo: \"\\\r\\\n\"\r\n"
                                      "\r\n"));
  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onError(1, _, _))
      .WillOnce(Invoke(
          [&](HTTPCodec::StreamID, std::shared_ptr<HTTPException> error, bool) {
            EXPECT_EQ(error->getHttpStatusCode(), 400);
            EXPECT_THAT(error->what(), HasSubstr("[Context]=lf-expected"));
          }));
  codec.onIngress(*buffer);
}

TEST(HTTP1xLlCodecTest, HeaderCtlsMiddle) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM,
                      /*force1_1=*/true);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto buffer =
      folly::IOBuf::copyBuffer(string("GET / HTTP/1.1\r\n"
                                      "Foo: \"\\\r\\\n\"\r\n"
                                      "Bar: b\r\n"
                                      "\r\n"));
  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onError(1, _, _))
      .WillOnce(Invoke(
          [&](HTTPCodec::StreamID, std::shared_ptr<HTTPException> error, bool) {
            EXPECT_EQ(error->getHttpStatusCode(), 400);
            EXPECT_THAT(error->what(), HasSubstr("[Context]=lf-expected"));
          }));
  codec.onIngress(*buffer);
}

TEST(HTTP1xLlCodecTest, TrailerCtls) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM,
                      /*force1_1=*/true);
  MockHTTPCodecCallback callbacks;
  codec.setCallback(&callbacks);
  auto buffer =
      folly::IOBuf::copyBuffer(string("GET / HTTP/1.1\r\n"
                                      "Transfer-Encoding: chunked\r\n"
                                      "\r\n"
                                      "0\r\n"
                                      "Foo: \"\\\r\\\n\"\r\n"
                                      "\r\n"));
  EXPECT_CALL(callbacks, onMessageBegin(1, _));
  EXPECT_CALL(callbacks, onHeadersComplete(1, _));
  EXPECT_CALL(callbacks, onError(1, _, _))
      .WillOnce(Invoke(
          [&](HTTPCodec::StreamID, std::shared_ptr<HTTPException> error, bool) {
            EXPECT_EQ(error->getHttpStatusCode(), 400);
            EXPECT_THAT(error->what(), HasSubstr("[Context]=lf-expected"));
          }));
  codec.onIngress(*buffer);
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

class HTTP1xLlConnectionHeaderTest
    : public TestWithParam<std::pair<std::list<string>, string>> {
 public:
  using ParamType = std::pair<std::list<string>, string>;
};

TEST_P(HTTP1xLlConnectionHeaderTest, TestConnectionHeaders) {
  HTTP1xLlCodec upstream(TransportDirection::UPSTREAM);
  HTTP1xLlCodec downstream(TransportDirection::DOWNSTREAM);
  HTTP1xLlCodecCallback callbacks;
  downstream.setCallback(&callbacks);
  HTTPMessage req;
  req.setMethod(HTTPMethod::GET);
  req.setURL("/");
  auto val = GetParam();
  for (const auto& header : val.first) {
    req.getHeaders().add(HTTP_HEADER_CONNECTION, header);
  }
  folly::IOBufQueue writeBuf(folly::IOBufQueue::cacheChainLength());
  upstream.generateHeader(writeBuf, upstream.createStream(), req);
  downstream.onIngress(*writeBuf.front());
  EXPECT_EQ(callbacks.headersComplete, 1);
  auto& headers = callbacks.msg_->getHeaders();
  EXPECT_EQ(headers.getSingleOrEmpty(HTTP_HEADER_CONNECTION), val.second);
}

INSTANTIATE_TEST_SUITE_P(
    HTTP1xLlCodec,
    HTTP1xLlConnectionHeaderTest,
    ::testing::Values(
        // Moves close to the end
        HTTP1xLlConnectionHeaderTest::ParamType({"foo", "bar", "close", "baz"},
                                                "foo, bar, baz, close"),
        // has to resize token vector
        HTTP1xLlConnectionHeaderTest::ParamType({"foo", "bar, close", "baz"},
                                                "foo, bar, baz, close"),
        // whitespace trimming
        HTTP1xLlConnectionHeaderTest::ParamType(
            {" foo", "bar, close ", " baz "}, "foo, bar, baz, close"),
        // No close token => keep-alive
        HTTP1xLlConnectionHeaderTest::ParamType(
            {"foo", "bar, boo", "baz"}, "foo, bar, boo, baz, keep-alive"),
        // close and keep-alive => close
        HTTP1xLlConnectionHeaderTest::ParamType(
            {"foo", "keep-alive, boo", "close"}, "foo, boo, close"),
        // upgrade gets no special treatment
        HTTP1xLlConnectionHeaderTest::ParamType(
            {"foo", "upgrade, boo", "baz"},
            "foo, upgrade, boo, baz, keep-alive")));
