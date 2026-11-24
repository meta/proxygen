/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <proxygen/lib/http/codec/HTTPBinaryCodec.h>

#include <folly/portability/GTest.h>
#include <proxygen/lib/http/codec/test/TestUtils.h>
#include <quic/codec/QuicInteger.h>

namespace proxygen::test {

class HTTPBinaryCodecForTest : public HTTPBinaryCodec {
 public:
  explicit HTTPBinaryCodecForTest(TransportDirection direction)
      : HTTPBinaryCodec{direction} {
  }
  explicit HTTPBinaryCodecForTest(TransportDirection direction,
                                  bool knownLength)
      : HTTPBinaryCodec{direction, knownLength} {
  }
  ParseResult parseFramingIndicator(folly::io::Cursor& cursor,
                                    bool& request,
                                    bool& knownLength) {
    return HTTPBinaryCodec::parseFramingIndicator(cursor, request, knownLength);
  }

  ParseResult parseRequestControlData(folly::io::Cursor& cursor,
                                      size_t remaining,
                                      HTTPMessage& msg) {
    return HTTPBinaryCodec::parseRequestControlData(cursor, remaining, msg);
  }

  ParseResult parseResponseControlData(folly::io::Cursor& cursor,
                                       size_t remaining,
                                       HTTPMessage& msg) {
    return HTTPBinaryCodec::parseResponseControlData(cursor, remaining, msg);
  }

  ParseResult parseHeaders(folly::io::Cursor& cursor,
                           size_t remaining,
                           HeaderDecodeInfo& decodeInfo,
                           bool knownLength) {
    return HTTPBinaryCodec::parseHeaders(
        cursor, remaining, decodeInfo, knownLength);
  }

  ParseResult parseContent(folly::io::Cursor& cursor, size_t remaining) {
    return HTTPBinaryCodec::parseContent(cursor, remaining);
  }

  folly::IOBuf& getMsgBody() {
    return *msgBody_;
  }

  size_t getPendingFieldCount() const {
    return decodeInfo_.msg ? decodeInfo_.msg->getHeaders().size() : 0;
  }

  size_t getBufferedIngressSize() const {
    return bufferedIngress_.chainLength();
  }

  const std::string* getPendingFieldName() const {
    return pendingFieldName_ ? &*pendingFieldName_ : nullptr;
  }
};

namespace {

void writeVarint(folly::io::QueueAppender& appender, uint64_t value) {
  auto encoded = quic::encodeQuicInteger(value, [&](auto val) {
    appender.writeBE(folly::tag<decltype(val)>, val);
  });
  CHECK(encoded.has_value());
}

void writeVarintString(folly::io::QueueAppender& appender,
                       folly::StringPiece str) {
  writeVarint(appender, str.size());
  appender.pushAtMost(reinterpret_cast<const uint8_t*>(str.data()), str.size());
}

// Framing indicator plus request control data for `GET
// https://www.example.com/`
void writeRequestPreamble(folly::io::QueueAppender& appender,
                          uint64_t framingIndicator) {
  writeVarint(appender, framingIndicator);
  writeVarintString(appender, "GET");
  writeVarintString(appender, "https");
  writeVarintString(appender, "www.example.com");
  writeVarintString(appender, "/");
}

void writeOverrunningKnownLengthFieldSection(
    folly::io::QueueAppender& appender) {
  folly::IOBufQueue fieldSection{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender fieldAppender(&fieldSection, 64);
  writeVarintString(fieldAppender, "x-first");
  writeVarintString(fieldAppender, "first-value");
  writeVarintString(fieldAppender, "x-last");
  writeVarint(fieldAppender, 64);

  writeVarint(appender, fieldSection.chainLength());
  appender.insert(fieldSection.move());
}

std::unique_ptr<folly::IOBuf> makeFiller(size_t size) {
  auto buf = folly::IOBuf::create(size);
  buf->append(size);
  memset(buf->writableData(), 'a', size);
  return buf;
}

constexpr size_t kFillerChunkSize = size_t{64} * 1024;
constexpr size_t kFillerChunkCount = 16;
constexpr uint64_t kAbsurdDeclaredLength = uint64_t(1) << 40;

} // namespace

template <TransportDirection dir>
class HTTPBinaryCodecTest : public ::testing::Test {
 protected:
  void SetUp() override {
    binaryCodecKnownLength_ = std::make_unique<HTTPBinaryCodecForTest>(dir);
    binaryCodecIndeterminateLength_ =
        std::make_unique<HTTPBinaryCodecForTest>(dir, false /* knownLength */);
  }

  void TearDown() override {
  }

  std::unique_ptr<HTTPBinaryCodecForTest> binaryCodecKnownLength_;
  std::unique_ptr<HTTPBinaryCodecForTest> binaryCodecIndeterminateLength_;
};

class HttpBinaryUpstreamCodecTest
    : public HTTPBinaryCodecTest<TransportDirection::UPSTREAM> {};

class HttpBinaryDownstreamCodecTest
    : public HTTPBinaryCodecTest<TransportDirection::DOWNSTREAM> {};

TEST_F(HttpBinaryUpstreamCodecTest, testParseFramingIndicatorSuccess) {
  // Test Known Length Request
  const std::vector<uint8_t> framingIndicatorKnownRequest{0x00};
  auto framingIndicatorIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(framingIndicatorKnownRequest.data(),
                       framingIndicatorKnownRequest.size()));
  folly::io::Cursor cursor(framingIndicatorIOBuf.get());

  bool request = false;
  bool knownLength = false;
  EXPECT_EQ(binaryCodecKnownLength_
                ->parseFramingIndicator(cursor, request, knownLength)
                .bytesParsed_,
            1);
  EXPECT_EQ(request, true);
  EXPECT_EQ(knownLength, true);

  // Test Indeterminate Length Response
  const std::vector<uint8_t> framingIndicatorIndeterminateResponse{0x03};
  framingIndicatorIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(framingIndicatorIndeterminateResponse.data(),
                       framingIndicatorIndeterminateResponse.size()));
  cursor = folly::io::Cursor(framingIndicatorIOBuf.get());

  EXPECT_EQ(binaryCodecKnownLength_
                ->parseFramingIndicator(cursor, request, knownLength)
                .bytesParsed_,
            1);
  EXPECT_EQ(request, false);
  EXPECT_EQ(knownLength, false);
}

TEST_F(HttpBinaryUpstreamCodecTest, testParseFramingIndicatorFailure) {
  // Test Invalid Framing Indicator
  const std::vector<uint8_t> framingIndicatorInvalidResponse{0x04};
  auto framingIndicatorIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(framingIndicatorInvalidResponse.data(),
                       framingIndicatorInvalidResponse.size()));
  folly::io::Cursor cursor(framingIndicatorIOBuf.get());

  bool request = false;
  bool knownLength = false;
  EXPECT_EQ(binaryCodecKnownLength_
                ->parseFramingIndicator(cursor, request, knownLength)
                .error_,
            "Invalid Framing Indicator: 4");
}

TEST_F(HttpBinaryUpstreamCodecTest, testParseRequestControlDataSuccess) {
  // Format is `*GET*https*www.example.com*/hello.txt` where `*` represents
  // the length of each subsequent string
  const std::vector<uint8_t> controlDataRequest{
      0x03, 0x47, 0x45, 0x54, 0x05, 0x68, 0x74, 0x74, 0x70, 0x73,
      0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70,
      0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x0a, 0x2f, 0x68, 0x65,
      0x6c, 0x6c, 0x6f, 0x2e, 0x74, 0x78, 0x74};
  auto controlDataIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(controlDataRequest.data(), controlDataRequest.size()));
  folly::io::Cursor cursor(controlDataIOBuf.get());

  HTTPMessage msg;
  EXPECT_EQ(
      binaryCodecKnownLength_
          ->parseRequestControlData(cursor, controlDataRequest.size(), msg)
          .bytesParsed_,
      controlDataRequest.size());
  EXPECT_EQ(msg.isSecure(), true);
  EXPECT_EQ(msg.getMethod(), proxygen::HTTPMethod::GET);
  EXPECT_EQ(msg.getURL(), "/hello.txt");
}

TEST_F(HttpBinaryUpstreamCodecTest, testParseRequestControlDataWaiting) {
  // Format is `*GET*https*www.example.com*/hello.txt` where `*` before
  // /hello.txt is value 11 instead of value 10, which should cause the parsing
  // to wait for more data
  const std::vector<uint8_t> controlDataInvalidRequest{
      0x03, 0x47, 0x45, 0x54, 0x05, 0x68, 0x74, 0x74, 0x70, 0x73,
      0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70,
      0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x0b, 0x2f, 0x68, 0x65,
      0x6c, 0x6c, 0x6f, 0x2e, 0x74, 0x78, 0x74};
  auto controlDataIOBuf = folly::IOBuf::wrapBuffer(folly::ByteRange(
      controlDataInvalidRequest.data(), controlDataInvalidRequest.size()));
  folly::io::Cursor cursor(controlDataIOBuf.get());

  HTTPMessage msg;
  EXPECT_EQ(binaryCodecKnownLength_
                ->parseRequestControlData(
                    cursor, controlDataInvalidRequest.size(), msg)
                .parseResultState_,
            ParseResultState::WAITING_FOR_MORE_DATA);
}

TEST_F(HttpBinaryUpstreamCodecTest, testParseRequestControlDataFailure) {
  // Format is `*GET*http*www.example.com*/hello.txt` where `*` represents the
  // length of each subsequent string.
  const std::vector<uint8_t> controlDataInvalidScheme{
      0x03, 0x47, 0x45, 0x54, 0x05, 0x68, 0x74, 0x74, 0x70, 0x74,
      0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70,
      0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x0a, 0x2f, 0x68, 0x65,
      0x6c, 0x6c, 0x6f, 0x2e, 0x74, 0x78, 0x74};
  auto controlDataIOBuf = folly::IOBuf::wrapBuffer(folly::ByteRange(
      controlDataInvalidScheme.data(), controlDataInvalidScheme.size()));
  auto cursor = folly::io::Cursor(controlDataIOBuf.get());

  HTTPMessage msg;
  EXPECT_EQ(binaryCodecKnownLength_
                ->parseRequestControlData(
                    cursor, controlDataInvalidScheme.size(), msg)
                .error_,
            "Failure to parse: scheme. Should be 'http' or 'https'");

  // Format is `*GET*https*www.example.com*hello.tx[\x1]` where `*` represents
  // the length of each subsequent string.
  const std::vector<uint8_t> controlDataInvalidPath{
      0x03, 0x47, 0x45, 0x54, 0x05, 0x68, 0x74, 0x74, 0x70, 0x73, 0x0f, 0x77,
      0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x63,
      0x6f, 0x6d, 0x09, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x2e, 0x74, 0x78, 0x01};
  controlDataIOBuf = folly::IOBuf::wrapBuffer(folly::ByteRange(
      controlDataInvalidPath.data(), controlDataInvalidPath.size()));
  cursor = folly::io::Cursor(controlDataIOBuf.get());

  EXPECT_EQ(
      binaryCodecKnownLength_
          ->parseRequestControlData(cursor, controlDataInvalidPath.size(), msg)
          .error_,
      "Failure to parse: invalid URL path 'hello.tx\x1'");
}

// The path is echoed into the parse error, which ends up in logs, and the query
// is where a URL carries its credentials, so the error stops short of it.
TEST_F(HttpBinaryUpstreamCodecTest, testParseRequestControlDataOmitsQuery) {
  std::string controlData;
  for (std::string_view field :
       {"GET",
        "https",
        "www.example.com",
        "/hello.txt?auth_token=TEST_ONLY_NOT_A_TOKEN\x01"}) {
    // Every field is shorter than 64 bytes, so its varint length is one byte.
    controlData.push_back(static_cast<char>(field.size()));
    controlData.append(field);
  }
  auto controlDataIOBuf = folly::IOBuf::copyBuffer(controlData);
  folly::io::Cursor cursor(controlDataIOBuf.get());

  HTTPMessage msg;
  EXPECT_EQ(binaryCodecKnownLength_
                ->parseRequestControlData(cursor, controlData.size(), msg)
                .error_,
            "Failure to parse: invalid URL path '/hello.txt'");
}

TEST_F(HttpBinaryUpstreamCodecTest, testParseResponseControlDataSuccess) {
  // Reponse Code 200 OK
  folly::IOBufQueue controlDataIOBuf;
  folly::io::QueueAppender appender(&controlDataIOBuf, 0);
  auto parsedBytes = quic::encodeQuicInteger(200, [&appender](auto val) {
                       appender.writeBE(folly::tag<decltype(val)>, val);
                     }).value();
  folly::io::Cursor cursor(controlDataIOBuf.front());

  HTTPMessage msg;
  EXPECT_EQ(binaryCodecKnownLength_
                ->parseResponseControlData(cursor, parsedBytes, msg)
                .bytesParsed_,
            parsedBytes);
  EXPECT_EQ(msg.getStatusCode(), 200);
}

TEST_F(HttpBinaryUpstreamCodecTest, testParseResponseControlDataFailure) {
  // Invalid Status Code
  folly::IOBufQueue controlInvalidDataIOBuf;
  folly::io::QueueAppender appender(&controlInvalidDataIOBuf, 0);
  auto parsedBytes = quic::encodeQuicInteger(600, [&appender](auto val) {
                       appender.writeBE(folly::tag<decltype(val)>, val);
                     }).value();
  folly::io::Cursor cursor(controlInvalidDataIOBuf.front());

  HTTPMessage msg;
  EXPECT_EQ(binaryCodecKnownLength_
                ->parseResponseControlData(cursor, parsedBytes, msg)
                .error_,
            "Invalid response status code: 600");

  folly::IOBufQueue controlInvalidDataInformationalResponseIOBuf;
  appender = folly::io::QueueAppender(
      &controlInvalidDataInformationalResponseIOBuf, 0);
  parsedBytes = quic::encodeQuicInteger(101, [&appender](auto val) {
                  appender.writeBE(folly::tag<decltype(val)>, val);
                }).value();
  cursor =
      folly::io::Cursor(controlInvalidDataInformationalResponseIOBuf.front());

  EXPECT_EQ(binaryCodecKnownLength_
                ->parseResponseControlData(cursor, parsedBytes, msg)
                .error_,
            "Invalid response status code: 101");
}

TEST_F(HttpBinaryUpstreamCodecTest, testParseKnownLengthHeadersSuccess) {
  // Format is `**user-agent*curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l
  // zlib/1.2.3*host*www.example.com*accept-language*en, mi` where `*`
  // represents the length of the each subsequent string. The first `*` is
  // actually a Quic Integer that takes 2 bytes (and encodes the length of the
  // overall header as 108)
  const std::vector<uint8_t> headers{
      0x40, 0x6c, 0x0a, 0x75, 0x73, 0x65, 0x72, 0x2d, 0x61, 0x67, 0x65,
      0x6e, 0x74, 0x34, 0x63, 0x75, 0x72, 0x6c, 0x2f, 0x37, 0x2e, 0x31,
      0x36, 0x2e, 0x33, 0x20, 0x6c, 0x69, 0x62, 0x63, 0x75, 0x72, 0x6c,
      0x2f, 0x37, 0x2e, 0x31, 0x36, 0x2e, 0x33, 0x20, 0x4f, 0x70, 0x65,
      0x6e, 0x53, 0x53, 0x4c, 0x2f, 0x30, 0x2e, 0x39, 0x2e, 0x37, 0x6c,
      0x20, 0x7a, 0x6c, 0x69, 0x62, 0x2f, 0x31, 0x2e, 0x32, 0x2e, 0x33,
      0x04, 0x68, 0x6f, 0x73, 0x74, 0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65,
      0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x0f,
      0x61, 0x63, 0x63, 0x65, 0x70, 0x74, 0x2d, 0x6c, 0x61, 0x6e, 0x67,
      0x75, 0x61, 0x67, 0x65, 0x06, 0x65, 0x6e, 0x2c, 0x20, 0x6d, 0x69};
  auto headersIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(headers.data(), headers.size()));
  folly::io::Cursor cursor(headersIOBuf.get());

  HeaderDecodeInfo decodeInfo;
  decodeInfo.init(true /* request */,
                  false /* isRequestTrailers */,
                  true /* validate */,
                  false /* strictValidation */,
                  false /* allowEmptyPath */);
  EXPECT_EQ(binaryCodecKnownLength_
                ->parseHeaders(
                    cursor, headers.size(), decodeInfo, /*knownLength=*/true)
                .bytesParsed_,
            headers.size());

  HTTPHeaders httpHeaders = decodeInfo.msg->getHeaders();
  EXPECT_EQ(httpHeaders.exists("user-agent"), true);
  EXPECT_EQ(httpHeaders.exists("host"), true);
  EXPECT_EQ(httpHeaders.exists("accept-language"), true);
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("user-agent"),
            "curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3");
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("host"), "www.example.com");
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("accept-language"), "en, mi");
}

TEST_F(HttpBinaryUpstreamCodecTest, testParseKnownLengthHeadersEmpty) {
  const std::vector<uint8_t> invalidHeadersCount{0x00};
  auto headersIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(invalidHeadersCount.data(), invalidHeadersCount.size()));
  folly::io::Cursor cursor(headersIOBuf.get());

  HeaderDecodeInfo decodeInfo;
  decodeInfo.init(true /* request */,
                  false /* isRequestTrailers */,
                  true /* validate */,
                  false /* strictValidation */,
                  false /* allowEmptyPath */);
  EXPECT_EQ(binaryCodecKnownLength_
                ->parseHeaders(cursor, 1, decodeInfo, /*knownLength=*/true)
                .bytesParsed_,
            1);
}
TEST_F(HttpBinaryUpstreamCodecTest,
       testParseKnownLengthHeadersOverrunsDeclaredSection) {
  // Format is `**user-agent*curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l
  // zlib/1.2.3*host*www.example.com*accept-language.en, mi` where the `*` after
  // accept-language is value 7 instead of 6, which exceeds the declared
  // section boundary.
  const std::vector<uint8_t> invalidHeadersLength{
      0x40, 0x6c, 0x0a, 0x75, 0x73, 0x65, 0x72, 0x2d, 0x61, 0x67, 0x65,
      0x6e, 0x74, 0x34, 0x63, 0x75, 0x72, 0x6c, 0x2f, 0x37, 0x2e, 0x31,
      0x36, 0x2e, 0x33, 0x20, 0x6c, 0x69, 0x62, 0x63, 0x75, 0x72, 0x6c,
      0x2f, 0x37, 0x2e, 0x31, 0x36, 0x2e, 0x33, 0x20, 0x4f, 0x70, 0x65,
      0x6e, 0x53, 0x53, 0x4c, 0x2f, 0x30, 0x2e, 0x39, 0x2e, 0x37, 0x6c,
      0x20, 0x7a, 0x6c, 0x69, 0x62, 0x2f, 0x31, 0x2e, 0x32, 0x2e, 0x33,
      0x04, 0x68, 0x6f, 0x73, 0x74, 0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65,
      0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x0f,
      0x61, 0x63, 0x63, 0x65, 0x70, 0x74, 0x2d, 0x6c, 0x61, 0x6e, 0x67,
      0x75, 0x61, 0x67, 0x65, 0x07, 0x65, 0x6e, 0x2c, 0x20, 0x6d, 0x69};
  auto headersIOBuf = folly::IOBuf::wrapBuffer(folly::ByteRange(
      invalidHeadersLength.data(), invalidHeadersLength.size()));
  auto cursor = folly::io::Cursor(headersIOBuf.get());

  HeaderDecodeInfo decodeInfo;
  decodeInfo.init(true /* request */,
                  false /* isRequestTrailers */,
                  true /* validate */,
                  false /* strictValidation */,
                  false /* allowEmptyPath */);
  EXPECT_EQ(binaryCodecKnownLength_
                ->parseHeaders(cursor,
                               invalidHeadersLength.size(),
                               decodeInfo,
                               /*knownLength=*/true)
                .parseResultState_,
            ParseResultState::ERROR);

  // Format is `**a*b` where the first `*` represents a too long length
  const std::vector<uint8_t> invalidHeadersUnderflow{
      0x09, 0x01, 0x61, 0x01, 0x62};
  headersIOBuf = folly::IOBuf::wrapBuffer(folly::ByteRange(
      invalidHeadersUnderflow.data(), invalidHeadersUnderflow.size()));
  cursor = folly::io::Cursor(headersIOBuf.get());

  EXPECT_EQ(binaryCodecKnownLength_
                ->parseHeaders(cursor,
                               invalidHeadersUnderflow.size(),
                               decodeInfo,
                               /*knownLength=*/true)
                .parseResultState_,
            ParseResultState::WAITING_FOR_MORE_DATA);

  // Format is `*` where the first `*` represents an underflowed quic integer
  const std::vector<uint8_t> invalidHeadersUnderflowQuic{0x99};
  headersIOBuf = folly::IOBuf::wrapBuffer(folly::ByteRange(
      invalidHeadersUnderflowQuic.data(), invalidHeadersUnderflowQuic.size()));
  cursor = folly::io::Cursor(headersIOBuf.get());

  EXPECT_EQ(binaryCodecKnownLength_
                ->parseHeaders(cursor,
                               invalidHeadersUnderflowQuic.size(),
                               decodeInfo,
                               /*knownLength=*/true)
                .parseResultState_,
            ParseResultState::WAITING_FOR_MORE_DATA);
}

TEST_F(HttpBinaryUpstreamCodecTest,
       testParseIndeterminateLengthHeadersSuccess) {
  // Format is `*user-agent*curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l
  // zlib/1.2.3*host*www.example.com*accept-language*en, mi0` where `*`
  // represents the length of the each subsequent string. The stream is
  // terminated by a 0 byte
  const std::vector<uint8_t> headers{
      0x0a, 0x75, 0x73, 0x65, 0x72, 0x2d, 0x61, 0x67, 0x65, 0x6e, 0x74,
      0x34, 0x63, 0x75, 0x72, 0x6c, 0x2f, 0x37, 0x2e, 0x31, 0x36, 0x2e,
      0x33, 0x20, 0x6c, 0x69, 0x62, 0x63, 0x75, 0x72, 0x6c, 0x2f, 0x37,
      0x2e, 0x31, 0x36, 0x2e, 0x33, 0x20, 0x4f, 0x70, 0x65, 0x6e, 0x53,
      0x53, 0x4c, 0x2f, 0x30, 0x2e, 0x39, 0x2e, 0x37, 0x6c, 0x20, 0x7a,
      0x6c, 0x69, 0x62, 0x2f, 0x31, 0x2e, 0x32, 0x2e, 0x33, 0x04, 0x68,
      0x6f, 0x73, 0x74, 0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61,
      0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x0f, 0x61, 0x63,
      0x63, 0x65, 0x70, 0x74, 0x2d, 0x6c, 0x61, 0x6e, 0x67, 0x75, 0x61,
      0x67, 0x65, 0x06, 0x65, 0x6e, 0x2c, 0x20, 0x6d, 0x69, 0x00};
  auto headersIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(headers.data(), headers.size()));
  folly::io::Cursor cursor(headersIOBuf.get());

  HeaderDecodeInfo decodeInfo;
  decodeInfo.init(true /* request */,
                  false /* isRequestTrailers */,
                  true /* validate */,
                  false /* strictValidation */,
                  false /* allowEmptyPath */);
  EXPECT_EQ(binaryCodecIndeterminateLength_
                ->parseHeaders(
                    cursor, headers.size(), decodeInfo, /*knownLength=*/false)
                .bytesParsed_,
            headers.size());

  HTTPHeaders httpHeaders = decodeInfo.msg->getHeaders();
  EXPECT_EQ(httpHeaders.exists("user-agent"), true);
  EXPECT_EQ(httpHeaders.exists("host"), true);
  EXPECT_EQ(httpHeaders.exists("accept-language"), true);
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("user-agent"),
            "curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3");
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("host"), "www.example.com");
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("accept-language"), "en, mi");
}

TEST_F(HttpBinaryUpstreamCodecTest, testParseIndeterminateLengthHeadersEmpty) {
  const std::vector<uint8_t> invalidHeadersCount{0x00};
  auto headersIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(invalidHeadersCount.data(), invalidHeadersCount.size()));
  folly::io::Cursor cursor(headersIOBuf.get());

  HeaderDecodeInfo decodeInfo;
  decodeInfo.init(true /* request */,
                  false /* isRequestTrailers */,
                  true /* validate */,
                  false /* strictValidation */,
                  false /* allowEmptyPath */);
  EXPECT_EQ(binaryCodecIndeterminateLength_
                ->parseHeaders(cursor, 1, decodeInfo, /*knownLength=*/false)
                .bytesParsed_,
            1);
  HTTPHeaders httpHeaders = decodeInfo.msg->getHeaders();
  EXPECT_EQ(httpHeaders.size(), 0);
}

TEST_F(HttpBinaryUpstreamCodecTest,
       testParseIndeterminateLengthHeadersWaiting) {
  // Format is `*user-agent*curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l
  // zlib/1.2.3*host*www.example.com*accept-language*en, mi0` where the
  // `*` after accept-language is value 7 instead of 6 which should cause
  // parsing to wait to get more data
  const std::vector<uint8_t> invalidHeadersLength{
      0x0a, 0x75, 0x73, 0x65, 0x72, 0x2d, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x34,
      0x63, 0x75, 0x72, 0x6c, 0x2f, 0x37, 0x2e, 0x31, 0x36, 0x2e, 0x33, 0x20,
      0x6c, 0x69, 0x62, 0x63, 0x75, 0x72, 0x6c, 0x2f, 0x37, 0x2e, 0x31, 0x36,
      0x2e, 0x33, 0x20, 0x4f, 0x70, 0x65, 0x6e, 0x53, 0x53, 0x4c, 0x2f, 0x30,
      0x2e, 0x39, 0x2e, 0x37, 0x6c, 0x20, 0x7a, 0x6c, 0x69, 0x62, 0x2f, 0x31,
      0x2e, 0x32, 0x2e, 0x33, 0x04, 0x68, 0x6f, 0x73, 0x74, 0x0f, 0x77, 0x77,
      0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x63, 0x6f,
      0x6d, 0x0f, 0x61, 0x63, 0x63, 0x65, 0x70, 0x74, 0x2d, 0x6c, 0x61, 0x6e,
      0x67, 0x75, 0x61, 0x67, 0x65, 0x07, 0x65, 0x6e, 0x2c, 0x20, 0x6d, 0x69};
  auto headersIOBuf = folly::IOBuf::wrapBuffer(folly::ByteRange(
      invalidHeadersLength.data(), invalidHeadersLength.size()));
  auto cursor = folly::io::Cursor(headersIOBuf.get());

  HeaderDecodeInfo decodeInfo;
  decodeInfo.init(true /* request */,
                  false /* isRequestTrailers */,
                  true /* validate */,
                  false /* strictValidation */,
                  false /* allowEmptyPath */);
  EXPECT_EQ(binaryCodecIndeterminateLength_
                ->parseHeaders(cursor,
                               invalidHeadersLength.size(),
                               decodeInfo,
                               /*knownLength=*/false)
                .parseResultState_,
            ParseResultState::WAITING_FOR_MORE_DATA);

  // Format is `.a.b` where we don't have a 0 byte at the end
  const std::vector<uint8_t> invalidHeadersUnderflow{0x01, 0x61, 0x01, 0x62};
  headersIOBuf = folly::IOBuf::wrapBuffer(folly::ByteRange(
      invalidHeadersUnderflow.data(), invalidHeadersUnderflow.size()));
  cursor = folly::io::Cursor(headersIOBuf.get());

  EXPECT_EQ(binaryCodecIndeterminateLength_
                ->parseHeaders(cursor,
                               invalidHeadersUnderflow.size(),
                               decodeInfo,
                               /*knownLength=*/false)
                .parseResultState_,
            ParseResultState::WAITING_FOR_MORE_DATA);
}

TEST_F(HttpBinaryUpstreamCodecTest, testParseContentKnownLengthSuccess) {
  // Format is `*hello`
  const std::vector<uint8_t> content{
      0x07, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0d, 0x0a};
  auto contentIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(content.data(), content.size()));
  folly::io::Cursor cursor(contentIOBuf.get());

  HTTPMessage msg;
  EXPECT_EQ(binaryCodecKnownLength_->parseContent(cursor, content.size())
                .bytesParsed_,
            content.size());
  EXPECT_EQ(binaryCodecKnownLength_->getMsgBody().to<std::string>(),
            "hello\r\n");
}

TEST_F(HttpBinaryUpstreamCodecTest, testParseContentKnownLengthFailure) {
  // Format is `*hello` where * is value 8 instead of 7
  const std::vector<uint8_t> contentInvalid{
      0x08, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0d, 0x0a};
  auto contentIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(contentInvalid.data(), contentInvalid.size()));
  folly::io::Cursor cursor(contentIOBuf.get());

  HTTPMessage msg;
  EXPECT_EQ(binaryCodecKnownLength_->parseContent(cursor, contentInvalid.size())
                .parseResultState_,
            ParseResultState::WAITING_FOR_MORE_DATA);
}

TEST_F(HttpBinaryUpstreamCodecTest,
       testParseContentIndeterminateLengthSuccess) {
  // Format is `*hello0`
  const std::vector<uint8_t> content{
      0x07, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0d, 0x0a, 0x00};
  auto contentIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(content.data(), content.size()));
  folly::io::Cursor cursor(contentIOBuf.get());

  HTTPMessage msg;
  EXPECT_EQ(
      binaryCodecIndeterminateLength_->parseContent(cursor, content.size())
          .bytesParsed_,
      content.size());
  EXPECT_EQ(binaryCodecIndeterminateLength_->getMsgBody().to<std::string>(),
            "hello\r\n");
}

TEST_F(HttpBinaryUpstreamCodecTest,
       testParseContentIndeterminateLengthWaiting) {
  // Format is `*hello` where * is value 8 instead of 7
  const std::vector<uint8_t> contentInvalid{
      0x08, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0d, 0x0a};
  auto contentIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(contentInvalid.data(), contentInvalid.size()));
  folly::io::Cursor cursor(contentIOBuf.get());

  HTTPMessage msg;
  EXPECT_EQ(binaryCodecIndeterminateLength_
                ->parseContent(cursor, contentInvalid.size())
                .parseResultState_,
            ParseResultState::WAITING_FOR_MORE_DATA);
}

TEST_F(HttpBinaryDownstreamCodecTest, testOnIngressSuccess) {
  // Format is `**GET*https.www.example.com*/hello.txt**user-agent*curl/7.16.3
  // libcurl/7.16.3 OpenSSL/0.9.7l
  // zlib/1.2.3*host*www.example.com*accept-language.en, mi`
  const std::vector<uint8_t> binaryHTTPMessage{
      0x00, 0x03, 0x47, 0x45, 0x54, 0x05, 0x68, 0x74, 0x74, 0x70, 0x73, 0x0f,
      0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e,
      0x63, 0x6f, 0x6d, 0x0a, 0x2f, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x2e, 0x74,
      0x78, 0x74, 0x40, 0x6c, 0x0a, 0x75, 0x73, 0x65, 0x72, 0x2d, 0x61, 0x67,
      0x65, 0x6e, 0x74, 0x34, 0x63, 0x75, 0x72, 0x6c, 0x2f, 0x37, 0x2e, 0x31,
      0x36, 0x2e, 0x33, 0x20, 0x6c, 0x69, 0x62, 0x63, 0x75, 0x72, 0x6c, 0x2f,
      0x37, 0x2e, 0x31, 0x36, 0x2e, 0x33, 0x20, 0x4f, 0x70, 0x65, 0x6e, 0x53,
      0x53, 0x4c, 0x2f, 0x30, 0x2e, 0x39, 0x2e, 0x37, 0x6c, 0x20, 0x7a, 0x6c,
      0x69, 0x62, 0x2f, 0x31, 0x2e, 0x32, 0x2e, 0x33, 0x04, 0x68, 0x6f, 0x73,
      0x74, 0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c,
      0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x0f, 0x61, 0x63, 0x63, 0x65, 0x70, 0x74,
      0x2d, 0x6c, 0x61, 0x6e, 0x67, 0x75, 0x61, 0x67, 0x65, 0x06, 0x65, 0x6e,
      0x2c, 0x20, 0x6d, 0x69, 0x00, 0x00};
  auto binaryHTTPMessageIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(binaryHTTPMessage.data(), binaryHTTPMessage.size()));
  folly::io::Cursor cursor(binaryHTTPMessageIOBuf.get());

  FakeHTTPCodecCallback callback;
  binaryCodecKnownLength_->setCallback(&callback);
  binaryCodecKnownLength_->onIngress(*binaryHTTPMessageIOBuf);
  binaryCodecKnownLength_->onIngressEOF();

  // Check onError was not called for the callback
  EXPECT_EQ(callback.lastParseError, nullptr);

  // Check msg and header fields
  EXPECT_EQ(callback.msg->isSecure(), true);
  EXPECT_EQ(callback.msg->getMethod(), proxygen::HTTPMethod::GET);
  EXPECT_EQ(callback.msg->getURL(), "/hello.txt");
  HTTPHeaders httpHeaders = callback.msg->getHeaders();
  EXPECT_EQ(httpHeaders.exists("user-agent"), true);
  EXPECT_EQ(httpHeaders.exists("host"), true);
  EXPECT_EQ(httpHeaders.exists("accept-language"), true);
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("user-agent"),
            "curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3");
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("host"), "www.example.com");
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("accept-language"), "en, mi");
}

TEST_F(HttpBinaryDownstreamCodecTest, testOnIngressSuccessForControlData) {
  // Format is `**GET*https*www.example.com*/`
  const std::vector<uint8_t> binaryHTTPMessage{
      0x00, 0x03, 0x47, 0x45, 0x54, 0x05, 0x68, 0x74, 0x74,
      0x70, 0x73, 0x0b, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c,
      0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x01, 0x2f};
  auto binaryHTTPMessageIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(binaryHTTPMessage.data(), binaryHTTPMessage.size()));
  folly::io::Cursor cursor(binaryHTTPMessageIOBuf.get());

  FakeHTTPCodecCallback callback;
  binaryCodecKnownLength_->setCallback(&callback);
  binaryCodecKnownLength_->onIngress(*binaryHTTPMessageIOBuf);
  binaryCodecKnownLength_->onIngressEOF();

  // Check onError was not called for the callback
  EXPECT_EQ(callback.lastParseError, nullptr);

  // Check msg and header fields
  EXPECT_EQ(callback.msg->isSecure(), true);
  EXPECT_EQ(callback.msg->getMethod(), proxygen::HTTPMethod::GET);
  EXPECT_EQ(callback.msg->getURL(), "/");
}

TEST_F(HttpBinaryDownstreamCodecTest,
       testOnIngressSuccessChunkedOnBoundaryMessage) {
  // Format is chunk1 = `**GET`, chunk2 =
  // `*https*www.example.com*/hello.txt**user-agent*curl/7.16.3 libcurl/7.16.3
  // OpenSSL/0.9.7l zlib/1.2.3.host*www.example.com*accept-language*en, mi`
  const std::vector<uint8_t> binaryHTTPMessageChunk1{
      0x00, 0x03, 0x47, 0x45, 0x54, 0x05, 0x68, 0x74, 0x74, 0x70,
      0x73, 0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d,
      0x70, 0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x0a, 0x2f, 0x68,
      0x65, 0x6c, 0x6c, 0x6f, 0x2e, 0x74, 0x78, 0x74};
  const std::vector<uint8_t> binaryHTTPMessageChunk2{
      0x40, 0x6c, 0x0a, 0x75, 0x73, 0x65, 0x72, 0x2d, 0x61, 0x67, 0x65, 0x6e,
      0x74, 0x34, 0x63, 0x75, 0x72, 0x6c, 0x2f, 0x37, 0x2e, 0x31, 0x36, 0x2e,
      0x33, 0x20, 0x6c, 0x69, 0x62, 0x63, 0x75, 0x72, 0x6c, 0x2f, 0x37, 0x2e,
      0x31, 0x36, 0x2e, 0x33, 0x20, 0x4f, 0x70, 0x65, 0x6e, 0x53, 0x53, 0x4c,
      0x2f, 0x30, 0x2e, 0x39, 0x2e, 0x37, 0x6c, 0x20, 0x7a, 0x6c, 0x69, 0x62,
      0x2f, 0x31, 0x2e, 0x32, 0x2e, 0x33, 0x04, 0x68, 0x6f, 0x73, 0x74, 0x0f,
      0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e,
      0x63, 0x6f, 0x6d, 0x0f, 0x61, 0x63, 0x63, 0x65, 0x70, 0x74, 0x2d, 0x6c,
      0x61, 0x6e, 0x67, 0x75, 0x61, 0x67, 0x65, 0x06, 0x65, 0x6e, 0x2c, 0x20,
      0x6d, 0x69, 0x00, 0x00};

  auto binaryHTTPMessageIOBufChunk1 = folly::IOBuf::wrapBuffer(folly::ByteRange(
      binaryHTTPMessageChunk1.data(), binaryHTTPMessageChunk1.size()));
  folly::io::Cursor cursor1(binaryHTTPMessageIOBufChunk1.get());
  auto binaryHTTPMessageIOBufChunk2 = folly::IOBuf::wrapBuffer(folly::ByteRange(
      binaryHTTPMessageChunk2.data(), binaryHTTPMessageChunk2.size()));
  folly::io::Cursor cursor2(binaryHTTPMessageIOBufChunk2.get());

  FakeHTTPCodecCallback callback;
  binaryCodecKnownLength_->setCallback(&callback);
  binaryCodecKnownLength_->onIngress(*binaryHTTPMessageIOBufChunk1);
  binaryCodecKnownLength_->onIngress(*binaryHTTPMessageIOBufChunk2);
  binaryCodecKnownLength_->onIngressEOF();

  // Check onError was not called for the callback
  EXPECT_EQ(callback.lastParseError, nullptr);

  // Check msg and header fields
  EXPECT_EQ(callback.msg->isSecure(), true);
  EXPECT_EQ(callback.msg->getMethod(), proxygen::HTTPMethod::GET);
  EXPECT_EQ(callback.msg->getURL(), "/hello.txt");
  HTTPHeaders httpHeaders = callback.msg->getHeaders();
  EXPECT_EQ(httpHeaders.exists("user-agent"), true);
  EXPECT_EQ(httpHeaders.exists("host"), true);
  EXPECT_EQ(httpHeaders.exists("accept-language"), true);
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("user-agent"),
            "curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3");
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("host"), "www.example.com");
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("accept-language"), "en, mi");
}

TEST_F(HttpBinaryDownstreamCodecTest, testOnIngressSuccessOneByteChunks) {
  // Format is `**GET*https.www.example.com*/hello.txt**user-agent*curl/7.16.3
  // libcurl/7.16.3 OpenSSL/0.9.7l
  // zlib/1.2.3*host*www.example.com*accept-language*en, mi`
  const std::vector<uint8_t> binaryHTTPMessage{
      0x00, 0x03, 0x47, 0x45, 0x54, 0x05, 0x68, 0x74, 0x74, 0x70, 0x73, 0x0f,
      0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e,
      0x63, 0x6f, 0x6d, 0x0a, 0x2f, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x2e, 0x74,
      0x78, 0x74, 0x40, 0x6c, 0x0a, 0x75, 0x73, 0x65, 0x72, 0x2d, 0x61, 0x67,
      0x65, 0x6e, 0x74, 0x34, 0x63, 0x75, 0x72, 0x6c, 0x2f, 0x37, 0x2e, 0x31,
      0x36, 0x2e, 0x33, 0x20, 0x6c, 0x69, 0x62, 0x63, 0x75, 0x72, 0x6c, 0x2f,
      0x37, 0x2e, 0x31, 0x36, 0x2e, 0x33, 0x20, 0x4f, 0x70, 0x65, 0x6e, 0x53,
      0x53, 0x4c, 0x2f, 0x30, 0x2e, 0x39, 0x2e, 0x37, 0x6c, 0x20, 0x7a, 0x6c,
      0x69, 0x62, 0x2f, 0x31, 0x2e, 0x32, 0x2e, 0x33, 0x04, 0x68, 0x6f, 0x73,
      0x74, 0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c,
      0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x0f, 0x61, 0x63, 0x63, 0x65, 0x70, 0x74,
      0x2d, 0x6c, 0x61, 0x6e, 0x67, 0x75, 0x61, 0x67, 0x65, 0x06, 0x65, 0x6e,
      0x2c, 0x20, 0x6d, 0x69, 0x00, 0x00};
  auto binaryHTTPMessageIOBuf = folly::IOBuf::wrapBuffer(
      folly::ByteRange(binaryHTTPMessage.data(), binaryHTTPMessage.size()));
  folly::io::Cursor cursor(binaryHTTPMessageIOBuf.get());

  FakeHTTPCodecCallback callback;
  binaryCodecKnownLength_->setCallback(&callback);
  for (auto& byte : binaryHTTPMessage) {
    auto stp = folly::IOBuf::wrapBuffer(folly::ByteRange(&byte, 1));
    binaryCodecKnownLength_->onIngress(*stp);
  }
  binaryCodecKnownLength_->onIngressEOF();

  // Check onError was not called for the callback
  EXPECT_EQ(callback.lastParseError, nullptr);

  // Check msg and header fields
  EXPECT_EQ(callback.msg->isSecure(), true);
  EXPECT_EQ(callback.msg->getMethod(), proxygen::HTTPMethod::GET);
  EXPECT_EQ(callback.msg->getURL(), "/hello.txt");
  HTTPHeaders httpHeaders = callback.msg->getHeaders();
  EXPECT_EQ(httpHeaders.exists("user-agent"), true);
  EXPECT_EQ(httpHeaders.exists("host"), true);
  EXPECT_EQ(httpHeaders.exists("accept-language"), true);
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("user-agent"),
            "curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3");
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("host"), "www.example.com");
  EXPECT_EQ(httpHeaders.getSingleOrEmpty("accept-language"), "en, mi");
}

TEST_F(HttpBinaryDownstreamCodecTest, testOnIngressFailureMalformedMessage) {
  // Format is `**GET*https*www.example.com*/hello.txt**user-agent*curl/7.16.3
  // libcurl/7.16.3 OpenSSL/0.9.7l
  // zlib/1.2.3*host*www.example.com*accept-language*en, mi` where the last `*`
  // is value 7 instead of 6
  const std::vector<uint8_t> binaryInvalidHTTPMessage{
      0x00, 0x03, 0x47, 0x45, 0x54, 0x05, 0x68, 0x74, 0x74, 0x70, 0x73, 0x0f,
      0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e,
      0x63, 0x6f, 0x6d, 0x0a, 0x2f, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x2e, 0x74,
      0x78, 0x74, 0x40, 0x6c, 0x0a, 0x75, 0x73, 0x65, 0x72, 0x2d, 0x61, 0x67,
      0x65, 0x6e, 0x74, 0x34, 0x63, 0x75, 0x72, 0x6c, 0x2f, 0x37, 0x2e, 0x31,
      0x36, 0x2e, 0x33, 0x20, 0x6c, 0x69, 0x62, 0x63, 0x75, 0x72, 0x6c, 0x2f,
      0x37, 0x2e, 0x31, 0x36, 0x2e, 0x33, 0x20, 0x4f, 0x70, 0x65, 0x6e, 0x53,
      0x53, 0x4c, 0x2f, 0x30, 0x2e, 0x39, 0x2e, 0x37, 0x6c, 0x20, 0x7a, 0x6c,
      0x69, 0x62, 0x2f, 0x31, 0x2e, 0x32, 0x2e, 0x33, 0x04, 0x68, 0x6f, 0x73,
      0x74, 0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c,
      0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x0f, 0x61, 0x63, 0x63, 0x65, 0x70, 0x74,
      0x2d, 0x6c, 0x61, 0x6e, 0x67, 0x75, 0x61, 0x67, 0x65, 0x07, 0x65, 0x6e,
      0x2c, 0x20, 0x6d, 0x69};
  auto binaryHTTPMessageIOBuf = folly::IOBuf::wrapBuffer(folly::ByteRange(
      binaryInvalidHTTPMessage.data(), binaryInvalidHTTPMessage.size()));
  folly::io::Cursor cursor(binaryHTTPMessageIOBuf.get());

  FakeHTTPCodecCallback callback;
  binaryCodecKnownLength_->setCallback(&callback);
  binaryCodecKnownLength_->onIngress(*binaryHTTPMessageIOBuf);
  binaryCodecKnownLength_->onIngressEOF();

  // Check onError was called with the correct error
  EXPECT_EQ(std::string(callback.lastParseError.get()->what()),
            "Invalid Message: Header field exceeds declared section length of "
            "108");
  EXPECT_EQ(callback.headersComplete, 0);
}

TEST_F(HttpBinaryDownstreamCodecTest, testOnIngressFailureIncompleteMessage) {
  // Contains only framing indicator
  const std::vector<uint8_t> binaryInvalidHTTPMessage{0x00};
  auto binaryHTTPMessageIOBuf = folly::IOBuf::wrapBuffer(folly::ByteRange(
      binaryInvalidHTTPMessage.data(), binaryInvalidHTTPMessage.size()));
  folly::io::Cursor cursor(binaryHTTPMessageIOBuf.get());

  FakeHTTPCodecCallback callback;
  binaryCodecKnownLength_->setCallback(&callback);
  binaryCodecKnownLength_->onIngress(*binaryHTTPMessageIOBuf);
  binaryCodecKnownLength_->onIngressEOF();

  // Check onError was called with the correct error
  EXPECT_EQ(std::string(callback.lastParseError.get()->what()),
            "Incomplete message received, either empty or only contains "
            "framing indicator");
}

TEST_F(HttpBinaryUpstreamCodecTest, testGenerateKnownLengthHeaders) {
  // Create HTTPMessage and encode it to a buffer
  HTTPMessage msgEncoded;
  msgEncoded.setMethod("GET");
  msgEncoded.setSecure(true);
  msgEncoded.setURL("/hello.txt");
  HTTPHeaders& headersEncoded = msgEncoded.getHeaders();
  headersEncoded.set("user-agent",
                     "curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3");
  headersEncoded.set("host", "www.example.com");
  headersEncoded.set("accept-language", "en, mi");

  folly::IOBufQueue writeBuffer;
  binaryCodecKnownLength_->generateHeader(writeBuffer, 0, msgEncoded);

  // Now, decode the HTTPMessage from the buffer and check values
  FakeHTTPCodecCallback callback;
  HTTPBinaryCodec downstreamCodec{TransportDirection::DOWNSTREAM};
  downstreamCodec.setCallback(&callback);
  downstreamCodec.onIngress(*writeBuffer.front());
  downstreamCodec.onIngressEOF();

  EXPECT_EQ(callback.msg->getMethod(), msgEncoded.getMethod());
  EXPECT_EQ(callback.msg->isSecure(), msgEncoded.isSecure());
  EXPECT_EQ(callback.msg->getURL(), msgEncoded.getURL());
  auto headersDecoded = callback.msg->getHeaders();
  EXPECT_EQ(headersDecoded.size(), headersEncoded.size());
  headersEncoded.forEach([&headersDecoded](const std::string& headerName,
                                           const std::string& headerValue) {
    EXPECT_EQ(headersDecoded.exists(headerName), true);
    EXPECT_EQ(headersDecoded.getSingleOrEmpty(headerName), headerValue);
  });
}

TEST_F(HttpBinaryUpstreamCodecTest, testGenerateIndeterminateLengthHeaders) {
  // Create HTTPMessage and encode it to a buffer
  HTTPMessage msgEncoded;
  msgEncoded.setMethod("GET");
  msgEncoded.setSecure(true);
  msgEncoded.setURL("/hello.txt");
  HTTPHeaders& headersEncoded = msgEncoded.getHeaders();
  headersEncoded.set("user-agent",
                     "curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3");
  headersEncoded.set("host", "www.example.com");
  headersEncoded.set("accept-language", "en, mi");

  folly::IOBufQueue writeBuffer;
  binaryCodecIndeterminateLength_->generateHeader(writeBuffer, 0, msgEncoded);

  // Now, decode the HTTPMessage from the buffer and check values
  FakeHTTPCodecCallback callback;
  HTTPBinaryCodec downstreamCodec{TransportDirection::DOWNSTREAM};
  downstreamCodec.setCallback(&callback);
  downstreamCodec.onIngress(*writeBuffer.front());
  downstreamCodec.onIngressEOF();

  EXPECT_EQ(callback.msg->getMethod(), msgEncoded.getMethod());
  EXPECT_EQ(callback.msg->isSecure(), msgEncoded.isSecure());
  EXPECT_EQ(callback.msg->getURL(), msgEncoded.getURL());
  auto headersDecoded = callback.msg->getHeaders();
  EXPECT_EQ(headersDecoded.size(), headersEncoded.size());
  headersEncoded.forEach([&headersDecoded](const std::string& headerName,
                                           const std::string& headerValue) {
    EXPECT_EQ(headersDecoded.exists(headerName), true);
    EXPECT_EQ(headersDecoded.getSingleOrEmpty(headerName), headerValue);
  });
}

TEST_F(HttpBinaryUpstreamCodecTest, testGenerateKnownLengthBody) {
  // Create Test Body and encode
  std::string body = "Sample Test Body!";
  std::unique_ptr<folly::IOBuf> testBody =
      folly::IOBuf::wrapBuffer(body.data(), body.size());

  folly::IOBufQueue writeBuffer;
  binaryCodecKnownLength_->generateBody(writeBuffer, 0, std::move(testBody));

  // Decode Test Body and check
  folly::io::Cursor cursor(writeBuffer.front());
  HTTPMessage msg;
  EXPECT_EQ(binaryCodecKnownLength_->parseContent(cursor, 18).bytesParsed_, 18);
  EXPECT_EQ(binaryCodecKnownLength_->getMsgBody().to<std::string>(),
            "Sample Test Body!");
}

TEST_F(HttpBinaryUpstreamCodecTest, testGenerateIndeterminateLengthBody) {
  // Create Test Body and encode
  std::string body = "Sample Test Body!";
  std::unique_ptr<folly::IOBuf> testBody =
      folly::IOBuf::wrapBuffer(body.data(), body.size());

  folly::IOBufQueue writeBuffer;
  binaryCodecIndeterminateLength_->generateBody(
      writeBuffer, 0, std::move(testBody), folly::none, true);

  // Decode Test Body and check
  folly::io::Cursor cursor(writeBuffer.front());
  HTTPMessage msg;
  // 18 bytes + 1 for the Content Terminator
  EXPECT_EQ(
      binaryCodecIndeterminateLength_->parseContent(cursor, 19).bytesParsed_,
      19);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getMsgBody().to<std::string>(),
            "Sample Test Body!");
}

TEST_F(HttpBinaryUpstreamCodecTest, testEncodeAndDecodeKnownLengthRequest) {
  // Create full request encode it to a buffer
  folly::IOBufQueue writeBuffer;

  // Encode Framing Indicator, Control Data, and Headers
  HTTPMessage msgEncoded;
  msgEncoded.setMethod("POST");
  msgEncoded.setSecure(false);
  msgEncoded.setURL("/hello.txt");
  HTTPHeaders& headersEncoded = msgEncoded.getHeaders();
  headersEncoded.set("user-agent",
                     "curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3");
  headersEncoded.set("host", "www.example.com");
  headersEncoded.set("accept-language", "en, mi");
  binaryCodecKnownLength_->generateHeader(writeBuffer, 0, msgEncoded);

  // Encode Body
  std::string body = "Sample Test Body!";
  std::unique_ptr<folly::IOBuf> testBody =
      folly::IOBuf::wrapBuffer(body.data(), body.size());
  binaryCodecKnownLength_->generateBody(writeBuffer, 0, std::move(testBody));

  // Encode Trailing Headers
  std::unique_ptr<HTTPHeaders> trailersEncoded =
      std::make_unique<HTTPHeaders>();
  trailersEncoded->set("test-trailer", "test-trailer-value");
  msgEncoded.setTrailers(std::move(trailersEncoded));
  binaryCodecKnownLength_->generateTrailers(
      writeBuffer, 0, *msgEncoded.getTrailers());

  // Now, decode the request and check values
  FakeHTTPCodecCallback callback;
  HTTPBinaryCodec downstreamCodec{TransportDirection::DOWNSTREAM};
  downstreamCodec.setCallback(&callback);
  downstreamCodec.onIngress(*writeBuffer.front());
  downstreamCodec.onIngressEOF();

  EXPECT_EQ(callback.msg->getMethod(), msgEncoded.getMethod());
  EXPECT_EQ(callback.msg->isSecure(), msgEncoded.isSecure());
  EXPECT_EQ(callback.msg->getURL(), msgEncoded.getURL());
  auto headersDecoded = callback.msg->getHeaders();
  EXPECT_EQ(headersDecoded.size(), headersEncoded.size());
  headersEncoded.forEach([&headersDecoded](const std::string& headerName,
                                           const std::string& headerValue) {
    EXPECT_EQ(headersDecoded.exists(headerName), true);
    EXPECT_EQ(headersDecoded.getSingleOrEmpty(headerName), headerValue);
  });

  EXPECT_EQ(callback.data_.move()->to<std::string>(), "Sample Test Body!");

  auto trailersDecoded = *callback.msg->getTrailers();
  EXPECT_EQ(trailersDecoded.size(), 1);
  EXPECT_EQ(trailersDecoded.exists("test-trailer"), true);
  EXPECT_EQ(trailersDecoded.getSingleOrEmpty("test-trailer"),
            "test-trailer-value");
}

TEST_F(HttpBinaryUpstreamCodecTest, testEncodeAndDecodeRequestEmptyBody) {
  // Create full request encode it to a buffer
  folly::IOBufQueue writeBuffer;

  // Encode Framing Indicator, Control Data, and Headers
  HTTPMessage msgEncoded;
  msgEncoded.setMethod("POST");
  msgEncoded.setSecure(false);
  msgEncoded.setURL("/hello.txt");
  HTTPHeaders& headersEncoded = msgEncoded.getHeaders();
  headersEncoded.set("user-agent",
                     "curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3");
  headersEncoded.set("host", "www.example.com");
  headersEncoded.set("accept-language", "en, mi");
  binaryCodecKnownLength_->generateHeader(writeBuffer, 0, msgEncoded);

  // Encode Empty Body
  std::unique_ptr<folly::IOBuf> emptyBody;
  binaryCodecKnownLength_->generateBody(writeBuffer, 0, std::move(emptyBody));

  // Encode Trailing Headers
  std::unique_ptr<HTTPHeaders> trailersEncoded =
      std::make_unique<HTTPHeaders>();
  trailersEncoded->set("test-trailer", "test-trailer-value");
  msgEncoded.setTrailers(std::move(trailersEncoded));
  binaryCodecKnownLength_->generateTrailers(
      writeBuffer, 0, *msgEncoded.getTrailers());

  // Now, decode the request and check values
  FakeHTTPCodecCallback callback;
  HTTPBinaryCodec downstreamCodec{TransportDirection::DOWNSTREAM};
  downstreamCodec.setCallback(&callback);
  downstreamCodec.onIngress(*writeBuffer.front());
  downstreamCodec.onIngressEOF();

  EXPECT_EQ(callback.msg->getMethod(), msgEncoded.getMethod());
  EXPECT_EQ(callback.msg->isSecure(), msgEncoded.isSecure());
  EXPECT_EQ(callback.msg->getURL(), msgEncoded.getURL());
  auto headersDecoded = callback.msg->getHeaders();
  EXPECT_EQ(headersDecoded.size(), headersEncoded.size());
  headersEncoded.forEach([&headersDecoded](const std::string& headerName,
                                           const std::string& headerValue) {
    EXPECT_EQ(headersDecoded.exists(headerName), true);
    EXPECT_EQ(headersDecoded.getSingleOrEmpty(headerName), headerValue);
  });

  auto trailersDecoded = *callback.msg->getTrailers();
  EXPECT_EQ(trailersDecoded.size(), 1);
  EXPECT_EQ(trailersDecoded.exists("test-trailer"), true);
  EXPECT_EQ(trailersDecoded.getSingleOrEmpty("test-trailer"),
            "test-trailer-value");
}

TEST_F(HttpBinaryUpstreamCodecTest,
       testEncodeAndDecodeIndeterminateLengthRequest) {
  // Create full request encode it to a buffer
  folly::IOBufQueue writeBuffer;

  // Encode Framing Indicator, Control Data, and Headers
  HTTPMessage msgEncoded;
  msgEncoded.setMethod("POST");
  msgEncoded.setSecure(false);
  msgEncoded.setURL("/hello.txt");
  HTTPHeaders& headersEncoded = msgEncoded.getHeaders();
  headersEncoded.set("user-agent",
                     "curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3");
  headersEncoded.set("host", "www.example.com");
  headersEncoded.set("accept-language", "en, mi");
  binaryCodecIndeterminateLength_->generateHeader(writeBuffer, 0, msgEncoded);

  // Encode Body
  std::string body = "Sample Test Body!";
  std::unique_ptr<folly::IOBuf> testBody =
      folly::IOBuf::wrapBuffer(body.data(), body.size());
  binaryCodecIndeterminateLength_->generateBody(
      writeBuffer, 0, std::move(testBody), folly::none, true);

  // Encode Trailing Headers
  std::unique_ptr<HTTPHeaders> trailersEncoded =
      std::make_unique<HTTPHeaders>();
  trailersEncoded->set("test-trailer", "test-trailer-value");
  msgEncoded.setTrailers(std::move(trailersEncoded));
  binaryCodecIndeterminateLength_->generateTrailers(
      writeBuffer, 0, *msgEncoded.getTrailers());

  // Now, decode the request and check values
  FakeHTTPCodecCallback callback;
  HTTPBinaryCodec downstreamCodec{TransportDirection::DOWNSTREAM};
  downstreamCodec.setCallback(&callback);
  downstreamCodec.onIngress(*writeBuffer.front());
  downstreamCodec.onIngressEOF();

  EXPECT_EQ(callback.msg->getMethod(), msgEncoded.getMethod());
  EXPECT_EQ(callback.msg->isSecure(), msgEncoded.isSecure());
  EXPECT_EQ(callback.msg->getURL(), msgEncoded.getURL());
  auto headersDecoded = callback.msg->getHeaders();
  EXPECT_EQ(headersDecoded.size(), headersEncoded.size());
  headersEncoded.forEach([&headersDecoded](const std::string& headerName,
                                           const std::string& headerValue) {
    EXPECT_EQ(headersDecoded.exists(headerName), true);
    EXPECT_EQ(headersDecoded.getSingleOrEmpty(headerName), headerValue);
  });

  EXPECT_EQ(callback.data_.move()->to<std::string>(), "Sample Test Body!");

  auto trailersDecoded = *callback.msg->getTrailers();
  EXPECT_EQ(trailersDecoded.size(), 1);
  EXPECT_EQ(trailersDecoded.exists("test-trailer"), true);
  EXPECT_EQ(trailersDecoded.getSingleOrEmpty("test-trailer"),
            "test-trailer-value");
}

TEST_F(HttpBinaryUpstreamCodecTest,
       testEncodeAndDecodeIndeterminateLengthRequestEmptyBody) {
  // Create full request encode it to a buffer
  folly::IOBufQueue writeBuffer;

  // Encode Framing Indicator, Control Data, and Headers
  HTTPMessage msgEncoded;
  msgEncoded.setMethod("POST");
  msgEncoded.setSecure(false);
  msgEncoded.setURL("/hello.txt");
  HTTPHeaders& headersEncoded = msgEncoded.getHeaders();
  headersEncoded.set("user-agent",
                     "curl/7.16.3 libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3");
  headersEncoded.set("host", "www.example.com");
  headersEncoded.set("accept-language", "en, mi");
  binaryCodecIndeterminateLength_->generateHeader(writeBuffer, 0, msgEncoded);

  // Encode Empty Body
  std::unique_ptr<folly::IOBuf> emptyBody;
  binaryCodecIndeterminateLength_->generateBody(
      writeBuffer, 0, std::move(emptyBody), folly::none, true);

  // Encode Trailing Headers
  std::unique_ptr<HTTPHeaders> trailersEncoded =
      std::make_unique<HTTPHeaders>();
  trailersEncoded->set("test-trailer", "test-trailer-value");
  msgEncoded.setTrailers(std::move(trailersEncoded));
  binaryCodecIndeterminateLength_->generateTrailers(
      writeBuffer, 0, *msgEncoded.getTrailers());

  // Now, decode the request and check values
  FakeHTTPCodecCallback callback;
  HTTPBinaryCodec downstreamCodec{TransportDirection::DOWNSTREAM};
  downstreamCodec.setCallback(&callback);
  downstreamCodec.onIngress(*writeBuffer.front());
  downstreamCodec.onIngressEOF();

  EXPECT_EQ(callback.msg->getMethod(), msgEncoded.getMethod());
  EXPECT_EQ(callback.msg->isSecure(), msgEncoded.isSecure());
  EXPECT_EQ(callback.msg->getURL(), msgEncoded.getURL());
  auto headersDecoded = callback.msg->getHeaders();
  EXPECT_EQ(headersDecoded.size(), headersEncoded.size());
  headersEncoded.forEach([&headersDecoded](const std::string& headerName,
                                           const std::string& headerValue) {
    EXPECT_EQ(headersDecoded.exists(headerName), true);
    EXPECT_EQ(headersDecoded.getSingleOrEmpty(headerName), headerValue);
  });

  auto trailersDecoded = *callback.msg->getTrailers();
  EXPECT_EQ(trailersDecoded.size(), 1);
  EXPECT_EQ(trailersDecoded.exists("test-trailer"), true);
  EXPECT_EQ(trailersDecoded.getSingleOrEmpty("test-trailer"),
            "test-trailer-value");
}

TEST_F(HttpBinaryUpstreamCodecTest, IndeterminateLenReqKnownLengthResp) {
  folly::IOBufQueue upstreamEgress;
  HTTPMessage req;
  req.setURL("/");
  req.getHeaders().set("host", "test.com");
  // serialize headers + 10-byte body on a codec w/ indeterminate length
  binaryCodecIndeterminateLength_->generateHeader(
      upstreamEgress, /*txn=*/0, req);
  binaryCodecIndeterminateLength_->generateBody(
      upstreamEgress, /*txn=*/0, /*chain=*/makeBuf(10));

  // generate known length 200/ok resp; parse via upstream codec
  auto resp = folly::IOBuf::fromString(folly::unhexlify("0140c800"));
  FakeHTTPCodecCallback upstreamCodecCb;
  binaryCodecIndeterminateLength_->setCallback(&upstreamCodecCb);
  binaryCodecIndeterminateLength_->onIngress(*resp);
  binaryCodecIndeterminateLength_->onIngressEOF();
  EXPECT_EQ(upstreamCodecCb.msg->getStatusCode(), 200);

  // upstream codec serialize a 10-byte body chunk + eom
  binaryCodecIndeterminateLength_->generateBody(
      upstreamEgress, /*txn=*/0, /*chain=*/makeBuf(10));
  // egress known length still expected to be false => ::generateEOM > 0 bytes
  EXPECT_GT(
      binaryCodecIndeterminateLength_->generateEOM(upstreamEgress, /*txn=*/0),
      0);

  // downstream codec parse upstream req
  HTTPBinaryCodec downstreamCodec{TransportDirection::DOWNSTREAM};
  FakeHTTPCodecCallback downstreamCodecCb;
  downstreamCodec.setCallback(&downstreamCodecCb);
  downstreamCodec.onIngress(*upstreamEgress.front());
  downstreamCodec.onIngressEOF();

  EXPECT_EQ(downstreamCodecCb.msg->getURL(), "/");
  EXPECT_EQ(downstreamCodecCb.msg->getHeaders().getSingleOrEmpty("host"),
            "test.com");
  EXPECT_FALSE(downstreamCodecCb.lastParseError);
}

// A chunk that is split across onIngress calls leaves the chunks before it
// unparsed, so they must not be handed to the callback twice
TEST_F(HttpBinaryDownstreamCodecTest, testSplitContentChunkIsDeliveredOnce) {
  const std::string body(512, 'a');
  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 1024);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  writeVarint(appender, 0 /* empty field section */);
  writeVarintString(appender, "first-");
  writeVarintString(appender, body);

  // Split partway through the second chunk, so the first chunk is complete and
  // already delivered while the second one is still outstanding
  auto head = message.split(message.chainLength() - 256);
  auto tail = message.move();

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*head);
  binaryCodecIndeterminateLength_->onIngress(*tail);

  EXPECT_EQ(callback.lastParseError, nullptr);
  EXPECT_EQ(callback.bodyLength, body.size() + 6);
  EXPECT_EQ(callback.data_.move()->to<std::string>(), "first-" + body);
}

// The bounds check for each chunk has to be made against what the call has not
// already consumed, not against the whole buffer
TEST_F(HttpBinaryDownstreamCodecTest, testPartialChunkIsNotOverCounted) {
  const std::string first(200, 'a');
  const std::string second(200, 'b');
  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 1024);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  writeVarint(appender, 0 /* empty field section */);
  writeVarintString(appender, first);
  writeVarintString(appender, second);

  // Withhold the second half of the second chunk. Checking its declared length
  // against the whole buffer rather than the unconsumed part would accept it.
  auto head = message.split(message.chainLength() - 100);
  auto tail = message.move();

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*head);
  binaryCodecIndeterminateLength_->onIngress(*tail);

  EXPECT_EQ(callback.lastParseError, nullptr);
  EXPECT_EQ(callback.bodyLength, first.size() + second.size());
  EXPECT_EQ(callback.data_.move()->to<std::string>(), first + second);
}

TEST_F(HttpBinaryDownstreamCodecTest, testSplitHeaderFieldIsDecodedOnce) {
  const std::string splitValue = "split-value";
  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 256);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  writeVarintString(appender, "x-first");
  writeVarintString(appender, "first-value");
  writeVarintString(appender, "x-split");
  writeVarint(appender, splitValue.size());

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*message.front());
  EXPECT_EQ(binaryCodecIndeterminateLength_->getPendingFieldCount(), 1);

  for (size_t i = 0; i < splitValue.size(); i++) {
    auto chunk = folly::IOBuf::copyBuffer(splitValue.data() + i, 1);
    binaryCodecIndeterminateLength_->onIngress(*chunk);
    EXPECT_EQ(binaryCodecIndeterminateLength_->getPendingFieldCount(),
              i + 1 == splitValue.size() ? 2 : 1);
  }
  EXPECT_EQ(binaryCodecIndeterminateLength_->getBufferedIngressSize(), 0);

  folly::IOBufQueue terminators{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender terminatorAppender(&terminators, 4);
  writeVarint(terminatorAppender, 0 /* field section terminator */);
  writeVarint(terminatorAppender, 0 /* empty content field */);
  writeVarint(terminatorAppender, 0 /* content terminator */);
  writeVarint(terminatorAppender, 0 /* trailer section terminator */);
  binaryCodecIndeterminateLength_->onIngress(*terminators.front());

  EXPECT_EQ(callback.lastParseError, nullptr);
  ASSERT_NE(callback.msg, nullptr);
  EXPECT_EQ(callback.msg->getHeaders().size(), 2);
  EXPECT_EQ(callback.msg->getHeaders().getSingleOrEmpty("x-first"),
            "first-value");
  EXPECT_EQ(callback.msg->getHeaders().getSingleOrEmpty("x-split"), splitValue);
}

TEST_F(HttpBinaryDownstreamCodecTest, testSplitTrailerFieldIsDecodedOnce) {
  const std::string splitValue = "split-value";
  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 256);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  writeVarint(appender, 0 /* field section terminator */);
  writeVarint(appender, 0 /* empty content field */);
  writeVarint(appender, 0 /* content terminator */);
  writeVarintString(appender, "x-first");
  writeVarintString(appender, "first-value");
  writeVarintString(appender, "x-split");
  writeVarint(appender, splitValue.size());

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*message.front());
  EXPECT_EQ(binaryCodecIndeterminateLength_->getPendingFieldCount(), 1);

  for (size_t i = 0; i < splitValue.size(); i++) {
    auto chunk = folly::IOBuf::copyBuffer(splitValue.data() + i, 1);
    binaryCodecIndeterminateLength_->onIngress(*chunk);
    EXPECT_EQ(binaryCodecIndeterminateLength_->getPendingFieldCount(),
              i + 1 == splitValue.size() ? 2 : 1);
  }
  EXPECT_EQ(binaryCodecIndeterminateLength_->getBufferedIngressSize(), 0);

  auto terminator = folly::IOBuf::copyBuffer("\0", 1);
  binaryCodecIndeterminateLength_->onIngress(*terminator);

  EXPECT_EQ(callback.lastParseError, nullptr);
  ASSERT_NE(callback.msg, nullptr);
  ASSERT_NE(callback.msg->getTrailers(), nullptr);
  EXPECT_EQ(callback.msg->getTrailers()->size(), 2);
  EXPECT_EQ(callback.msg->getTrailers()->getSingleOrEmpty("x-first"),
            "first-value");
  EXPECT_EQ(callback.msg->getTrailers()->getSingleOrEmpty("x-split"),
            splitValue);
}

TEST_F(HttpBinaryDownstreamCodecTest,
       testSplitHeaderLengthVarintIsDecodedOnce) {
  const std::string splitValue(64, 'a');
  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 256);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  writeVarintString(appender, "x-first");
  writeVarintString(appender, "first-value");
  writeVarintString(appender, "x-split");
  writeVarint(appender, splitValue.size());

  auto head = message.split(message.chainLength() - 1);
  auto lengthSuffix = message.move();

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*head);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getPendingFieldCount(), 1);

  binaryCodecIndeterminateLength_->onIngress(*lengthSuffix);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getPendingFieldCount(), 1);

  auto value = folly::IOBuf::copyBuffer(splitValue.data(), splitValue.size());
  binaryCodecIndeterminateLength_->onIngress(*value);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getPendingFieldCount(), 2);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getBufferedIngressSize(), 0);

  auto terminators = folly::IOBuf::copyBuffer("\0\0\0\0", 4);
  binaryCodecIndeterminateLength_->onIngress(*terminators);

  EXPECT_EQ(callback.lastParseError, nullptr);
  ASSERT_NE(callback.msg, nullptr);
  EXPECT_EQ(callback.msg->getHeaders().getSingleOrEmpty("x-split"), splitValue);
}

TEST_F(HttpBinaryDownstreamCodecTest,
       testBytePacedLargeFieldNameIsTrimmedOnce) {
  constexpr size_t kOhttpFieldSectionSize = size_t{64} * 1024;
  constexpr size_t kFieldPartSize = kOhttpFieldSectionSize / 2 - 16;
  const std::string headerName(kFieldPartSize, 'x');
  const std::string headerValue(kFieldPartSize, 'v');

  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 256);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  writeVarint(appender, headerName.size());

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setMaxFieldSectionSize(
      kOhttpFieldSectionSize);
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*message.front());

  const auto sendByte = [&](const std::string& input, size_t offset) {
    auto chunk = folly::IOBuf::copyBuffer(input.data() + offset, 1);
    binaryCodecIndeterminateLength_->onIngress(*chunk);
  };
  for (size_t i = 0; i < headerName.size(); i++) {
    sendByte(headerName, i);
  }

  const auto* pendingFieldName =
      binaryCodecIndeterminateLength_->getPendingFieldName();
  ASSERT_NE(pendingFieldName, nullptr);
  EXPECT_EQ(*pendingFieldName, headerName);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getPendingFieldCount(), 0);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getBufferedIngressSize(), 0);

  folly::IOBufQueue valueLength{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender valueLengthAppender(&valueLength, 8);
  writeVarint(valueLengthAppender, headerValue.size());
  const auto valueLengthSize = valueLength.chainLength();
  binaryCodecIndeterminateLength_->onIngress(*valueLength.front());
  EXPECT_EQ(binaryCodecIndeterminateLength_->getBufferedIngressSize(),
            valueLengthSize);

  for (size_t i = 0; i + 1 < headerValue.size(); i++) {
    sendByte(headerValue, i);
  }
  pendingFieldName = binaryCodecIndeterminateLength_->getPendingFieldName();
  ASSERT_NE(pendingFieldName, nullptr);
  EXPECT_EQ(*pendingFieldName, headerName);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getBufferedIngressSize(),
            valueLengthSize + headerValue.size() - 1);
  sendByte(headerValue, headerValue.size() - 1);

  EXPECT_EQ(binaryCodecIndeterminateLength_->getPendingFieldName(), nullptr);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getPendingFieldCount(), 1);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getBufferedIngressSize(), 0);

  auto terminators = folly::IOBuf::copyBuffer("\0\0\0\0", 4);
  binaryCodecIndeterminateLength_->onIngress(*terminators);

  EXPECT_EQ(callback.lastParseError, nullptr);
  ASSERT_NE(callback.msg, nullptr);
  EXPECT_EQ(callback.msg->getHeaders().getSingleOrEmpty(headerName),
            headerValue);
}

TEST_F(HttpBinaryDownstreamCodecTest, testCachedFieldNameAllowsEmptyValue) {
  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 128);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  writeVarintString(appender, "x-empty");

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*message.front());

  ASSERT_NE(binaryCodecIndeterminateLength_->getPendingFieldName(), nullptr);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getBufferedIngressSize(), 0);

  auto emptyValueAndTerminators = folly::IOBuf::copyBuffer("\0\0\0\0\0", 5);
  binaryCodecIndeterminateLength_->onIngress(*emptyValueAndTerminators);

  EXPECT_EQ(callback.lastParseError, nullptr);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getPendingFieldName(), nullptr);
  ASSERT_NE(callback.msg, nullptr);
  EXPECT_EQ(callback.msg->getHeaders().size(), 1);
  EXPECT_TRUE(callback.msg->getHeaders().exists("x-empty"));
  EXPECT_EQ(callback.msg->getHeaders().getSingleOrEmpty("x-empty"), "");
}

TEST_F(HttpBinaryDownstreamCodecTest,
       testKnownLengthHeaderFieldCannotExceedDeclaredSection) {
  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 128);
  writeRequestPreamble(appender, 0 /* request, known length */);
  writeOverrunningKnownLengthFieldSection(appender);

  FakeHTTPCodecCallback callback;
  binaryCodecKnownLength_->setCallback(&callback);
  binaryCodecKnownLength_->onIngress(*message.front());

  ASSERT_NE(callback.lastParseError, nullptr);
  EXPECT_EQ(std::string(callback.lastParseError->what()),
            "Invalid Message: Failure to parse: headerValue of declared length "
            "64 exceeds the remaining field section budget of 2");
  EXPECT_EQ(binaryCodecKnownLength_->getPendingFieldCount(), 1);
}

TEST_F(HttpBinaryDownstreamCodecTest,
       testKnownLengthTrailerFieldCannotExceedDeclaredSection) {
  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 128);
  writeRequestPreamble(appender, 0 /* request, known length */);
  writeVarint(appender, 0 /* empty header section */);
  writeVarint(appender, 0 /* empty content */);
  writeOverrunningKnownLengthFieldSection(appender);

  FakeHTTPCodecCallback callback;
  binaryCodecKnownLength_->setCallback(&callback);
  binaryCodecKnownLength_->onIngress(*message.front());

  ASSERT_NE(callback.lastParseError, nullptr);
  EXPECT_EQ(std::string(callback.lastParseError->what()),
            "Invalid Message: Failure to parse: headerValue of declared length "
            "64 exceeds the remaining field section budget of 2");
  EXPECT_EQ(binaryCodecKnownLength_->getPendingFieldCount(), 1);
}

// Field section lengths are QUIC varints, so a peer can declare up to 2^62-1
// bytes. The declaration must be rejected up front instead of buffering
// ingress until the section is complete.
TEST_F(HttpBinaryDownstreamCodecTest, testDeclaredFieldSectionSizeIsBounded) {
  folly::IOBufQueue preamble{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&preamble, 128);
  writeRequestPreamble(appender, 0 /* request, known length */);
  writeVarint(appender, kAbsurdDeclaredLength);

  FakeHTTPCodecCallback callback;
  binaryCodecKnownLength_->setCallback(&callback);
  binaryCodecKnownLength_->onIngress(*preamble.front());

  auto filler = makeFiller(kFillerChunkSize);
  for (size_t i = 0; i < kFillerChunkCount && !callback.lastParseError; i++) {
    binaryCodecKnownLength_->onIngress(*filler);
  }

  EXPECT_NE(callback.lastParseError, nullptr);
  EXPECT_LT(binaryCodecKnownLength_->getBufferedIngressSize(),
            kFillerChunkSize * kFillerChunkCount);
}

// The same bound applies to an individual field, whose length is also a varint
// and which is buffered whole before the name/value pair is validated.
TEST_F(HttpBinaryDownstreamCodecTest, testDeclaredFieldSizeIsBounded) {
  folly::IOBufQueue preamble{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&preamble, 128);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  writeVarintString(appender, "x-pad");
  writeVarint(appender, kAbsurdDeclaredLength);

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*preamble.front());

  auto filler = makeFiller(kFillerChunkSize);
  for (size_t i = 0; i < kFillerChunkCount && !callback.lastParseError; i++) {
    binaryCodecIndeterminateLength_->onIngress(*filler);
  }

  EXPECT_NE(callback.lastParseError, nullptr);
  EXPECT_LT(binaryCodecIndeterminateLength_->getBufferedIngressSize(),
            kFillerChunkSize * kFillerChunkCount);
}

// An indeterminate-length field section is terminated by a zero byte rather
// than a declared length, so it is bounded by the bytes actually seen.
TEST_F(HttpBinaryDownstreamCodecTest,
       testIndeterminateFieldSectionSizeIsBounded) {
  folly::IOBufQueue preamble{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&preamble, 128);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*preamble.front());

  // A stream of well-formed fields that never reaches the terminator
  folly::IOBufQueue fields{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender fieldsAppender(&fields, 1024);
  const std::string padValue(64, 'a');
  for (size_t i = 0; i < 512; i++) {
    writeVarintString(fieldsAppender, "x-pad");
    writeVarintString(fieldsAppender, padValue);
  }
  constexpr size_t kSuccessfulBatchCount = 2;
  binaryCodecIndeterminateLength_->setMaxFieldSectionSize(
      fields.chainLength() * kSuccessfulBatchCount);
  auto fieldsBuf = fields.move();

  for (size_t i = 0; i < kSuccessfulBatchCount; i++) {
    binaryCodecIndeterminateLength_->onIngress(*fieldsBuf);
    ASSERT_EQ(callback.lastParseError, nullptr);
    EXPECT_EQ(binaryCodecIndeterminateLength_->getBufferedIngressSize(), 0);
  }

  binaryCodecIndeterminateLength_->onIngress(*fieldsBuf);
  EXPECT_NE(callback.lastParseError, nullptr);
}

TEST_F(HttpBinaryDownstreamCodecTest,
       testIndeterminateFieldSectionLimitResetsForTrailers) {
  const std::string fieldName = "x-near-cap";
  const std::string fieldValue(128, 'a');
  const auto fieldSize =
      quic::getQuicIntegerSize(fieldName.size()).value() + fieldName.size() +
      quic::getQuicIntegerSize(fieldValue.size()).value() + fieldValue.size();

  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 512);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  writeVarintString(appender, fieldName);
  writeVarintString(appender, fieldValue);
  writeVarint(appender, 0 /* field section terminator */);
  writeVarint(appender, 0 /* empty content field */);
  writeVarint(appender, 0 /* content terminator */);
  writeVarintString(appender, fieldName);
  writeVarintString(appender, fieldValue);
  writeVarint(appender, 0 /* trailer section terminator */);

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setMaxFieldSectionSize(fieldSize);
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*message.front());

  EXPECT_EQ(callback.lastParseError, nullptr);
  ASSERT_NE(callback.msg, nullptr);
  EXPECT_EQ(callback.msg->getHeaders().getSingleOrEmpty(fieldName), fieldValue);
  ASSERT_NE(callback.msg->getTrailers(), nullptr);
  EXPECT_EQ(callback.msg->getTrailers()->getSingleOrEmpty(fieldName),
            fieldValue);
}

TEST_F(HttpBinaryDownstreamCodecTest, testMaxFieldSectionSizeIsConfigurable) {
  const std::string headerValue(256, 'a');
  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 512);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  writeVarintString(appender, "x-pad");
  writeVarintString(appender, headerValue);
  writeVarint(appender, 0 /* field section terminator */);
  auto messageBuf = message.move();

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setMaxFieldSectionSize(64);
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*messageBuf);

  EXPECT_NE(callback.lastParseError, nullptr);
}

// A field is bounded by what is left of the section, not by the section
// maximum, so a declaration that only fits when the fields already parsed are
// ignored is rejected instead of being waited on.
TEST_F(HttpBinaryDownstreamCodecTest, testFieldIsBoundedByRemainingSection) {
  constexpr size_t kMaxFieldSectionSize = 4096;
  constexpr size_t kPadFieldCount = 3;

  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 1024);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  const std::string padValue(1024, 'a');
  for (size_t i = 0; i < kPadFieldCount; i++) {
    writeVarintString(appender, "x-pad");
    writeVarintString(appender, padValue);
  }
  // Comfortably under the maximum on its own, but well over the ~1000 bytes
  // the fields above have left of it. None of the declared bytes follow.
  writeVarintString(appender, "x-last");
  writeVarint(appender, kMaxFieldSectionSize / 2);
  auto messageBuf = message.move();

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setMaxFieldSectionSize(kMaxFieldSectionSize);
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*messageBuf);

  EXPECT_NE(callback.lastParseError, nullptr);
}

// A declared content length is also a varint. The codec must hand body bytes
// to the callback as they arrive rather than buffering the whole chunk.
TEST_F(HttpBinaryDownstreamCodecTest, testLargeKnownLengthContentIsStreamed) {
  folly::IOBufQueue preamble{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&preamble, 128);
  writeRequestPreamble(appender, 0 /* request, known length */);
  writeVarint(appender, 0 /* empty field section */);
  writeVarint(appender, kAbsurdDeclaredLength);

  FakeHTTPCodecCallback callback;
  binaryCodecKnownLength_->setCallback(&callback);
  binaryCodecKnownLength_->onIngress(*preamble.front());

  auto filler = makeFiller(kFillerChunkSize);
  for (size_t i = 0; i < kFillerChunkCount; i++) {
    binaryCodecKnownLength_->onIngress(*filler);
  }

  EXPECT_EQ(callback.lastParseError, nullptr);
  EXPECT_EQ(callback.bodyLength, kFillerChunkSize * kFillerChunkCount);
  EXPECT_LT(binaryCodecKnownLength_->getBufferedIngressSize(),
            kFillerChunkSize);
}

TEST_F(HttpBinaryDownstreamCodecTest,
       testLargeIndeterminateLengthContentIsStreamed) {
  folly::IOBufQueue preamble{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&preamble, 128);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  writeVarint(appender, 0 /* empty field section */);
  writeVarint(appender, kAbsurdDeclaredLength);

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*preamble.front());

  auto filler = makeFiller(kFillerChunkSize);
  for (size_t i = 0; i < kFillerChunkCount; i++) {
    binaryCodecIndeterminateLength_->onIngress(*filler);
  }

  EXPECT_EQ(callback.lastParseError, nullptr);
  EXPECT_EQ(callback.bodyLength, kFillerChunkSize * kFillerChunkCount);
  EXPECT_LT(binaryCodecIndeterminateLength_->getBufferedIngressSize(),
            kFillerChunkSize);
}

// Content is trimmed from bufferedIngress_ as it is delivered, so a truncated
// chunk must still be reported as an incomplete message at EOF
TEST_F(HttpBinaryDownstreamCodecTest, testTruncatedContentIsIncompleteAtEOF) {
  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 128);
  writeRequestPreamble(appender, 0 /* request, known length */);
  writeVarint(appender, 0 /* empty field section */);
  writeVarint(appender, 64 /* declared content length */);
  const std::string partialContent(32, 'a');
  appender.pushAtMost(reinterpret_cast<const uint8_t*>(partialContent.data()),
                      partialContent.size());
  auto messageBuf = message.move();

  FakeHTTPCodecCallback callback;
  binaryCodecKnownLength_->setCallback(&callback);
  binaryCodecKnownLength_->onIngress(*messageBuf);
  binaryCodecKnownLength_->onIngressEOF();

  ASSERT_NE(callback.lastParseError, nullptr);
  EXPECT_EQ(std::string(callback.lastParseError->what()),
            "Incomplete message received");
}

TEST_F(HttpBinaryDownstreamCodecTest,
       testTrimmedFieldWithoutTerminatorIsIncompleteAtEOF) {
  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 128);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  writeVarintString(appender, "x-complete");
  writeVarintString(appender, "value");

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*message.front());

  EXPECT_EQ(callback.lastParseError, nullptr);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getPendingFieldCount(), 1);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getBufferedIngressSize(), 0);

  binaryCodecIndeterminateLength_->onIngressEOF();

  ASSERT_NE(callback.lastParseError, nullptr);
  EXPECT_EQ(std::string(callback.lastParseError->what()),
            "Incomplete message received");
}

TEST_F(HttpBinaryDownstreamCodecTest,
       testTrimmedTrailerWithoutTerminatorIsIncompleteAtEOF) {
  folly::IOBufQueue message{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&message, 128);
  writeRequestPreamble(appender, 2 /* request, indeterminate length */);
  writeVarint(appender, 0 /* field section terminator */);
  writeVarint(appender, 0 /* empty content field */);
  writeVarint(appender, 0 /* content terminator */);
  writeVarintString(appender, "x-complete");
  writeVarintString(appender, "value");

  FakeHTTPCodecCallback callback;
  binaryCodecIndeterminateLength_->setCallback(&callback);
  binaryCodecIndeterminateLength_->onIngress(*message.front());

  EXPECT_EQ(callback.lastParseError, nullptr);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getPendingFieldCount(), 1);
  EXPECT_EQ(binaryCodecIndeterminateLength_->getBufferedIngressSize(), 0);

  binaryCodecIndeterminateLength_->onIngressEOF();

  ASSERT_NE(callback.lastParseError, nullptr);
  EXPECT_EQ(std::string(callback.lastParseError->what()),
            "Incomplete message received");
}

} // namespace proxygen::test
