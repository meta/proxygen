/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/portability/GFlags.h>
#include <glog/logging.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/codec/HTTP1xCodec.h>
#include <proxygen/lib/http/experimental/llhttp/HTTP1xLlCodec.h>
#include <string>
#include <vector>

using namespace proxygen;

namespace {

class CountingCallback : public HTTPCodec::Callback {
 public:
  void onMessageBegin(HTTPCodec::StreamID, HTTPMessage*) override {
  }
  void onHeadersComplete(HTTPCodec::StreamID,
                         std::unique_ptr<HTTPMessage>) override {
  }
  void onBody(HTTPCodec::StreamID,
              std::unique_ptr<folly::IOBuf> chain,
              uint16_t) override {
    bodyBytes += chain->computeChainDataLength();
  }
  void onTrailersComplete(HTTPCodec::StreamID,
                          std::unique_ptr<HTTPHeaders>) override {
  }
  void onMessageComplete(HTTPCodec::StreamID, bool) override {
    ++messages;
  }
  void onError(HTTPCodec::StreamID, const HTTPException& error, bool) override {
    LOG(FATAL) << "parse error " << error;
  }

  size_t messages{0};
  size_t bodyBytes{0};
};

std::string body(size_t len) {
  return std::string(len, 'a');
}

std::string chunked(size_t chunks, size_t chunkLen) {
  std::string s;
  for (size_t i = 0; i < chunks; ++i) {
    s += fmt::format("{:x}\r\n{}\r\n", chunkLen, body(chunkLen));
  }
  return s + "0\r\n\r\n";
}

const std::string kSmallGet =
    "GET / HTTP/1.1\r\n"
    "Host: www.facebook.com\r\n"
    "\r\n";

const std::string kBrowserGet = [] {
  std::string cookie;
  for (int i = 0; i < 20; ++i) {
    cookie += fmt::format("c{}=abcdef0123456789; ", i);
  }
  return "GET /path/to/resource?query=value&other=1 HTTP/1.1\r\n"
         "Host: www.facebook.com\r\n"
         "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 "
         "Safari/537.36\r\n"
         "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,"
         "image/avif,image/webp,*/*;q=0.8\r\n"
         "Accept-Language: en-US,en;q=0.9\r\n"
         "Accept-Encoding: gzip, deflate, br\r\n"
         "Connection: keep-alive\r\n"
         "Upgrade-Insecure-Requests: 1\r\n"
         "Sec-Fetch-Dest: document\r\n"
         "Sec-Fetch-Mode: navigate\r\n"
         "Sec-Fetch-Site: none\r\n"
         "Sec-Fetch-User: ?1\r\n"
         "Cache-Control: max-age=0\r\n"
         "Referer: https://www.facebook.com/\r\n"
         "Cookie: " +
         cookie +
         "\r\n"
         "\r\n";
}();

const std::string kPost1KB =
    "POST /upload HTTP/1.1\r\n"
    "Host: www.facebook.com\r\n"
    "Content-Type: application/octet-stream\r\n"
    "Content-Length: 1024\r\n"
    "\r\n" +
    body(1024);

const std::string kPostChunked =
    "POST /upload HTTP/1.1\r\n"
    "Host: www.facebook.com\r\n"
    "Transfer-Encoding: chunked\r\n"
    "\r\n" +
    chunked(16, 1024);

const std::string kResponse1KB =
    "HTTP/1.1 200 OK\r\n"
    "Date: Wed, 21 Oct 2026 07:28:00 GMT\r\n"
    "Content-Type: text/html; charset=utf-8\r\n"
    "Content-Length: 1024\r\n"
    "\r\n" +
    body(1024);

const std::string kResponseChunked =
    "HTTP/1.1 200 OK\r\n"
    "Date: Wed, 21 Oct 2026 07:28:00 GMT\r\n"
    "Content-Type: text/html; charset=utf-8\r\n"
    "Transfer-Encoding: chunked\r\n"
    "\r\n" +
    chunked(16, 1024);

const std::string kApiPost =
    "POST /graphql HTTP/1.1\r\n"
    "Host: graph.facebook.com\r\n"
    "User-Agent: FBAndroidSDK/17.0.0 (Linux; Android 14; Pixel 8)\r\n"
    "Authorization: Bearer " +
    std::string(200, 't') +
    "\r\n"
    "Accept: application/json\r\n"
    "Accept-Encoding: gzip, deflate\r\n"
    "Content-Type: application/json\r\n"
    "Content-Length: 512\r\n"
    "X-FB-Friendly-Name: FetchNewsFeed\r\n"
    "X-FB-Request-Analytics-Tags: {\"product\":\"feed\"}\r\n"
    "\r\n" +
    std::string(512, 'j');

const std::string kProxiedGet =
    "GET /api/v1/items/12345?fields=id,name,price HTTP/1.1\r\n"
    "Host: internal.service.example.com\r\n"
    "User-Agent: proxygen-client/1.0\r\n"
    "Accept: */*\r\n"
    "X-Forwarded-For: 2401:db00:1234:5678::1, 10.0.0.1\r\n"
    "X-Forwarded-Proto: https\r\n"
    "X-Forwarded-Host: www.example.com\r\n"
    "Via: 1.1 edge-proxy-01, 1.1 origin-proxy-02\r\n"
    "X-Request-Id: 7f9c2a1e-4b3d-4e8a-9c1f-2d6e8b4a0c13\r\n"
    "traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\r\n"
    "X-Real-IP: 2401:db00:1234:5678::1\r\n"
    "\r\n";

const std::string kLargeCookieGet = [] {
  std::string cookie;
  while (cookie.size() < 4096) {
    cookie += fmt::format("c{}=abcdef0123456789abcdef; ", cookie.size());
  }
  return "GET / HTTP/1.1\r\n"
         "Host: www.facebook.com\r\n"
         "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)\r\n"
         "Accept: text/html\r\n"
         "Cookie: " +
         cookie +
         "\r\n"
         "\r\n";
}();

const std::string kPost64KB =
    "POST /upload HTTP/1.1\r\n"
    "Host: www.facebook.com\r\n"
    "Content-Type: application/octet-stream\r\n"
    "Content-Length: 65536\r\n"
    "\r\n" +
    body(65536);

constexpr size_t kPipelineDepth = 8;
const std::string kPipelinedGets = [] {
  std::string s;
  for (size_t i = 0; i < kPipelineDepth; ++i) {
    s += kSmallGet;
  }
  return s;
}();

const std::string kCdnResponse =
    "HTTP/1.1 200 OK\r\n"
    "Date: Wed, 21 Oct 2026 07:28:00 GMT\r\n"
    "Content-Type: image/jpeg\r\n"
    "Content-Length: 256\r\n"
    "Cache-Control: public, max-age=31536000, immutable\r\n"
    "ETag: \"5d8c72a5edda8d6a:0\"\r\n"
    "Last-Modified: Tue, 20 Oct 2026 12:00:00 GMT\r\n"
    "Accept-Ranges: bytes\r\n"
    "Access-Control-Allow-Origin: *\r\n"
    "Timing-Allow-Origin: *\r\n"
    "Vary: Accept-Encoding\r\n"
    "X-Content-Type-Options: nosniff\r\n"
    "Strict-Transport-Security: max-age=31536000; includeSubDomains\r\n"
    "X-FB-Debug: " +
    std::string(88, 'd') +
    "\r\n"
    "Alt-Svc: h3=\":443\"; ma=86400\r\n"
    "Connection: keep-alive\r\n"
    "\r\n" +
    body(256);

// Parses the same keep-alive request over and over on one codec
template <class Codec>
void parseRequests(size_t iters,
                   const std::string& request,
                   size_t messagesPerRequest = 1) {
  folly::BenchmarkSuspender suspender;
  Codec codec{TransportDirection::DOWNSTREAM};
  CountingCallback callback;
  codec.setCallback(&callback);
  auto buf = folly::IOBuf::copyBuffer(request);
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    codec.onIngress(*buf);
  }

  suspender.rehire();
  CHECK_EQ(callback.messages, iters * messagesPerRequest);
}

// Same as parseRequests, but delivers each request in pieces of pieceLen bytes
template <class Codec>
void parseRequestsInPieces(size_t iters,
                           const std::string& request,
                           size_t pieceLen) {
  folly::BenchmarkSuspender suspender;
  Codec codec{TransportDirection::DOWNSTREAM};
  CountingCallback callback;
  codec.setCallback(&callback);
  std::vector<std::unique_ptr<folly::IOBuf>> pieces;
  for (size_t off = 0; off < request.size(); off += pieceLen) {
    pieces.push_back(folly::IOBuf::copyBuffer(request.substr(off, pieceLen)));
  }
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    for (const auto& piece : pieces) {
      codec.onIngress(*piece);
    }
  }

  suspender.rehire();
  CHECK_EQ(callback.messages, iters);
}

// Parses one response per iteration; the pipelined requests they answer are
// generated before timing starts
template <class Codec>
void parseResponses(size_t iters, const std::string& response) {
  folly::BenchmarkSuspender suspender;
  Codec codec{TransportDirection::UPSTREAM};
  CountingCallback callback;
  codec.setCallback(&callback);
  auto buf = folly::IOBuf::copyBuffer(response);
  HTTPMessage req;
  req.setMethod(HTTPMethod::GET);
  req.setURL("/");
  req.setHTTPVersion(1, 1);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  for (size_t i = 0; i < iters; ++i) {
    codec.generateHeader(writeBuf, codec.createStream(), req, true);
    writeBuf.move();
  }
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    codec.onIngress(*buf);
  }

  suspender.rehire();
  CHECK_EQ(callback.messages, iters);
}

// Serializes a response (headers, 1KB body, EOM) to each of a stream of
// pipelined requests that are parsed before timing starts
template <class Codec>
void generateResponses(size_t iters) {
  folly::BenchmarkSuspender suspender;
  Codec codec{TransportDirection::DOWNSTREAM};
  CountingCallback callback;
  codec.setCallback(&callback);
  auto request = folly::IOBuf::copyBuffer(kSmallGet);
  HTTPMessage resp;
  resp.setStatusCode(200);
  resp.setStatusMessage("OK");
  resp.setHTTPVersion(1, 1);
  resp.getHeaders().set(HTTP_HEADER_CONTENT_TYPE, "text/html");
  resp.getHeaders().set(HTTP_HEADER_CONTENT_LENGTH, "1024");
  auto respBody = folly::IOBuf::copyBuffer(body(1024));
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  for (size_t i = 0; i < iters; ++i) {
    codec.onIngress(*request);
  }
  suspender.dismiss();

  for (size_t i = 0; i < iters; ++i) {
    auto txn = static_cast<HTTPCodec::StreamID>(i + 1);
    codec.generateHeader(writeBuf, txn, resp);
    codec.generateBody(writeBuf, txn, respBody->clone(), folly::none, true);
    writeBuf.move();
  }
}

std::string request(const std::string& headers) {
  return "GET / HTTP/1.1\r\nHost: www.facebook.com\r\n" + headers + "\r\n";
}

std::string repeatHeader(const std::string& line, size_t count) {
  std::string s;
  for (size_t i = 0; i < count; ++i) {
    s += line + "\r\n";
  }
  return s;
}

// Many tiny headers: per-byte state dispatch and per-header transitions
const std::string kManyShortHeaders = [] {
  std::string s;
  for (int i = 0; i < 50; ++i) {
    s += fmt::format("X-Hdr-{:02}: v\r\n", i);
  }
  return request(s);
}();

// Header names both parsers match against known names. The lookalike differs
// only in its last byte; the plain name is ruled out by its first byte.
const std::string kSpecialHeaderNames =
    request(repeatHeader("Connection: keep-alive", 20));
const std::string kLookalikeHeaderNames =
    request(repeatHeader("Connectiom: keep-alive", 20));
const std::string kPlainHeaderNames =
    request(repeatHeader("X-Abcdefgh: keep-alive", 20));

// Long runs of ordinary characters: llhttp's SIMD scan vs the fork's unrolled
// loops
const std::string kLongHeaderValue =
    request("X-Long: " + std::string(8192, 'v') + "\r\n");
const std::string kLongHeaderNames = [] {
  std::string s;
  for (int i = 0; i < 20; ++i) {
    s += fmt::format("X-{:062}: v\r\n", i);
  }
  return request(s);
}();

} // namespace

BENCHMARK(HTTP1x_ParseSmallGet, iters) {
  parseRequests<HTTP1xCodec>(iters, kSmallGet);
}
BENCHMARK_RELATIVE(HTTP1xLl_ParseSmallGet, iters) {
  parseRequests<HTTP1xLlCodec>(iters, kSmallGet);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HTTP1x_ParseBrowserGet, iters) {
  parseRequests<HTTP1xCodec>(iters, kBrowserGet);
}
BENCHMARK_RELATIVE(HTTP1xLl_ParseBrowserGet, iters) {
  parseRequests<HTTP1xLlCodec>(iters, kBrowserGet);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HTTP1x_ParseBrowserGetIn64BytePieces, iters) {
  parseRequestsInPieces<HTTP1xCodec>(iters, kBrowserGet, 64);
}
BENCHMARK_RELATIVE(HTTP1xLl_ParseBrowserGetIn64BytePieces, iters) {
  parseRequestsInPieces<HTTP1xLlCodec>(iters, kBrowserGet, 64);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HTTP1x_ParsePost1KB, iters) {
  parseRequests<HTTP1xCodec>(iters, kPost1KB);
}
BENCHMARK_RELATIVE(HTTP1xLl_ParsePost1KB, iters) {
  parseRequests<HTTP1xLlCodec>(iters, kPost1KB);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HTTP1x_ParsePostChunked16x1KB, iters) {
  parseRequests<HTTP1xCodec>(iters, kPostChunked);
}
BENCHMARK_RELATIVE(HTTP1xLl_ParsePostChunked16x1KB, iters) {
  parseRequests<HTTP1xLlCodec>(iters, kPostChunked);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HTTP1x_ParseResponse1KB, iters) {
  parseResponses<HTTP1xCodec>(iters, kResponse1KB);
}
BENCHMARK_RELATIVE(HTTP1xLl_ParseResponse1KB, iters) {
  parseResponses<HTTP1xLlCodec>(iters, kResponse1KB);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HTTP1x_ParseResponseChunked16x1KB, iters) {
  parseResponses<HTTP1xCodec>(iters, kResponseChunked);
}
BENCHMARK_RELATIVE(HTTP1xLl_ParseResponseChunked16x1KB, iters) {
  parseResponses<HTTP1xLlCodec>(iters, kResponseChunked);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HTTP1x_GenerateResponse1KB, iters) {
  generateResponses<HTTP1xCodec>(iters);
}
BENCHMARK_RELATIVE(HTTP1xLl_GenerateResponse1KB, iters) {
  generateResponses<HTTP1xLlCodec>(iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HTTP1x_ParseApiPost, iters) {
  parseRequests<HTTP1xCodec>(iters, kApiPost);
}
BENCHMARK_RELATIVE(HTTP1xLl_ParseApiPost, iters) {
  parseRequests<HTTP1xLlCodec>(iters, kApiPost);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HTTP1x_ParseProxiedGet, iters) {
  parseRequests<HTTP1xCodec>(iters, kProxiedGet);
}
BENCHMARK_RELATIVE(HTTP1xLl_ParseProxiedGet, iters) {
  parseRequests<HTTP1xLlCodec>(iters, kProxiedGet);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HTTP1x_ParseLargeCookieGet, iters) {
  parseRequests<HTTP1xCodec>(iters, kLargeCookieGet);
}
BENCHMARK_RELATIVE(HTTP1xLl_ParseLargeCookieGet, iters) {
  parseRequests<HTTP1xLlCodec>(iters, kLargeCookieGet);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HTTP1x_ParsePost64KB, iters) {
  parseRequests<HTTP1xCodec>(iters, kPost64KB);
}
BENCHMARK_RELATIVE(HTTP1xLl_ParsePost64KB, iters) {
  parseRequests<HTTP1xLlCodec>(iters, kPost64KB);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HTTP1x_ParsePipelinedGets, iters) {
  parseRequests<HTTP1xCodec>(iters, kPipelinedGets, kPipelineDepth);
}
BENCHMARK_RELATIVE(HTTP1xLl_ParsePipelinedGets, iters) {
  parseRequests<HTTP1xLlCodec>(iters, kPipelinedGets, kPipelineDepth);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(HTTP1x_ParseCdnResponse, iters) {
  parseResponses<HTTP1xCodec>(iters, kCdnResponse);
}
BENCHMARK_RELATIVE(HTTP1xLl_ParseCdnResponse, iters) {
  parseResponses<HTTP1xLlCodec>(iters, kCdnResponse);
}

#define HEADER_BENCHMARKS(name, input, pieceLen)                  \
  BENCHMARK(HTTP1x_##name, iters) {                               \
    parseRequestsInPieces<HTTP1xCodec>(iters, input, pieceLen);   \
  }                                                               \
  BENCHMARK_RELATIVE(HTTP1xLl_##name, iters) {                    \
    parseRequestsInPieces<HTTP1xLlCodec>(iters, input, pieceLen); \
  }                                                               \
  BENCHMARK_DRAW_LINE();

HEADER_BENCHMARKS(ManyShortHeaders, kManyShortHeaders, kManyShortHeaders.size())
HEADER_BENCHMARKS(SpecialHeaderNames,
                  kSpecialHeaderNames,
                  kSpecialHeaderNames.size())
HEADER_BENCHMARKS(LookalikeHeaderNames,
                  kLookalikeHeaderNames,
                  kLookalikeHeaderNames.size())
HEADER_BENCHMARKS(PlainHeaderNames, kPlainHeaderNames, kPlainHeaderNames.size())
HEADER_BENCHMARKS(LongHeaderValue8KB, kLongHeaderValue, kLongHeaderValue.size())
HEADER_BENCHMARKS(LongHeaderNames, kLongHeaderNames, kLongHeaderNames.size())
HEADER_BENCHMARKS(BrowserGetByteAtATime, kBrowserGet, 1)

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  folly::runBenchmarks();
  return 0;
}
