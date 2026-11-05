/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "proxygen/lib/http/experimental/llhttp/LlhttpParser.h"

#include <folly/portability/GTest.h>

#include <string>

namespace facebook::detail { namespace {

struct CallbackState {
  static CallbackState& from(llhttp_t* parser) {
    return *static_cast<CallbackState*>(parser->data);
  }

  static int onUrl(llhttp_t* parser, const char* data, size_t len) {
    from(parser).url.append(data, len);
    return HPE_OK;
  }

  static int onHeadersComplete(llhttp_t* parser) {
    auto& state = from(parser);
    state.headersComplete++;
    return state.pauseAtHeaders ? HPE_PAUSED : HPE_OK;
  }

  static int onMessageComplete(llhttp_t* parser) {
    auto& state = from(parser);
    state.messageComplete++;
    if (state.parser) {
      state.pendingDuringMessageComplete = state.parser->hasPendingIngress();
    }
    return HPE_OK;
  }

  static const llhttp_settings_t* settings() {
    static const llhttp_settings_t kSettings = [] {
      llhttp_settings_t settings;
      llhttp_settings_init(&settings);
      settings.on_url = onUrl;
      settings.on_headers_complete = onHeadersComplete;
      settings.on_message_complete = onMessageComplete;
      return settings;
    }();
    return &kSettings;
  }

  bool pauseAtHeaders{false};
  LlhttpParser* parser{nullptr};
  bool pendingDuringMessageComplete{false};
  size_t headersComplete{0};
  size_t messageComplete{0};
  std::string url;
};

TEST(LlhttpParserTest, UsesCallerCallbacksAndContext) {
  CallbackState callbacks;
  LlhttpParser parser{HTTP_REQUEST, CallbackState::settings(), &callbacks};
  const std::string request = "GET /foo HTTP/1.1\r\nHost: h\r\n\r\n";

  EXPECT_EQ(parser.execute(request.data(), request.size()), request.size());
  EXPECT_EQ(callbacks.url, "/foo");
  EXPECT_EQ(callbacks.headersComplete, 1);
  EXPECT_EQ(callbacks.messageComplete, 1);
  EXPECT_EQ(parser.httpMajor(), 1);
  EXPECT_EQ(parser.httpMinor(), 1);
  EXPECT_STREQ(parser.methodStr(), "GET");
  EXPECT_FALSE(parser.isChunked());
  EXPECT_FALSE(parser.hasFatalError());
}

TEST(LlhttpParserTest, ReturnsBytesConsumedBeforePause) {
  CallbackState callbacks;
  callbacks.pauseAtHeaders = true;
  LlhttpParser parser{HTTP_REQUEST, CallbackState::settings(), &callbacks};
  const std::string headers = "POST /foo HTTP/1.1\r\nContent-Length: 4\r\n\r\n";
  const std::string request = headers + "body";

  EXPECT_EQ(parser.execute(request.data(), request.size()), headers.size());
  EXPECT_TRUE(parser.isPaused());
  EXPECT_EQ(parser.execute(request.data() + headers.size(), 4), 0);

  callbacks.pauseAtHeaders = false;
  parser.resume();
  EXPECT_EQ(parser.execute(request.data() + headers.size(), 4), 4);
  EXPECT_EQ(callbacks.messageComplete, 1);
}

TEST(LlhttpParserTest, PauseAtEndOfInputLeavesPendingIngress) {
  CallbackState callbacks;
  callbacks.pauseAtHeaders = true;
  LlhttpParser parser{HTTP_REQUEST, CallbackState::settings(), &callbacks};
  const std::string request = "GET /foo HTTP/1.1\r\nHost: h\r\n\r\n";

  EXPECT_EQ(parser.execute(request.data(), request.size()), request.size());
  EXPECT_TRUE(parser.hasPendingIngress());
  EXPECT_EQ(callbacks.messageComplete, 0);

  callbacks.pauseAtHeaders = false;
  parser.resume();
  parser.runPendingIngress();
  EXPECT_FALSE(parser.hasPendingIngress());
  EXPECT_EQ(callbacks.messageComplete, 1);
}

TEST(LlhttpParserTest, PendingIngressIsClearWhileExecuting) {
  CallbackState callbacks;
  callbacks.pauseAtHeaders = true;
  LlhttpParser parser{HTTP_REQUEST, CallbackState::settings(), &callbacks};
  callbacks.parser = &parser;
  const std::string request = "GET /foo HTTP/1.1\r\nHost: h\r\n\r\n";

  EXPECT_EQ(parser.execute(request.data(), request.size()), request.size());
  ASSERT_TRUE(parser.hasPendingIngress());

  callbacks.pauseAtHeaders = false;
  parser.resume();
  parser.execute(request.data(), request.size());
  EXPECT_EQ(callbacks.messageComplete, 2);
  EXPECT_FALSE(callbacks.pendingDuringMessageComplete);
}

TEST(LlhttpParserTest, PauseBeforeEndOfInputLeavesNoPendingIngress) {
  CallbackState callbacks;
  callbacks.pauseAtHeaders = true;
  LlhttpParser parser{HTTP_REQUEST, CallbackState::settings(), &callbacks};
  const std::string headers = "POST /foo HTTP/1.1\r\nContent-Length: 4\r\n\r\n";
  const std::string request = headers + "body";

  EXPECT_EQ(parser.execute(request.data(), request.size()), headers.size());
  EXPECT_FALSE(parser.hasPendingIngress());
}

TEST(LlhttpParserTest, ReturnsBytesConsumedBeforeParseError) {
  CallbackState callbacks;
  LlhttpParser parser{HTTP_REQUEST, CallbackState::settings(), &callbacks};
  const std::string request = "GET / HTTP/1.1\r\nBad Header: value\r\n\r\n";
  const auto invalidSpace = request.find(' ', request.find("Bad"));

  EXPECT_EQ(parser.execute(request.data(), request.size()), invalidSpace);
  EXPECT_TRUE(parser.hasFatalError());
  EXPECT_EQ(parser.error(), HPE_INVALID_HEADER_TOKEN);

  parser.finish();
  EXPECT_EQ(parser.error(), HPE_INVALID_HEADER_TOKEN);
}

TEST(LlhttpParserTest, UpgradeLeavesProtocolBytesUnconsumed) {
  CallbackState callbacks;
  LlhttpParser parser{HTTP_REQUEST, CallbackState::settings(), &callbacks};
  const std::string headers =
      "GET / HTTP/1.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n";
  const std::string request = headers + "websocket-data";

  EXPECT_EQ(parser.execute(request.data(), request.size()), headers.size());
  EXPECT_EQ(parser.error(), HPE_PAUSED_UPGRADE);
  EXPECT_FALSE(parser.hasFatalError());

  parser.finish();
  EXPECT_EQ(parser.error(), HPE_PAUSED_UPGRADE);
}

TEST(LlhttpParserTest, ReportsChunkedFraming) {
  CallbackState callbacks;
  callbacks.pauseAtHeaders = true;
  LlhttpParser parser{HTTP_REQUEST, CallbackState::settings(), &callbacks};
  const std::string request =
      "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n";

  ASSERT_LT(parser.execute(request.data(), request.size()), request.size());

  EXPECT_TRUE(parser.isChunked());
}

TEST(LlhttpParserTest, RejectsBareLfLineEndingsByDefault) {
  CallbackState callbacks;
  LlhttpParser parser{HTTP_REQUEST, CallbackState::settings(), &callbacks};
  const std::string request = "GET / HTTP/1.1\nHost: h\n\n";

  EXPECT_LT(parser.execute(request.data(), request.size()), request.size());
  EXPECT_TRUE(parser.hasFatalError());
}

TEST(LlhttpParserTest, AcceptsBareLfLineEndings) {
  CallbackState callbacks;
  LlhttpParserOptions options;
  options.lenientOptionalLfAfterCr = true;
  options.lenientOptionalCrBeforeLf = true;
  LlhttpParser parser{
      HTTP_REQUEST, CallbackState::settings(), &callbacks, options};
  const std::string request = "GET / HTTP/1.1\nHost: h\n\n";

  EXPECT_EQ(parser.execute(request.data(), request.size()), request.size());
  EXPECT_EQ(callbacks.messageComplete, 1);
  EXPECT_FALSE(parser.hasFatalError());
}

TEST(LlhttpParserTest, AcceptsFinalCrWithoutLf) {
  CallbackState callbacks;
  LlhttpParserOptions options;
  options.lenientOptionalLfAfterCr = true;
  LlhttpParser parser{
      HTTP_REQUEST, CallbackState::settings(), &callbacks, options};
  const std::string request = "GET / HTTP/1.1\r\nHost: h\r\n\r";

  EXPECT_EQ(parser.execute(request.data(), request.size()), request.size());
  EXPECT_FALSE(parser.hasFatalError());
}

TEST(LlhttpParserTest, AcceptsChunkedTransferEncodingWithContentLength) {
  CallbackState callbacks;
  LlhttpParserOptions options;
  options.lenientChunkedLength = true;
  LlhttpParser parser{
      HTTP_REQUEST, CallbackState::settings(), &callbacks, options};
  const std::string request =
      "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: "
      "4\r\n\r\n"
      "4\r\nbody\r\n0\r\n\r\n";

  EXPECT_EQ(parser.execute(request.data(), request.size()), request.size());
  EXPECT_EQ(callbacks.messageComplete, 1);
  EXPECT_FALSE(parser.hasFatalError());
}

TEST(LlhttpParserTest, FinishReportsTruncatedMessage) {
  CallbackState callbacks;
  LlhttpParser parser{HTTP_REQUEST, CallbackState::settings(), &callbacks};
  const std::string request = "POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\nbo";
  ASSERT_EQ(parser.execute(request.data(), request.size()), request.size());

  parser.finish();

  EXPECT_TRUE(parser.hasFatalError());
  EXPECT_EQ(parser.error(), HPE_INVALID_EOF_STATE);
}

}} // namespace facebook::detail
