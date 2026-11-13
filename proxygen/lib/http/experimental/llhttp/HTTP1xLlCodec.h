/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <proxygen/lib/http/codec/HTTPCodec.h>
#include <proxygen/lib/http/codec/TransportDirection.h>
#include <proxygen/lib/http/experimental/llhttp/LlhttpParser.h>

namespace proxygen {

class HTTP1xLlCodec : public HTTPCodec {
 public:
  explicit HTTP1xLlCodec(
      TransportDirection direction,
      bool force1_1 = false,
      facebook::detail::LlhttpParserOptions parserOptions = {});

  static HTTP1xLlCodec makeResponseCodec(bool mayChunkEgress);

  [[nodiscard]] HTTPCodecTraits getTraits() const override {
    return HTTPCodecTraits{.protocol = CodecProtocol::HTTP_1_1,
                           .direction = transportDirection_};
  }

  [[nodiscard]] const std::string& getUserAgent() const override {
    return userAgent_;
  }

  StreamID createStream() override;

  void setCallback(Callback* callback) override {
    callback_ = callback;
  }

  [[nodiscard]] bool isBusy() const override;
  void setParserPaused(bool paused) override;
  [[nodiscard]] bool isParserPaused() const override;
  bool isParserError();
  size_t onIngress(const folly::IOBuf& buf) override;
  size_t onIngressImpl(const folly::IOBuf& buf);
  void onIngressEOF() override;
  [[nodiscard]] bool isReusable() const override;
  [[nodiscard]] bool isWaitingToDrain() const override;
  [[nodiscard]] bool closeOnEgressComplete() const override;

  [[nodiscard]] bool supportsPushTransactions() const override {
    return false;
  }

  void generateHeader(
      folly::IOBufQueue& writeBuf,
      StreamID txn,
      const HTTPMessage& msg,
      bool eom = false,
      HTTPHeaderSize* size = nullptr,
      const folly::Optional<HTTPHeaders>& extraHeaders = folly::none) override;
  size_t generateBody(folly::IOBufQueue& writeBuf,
                      StreamID txn,
                      std::unique_ptr<folly::IOBuf> chain,
                      folly::Optional<uint8_t> padding,
                      bool eom) override;
  size_t generateChunkHeader(folly::IOBufQueue& writeBuf,
                             StreamID txn,
                             size_t length) override;
  size_t generateChunkTerminator(folly::IOBufQueue& writeBuf,
                                 StreamID txn) override;
  size_t generateTrailers(folly::IOBufQueue& writeBuf,
                          StreamID txn,
                          const HTTPHeaders& trailers) override;
  size_t generatePadding(folly::IOBufQueue&, StreamID, uint16_t) override;
  size_t generateEOM(folly::IOBufQueue& writeBuf, StreamID txn) override;
  size_t generateRstStream(folly::IOBufQueue& writeBuf,
                           StreamID txn,
                           ErrorCode statusCode) override;
  size_t generateGoaway(
      folly::IOBufQueue& writeBuf,
      StreamID lastStream,
      ErrorCode statusCode,
      std::unique_ptr<folly::IOBuf> debugData = nullptr) override;
  size_t generateImmediateGoaway(
      folly::IOBufQueue& writeBuf,
      ErrorCode statusCode,
      std::unique_ptr<folly::IOBuf> debugData = nullptr) override;

  void setReleaseEgressAfterRequest(bool releaseEgress);
  static bool supportsNextProtocol(folly::StringPiece alpn);

 private:
  [[nodiscard]] bool isEgressBusy() const;
  int onMessageBegin();
  int onProtocol(const char* data, size_t len);
  int onURL(const char* data, size_t len);
  int onURLComplete();
  int onVersionComplete();
  int onStatus(const char* data, size_t len);
  int onHeaderField(const char* data, size_t len);
  int onHeaderValue(const char* data, size_t len);
  int onHeaderValueComplete();
  int onHeadersComplete();
  int onBody(const char* data, size_t len);
  int onChunkHeader();
  int onChunkComplete();
  int onMessageComplete();
  void endHttp09RequestLine();

  // LlHttpParser Callbacks
  static int onMessageBeginCB(llhttp_t* parser);
  static int onProtocolCB(llhttp_t* parser, const char* data, size_t len);
  static int onURLCB(llhttp_t* parser, const char* data, size_t len);
  static int onURLCompleteCB(llhttp_t* parser);
  static int onVersionCompleteCB(llhttp_t* parser);
  static int onStatusCB(llhttp_t* parser, const char* data, size_t len);
  static int onHeaderFieldCB(llhttp_t* parser, const char* data, size_t len);
  static int onHeaderValueCB(llhttp_t* parser, const char* data, size_t len);
  static int onHeaderValueCompleteCB(llhttp_t* parser);
  static int onHeadersCompleteCB(llhttp_t* parser);
  static int onBodyCB(llhttp_t* parser, const char* data, size_t len);
  static int onChunkHeaderCB(llhttp_t* parser);
  static int onChunkCompleteCB(llhttp_t* parser);
  static int onMessageCompleteCB(llhttp_t* parser);
  static const llhttp_settings_t* getParserSettings();

  static HTTP1xLlCodec* getCodec(llhttp_t* parser);

  enum ParseState : uint8_t {
    kIdle,
    kHeaders,
    kBody,
    kTrailers,
    kUpgraded,
  };

  struct IngressState {
    StreamID txnID{0};
    const folly::IOBuf* buf{nullptr};

    std::unique_ptr<HTTPMessage> msg;
    std::unique_ptr<HTTPHeaders> trailers;
    std::string headerName;
    folly::StringPiece headerNamePiece;
    std::string headerValue;
    std::string url;
    std::string reason;
    std::string errorContext;
    std::string errorMessage;
    HTTPHeaderSize headerSize;

    ParseState parseState{kIdle};
    bool pauseRequested{false};
    bool awaitingEOF{false};
    bool messagePending{false};
    bool http09Request{false};
    // Only used for upstream
    bool is1xxResponse{false};

    void resetState() {
      headerSize.uncompressed = 0;
      headerSize.compressed = 0;
      url.clear();
      errorContext.clear();
      errorMessage.clear();
      msg = std::make_unique<HTTPMessage>();
      trailers.reset();
      parseState = ParseState::kHeaders;
    }

    void resetHeaders() {
      headerName.clear();
      headerNamePiece.clear();
      headerValue.clear();
    }
  };

  // Once a request disables keepalive it stays disabled for HTTP/1.0 responses
  enum class KeepaliveRequested : uint8_t { UNSET, ENABLED, DISABLED };

  struct EgressState {
    StreamID txnID{0};
    bool messagePending{false};

    KeepaliveRequested peerRequestedKeepalive{KeepaliveRequested::UNSET};
    bool is1xxResponse{false};
    bool releaseAfterRequest{false};

    bool mayChunk{false};
    bool chunked{false};
    bool finalChunkWritten{false};
    bool chunkOpen{false};
  };

  enum UpgradePhase : uint8_t { NONE, PENDING, UPGRADED };

  struct ConnectionState {
    UpgradePhase upgradePhase{NONE};
    std::string upgradeHeader;
    bool keepAlive{true};
    bool disableKeepalivePending{false};
    bool connectRequest{false};
    bool headRequest{false};
    std::string websocketAccept;
  };

  Callback* callback_{nullptr};
  TransportDirection transportDirection_;
  std::string userAgent_;
  bool force1_1_ : 1;
  std::unique_ptr<facebook::detail::LlhttpParser> parser_;
  IngressState ingressState_;
  EgressState egressState_;
  ConnectionState connectionState_;

  // Helper Methods
  [[nodiscard]] bool hasPendingIngress() const;
  void flushPendingIngress();
  [[nodiscard]] bool isParsingHeaders() const {
    return ingressState_.parseState == ParseState::kHeaders;
  }
  [[nodiscard]] bool isParsingTrailers() const {
    return ingressState_.parseState == ParseState::kTrailers;
  }
  [[nodiscard]] bool isHeaderNameEmpty() const {
    return ingressState_.headerName.empty();
  }
  [[nodiscard]] bool isHeaderPieceEmpty() const {
    return ingressState_.headerNamePiece.empty();
  }
  [[nodiscard]] bool isBody() const {
    return ingressState_.parseState == ParseState::kBody;
  }
  [[nodiscard]] int pauseIfRequested() const {
    return isParserPaused() ? ::HPE_PAUSED : 0;
  }
  [[nodiscard]] bool isParseStateUpgraded() const {
    return ingressState_.parseState == ParseState::kUpgraded;
  }
  [[nodiscard]] bool isUpgrade() const {
    return connectionState_.upgradePhase != UpgradePhase::NONE;
  }
  [[nodiscard]] bool isUpgradePending() const {
    return connectionState_.upgradePhase == UpgradePhase::PENDING;
  }
  [[nodiscard]] bool isUpgradeComplete() const {
    return connectionState_.upgradePhase == UpgradePhase::UPGRADED;
  }
  void pushHeaderNameAndValue(HTTPHeaders& hdrs);
  int trackHeaderBytes(size_t len);
  void onParserError();
  void backfillPartialRequest();
  int setErrorContext(std::string context);
  bool validateTransferEncoding(const HTTPHeaders& hdrs) noexcept;
  void addDateHeader(folly::IOBufQueue& writeBuf, size_t& len);
  [[nodiscard]] std::string generateWebsocketKey() const;
  void serializeWebsocketHeader(folly::IOBufQueue& writeBuf,
                                size_t& len,
                                bool upstream);
  [[nodiscard]] std::string generateWebsocketAccept(
      const std::string& key) const;
};

} // namespace proxygen
