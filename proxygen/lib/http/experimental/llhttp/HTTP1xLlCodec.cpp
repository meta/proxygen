/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/base64.h>
#include <folly/io/Cursor.h>
#include <folly/ssl/OpenSSLHash.h>
#include <limits>
#include <proxygen/lib/http/RFC2616.h>
#include <proxygen/lib/http/codec/CodecUtil.h>
#include <proxygen/lib/http/experimental/llhttp/HTTP1xLlCodec.h>

namespace proxygen {

constexpr uint32_t kHeaderByteCap = (80 * 1024);
constexpr size_t kHeaderSeparatorBytes = 2;
// llhttp accepts lengths up to UINT64_MAX; http_parser capped at INT64_MAX
constexpr uint64_t kMaxDataSize = std::numeric_limits<int64_t>::max();
constexpr folly::StringPiece kHttpProtocol = "HTTP";
constexpr std::string_view kChunked = "chunked";
constexpr std::string_view kUpgradeToken = "websocket";
constexpr std::string_view kWSMagicString =
    "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

HTTP1xLlCodec::HTTP1xLlCodec(
    TransportDirection direction,
    bool force1_1,
    facebook::detail::LlhttpParserOptions parserOptions)
    : transportDirection_(direction),
      force1_1_(force1_1),
      parser_(std::make_unique<facebook::detail::LlhttpParser>(
          isDownstream(direction) ? ::HTTP_REQUEST : ::HTTP_RESPONSE,
          getParserSettings(),
          this,
          parserOptions)) {
}

HTTP1xLlCodec HTTP1xLlCodec::makeResponseCodec(bool mayChunkEgress) {
  HTTP1xLlCodec codec(TransportDirection::DOWNSTREAM);
  codec.egressState_.mayChunk = mayChunkEgress;
  return codec;
}

HTTPCodec::StreamID HTTP1xLlCodec::createStream() {
  return isDownstream(transportDirection_) ? ++ingressState_.txnID
                                           : ++egressState_.txnID;
}

bool HTTP1xLlCodec::isBusy() const {
  return ingressState_.messagePending || egressState_.messagePending;
}

bool HTTP1xLlCodec::isParserError() {
  return parser_->hasFatalError();
}

void HTTP1xLlCodec::setParserPaused(bool paused) {
  ingressState_.pauseRequested = paused;
  if (parser_->isActive()) {
    return;
  }
  if (paused) {
    parser_->pause();
  } else {
    parser_->resume();
    resumeDeclinedUpgrade();
  }
}

bool HTTP1xLlCodec::isParserPaused() const {
  return ingressState_.pauseRequested;
}

void HTTP1xLlCodec::resumeDeclinedUpgrade() {
  if (parser_->error() == HPE_PAUSED_UPGRADE && !isParseStateUpgraded() &&
      !isParserPaused()) {
    parser_->resumeAfterUpgrade();
  }
}

size_t HTTP1xLlCodec::onIngress(const folly::IOBuf& buf) {
  folly::io::Cursor cursor(&buf);

  size_t totalBytesParsed = 0;
  while (!cursor.isAtEnd()) {
    folly::IOBuf currentReadBuf;
    auto bufSize = cursor.peekBytes().size();
    cursor.cloneAtMost(currentReadBuf, bufSize);
    size_t bytesParsed = onIngressImpl(currentReadBuf);

    if (bytesParsed == 0) {
      // If the codec didn't make any progress with current input, we
      // wait for more data until this is called again
      break;
    }
    totalBytesParsed += bytesParsed;
    if (bufSize > bytesParsed) {
      cursor.retreat(bufSize - bytesParsed);
    }
    if (isParserPaused()) {
      break;
    }
  }
  return totalBytesParsed;
}

size_t HTTP1xLlCodec::onIngressImpl(const folly::IOBuf& buf) {
  if (isParserError()) {
    return 0;
  } else if (isUpgradeComplete() && isParseStateUpgraded()) {
    callback_->onBody(ingressState_.txnID, buf.clone(), 0);
    return buf.computeChainDataLength();
  } else {
    ingressState_.buf = &buf;
    if (isUpstream(transportDirection_) && parser_->httpMajor() == 0 &&
        parser_->httpMinor() == 9) {
      // Handle HTTP/0.9 ?
    }
    // Callers responsibility to prevent calling onIngress from a callback
    CHECK(!parser_->isActive());
    const char* data = (const char*)buf.data();
    const size_t len = buf.length();
    size_t bytesParsed = parser_->execute(data, len);
    resumeDeclinedUpgrade();
    if (isHeaderNameEmpty() && !isHeaderPieceEmpty()) {
      ingressState_.headerName = ingressState_.headerNamePiece.str();
    }
    ingressState_.buf = nullptr;
    if (ingressState_.awaitingEOF) {
      onIngressEOF();
      ingressState_.awaitingEOF = false;
    }
    return bytesParsed;
  }
}

void HTTP1xLlCodec::onIngressEOF() {
  if (parser_->isActive()) {
    ingressState_.awaitingEOF = true;
    return;
  }
  if (isParseStateUpgraded()) {
    callback_->onMessageComplete(ingressState_.txnID, false);
    return;
  }
  if (ingressState_.parseState == ParseState::kIdle) {
    return;
  }
  parser_->finish();
}

bool HTTP1xLlCodec::isReusable() const {
  return connectionState_.keepAlive && !isUpgrade() &&
         !parser_->hasFatalError() && connectionState_.websocketAccept.empty();
}

bool HTTP1xLlCodec::isWaitingToDrain() const {
  return connectionState_.disableKeepalivePending && connectionState_.keepAlive;
}

bool HTTP1xLlCodec::isEgressBusy() const {
  if (isDownstream(transportDirection_)) {
    return egressState_.messagePending;
  }
  return egressState_.messagePending ||
         (!isUpgrade() && ingressState_.messagePending &&
          !egressState_.releaseAfterRequest);
}

bool HTTP1xLlCodec::closeOnEgressComplete() const {
  return !isEgressBusy() && !isReusable();
}

void HTTP1xLlCodec::generateHeader(
    folly::IOBufQueue& /*writeBuf*/,
    StreamID /*txn*/,
    const HTTPMessage& /*msg*/,
    bool /*eom*/,
    HTTPHeaderSize* /*size*/,
    const folly::Optional<HTTPHeaders>& /*extraHeaders*/) {
}

size_t HTTP1xLlCodec::generateBody(folly::IOBufQueue& /*writeBuf*/,
                                   StreamID /*txn*/,
                                   std::unique_ptr<folly::IOBuf> /*chain*/,
                                   folly::Optional<uint8_t> /*padding*/,
                                   bool /*eom*/) {
  return 0;
}

size_t HTTP1xLlCodec::generateChunkHeader(folly::IOBufQueue& /*writeBuf*/,
                                          StreamID /*txn*/,
                                          size_t /*length*/) {
  return 0;
}

size_t HTTP1xLlCodec::generateChunkTerminator(folly::IOBufQueue& /*writeBuf*/,
                                              StreamID /*txn*/) {
  return 0;
}

size_t HTTP1xLlCodec::generateTrailers(folly::IOBufQueue& /*writeBuf*/,
                                       StreamID /*txn*/,
                                       const HTTPHeaders& /*trailers*/) {
  return 0;
}

size_t HTTP1xLlCodec::generateEOM(folly::IOBufQueue& /*writeBuf*/,
                                  StreamID /*txn*/) {
  return 0;
}

size_t HTTP1xLlCodec::generateRstStream(folly::IOBufQueue& /*writeBuf*/,
                                        StreamID /*txn*/,
                                        ErrorCode /*statusCode*/) {
  return 0;
}

size_t HTTP1xLlCodec::generateGoaway(folly::IOBufQueue&,
                                     StreamID,
                                     ErrorCode,
                                     std::unique_ptr<folly::IOBuf>) {
  return 0;
}

size_t HTTP1xLlCodec::generatePadding(folly::IOBufQueue&, StreamID, uint16_t) {
  return 0;
}

size_t HTTP1xLlCodec::generateImmediateGoaway(folly::IOBufQueue&,
                                              ErrorCode,
                                              std::unique_ptr<folly::IOBuf>) {
  return 0;
}

void HTTP1xLlCodec::setReleaseEgressAfterRequest(bool releaseEgress) {
  egressState_.releaseAfterRequest = releaseEgress;
}

bool HTTP1xLlCodec::supportsNextProtocol(folly::StringPiece alpn) {
  return alpn.size() == 8 && (alpn == "http/1.0" || alpn == "http/1.1");
}

HTTP1xLlCodec* HTTP1xLlCodec::getCodec(llhttp_t* parser) {
  auto* codec = static_cast<HTTP1xLlCodec*>(parser->data);
  DCHECK(codec != nullptr);
  return codec;
}

const llhttp_settings_t* HTTP1xLlCodec::getParserSettings() {
  static const llhttp_settings_t settings = [] {
    llhttp_settings_t value;
    llhttp_settings_init(&value);
    value.on_message_begin = onMessageBeginCB;
    value.on_protocol = onProtocolCB;
    value.on_url = onURLCB;
    value.on_status = onStatusCB;
    value.on_header_field = onHeaderFieldCB;
    value.on_header_value = onHeaderValueCB;
    value.on_header_value_complete = onHeaderValueCompleteCB;
    value.on_headers_complete = onHeadersCompleteCB;
    value.on_body = onBodyCB;
    value.on_chunk_header = onChunkHeaderCB;
    value.on_chunk_complete = onChunkCompleteCB;
    value.on_message_complete = onMessageCompleteCB;
    return value;
  }();
  return &settings;
}

int HTTP1xLlCodec::onMessageBegin() {
  ingressState_.resetState();

  if (isDownstream(transportDirection_)) {
    ingressState_.messagePending = true;
    egressState_.messagePending = true;
  }
  if (isDownstream(transportDirection_) || !ingressState_.is1xxResponse) {
    ++ingressState_.txnID;
  }
  if (isUpstream(transportDirection_)) {
    ingressState_.is1xxResponse = false;
  }
  callback_->onMessageBegin(ingressState_.txnID, ingressState_.msg.get());
  return pauseIfRequested();
}

int HTTP1xLlCodec::onURL(const char* buf, size_t len) {
  if (int rc = trackHeaderBytes(len)) {
    return rc;
  }
  ingressState_.url.append(buf, len);
  return 0;
}

int HTTP1xLlCodec::onProtocol(const char* buf, size_t len) {
  if (!kHttpProtocol.contains(folly::StringPiece(buf, len))) {
    return -1;
  }
  return 0;
}

int HTTP1xLlCodec::onStatus(const char* buf, size_t len) {
  if (int rc = trackHeaderBytes(len)) {
    return rc;
  }
  ingressState_.reason.append(buf, len);
  return 0;
}

int HTTP1xLlCodec::trackHeaderBytes(size_t len) {
  ingressState_.headerSize.compressed += len;
  ingressState_.headerSize.uncompressed += len;
  if (ingressState_.headerSize.compressed > kHeaderByteCap) {
    return -1;
  }
  return 0;
}

void HTTP1xLlCodec::pushHeaderNameAndValue(HTTPHeaders& hdrs) {
  auto headerName = isHeaderNameEmpty()
                        ? ingressState_.headerNamePiece
                        : folly::StringPiece(ingressState_.headerName);
  hdrs.add(headerName, std::move(ingressState_.headerValue));
  ingressState_.resetHeaders();
}

int HTTP1xLlCodec::onHeaderField(const char* buf, size_t len) {
  if (int rc = trackHeaderBytes(len)) {
    return rc;
  }
  if (isHeaderNameEmpty() && isHeaderPieceEmpty()) {
    ingressState_.headerNamePiece.reset(buf, len);
  } else if (isHeaderNameEmpty() &&
             ingressState_.headerNamePiece.end() == buf) {
    ingressState_.headerNamePiece.advance(len);
  } else {
    if (isHeaderNameEmpty()) {
      ingressState_.headerName = ingressState_.headerNamePiece.str();
    }
    ingressState_.headerName.append(buf, len);
  }
  return 0;
}

int HTTP1xLlCodec::onHeaderValue(const char* buf, size_t len) {
  if (int rc = trackHeaderBytes(len)) {
    return rc;
  }
  ingressState_.headerValue.append(buf, len);
  return 0;
}

int HTTP1xLlCodec::onHeaderValueComplete() {
  if (int rc = trackHeaderBytes(kHeaderSeparatorBytes)) {
    return rc;
  }
  if (isParsingTrailers()) {
    if (!ingressState_.trailers) {
      ingressState_.trailers = std::make_unique<HTTPHeaders>();
    }
    pushHeaderNameAndValue(*ingressState_.trailers);
  } else {
    pushHeaderNameAndValue(ingressState_.msg->getHeaders());
  }
  return 0;
}

int HTTP1xLlCodec::onHeadersComplete() {
  HTTPHeaders& hdrs = ingressState_.msg->getHeaders();
  if (!validateTransferEncoding(hdrs)) {
    return -1;
  }
  if (parser_->contentLength() > kMaxDataSize) {
    return -1;
  }

  ingressState_.msg->setIngressHeaderSize(ingressState_.headerSize);
  ingressState_.msg->setHTTPVersion(parser_->httpMajor(), parser_->httpMinor());
  ingressState_.msg->setIsChunked(parser_->isChunked());

  if (isDownstream(transportDirection_)) {
    ingressState_.msg->setMethod(parser_->methodStr());

    connectionState_.connectRequest =
        ingressState_.msg->getMethod() == HTTPMethod::CONNECT;

    connectionState_.headRequest =
        ingressState_.msg->getMethod() == HTTPMethod::HEAD;

    ParseURL parseUrl = ingressState_.msg->setURL(std::move(ingressState_.url));
    ingressState_.url.clear();
    if (parseUrl.hasHost()) {
      auto hostAndPort = parseUrl.hostAndPort();
      VLOG(4) << "Adding inferred host header: " << hostAndPort;
      ingressState_.msg->getHeaders().set(HTTP_HEADER_HOST, hostAndPort);
    }

    egressState_.mayChunk =
        ((parser_->httpMajor() == 1) && (parser_->httpMinor() >= 1));
  } else {
    ingressState_.msg->setStatusCode(parser_->statusCode());
    ingressState_.msg->setStatusMessage(std::move(ingressState_.reason));
    ingressState_.reason.clear();
  }

  if (isUpstream(transportDirection_)) {
    // If server sends 101 or 2xx for CONNECT, transition upgrade from PENDING
    // to UPGRADED, otherwise REJECT Upgrade request
    if (isUpgradePending() && connectionState_.connectRequest &&
        parser_->statusCode() >= 200 && parser_->statusCode() < 300) {
      connectionState_.upgradePhase = UpgradePhase::UPGRADED;
    } else if (parser_->statusCode() == 101) {
      const std::string& serverUpgrade =
          ingressState_.msg->getHeaders().getSingleOrEmpty(HTTP_HEADER_UPGRADE);
      if (!serverAcceptedUpgrade(connectionState_.upgradeHeader,
                                 serverUpgrade)) {
        LOG(ERROR) << "Invalid 101 response, client/server upgrade mismatch "
                      "client="
                   << connectionState_.upgradeHeader
                   << " server=" << serverUpgrade;
        return -1;
      }
      connectionState_.upgradePhase = UpgradePhase::UPGRADED;
    } else if (isUpgradePending() && parser_->statusCode() >= 200) {
      connectionState_.upgradePhase = UpgradePhase::NONE;
    }
  } else if (connectionState_.connectRequest) {
    connectionState_.upgradePhase = UpgradePhase::UPGRADED;
  }
  if (isUpstream(transportDirection_) && parser_->statusCode() != 100) {
    connectionState_.upgradeHeader.clear();
  }
  ingressState_.msg->setIsUpgraded(isUpgradeComplete());

  const std::string& upgrade = hdrs.getSingleOrEmpty(HTTP_HEADER_UPGRADE);
  // llhttp only flags Upgrade sent with Connection: upgrade (RFC 9110 7.8)
  const bool upgradeRequest = isDownstream(transportDirection_) &&
                              parser_->isUpgrade() &&
                              !connectionState_.connectRequest;
  if (upgradeRequest) {
    connectionState_.upgradePhase = UpgradePhase::PENDING;
  }
  // disable keepalive for upgraded connections and CONNECT reqs
  connectionState_.keepAlive &= upgrade.empty();
  connectionState_.keepAlive &= !connectionState_.connectRequest;

  if (caseInsensitiveEqual(upgrade, kUpgradeToken)) {
    if (isUpstream(transportDirection_)) {
      ingressState_.msg->setIngressWebsocketUpgrade();
      const std::string& accept =
          hdrs.getSingleOrEmpty(HTTP_HEADER_SEC_WEBSOCKET_ACCEPT);
      if (accept != connectionState_.websocketAccept) {
        LOG(ERROR)
            << "Mismatch in expected ws accept key: " << "upstream: " << accept
            << " expected: " << connectionState_.websocketAccept;
        return -1;
      }
    } else if (upgradeRequest) {
      ingressState_.msg->setIngressWebsocketUpgrade();
      if (!connectionState_.websocketAccept.empty()) {
        LOG(ERROR) << "ws accept key already set: '"
                   << connectionState_.websocketAccept << "'";
        return -1;
      }
      auto key = hdrs.getSingleOrEmpty(HTTP_HEADER_SEC_WEBSOCKET_KEY);
      connectionState_.websocketAccept = generateWebsocketAccept(key);
    }
  }

  bool msgKeepalive = ingressState_.msg->computeKeepalive();
  if (!msgKeepalive) {
    connectionState_.keepAlive = false;
  }

  if (isDownstream(transportDirection_)) {
    egressState_.peerRequestedKeepalive =
        msgKeepalive && ingressState_.msg->isHTTP1_0() &&
                egressState_.peerRequestedKeepalive !=
                    KeepaliveRequested::DISABLED
            ? KeepaliveRequested::ENABLED
            : KeepaliveRequested::DISABLED;
  }

  bool ignoreBody = false;
  if (isUpstream(transportDirection_)) {
    ingressState_.is1xxResponse = ingressState_.msg->is1xxResponse();
    // If CONNECT or HEAD req we don't expect a response body
    ignoreBody =
        connectionState_.connectRequest || connectionState_.headRequest ||
        RFC2616::responseBodyMustBeEmpty(ingressState_.msg->getStatusCode());
  }

  if (userAgent_.empty()) {
    userAgent_ = ingressState_.msg->getHeaders().getSingleOrEmpty(
        HTTP_HEADER_USER_AGENT);
  }

  callback_->onHeadersComplete(ingressState_.txnID,
                               std::move(ingressState_.msg));

  if (ignoreBody) {
    parser_->skipBody();
  }
  if (isUpstream(transportDirection_) && connectionState_.connectRequest &&
      isUpgradeComplete()) {
    parser_->markUpgrade();
  }
  ingressState_.parseState = ParseState::kBody;
  return pauseIfRequested();
}

int HTTP1xLlCodec::onBody(const char* buf, size_t len) {
  DCHECK_EQ(ingressState_.parseState, ParseState::kBody);
  CHECK_NOTNULL(ingressState_.buf);

  const auto* begin = reinterpret_cast<const char*>(ingressState_.buf->data());
  auto slice = ingressState_.buf->cloneOne();

  slice->trimStart(buf - begin);
  slice->trimEnd(ingressState_.buf->length() - len - (buf - begin));
  callback_->onBody(ingressState_.txnID, std::move(slice), 0);
  return pauseIfRequested();
}

int HTTP1xLlCodec::onChunkHeader() {
  auto len = parser_->contentLength();
  if (len > kMaxDataSize) {
    return -1;
  }
  if (len == 0) {
    ingressState_.parseState = ParseState::kTrailers;
    ingressState_.headerSize.uncompressed = 0;
    ingressState_.headerSize.compressed = 0;
  } else {
    callback_->onChunkHeader(ingressState_.txnID, len);
  }
  return pauseIfRequested();
}

int HTTP1xLlCodec::onChunkComplete() {
  // llhttp completes the zero-length chunk after its trailers
  if (!isParsingTrailers()) {
    callback_->onChunkComplete(ingressState_.txnID);
  }
  return pauseIfRequested();
}

int HTTP1xLlCodec::onMessageComplete() {
  DCHECK(isBody() || isParsingTrailers());
  ingressState_.parseState = ParseState::kIdle;
  if (ingressState_.trailers) {
    callback_->onTrailersComplete(ingressState_.txnID,
                                  std::move(ingressState_.trailers));
  }

  if (isDownstream(transportDirection_)) {
    ingressState_.messagePending = false;
  } else {
    ingressState_.messagePending = ingressState_.is1xxResponse;
    if (ingressState_.is1xxResponse && !isUpgradeComplete()) {
      return pauseIfRequested();
    }
  }

  callback_->onMessageComplete(ingressState_.txnID, isUpgradeComplete());

  if (isUpgradeComplete()) {
    ingressState_.parseState = ParseState::kUpgraded;
  }
  // llhttp pauses after an upgrade message itself
  return parser_->isUpgrade() ? 0 : pauseIfRequested();
}

int HTTP1xLlCodec::onMessageBeginCB(llhttp_t* parser) {
  try {
    return getCodec(parser)->onMessageBegin();
  } catch (const std::exception&) {
    return -1;
  }
}

int HTTP1xLlCodec::onProtocolCB(llhttp_t* parser,
                                const char* data,
                                size_t len) {
  try {
    return getCodec(parser)->onProtocol(data, len);
  } catch (const std::exception&) {
    return -1;
  }
}

int HTTP1xLlCodec::onURLCB(llhttp_t* parser, const char* data, size_t len) {
  try {
    return getCodec(parser)->onURL(data, len);
  } catch (const std::exception&) {
    return -1;
  }
}

int HTTP1xLlCodec::onStatusCB(llhttp_t* parser, const char* data, size_t len) {
  try {
    return getCodec(parser)->onStatus(data, len);
  } catch (const std::exception&) {
    return -1;
  }
}

int HTTP1xLlCodec::onHeaderFieldCB(llhttp_t* parser,
                                   const char* data,
                                   size_t len) {
  try {
    return getCodec(parser)->onHeaderField(data, len);
  } catch (const std::exception&) {
    return -1;
  }
}

int HTTP1xLlCodec::onHeaderValueCB(llhttp_t* parser,
                                   const char* data,
                                   size_t len) {
  try {
    return getCodec(parser)->onHeaderValue(data, len);
  } catch (const std::exception&) {
    return -1;
  }
}

int HTTP1xLlCodec::onHeaderValueCompleteCB(llhttp_t* parser) {
  try {
    return getCodec(parser)->onHeaderValueComplete();
  } catch (const std::exception&) {
    return -1;
  }
}

int HTTP1xLlCodec::onHeadersCompleteCB(llhttp_t* parser) {
  auto* codec = getCodec(parser);
  try {
    return codec->onHeadersComplete();
  } catch (const std::exception&) {
    return -1;
  }
}

int HTTP1xLlCodec::onBodyCB(llhttp_t* parser, const char* data, size_t len) {
  try {
    return getCodec(parser)->onBody(data, len);
  } catch (const std::exception&) {
    return -1;
  }
}

int HTTP1xLlCodec::onChunkHeaderCB(llhttp_t* parser) {
  try {
    return getCodec(parser)->onChunkHeader();
  } catch (const std::exception&) {
    return -1;
  }
}

int HTTP1xLlCodec::onChunkCompleteCB(llhttp_t* parser) {
  try {
    return getCodec(parser)->onChunkComplete();
  } catch (const std::exception&) {
    return -1;
  }
}

int HTTP1xLlCodec::onMessageCompleteCB(llhttp_t* parser) {
  try {
    return getCodec(parser)->onMessageComplete();
  } catch (const std::exception&) {
    return -1;
  }
}

bool HTTP1xLlCodec::validateTransferEncoding(const HTTPHeaders& hdrs) noexcept {
  bool ok = !hdrs.forEachValueOfHeader(
      HTTP_HEADER_TRANSFER_ENCODING, [&](folly::StringPiece value) -> bool {
        bool err = !value.equals(kChunked, folly::AsciiCaseInsensitive{});
        LOG_IF(ERROR, err) << "invalid transfer-encoding val=" << value;
        return err; // stop on err
      });
  return ok &&
         (parser_->isChunked() || !hdrs.exists(HTTP_HEADER_TRANSFER_ENCODING));
}

std::string HTTP1xLlCodec::generateWebsocketAccept(
    const std::string& key) const {
  folly::ssl::OpenSSLHash::Digest digest;
  digest.hash_init(EVP_sha1());
  digest.hash_update(folly::StringPiece(key));
  digest.hash_update(folly::StringPiece(kWSMagicString));
  std::array<unsigned char, 20> arr;
  folly::MutableByteRange accept(arr.data(), arr.size());
  digest.hash_final(accept);
  return folly::base64Encode(
      std::string_view((char*)accept.data(), accept.size()));
}

} // namespace proxygen
