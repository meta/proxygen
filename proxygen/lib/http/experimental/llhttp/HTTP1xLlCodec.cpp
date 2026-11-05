/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/Conv.h>
#include <folly/Random.h>
#include <folly/String.h>
#include <folly/base64.h>
#include <folly/io/Cursor.h>
#include <folly/ssl/OpenSSLHash.h>
#include <limits>
#include <proxygen/lib/http/RFC2616.h>
#include <proxygen/lib/http/codec/CodecUtil.h>
#include <proxygen/lib/http/experimental/llhttp/HTTP1xLlCodec.h>

namespace {

unsigned u64toa(uint64_t value, char* dst) {

  char* start = dst;
  do {
    *dst++ = '0' + (value % 10);
    value /= 10;
  } while (value != 0);
  unsigned length = dst - start;

  dst--;
  while (dst > start) {
    char swap = *dst;
    *dst = *start;
    *start = swap;
    dst--;
    start++;
  }
  return length;
}

void appendUint(folly::IOBufQueue& queue, size_t& len, uint64_t value) {
  char buf[32];
  size_t encodedLen = u64toa(value, buf);
  queue.append(buf, encodedLen);
  len += encodedLen;
}

#define appendLiteral(queue, len, str) \
  (len) += (sizeof(str) - 1);          \
  (queue).append(str, sizeof(str) - 1)

void appendString(folly::IOBufQueue& queue,
                  size_t& len,
                  folly::StringPiece str) {
  queue.append(str.data(), str.size());
  len += str.size();
}

bool hasUserInfo(std::string_view url) {
  auto start = url.find("://");
  if (start == std::string_view::npos) {
    return false;
  }
  start += 3;
  auto end = url.find_first_of("/?#", start);
  return url.substr(start, end - start).find('@') != std::string_view::npos;
}
} // namespace

namespace proxygen {

constexpr uint32_t kHeaderByteCap = (80 * 1024);
constexpr size_t kHeaderSeparatorBytes = 2;
// llhttp accepts lengths up to UINT64_MAX; http_parser capped at INT64_MAX
constexpr uint64_t kMaxDataSize = std::numeric_limits<int64_t>::max();
constexpr folly::StringPiece kHttpProtocol = "HTTP";
constexpr std::string_view kChunked = "chunked";
constexpr std::string_view CRLF = "\r\n";
constexpr std::string_view kUpgradeToken = "websocket";
constexpr std::string_view kUpgradeConnectionToken = "Upgrade";
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

bool HTTP1xLlCodec::hasPendingIngress() const {
  return parser_->hasPendingIngress() && !isParseStateUpgraded();
}

void HTTP1xLlCodec::flushPendingIngress() {
  if (parser_->isActive() || !hasPendingIngress()) {
    return;
  }
  parser_->resume();
  parser_->runPendingIngress();
  if (isParserPaused()) {
    parser_->pause();
  }
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
    const auto prevParseState = ingressState_.parseState;
    size_t bytesParsed = onIngressImpl(currentReadBuf);

    if (bytesParsed == 0 && ingressState_.parseState == prevParseState) {
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
      // HTTP/0.9 responses have no header block, so create a fake 200 response
      // and put the codec in upgrade mode
      onMessageBegin();
      parser_->setStatusCode(200);
      onHeadersComplete();
      connectionState_.upgradePhase = UpgradePhase::UPGRADED;
      ingressState_.parseState = ParseState::kUpgraded;
      return onIngressImpl(buf);
    }
    // Callers responsibility to prevent calling onIngress from a callback
    CHECK(!parser_->isActive());
    const char* data = (const char*)buf.data();
    const size_t len = buf.length();
    size_t bytesParsed = parser_->execute(data, len);
    if (ingressState_.http09Request && !isParserError() &&
        (parser_->isPaused() || data[len - 1] == '\n')) {
      endHttp09RequestLine();
    }
    if (!isUpgradePending()) {
      flushPendingIngress();
    }
    resumeDeclinedUpgrade();
    if (isParserError()) {
      onParserError();
    }
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
  if (isParserError()) {
    return;
  }
  if (parser_->isActive()) {
    ingressState_.awaitingEOF = true;
    return;
  }
  flushPendingIngress();
  resumeDeclinedUpgrade();
  if (isParserError()) {
    onParserError();
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
  if (isParserError()) {
    onParserError();
  }
}

void HTTP1xLlCodec::backfillPartialRequest() {
  if (ingressState_.url.empty()) {
    return;
  }
  ingressState_.msg->setIngressHeaderSize(ingressState_.headerSize);
  ingressState_.msg->setHTTPVersion(parser_->httpMajor(), parser_->httpMinor());
  ingressState_.msg->setMethod(parser_->methodStr());
  ingressState_.msg->setURL(ingressState_.url, /*strict=*/false);
}

void HTTP1xLlCodec::onParserError() {
  const llhttp_errno_t err = parser_->error();
  std::string errorMsg;
  if (!ingressState_.errorMessage.empty()) {
    errorMsg = ingressState_.errorMessage;
  } else {
    const char* reason = parser_->errorReason();
    errorMsg = folly::to<std::string>("Error parsing message: ",
                                      llhttp_errno_name(err),
                                      " ",
                                      reason ? reason : "");
  }
  if (ingressState_.errorContext.empty()) {
    ingressState_.errorContext = facebook::detail::llhttpErrToContext(err);
  }
  if (!ingressState_.errorContext.empty()) {
    errorMsg = folly::to<std::string>(
        errorMsg, " [Context]=", ingressState_.errorContext);
  }
  HTTPException error(HTTPException::Direction::INGRESS, errorMsg);
  if (!ingressState_.errorContext.empty()) {
    error.setContext(ingressState_.errorContext);
  }

  if (ingressState_.msg) {
    backfillPartialRequest();
    if (ingressState_.msg->isRequest() || ingressState_.msg->isResponse()) {
      error.setPartialMsg(std::move(ingressState_.msg));
    }
  }
  if (isDownstream(transportDirection_) &&
      egressState_.txnID < ingressState_.txnID) {
    error.setHttpStatusCode(400);
  }
  if (err == HPE_INVALID_EOF_STATE) {
    error.setProxygenError(kErrorEOF);
  } else if (isParsingHeaders()) {
    error.setProxygenError(kErrorParseHeader);
  } else if (isBody() || isParsingTrailers()) {
    error.setProxygenError(kErrorParseBody);
  } else {
    error.setProxygenError(kErrorUnknown);
  }
  callback_->onError(ingressState_.txnID, error);
}

int HTTP1xLlCodec::setErrorContext(std::string context) {
  ingressState_.errorContext = std::move(context);
  return -1;
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

void HTTP1xLlCodec::addDateHeader(folly::IOBufQueue& writeBuf, size_t& len) {
  appendLiteral(writeBuf, len, "Date: ");
  appendString(writeBuf, len, HTTPMessage::formatDateHeader());
  appendString(writeBuf, len, CRLF);
}

std::string HTTP1xLlCodec::generateWebsocketKey() const {
  std::array<unsigned char, 16> arr{};
  folly::Random::secureRandom(arr.data(), arr.size());
  return folly::base64Encode(std::string_view((char*)arr.data(), arr.size()));
}

void HTTP1xLlCodec::serializeWebsocketHeader(folly::IOBufQueue& writeBuf,
                                             size_t& len,
                                             bool upstream) {
  if (upstream) {
    appendLiteral(writeBuf, len, "Upgrade: ");
    appendString(writeBuf, len, kUpgradeToken);
    appendString(writeBuf, len, CRLF);
    connectionState_.upgradeHeader = std::string(kUpgradeToken);

    auto key = generateWebsocketKey();
    appendLiteral(writeBuf, len, "Sec-WebSocket-Key: ");
    appendString(writeBuf, len, key);
    appendString(writeBuf, len, CRLF);
    DCHECK(connectionState_.websocketAccept.empty());
    connectionState_.websocketAccept = generateWebsocketAccept(key);
  } else {
    appendLiteral(writeBuf, len, "Upgrade: ");
    appendString(writeBuf, len, kUpgradeToken);
    appendString(writeBuf, len, CRLF);

    appendLiteral(writeBuf, len, "Sec-WebSocket-Accept: ");
    appendString(writeBuf, len, connectionState_.websocketAccept);
    appendString(writeBuf, len, CRLF);
  }
}

void HTTP1xLlCodec::generateHeader(
    folly::IOBufQueue& writeBuf,
    StreamID txn,
    const HTTPMessage& msg,
    bool eom,
    HTTPHeaderSize* size,
    const folly::Optional<HTTPHeaders>& extraHeaders) {
  if (connectionState_.keepAlive && connectionState_.disableKeepalivePending) {
    connectionState_.keepAlive = false;
  }
  const bool upstream = isUpstream(transportDirection_);
  const bool downstream = !upstream;
  bool expectsNoResponseBody = false;
  if (upstream) {
    DCHECK_EQ(txn, egressState_.txnID);
    if (!connectionState_.upgradeHeader.empty()) {
      LOG(DFATAL) << "Attempted to pipeline HTTP request with pending upgrade";
      connectionState_.upgradeHeader.clear();
      connectionState_.upgradePhase = UpgradePhase::NONE;
    }
    egressState_.messagePending = true;
    ingressState_.messagePending = true;
    connectionState_.connectRequest =
        (msg.getMethod() == HTTPMethod::CONNECT) &&
        !msg.isEgressWebsocketUpgrade();
    connectionState_.headRequest = (msg.getMethod() == HTTPMethod::HEAD);
  } else {
    if ((txn != egressState_.txnID + 1 &&
         !(txn == egressState_.txnID && egressState_.is1xxResponse)) ||
        (txn > ingressState_.txnID)) {
      LOG(DFATAL) << "Out of order, duplicate or premature HTTP response";
    }
    if (!egressState_.is1xxResponse) {
      ++egressState_.txnID;
    }
    egressState_.is1xxResponse =
        msg.is1xxResponse() || msg.isEgressWebsocketUpgrade();

    expectsNoResponseBody =
        connectionState_.connectRequest || connectionState_.headRequest ||
        RFC2616::responseBodyMustBeEmpty(msg.getStatusCode());
  }

  int statusCode = 0;
  folly::StringPiece statusMessage;
  if (downstream) {
    statusCode = msg.getStatusCode();
    statusMessage = msg.getStatusMessage();

    if (msg.isEgressWebsocketUpgrade()) {
      statusCode = 101;
      statusMessage = HTTPMessage::getDefaultReason(101);
    }
    if (isUpgradePending() && statusCode == 101) {
      connectionState_.upgradePhase = UpgradePhase::UPGRADED;
    } else if (isUpgradePending() && statusCode >= 200) {
      connectionState_.upgradePhase = UpgradePhase::NONE;
    } else if (connectionState_.connectRequest && isUpgradeComplete() &&
               statusCode >= 300) {
      connectionState_.upgradePhase = UpgradePhase::NONE;
      connectionState_.keepAlive = false;
    }
  } else {
    if (connectionState_.connectRequest || msg.isEgressWebsocketUpgrade()) {
      connectionState_.upgradePhase = UpgradePhase::PENDING;
    }
  }

  egressState_.chunked = msg.getIsChunked() && !isUpgrade();
  egressState_.finalChunkWritten = false;
  std::pair<uint8_t, uint8_t> version = msg.getHTTPVersion();
  if (version > HTTPMessage::kHTTPVersion11) {
    version = HTTPMessage::kHTTPVersion11;
  }

  size_t len = 0;
  switch (transportDirection_) {
    case TransportDirection::DOWNSTREAM:
      DCHECK_NE(statusCode, 0);
      if (version == HTTPMessage::kHTTPVersion09) {
        return;
      }
      if (force1_1_ && version < HTTPMessage::kHTTPVersion11) {
        version = HTTPMessage::kHTTPVersion11;
      }
      appendLiteral(writeBuf, len, "HTTP/");
      appendUint(writeBuf, len, version.first);
      appendLiteral(writeBuf, len, ".");
      appendUint(writeBuf, len, version.second);
      appendLiteral(writeBuf, len, " ");
      appendUint(writeBuf, len, statusCode);
      appendLiteral(writeBuf, len, " ");
      appendString(writeBuf, len, statusMessage);
      break;
    case TransportDirection::UPSTREAM:
      if (force1_1_ && version < HTTPMessage::kHTTPVersion11) {
        version = HTTPMessage::kHTTPVersion11;
      }
      if (msg.isEgressWebsocketUpgrade()) {
        appendString(writeBuf, len, methodToString(HTTPMethod::GET));
      } else {
        appendString(writeBuf, len, msg.getMethodString());
      }
      appendLiteral(writeBuf, len, " ");
      if (connectionState_.connectRequest && msg.getURL().empty()) {
        appendString(
            writeBuf, len, msg.getHeaders().getSingleOrEmpty(HTTP_HEADER_HOST));
      } else {
        appendString(writeBuf, len, msg.getURL());
      }
      if (version != HTTPMessage::kHTTPVersion09) {
        appendLiteral(writeBuf, len, " HTTP/");
        appendUint(writeBuf, len, version.first);
        appendLiteral(writeBuf, len, ".");
        appendUint(writeBuf, len, version.second);
      }
      egressState_.mayChunk = (version.first == 1) && (version.second >= 1);
      break;
  }
  appendString(writeBuf, len, CRLF);

  if (connectionState_.keepAlive &&
      (!msg.wantsKeepalive() || version.first < 1 ||
       (downstream && version == HTTPMessage::kHTTPVersion10 &&
        egressState_.peerRequestedKeepalive != KeepaliveRequested::ENABLED))) {
    connectionState_.keepAlive = false;
  }
  egressState_.chunked &= egressState_.mayChunk;
  if (version == HTTPMessage::kHTTPVersion09) {
    parser_->setHttpVersion(0, 9);
    return;
  }
  folly::StringPiece deferredContentLength;
  bool hasTransferEncodingChunked = false;
  bool hasDateHeader = false;
  bool hasUpgradeHeader = false;
  std::vector<folly::StringPiece> connectionTokens;
  size_t lastConnectionToken = 0;
  bool egressWebsocketUpgrade = msg.isEgressWebsocketUpgrade();
  bool hasUpgradeTokeninConnection = false;
  auto headerEncoder = [&](HTTPHeaderCode code,
                           folly::StringPiece header,
                           folly::StringPiece value) {
    if (code == HTTP_HEADER_CONTENT_LENGTH) {
      deferredContentLength = value;
      return;
    } else if (code == HTTP_HEADER_CONNECTION &&
               (!egressState_.is1xxResponse || egressWebsocketUpgrade)) {
      static const std::string kClose = "close";
      static const std::string kKeepAlive = "keep-alive";
      folly::split(',', value, connectionTokens);
      for (auto curConnectionToken = lastConnectionToken;
           curConnectionToken < connectionTokens.size();
           curConnectionToken++) {
        auto token = trimWhitespace(connectionTokens[curConnectionToken]);
        if (caseInsensitiveEqual(token, "upgrade")) {
          hasUpgradeTokeninConnection = true;
        }
        if (caseInsensitiveEqual(token, kClose)) {
          connectionState_.keepAlive = false;
        } else if (!caseInsensitiveEqual(token, kKeepAlive)) {
          connectionTokens[lastConnectionToken++] = token;
        }
      }
      connectionTokens.resize(lastConnectionToken);

      return;
    } else if (code == HTTP_HEADER_UPGRADE) {
      hasUpgradeHeader = true;
      if (upstream) {
        connectionState_.upgradeHeader = value.str();
      }
    } else if (!hasTransferEncodingChunked &&
               code == HTTP_HEADER_TRANSFER_ENCODING) {
      if (!caseInsensitiveEqual(value, kChunked)) {
        return;
      }
      hasTransferEncodingChunked = true;
      if (!egressState_.mayChunk) {
        return;
      }
    } else if (!hasDateHeader && code == HTTP_HEADER_DATE) {
      hasDateHeader = true;
    } else if (egressWebsocketUpgrade &&
               code == HTTP_HEADER_SEC_WEBSOCKET_KEY) {
      return;
    } else if (egressWebsocketUpgrade &&
               code == HTTP_HEADER_SEC_WEBSOCKET_ACCEPT) {
      return;
    }
    if (value.find_first_of("\r\n") != std::string::npos) {
      return;
    }
    size_t lineLen = header.size() + value.size() + 4;
    auto writable =
        writeBuf.preallocate(lineLen, std::max(lineLen, size_t(2000)));
    char* dst = (char*)writable.first;
    memcpy(dst, header.data(), header.size());
    dst += header.size();
    *dst++ = ':';
    *dst++ = ' ';
    memcpy(dst, value.data(), value.size());
    dst += value.size();
    *dst++ = '\r';
    *dst = '\n';
    DCHECK_EQ(size_t(++dst - (char*)writable.first), lineLen);
    writeBuf.postallocate(lineLen);
    len += lineLen;
  };
  msg.getHeaders().forEachWithCode(headerEncoder);
  if (extraHeaders) {
    extraHeaders->forEachWithCode(headerEncoder);
  }

  bool bodyCheck = (downstream && !expectsNoResponseBody && !isUpgrade()) ||
                   (upstream && ((msg.getMethod() == HTTPMethod::POST) ||
                                 egressState_.chunked));

  egressState_.chunked &= hasTransferEncodingChunked;
  if (bodyCheck && !egressState_.chunked && deferredContentLength.empty()) {
    if (!hasTransferEncodingChunked && egressState_.mayChunk) {
      appendLiteral(writeBuf, len, "Transfer-Encoding: chunked\r\n");
      egressState_.chunked = true;
    } else {
      connectionState_.keepAlive = false;
    }
  }
  if (downstream && !hasDateHeader) {
    addDateHeader(writeBuf, len);
  }

  if (msg.isEgressWebsocketUpgrade()) {
    if (!hasUpgradeHeader) {
      serializeWebsocketHeader(writeBuf, len, upstream);
      if (!hasUpgradeTokeninConnection) {
        connectionTokens.push_back(kUpgradeConnectionToken);
        lastConnectionToken++;
      }
    } else {
      LOG(ERROR) << folly::to<std::string>(
          "Not serializing headers. "
          "Upgrade headers present/txn: ",
          hasUpgradeHeader,
          txn);
    }
  }

  if (!egressState_.is1xxResponse || upstream || !connectionTokens.empty()) {
    appendLiteral(writeBuf, len, "Connection: ");
    if (connectionTokens.size() > 0) {
      appendString(writeBuf, len, folly::join(", ", connectionTokens));
    }
    if (!egressState_.is1xxResponse) {
      if (connectionTokens.size() > 0) {
        appendString(writeBuf, len, ", ");
      }
      if (connectionState_.keepAlive) {
        appendLiteral(writeBuf, len, "keep-alive");
      } else {
        appendLiteral(writeBuf, len, "close");
      }
    }
    appendString(writeBuf, len, CRLF);
  }

  if (!deferredContentLength.empty()) {
    appendLiteral(writeBuf, len, "Content-Length: ");
    appendString(writeBuf, len, deferredContentLength);
    appendString(writeBuf, len, CRLF);
  }
  appendString(writeBuf, len, CRLF);
  if (eom) {
    len += generateEOM(writeBuf, txn);
  }

  if (size) {
    size->compressed = len;
    size->uncompressed = len;
  }
}

size_t HTTP1xLlCodec::generateBody(folly::IOBufQueue& writeBuf,
                                   StreamID txn,
                                   std::unique_ptr<folly::IOBuf> chain,
                                   folly::Optional<uint8_t> /*padding*/,
                                   bool eom) {
  DCHECK_EQ(txn, egressState_.txnID);
  size_t buflen = 0;
  size_t totLen = 0;
  if (chain) {
    buflen = chain->computeChainDataLength();
    totLen = buflen;
  }
  if (totLen == 0) {
    if (eom) {
      totLen += generateEOM(writeBuf, txn);
    }
    return totLen;
  }

  if (egressState_.chunked && !egressState_.chunkOpen) {
    char chunkLenBuf[32];
    int rc = snprintf(chunkLenBuf, sizeof(chunkLenBuf), "%zx\r\n", buflen);
    CHECK_GT(rc, 0);
    CHECK_LT(size_t(rc), sizeof(chunkLenBuf));

    writeBuf.append(chunkLenBuf, rc);
    totLen += rc;

    writeBuf.append(std::move(chain));
    writeBuf.append(CRLF.data(), CRLF.size());
    totLen += CRLF.size();
  } else {
    writeBuf.append(std::move(chain));
  }
  if (eom) {
    totLen += generateEOM(writeBuf, txn);
  }

  return totLen;
}

size_t HTTP1xLlCodec::generateChunkHeader(folly::IOBufQueue& writeBuf,
                                          StreamID /*txn*/,
                                          size_t length) {

  CHECK(length) << "use sendEOM to terminate the message using the "
                << "standard zero-length chunk. Don't "
                << "send zero-length chunks using this API.";
  if (egressState_.chunked) {
    CHECK(!egressState_.chunkOpen);
    egressState_.chunkOpen = true;
    char chunkLenBuf[32];
    int rc = snprintf(chunkLenBuf, sizeof(chunkLenBuf), "%zx\r\n", length);
    CHECK_GT(rc, 0);
    CHECK_LT(size_t(rc), sizeof(chunkLenBuf));

    writeBuf.append(chunkLenBuf, rc);
    return rc;
  }

  return 0;
}

size_t HTTP1xLlCodec::generateChunkTerminator(folly::IOBufQueue& writeBuf,
                                              StreamID /*txn*/) {
  if (egressState_.chunked && egressState_.chunkOpen) {
    egressState_.chunkOpen = false;
    writeBuf.append(CRLF.data(), CRLF.size());
    return CRLF.size();
  }

  return 0;
}

size_t HTTP1xLlCodec::generateTrailers(folly::IOBufQueue& writeBuf,
                                       StreamID txn,
                                       const HTTPHeaders& trailers) {
  DCHECK_EQ(txn, egressState_.txnID);
  size_t len = 0;
  if (egressState_.chunked) {
    CHECK(!egressState_.chunkOpen);
    appendLiteral(writeBuf, len, "0\r\n");
    egressState_.finalChunkWritten = true;
    trailers.forEach([&](const std::string& trailer, const std::string& value) {
      appendString(writeBuf, len, trailer);
      appendLiteral(writeBuf, len, ": ");
      appendString(writeBuf, len, value);
      appendString(writeBuf, len, CRLF);
    });
    len += generateEOM(writeBuf, txn);
  }
  return len;
}

size_t HTTP1xLlCodec::generateEOM(folly::IOBufQueue& writeBuf, StreamID txn) {
  DCHECK_EQ(txn, egressState_.txnID);
  size_t len = 0;
  if (egressState_.chunked) {
    CHECK(!egressState_.chunkOpen);
    if (connectionState_.headRequest && isDownstream(transportDirection_)) {
      egressState_.finalChunkWritten = true;
    } else {

      if (!egressState_.finalChunkWritten) {
        egressState_.finalChunkWritten = true;
        appendLiteral(writeBuf, len, "0\r\n");
      }
      appendString(writeBuf, len, CRLF);
    }
  }
  egressState_.messagePending = false;
  return len;
}

size_t HTTP1xLlCodec::generateRstStream(folly::IOBufQueue& /*writeBuf*/,
                                        StreamID /*txn*/,
                                        ErrorCode /*statusCode*/) {

  connectionState_.keepAlive = false;
  return 0;
}

size_t HTTP1xLlCodec::generateGoaway(folly::IOBufQueue&,
                                     StreamID lastStream,
                                     ErrorCode error,
                                     std::unique_ptr<folly::IOBuf>) {

  if (isUpstream(transportDirection_) ||
      connectionState_.disableKeepalivePending ||
      lastStream != HTTPCodec::MaxStreamID || error != ErrorCode::NO_ERROR) {
    connectionState_.keepAlive = false;
  } else {
    connectionState_.disableKeepalivePending = true;
  }
  return 0;
}

size_t HTTP1xLlCodec::generatePadding(folly::IOBufQueue&, StreamID, uint16_t) {
  return 0;
}

size_t HTTP1xLlCodec::generateImmediateGoaway(folly::IOBufQueue&,
                                              ErrorCode,
                                              std::unique_ptr<folly::IOBuf>) {
  connectionState_.keepAlive = false;
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
    value.on_url_complete = onURLCompleteCB;
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
  // A request line ending right after the URL is HTTP/0.9
  const char* urlEnd = buf + len;
  const char* bufEnd =
      reinterpret_cast<const char*>(ingressState_.buf->data()) +
      ingressState_.buf->length();
  if (urlEnd < bufEnd && (*urlEnd == '\r' || *urlEnd == '\n')) {
    ingressState_.http09Request = true;
  }
  return 0;
}

int HTTP1xLlCodec::onProtocol(const char* buf, size_t len) {
  if (!kHttpProtocol.contains(folly::StringPiece(buf, len))) {
    return setErrorContext("invalid-protocol");
  }
  return 0;
}

int HTTP1xLlCodec::onURLComplete() {
  return ingressState_.http09Request ? HPE_PAUSED : 0;
}

int HTTP1xLlCodec::onStatus(const char* buf, size_t len) {
  if (int rc = trackHeaderBytes(len)) {
    return rc;
  }
  ingressState_.reason.append(buf, len);
  return 0;
}

void HTTP1xLlCodec::endHttp09RequestLine() {
  // HTTP/0.9 requests have no header block, but llhttp parses one after the
  // request line, so give it an empty one instead of the bytes that follow
  ingressState_.http09Request = false;
  parser_->resume();
  parser_->execute(CRLF.data(), CRLF.size());
}

int HTTP1xLlCodec::trackHeaderBytes(size_t len) {
  ingressState_.headerSize.compressed += len;
  ingressState_.headerSize.uncompressed += len;
  if (ingressState_.headerSize.compressed > kHeaderByteCap) {
    return setErrorContext("header-overflow");
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
    return setErrorContext("invalid-transfer-encoding");
  }
  if (parser_->contentLength() > kMaxDataSize) {
    return setErrorContext("content-length-exceeds-max-limit");
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

    ParseURL parseUrl = ingressState_.msg->setURL(std::move(ingressState_.url),
                                                  /*strict=*/true);
    ingressState_.url.clear();
    if (!parseUrl.valid()) {
      // The query is where a URL carries its credentials
      LOG(ERROR)
          << "Invalid URL: "
          << CodecUtil::stripQueryAndFragment(ingressState_.msg->getURL());
      return setErrorContext("invalid-url");
    }
    if (parseUrl.hasHost() && hasUserInfo(ingressState_.msg->getURL())) {
      return setErrorContext("url-userinfo");
    }
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
        return setErrorContext("client-server-upgrade-mismatch");
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
        return setErrorContext("websocket-accept-key-mismatch");
      }
    } else if (upgradeRequest) {
      ingressState_.msg->setIngressWebsocketUpgrade();
      if (!connectionState_.websocketAccept.empty()) {
        LOG(ERROR) << "ws accept key already set: '"
                   << connectionState_.websocketAccept << "'";
        return setErrorContext("websocket-accept-key-already-set");
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
  if (len == 0) {
    return 0;
  }
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
    return setErrorContext("chunk-size-exceeds-max-limit");
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
  auto* codec = getCodec(parser);
  try {
    return codec->onMessageBegin();
  } catch (const std::exception& ex) {
    codec->ingressState_.errorMessage = ex.what();
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
  auto* codec = getCodec(parser);
  try {
    return codec->onURL(data, len);
  } catch (const std::exception& ex) {
    codec->ingressState_.errorMessage = ex.what();
    return -1;
  }
}

int HTTP1xLlCodec::onURLCompleteCB(llhttp_t* parser) {
  auto* codec = getCodec(parser);
  try {
    return codec->onURLComplete();
  } catch (const std::exception& ex) {
    codec->ingressState_.errorMessage = ex.what();
    return -1;
  }
}

int HTTP1xLlCodec::onStatusCB(llhttp_t* parser, const char* data, size_t len) {
  auto* codec = getCodec(parser);
  try {
    return codec->onStatus(data, len);
  } catch (const std::exception& ex) {
    codec->ingressState_.errorMessage = ex.what();
    return -1;
  }
}

int HTTP1xLlCodec::onHeaderFieldCB(llhttp_t* parser,
                                   const char* data,
                                   size_t len) {
  auto* codec = getCodec(parser);
  try {
    return codec->onHeaderField(data, len);
  } catch (const std::exception& ex) {
    codec->ingressState_.errorMessage = ex.what();
    return -1;
  }
}

int HTTP1xLlCodec::onHeaderValueCB(llhttp_t* parser,
                                   const char* data,
                                   size_t len) {
  auto* codec = getCodec(parser);
  try {
    return codec->onHeaderValue(data, len);
  } catch (const std::exception& ex) {
    codec->ingressState_.errorMessage = ex.what();
    return -1;
  }
}

int HTTP1xLlCodec::onHeaderValueCompleteCB(llhttp_t* parser) {
  auto* codec = getCodec(parser);
  try {
    return codec->onHeaderValueComplete();
  } catch (const std::exception& ex) {
    codec->ingressState_.errorMessage = ex.what();
    return -1;
  }
}

int HTTP1xLlCodec::onHeadersCompleteCB(llhttp_t* parser) {
  auto* codec = getCodec(parser);
  try {
    return codec->onHeadersComplete();
  } catch (const std::exception& ex) {
    codec->ingressState_.errorMessage = ex.what();
    return -1;
  }
}

int HTTP1xLlCodec::onBodyCB(llhttp_t* parser, const char* data, size_t len) {
  auto* codec = getCodec(parser);
  try {
    return codec->onBody(data, len);
  } catch (const std::exception& ex) {
    codec->ingressState_.errorMessage = ex.what();
    return -1;
  }
}

int HTTP1xLlCodec::onChunkHeaderCB(llhttp_t* parser) {
  auto* codec = getCodec(parser);
  try {
    return codec->onChunkHeader();
  } catch (const std::exception& ex) {
    codec->ingressState_.errorMessage = ex.what();
    return -1;
  }
}

int HTTP1xLlCodec::onChunkCompleteCB(llhttp_t* parser) {
  auto* codec = getCodec(parser);
  try {
    return codec->onChunkComplete();
  } catch (const std::exception& ex) {
    codec->ingressState_.errorMessage = ex.what();
    return -1;
  }
}

int HTTP1xLlCodec::onMessageCompleteCB(llhttp_t* parser) {
  auto* codec = getCodec(parser);
  try {
    return codec->onMessageComplete();
  } catch (const std::exception& ex) {
    codec->ingressState_.errorMessage = ex.what();
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
