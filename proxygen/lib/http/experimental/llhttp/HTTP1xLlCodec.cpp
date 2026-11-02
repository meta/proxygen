/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <proxygen/lib/http/experimental/llhttp/HTTP1xLlCodec.h>

namespace proxygen {

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

size_t HTTP1xLlCodec::generatePadding(folly::IOBufQueue&, StreamID, uint16_t) {
  return 0;
}

void HTTP1xLlCodec::setReleaseEgressAfterRequest(bool releaseEgress) {
  egressState_.releaseAfterRequest = releaseEgress;
}

bool HTTP1xLlCodec::supportsNextProtocol(folly::StringPiece alpn) {
  return alpn.size() == 8 && (alpn == "http/1.0" || alpn == "http/1.1");
}

void HTTP1xLlCodec::setParserPaused(bool /*paused*/) {
}

bool HTTP1xLlCodec::isParserPaused() const {
  return false;
}

size_t HTTP1xLlCodec::onIngress(const folly::IOBuf& /*buf*/) {
  return 0;
}

size_t HTTP1xLlCodec::onIngressImpl(const folly::IOBuf& /*buf*/) {
  return 0;
}

void HTTP1xLlCodec::onIngressEOF() {
}

const llhttp_settings_t* HTTP1xLlCodec::getParserSettings() {
  static const llhttp_settings_t settings = [] {
    llhttp_settings_t value;
    llhttp_settings_init(&value);
    return value;
  }();
  return &settings;
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

size_t HTTP1xLlCodec::generateImmediateGoaway(folly::IOBufQueue&,
                                              ErrorCode,
                                              std::unique_ptr<folly::IOBuf>) {
  return 0;
}

} // namespace proxygen
