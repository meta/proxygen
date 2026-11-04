/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <proxygen/lib/http/codec/HQFramedCodec.h>

#include <proxygen/lib/http/codec/HQUtils.h>
#include <proxygen/lib/utils/Logging.h>

#include <folly/io/Cursor.h>
#include <folly/tracing/ScopedTraceSection.h>

#include <algorithm>
#include <iomanip>
#include <limits>

namespace proxygen::hq {

using namespace folly::io;
using namespace folly;

namespace {
// CANCEL_PUSH, GOAWAY and MAX_PUSH_ID carry a single varint.
constexpr uint64_t kMaxIdFrameLength = 8;
constexpr uint64_t kMaxControlFrameLength = 4096;
// Partial frames in longer chains are copied to keep rescans short.
constexpr size_t kMaxPendingChainElements = 10;
constexpr size_t kPendingPayloadAllocSize = 4096;

// folly::none for frame types that are skipped.
folly::Optional<uint64_t> maxFrameLength(FrameType type) {
  switch (type) {
    case FrameType::DATA:
    case FrameType::HEADERS:
    case FrameType::PUSH_PROMISE:
      return std::numeric_limits<uint64_t>::max();
    case FrameType::CANCEL_PUSH:
    case FrameType::GOAWAY:
    case FrameType::MAX_PUSH_ID:
      return kMaxIdFrameLength;
    case FrameType::SETTINGS:
    case FrameType::PRIORITY_UPDATE:
    case FrameType::PUSH_PRIORITY_UPDATE:
    case FrameType::FB_PRIORITY_UPDATE:
    case FrameType::FB_PUSH_PRIORITY_UPDATE:
      return kMaxControlFrameLength;
    case FrameType::PADDING:
    case FrameType::WEBTRANSPORT_BIDI:
    default:
      return folly::none;
  }
}
} // namespace

ParseResult HQFramedCodec::checkFrameLength(const FrameHeader& header) {
  if (header.length > maxFrameLength(header.type).value_or(0)) {
    setParseErrorContext(
        "frame-too-large",
        folly::to<std::string>("type=", getFrameTypeString(header.type)));
    return HTTP3::ErrorCode::HTTP_FRAME_ERROR;
  }
  return folly::none;
}

size_t HQFramedCodec::bufferPayload(Cursor& cursor, size_t bufLen) {
  const uint64_t missing = curHeader_.length - pendingPayload_.chainLength();
  const size_t toCopy = std::min<uint64_t>(missing, bufLen);
  QueueAppender appender(&pendingPayload_,
                         std::min<uint64_t>(missing, kPendingPayloadAllocSize));
  appender.push(cursor, toCopy);
  cursor.skip(toCopy);
  return toCopy;
}

ParseResult HQFramedCodec::parseFrame(Cursor& cursor) {
  switch (curHeader_.type) {
    case hq::FrameType::DATA:
      return parseData(cursor, curHeader_);
    case hq::FrameType::HEADERS:
      return parseHeaders(cursor, curHeader_);
    case hq::FrameType::CANCEL_PUSH:
      return parseCancelPush(cursor, curHeader_);
    case hq::FrameType::SETTINGS:
      return parseSettings(cursor, curHeader_);
    case hq::FrameType::PUSH_PROMISE:
      return parsePushPromise(cursor, curHeader_);
    case hq::FrameType::GOAWAY:
      return parseGoaway(cursor, curHeader_);
    case hq::FrameType::MAX_PUSH_ID:
      return parseMaxPushId(cursor, curHeader_);
    case hq::FrameType::PRIORITY_UPDATE:
    case hq::FrameType::PUSH_PRIORITY_UPDATE:
    case hq::FrameType::FB_PRIORITY_UPDATE:
    case hq::FrameType::FB_PUSH_PRIORITY_UPDATE:
      return parsePriorityUpdate(cursor, curHeader_);
    default:
      LOG(DFATAL) << "Unparsed frame type=" << curHeader_.type;
      cursor.skip(curHeader_.length);
      return folly::none;
  }
}

size_t HQFramedCodec::onFramedIngress(const IOBuf& buf) {
  FOLLY_SCOPED_TRACE_SECTION("HQFramedCodec - onFramedIngress");
  if (connError_ != folly::none || parserPaused_) {
    return 0;
  }
  Cursor cursor(&buf);
  size_t parsedTot = 0;
  auto bufLen = cursor.totalLength();
  while (connError_ == folly::none && bufLen > 0 && !parserPaused_) {
    size_t parsed = 0;
    if (frameState_ == FrameState::FRAME_HEADER_TYPE) {
      auto type = quic::follyutils::decodeQuicInteger(cursor);
      if (!type) {
        break;
      }
      curHeader_.type = FrameType(type->first);
      parsed += type->second;
      parseErrorContext_ = {};
      parseErrorAdditionalInfo_ = {};
      auto res = checkFrameAllowed(curHeader_.type);
      if (res) {
        VLOG(4) << "Frame not allowed: 0x" << std::setfill('0')
                << std::setw(sizeof(uint64_t) * 2) << std::hex
                << (uint64_t)curHeader_.type << " on streamID=" << streamId_;
        // Only generic if the derived codec did not name a specific reason.
        if (parseErrorContext_.empty()) {
          setParseErrorContext(
              "frame-not-allowed",
              folly::to<std::string>("type=",
                                     getFrameTypeString(curHeader_.type)));
        }
        connError_ = res;
        break;
      }
      frameState_ = FrameState::FRAME_HEADER_LENGTH;
    } else if (frameState_ == FrameState::FRAME_HEADER_LENGTH) {
      auto length = quic::follyutils::decodeQuicInteger(cursor);
      if (!length) {
        break;
      }
      curHeader_.length = length->first;
      parsed += length->second;
      if (callback_) {
        callback_->onFrameHeader(streamId_,
                                 0, // no flags!
                                 curHeader_.length,
                                 static_cast<uint64_t>(curHeader_.type));
      }
#ifndef NDEBUG
      receivedFrameCount_++;
#endif
      pendingDataFrameBytes_ = curHeader_.length;
      if (!maxFrameLength(curHeader_.type)) {
        // Implementations MUST ignore and discard any frame that has a
        // type that is unknown
        if (callback_) {
          callback_->onUnknownFrame(streamId_,
                                    static_cast<uint64_t>(curHeader_.type));
        }
        frameState_ = curHeader_.length == 0
                          ? FrameState::FRAME_HEADER_TYPE
                          : FrameState::FRAME_PAYLOAD_STREAMING;
      } else if (curHeader_.length == 0) {
        // If the frame length is zero, call parseFrame immediately.  It is up
        // to each frame to determine whether length 0 is valid.
        connError_ = parseFrame(cursor);
        frameState_ = FrameState::FRAME_HEADER_TYPE;
      } else if (curHeader_.type == FrameType::DATA) {
        frameState_ = FrameState::FRAME_PAYLOAD_STREAMING;
      } else {
        connError_ = checkFrameLength(curHeader_);
        frameState_ = FrameState::FRAME_PAYLOAD;
      }
    } else if (frameState_ == FrameState::FRAME_PAYLOAD) {
      // Already parsed the common frame header
      const auto frameLen = curHeader_.length;
      if (pendingPayload_.empty() && bufLen >= frameLen) {
        connError_ = parseFrame(cursor);
        // if connError_ is set, it means there was a frame error,
        // so it doesn't really matter whether we have actually parsed all the
        // data or not
        parsed += curHeader_.length;
        frameState_ = FrameState::FRAME_HEADER_TYPE;
      } else if (pendingPayload_.empty() &&
                 buf.countChainElements() <= kMaxPendingChainElements) {
        break;
      } else {
        parsed += bufferPayload(cursor, bufLen);
        if (pendingPayload_.chainLength() == frameLen) {
          auto payload = pendingPayload_.move();
          Cursor payloadCursor(payload.get());
          connError_ = parseFrame(payloadCursor);
          frameState_ = FrameState::FRAME_HEADER_TYPE;
        }
      }
    } else if (bufLen > 0 &&
               frameState_ == FrameState::FRAME_PAYLOAD_STREAMING) {
      const auto chunkLen = std::min(pendingDataFrameBytes_, bufLen);
      if (curHeader_.type == FrameType::DATA) {
        FrameHeader auxDataFrameHeader;
        auxDataFrameHeader.type = FrameType::DATA;
        auxDataFrameHeader.length = chunkLen;
        connError_ = parseData(cursor, auxDataFrameHeader);
      } else {
        cursor.skip(chunkLen);
      }
      parsed += chunkLen;
      pendingDataFrameBytes_ -= chunkLen;
      if (pendingDataFrameBytes_ == 0) {
        frameState_ = FrameState::FRAME_HEADER_TYPE;
      }
    }
    CHECK_GE(bufLen, parsed);
    bufLen -= parsed;
    parsedTot += parsed;
    totalBytesParsed_ += parsed;
  }
  if (!checkConnectionError(connError_, &buf) && parserPaused_ && pauseHook_) {
    pauseHook_();
  }
  return parsedTot;
}

bool HQFramedCodec::onFramedIngressEOF() {
  if (connError_ != folly::none) {
    return false;
  } else if (parserPaused_) {
    deferredEOF_ = true;
    return false;
  } else if (frameState_ != FrameState::FRAME_HEADER_TYPE) {
    VLOG(3) << "Stream ended in the middle of a frame type=" << curHeader_.type;
    setParseErrorContext(
        "eof-in-middle-of-frame",
        folly::to<std::string>("type=", getFrameTypeString(curHeader_.type)));
    connError_ = HTTP3::ErrorCode::HTTP_FRAME_ERROR;
    checkConnectionError(connError_, nullptr);
    return false;
  }
  // Caller will fire onMessageComplete
  return true;
}

bool HQFramedCodec::checkConnectionError(ParseResult err,
                                         const folly::IOBuf* buf) {
  if (err != folly::none) {
    std::string errorMessage("Connection error");
    if (!parseErrorContext_.empty()) {
      errorMessage = folly::to<std::string>(
          errorMessage, " [Context]=", parseErrorContext_);
      if (!parseErrorAdditionalInfo_.empty()) {
        errorMessage = folly::to<std::string>(
            errorMessage, " ", parseErrorAdditionalInfo_);
      }
    }
    LOG(ERROR) << errorMessage << " with ingress=";
    if (buf) {
      VLOG(3) << IOBufPrinter::printHexFolly(buf, true);
    }
    setParserPaused(true);
    if (callback_) {
      HTTPException ex(HTTPException::Direction::INGRESS_AND_EGRESS,
                       errorMessage);
      ex.setContext(parseErrorContext_);
      ex.setHttp3ErrorCode(err.value());
      callback_->onError(kSessionStreamId, ex, false);
    }
    return true;
  }
  return false;
}

} // namespace proxygen::hq
