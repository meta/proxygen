/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <proxygen/lib/http/codec/CodecUtil.h>
#include <proxygen/lib/http/codec/HQControlCodec.h>

#include <proxygen/lib/http/HTTP3ErrorCode.h>
#include <proxygen/lib/http/codec/HQUtils.h>

#include <folly/Random.h>

namespace {
using namespace proxygen::hq;

uint64_t drainingId(proxygen::TransportDirection dir) {
  if (dir == proxygen::TransportDirection::DOWNSTREAM) {
    return kMaxClientBidiStreamId;
  } else {
    return kMaxPushId + 1;
  }
}

} // namespace

namespace proxygen::hq {

using namespace folly::io;

// HTTPPriority::urgency is a 3-bit field, so it never exceeds kMaxPriority (7)
// and always fits within the QUIC priority range.
static_assert(kMaxPriority <= quic::kDefaultMaxPriority);

ParseResult HQControlCodec::checkFrameAllowed(FrameType type) {
  switch (type) {
    case hq::FrameType::DATA:
    case hq::FrameType::HEADERS:
    case hq::FrameType::PUSH_PROMISE:
    case hq::FrameType::WEBTRANSPORT_BIDI:
      setParseErrorContext(
          "data-frame-on-control-stream",
          folly::to<std::string>("type=", getFrameTypeString(type)));
      return HTTP3::ErrorCode::HTTP_FRAME_UNEXPECTED;
    default:
      break;
  }

  if (getStreamType() == hq::UnidirectionalStreamType::CONTROL) {
    // SETTINGS MUST be the first frame on an HQ Control Stream
    if (!receivedSettings_ && type != hq::FrameType::SETTINGS) {
      setParseErrorContext(
          "settings-not-first-frame",
          folly::to<std::string>("type=", getFrameTypeString(type)));
      return HTTP3::ErrorCode::HTTP_MISSING_SETTINGS;
    }
    // multiple SETTINGS frames are not allowed
    if (receivedSettings_ && type == hq::FrameType::SETTINGS) {
      setParseErrorContext("duplicate-settings");
      return HTTP3::ErrorCode::HTTP_FRAME_UNEXPECTED;
    }
    // A client MUST treat the receipt of a MAX_PUSH_ID frame as a connection
    // error of type HTTP_FRAME_UNEXPECTED
    if (transportDirection_ == TransportDirection::UPSTREAM &&
        type == hq::FrameType::MAX_PUSH_ID) {
      setParseErrorContext("max-push-id-on-upstream-control-stream");
      return HTTP3::ErrorCode::HTTP_FRAME_UNEXPECTED;
    }

    // PRIORITY_UPDATE is downstream control codec only
    if (transportDirection_ == TransportDirection::UPSTREAM &&
        (type == hq::FrameType::PRIORITY_UPDATE ||
         type == hq::FrameType::PUSH_PRIORITY_UPDATE ||
         type == hq::FrameType::FB_PUSH_PRIORITY_UPDATE ||
         type == hq::FrameType::FB_PRIORITY_UPDATE)) {
      setParseErrorContext(
          "priority-update-on-upstream-control-stream",
          folly::to<std::string>("type=", getFrameTypeString(type)));
      return HTTP3::ErrorCode::HTTP_FRAME_UNEXPECTED;
    }
  }

  return folly::none;
}

ParseResult HQControlCodec::parseCancelPush(Cursor& cursor,
                                            const FrameHeader& header) {
  PushId outPushId;
  auto res = hq::parseCancelPush(cursor, header, outPushId);
  return res;
}

ParseResult HQControlCodec::parseSettings(Cursor& cursor,
                                          const FrameHeader& header) {
  VLOG(4) << "parsing SETTINGS frame length=" << header.length;
  CHECK(isIngress());
  std::deque<SettingPair> outSettings;
  receivedSettings_ = true;
  auto res = hq::parseSettings(cursor, header, outSettings);
  VLOG(4) << "Received n=" << outSettings.size() << " settings";
  if (res) {
    return res;
  }

  CHECK(isIngress());
  auto& ingressSettings = settings_;
  SettingsList settingsList;
  for (auto& setting : outSettings) {
    switch (setting.first) {
      case hq::SettingId::HEADER_TABLE_SIZE:
      case hq::SettingId::MAX_HEADER_LIST_SIZE:
      case hq::SettingId::QPACK_BLOCKED_STREAMS:
      case hq::SettingId::H3_WT_MAX_SESSIONS:
      case hq::SettingId::WT_INITIAL_MAX_DATA:
      case hq::SettingId::WT_INITIAL_MAX_STREAMS_UNI:
      case hq::SettingId::WT_INITIAL_MAX_STREAMS_BIDI:
        break;
      case hq::SettingId::ENABLE_CONNECT_PROTOCOL:
      case hq::SettingId::H3_DATAGRAM:
      case hq::SettingId::H3_DATAGRAM_RFC:
      case hq::SettingId::ENABLE_WEBTRANSPORT:
      case hq::SettingId::WT_ENABLED:
        // only 0/1 are legal
        if (setting.second > 1) {
          setParseErrorContext(
              "boolean-setting-out-of-range",
              folly::to<std::string>("id=",
                                     static_cast<uint64_t>(setting.first),
                                     " value=",
                                     setting.second));
          return HTTP3::ErrorCode::HTTP_SETTINGS_ERROR;
        }
        break;
      default:
        continue; // ignore unknown settings
    }
    auto httpSettingId = hqToHttpSettingsId(setting.first);
    ingressSettings.setSetting(*httpSettingId, setting.second);
    settingsList.push_back(*ingressSettings.getSetting(*httpSettingId));
  }

  if (callback_) {
    callback_->onSettings(settingsList);
  }
  return folly::none;
}

ParseResult HQControlCodec::parseGoaway(Cursor& cursor,
                                        const FrameHeader& header) {
  quic::StreamId outStreamId;
  auto res = hq::parseGoaway(cursor, header, outStreamId);
  if (!res && callback_) {
    callback_->onGoaway(outStreamId, ErrorCode::NO_ERROR);
  }
  return res;
}

ParseResult HQControlCodec::parseMaxPushId(Cursor& cursor,
                                           const FrameHeader& header) {
  quic::StreamId outPushId;
  auto res = hq::parseMaxPushId(cursor, header, outPushId);
  return res;
}

ParseResult HQControlCodec::parsePriorityUpdate(Cursor& cursor,
                                                const FrameHeader& header) {
  HTTPCodec::StreamID prioritizedElement;
  HTTPPriority priorityUpdate;
  auto res = hq::parsePriorityUpdate(
      cursor, header, prioritizedElement, priorityUpdate);
  if (!res) {
    callback_->onPriority(folly::to<quic::StreamId>(prioritizedElement),
                          priorityUpdate);
  }
  return res;
}

ParseResult HQControlCodec::parsePushPriorityUpdate(Cursor& cursor,
                                                    const FrameHeader& header) {
  HTTPCodec::StreamID prioritizedElement;
  HTTPPriority priorityUpdate;
  auto res = hq::parsePriorityUpdate(
      cursor, header, prioritizedElement, priorityUpdate);
  if (!res) {
    callback_->onPushPriority(folly::to<PushId>(prioritizedElement),
                              priorityUpdate);
  }
  return res;
}

bool HQControlCodec::isWaitingToDrain() const {
  return (!doubleGoaway_ && !sentGoaway_) ||
         (doubleGoaway_ && sentGoaway_ && !sentFinalGoaway_);
}

uint64_t HQControlCodec::finalGoawayId() {
  if (transportDirection_ == TransportDirection::DOWNSTREAM) {
    return minUnseenStreamID_;
  } else {
    return minUnseenPushID_;
  }
}

size_t HQControlCodec::generateGoaway(
    folly::IOBufQueue& writeBuf,
    StreamID minUnseenId,
    ErrorCode statusCode,
    std::unique_ptr<folly::IOBuf> /*debugData*/) {
  if (sentFinalGoaway_) {
    return 0;
  }
  if (minUnseenId == HTTPCodec::MaxStreamID) {
    if (statusCode != ErrorCode::NO_ERROR || isWaitingToDrain()) {
      // Non-draining goaway, but the caller didn't know the ID
      // HQSession doesn't use this path now
      minUnseenId = finalGoawayId();
      sentFinalGoaway_ = true;
    } else {
      // Draining goaway
      minUnseenId = drainingId(transportDirection_);
    }
  } else {
    // Non-draining goaway, caller supplied ID
    sentFinalGoaway_ = true;
  }
  VLOG(4) << "generating GOAWAY minUnseenId=" << minUnseenId
          << " statusCode=" << uint32_t(statusCode);

  DCHECK_GE(egressGoawayAck_, minUnseenId);
  egressGoawayAck_ = minUnseenId;
  auto writeRes = hq::writeGoaway(writeBuf, minUnseenId);
  LOG_IF(FATAL, !writeRes) << "::writeGoaway err minUnseenId=" << minUnseenId;
  sentGoaway_ = true;
  return *writeRes;
}

size_t HQControlCodec::generateSettings(folly::IOBufQueue& writeBuf) {
  CHECK(!sentSettings_);
  sentSettings_ = true;
  std::deque<hq::SettingPair> settings;
  for (const auto& setting : getEgressSettings()->getAllSettings()) {
    auto id = httpToHqSettingsId(setting.id); // unknown id returns folly::none
    if (id) {
      settings.emplace_back(*id, (SettingValue)setting.value);
    }
  }
  // add a random setting from the greasing pool
  settings.emplace_back(
      static_cast<SettingId>(*getGreaseId(folly::Random::rand32(16))),
      static_cast<SettingValue>(0xFACEB00C));
  auto writeRes = writeSettings(writeBuf, settings);
  LOG_IF(FATAL, !writeRes) << "error writing settings frame";
  return *writeRes;
}

size_t HQControlCodec::generatePriority(folly::IOBufQueue& writeBuf,
                                        StreamID stream,
                                        HTTPPriority priority) {
  auto updateString = folly::to<std::string>(
      "u=", priority.urgency, (priority.incremental ? ",i" : ""));
  auto writeRet = hq::writePriorityUpdate(writeBuf, stream, updateString);
  if (!writeRet) {
    LOG(ERROR) << "error writing priority update, stream=" << stream
               << ", priority=" << updateString;
    return 0;
  }
  return *writeRet;
}

size_t HQControlCodec::generatePushPriority(folly::IOBufQueue& writeBuf,
                                            StreamID pushId,
                                            HTTPPriority priority) {
  auto updateString = folly::to<std::string>(
      "u=", priority.urgency, (priority.incremental ? ",i" : ""));
  auto writeRet = hq::writePushPriorityUpdate(writeBuf, pushId, updateString);
  if (!writeRet) {
    LOG(ERROR) << "error writing push priority update, pushId=" << pushId
               << ", priority=" << updateString;
    return 0;
  }
  return *writeRet;
}

} // namespace proxygen::hq
