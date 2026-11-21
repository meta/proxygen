/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <proxygen/lib/http/webtransport/WtUtils.h>

#include <folly/io/IOBufQueue.h>
#include <folly/portability/GTest.h>
#include <proxygen/lib/http/codec/HTTPSettings.h>
#include <proxygen/lib/http/codec/webtransport/WebTransportFramer.h>

using namespace proxygen;
using proxygen::detail::WtEventVisitor;
using proxygen::detail::WtStreamManager;

namespace {

std::string coalesceToString(folly::IOBufQueue& queue) {
  auto buf = queue.move();
  return buf ? buf->toString() : std::string{};
}

} // namespace

// draft-ietf-webtrans-http3-17 §5.1
TEST(WtUtilsTest, H3WtFlowControlNeedsANonZeroLimitFromEachSide) {
  const HTTPSettings limit{{SettingsId::WT_INITIAL_MAX_DATA, 1}};
  auto enabled = [](const HTTPSettings& peer, const HTTPSettings& self) {
    return detail::h3WtFlowControlEnabled(detail::getH3WtConfig(&peer, &self));
  };
  for (auto id : {SettingsId::WT_INITIAL_MAX_STREAMS_UNI,
                  SettingsId::WT_INITIAL_MAX_STREAMS_BIDI,
                  SettingsId::WT_INITIAL_MAX_DATA}) {
    const HTTPSettings one{{id, 1}};
    EXPECT_TRUE(enabled(one, limit));
    EXPECT_TRUE(enabled(limit, one));
    const HTTPSettings zero{{id, 0}};
    EXPECT_FALSE(enabled(zero, limit));
    EXPECT_FALSE(enabled(limit, zero));
  }
  const HTTPSettings none;
  EXPECT_FALSE(enabled(none, none));
}

TEST(WtUtilsTest, SupportsH3WtUpstreamUsesLocalSettings) {
  HTTPSettings local;
  local.setSetting(SettingsId::WT_ENABLED, 1);
  local.setSetting(SettingsId::_HQ_DATAGRAM_RFC, 1);

  EXPECT_TRUE(detail::supportsH3Wt(
      TransportDirection::UPSTREAM, /*ingress=*/nullptr, &local));

  local.unsetSetting(SettingsId::WT_ENABLED);
  EXPECT_FALSE(detail::supportsH3Wt(
      TransportDirection::UPSTREAM, /*ingress=*/nullptr, &local));

  local.setSetting(SettingsId::WT_ENABLED, 1);
  local.unsetSetting(SettingsId::_HQ_DATAGRAM_RFC);
  EXPECT_FALSE(detail::supportsH3Wt(
      TransportDirection::UPSTREAM, /*ingress=*/nullptr, &local));
}

TEST(WtUtilsTest, SupportsH3WtDownstreamUsesLocalAndPeerSettings) {
  HTTPSettings client;
  client.setSetting(SettingsId::_HQ_DATAGRAM_RFC, 1);
  HTTPSettings server;
  server.setSetting(SettingsId::H3_WT_ENABLED, 1);
  server.setSetting(SettingsId::ENABLE_CONNECT_PROTOCOL, 1);
  server.setSetting(SettingsId::_HQ_DATAGRAM_RFC, 1);

  EXPECT_TRUE(
      detail::supportsH3Wt(TransportDirection::DOWNSTREAM, &client, &server));

  client.unsetSetting(SettingsId::_HQ_DATAGRAM_RFC);
  EXPECT_FALSE(
      detail::supportsH3Wt(TransportDirection::DOWNSTREAM, &client, &server));

  client.setSetting(SettingsId::_HQ_DATAGRAM_RFC, 1);
  server.unsetSetting(SettingsId::H3_WT_ENABLED);
  EXPECT_FALSE(
      detail::supportsH3Wt(TransportDirection::DOWNSTREAM, &client, &server));

  server.setSetting(SettingsId::H3_WT_ENABLED, 1);
  server.unsetSetting(SettingsId::ENABLE_CONNECT_PROTOCOL);
  EXPECT_FALSE(
      detail::supportsH3Wt(TransportDirection::DOWNSTREAM, &client, &server));

  server.setSetting(SettingsId::ENABLE_CONNECT_PROTOCOL, 1);
  server.unsetSetting(SettingsId::_HQ_DATAGRAM_RFC);
  EXPECT_FALSE(
      detail::supportsH3Wt(TransportDirection::DOWNSTREAM, &client, &server));
}

// 1) With the default protocol unset, WtEventVisitor's serialization for a
//    ResetStream event must match what writeWTResetStream produces with the
//    WT_CAPSULE wire format.
TEST(WtEventVisitorTest, ResetStream_DefaultProtocol_MatchesWtCapsule) {
  const WtStreamManager::ResetStream event{
      .streamId = 4, .err = 0x42, .reliableSize = 100};

  folly::IOBufQueue visitorBuf{folly::IOBufQueue::cacheChainLength()};
  WtEventVisitor{.egress = visitorBuf}(event);

  folly::IOBufQueue directBuf{folly::IOBufQueue::cacheChainLength()};
  writeWTResetStream(
      directBuf,
      WTResetStreamCapsule{.streamId = event.streamId,
                           .appProtocolErrorCode = uint32_t(event.err),
                           .reliableSize = event.reliableSize},
      FrameProtocol::WT_CAPSULE);

  EXPECT_EQ(coalesceToString(visitorBuf), coalesceToString(directBuf));
}

// 2) With protocol=QMUX, WtEventVisitor's serialization for a ResetStream event
//    must match what writeWTResetStream produces with the QMUX wire format —
//    i.e. the protocol field is plumbed through to the underlying writer.
TEST(WtEventVisitorTest, ResetStream_QmuxProtocol_MatchesQmuxEncoding) {
  const WtStreamManager::ResetStream event{
      .streamId = 4, .err = 0x42, .reliableSize = 100};

  folly::IOBufQueue visitorBuf{folly::IOBufQueue::cacheChainLength()};
  WtEventVisitor{.egress = visitorBuf, .protocol = FrameProtocol::QMUX}(event);

  folly::IOBufQueue directBuf{folly::IOBufQueue::cacheChainLength()};
  writeWTResetStream(
      directBuf,
      WTResetStreamCapsule{.streamId = event.streamId,
                           .appProtocolErrorCode = uint32_t(event.err),
                           .reliableSize = event.reliableSize},
      FrameProtocol::QMUX);

  EXPECT_EQ(coalesceToString(visitorBuf), coalesceToString(directBuf));
}

// 3) Sanity check: the two protocols actually serialize to different bytes for
//    the same event. If they didn't, tests (1) and (2) would tautologically
//    pass even with the protocol field ignored.
TEST(WtEventVisitorTest, ResetStream_TwoProtocols_ProduceDifferentBytes) {
  const WtStreamManager::ResetStream event{
      .streamId = 4, .err = 0x42, .reliableSize = 100};

  folly::IOBufQueue capsuleBuf{folly::IOBufQueue::cacheChainLength()};
  WtEventVisitor{.egress = capsuleBuf}(event);

  folly::IOBufQueue qmuxBuf{folly::IOBufQueue::cacheChainLength()};
  WtEventVisitor{.egress = qmuxBuf, .protocol = FrameProtocol::QMUX}(event);

  EXPECT_NE(coalesceToString(capsuleBuf), coalesceToString(qmuxBuf));
}

// 4) MaxStreamsBidi takes an extra `isBidi` argument before `protocol` in the
//    underlying writer. Cover it explicitly to confirm the protocol field
//    survived being placed in the right argument slot.
TEST(WtEventVisitorTest, MaxStreamsBidi_QmuxProtocol_MatchesQmuxEncoding) {
  const WtStreamManager::MaxStreamsBidi event{{.maxStreams = 50}};

  folly::IOBufQueue visitorBuf{folly::IOBufQueue::cacheChainLength()};
  WtEventVisitor{.egress = visitorBuf, .protocol = FrameProtocol::QMUX}(event);

  folly::IOBufQueue directBuf{folly::IOBufQueue::cacheChainLength()};
  writeWTMaxStreams(directBuf,
                    WTMaxStreamsCapsule{.maximumStreams = event.maxStreams},
                    /*isBidi=*/true,
                    FrameProtocol::QMUX);

  EXPECT_EQ(coalesceToString(visitorBuf), coalesceToString(directBuf));
}
