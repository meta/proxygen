/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <proxygen/lib/http/coro/HTTPSourceHolder.h>
#include <proxygen/lib/http/coro/util/CoroWtSession.h>
#include <proxygen/lib/http/webtransport/QuicWtSession.h>

namespace proxygen::coro::detail {

/**
 * Glue between an http/3 coro session and H3WtSession.
 *
 * Over http/3 WebTransport streams are real quic streams, which H3WtSession
 * maps directly onto the QuicSocket. Only the session-level capsules (MAX_DATA,
 * MAX_STREAMS, DRAIN_SESSION, CLOSE_SESSION and optionally DATAGRAM) travel on
 * the CONNECT stream; this class owns the read & write loops that exchange them
 * over the CONNECT stream's ingress/egress HTTPSources.
 *
 * The backing http/3 session hands peer-initiated wt streams
 * (H3WtSession::acquireIngressStream) and quic datagrams
 * (H3WtSession::onDatagram) to ::wtSession().
 */
class H3CoroWtSession {
 public:
  using Ptr = std::shared_ptr<H3CoroWtSession>;

  ~H3CoroWtSession() noexcept;

  static Ptr make(folly::EventBase* evb,
                  std::shared_ptr<quic::QuicSocket> quicSocket,
                  std::unique_ptr<proxygen::WebTransportHandler> wtHandler,
                  proxygen::detail::WtStreamManager::WtConfig wtConfig,
                  uint64_t connectStreamId);

  // launches the CONNECT stream read & write loops, which keep `self` alive for
  // the lifetime of the session, and notifies the WebTransportHandler.
  // WebTransport activity restarts ingressSource's readTimeout.
  std::shared_ptr<WebTransport> start(Ptr self,
                                      HTTPSourceHolder ingressSource,
                                      EgressSourcePtr egressSource,
                                      std::chrono::milliseconds readTimeout);

  // for the backing http/3 session to deliver peer streams and quic datagrams
  proxygen::H3WtSession& wtSession() noexcept {
    return h3Wt_;
  }

 private:
  H3CoroWtSession(folly::EventBase* evb,
                  std::shared_ptr<quic::QuicSocket> quicSocket,
                  std::unique_ptr<proxygen::WebTransportHandler> wtHandler,
                  proxygen::detail::WtStreamManager::WtConfig wtConfig,
                  uint64_t connectStreamId);

  struct H3ConnectStreamCb : proxygen::H3ConnectStreamCallback {
    using proxygen::H3ConnectStreamCallback::H3ConnectStreamCallback;
    CancellableBaton waitForEvent;
    // set while the read loop runs
    HTTPSourceHolder* ingress{nullptr};
    std::chrono::milliseconds readTimeout{0};
    void onEvent(
        proxygen::detail::WtStreamManager::Event&& ev) noexcept override {
      proxygen::H3ConnectStreamCallback::onEvent(std::move(ev));
      waitForEvent.signal();
    }
    void onSessionActivity() noexcept override {
      if (ingress && ingress->readable()) {
        ingress->setReadTimeout(readTimeout);
      }
    }
  };

  folly::coro::Task<void> readLoop(Ptr self, HTTPSourceHolder ingressSource);
  folly::coro::Task<void> writeLoop(Ptr self, EgressSourcePtr egressSource);

  void loopFinished() noexcept;

  folly::EventBase* evb_;
  folly::CancellationSource cs_;
  folly::IOBufQueue connectBuf_{folly::IOBufQueue::cacheChainLength()};
  H3ConnectStreamCb connectCb_{connectBuf_};

  // must be declared after connectCb_, which it references; ~H3WtSession may
  // egress a CLOSE_SESSION capsule through it
  proxygen::H3WtSession h3Wt_;
};

} // namespace proxygen::coro::detail
