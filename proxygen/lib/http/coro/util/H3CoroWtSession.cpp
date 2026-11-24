/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "proxygen/lib/http/coro/util/H3CoroWtSession.h"

#include <folly/logging/xlog.h>

namespace proxygen::coro::detail {

H3CoroWtSession::H3CoroWtSession(
    folly::EventBase* evb,
    std::shared_ptr<quic::QuicSocket> quicSocket,
    std::unique_ptr<proxygen::WebTransportHandler> wtHandler,
    proxygen::detail::WtStreamManager::WtConfig wtConfig,
    uint64_t connectStreamId)
    : evb_{evb},
      h3Wt_{std::move(quicSocket),
            std::move(wtHandler),
            wtConfig,
            connectStreamId,
            connectCb_} {
}

H3CoroWtSession::~H3CoroWtSession() noexcept = default;

H3CoroWtSession::Ptr H3CoroWtSession::make(
    folly::EventBase* evb,
    std::shared_ptr<quic::QuicSocket> quicSocket,
    std::unique_ptr<proxygen::WebTransportHandler> wtHandler,
    proxygen::detail::WtStreamManager::WtConfig wtConfig,
    uint64_t connectStreamId) {
  return Ptr(new H3CoroWtSession(evb,
                                 std::move(quicSocket),
                                 std::move(wtHandler),
                                 wtConfig,
                                 connectStreamId));
}
using folly::coro::co_awaitTry;
using folly::coro::co_withCancellation;
using folly::coro::co_withExecutor;

std::shared_ptr<WebTransport> H3CoroWtSession::start(
    Ptr self,
    HTTPSourceHolder ingressSource,
    EgressSourcePtr egressSource,
    std::chrono::milliseconds readTimeout) {
  XLOG(DBG4) << __func__;
  connectCb_.readTimeout = readTimeout;
  std::shared_ptr<WebTransport> wt{self, &h3Wt_};
  h3Wt_.onWtSession(wt);
  auto ct = cs_.getToken();
  co_withExecutor(
      evb_, co_withCancellation(ct, readLoop(self, std::move(ingressSource))))
      .start();
  co_withExecutor(evb_,
                  co_withCancellation(
                      ct, writeLoop(std::move(self), std::move(egressSource))))
      .start();
  return wt;
}

folly::coro::Task<void> H3CoroWtSession::readLoop(
    Ptr /*self*/, HTTPSourceHolder ingressSource) {
  // only the read loop parses capsules, so the codec lives on its stack
  proxygen::H3WtCapsuleCallback capsuleCb{h3Wt_};
  proxygen::WebTransportCapsuleCodec codec{&capsuleCb,
                                           proxygen::CodecVersion::H3};

  bool done = !bool(ingressSource);
  connectCb_.ingress = &ingressSource;
  while (!done) {
    auto bodyEv =
        co_await folly::coro::co_awaitTry(ingressSource.readBodyEvent());
    if (bodyEv.hasException()) {
      XLOG(DBG4) << __func__ << "; ex=" << bodyEv.exception();
      break;
    }
    if (auto* body = asBodyEv(*bodyEv)) {
      XLOG(DBG6) << __func__ << "; len=" << body->chainLength()
                 << "; eom=" << bodyEv->eom;
      codec.onIngress(body->move(), bodyEv->eom);
    } else if (bodyEv->eom) {
      codec.onIngress(nullptr, /*eom=*/true);
    }
    done = bodyEv->eom;
  }
  connectCb_.ingress = nullptr;

  XLOG(DBG4) << "H3CoroWtSession::readLoop exiting";
  loopFinished();
}

folly::coro::Task<void> H3CoroWtSession::writeLoop(
    Ptr /*self*/, EgressSourcePtr egressSource) {
  EgressBackPressure backpressure;
  egressSource->setCallback(&backpressure);
  const auto& ct = co_await folly::coro::co_current_cancellation_token;
  bool done = false;
  while (!done) {
    co_await connectCb_.waitForEvent.wait();
    connectCb_.waitForEvent.reset();
    if (egressSource->window().getNonNegativeSize() == 0) {
      XLOG(DBG6) << __func__ << "; egress blocked";
      backpressure.waitForEgress.reset();
      co_await backpressure.waitForEgress.wait();
    }
    XLOG(DBG6) << __func__
               << "; len=" << connectCb_.visitor.egress.chainLength()
               << "; closed=" << connectCb_.visitor.sessionClosed;
    const bool eom = connectCb_.visitor.sessionClosed;
    if (!connectCb_.visitor.egress.empty() || eom) {
      auto fcState = egressSource->body(connectCb_.visitor.egress.move(),
                                        /*padding=*/0,
                                        eom);
      XCHECK_NE(fcState, HTTPStreamSource::FlowControlState::ERROR);
    }
    done = eom || ct.isCancellationRequested();
  }

  XLOG(DBG4) << __func__ << " exiting";
  loopFinished();
}

void H3CoroWtSession::loopFinished() noexcept {
  h3Wt_.closeSession(folly::none);
  cs_.requestCancellation();
}

} // namespace proxygen::coro::detail
