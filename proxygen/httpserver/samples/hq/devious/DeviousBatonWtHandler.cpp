/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <proxygen/httpserver/samples/hq/devious/DeviousBatonWtHandler.h>

#include <folly/container/F14Map.h>
#include <folly/futures/Future.h>
#include <proxygen/lib/http/webtransport/HTTPWebTransport.h>

using namespace proxygen;

namespace devious {

struct DeviousBatonWtHandler::State
    : public std::enable_shared_from_this<State> {
  explicit State(folly::EventBase* inEvb)
      : evb(inEvb),
        baton(/*inWt=*/nullptr,
              DeviousBaton::Mode::SERVER,
              [this](WebTransport::StreamReadHandle* readHandle) {
                readNext(readHandle);
              }) {
  }

  void readNext(WebTransport::StreamReadHandle* readHandle) {
    readHandle->awaitNextRead(
        evb,
        [self = shared_from_this()](
            WebTransport::StreamReadHandle* readHandle,
            uint64_t id,
            folly::Try<WebTransport::StreamData> streamData) {
          self->onRead(readHandle, id, std::move(streamData));
        });
  }

  void onRead(WebTransport::StreamReadHandle* readHandle,
              uint64_t id,
              folly::Try<WebTransport::StreamData> streamData) {
    if (!wt) {
      return;
    }
    if (streamData.hasException()) {
      VLOG(4) << "read error id=" << id
              << " err=" << streamData.exception().what();
      streams.erase(id);
      return;
    }
    baton.onStreamData(
        id, streams[id], std::move(streamData->data), streamData->fin);
    if (!readHandle) {
      streams.erase(id);
      return;
    }
    if (wt && !readHandle->getCancelToken().isCancellationRequested()) {
      readNext(readHandle);
    }
  }

  void end() {
    wt = nullptr;
    baton.setWebTransport(nullptr);
  }

  folly::EventBase* evb;
  // Null outside of onWebTransportSession..end
  WebTransport* wt{nullptr};
  DeviousBaton baton;
  folly::F14FastMap<uint64_t, DeviousBaton::BatonMessageState> streams;
};

DeviousBatonWtHandler::DeviousBatonWtHandler(folly::EventBase* evb)
    : state_(std::make_shared<State>(evb)) {
}

DeviousBatonWtHandler::~DeviousBatonWtHandler() noexcept {
  state_->end();
}

HTTPMessage DeviousBatonWtHandler::onRequest(const HTTPMessage& request) {
  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);
  uint16_t status = 200;
  if (!HTTPWebTransport::isConnectMessage(request)) {
    status = 400;
  } else if (auto res = state_->baton.onRequest(request); res.hasError()) {
    status = res.error();
  } else {
    startImmediately_ = request.getQueryParam("immediate") == "1";
    resp.getHeaders().add("sec-webtransport-http3-draft", "draft02");
    if (auto protocols = HTTPWebTransport::getWTAvailableProtocols(request)) {
      if (auto protocol = HTTPWebTransport::negotiateWTProtocol(
              protocols.value(), {"deviousbaton-01"})) {
        HTTPWebTransport::setWTProtocol(resp, std::move(protocol.value()));
      } else {
        status = 400;
      }
    }
  }
  resp.setStatusCode(status);
  resp.setStatusMessage(HTTPMessage::getDefaultReason(status));
  return resp;
}

void DeviousBatonWtHandler::onWebTransportSession(
    std::shared_ptr<WebTransport> wt) noexcept {
  state_->wt = wt.get();
  state_->baton.setWebTransport(wt.get());
  if (startImmediately_) {
    state_->baton.start();
    return;
  }
  // Over h3 the client's initial stream limits are in its SETTINGS, which can
  // arrive after the session starts
  wt->awaitUniStreamCredit()
      .via(state_->evb)
      .thenTry([state = state_](const auto&) {
        if (state->wt) {
          state->baton.start();
        }
      });
}

void DeviousBatonWtHandler::onNewUniStream(
    WebTransport::StreamReadHandle* readHandle) noexcept {
  VLOG(4) << "new uni stream id=" << readHandle->getID();
  state_->readNext(readHandle);
}

void DeviousBatonWtHandler::onNewBidiStream(
    WebTransport::BidiStreamHandle bidiHandle) noexcept {
  VLOG(4) << "new bidi stream id=" << bidiHandle.readHandle->getID();
  state_->readNext(bidiHandle.readHandle);
}

void DeviousBatonWtHandler::onDatagrams(
    std::vector<std::unique_ptr<folly::IOBuf>> datagrams) noexcept {
  VLOG(4) << "received datagrams=" << datagrams.size();
}

void DeviousBatonWtHandler::onSessionEnd(
    folly::Optional<uint32_t> error) noexcept {
  VLOG(4) << "session end error="
          << (error ? folly::to<std::string>(*error) : std::string("none"));
  state_->end();
}

void DeviousBatonWtHandler::onSessionDrain() noexcept {
  VLOG(4) << "session drain";
}

} // namespace devious
