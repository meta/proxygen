/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/CancellationToken.h>
#include <folly/coro/AsyncScope.h>
#include <folly/coro/DetachOnCancel.h>
#include <folly/coro/Timeout.h>
#include <folly/futures/Promise.h>
#include <folly/init/Init.h>
#include <folly/logging/xlog.h>
#include <folly/portability/GFlags.h>
#include <proxygen/httpserver/samples/hq/InsecureVerifierDangerousDoNotUseInProduction.h>
#include <proxygen/httpserver/samples/hq/devious/DeviousBaton.h>
#include <proxygen/lib/http/coro/HTTPCoroSession.h>
#include <proxygen/lib/http/coro/client/HTTPCoroConnector.h>

DEFINE_string(host, "::1", "Server address");
DEFINE_int32(port, 4433, "Server port");
DEFINE_bool(h3, false, "Connect with h3 instead of h2");
DEFINE_uint32(count, 1, "Number of parallel batons");
// The query parameters are a map, so only one baton value reaches the server,
// which starts baton i at 255 - i for the rest
DEFINE_uint32(baton, 1, "Starting value of the first baton");
DEFINE_int32(timeout_ms, 10000, "Connect and session timeout");
DEFINE_bool(wait_for_settings,
            true,
            "Send the h3 CONNECT after the server's SETTINGS arrive; h2 always "
            "waits");

using namespace proxygen;
using namespace proxygen::coro;
using devious::DeviousBaton;

namespace {

class SettingsObserver : public LifecycleObserver {
 public:
  void onSettings(const HTTPCoroSession&, const SettingsList&) override {
    if (!received.isFulfilled()) {
      received.setValue();
    }
  }

  folly::Promise<folly::Unit> received;
};

folly::coro::Task<void> awaitWithTimeout(folly::EventBase* evb,
                                         folly::SemiFuture<folly::Unit> fut) {
  co_await folly::coro::timeout(
      folly::coro::detachOnCancel(std::move(fut).via(evb)),
      std::chrono::milliseconds(FLAGS_timeout_ms));
}

struct ClientState {
  explicit ClientState(folly::EventBase* inEvb)
      : evb(inEvb),
        baton(/*inWt=*/nullptr,
              DeviousBaton::Mode::CLIENT,
              [this](WebTransport::StreamReadHandle* readHandle) {
                startReading(readHandle);
              }) {
  }

  void startReading(WebTransport::StreamReadHandle* readHandle) {
    scope.add(co_withExecutor(evb,
                              readStream(readHandle->getID(),
                                         readHandle,
                                         readHandle->getCancelToken())));
  }

  // The transport frees readHandle when a read yields a FIN or an error, and
  // when it cancels streamToken, which can leave a pending read incomplete
  folly::coro::Task<void> readStream(uint64_t id,
                                     WebTransport::StreamReadHandle* readHandle,
                                     folly::CancellationToken streamToken) {
    auto token = folly::cancellation_token_merge(
        co_await folly::coro::co_current_cancellation_token,
        std::move(streamToken));
    DeviousBaton::BatonMessageState msg;
    while (!token.isCancellationRequested()) {
      auto data = co_await co_awaitTry(folly::coro::co_withCancellation(
          token,
          folly::coro::detachOnCancel(readHandle->readStreamData().via(evb))));
      if (data.hasException()) {
        XLOG(DBG2) << "read error id=" << id << " err=" << data.exception();
        co_return;
      }
      const bool wasDone = msg.state == DeviousBaton::BatonMessageState::DONE;
      const bool sessionOpen = wt != nullptr;
      if (sessionOpen) {
        baton.onStreamData(id, msg, std::move(data->data), data->fin);
      } else {
        // data that arrived before the session ended can still hold a final 0
        (void)baton.onBatonMessageData(msg, std::move(data->data), data->fin);
      }
      if (!wasDone && msg.state == DeviousBaton::BatonMessageState::DONE) {
        XLOG(DBG2) << "baton=" << uint32_t(msg.baton) << " id=" << id;
        // a baton finishes here on 0, or on 255 when the 0 reply is sent
        if (msg.baton == 0 || (msg.baton == 255 && sessionOpen)) {
          finishedBatons++;
        }
      }
      if (data->fin) {
        co_return;
      }
    }
  }

  void end() {
    wt = nullptr;
    baton.setWebTransport(nullptr);
    if (!done.isFulfilled()) {
      done.setValue();
    }
  }

  folly::EventBase* evb;
  WebTransport* wt{nullptr};
  DeviousBaton baton;
  folly::coro::CancellableAsyncScope scope;
  uint32_t finishedBatons{0};
  uint32_t datagrams{0};
  folly::Promise<folly::Unit> done;
};

class ClientWtHandler : public WebTransportHandler {
 public:
  explicit ClientWtHandler(std::shared_ptr<ClientState> state)
      : state_(std::move(state)) {
  }
  ~ClientWtHandler() noexcept override {
    state_->end();
  }
  ClientWtHandler(const ClientWtHandler&) = delete;
  ClientWtHandler& operator=(const ClientWtHandler&) = delete;
  ClientWtHandler(ClientWtHandler&&) = delete;
  ClientWtHandler& operator=(ClientWtHandler&&) = delete;

  void onWebTransportSession(
      std::shared_ptr<WebTransport> wt) noexcept override {
    state_->wt = wt.get();
    state_->baton.setWebTransport(wt.get());
  }
  void onNewUniStream(
      WebTransport::StreamReadHandle* readHandle) noexcept override {
    state_->startReading(readHandle);
  }
  void onNewBidiStream(
      WebTransport::BidiStreamHandle bidiHandle) noexcept override {
    state_->startReading(bidiHandle.readHandle);
  }
  void onDatagrams(
      std::vector<std::unique_ptr<folly::IOBuf>> datagrams) noexcept override {
    state_->datagrams += datagrams.size();
  }
  void onSessionEnd(folly::Optional<uint32_t> error) noexcept override {
    XLOG(DBG2) << "session end error=" << (error ? int64_t(*error) : -1);
    state_->end();
  }
  void onSessionDrain() noexcept override {
  }

 private:
  std::shared_ptr<ClientState> state_;
};

folly::coro::Task<CoroSessionHandle> connect(
    folly::EventBase* evb, SettingsObserver& settingsObserver) {
  folly::SocketAddress addr(FLAGS_host, FLAGS_port, /*allowNameLookup=*/true);
  auto timeout = std::chrono::milliseconds(FLAGS_timeout_ms);
  auto verifier =
      std::make_shared<InsecureVerifierDangerousDoNotUseInProduction>();
  HTTPCoroConnector::SessionParams sessParams;
  sessParams.settings = {{SettingsId::WT_ENABLED, 1}};
  // attached before the session starts reading, so the SETTINGS always reach it
  sessParams.lifecycleObserver = &settingsObserver;
  if (FLAGS_h3) {
    sessParams.settings.insert(
        sessParams.settings.end(),
        {
            {SettingsId::_HQ_DATAGRAM_RFC, 1},
            {SettingsId::WT_INITIAL_MAX_DATA, 1 << 24},
            {SettingsId::WT_INITIAL_MAX_STREAMS_UNI, 100},
            {SettingsId::WT_INITIAL_MAX_STREAMS_BIDI, 100},
        });
    HTTPCoroConnector::QuicConnectionParams connParams;
    connParams.fizzContextAndVerifier = {
        .fizzContext = HTTPCoroConnector::makeFizzClientContext(
            HTTPCoroConnector::defaultQuicTLSParams()),
        .fizzCertVerifier = verifier};
    connParams.transportSettings.datagramConfig.enabled = true;
    connParams.transportSettings.advertisedReliableResetStreamSupport = true;
    co_return co_await HTTPCoroConnector::connect(
        evb, addr, timeout, connParams, sessParams);
  }
  HTTPCoroConnector::ConnectionParams connParams;
  connParams.fizzContextAndVerifier = {
      .fizzContext = HTTPCoroConnector::makeFizzClientContext(
          HTTPCoroConnector::defaultTLSParams()),
      .fizzCertVerifier = verifier};
  co_return co_await HTTPCoroConnector::connect(
      evb, addr, timeout, connParams, sessParams);
}

folly::coro::Task<int> run(folly::EventBase* evb,
                           SettingsObserver& settingsObserver) {
  auto session = co_await connect(evb, settingsObserver);
  auto authority = folly::to<std::string>(
      folly::SocketAddress(FLAGS_host, FLAGS_port, true).describe());

  if (!FLAGS_h3 || FLAGS_wait_for_settings) {
    auto settings = co_await co_awaitTry(
        awaitWithTimeout(evb, settingsObserver.received.getSemiFuture()));
    if (settings.hasException()) {
      XLOG(ERR) << "no SETTINGS from the server: " << settings.exception();
      co_return 1;
    }
  }

  auto state = std::make_shared<ClientState>(evb);
  auto req = state->baton.makeRequest(
      /*version=*/0,
      FLAGS_count,
      std::vector<uint8_t>(FLAGS_count, uint8_t(FLAGS_baton)));
  req.setSecure(true);
  req.getHeaders().set(HTTP_HEADER_HOST, authority);

  auto reservation = session->reserveRequest();
  if (reservation.hasException()) {
    XLOG(ERR) << "failed to reserve request: " << reservation.exception();
    co_return 1;
  }
  auto res = co_await co_awaitTry(session->sendWtReq(
      std::move(*reservation), req, std::make_unique<ClientWtHandler>(state)));
  if (res.hasException()) {
    XLOG(ERR) << "WT request failed: " << res.exception();
    co_return 1;
  }
  XLOG(INFO) << "status=" << res->resp->getStatusCode();
  if (!res->wt) {
    co_return 1;
  }

  auto done =
      co_await co_awaitTry(awaitWithTimeout(evb, state->done.getSemiFuture()));
  if (done.hasException()) {
    XLOG(ERR) << "session did not finish: " << done.exception();
    if (state->wt) {
      state->wt->closeSession(uint32_t(devious::BatonSessionError::BORED));
    }
    co_await state->scope.cancelAndJoinAsync();
    session->initiateDrain();
    co_return 1;
  }
  // The end of the session completes every read, and a read whose data arrived
  // before the end can still finish a baton
  co_await state->scope.joinAsync();
  session->initiateDrain();
  XLOG(INFO) << "finished batons=" << state->finishedBatons << "/"
             << FLAGS_count << " datagrams=" << state->datagrams;
  // With several batons, the peer can close the session while its final 0 for
  // another baton is still in flight, and that 0 is lost, so this can fail
  co_return state->finishedBatons == FLAGS_count ? 0 : 1;
}

} // namespace

int main(int argc, char** argv) {
  const folly::Init init(&argc, &argv);
  // declared before evb so that it outlives the session
  SettingsObserver settingsObserver;
  folly::EventBase evb;
  int ret = 1;
  co_withExecutor(&evb, run(&evb, settingsObserver))
      .start([&ret](folly::Try<int>&& result) {
        if (result.hasException()) {
          XLOG(ERR) << "failed: " << result.exception();
          return;
        }
        ret = *result;
      });
  evb.loop();
  return ret;
}
