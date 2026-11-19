/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/ExceptionString.h>
#include <folly/init/Init.h>
#include <folly/logging/xlog.h>
#include <folly/portability/GFlags.h>
#include <folly/synchronization/Baton.h>
#include <proxygen/httpserver/samples/hq/devious/DeviousBatonWtHandler.h>
#include <proxygen/lib/http/coro/HTTPCoroSession.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/HTTPSourceHolder.h>
#include <proxygen/lib/http/coro/server/HTTPServer.h>
#include <thread>

DEFINE_string(host, "::", "Address to listen on");
DEFINE_int32(port, 4433, "Port for h3 over UDP, and h2 unless --h2port is set");
DEFINE_int32(h2port, 0, "Port for h2 over TCP; 0 uses --port");
DEFINE_string(cert, "", "Certificate file");
DEFINE_string(key, "", "Key file");
DEFINE_int32(threads, 1, "Number of IO threads");

using namespace proxygen;
using namespace proxygen::coro;

namespace {

// Yields the 2xx response with the handler that serves the WebTransport
// session
class WtResponseSource : public HTTPSource {
 public:
  WtResponseSource(std::unique_ptr<HTTPMessage> resp,
                   std::unique_ptr<WebTransportHandler> wtHandler)
      : resp_(std::move(resp)), wtHandler_(std::move(wtHandler)) {
    setHeapAllocated();
  }

  folly::coro::Task<HTTPHeaderEvent> readHeaderEvent() override {
    HTTPHeaderEvent event(std::move(resp_), /*inEOM=*/false);
    event.wtHandler = std::move(wtHandler_);
    co_return event;
  }

  folly::coro::Task<HTTPBodyEvent> readBodyEvent(uint32_t /*max*/) override {
    co_yield folly::coro::co_error(
        HTTPError(HTTPErrorCode::INTERNAL_ERROR, "no body after WT upgrade"));
  }

  void stopReading(folly::Optional<const HTTPErrorCode>) noexcept override {
    delete this;
  }

 private:
  std::unique_ptr<HTTPMessage> resp_;
  std::unique_ptr<WebTransportHandler> wtHandler_;
};

class DeviousBatonHandler : public HTTPHandler {
 public:
  folly::coro::Task<HTTPSourceHolder> handleRequest(
      folly::EventBase* evb,
      HTTPSessionContextPtr /*ctx*/,
      HTTPSourceHolder requestSource) override {
    auto headerEvent = co_await co_awaitTry(requestSource.readHeaderEvent());
    if (headerEvent.hasException()) {
      co_yield folly::coro::co_error(getHTTPError(headerEvent));
    }
    auto wtHandler = std::make_unique<devious::DeviousBatonWtHandler>(evb);
    auto resp = std::make_unique<HTTPMessage>(
        wtHandler->onRequest(*headerEvent->headers));
    if (!resp->is2xxResponse()) {
      co_return HTTPFixedSource::makeFixedSource(std::move(resp));
    }
    // The WebTransport session consumes the rest of the request
    requestSource.release();
    co_return new WtResponseSource(std::move(resp), std::move(wtHandler));
  }
};

HTTPServer::Config makeConfig(uint16_t port, const SettingsList& wtSettings) {
  HTTPServer::Config config;
  config.socketConfig.bindAddress =
      folly::SocketAddress(FLAGS_host, port, /*allowNameLookup=*/true);
  config.numIOThreads = FLAGS_threads;
  auto tlsConfig = HTTPServer::getDefaultTLSConfig();
  tlsConfig.setCertificate(FLAGS_cert, FLAGS_key, "");
  config.socketConfig.sslContextConfigs.emplace_back(std::move(tlsConfig));
  auto& settings = config.sessionConfig.settings;
  settings.insert(settings.end(), wtSettings.begin(), wtSettings.end());
  return config;
}

} // namespace

int main(int argc, char** argv) {
  const folly::Init init(&argc, &argv);
  if (FLAGS_cert.empty() || FLAGS_key.empty()) {
    XLOG(ERR) << "--cert and --key are required";
    return 1;
  }

  // An HTTPServer listens on either TCP or QUIC, so h2 and h3 each get one
  auto h2Config = makeConfig(FLAGS_h2port ? FLAGS_h2port : FLAGS_port,
                             {
                                 {SettingsId::ENABLE_CONNECT_PROTOCOL, 1},
                                 {SettingsId::WT_ENABLED, 1},
                             });
  h2Config.shutdownOnSignals.clear();

  auto h3Config = makeConfig(FLAGS_port,
                             {
                                 {SettingsId::ENABLE_CONNECT_PROTOCOL, 1},
                                 {SettingsId::H3_WT_ENABLED, 1},
                                 {SettingsId::H3_WT_MAX_SESSIONS, 1},
                                 {SettingsId::ENABLE_WEBTRANSPORT, 1},
                                 {SettingsId::_HQ_DATAGRAM, 1},
                                 {SettingsId::_HQ_DATAGRAM_RFC, 1},
                             });
  h3Config.quicConfig = HTTPServer::QuicConfig();
  h3Config.quicConfig->transportSettings.datagramConfig.enabled = true;
  h3Config.quicConfig->transportSettings.advertisedReliableResetStreamSupport =
      true;

  auto handler = std::make_shared<DeviousBatonHandler>();
  HTTPServer h2Server(std::move(h2Config), handler);
  HTTPServer h3Server(std::move(h3Config), handler);

  folly::Baton<> h2Started;
  std::thread h2Thread([&] {
    h2Server.start([&] { h2Started.post(); },
                   [](std::exception_ptr ex) {
                     XLOG(FATAL) << "h2 server failed to start: "
                                 << folly::exceptionStr(ex);
                   });
  });
  h2Started.wait();
  // Runs until SIGINT
  h3Server.start();
  h2Server.forceStop();
  h2Thread.join();
  return 0;
}
