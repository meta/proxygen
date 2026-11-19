/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <proxygen/httpserver/samples/hq/SampleHandlers.h>

#include <folly/String.h>
#include <proxygen/lib/utils/Logging.h>
#include <string>

namespace {
std::atomic<bool> shouldPassHealthChecks{true};
}

DEFINE_string(static_root,
              "",
              "Path to serve static files from. Disabled if empty.");

namespace quic::samples {

using namespace proxygen;

HTTPTransactionHandler* Dispatcher::getRequestHandler(HTTPMessage* msg) {
  DCHECK(msg);
  auto path = msg->getPathAsStringPiece();
  if (path == "/" || path == "/echo") {
    return new EchoHandler(params_);
  }
  if (path == "/continue") {
    return new ContinueHandler(params_);
  }
  if (path.size() > 1 && path[0] == '/' && std::isdigit(path[1])) {
    return new RandBytesGenHandler(params_);
  }
  if (path == "/status") {
    return new HealthCheckHandler(shouldPassHealthChecks, params_);
  }
  if (path == "/status_ok") {
    shouldPassHealthChecks = true;
    return new HealthCheckHandler(true, params_);
  }
  if (path == "/status_fail") {
    shouldPassHealthChecks = false;
    return new HealthCheckHandler(false, params_);
  }
  if (path == "/wait" || path == "/release") {
    return new WaitReleaseHandler(
        folly::EventBaseManager::get()->getEventBase(), params_);
  }

  if (path == "/post") {
    return new SimplePostHandler(params_);
  }

  if (path.startsWith("/chunked")) {
    return new ChunkedHandler(params_,
                              folly::EventBaseManager::get()->getEventBase());
  }

  if (path.startsWith("/webtransport/devious-baton")) {
    return new DeviousBatonHandler(
        params_, folly::EventBaseManager::get()->getEventBase());
  }

  if (!FLAGS_static_root.empty()) {
    auto staticFileHandler =
        StaticFileHandler::make(params_, FLAGS_static_root);
    return staticFileHandler.get();
  }
  if (path.startsWith("/delay")) {
    return new DelayHandler(params_,
                            folly::EventBaseManager::get()->getEventBase());
  }
  return new DummyHandler(params_);
}

class WaitReleaseHandler;

std::unordered_map<uint, WaitReleaseHandler*>&
WaitReleaseHandler::getWaitingHandlers() {
  static std::unordered_map<uint, WaitReleaseHandler*> waitingHandlers;
  return waitingHandlers;
}

std::mutex& WaitReleaseHandler::getMutex() {
  static std::mutex mutex;
  return mutex;
}

void WaitReleaseHandler::onHeadersComplete(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept {
  VLOG(10) << "WaitReleaseHandler::onHeadersComplete";
  msg->dumpMessage(2);
  path_ = msg->getPath();
  auto idstr = msg->getQueryParam("id");

  if (msg->getMethod() != proxygen::HTTPMethod::GET ||
      idstr == proxygen::empty_string ||
      (path_ != "/wait" && path_ != "/release")) {
    sendErrorResponse("bad request\n");
    return;
  }

  auto id = folly::tryTo<uint>(idstr);
  if (!id.hasValue() || id.value() == 0) {
    sendErrorResponse("invalid id\n");
    return;
  }

  id_ = id.value();

  txn_->setIdleTimeout(std::chrono::milliseconds(120000));
  std::lock_guard<std::mutex> g(getMutex());
  auto& waitingHandlers = getWaitingHandlers();
  if (path_ == "/wait") {
    auto waitHandler = waitingHandlers.find(id_);
    if (waitHandler != waitingHandlers.end()) {
      sendErrorResponse("id already exists\n");
      return;
    }
    waitingHandlers.insert(std::make_pair(id_, this));
    sendOkResponse("waiting\n", false /* eom */);
  } else if (path_ == "/release") {
    auto waitHandler = waitingHandlers.find(id.value());
    if (waitHandler == waitingHandlers.end()) {
      sendErrorResponse("id does not exist\n");
      return;
    }
    waitHandler->second->release();
    waitingHandlers.erase(waitHandler);
    sendOkResponse("released\n", true /* eom */);
  }
}

void WaitReleaseHandler::maybeCleanup() {
  if (path_ == "/wait" && id_ != 0) {
    std::lock_guard<std::mutex> g(getMutex());
    auto& waitingHandlers = getWaitingHandlers();
    auto waitHandler = waitingHandlers.find(id_);
    if (waitHandler != waitingHandlers.end()) {
      waitingHandlers.erase(waitHandler);
    }
  }
}
void DeviousBatonHandler::onHeadersComplete(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept {
  msg->dumpMessage(2);
  auto wtHandler = std::make_unique<devious::DeviousBatonWtHandler>(evb_);
  auto resp = wtHandler->onRequest(*msg);
  resp.setVersionString(getHttpVersion());
  resp.dumpMessage(4);
  if (resp.is2xxResponse()) {
    // Detaches and deletes this handler
    txn_->sendWtHeaders(resp, std::move(wtHandler));
    return;
  }
  txn_->sendHeadersWithEOM(resp);
}

void DeviousBatonHandler::onError(
    const proxygen::HTTPException& error) noexcept {
  VLOG(4) << "DeviousBatonHandler::onError error=" << error.what();
}
} // namespace quic::samples
