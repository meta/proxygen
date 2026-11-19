/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/async/AsyncServerSocket.h>
#include <glog/logging.h>
#include <list>
#include <memory>
#include <proxygen/lib/utils/AcceptorAddress.h>
#include <wangle/acceptor/Acceptor.h>
#include <wangle/acceptor/ConnectionCounter.h>

namespace proxygen {

class Service;
class RequestWorkerThread;

/**
 * ServiceWorker contains all of the per-thread information for a Service.
 *
 * ServiceWorker contains a pointer back to the single global Service.
 * It contains a list of ProxyAcceptor objects for this worker thread,
 * one per VIP.
 *
 * ServiceWorker fits into the Proxygen object hierarchy as follows:
 *
 * - Service: one instance (globally across the entire program)
 * - ServiceWorker: one instance per thread
 * - ServiceAcceptor: one instance per VIP per thread
 */
class ServiceWorker {
 public:
  using AcceptorMap =
      std::map<AcceptorAddress, std::unique_ptr<wangle::Acceptor>>;

  using NamedAddressMap = std::map<std::string, AcceptorAddress>;

  ServiceWorker(Service* service, RequestWorkerThread* worker)
      : service_(service), worker_(worker) {
  }

  virtual ~ServiceWorker() = default;

  [[nodiscard]] Service* getService() const {
    return service_;
  }

  void addServiceAcceptor(const folly::SocketAddress& address,
                          std::unique_ptr<wangle::Acceptor> acceptor) {
    addServiceAcceptor(
        AcceptorAddress(address, AcceptorAddress::AcceptorType::TCP),
        std::move(acceptor));
  }

  void addServiceAcceptor(const AcceptorAddress& accAddress,
                          std::unique_ptr<wangle::Acceptor> acceptor) {
    namedAddress_.emplace(acceptor->getName(), accAddress);
    addAcceptor(accAddress, std::move(acceptor), acceptors_);
  }

  void drainServiceAcceptor(const folly::SocketAddress& address) {
    drainServiceAcceptor(
        AcceptorAddress(address, AcceptorAddress::AcceptorType::TCP));
  }

  void drainServiceAcceptor(const AcceptorAddress& accAddress) {
    // Move the old acceptor to drainingAcceptors_ if present
    const auto& it = acceptors_.find(accAddress);
    if (it == acceptors_.end()) {
      LOG(INFO) << "drainServiceAcceptor: no active acceptor for "
                << accAddress;
      return;
    }
    auto name = it->second->getName();
    LOG(INFO) << "drainServiceAcceptor: draining acceptor " << name << " for "
              << accAddress
              << " (drainingAcceptors_.size=" << drainingAcceptors_.size()
              << ")";
    // Evict any previous draining acceptor still occupying this address.
    //
    // FIXME: use-after-free if this branch is ever taken. forceStop() only
    // queues the drop of the acceptor's connections on its EventBase, even
    // when called on that thread, and erase() frees the acceptor before the
    // drop runs. Current callers release every draining acceptor before
    // draining its address again, so this is not reached. Free the evicted
    // acceptor on a later loop iteration before relying on it.
    auto drainingIt = drainingAcceptors_.find(accAddress);
    if (drainingIt != drainingAcceptors_.end()) {
      LOG(INFO)
          << "drainServiceAcceptor: evicting previously-draining acceptor "
          << drainingIt->second->getName() << " for " << accAddress
          << " via forceStop";
      drainingIt->second->forceStop();
      drainingAcceptors_.erase(drainingIt);
    }

    addAcceptor(accAddress, std::move(it->second), drainingAcceptors_);
    acceptors_.erase(it);
    namedAddress_.erase(name);
  }

  // Release a draining acceptor, the caller now owns its lifetime. If it may
  // still hold connections, forceStop() it and free it on a later loop
  // iteration of its EventBase: forceStop() only queues the drop, so freeing
  // the acceptor right after it leaves the queued drop running on freed
  // memory.
  [[nodiscard]] wangle::Acceptor* releaseDrainingAcceptor(
      const AcceptorAddress& accAddress) {
    auto it = drainingAcceptors_.find(accAddress);
    if (it != drainingAcceptors_.end()) {
      auto acceptor = it->second.release();
      drainingAcceptors_.erase(it);
      return acceptor;
    }
    return nullptr;
  }

  // Takes the live acceptor at `accAddress` out; the caller now owns it.
  // nullptr if there is none.
  [[nodiscard]] std::unique_ptr<wangle::Acceptor> releaseServiceAcceptor(
      const AcceptorAddress& accAddress) {
    auto it = acceptors_.find(accAddress);
    if (it == acceptors_.end()) {
      return nullptr;
    }
    auto acceptor = std::move(it->second);
    acceptors_.erase(it);
    if (auto named = namedAddress_.find(acceptor->getName());
        named != namedAddress_.end() && named->second == accAddress) {
      namedAddress_.erase(named);
    }
    return acceptor;
  }

  // Undoes drainServiceAcceptor(): the draining acceptor at `accAddress`
  // serves again. False, and nothing changes, if nothing was draining there or
  // something is live there.
  bool restoreDrainingAcceptor(const AcceptorAddress& accAddress) {
    auto it = drainingAcceptors_.find(accAddress);
    if (it == drainingAcceptors_.end() || acceptors_.contains(accAddress)) {
      return false;
    }
    namedAddress_.emplace(it->second->getName(), accAddress);
    addAcceptor(accAddress, std::move(it->second), acceptors_);
    drainingAcceptors_.erase(it);
    return true;
  }

  [[nodiscard]] RequestWorkerThread* getRequestWorkerThread() const {
    return worker_;
  }

  const AcceptorMap& getAcceptors() {
    return acceptors_;
  }

  /**
   * Find an acceptor by name running in the same request worker.
   */
  [[nodiscard]] wangle::Acceptor* getAcceptorByName(std::string name) const {
    auto it = namedAddress_.find(name);
    if (it != namedAddress_.end()) {
      auto it2 = acceptors_.find(it->second);
      if (it2 != acceptors_.end()) {
        return it2->second.get();
      }
    }
    return nullptr;
  }

  const AcceptorMap& getDrainingAcceptors() {
    return drainingAcceptors_;
  }

  // Flush any thread-local stats that the service is tracking
  virtual void flushStats() {
  }

  // Destruct all the acceptors.
  //
  // Deliberately does not forceStop() first: this runs from worker destruction,
  // after the worker's EventBase is destroyed, and forceStop() posts to it.
  // Anything an acceptor still owns must have been reaped before then.
  virtual void clearAcceptors() {
    acceptors_.clear();
    namedAddress_.clear();
    drainingAcceptors_.clear();
  }

  // Frees every draining acceptor at once, without forceStop(). It has no
  // callers; it is for discarding drains that are over: call it on the worker's
  // EventBase once no draining acceptor holds connections or has work queued
  // there. To retire a live one, take it with releaseDrainingAcceptor()
  // instead.
  virtual void clearDrainingAcceptors() {
    drainingAcceptors_.clear();
  }

  virtual void forceStop() {
  }

 private:
  // Forbidden copy constructor and assignment operator
  ServiceWorker(ServiceWorker const&) = delete;
  ServiceWorker& operator=(ServiceWorker const&) = delete;

  void addAcceptor(const AcceptorAddress& accAddress,
                   std::unique_ptr<wangle::Acceptor> acceptor,
                   AcceptorMap& acceptors) {
    CHECK(acceptors.find(accAddress) == acceptors.end());
    acceptors.insert(std::make_pair(accAddress, std::move(acceptor)));
  }

  /**
   * The global Service object.
   */
  Service* service_;

  /**
   * The RequestWorkerThread that is actually responsible for running the
   * EventBase loop in this thread.
   */
  RequestWorkerThread* worker_;

  /**
   * A list of the Acceptor objects specific to this worker thread, one
   * Acceptor per VIP.
   */
  AcceptorMap acceptors_;

  /**
   * A list of the addresses indexed by name, used to look up an
   * acceptor by name
   */
  NamedAddressMap namedAddress_;

  /**
   * A list of Acceptors that are being drained and will be deleted soon.
   */
  AcceptorMap drainingAcceptors_;
};

} // namespace proxygen
