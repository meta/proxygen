/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <deque>
#include <folly/CancellationToken.h>
#include <folly/ExceptionWrapper.h>
#include <folly/Executor.h>
#include <folly/container/F14Set.h>
#include <folly/portability/GMock.h>
#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <quic/priority/HTTPPriorityQueue.h>

namespace proxygen::test {

using GenericApiRet = folly::Expected<folly::Unit, WebTransport::ErrorCode>;

// WebTransport stream handle that is both a read and write handle.  It's
// designed to be held by two FakeSharedWebTransport objects so that writes
// from one are delivered as reads to the other.
class FakeStreamHandle
    : public WebTransport::StreamReadHandle
    , public WebTransport::StreamWriteHandle
    , public std::enable_shared_from_this<FakeStreamHandle> {
 public:
  explicit FakeStreamHandle(uint64_t inId)
      : WebTransport::StreamReadHandle(inId),
        WebTransport::StreamWriteHandle(inId) {
  }

  ~FakeStreamHandle() {
    WebTransport::StreamReadHandle::cs_.requestCancellation();
    WebTransport::StreamWriteHandle::cs_.requestCancellation();
  }

  uint64_t getID() const {
    return WebTransport::StreamReadHandle::getID();
  }

  folly::Optional<uint32_t> getWriteErr() {
    return writeErr_;
  }

  auto* writeException() {
    return WebTransport::StreamWriteHandle::exception();
  }

  auto* readException() {
    return WebTransport::StreamReadHandle::exception();
  }

  folly::SemiFuture<WebTransport::StreamData> readStreamData() override {
    XCHECK(!promise_) << "One read at a time";
    readCount_++;
    if (!buf_.empty() || (fin_ && inflightBuf_.empty())) {
      return folly::makeFuture(WebTransport::StreamData{
          .data = takeReadData(), .fin = fin_ && inflightBuf_.empty()});
    }

    if (writeErr_ && dataRead_ >= reliableResetCheckpoint_) {
      auto exwrapper =
          folly::make_exception_wrapper<WebTransport::Exception>(*writeErr_);
      return folly::makeFuture<WebTransport::StreamData>(exwrapper);
    }

    auto [promise, future] =
        folly::makePromiseContract<WebTransport::StreamData>();
    promise_ = std::move(promise);
    return std::move(future);
  }
  GenericApiRet stopSending(uint32_t code) override {
    stopSendingCount_++;
    if (!stopSendingErr_) {
      stopSendingErr_ = code;
    }
    if (!writeErr_) {
      writeErr_.emplace(code);
    }
    // Per spec the writer responds with RST; shortcut locally so the
    // tracker for our read half terminates without depending on the peer.
    if (!fin_ && onWriteTerminal_) {
      auto cb = std::move(onWriteTerminal_);
      cb();
    }
    // Last: a synchronous peer reaction can destroy this handle.
    auto& ex = WebTransport::StreamWriteHandle::ex_;
    if (!ex) {
      ex = folly::make_exception_wrapper<WebTransport::Exception>(code);
      WebTransport::StreamWriteHandle::cs_.requestCancellation();
    }
    return folly::unit;
  }

  void setImmediateDelivery(bool immediateDelivery) {
    immediateDelivery_ = immediateDelivery;
  }

  void deliverInflightData(size_t bytes = std::numeric_limits<size_t>::max()) {
    CHECK_GT(bytes, 0);
    XLOG(DBG4) << "deliverInflightData bytes=" << bytes
               << " inflightBuf_ size=" << inflightBuf_.chainLength()
               << " fin=" << (fin_ ? "true" : "false");

    // After a reset, only data below the reliable checkpoint is delivered
    if (writeErr_) {
      uint64_t maxDeliver = reliableResetCheckpoint_ > dataDelivered_
                                ? reliableResetCheckpoint_ - dataDelivered_
                                : 0;
      bytes = std::min(bytes, static_cast<size_t>(maxDeliver));
      if (bytes == 0) {
        inflightBuf_.move();
        if (promise_) {
          promise_->setException(WebTransport::Exception(*writeErr_));
          promise_.reset();
        }
        return;
      }
    }

    auto buf = inflightBuf_.splitAtMost(bytes);
    dataDelivered_ += buf->computeChainDataLength();
    buf_.append(std::move(buf));

    if (writeErr_ && dataDelivered_ >= reliableResetCheckpoint_) {
      inflightBuf_.move();
      if (promise_) {
        // the next read gets the reset error
        promise_->setValue(
            WebTransport::StreamData{.data = takeReadData(), .fin = false});
        promise_.reset();
      }
      return;
    }

    if (promise_) {
      promise_->setValue(WebTransport::StreamData{
          .data = takeReadData(), .fin = fin_ && inflightBuf_.empty()});
      promise_.reset();
    }
    fireDeliveryCallbacks();
  }

  void setDeliveryExecutor(folly::Executor* executor) {
    deliveryExecutor_ = executor;
  }

  // A callback can close the session, so acked entries leave the map before
  // any of them runs.
  void fireDeliveryCallbacks() {
    auto self = shared_from_this();
    std::vector<std::pair<uint64_t, WebTransport::ByteEventCallback*>> acked;
    for (auto it = offsetToDeliveryCallback_.begin();
         it != offsetToDeliveryCallback_.end();) {
      if (it->first > dataDelivered_) {
        ++it;
        continue;
      }
      for (auto* callback : it->second) {
        acked.emplace_back(it->first, callback);
      }
      it = offsetToDeliveryCallback_.erase(it);
    }
    std::stable_sort(acked.begin(), acked.end(), [](auto& a, auto& b) {
      return a.first < b.first;
    });
    for (auto& [offset, callback] : acked) {
      callback->onByteEvent(getID(), offset);
    }
  }

  // Invoked when this handle's write half reaches terminal state (FIN sent
  // or RST via resetStream). NOT invoked on stopSending, which is a signal
  // rather than a stream terminator. Fires at most once.
  void setOnWriteTerminal(std::function<void()> cb) {
    onWriteTerminal_ = std::move(cb);
  }

  using WriteStreamDataRet =
      folly::Expected<WebTransport::FCState, WebTransport::ErrorCode>;
  using WebTransport::StreamWriteHandle::writeStreamData;
  WriteStreamDataRet writeStreamData(
      std::unique_ptr<folly::IOBuf> data,
      bool fin,
      WebTransport::ByteEventCallback* deliveryCallback,
      bool checkpoint) override {
    bool wasTerminal = fin_ || writeErr_.has_value();
    size_t length = 0;
    if (data) {
      length = data->computeChainDataLength();
      dataWritten_ += length;
    }
    if (checkpoint && length == 0) {
      return folly::makeUnexpected(WebTransport::ErrorCode::GENERIC_ERROR);
    }
    if (checkpoint) {
      reliableResetCheckpoint_ = dataWritten_;
    }
    if (immediateDelivery_) {
      if (data) {
        dataDelivered_ += length;
        buf_.append(std::move(data));
      }
    } else {
      if (data) {
        inflightBuf_.append(std::move(data));
      }
    }
    fin_ = fin;
    if (promise_) {
      if (immediateDelivery_) {
        promise_->setValue(
            WebTransport::StreamData{.data = takeReadData(), .fin = fin_});
        promise_.reset();
      }
    }

    if (deliveryCallback) {
      if (immediateDelivery_ && !deliveryExecutor_) {
        deliveryCallback->onByteEvent(getID(), dataWritten_);
      } else {
        offsetToDeliveryCallback_[dataWritten_].push_back(deliveryCallback);
      }
      if (immediateDelivery_ && deliveryExecutor_) {
        deliveryExecutor_->add([weak = weak_from_this()] {
          if (auto self = weak.lock()) {
            self->fireDeliveryCallbacks();
          }
        });
      }
    }
    if (fin && !wasTerminal && onWriteTerminal_) {
      auto cb = std::move(onWriteTerminal_);
      cb();
    }
    return writeBlocked_ ? WebTransport::FCState::BLOCKED
                         : WebTransport::FCState::UNBLOCKED;
  }

  // While true, writes are accepted but report BLOCKED and awaitWritable()
  // stays pending.
  void setWriteBlocked(bool blocked) {
    writeBlocked_ = blocked;
    setWritable(!blocked);
  }

  void unblockWrite() {
    setWriteBlocked(false);
  }

  folly::Expected<folly::SemiFuture<uint64_t>, WebTransport::ErrorCode>
  awaitWritable() override {
    if (writable_) {
      return folly::makeFuture<uint64_t>(0);
    }
    auto [promise, future] = folly::makePromiseContract<uint64_t>();
    writablePromises_.push_back(std::move(promise));
    return std::move(future);
  }

  // While false, awaitWritable() stays pending; writes are still accepted.
  void setWritable(bool writable) {
    writable_ = writable;
    if (!writable_) {
      return;
    }
    auto promises = std::move(writablePromises_);
    writablePromises_.clear();
    for (auto& promise : promises) {
      promise.setValue(0);
    }
  }

  size_t pendingWritableWaiters() const {
    return writablePromises_.size();
  }

  // Every registration ends in exactly one onByteEvent or onByteEventCanceled.
  void cancelDeliveryCallbacks() {
    auto callbacks = std::exchange(offsetToDeliveryCallback_, {});
    for (auto& [offset, deliveryCallback] : callbacks) {
      for (auto& callback : deliveryCallback) {
        callback->onByteEventCanceled(getID(), offset);
      }
    }
  }

  GenericApiRet resetStream(uint32_t err) override {
    resetCount_++;
    cancelDeliveryCallbacks();

    writeErr_ = err;

    // Drop buffered data beyond the reliable checkpoint
    if (reliableResetCheckpoint_ > dataRead_) {
      uint64_t maxBuffered = reliableResetCheckpoint_ - dataRead_;
      if (buf_.chainLength() > maxBuffered) {
        auto keep = buf_.split(maxBuffered);
        buf_.move();
        buf_.append(std::move(keep));
      }
    } else {
      buf_.move();
    }

    // A pending read stays pending while reliable data is still inflight
    if (promise_ && dataDelivered_ >= reliableResetCheckpoint_) {
      promise_->setException(WebTransport::Exception(err));
      promise_.reset();
    }
    // Guard via onWriteTerminal_ presence (cleared on first fire) — writeErr_
    // may already be set by an earlier stopSending and must not gate this.
    if (!fin_ && onWriteTerminal_) {
      auto cb = std::move(onWriteTerminal_);
      cb();
    }
    // Like WtStreamManager: a local reset sets the write exception and fires
    // the write cancel token synchronously, before resetStream returns.
    auto& ex = WebTransport::StreamWriteHandle::ex_;
    if (!ex) {
      ex = folly::make_exception_wrapper<WebTransport::Exception>(err);
    }
    WebTransport::StreamWriteHandle::cs_.requestCancellation();
    return folly::unit;
  }

  // Simulate a peer RESET_STREAM on this read half. The writer's state stays as
  // if it had not reset.
  void deliverPeerReset(uint32_t err) {
    if (promise_) {
      promise_->setException(WebTransport::Exception(err));
      promise_.reset();
    }
    writeErr_ = err;
    if (!fin_ && onWriteTerminal_) {
      auto cb = std::move(onWriteTerminal_);
      cb();
    }
  }

  GenericApiRet setPriority(quic::PriorityQueue::Priority priority) override {
    quic::HTTPPriorityQueue::Priority httpPri(priority);
    pri.emplace(std::forward_as_tuple(
        httpPri->urgency, httpPri->order, httpPri->incremental));
    return folly::unit;
  }

  bool open() const {
    return !fin_ && !writeErr_ && (!promise_ || !promise_->isFulfilled());
  }

  std::unique_ptr<folly::IOBuf> takeReadData() {
    auto data = buf_.move();
    dataRead_ += data ? data->computeChainDataLength() : 0;
    return data;
  }

  uint64_t id{0};
  std::function<void()> onWriteTerminal_;
  folly::Optional<folly::Promise<WebTransport::StreamData>> promise_;
  folly::IOBufQueue buf_{folly::IOBufQueue::cacheChainLength()};
  uint32_t dataWritten_{0};
  uint32_t dataDelivered_{0};
  // This handle survives its FIN, unlike a real one, so tests assert on these
  // instead of on a use-after-free.
  uint32_t readCount_{0};
  uint32_t stopSendingCount_{0};
  uint32_t resetCount_{0};
  folly::Optional<uint32_t> stopSendingErr_;
  uint32_t dataRead_{0};
  uint32_t reliableResetCheckpoint_{0};
  bool fin_{false};
  folly::Optional<std::tuple<uint8_t, uint64_t, bool>> pri;
  folly::Optional<uint32_t> writeErr_;

  // If immediateDelivery_ == false, we stash data in inflightBuf_ until
  // deliverInflightData() is called.
  bool immediateDelivery_{true};
  bool writable_{true};
  bool writeBlocked_{false};
  std::vector<folly::Promise<uint64_t>> writablePromises_;
  folly::IOBufQueue inflightBuf_{folly::IOBufQueue::cacheChainLength()};
  folly::F14FastMap<uint64_t, std::vector<WebTransport::ByteEventCallback*>>
      offsetToDeliveryCallback_;
  folly::Executor* deliveryExecutor_{nullptr};
};

// Implementation of WebTransport for testing two connected endpoints.
//
// Usage:
//
// auto [client, server] = FakeSharedWebTransport::makeSharedWebTransport();
//
// Each FakeSharedWebTransport also requires a WebTransportHandler for the peer
// to deliver new streams, datagrams, and end-of-session events.

class FakeSharedWebTransport : public WebTransport {
 public:
  static std::pair<std::unique_ptr<FakeSharedWebTransport>,
                   std::unique_ptr<FakeSharedWebTransport>>
  makeSharedWebTransport() {
    auto a = std::make_unique<FakeSharedWebTransport>();
    auto b = std::make_unique<FakeSharedWebTransport>();
    // Use QUIC-style stream ID spaces: client uses even types (bidi=0,
    // uni=2), server uses odd types (bidi=1, uni=3).
    b->nextBidiStreamId_ = 1;
    b->nextUniStreamId_ = 3;
    a->setPeer(b.get());
    b->setPeer(a.get());
    return {std::move(a), std::move(b)};
  }
  FakeSharedWebTransport() = default;
  ~FakeSharedWebTransport() override {
    for (auto& h : writeHandles) {
      h.second->cancelDeliveryCallbacks();
    }
    writeHandles.clear();
    readHandles.clear();
  }
  quic::TransportInfo getTransportInfo() const override {
    return quic::TransportInfo{};
  }

  const folly::SocketAddress& getPeerAddress() const override {
    return peerAddress_;
  }

  const folly::SocketAddress& getLocalAddress() const override {
    return localAddress_;
  }

  void setPeer(FakeSharedWebTransport* peer) {
    peer_ = peer;
  }

  void setPeerHandler(WebTransportHandler* peerHandler) {
    peerHandler_ = peerHandler;
  }

  folly::Expected<StreamWriteHandle*, ErrorCode> createUniStream() override {
    if (maxLocalUniStreams_ &&
        localUniStreams_.size() >= *maxLocalUniStreams_) {
      return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
    }
    auto id = nextUniStreamId_;
    nextUniStreamId_ += 4;
    auto handle = std::make_shared<FakeStreamHandle>(id);
    handle->setImmediateDelivery(!deferNewStreamData_);
    handle->setDeliveryExecutor(deliveryExecutor_);
    handle->setWritable(newStreamsWritable_);
    writeHandles.emplace(id, handle);
    peer_->readHandles.emplace(id, handle);
    localUniStreams_.insert(id);
    handle->setOnWriteTerminal([this, id]() { onLocalUniWriteTerminal(id); });
    peerHandler_->onNewUniStream(handle.get());
    return handle.get();
  }
  folly::Expected<BidiStreamHandle, ErrorCode> createBidiStream() override {
    if (nextCreateBidiStreamError_) {
      auto error = *nextCreateBidiStreamError_;
      nextCreateBidiStreamError_.reset();
      return folly::makeUnexpected(error);
    }
    if (maxLocalBidiStreams_ &&
        localBidiStreams_.size() >= *maxLocalBidiStreams_) {
      return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
    }
    auto id = nextBidiStreamId_;
    nextBidiStreamId_ += 4;
    auto readH = std::make_shared<FakeStreamHandle>(id);
    auto writeH = std::make_shared<FakeStreamHandle>(id);
    writeH->setImmediateDelivery(!deferNewStreamData_);
    writeH->setDeliveryExecutor(deliveryExecutor_);
    readH->setDeliveryExecutor(peer_->deliveryExecutor_);
    readHandles.emplace(id, readH);
    writeHandles.emplace(id, writeH);
    peer_->readHandles.emplace(id, writeH);
    peer_->writeHandles.emplace(id, readH);
    localBidiStreams_.emplace(id, BidiHalves{});
    // writeH is the local write half; readH is also peer's write handle,
    // so its onWriteTerminal fires when peer FINs or RSTs us.
    writeH->setOnWriteTerminal([this, id]() { onLocalBidiWriteTerminal(id); });
    readH->setOnWriteTerminal([this, id]() { onLocalBidiReadTerminal(id); });
    peerHandler_->onNewBidiStream({writeH.get(), readH.get()});
    return BidiStreamHandle({readH.get(), writeH.get()});
  }
  using AwaitStreamCreditRet = folly::SemiFuture<folly::Unit>;
  AwaitStreamCreditRet awaitUniStreamCredit() override {
    uniCreditWaitCalls_++;
    if (!maxLocalUniStreams_ ||
        localUniStreams_.size() < *maxLocalUniStreams_) {
      return folly::makeFuture(folly::unit);
    }
    auto [p, f] = folly::makePromiseContract<folly::Unit>();
    pendingUniCreditPromises_.push_back(std::move(p));
    return std::move(f);
  }
  AwaitStreamCreditRet awaitBidiStreamCredit() override {
    if (!maxLocalBidiStreams_ ||
        localBidiStreams_.size() < *maxLocalBidiStreams_) {
      return folly::makeFuture(folly::unit);
    }
    auto [p, f] = folly::makePromiseContract<folly::Unit>();
    pendingBidiCreditPromises_.push_back(std::move(p));
    return std::move(f);
  }

  // Simulate QUIC peer-issued MAX_STREAMS bidi limit on this endpoint. Once
  // set, createBidiStream() fails synchronously when the limit is reached;
  // awaitBidiStreamCredit() returns a pending future. Credit is replenished
  // when both halves of a locally-initiated bidi reach terminal state, or
  // when releaseBidiCredit() is called explicitly.
  void setMaxLocalBidiStreams(uint64_t n) {
    maxLocalBidiStreams_ = n;
    notifyBidiCreditAvailable();
  }
  void setNextCreateBidiStreamError(ErrorCode error) {
    nextCreateBidiStreamError_ = error;
  }
  // Data written on streams this endpoint creates afterwards is held until
  // the test calls deliverInflightData() on their write handle, so tests can
  // choose the order in which the peer sees each stream.
  void setDeferNewStreamData(bool defer) {
    deferNewStreamData_ = defer;
  }
  // Write handles created afterwards ack immediately delivered writes from the
  // executor, as a real transport acks after the write returns.
  void setDeliveryExecutor(folly::Executor* executor) {
    deliveryExecutor_ = executor;
  }
  // Uni streams created afterwards start with awaitWritable() pending until
  // the test calls setWritable(true) on their write handle.
  void setNewStreamsWritable(bool writable) {
    newStreamsWritable_ = writable;
  }
  void releaseBidiCredit(uint64_t id) {
    if (localBidiStreams_.erase(id)) {
      notifyBidiCreditAvailable();
    }
  }
  // Simulate QUIC peer-issued MAX_STREAMS uni limit. Same semantics as
  // setMaxLocalBidiStreams; credit is replenished when a locally-initiated
  // uni stream's write half reaches terminal state, or via releaseUniCredit.
  void setMaxLocalUniStreams(uint64_t n) {
    maxLocalUniStreams_ = n;
    notifyUniCreditAvailable();
  }
  void releaseUniCredit(uint64_t id) {
    if (localUniStreams_.erase(id)) {
      notifyUniCreditAvailable();
    }
  }
  size_t pendingUniCreditWaiters() const {
    return pendingUniCreditPromises_.size();
  }
  // Some transports cannot wait for credit (HTTP/3 WebTransport aborts).
  size_t uniCreditWaitCalls() const {
    return uniCreditWaitCalls_;
  }
  // FIN/RST observed on the write half of an outstanding local bidi stream.
  void onLocalBidiWriteTerminal(uint64_t id) {
    auto it = localBidiStreams_.find(id);
    if (it == localBidiStreams_.end()) {
      return;
    }
    it->second.writeDone = true;
    if (it->second.readDone) {
      localBidiStreams_.erase(it);
      notifyBidiCreditAvailable();
    }
  }
  // FIN/RST observed on the read half of an outstanding local bidi stream.
  void onLocalBidiReadTerminal(uint64_t id) {
    auto it = localBidiStreams_.find(id);
    if (it == localBidiStreams_.end()) {
      return;
    }
    it->second.readDone = true;
    if (it->second.writeDone) {
      localBidiStreams_.erase(it);
      notifyBidiCreditAvailable();
    }
  }
  // FIN/RST observed on a locally-initiated uni stream's (sole) write half.
  void onLocalUniWriteTerminal(uint64_t id) {
    if (localUniStreams_.erase(id)) {
      notifyUniCreditAvailable();
    }
  }

 public:
  struct BidiHalves {
    bool writeDone{false};
    bool readDone{false};
  };

 private:
  void notifyBidiCreditAvailable() {
    while (!pendingBidiCreditPromises_.empty() &&
           (!maxLocalBidiStreams_ ||
            localBidiStreams_.size() < *maxLocalBidiStreams_)) {
      auto p = std::move(pendingBidiCreditPromises_.front());
      pendingBidiCreditPromises_.pop_front();
      p.setValue(folly::unit);
    }
  }
  void notifyUniCreditAvailable() {
    while (!pendingUniCreditPromises_.empty() &&
           (!maxLocalUniStreams_ ||
            localUniStreams_.size() < *maxLocalUniStreams_)) {
      auto p = std::move(pendingUniCreditPromises_.front());
      pendingUniCreditPromises_.pop_front();
      p.setValue(folly::unit);
    }
  }

 public:
  using ReadStreamDataRet =
      folly::Expected<folly::SemiFuture<StreamData>, WebTransport::ErrorCode>;
  ReadStreamDataRet readStreamData(uint64_t id) override {
    auto h = readHandles.find(id);
    if (h == readHandles.end()) {
      return folly::makeUnexpected(WebTransport::ErrorCode::GENERIC_ERROR);
    }
    return h->second->readStreamData();
  }

  using WebTransport::writeStreamData;
  folly::Expected<FCState, ErrorCode> writeStreamData(
      uint64_t id,
      std::unique_ptr<folly::IOBuf> data,
      bool fin,
      WebTransport::ByteEventCallback* deliveryCallback,
      bool checkpoint) override {
    auto h = writeHandles.find(id);
    if (h == writeHandles.end()) {
      return folly::makeUnexpected(WebTransport::ErrorCode::GENERIC_ERROR);
    }
    return h->second->writeStreamData(
        std::move(data), fin, deliveryCallback, checkpoint);
  }

  folly::Expected<folly::SemiFuture<uint64_t>, ErrorCode> awaitWritable(
      uint64_t id) override {
    auto h = writeHandles.find(id);
    if (h == writeHandles.end()) {
      return folly::makeUnexpected(WebTransport::ErrorCode::GENERIC_ERROR);
    }
    return h->second->awaitWritable();
  }

  folly::Expected<folly::Unit, ErrorCode> resetStream(uint64_t streamId,
                                                      uint32_t error) override {
    auto h = writeHandles.find(streamId);
    if (h == writeHandles.end()) {
      return folly::makeUnexpected(WebTransport::ErrorCode::GENERIC_ERROR);
    }
    return h->second->resetStream(error);
  }

  folly::Expected<folly::Unit, ErrorCode> setPriority(
      uint64_t streamId, quic::PriorityQueue::Priority priority) override {
    auto h = writeHandles.find(streamId);
    if (h == writeHandles.end()) {
      return folly::makeUnexpected(WebTransport::ErrorCode::GENERIC_ERROR);
    }
    return h->second->setPriority(priority);
  }

  folly::Expected<folly::Unit, ErrorCode> setPriorityQueue(
      std::unique_ptr<quic::PriorityQueue> /*queue*/) noexcept override {
    return folly::unit;
  }

  folly::Expected<folly::Unit, ErrorCode> stopSending(uint64_t streamId,
                                                      uint32_t error) override {
    auto h = readHandles.find(streamId);
    if (h == readHandles.end()) {
      return folly::makeUnexpected(WebTransport::ErrorCode::GENERIC_ERROR);
    }
    // STOP_SENDING is a signal, not a stream terminator — no credit change.
    return h->second->stopSending(error);
  }

  folly::Expected<folly::Unit, ErrorCode> sendDatagram(
      std::unique_ptr<folly::IOBuf> datagram) override {
    peerHandler_->onDatagram(std::move(datagram));
    return folly::unit;
  }

  // Close the WebTransport session, with an optional error
  //
  // Any pending futures will complete with a folly::OperationCancelled
  // exception
  folly::Expected<folly::Unit, ErrorCode> closeSession(
      folly::Optional<uint32_t> error = folly::none) override {
    // TODO: This mirrors mvfst and WebTransportImpl behavior but seems wrong.
    // A local code for stopSending/reset would be better that the input code.
    auto closeCode = error.value_or(std::numeric_limits<uint32_t>::max());
    for (auto& h : writeHandles) {
      if (h.second->open()) {
        h.second->resetStream(closeCode);
      } else {
        h.second->cancelDeliveryCallbacks();
      }
    }
    writeHandles.clear();
    for (auto& h : readHandles) {
      h.second->stopSending(closeCode);
    }
    readHandles.clear();
    // localBidiStreams_ is intentionally not cleared so openLocalBidiStreams()
    // can still report any leak after close.
    while (!pendingBidiCreditPromises_.empty()) {
      auto p = std::move(pendingBidiCreditPromises_.front());
      pendingBidiCreditPromises_.pop_front();
      p.setException(folly::OperationCancelled());
    }
    while (!pendingUniCreditPromises_.empty()) {
      auto p = std::move(pendingUniCreditPromises_.front());
      pendingUniCreditPromises_.pop_front();
      p.setException(folly::OperationCancelled());
    }
    if (peerHandler_) {
      peerHandler_->onSessionEnd(error);
    }
    sessionClosed_ = true;
    return folly::unit;
  }

  bool isSessionClosed() const {
    return sessionClosed_;
  }

  // Locally-initiated bidi streams whose two halves have not both reached
  // terminal state (FIN or RST). Entries are erased automatically when both
  // halves go terminal, so anything left here at a quiescent test point is
  // a stream we failed to close cleanly on one or both sides. closeSession()
  // leaves this map intact so leaks remain observable post-close.
  const folly::F14FastMap<uint64_t, BidiHalves>& openLocalBidiStreams() const {
    return localBidiStreams_;
  }

  // Locally-initiated uni streams whose (sole) write half has not yet reached
  // terminal state (FIN or RST). Same closeSession() semantics as above.
  const folly::F14FastSet<uint64_t>& openLocalUniStreams() const {
    return localUniStreams_;
  }

  std::map<uint64_t, std::shared_ptr<FakeStreamHandle>> writeHandles;
  std::map<uint64_t, std::shared_ptr<FakeStreamHandle>> readHandles;

 private:
  bool sessionClosed_{false};
  uint64_t nextBidiStreamId_{0};
  uint64_t nextUniStreamId_{2};
  FakeSharedWebTransport* peer_{nullptr};
  WebTransportHandler* peerHandler_{nullptr};
  folly::SocketAddress peerAddress_{"0.0.0.0", 123};
  folly::SocketAddress localAddress_{"0.0.0.0", 456};
  folly::Optional<uint64_t> maxLocalBidiStreams_;
  folly::Optional<uint64_t> maxLocalUniStreams_;
  folly::F14FastMap<uint64_t, BidiHalves> localBidiStreams_;
  folly::F14FastSet<uint64_t> localUniStreams_;
  std::deque<folly::Promise<folly::Unit>> pendingBidiCreditPromises_;
  std::deque<folly::Promise<folly::Unit>> pendingUniCreditPromises_;
  size_t uniCreditWaitCalls_{0};
  folly::Optional<ErrorCode> nextCreateBidiStreamError_;
  bool deferNewStreamData_{false};
  folly::Executor* deliveryExecutor_{nullptr};
  bool newStreamsWritable_{true};
};

} // namespace proxygen::test
