/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <proxygen/lib/http/webtransport/QuicWtSession.h>

#include <quic/common/address/QuicSocketAddressBridge.h>

using namespace proxygen;
using namespace proxygen::detail;
using FCState = WebTransport::FCState;

namespace {
static constexpr uint64_t kDefaultWtWrite = 65'535;

using WtConfig = WtStreamManager::WtConfig;
WtConfig& overwritePeerMaxDataWtConfig(WtConfig& config) {
  config.peerMaxStreamDataBidiLocal = kDefaultWtWrite;
  config.peerMaxStreamDataBidiRemote = kDefaultWtWrite;
  config.peerMaxStreamDataUni = kDefaultWtWrite;
  return config;
}

WtConfig createQuicConfig(const quic::TransportSettings& settings) {
  WtConfig config;
  config.selfMaxStreamsBidi = kMaxVarint;
  config.selfMaxStreamsUni = kMaxVarint;
  config.selfMaxConnData = kMaxVarint;
  config.selfMaxStreamDataBidiLocal =
      settings.advertisedInitialBidiLocalStreamFlowControlWindow;
  config.selfMaxStreamDataBidiRemote =
      settings.advertisedInitialBidiRemoteStreamFlowControlWindow;
  config.selfMaxStreamDataUni =
      settings.advertisedInitialUniStreamFlowControlWindow;
  config.peerMaxStreamsBidi = kMaxVarint;
  config.peerMaxStreamsUni = kMaxVarint;
  config.peerMaxConnData = kMaxVarint;
  overwritePeerMaxDataWtConfig(config);
  return config;
}

bool isWtStreamEgressFcBlocked(
    const WtStreamManager& sm,
    const WtStreamManager::WtWriteHandle& wh) noexcept {
  const auto [currentOffset, maxOffset] = sm.getFlowControlInfo(wh);
  return currentOffset == maxOffset;
}

/**
 * Queries the quic socket for the peer's current max advertised offset and sets
 * it accordingly on the WtStreamManager's WriteHandle. As of now this function
 * is invoked mainly from two places:
 *
 * - When we first create an egress stream (either peer bidi, local bidi, or
 *   local uni). Since we start with a default minimum value of kDefaultWtWrite.
 *
 * - During the write loop, when we eventually saturate the window previously
 *   queried, we reinstall the notifyPendingWriteOnStream and re-query the
 *   window.
 */
void grantMaxData(const QuicSocket& sock,
                  WtStreamManager& sm,
                  const WtStreamManager::WtWriteHandle& wh) noexcept {
  using MaxStreamData = WtStreamManager::MaxStreamData;
  const auto id = wh.getID();
  // query quic flow control
  const auto fc = sock.getStreamFlowControl(id);
  if (fc.hasError()) { // let normal flows handle errors (e.g. ::readError)
    return;
  }
  // no-op if the peer rwnd max offset hasn't increased
  sm.onMaxData(MaxStreamData{{.maxData = fc->sendWindowMaxOffset}, id});
  return;
}

struct QuicWtEventVisitor {
  quic::QuicSocket& quicSocket;
  H3ConnectStreamCallback* observer{nullptr};
  const QuicWtSessionBase& sess;

  // operations map to QuicSocket invocations
  void operator()(WtStreamManager::ResetStream ev) const {
    quicSocket.resetStream(ev.streamId, sess.toTransportErrorCode(ev.err));
    quicSocket.setStopSendingCallback(ev.streamId, nullptr);
  }

  void operator()(WtStreamManager::StopSending ev) const {
    // not setReadCallback(id, nullptr, err): that also unregisters the read
    // callback, so the peer's RESET_STREAM would never close the ingress side
    quicSocket.stopSending(ev.streamId, sess.toTransportErrorCode(ev.err));
  }

  void operator()(WtStreamManager::EgressPriority pri) const {
    // Apply the stream's priority locally on the quic socket; internally all
    // flows (streams and dgrams) are scheduled against eachother.
    auto res = quicSocket.setStreamPriority(pri.streamId, pri.priority);
    XLOG_IF(DBG4, res.hasError()) << __func__ << "; id=" << pri.streamId
                                  << "; err=" << toString(res.error());
  }

  // operations need to be serialized on the backing http/3 connect stream (if
  // exists).
  void operator()(WtStreamManager::CloseSession ev) const {
    if (observer) {
      observer->onEvent(std::move(ev));
    }
  }

  void operator()(WtStreamManager::MaxConnData ev) const {
    if (observer) {
      observer->onEvent(std::move(ev));
    }
  }
  void operator()(WtStreamManager::MaxStreamsBidi ev) const {
    if (observer) {
      observer->onEvent(std::move(ev));
    }
  }
  void operator()(WtStreamManager::MaxStreamsUni ev) const {
    if (observer) {
      observer->onEvent(std::move(ev));
    }
  }
  void operator()(WtStreamManager::DrainSession ev) const {
    if (observer) {
      observer->onEvent(std::move(ev));
    }
  }

  // not applicable to http/3-wt or quic-wt
  void operator()(WtStreamManager::MaxStreamData /*ev*/) const {
  }
};

uint64_t getQuicAppErrCode(const QuicError& err) noexcept {
  auto* appEc = err.code.asApplicationErrorCode();
  return appEc ? *appEc : 0;
}

} // namespace

namespace proxygen {

const folly::SocketAddress& QuicWtSessionBase::getLocalAddress() const {
  return quic::toFollySocketAddressRef(quicSocket_->getLocalAddress(),
                                       cachedLocalAddr_);
}

const folly::SocketAddress& QuicWtSessionBase::getPeerAddress() const {
  return quic::toFollySocketAddressRef(quicSocket_->getPeerAddress(),
                                       cachedPeerAddr_);
}

QuicWtSessionBase::QuicWtSessionBase(
    std::shared_ptr<quic::QuicSocket> quicSocket,
    WtHandlerPtr wtHandler,
    WtStreamManager::WtConfig wtConfig,
    bool usesEncodedApplicationErrorCodes,
    H3ConnectStreamCallback* observer)
    : WtSessionBase(nullptr, sm_),
      quicSocket_(std::move(quicSocket)),
      wtHandler_(std::move(wtHandler)),
      usesEncodedApplicationErrorCodes_(usesEncodedApplicationErrorCodes),
      priorityQueue_(std::make_unique<quic::HTTPPriorityQueue>()),
      sm_{quicSocket_->getNodeType() == quic::QuicNodeType::Server
              ? WtDir::Server
              : WtDir::Client,
          overwritePeerMaxDataWtConfig(wtConfig),
          smCb_,
          smCb_,
          *priorityQueue_},
      observer_(observer) {
}

uint64_t QuicWtSessionBase::toTransportErrorCode(uint64_t err) const noexcept {
  return usesEncodedApplicationErrorCodes_
             ? WebTransport::toHTTPErrorCode(static_cast<uint32_t>(err))
             : err;
}

uint64_t QuicWtSessionBase::fromTransportErrorCode(
    uint64_t err) const noexcept {
  if (!usesEncodedApplicationErrorCodes_) {
    return err;
  }
  return WebTransport::toApplicationErrorCode(err).value_or(err);
}

QuicWtSessionBase::~QuicWtSessionBase() {
  QuicWtSessionBase::closeSession(folly::none);
}

auto QuicWtSessionBase::createWtEgressHandle(StreamId id) noexcept
    -> BidiStreamHandle {
  // bidi or uni type deduced by WtStreamManager from id
  auto res = sm_.getOrCreateBidiHandle(id);
  const bool success = res.writeHandle;
  const bool bidi = success && res.readHandle;
  // canCreate(Uni|Bidi) checked in ::create(Uni|Bidi)Stream
  XCHECK(success);
  grantMaxData(*quicSocket_, sm_, *res.writeHandle);
  quicSocket_->setStopSendingCallback(id, &stopSendingCb_);
  if (bidi) {
    sm_.setReadCb(*res.readHandle, &smCb_);
    installReadCallback(id);
  }
  return res;
}

void QuicWtSessionBase::installReadCallback(StreamId id) noexcept {
  quicSocket_->setReadCallback(id, &readCb_);
  if (manualRead_) {
    auto res = quicSocket_->setStreamManualRead(id);
    XLOG_IF(ERR, res.hasError()) << __func__ << "; err id=" << id;
  }
}

void QuicWtSessionBase::noteStreamActivity() noexcept {
  if (observer_) {
    observer_->onStreamActivity();
  }
}

bool QuicWtSessionBase::hasEgressUniCredit() const noexcept {
  return quicSocket_->getNumOpenableUnidirectionalStreams() &&
         sm_.canCreateUni();
}
bool QuicWtSessionBase::hasEgressBidiCredit() const noexcept {
  return quicSocket_->getNumOpenableBidirectionalStreams() &&
         sm_.canCreateBidi();
}

folly::Expected<WebTransport::StreamWriteHandle*, WebTransport::ErrorCode>
QuicWtSessionBase::createUniStream() noexcept {
  XCHECK(quicSocket_);
  if (!hasEgressUniCredit()) {
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }
  auto id = quicSocket_->createUnidirectionalStream();
  XCHECK(id);
  XLOG(DBG6) << __func__ << "; id=" << *id;
  return CHECK_NOTNULL(createWtEgressHandle(*id).writeHandle);
}

folly::Expected<WebTransport::BidiStreamHandle, WebTransport::ErrorCode>
QuicWtSessionBase::createBidiStream() noexcept {
  XCHECK(quicSocket_);
  if (!hasEgressBidiCredit()) {
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }
  auto id = quicSocket_->createBidirectionalStream();
  XCHECK(id);
  auto res = createWtEgressHandle(*id);
  XCHECK(res.readHandle && res.writeHandle);
  XLOG(DBG6) << __func__ << "; id=" << *id;
  return res;
}

folly::SemiFuture<folly::Unit>
QuicWtSessionBase::awaitUniStreamCredit() noexcept {
  XCHECK(quicSocket_);
  if (hasEgressUniCredit()) {
    return folly::makeFuture(folly::unit);
  }
  auto [promise, future] = folly::makePromiseContract<folly::Unit>();
  uniCreditPromise() = std::move(promise);
  return std::move(future);
}

folly::SemiFuture<folly::Unit>
QuicWtSessionBase::awaitBidiStreamCredit() noexcept {
  XCHECK(quicSocket_);
  if (hasEgressBidiCredit()) {
    return folly::makeFuture(folly::unit);
  }
  auto [promise, future] = folly::makePromiseContract<folly::Unit>();
  bidiCreditPromise() = std::move(promise);
  return std::move(future);
}

folly::Expected<folly::Unit, WebTransport::ErrorCode>
QuicWtSessionBase::sendDatagram(IoBufPtr datagram) noexcept {
  XCHECK(quicSocket_);
  auto framed = frameDatagram(std::move(datagram));
  if (!framed) {
    return folly::makeUnexpected(ErrorCode::GENERIC_ERROR);
  }
  auto writeRes = quicSocket_->writeDatagram(std::move(framed));
  if (writeRes.hasError()) {
    XLOG(ERR) << __func__ << "; err= " << writeRes.error();
    return folly::makeUnexpected(ErrorCode::GENERIC_ERROR);
  }
  return folly::unit;
}

folly::Expected<folly::Unit, WebTransport::ErrorCode>
QuicWtSessionBase::closeSession(folly::Optional<uint32_t> error) noexcept {
  for (auto streamId : sm_.streamIds()) {
    if (sm_.getBidiHandle(streamId).readHandle) {
      quicSocket_->setReadCallback(streamId, nullptr, std::nullopt);
    }
  }
  sm_.shutdown({.err = error.value_or(0), .msg = "closeSession"});
  if (auto wtHandler = std::exchange(wtHandler_, nullptr)) {
    wtHandler->onSessionEnd(error);
  }
  return folly::unit;
}

void QuicWtSessionBase::onDatagrams(std::vector<IoBufPtr> dgrams) noexcept {
  if (wtHandler_) {
    wtHandler_->onDatagrams(std::move(dgrams));
  }
}

void QuicWtSessionBase::onDatagram(IoBufPtr dgram) noexcept {
  std::vector<IoBufPtr> dgrams;
  dgrams.push_back(std::move(dgram));
  onDatagrams(std::move(dgrams));
}

// -- QuicReadCallback overrides --
void QuicWtSessionBase::QuicReadCallback::readAvailable(StreamId id) noexcept {
  XCHECK(sess.quicSocket_);
  auto& sm = sess.sm_;
  auto& quicSocket = sess.quicSocket_;

  auto* rh = sm.getBidiHandle(id).readHandle;
  if (!rh) {
    XLOG(ERR) << "nullptr rh id=" << id;
    return;
  }
  const auto canRead = sm.recvBytesAvail(*rh);
  if (canRead == 0) {
    sess.maybePauseIngress(*rh);
    return;
  }
  auto readRes = quicSocket->read(id, canRead);
  if (readRes.hasError()) {
    XLOG(ERR) << "::read err id=" << id;
    return;
  }
  auto& [data, eof] = readRes.value();
  sess.noteStreamActivity();
  if (eof) {
    quicSocket->setReadCallback(id, nullptr, std::nullopt);
  }
  auto res = sm.enqueue(
      *rh, WebTransport::StreamData{.data = std::move(data), .fin = eof});
  XCHECK_NE(res, WtStreamManager::Result::Fail);
  if (!eof) { // ::enqueue w/ eof=true may deallocate rh
    sess.maybePauseIngress(*rh);
  }
}

void QuicWtSessionBase::QuicReadCallback::readError(StreamId id,
                                                    QuicError error) noexcept {
  XLOG(DBG4) << __func__ << "; id=" << id << "; err=" << error;
  sess.noteStreamActivity();
  sess.sm_.onResetStream(WtStreamManager::ResetStream{
      .streamId = id,
      .err = sess.fromTransportErrorCode(getQuicAppErrCode(error))});
}

void QuicWtSessionBase::QuicStopSendingCallback::onStopSending(
    StreamId id, quic::ApplicationErrorCode ec) noexcept {
  XLOG(DBG4) << __func__ << "; id=" << id << "; err=" << ec;
  sess.sm_.onStopSending(WtStreamManager::StopSending{
      .streamId = id, .err = sess.fromTransportErrorCode(ec)});
}

// -- StreamManagerCallback overrides --
void QuicWtSessionBase::StreamManagerCallback::readReady(
    WtStreamManager::WtReadHandle& rh, uint64_t bytesRead) noexcept {
  if (sess.manualRead_) {
    // fails harmlessly after stopSending, which already released everything
    auto res =
        sess.quicSocket_->releaseManualReadFlowControl(rh.getID(), bytesRead);
    XLOG_IF(DBG4, res.hasError()) << __func__ << "; err id=" << rh.getID();
  }
  sess.maybeResumeIngress(rh);
}

// no callbacks from ::eventsAvailable is expected to destruct the session
void QuicWtSessionBase::StreamManagerCallback::eventsAvailable() noexcept {
  if (eventsAvailable_++ > 0) { // active ::eventsAvailable already in progress
    return;
  }
  do { // drain all events; in practice not expected to loop more than 1-2 times
    eventsAvailableImpl();
  } while (--eventsAvailable_ > 0);
}

void QuicWtSessionBase::StreamManagerCallback::eventsAvailableImpl() noexcept {
  XCHECK(sess.quicSocket_);
  auto& sock = sess.quicSocket_;
  // process control events first
  QuicWtEventVisitor visitor{*sock, sess.observer_, sess};
  auto events = sess.sm_.moveEvents();
  bool active = !events.empty();
  for (auto& event : events) {
    std::visit(visitor, std::move(event));
  }

  // then process writable streams
  auto& pq = *sess.priorityQueue_;
  while (!pq.empty()) {
    active = true;
    const auto id = pq.getNextScheduledID(std::nullopt);
    XCHECK(id.isStreamID());
    const auto streamId = id.asStreamID();
    auto* wh = CHECK_NOTNULL(sess.sm_.getBidiHandle(streamId).writeHandle);
    /**
     * Dequeue will be capped by the quic conn/stream flow control. This will
     * either dequeue all the buffered data, or saturate the conn/stream flow
     * control -- in either case the stream will be removed from the
     * PriorityQueue by WtStreamManager
     */
    auto streamData = sess.sm_.dequeue(*wh, kMaxVarint);
    XCHECK(streamData.data || streamData.fin);
    auto res = sock->writeChain(streamId,
                                std::move(streamData.data),
                                streamData.fin,
                                streamData.deliveryCallback);
    XLOG_IF(ERR, res.hasError())
        << "::writeChain err= " << res.error() << "; id=" << streamId;

    if (streamData.fin) {
      sock->setStopSendingCallback(streamId, nullptr);
    } else if (isWtStreamEgressFcBlocked(sess.sm_, *wh)) {
      // The cached window may lag the peer's MAX_STREAM_DATA; refreshing it
      // here saves a loop iteration through notifyPendingWriteOnStream.
      grantMaxData(*sock, sess.sm_, *wh);
      if (isWtStreamEgressFcBlocked(sess.sm_, *wh)) {
        sess.quicSocket_->notifyPendingWriteOnStream(streamId, &sess);
      }
    }
  }
  if (active) {
    sess.noteStreamActivity();
  }
}

void QuicWtSessionBase::StreamManagerCallback::onNewPeerStream(
    uint64_t /*streamId*/) noexcept {
}

// -- StreamWriteCallback overrides --
void QuicWtSessionBase::onStreamWriteReady(quic::StreamId streamId,
                                           uint64_t /*maxToSend*/) noexcept {
  XCHECK(quicSocket_);
  if (auto* wh = sm_.getBidiHandle(streamId).writeHandle) {
    grantMaxData(*quicSocket_, sm_, *wh);
  }
}

void QuicWtSessionBase::onStreamWriteError(quic::StreamId id,
                                           QuicError error) noexcept {
  XLOG(ERR) << __func__ << "; id=" << id << "; err=" << error;
  // either transport closed or egress is closed; nothing to do
}

void QuicWtSessionBase::maybePauseIngress(
    WtStreamManager::WtReadHandle& handle) noexcept {
  XCHECK(quicSocket_);
  const auto id = handle.getID();
  if (sm_.recvBytesAvail(handle) == 0) {
    XLOG(DBG4) << __func__ << "; id=" << id;
    auto res = quicSocket_->pauseRead(id);
    XLOG_IF(ERR, res.hasError()) << __func__ << "; err id=" << id;
  }
}

void QuicWtSessionBase::maybeResumeIngress(
    WtStreamManager::WtReadHandle& handle) noexcept {
  XCHECK(quicSocket_);
  const auto id = handle.getID();
  if (sm_.recvBytesAvail(handle) > 0) {
    XLOG(DBG4) << __func__ << "; id=" << id;
    auto res = quicSocket_->resumeRead(id);
    XLOG_IF(ERR, res.hasError()) << __func__ << "; err id=" << id;
  }
}

bool QuicWtSessionBase::acquireIngressStream(uint64_t id) noexcept {
  XCHECK(quicSocket_ && quic::isRemoteStream(quicSocket_->getNodeType(), id));
  // WtStreamManager deduces type from id (i.e. works whether uni or bidi)
  auto handle = sm_.getOrCreateBidiHandle(id);
  const bool success = handle.readHandle;
  const bool bidi = success && handle.writeHandle;
  XLOG(DBG6) << __func__ << "; id=" << id << "; ok=" << success;
  if (success) {
    sm_.setReadCb(*handle.readHandle, &smCb_);
    installReadCallback(id);
    if (bidi) {
      quicSocket_->setStopSendingCallback(id, &stopSendingCb_);
      grantMaxData(*quicSocket_, sm_, *handle.writeHandle);
      wtHandler_->onNewBidiStream(handle);
    } else {
      wtHandler_->onNewUniStream(handle.readHandle);
    }
  }
  return success;
}

/**
 * QuicWtSession implementation below. Most of the functionality is shared with
 * QuicWtSessionBase - however this derived class assumes ownership of all of
 * QuicSocket's streams, so it owns and installs a
 * QuicConnectionCallback. Similarly the destructor/::closeSession closes all
 * streams on the underlying QuicSocket via QuicSocket::close.
 */
QuicWtSession::QuicWtSession(std::shared_ptr<quic::QuicSocket> quicSocket,
                             std::unique_ptr<WebTransportHandler> wtHandler)
    : QuicWtSession(
          quicSocket,
          WtHandlerPtr{wtHandler.release(), WtHandlerDeleter{.owning = true}}) {
}

QuicWtSession::QuicWtSession(std::shared_ptr<quic::QuicSocket> quicSocket,
                             WebTransportHandler* wtHandler)
    : QuicWtSession(
          quicSocket,
          WtHandlerPtr{wtHandler, WtHandlerDeleter{.owning = false}}) {
}

QuicWtSession::QuicWtSession(
    const std::shared_ptr<quic::QuicSocket>& quicSocket, WtHandlerPtr wtHandler)
    : QuicWtSessionBase(quicSocket,
                        std::move(wtHandler),
                        createQuicConfig(quicSocket->getTransportSettings()),
                        /*usesEncodedApplicationErrorCodes=*/false) {
  manualRead_ = true;
  quicSocket_->setConnectionCallback(&connCb_);
  quicSocket_->setDatagramCallback(&dgramCb_);
}

QuicWtSession::~QuicWtSession() {
  QuicWtSession::closeSession(folly::none);
}

folly::Expected<folly::Unit, WebTransport::ErrorCode>
QuicWtSession::closeSession(folly::Optional<uint32_t> error) noexcept {
  // Keep the socket alive across handler callbacks.
  auto quicSocket = quicSocket_;
  quicSocket->setConnectionCallback(nullptr);
  quicSocket->setDatagramCallback(nullptr);
  QuicWtSessionBase::closeSession(error);
  quicSocket->close(QuicError(quic::ApplicationErrorCode(error.value_or(0))));
  return folly::unit;
}

// -- QuicConnectionCallback overrides --
void QuicWtSession::QuicConnectionCallback::onNewBidirectionalStream(
    StreamId id) noexcept {
  // always expected to succeed as config.peerMaxStreamsBidi = inf
  XCHECK(sess.acquireIngressStream(id));
}

void QuicWtSession::QuicConnectionCallback::onNewUnidirectionalStream(
    StreamId id) noexcept {
  // always expected to succeed as config.peerMaxStreamsUni = inf
  XCHECK(sess.acquireIngressStream(id));
}

void QuicWtSession::QuicConnectionCallback::onConnectionEnd() noexcept {
  sess.closeSession(folly::none);
}

void QuicWtSession::QuicConnectionCallback::onConnectionEnd(
    QuicError error) noexcept {
  sess.closeSession(getQuicAppErrCode(error));
}

void QuicWtSession::QuicConnectionCallback::onConnectionError(
    QuicError error) noexcept {
  sess.closeSession(getQuicAppErrCode(error));
}

void QuicWtSession::QuicConnectionCallback::onBidirectionalStreamsAvailable(
    uint64_t numStreamsAvailable) noexcept {
  if (numStreamsAvailable > 0) {
    sess.onBidiStreamCreditAvail();
  }
}

void QuicWtSession::QuicConnectionCallback::onUnidirectionalStreamsAvailable(
    uint64_t numStreamsAvailable) noexcept {
  if (numStreamsAvailable > 0) {
    sess.onUniStreamCreditAvail();
  }
}

void QuicWtSession::QuicDgramCallback::onDatagramsAvailable() noexcept {
  XCHECK(sess.quicSocket_);
  auto& quicSocket = sess.quicSocket_;

  auto result = quicSocket->readDatagramBufs();
  if (result.hasError()) {
    XLOG(ERR) << __func__ << "; err=" << toString(result.error());
    sess.closeSession(WebTransport::kInternalError);
    return;
  }

  XLOG(DBG4) << "rx nDatagrams=" << result->size();
  sess.onDatagrams(std::move(result.value()));
}

H3WtSession::H3WtSession(std::shared_ptr<quic::QuicSocket> quicSocket,
                         std::unique_ptr<WebTransportHandler> wtHandler,
                         WtStreamManager::WtConfig wtConfig,
                         uint64_t connectStreamId,
                         H3ConnectStreamCallback& observer) noexcept
    : QuicWtSessionBase(
          std::move(quicSocket),
          WtHandlerPtr{wtHandler.release(), WtHandlerDeleter{.owning = true}},
          std::move(wtConfig),
          /*usesEncodedApplicationErrorCodes=*/true,
          &observer),
      connectStreamId_(connectStreamId) {
  XCHECK_LE(connectStreamId, detail::kMaxVarint);
}

H3WtSession::~H3WtSession() noexcept {
  H3WtSession::closeSession(folly::none);
}

void H3WtSession::onWtSession(std::shared_ptr<WebTransport> wt) noexcept {
  CHECK_NOTNULL(wtHandler_.get())->onWebTransportSession(std::move(wt));
}

folly::Expected<folly::Unit, WebTransport::ErrorCode> H3WtSession::closeSession(
    folly::Optional<uint32_t> error) noexcept {
  // we need to bidi reset all assoc quic streams (ss+rst_stream)
  auto streamIds = sm_.streamIds();
  XLOG(DBG4) << __func__ << "; nStreams=" << streamIds.size();
  const uint64_t ec = toTransportErrorCode(error.value_or(0));
  // bidirectionally reset all assoc quic streams
  for (uint64_t id : streamIds) {
    quicSocket_->setReadCallback(id, nullptr, ec);
    quicSocket_->setStopSendingCallback(id, nullptr);
    quicSocket_->resetStream(id, ec);
  }
  QuicWtSessionBase::closeSession(error);
  return folly::unit;
}

H3WtSession::IoBufPtr H3WtSession::frameDatagram(IoBufPtr datagram) noexcept {
  /**
   * RFC9297:
   * HTTP/3 Datagram {
   *   Quarter Stream ID (i),
   *   HTTP Datagram Payload (..),
   * }
   */
  const uint64_t quarterStreamId = connectStreamId_ / 4;
  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  constexpr uint8_t kGrowth = 8;
  folly::io::QueueAppender appender(&queue, kGrowth);
  auto encodeRes = quic::encodeQuicInteger(
      quarterStreamId, [&appender](auto val) { appender.writeBE(val); });
  XCHECK(encodeRes);
  queue.append(std::move(datagram));
  return queue.move();
}

folly::Expected<WebTransport::StreamWriteHandle*, WebTransport::ErrorCode>
H3WtSession::createUniStream() noexcept {
  auto result = QuicWtSessionBase::createUniStream();
  if (result) {
    writeWtFramePrefix(result.value()->getID());
  }
  return result;
}

folly::Expected<WebTransport::BidiStreamHandle, WebTransport::ErrorCode>
H3WtSession::createBidiStream() noexcept {
  auto result = QuicWtSessionBase::createBidiStream();
  if (result) {
    writeWtFramePrefix(result->writeHandle->getID());
  }
  return result;
}

void H3WtSession::writeWtFramePrefix(uint64_t id) noexcept {
  auto* writeFn = quic::isBidirectionalStream(id) ? writeWtBidiFramePrefix
                                                  : writeWtUniFramePrefix;
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  writeFn(writeBuf, connectStreamId_);
  XCHECK(quicSocket_->writeChain(id, writeBuf.move(), false));
  noteStreamActivity();
}

auto H3WtSession::onConnMaxData(WtStreamManager::MaxConnData mcd) noexcept
    -> WtSmResult {
  return sm_.onMaxData(mcd);
}
auto H3WtSession::onMaxStreams(WtStreamManager::MaxStreamsUni ms) noexcept
    -> WtSmResult {
  const bool wasAvail = hasEgressUniCredit();
  auto res = sm_.onMaxStreams(ms);
  if (!wasAvail && hasEgressUniCredit()) {
    onUniStreamCreditAvail();
  }
  return res;
}
auto H3WtSession::onMaxStreams(WtStreamManager::MaxStreamsBidi ms) noexcept
    -> WtSmResult {
  const bool wasAvail = hasEgressBidiCredit();
  auto res = sm_.onMaxStreams(ms);
  if (!wasAvail && hasEgressBidiCredit()) {
    onBidiStreamCreditAvail();
  }
  return res;
}
void H3WtSession::onDrainSession(WtStreamManager::DrainSession ds) noexcept {
  return sm_.onDrainSession(ds);
}
void H3WtSession::onCloseSession(WtStreamManager::CloseSession&& cs) noexcept {
  // TODO(@damlaj): change WebTransport::closeSession to take a std::string msg?
  XLOG(DBG4) << __func__ << "; cs=" << cs.err << "; msg=" << cs.msg;
  closeSession(cs.err);
}

void H3ConnectStreamCallback::onEvent(WtStreamManager::Event&& ev) noexcept {
  std::visit(visitor, ev);
}

H3WtCapsuleCallback::H3WtCapsuleCallback(H3WtSession& session) noexcept
    : h3Wt_(session) {
}

void H3WtCapsuleCallback::onMaxData(WTMaxDataCapsule c) noexcept {
  XLOG(DBG6) << __func__ << "; maxData=" << c.maximumData;
  h3Wt_.onConnMaxData({.maxData = c.maximumData});
}

void H3WtCapsuleCallback::onMaxStreamsBidi(WTMaxStreamsCapsule c) noexcept {
  XLOG(DBG6) << __func__ << "; maxStreams=" << c.maximumStreams;
  h3Wt_.onMaxStreams(WtStreamManager::MaxStreamsBidi{c.maximumStreams});
}

void H3WtCapsuleCallback::onMaxStreamsUni(WTMaxStreamsCapsule c) noexcept {
  XLOG(DBG6) << __func__ << "; maxStreams=" << c.maximumStreams;
  h3Wt_.onMaxStreams(WtStreamManager::MaxStreamsUni{c.maximumStreams});
}

void H3WtCapsuleCallback::onDrainSession(
    DrainWebTransportSessionCapsule) noexcept {
  XLOG(DBG6) << __func__;
  h3Wt_.onDrainSession({});
}

void H3WtCapsuleCallback::onCloseSession(
    CloseWebTransportSessionCapsule c) noexcept {
  XLOG(DBG6) << __func__ << "; err=" << c.applicationErrorCode
             << "; msg=" << c.applicationErrorMessage;
  h3Wt_.onCloseSession(
      {.err = c.applicationErrorCode, .msg = c.applicationErrorMessage});
}

void H3WtCapsuleCallback::onDatagram(DatagramCapsule c) noexcept {
  XLOG(DBG6) << __func__;
  h3Wt_.onDatagram(std::move(c.httpDatagramPayload));
}

void H3WtCapsuleCallback::onConnectionError(
    CapsuleCodec::ErrorCode error) noexcept {
  XLOG(DBG4) << __func__ << "; err=" << uint64_t(error);
  onCloseSession({.applicationErrorCode = uint32_t(error),
                  .applicationErrorMessage = "capsule parse error"});
}

} // namespace proxygen
