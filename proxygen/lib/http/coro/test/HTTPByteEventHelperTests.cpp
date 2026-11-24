/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/io/async/testing/MockAsyncSocket.h>
#include <folly/portability/GTest.h>
#include <proxygen/lib/http/coro/HTTPByteEventHelpers.h>
#include <proxygen/lib/http/coro/test/Mocks.h>

using namespace testing;
using namespace folly::test;

namespace proxygen::coro::test {

using ByteEvent = folly::AsyncSocketObserverInterface::ByteEvent;
using TxAckEvent = AsyncSocketByteEventObserver::TxAckEvent;

TEST(HttpByteEventHelper, TimeoutThenFire) {
  // construct objects
  folly::EventBase evb;
  MockAsyncSocket sock{&evb};
  AsyncSocketByteEventObserver obs;
  obs.setByteEventTimeout(std::chrono::milliseconds(10));
  obs.observerAttach(&sock);
  obs.byteEventsEnabled(&sock);

  // add byte event registration
  StrictMock<MockByteEventCallback> mockCallback;
  std::vector<HTTPByteEventRegistration> registrations;
  HTTPByteEventRegistration reg;
  reg.streamID = 1ull;
  reg.events = uint8_t(HTTPByteEvent::Type::NIC_TX);
  reg.callback = mockCallback.getWeakRefCountedPtr();
  registrations.push_back(std::move(reg));
  obs.registerByteEvents(
      /*streamID=*/1,
      /*sessionByteOffset=*/0,
      /*sessionBytesScheduled=*/100,
      /*streamOffset=*/0,
      /*fsInfo=*/folly::none,
      /*bodyOffset=*/0,
      std::move(registrations),
      /*eom=*/false);

  // assume transport write completed successfully
  ByteEvent txEvent;
  txEvent.type = ByteEvent::WRITE;
  txEvent.offset = 100;
  obs.byteEvent(&sock, txEvent);
  obs.transportWriteComplete(100, obs.nextTxAckEvent());

  // timeout fires at 10ms, calling onByteEventCanceled.
  EXPECT_CALL(mockCallback, onByteEventCanceled(_, _));

  evb.runAfterDelay(
      [&] {
        // deliver TX byte event after 100ms (after timeout_ms)
        ByteEvent txEvent;
        txEvent.type = ByteEvent::TX;
        txEvent.offset = 100;
        obs.byteEvent(&sock, txEvent);
        evb.terminateLoopSoon();
      },
      /*milliseconds=*/100);

  evb.loopForever();
}

} // namespace proxygen::coro::test
