/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <proxygen/lib/transport/qmux/QmuxTransport.h>

#include <glog/logging.h>

namespace proxygen::qmux {

QmuxTransport::~QmuxTransport() = default;

folly::coro::Task<folly::Unit> QmuxTransport::write(
    folly::IOBufQueue& /*writeBuf*/,
    std::chrono::milliseconds /*timeout*/,
    DeliveryCallback /*callback*/) {
  LOG(FATAL) << "Unimplemented";
}

} // namespace proxygen::qmux
