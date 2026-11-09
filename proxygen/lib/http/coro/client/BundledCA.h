/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace proxygen::coro {

inline constexpr char kFallbackCABundle[] = R"(
{{CA_BUNDLE}}
)";

} // namespace proxygen::coro
