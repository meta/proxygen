# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

proxygen_add_library(proxygen_hq_devious_baton
  SRCS
    DeviousBaton.cpp
    DeviousBatonWtHandler.cpp
  DEPS
    proxygen_http_webtransport_httpwebtransport
    mvfst::mvfst_codec_types
    mvfst::mvfst_folly_utils
    Folly::folly_container_f14_hash
    Folly::folly_futures_core
  EXPORTED_DEPS
    proxygen_http_message
    proxygen_webtransport
    Folly::folly_io_iobuf
    Folly::folly_io_async_async_base
)
