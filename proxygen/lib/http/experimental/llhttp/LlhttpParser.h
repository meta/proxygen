/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>

#ifdef HTTP_ERRNO_MAP
#pragma push_macro("HTTP_ERRNO_MAP")
#undef HTTP_ERRNO_MAP
#define PROXYGEN_LLHTTP_RESTORE_HTTP_ERRNO_MAP
#endif
#include "llhttp.h"
#ifdef PROXYGEN_LLHTTP_RESTORE_HTTP_ERRNO_MAP
#undef HTTP_ERRNO_MAP
#pragma pop_macro("HTTP_ERRNO_MAP")
#undef PROXYGEN_LLHTTP_RESTORE_HTTP_ERRNO_MAP
#else
#undef HTTP_ERRNO_MAP
#endif

namespace facebook::detail {

// One field per llhttp_set_lenient_*() flag
struct LlhttpParserOptions {
  bool lenientHeaders{false};
  bool lenientChunkedLength{false};
  bool lenientKeepAlive{false};
  bool lenientTransferEncoding{false};
  bool lenientVersion{false};
  bool lenientDataAfterClose{false};
  bool lenientOptionalLfAfterCr{false};
  bool lenientOptionalCrBeforeLf{false};
  bool lenientOptionalCrlfAfterChunk{false};
  bool lenientSpacesAfterChunkSize{false};
  bool lenientHeaderValueRelaxed{false};
};

class LlhttpParser {
 public:
  LlhttpParser(llhttp_type_t type,
               const llhttp_settings_t* settings,
               void* callbackContext,
               LlhttpParserOptions options = {});
  ~LlhttpParser() = default;

  LlhttpParser(const LlhttpParser&) = delete;
  LlhttpParser& operator=(const LlhttpParser&) = delete;
  LlhttpParser(LlhttpParser&&) = delete;
  LlhttpParser& operator=(LlhttpParser&&) = delete;

  size_t execute(const char* data, size_t len);
  void finish();
  void setError(llhttp_errno_t error, const char* reason);

  void pause();
  void resume();
  void resumeAfterUpgrade();
  [[nodiscard]] bool isUpgrade() const {
    return parser_.upgrade != 0;
  }
  // What returning 2 from on_headers_complete does
  void markUpgrade() {
    parser_.upgrade = 1;
    parser_.flags |= F_SKIPBODY;
  }

  [[nodiscard]] llhttp_errno_t error() const {
    return static_cast<llhttp_errno_t>(parser_.error);
  }
  [[nodiscard]] const char* errorReason() const;
  [[nodiscard]] bool isPaused() const {
    return error() == HPE_PAUSED;
  }
  [[nodiscard]] bool hasFatalError() const {
    const auto currentError = error();
    return currentError != HPE_OK && currentError != HPE_PAUSED &&
           currentError != HPE_PAUSED_UPGRADE;
  }
  [[nodiscard]] bool isActive() const {
    return active_;
  }

  [[nodiscard]] uint8_t httpMajor() const {
    return parser_.http_major;
  }
  [[nodiscard]] uint8_t httpMinor() const {
    return parser_.http_minor;
  }
  void setHttpVersion(uint8_t major, uint8_t minor) {
    parser_.http_major = major;
    parser_.http_minor = minor;
  }
  [[nodiscard]] uint16_t statusCode() const {
    return parser_.status_code;
  }
  [[nodiscard]] const char* methodStr() const;
  [[nodiscard]] bool isChunked() const {
    return (parser_.flags & F_CHUNKED) != 0;
  }
  [[nodiscard]] uint64_t contentLength() const {
    return parser_.content_length;
  }
  void skipBody() {
    parser_.flags |= F_SKIPBODY;
  }

 private:
  llhttp_t parser_{};
  bool active_{false};
};

} // namespace facebook::detail
