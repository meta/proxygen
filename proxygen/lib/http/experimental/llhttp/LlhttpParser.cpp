/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "proxygen/lib/http/experimental/llhttp/LlhttpParser.h"

#include <cassert>

namespace facebook::detail {

constexpr char kNoInput = '\0';

LlhttpParser::LlhttpParser(llhttp_type_t type,
                           const llhttp_settings_t* settings,
                           void* callbackContext,
                           LlhttpParserOptions options) {
  assert(settings != nullptr);
  llhttp_init(&parser_, type, settings);
  parser_.data = callbackContext;

  llhttp_set_lenient_headers(&parser_, options.lenientHeaders);
  llhttp_set_lenient_chunked_length(&parser_, options.lenientChunkedLength);
  llhttp_set_lenient_keep_alive(&parser_, options.lenientKeepAlive);
  llhttp_set_lenient_transfer_encoding(&parser_,
                                       options.lenientTransferEncoding);
  llhttp_set_lenient_version(&parser_, options.lenientVersion);
  llhttp_set_lenient_data_after_close(&parser_, options.lenientDataAfterClose);
  llhttp_set_lenient_optional_lf_after_cr(&parser_,
                                          options.lenientOptionalLfAfterCr);
  llhttp_set_lenient_optional_cr_before_lf(&parser_,
                                           options.lenientOptionalCrBeforeLf);
  llhttp_set_lenient_optional_crlf_after_chunk(
      &parser_, options.lenientOptionalCrlfAfterChunk);
  llhttp_set_lenient_spaces_after_chunk_size(
      &parser_, options.lenientSpacesAfterChunkSize);
  llhttp_set_lenient_header_value_relaxed(&parser_,
                                          options.lenientHeaderValueRelaxed);
}

size_t LlhttpParser::execute(const char* data, size_t len) {
  if (error() != HPE_OK) {
    return 0;
  }

  pendingIngress_ = false;
  active_ = true;
  const auto executeError = llhttp_execute(&parser_, data, len);
  active_ = false;
  pendingIngress_ = executeError == HPE_PAUSED && len > 0 &&
                    llhttp_get_error_pos(&parser_) == data + len;
  if (executeError == HPE_OK) {
    return len;
  }

  return static_cast<size_t>(llhttp_get_error_pos(&parser_) - data);
}

void LlhttpParser::runPendingIngress() {
  pendingIngress_ = false;
  execute(&kNoInput, 0);
}

void LlhttpParser::finish() {
  // Don't finish if the parser is already in an error state
  if (hasFatalError()) {
    return;
  }
  active_ = true;
  const auto err = llhttp_finish(&parser_);
  active_ = false;
  if (err != HPE_OK) {
    parser_.error = err;
  }
}

void LlhttpParser::setError(llhttp_errno_t error, const char* reason) {
  assert(error != HPE_OK);
  assert(reason != nullptr);

  parser_.error = static_cast<int32_t>(error);
  llhttp_set_error_reason(&parser_, reason);
}

void LlhttpParser::pause() {
  llhttp_pause(&parser_);
}

void LlhttpParser::resume() {
  llhttp_resume(&parser_);
}

void LlhttpParser::resumeAfterUpgrade() {
  llhttp_resume_after_upgrade(&parser_);
}

const char* LlhttpParser::errorReason() const {
  return llhttp_get_error_reason(&parser_);
}

const char* LlhttpParser::methodStr() const {
  return llhttp_method_name(static_cast<llhttp_method_t>(parser_.method));
}

const char* llhttpErrToContext(llhttp_errno_t err) {
  switch (err) {
    case HPE_INTERNAL:
      return "internal";
    case HPE_STRICT:
      return "strict";
    case HPE_LF_EXPECTED:
      return "lf-expected";
    case HPE_CR_EXPECTED:
      return "cr-expected";
    case HPE_UNEXPECTED_SPACE:
      return "unexpected-space";
    case HPE_UNEXPECTED_CONTENT_LENGTH:
      return "unexpected-content-length";
    case HPE_CLOSED_CONNECTION:
      return "closed-connection";
    case HPE_INVALID_METHOD:
      return "invalid-method";
    case HPE_INVALID_URL:
      return "invalid-url";
    case HPE_INVALID_CONSTANT:
      return "invalid-constant";
    case HPE_INVALID_VERSION:
      return "invalid-version";
    case HPE_INVALID_HEADER_TOKEN:
      return "invalid-header-token";
    case HPE_INVALID_CONTENT_LENGTH:
      return "invalid-content-length";
    case HPE_INVALID_CHUNK_SIZE:
      return "invalid-chunk-size";
    case HPE_INVALID_STATUS:
      return "invalid-status";
    case HPE_INVALID_EOF_STATE:
      return "invalid-eof-state";
    case HPE_INVALID_TRANSFER_ENCODING:
      return "invalid-transfer-encoding";
    case HPE_PAUSED_H2_UPGRADE:
      return "h2-upgrade";
    default:
      return "";
  }
}

} // namespace facebook::detail
