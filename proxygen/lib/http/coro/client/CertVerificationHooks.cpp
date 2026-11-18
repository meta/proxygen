/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "proxygen/lib/http/coro/client/CertVerificationHooks.h"

#include <stdexcept>

#include <fizz/protocol/DefaultCertificateVerifier.h>
#include <fizz/util/Status.h>
#include <fmt/format.h>
#include <folly/Range.h>
#include <folly/ssl/OpenSSLCertUtils.h>

#include "proxygen/lib/http/coro/client/BundledCA.h"

namespace proxygen::coro {

std::shared_ptr<const fizz::CertificateVerifier> getFallbackVerifier() {
  fizz::Error err;
  std::unique_ptr<fizz::DefaultCertificateVerifier> verifier;
  const char* bundle = trustbundle::getBundledPEM();
  if (auto result = fizz::DefaultCertificateVerifier::create(
          verifier,
          err,
          fizz::VerificationContext::Client,
          folly::ssl::OpenSSLCertUtils::readStoreFromBuffer(
              folly::StringPiece(bundle)));
      result != fizz::Status::Success) {
    FIZZ_THROW_ON_ERROR(
        err.error(fmt::format(
            "HTTPCoroConnector::makeFizzCertVerifier failed "
            "to initialize TLS trust anchors with fallback CA bundle: {}",
            err.msg())),
        err);
  }
  return verifier;
}

CertVerifyLogFn getCertVerifyLogFn() noexcept {
  return nullptr;
}

std::optional<ValidationPolicy> getValidationPolicy(
    HTTPCoroConnector::IdentityValidation identityValidation) {
  switch (identityValidation) {
    case HTTPCoroConnector::IdentityValidation::Disabled_INSECURE:
      return std::nullopt;
    case HTTPCoroConnector::IdentityValidation::Enforcing:
      return ValidationPolicy::Enforcing;
    case HTTPCoroConnector::IdentityValidation::Logging:
      return ValidationPolicy::Logging;
  }
  throw std::invalid_argument("unknown identity validation mode");
}

} // namespace proxygen::coro
