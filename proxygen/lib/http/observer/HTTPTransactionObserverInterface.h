/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <folly/Optional.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/utils/Time.h>

namespace proxygen {

class HTTPTransactionObserverAccessor;

/**
 * Observer of HTTP transaction events.
 */
class HTTPTransactionObserverInterface {
 public:
  virtual ~HTTPTransactionObserverInterface() = default;

  enum class Events {
    TxnBytes = 1,
    TxnPriority = 2,
  };

  struct TxnBytesEvent {
    /**
     * Where one write's body payload landed on a transport stream: `length`
     * payload bytes starting at body offset `bodyOffset` were written at
     * stream offset `streamOffset` of stream `streamId`.
     */
    struct BodyByteStreamRange {
      uint64_t streamId;
      uint64_t bodyOffset;
      uint64_t streamOffset;
      size_t length;

      struct BuilderFields {
        folly::Optional<uint64_t> maybeStreamId;
        folly::Optional<uint64_t> maybeBodyOffset;
        folly::Optional<uint64_t> maybeStreamOffset;
        folly::Optional<size_t> maybeLength;
        explicit BuilderFields() = default;
      };

      struct Builder : private BuilderFields {
        Builder&& setStreamId(uint64_t streamIdIn);
        Builder&& setBodyOffset(uint64_t bodyOffsetIn);
        Builder&& setStreamOffset(uint64_t streamOffsetIn);
        Builder&& setLength(size_t lengthIn);
        BodyByteStreamRange build() &&;
        explicit Builder() = default;
      };

      explicit BodyByteStreamRange(BuilderFields& builderFields);
    };

    enum class Type : uint8_t {
      LAST_HEADER_BYTE_READ,
      BODY_BYTES_READ,
      FIRST_HEADER_BYTE_WRITE,
      FIRST_BODY_BYTE_WRITE,
      FIRST_BODY_BYTE_ACK,
      LAST_BODY_BYTE_WRITE,
      LAST_BODY_BYTE_ACK,
      BODY_BYTES_GENERATED,
      HEADER_BYTES_GENERATED,
    };

    const proxygen::TimePoint timestamp;
    const Type type;
    // For BODY_BYTES_GENERATED, each event describes one transport write:
    // `maybeNumBytes` is its encoded size, `maybeNumBodyBytes` the payload
    // bytes inside it, and `maybeBodyByteStreamRange` where that payload
    // landed on the stream, when the transport knows. EOM bytes written on
    // their own are an event with zero body bytes.
    const folly::Optional<size_t> maybeNumBytes;
    const folly::Optional<size_t> maybeNumBodyBytes;
    const folly::Optional<std::reference_wrapper<const HTTPMessage>>
        maybeHeadersRef;
    const folly::Optional<BodyByteStreamRange> maybeBodyByteStreamRange;

    TxnBytesEvent(TxnBytesEvent&&) = delete;
    TxnBytesEvent& operator=(const TxnBytesEvent&) = delete;
    TxnBytesEvent& operator=(TxnBytesEvent&& rhs) = delete;
    TxnBytesEvent(const TxnBytesEvent&) = delete;

    struct BuilderFields {
      folly::Optional<std::reference_wrapper<const proxygen::TimePoint>>
          maybeTimestampRef;
      Type type;
      size_t numBytes;
      folly::Optional<size_t> numBodyBytes;
      folly::Optional<std::reference_wrapper<const HTTPMessage>>
          maybeHeadersRef;
      folly::Optional<BodyByteStreamRange> maybeBodyByteStreamRange;
      explicit BuilderFields() = default;
    };

    struct Builder : public BuilderFields {
      Builder&& setTimestamp(const proxygen::TimePoint& timestampIn);
      Builder&& setType(Type typeIn);
      Builder&& setNumBytes(const size_t numBytes);
      Builder&& setNumBodyBytes(size_t numBodyBytesIn);
      Builder&& setHeaders(const proxygen::HTTPMessage& headersIn);
      Builder&& setBodyByteStreamRange(
          const BodyByteStreamRange& bodyByteStreamRangeIn);
      TxnBytesEvent build() &&;
      explicit Builder() = default;
    };

    // Use builder to construct
    explicit TxnBytesEvent(const BuilderFields& builderFields);
  };

  /**
   * A new HTTP priority for the transaction: one the session applied to
   * egress scheduling, or one the peer requested that the session did not
   * apply (see `applied`).
   */
  struct TxnPriorityEvent {
    // What the applied priority came from.
    enum class Source : uint8_t {
      // The Priority header of the request message.
      REQUEST_HEADER,
      // The Priority header of a response message.
      RESPONSE_HEADER,
      // A PRIORITY_UPDATE frame that arrived before the request headers,
      // applied once they arrived. It overrides the request header.
      BUFFERED_PRIORITY_UPDATE,
      // A PRIORITY_UPDATE frame that arrived after the request headers.
      PRIORITY_UPDATE,
      // The local application (`HTTPTransaction::updateAndSendPriority`).
      LOCAL,
    };

    TxnPriorityEvent(const proxygen::TimePoint& timestampIn,
                     const HTTPPriority& priorityIn,
                     Source sourceIn,
                     bool appliedIn)
        : timestamp(timestampIn),
          priority(priorityIn),
          source(sourceIn),
          applied(appliedIn) {
    }

    proxygen::TimePoint timestamp;
    HTTPPriority priority;
    Source source;
    // Whether the session applied `priority` to egress scheduling. False
    // means the peer requested this priority and the session did not apply
    // it: `HTTPSession` (HTTP/1.x and HTTP/2) does not schedule by priority.
    bool applied;
  };

  virtual void onBytesEvent(HTTPTransactionObserverAccessor* /* txn */,
                            const TxnBytesEvent& /* event */) noexcept {
  }

  virtual void onPriorityEvent(HTTPTransactionObserverAccessor* /* txn */,
                               const TxnPriorityEvent& /* event */) noexcept {
  }
};

} // namespace proxygen
