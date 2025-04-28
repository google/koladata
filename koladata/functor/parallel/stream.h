// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#ifndef KOLADATA_FUNCTOR_PARALLEL_STREAM_H_
#define KOLADATA_FUNCTOR_PARALLEL_STREAM_H_

#include <cstddef>
#include <memory>
#include <utility>
#include <variant>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/fingerprint.h"

namespace koladata::functor::parallel {

class Stream;
class StreamWriter;
class StreamReader;

using StreamPtr = std::shared_ptr<Stream>;
using StreamReaderPtr = std::shared_ptr<StreamReader>;

// Note: The stream writer is owned by the stream object. The recommended
// pattern is to use the StreamWriter with a std::weak_ptr. This way, the
// producer can determine if the stream is no longer owned by any consumer.
using StreamWriterPtr = std::weak_ptr<StreamWriter>;

// Stream of values.
//
// The stream keeps all the values in memory, and can be read more than once.
//
// Note: Consider replacing this interface with a class if it provides enough
// performance benefits.
class Stream {
 public:
  // Returns the value type of the stream.
  arolla::QTypePtr value_qtype() const { return value_qtype_; }

  // Returns a unique identifier for the stream.
  arolla::Fingerprint uuid() const { return uuid_; };

  // Creates a new reader for the stream.
  virtual StreamReaderPtr MakeReader() = 0;

  // Disallow copy and move.
  Stream(const Stream&) = delete;
  Stream& operator=(const Stream&) = delete;

  virtual ~Stream() = default;

 protected:
  explicit Stream(arolla::QTypePtr value_qtype) : value_qtype_(value_qtype) {}

 private:
  // Value type of the stream.
  const arolla::QTypePtr value_qtype_;

  // Unique identifier for the stream.
  const arolla::Fingerprint uuid_ = arolla::RandomFingerprint();
};

// Writer interface for the stream.
//
// Important:
//  * The stream writer is owned by the stream object. The recommended pattern
//    is to use the StreamWriter with a weak_ptr. This allows the stream to be
//    automatically deleted if the stream no longer owned by anything.
//  * It's strongly adviced that all stream were closed, because otherwise tasks
//    waiting for the next item can hang indefinitelly. The stream can only be
//    closed using the method Close(). Particularly, the stream cannot be closed
//    by itself.
//
class StreamWriter {
 public:
  // Writes a value to the stream.
  //
  // Important: writing a value of incorrect type (non value_qtype) or writing
  // to a closed stream results in undefined behaviour!
  virtual void Write(arolla::TypedRef value) = 0;

  // Closes the stream with the given status.
  //
  // Important: Closing a closed stream results in undefined behaviour!
  virtual void Close(absl::Status status) = 0;

  // Closes the stream with absl::OkStatus().
  //
  // Important: Closing a closed stream results in undefined behaviour!
  void Close() { Close(absl::OkStatus()); }

  // Disallow copy and move.
  StreamWriter(const StreamWriter&) = delete;
  StreamWriter& operator=(const StreamWriter&) = delete;

  StreamWriter() = default;
  virtual ~StreamWriter() = default;
};

// Reader interface for the stream.
class StreamReader {
 public:
  struct TryReadResult;

  // Attempts a non-blocking read operation. If no data is immediately available
  // and the stream is still open, returns a result with neither `item` nor
  // `close_status` set.
  virtual TryReadResult TryRead() = 0;

  // Subscribes a callback to be invoked when the stream's state changes
  // such that a subsequent TryRead() call is guaranteed to return a non-empty
  // result.
  //
  // Note: If the reader/stream is destroyed after the subscription, there is no
  // guarantee whether the callback will be invoked.
  virtual void SubscribeOnce(absl::AnyInvocable<void() &&>&& callback) = 0;

  // Disallow copy and move.
  StreamReader(const StreamReader&) = delete;
  StreamReader& operator=(const StreamReader&) = delete;

  StreamReader() = default;
  virtual ~StreamReader() = default;
};

// Variant type returned by StreamReader::TryRead().
struct StreamReader::TryReadResult
    : std::variant<std::monostate, arolla::TypedRef, absl::Status> {
  using std::variant<std::monostate, arolla::TypedRef, absl::Status>::variant;

  // Indicates that the stream was still open, but no data were immediately
  // available.
  bool empty() const { return index() == 0; }

  // Returns a pointer to the next stream element; `nullptr` indicates that no
  // data were immediately available.
  arolla::TypedRef* item() { return std::get_if<1>(this); }

  // Returns a pointer to the stream close status; `nullptr` indicates that
  // the stream may have more data to read.
  absl::Status* close_status() { return std::get_if<2>(this); }
};

// Creates a new stream with the given value type and initial capacity.
//
// The initial capacity can be specified to avoid additional allocations if
// the stream size is known in advance. However, the initial capacity can be
// exceeded.
std::pair<StreamPtr, StreamWriterPtr> MakeStream(arolla::QTypePtr value_qtype,
                                                 size_t initial_capacity = 0);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_STREAM_H_
