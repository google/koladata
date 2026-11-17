// Copyright 2025 Google LLC
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
#ifndef KOLADATA_FUNCTOR_PARALLEL_STREAM_COMPOSITION_H_
#define KOLADATA_FUNCTOR_PARALLEL_STREAM_COMPOSITION_H_

#include <memory>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

// This class can be used to interleave multiple streams together, including
// cases where not all streams are known in advance, but rather added one by
// one. This class is thread-safe.
class StreamInterleave {
 public:
  explicit StreamInterleave(StreamWriterPtr absl_nonnull writer);

  // Movable but non-copyable.
  StreamInterleave(StreamInterleave&&) = default;
  StreamInterleave& operator=(StreamInterleave&&) = default;

  // Returns true if further manipulations on the resulting stream can no longer
  // reach any reader (e.g., if no potential readers are left, or the stream has
  // already been closed).
  bool Orphaned() const;

  // Adds one more input stream to the interleave.
  void Add(const StreamPtr absl_nonnull& stream);

  // Adds one more item to the interleave.
  void AddItem(arolla::TypedRef item);

  // Adds an error to the interleaved stream; `status` must be non-OK.
  void AddError(absl::Status status);

 private:
  class Scheduler;

  // We need a shared pointer here since callbacks will hold a reference
  // to the Scheduler, and nothing else will own it once it is finalized.
  // Scheduler instances are expected to be only owned by callbacks and by the
  // the StreamInterleave. Therefore, as soon as the callbacks are executed and
  // the StreamInterleave is destroyed (so no new inputs can be added),
  // the Scheduler will be destroyed and the circular ownership of
  // stream->callback->scheduler->stream will be broken.
  std::shared_ptr<Scheduler> scheduler_;
};

// This class can be used to chain multiple streams together, including cases
// where not all streams are known in advance, but rather added one by one.
// This class is thread-safe (but it is expected that all Add calls will
// still be done in a particular sequence, otherwise the output can depend
// on a race).
class StreamChain {
 public:
  explicit StreamChain(StreamWriterPtr absl_nonnull writer);

  // Movable but non-copyable.
  StreamChain(StreamChain&&) = default;
  StreamChain& operator=(StreamChain&&) = default;

  // Returns true if further manipulations on the resulting stream can no longer
  // reach any reader (e.g., if no potential readers are left, or the stream has
  // already been closed).
  bool Orphaned() const;

  // Adds one more input stream to the chain.
  void Add(const StreamPtr absl_nonnull& stream);

  // Adds one more item to the chain.
  void AddItem(arolla::TypedRef item);

  // Adds an error to the chain stream; `status` must be non-OK.
  void AddError(absl::Status status);

 private:
  class Scheduler;

  // We need a shared pointer here since callbacks will hold a reference
  // to the Scheduler, and nothing else will own it once it is finalized.
  // Scheduler instances are expected to be only owned by callbacks and by the
  // StreamChain. Therefore, as soon as the callbacks are executed and the
  // StreamChain is destroyed (so no new inputs can be added), the Scheduler
  // will be destroyed and the circular ownership of
  // stream->callback->scheduler->stream will be broken.
  std::shared_ptr<Scheduler> scheduler_;
};

// Writer for a proxy stream (created via `MakeProxyStream()`), used to attach
// it to at most one input stream. If destroyed before an input stream is
// attached, the proxy stream is closed with `OrphanedError()`. This class is
// thread-safe.
class ProxyStreamWriter final {
 public:
  class Impl;

  // Internal constructor; use MakeProxyStream() instead.
  explicit ProxyStreamWriter(Impl& impl);

  // Movable, but non-copyable.
  ProxyStreamWriter(ProxyStreamWriter&&) noexcept = default;
  ProxyStreamWriter& operator=(ProxyStreamWriter&& other) noexcept;

  ~ProxyStreamWriter();

  // Returns the value type of the stream.
  arolla::QTypePtr absl_nonnull value_qtype() const { return value_qtype_; }

  // Returns true if further manipulations on the resulting stream can no longer
  // reach any reader.
  bool Orphaned() const;

  // Tries to attach the proxy stream to the given input stream. This method has
  // no effect if the stream has already been attached or orphaned.
  //
  // Important: Attaching a stream with a different `value_qtype` results in
  // undefined behaviour!
  void TryAttach(StreamPtr absl_nonnull stream);

 private:
  arolla::QTypePtr absl_nonnull value_qtype_;
  std::weak_ptr<Impl> impl_;
};

// Creates a proxy stream that can later be attached to an input stream using
// the returned `ProxyStreamWriter`.
std::pair<StreamPtr absl_nonnull, ProxyStreamWriter> MakeProxyStream(
    arolla::QTypePtr absl_nonnull value_qtype);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_STREAM_COMPOSITION_H_
