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
#ifndef KOLADATA_FUNCTOR_PARALLEL_STREAM_COMPOSITION_H_
#define KOLADATA_FUNCTOR_PARALLEL_STREAM_COMPOSITION_H_

#include <memory>

#include "absl/base/nullability.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

// This class can be used to interleave multiple streams together, including
// cases where not all streams are known in advance, but rather added one by
// one. This class is thread-safe.
class StreamInterleave {
 public:
  explicit StreamInterleave(StreamWriterPtr /*absl_nonnull*/ writer);

  // Movable but non-copyable.
  StreamInterleave(StreamInterleave&&) = default;
  StreamInterleave& operator=(StreamInterleave&&) = default;

  // Adds one more input stream to the interleave.
  void Add(const StreamPtr /*absl_nonnull*/& stream);

  // Adds an error to the interleaved stream; `status` must be non-OK.
  void AddError(absl::Status status) &&;

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
  explicit StreamChain(StreamWriterPtr /*absl_nonnull*/ writer);

  // Movable but non-copyable.
  StreamChain(StreamChain&&) = default;
  StreamChain& operator=(StreamChain&&) = default;

  // Adds one more input stream to the chain.
  void Add(const StreamPtr /*absl_nonnull*/& stream);

  // Adds an error to the chain stream; `status` must be non-OK.
  void AddError(absl::Status status) &&;

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

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_STREAM_COMPOSITION_H_
