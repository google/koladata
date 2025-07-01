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
#ifndef KOLADATA_FUNCTOR_PARALLEL_STREAM_CALL_H_
#define KOLADATA_FUNCTOR_PARALLEL_STREAM_CALL_H_

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

// Computes the given functor with the provided arguments on the specified
// executor, yielding its results within a new stream.
//
// For stream arguments labeled with AWAIT, `StreamCall` first awaits the
// corresponding input streams. Each of these streams is expected to yield
// exactly one item, which is then passed as the argument to the `functor`.
// If a labeled stream is empty or yields more than one item, it is considered
// an error.
//
// Importantly, if the `functor` itself returns a `Stream[return_value_qtype]`,
// the `StreamCall` operation will flatten this inner stream. The resulting
// output stream will directly contain the items produced by the functor.
absl::StatusOr<StreamPtr absl_nonnull> StreamCall(
    ExecutorPtr absl_nonnull executor,
    absl::AnyInvocable<
        absl::StatusOr<arolla::TypedValue>(
            absl::Span<const arolla::TypedRef> args) &&> /*nonnull*/
        functor,
    arolla::QTypePtr absl_nonnull return_value_qtype,
    absl::Span<const arolla::TypedRef> args);

// Adds an AWAIT label to the stream type indicating that StreamCall should wait
// for the item in the stream and use that item as the argument for a functor.
// If the given stream has no items or more than one item, it is considered
// an error.
//
// Note: If `arg` is not actually a stream, this label has no effect, and this
// function doesn't add it.
arolla::TypedRef MakeStreamCallAwaitArg(arolla::TypedRef arg);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_STREAM_CALL_H_
