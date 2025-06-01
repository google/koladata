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
#ifndef THIRD_PARTY_KOLA_DATA_FUNCTOR_PARALLEL_STREAM_REDUCE_H_
#define THIRD_PARTY_KOLA_DATA_FUNCTOR_PARALLEL_STREAM_REDUCE_H_

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/statusor.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

// Accumulates a value starting with `initial_value` and iteratively applying
// the functor to the current accumulated value and each stream item.
// The final value is returned in a stream with single item.
//
//    functor(functor(initial_value, stream[0]), stream[1]), ...)
//
StreamPtr /*absl_nonnull*/ StreamReduce(
    ExecutorPtr /*absl_nonnull*/ executor, arolla::TypedValue initial_value,
    StreamPtr /*absl_nonnull*/ input_stream,
    absl::AnyInvocable<absl::StatusOr<arolla::TypedValue>(
        arolla::TypedRef, arolla::TypedRef) const>
        functor);

}  // namespace koladata::functor::parallel

#endif  // THIRD_PARTY_KOLA_DATA_FUNCTOR_PARALLEL_STREAM_REDUCE_H_
