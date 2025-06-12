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
#ifndef KOLADATA_FUNCTOR_PARALLEL_STREAM_MAP_H_
#define KOLADATA_FUNCTOR_PARALLEL_STREAM_MAP_H_

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/statusor.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

// Applies the given functor to each item of the stream, and puts the resulting
// values to a new stream. All computations happen on the provided executor.
StreamPtr absl_nonnull StreamMap(
    ExecutorPtr absl_nonnull executor,
    const StreamPtr absl_nonnull& input_stream,
    arolla::QTypePtr absl_nonnull return_value_type,
    absl::AnyInvocable<  // clang-format hint
        absl::StatusOr<arolla::TypedValue>(arolla::TypedRef) const>
        functor);

// Applies the given functor to each item of the stream, and puts the resulting
// values to a new stream. All computations happen on the provided executor.
//
// Importantly, the order of the items in the resulting stream is not
// guaranteed.
StreamPtr absl_nonnull StreamMapUnordered(
    ExecutorPtr absl_nonnull executor,
    const StreamPtr absl_nonnull& input_stream,
    arolla::QTypePtr absl_nonnull return_value_type,
    absl::AnyInvocable<  // clang-format hint
        absl::StatusOr<arolla::TypedValue>(arolla::TypedRef) const>
        functor);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_STREAM_MAP_H_
