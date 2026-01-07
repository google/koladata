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
#ifndef KOLADATA_FUNCTOR_PARALLEL_STREAM_REDUCE_STACK_OR_CONCAT_H_
#define KOLADATA_FUNCTOR_PARALLEL_STREAM_REDUCE_STACK_OR_CONCAT_H_

#include <cstdint>

#include "absl/base/nullability.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/functor/parallel/executor.h"
#include "koladata/functor/parallel/stream.h"

namespace koladata::functor::parallel {

// A specialized version of StreamReduce designed to speed up the concatenation
// of data slices from a stream.
//
// Using a standard StreamReduce with a binary concatenation would result in
// an O(N**2) computational complexity. This implementation, however, achieves
// an O(N) complexity.
//
// See the docstring for `kd.concat` for more details about the concatenation
// semantics.
absl::StatusOr<StreamPtr absl_nonnull> StreamReduceConcat(
    ExecutorPtr absl_nonnull executor, int64_t ndim, DataSlice initial_value,
    StreamPtr absl_nonnull input_stream);

// A specialized version of StreamReduce designed to speed up the stacking
// of data slices from a stream.
//
// Using a standard StreamReduce with a binary stacking function would result in
// an O(N**2) computational complexity. This implementation, however, achieves
// an O(N) complexity.
//
// See the docstring for `kd.stack` for more details about the stacking
// semantics.
absl::StatusOr<StreamPtr absl_nonnull> StreamReduceStack(
    ExecutorPtr absl_nonnull executor, int64_t ndim, DataSlice initial_value,
    StreamPtr absl_nonnull input_stream);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_STREAM_REDUCE_STACK_OR_CONCAT_H_
