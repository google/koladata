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
#ifndef KOLADATA_FUNCTOR_PARALLEL_TRANSFORM_H_
#define KOLADATA_FUNCTOR_PARALLEL_TRANSFORM_H_

#include "absl/status/statusor.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice.h"
#include "koladata/functor/parallel/execution_context.h"

namespace koladata::functor::parallel {

// The parallel execution is done by translating the functors/expressions into
// a form where all operations take and consume futures and streams.
//
// Advanced users can create the future/stream version of the functors directly
// for more customization, in which case they do not need to use this method
// or kd.parallel.parallel_call, and can use the normal kd.call directly.
//
// This method applies this transformation to the given functor. Note that
// the sub-functors are not transformed, instead they are expected to be
// transformed at call time via the use of kd.parallel.parallel_call.
// This is done to support functors that are created dynamically during
// evaluation.
absl::StatusOr<DataSlice> TransformToParallel(
    const ExecutionContextPtr& context, DataSlice functor);

// A wrapper around TransformToParallel that can additionally process:
// 1) a DataSlice of several functors, in which case we will translate
//    all of them to parallel, taking care to translate each unique functor
//    once.
// 2) arolla.unspecified(), in which case we will return the input as-is.
//
// This is the operation that is applied to arguments marked as
// functor_argument_indices in the replacement config.
absl::StatusOr<arolla::TypedValue> TransformManyToParallel(
    const ExecutionContextPtr& context, arolla::TypedValue functors);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_TRANSFORM_H_
