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
#ifndef KOLADATA_FUNCTOR_PARALLEL_PARALLEL_CALL_UTILS_H_
#define KOLADATA_FUNCTOR_PARALLEL_PARALLEL_CALL_UTILS_H_

#include <string>

#include "absl/base/nullability.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice.h"
#include "koladata/functor/parallel/transform_config.h"

namespace koladata::functor::parallel {

using CallFn = absl::AnyInvocable<absl::StatusOr<arolla::TypedValue>(
    absl::Span<const arolla::TypedRef> args,
    absl::Span<const std::string> kwnames) const>;

// Returns the default execution context.
absl::StatusOr<ParallelTransformConfigPtr absl_nonnull>
GetDefaultParallelTransformConfig();

// Returns a value converted to a "parallel" type -- future or stream, or
// a tuple/namedtuple thereof.
//
// If the input value is already parallel, it is an error.
absl::StatusOr<arolla::TypedValue> AsParallel(arolla::TypedRef value);

// Applies TransformToParallel to the given Koda functor and constructs
// a C++ functor from it.
//
// The resulting C++ functor will have an additional positional-only
// parameter for the executor. The remaining arguments will correspond to
// arguments of the given Koda functor, however they must be "parallel" values.
// The result functor will return a "parallel" result (i.e., a future or
// stream).
absl::StatusOr<CallFn> TransformToParallelCallFn(
    const ParallelTransformConfigPtr& config, DataSlice functor);

// Applies TransformToParallel to a simple Koda functor and constructs
// a C++ functor from it.
//
// Important: The given functor should take non-parallel arguments and return
// a non-parallel result. Tuples and namedtuples are supported, but returning
// iterables or streams is not.
//
// The resulting C++ functor will expect non-parallel arguments and always
// return a single future for the result. The functor will use the current
// executor at the time of invocation.
//
absl::StatusOr<CallFn> TransformToSimpleParallelCallFn(
    DataSlice functor, bool allow_runtime_transforms = false);

}  // namespace koladata::functor::parallel

#endif  // KOLADATA_FUNCTOR_PARALLEL_PARALLEL_CALL_UTILS_H_
