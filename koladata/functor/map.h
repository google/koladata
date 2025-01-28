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
#ifndef KOLADATA_FUNCTOR_MAP_H_
#define KOLADATA_FUNCTOR_MAP_H_

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/expr/expr_eval.h"

namespace koladata::functor {

// Calls the given functors pointwise on items from the provided arguments and
// keyword arguments, expects them to return DataItems, and stacks
// those into a single DataSlice.
// `args` must contain first all positional arguments, then all keyword
// arguments, so `kwnames` corresponds to a suffix of `args`.
// `include_missing` controls whether to call the functors on missing items of
// `args` and `kwargs`.
//
// The `eval_options` parameter provides a default buffer factory
// (typically either the default allocator or an arena allocator)
// and a cancellation checker, which allows the computation to be
// interrupted midway if needed.
absl::StatusOr<DataSlice> MapFunctorWithCompilationCache(
    const DataSlice& functors, std::vector<DataSlice> args,
    absl::Span<const std::string> kwnames, bool include_missing,
    const expr::EvalOptions& eval_options);

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_MAP_H_
