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
#ifndef KOLADATA_FUNCTOR_WHILE_H_
#define KOLADATA_FUNCTOR_WHILE_H_

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "arolla/qtype/typed_value.h"

namespace koladata::functor {

// While `condition_fn` is present, runs `body_fn`, then returns the final value
// of all state variables.
//
// Equivalent to the python code:
//
//   vars = zip(var_names, var_values)
//   while condition_fn(**vars):
//     vars.update(body_fn(**vars).to_dict())
//   return vars.values()
//
// If `num_yields_vars` is greater than zero, then the first `num_yields_vars`
// vars behave differently:
// - They are not passed as arguments to `condition_fn` or `body_fn`.
// - Their initial value and their value from each update (when present) are
//   accumulated into an arolla::Sequence, which is used as their final value.
//
absl::StatusOr<std::vector<arolla::TypedValue>> WhileWithCompilationCache(
    const DataSlice& condition_fn, const DataSlice& body_fn,
    absl::Span<const std::string> var_names,
    std::vector<arolla::TypedValue> var_values, int num_yields_vars = 0);

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_WHILE_H_
