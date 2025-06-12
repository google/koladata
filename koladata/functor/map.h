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
#ifndef KOLADATA_FUNCTOR_MAP_H_
#define KOLADATA_FUNCTOR_MAP_H_

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"

namespace koladata::functor {

// Calls the given functors pointwise on items from the provided arguments and
// keyword arguments, expects them to return DataItems, and stacks
// those into a single DataSlice.
//
// `args` must contain values for positional arguments followed by values for
// keyword arguments; `kwnames` must contain the names of the keyword arguments,
// so `kwnames` corresponds to a suffix of `args`.
//
// `include_missing` controls whether to call the functors on missing items of
// `args` and `kwargs`.
absl::StatusOr<DataSlice> MapFunctorWithCompilationCache(
    const DataSlice& functors, std::vector<DataSlice> args,
    absl::Span<const std::string> kwnames, bool include_missing);

}  // namespace koladata::functor

#endif  // KOLADATA_FUNCTOR_MAP_H_
