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
#ifndef KOLADATA_SUBSLICE_UTILS_H_
#define KOLADATA_SUBSLICE_UTILS_H_

#include <optional>
#include <variant>
#include <vector>

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata::subslice {

// Represents a range slice for the Subslice method.
// Both start and stop can be nullptr, which means the corresponding end of the
// range is unbounded. When start/stop are not nullptr but one of their
// items is missing, the corresponding range is considered empty.
struct Slice {
  std::optional<DataSlice> start;
  std::optional<DataSlice> stop;
};

// Represents the handling of a single dimension in the Subslice method.
using SlicingArgType = std::variant<Slice, DataSlice>;

// The core logic for the kd.slices.subslice operator. See its docstring
// for more details.
// Computes an equivalent of kd.implode(x, ndim=...)[slice_arg1][slice_arg2]...
// Negative indices are supported (counting from the end in that case).
absl::StatusOr<DataSlice> Subslice(const DataSlice& x,
                                   std::vector<SlicingArgType> slice_args);

}  // namespace koladata::subslice

#endif  // KOLADATA_SUBSLICE_UTILS_H_
