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
#ifndef KOLADATA_TESTING_TRAVERSING_UTILS_H_
#define KOLADATA_TESTING_TRAVERSING_UTILS_H_

#include <cstdint>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/op_utils/deep_equivalent.h"

namespace koladata::testing {

using DeepEquivalentParams =
    ::koladata::internal::DeepEquivalentOp::DeepEquivalentParams;

// Returns a vector of mismatches between two data slices.
//
// Each mismatch is represented as a string, that contains the path to the
// mismatch and the values of the mismatched items.
// Example:
//  lhs = kd.new(x=kd.slice([1, 2, 3]))
//  rhs = kd.new(x=kd.slice([1, 2, 4]))
// There would be a single mismatch:
//  ".S[2].x: DataItem(3, ...) vs DataItem(4, ...)"
absl::StatusOr<std::vector<std::string>> DeepEquivalentMismatches(
    const DataSlice& lhs, const DataSlice& rhs, int64_t max_count,
    DeepEquivalentParams params);

};  // namespace koladata::testing

#endif  // KOLADATA_TESTING_TRAVERSING_UTILS_H_
