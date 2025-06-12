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
#ifndef KOLADATA_OPERATORS_COMPARISON_H_
#define KOLADATA_OPERATORS_COMPARISON_H_

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata::ops {

// kd.comparison.less.
absl::StatusOr<DataSlice> Less(const DataSlice& x, const DataSlice& y);

// kd.comparison.greater.
absl::StatusOr<DataSlice> Greater(const DataSlice& x, const DataSlice& y);

// kd.comparison.less_equal.
absl::StatusOr<DataSlice> LessEqual(const DataSlice& x, const DataSlice& y);

// kd.comparison.greater_equal.
absl::StatusOr<DataSlice> GreaterEqual(const DataSlice& x, const DataSlice& y);

// kd.comparison.equal.
absl::StatusOr<DataSlice> Equal(const DataSlice& x, const DataSlice& y);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_COMPARISON_H_
