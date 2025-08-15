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
#ifndef KOLADATA_OPERATORS_BITWISE_H_
#define KOLADATA_OPERATORS_BITWISE_H_

#include "koladata/operators/bitwise.h"
#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata::ops {

// kd.bitwise.bitwise_and.
absl::StatusOr<DataSlice> BitwiseAnd(const DataSlice& x, const DataSlice& y);

// kd.bitwise.bitwise_or.
absl::StatusOr<DataSlice> BitwiseOr(const DataSlice& x, const DataSlice& y);

// kd.bitwise.bitwise_xor.
absl::StatusOr<DataSlice> BitwiseXor(const DataSlice& x, const DataSlice& y);

// kd.bitwise.invert.
absl::StatusOr<DataSlice> BitwiseInvert(const DataSlice& x);

// kd.bitwise.count.
absl::StatusOr<DataSlice> BitwiseCount(const DataSlice& x);
}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_BITWISE_H_
