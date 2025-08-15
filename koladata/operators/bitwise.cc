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
#include "koladata/operators/bitwise.h"

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/arolla_bridge.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

absl::StatusOr<DataSlice> BitwiseAnd(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectInteger("x", x));
  RETURN_IF_ERROR(ExpectInteger("y", y));
  return SimplePointwiseEval("bitwise.bitwise_and", {x, y});
}

absl::StatusOr<DataSlice> BitwiseOr(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectInteger("x", x));
  RETURN_IF_ERROR(ExpectInteger("y", y));
  return SimplePointwiseEval("bitwise.bitwise_or", {x, y});
}

absl::StatusOr<DataSlice> BitwiseXor(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectInteger("x", x));
  RETURN_IF_ERROR(ExpectInteger("y", y));
  return SimplePointwiseEval("bitwise.bitwise_xor", {x, y});
}

absl::StatusOr<DataSlice> BitwiseInvert(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectInteger("x", x));
  return SimplePointwiseEval("bitwise.invert", {x});
}

absl::StatusOr<DataSlice> BitwiseCount(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectInteger("x", x));
  return SimplePointwiseEval(
      "bitwise.count", {x},
      /*output_schema=*/internal::DataItem(koladata::schema::kInt32));
}

}  // namespace koladata::ops
