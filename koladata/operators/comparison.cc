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
#include "koladata/operators/comparison.h"

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_op.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/equal.h"
#include "koladata/operators/arolla_bridge.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

absl::StatusOr<DataSlice> Less(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectHaveCommonPrimitiveSchema({"x", "y"}, x, y));
  RETURN_IF_ERROR(ExpectCanBeOrdered("x", x));
  RETURN_IF_ERROR(ExpectCanBeOrdered("y", y));
  return SimplePointwiseEval("core.less", {x, y},
                             internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> Greater(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectHaveCommonPrimitiveSchema({"x", "y"}, x, y));
  RETURN_IF_ERROR(ExpectCanBeOrdered("x", x));
  RETURN_IF_ERROR(ExpectCanBeOrdered("y", y));
  return SimplePointwiseEval("core.greater", {x, y},
                             internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> LessEqual(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectHaveCommonPrimitiveSchema({"x", "y"}, x, y));
  RETURN_IF_ERROR(ExpectCanBeOrdered("x", x));
  RETURN_IF_ERROR(ExpectCanBeOrdered("y", y));
  return SimplePointwiseEval("core.less_equal", {x, y},
                             internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> GreaterEqual(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectHaveCommonPrimitiveSchema({"x", "y"}, x, y));
  RETURN_IF_ERROR(ExpectCanBeOrdered("x", x));
  RETURN_IF_ERROR(ExpectCanBeOrdered("y", y));
  return SimplePointwiseEval("core.greater_equal", {x, y},
                             internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> Equal(const DataSlice& x, const DataSlice& y) {
  // NOTE: Casting is handled internally by EqualOp. The schema compatibility is
  // still verified to ensure that e.g. ITEMID and OBJECT are not compared.
  RETURN_IF_ERROR(ExpectHaveCommonSchema({"x", "y"}, x, y));
  return DataSliceOp<internal::EqualOp>()(
      x, y, internal::DataItem(schema::kMask), nullptr);
}

}  // namespace koladata::ops
