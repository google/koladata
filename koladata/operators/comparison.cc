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

#include <cstddef>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_op.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/equal.h"
#include "koladata/internal/op_utils/utils.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/operators/arolla_bridge.h"
#include "koladata/repr_utils.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {
namespace {

constexpr auto OpError = ::koladata::internal::ToOperatorEvalError;

bool HasOrderedType(const DataSlice& slice) {
  auto narrowed_schema = GetNarrowedSchema(slice);
  return schema::IsImplicitlyCastableTo(narrowed_schema,
                                        internal::DataItem(schema::kFloat64)) ||
         schema::IsImplicitlyCastableTo(narrowed_schema,
                                        internal::DataItem(schema::kBytes)) ||
         schema::IsImplicitlyCastableTo(narrowed_schema,
                                        internal::DataItem(schema::kString)) ||
         schema::IsImplicitlyCastableTo(narrowed_schema,
                                        internal::DataItem(schema::kBool));
}

// Returns OK if the DataSlices contain values that can be ordered.
absl::Status ExpectCanBeOrdered(absl::Span<const absl::string_view> arg_names,
                                const DataSlice& lhs, const DataSlice& rhs) {
  RETURN_IF_ERROR(ExpectHaveCommonPrimitiveSchema(arg_names, lhs, rhs));
  DCHECK_EQ(arg_names.size(), 2);
  for (size_t i = 0; i < 2; ++i) {
    const DataSlice& arg = i == 0 ? lhs : rhs;
    if (!HasOrderedType(arg)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "argument `%s` must be a slice of numerics, booleans, "
          "bytes or strings, got %s",
          arg_names[i], schema_utils_internal::DescribeSliceSchema(arg)));
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<DataSlice> Less(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectCanBeOrdered({"x", "y"}, x, y))
      .With(OpError("kd.comparison.less"));
  return SimplePointwiseEval("core.less", {x, y},
                             internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> Greater(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectCanBeOrdered({"x", "y"}, x, y))
      .With(OpError("kd.comparison.greater"));
  return SimplePointwiseEval("core.greater", {x, y},
                             internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> LessEqual(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectCanBeOrdered({"x", "y"}, x, y))
      .With(OpError("kd.comparison.less_equal"));
  return SimplePointwiseEval("core.less_equal", {x, y},
                             internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> GreaterEqual(const DataSlice& x, const DataSlice& y) {
  RETURN_IF_ERROR(ExpectCanBeOrdered({"x", "y"}, x, y))
      .With(OpError("kd.comparison.greater_equal"));
  return SimplePointwiseEval("core.greater_equal", {x, y},
                             internal::DataItem(schema::kMask));
}

absl::StatusOr<DataSlice> Equal(const DataSlice& x, const DataSlice& y) {
  // NOTE: Casting is handled internally by EqualOp. The schema compatibility is
  // still verified to ensure that e.g. ITEMID and OBJECT are not compared.
  RETURN_IF_ERROR(ExpectHaveCommonSchema({"x", "y"}, x, y))
      .With(OpError("kd.comparison.equal"));
  return DataSliceOp<internal::EqualOp>()(
      x, y, internal::DataItem(schema::kMask), nullptr);
}

}  // namespace koladata::ops
