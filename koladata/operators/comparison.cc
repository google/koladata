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
#include "koladata/operators/comparison.h"

#include <type_traits>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operators/core/logic_operators.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_op.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/equal.h"
#include "koladata/operators/binary_op.h"
#include "koladata/operators/utils.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

// Similar to RETURN_IF_ERROR, but doesn't add an extra source location
// to the status.
#define KD_RETURN_AS_IS_IF_ERROR(X)      \
  if (absl::Status st = (X); !st.ok()) { \
    return st;                           \
  }

struct CanBeOrdered {
  template <class T1, class T2>
  constexpr static bool kIsInvocable =
      (std::is_arithmetic_v<T1> && std::is_arithmetic_v<T2> &&
      !std::is_same_v<T1, bool> && !std::is_same_v<T2, bool>) ||
      (std::is_same_v<T1, bool> && std::is_same_v<T2, bool>) ||
      (std::is_same_v<T1, arolla::Unit> && std::is_same_v<T2, arolla::Unit>) ||
      (std::is_same_v<T1, arolla::Text> && std::is_same_v<T2, arolla::Text>) ||
      (std::is_same_v<T1, arolla::Bytes> && std::is_same_v<T2, arolla::Bytes>);

  explicit constexpr CanBeOrdered(absl::string_view name1,
                                  absl::string_view name2)
      : name1(name1), name2(name2) {}

  absl::Status CheckArgs(const DataSlice& ds1, const DataSlice& ds2) const {
    KD_RETURN_AS_IS_IF_ERROR(ExpectCanBeOrdered(name1, ds1));
    KD_RETURN_AS_IS_IF_ERROR(ExpectCanBeOrdered(name2, ds2));
    return ExpectHaveCommonPrimitiveSchema({name1, name2}, ds1, ds2);
  }

  const absl::string_view name1, name2;
};

#undef KD_RETURN_AS_IS_IF_ERROR

}  // namespace

absl::StatusOr<DataSlice> Less(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::MaskLessOp>(x, y, CanBeOrdered("x", "y"),
                                          BinaryOpReturns(schema::kMask));
}

absl::StatusOr<DataSlice> Greater(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::MaskLessOp>(y, x, CanBeOrdered("y", "x"),
                                          BinaryOpReturns(schema::kMask));
}

absl::StatusOr<DataSlice> LessEqual(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::MaskLessEqualOp>(x, y, CanBeOrdered("x", "y"),
                                               BinaryOpReturns(schema::kMask));
}

absl::StatusOr<DataSlice> GreaterEqual(const DataSlice& x, const DataSlice& y) {
  return BinaryOpEval<arolla::MaskLessEqualOp>(y, x, CanBeOrdered("y", "x"),
                                               BinaryOpReturns(schema::kMask));
}

absl::StatusOr<DataSlice> Equal(const DataSlice& x, const DataSlice& y) {
  // NOTE: Casting is handled internally by EqualOp. The schema compatibility is
  // still verified to ensure that e.g. ITEMID and OBJECT are not compared.
  RETURN_IF_ERROR(ExpectHaveCommonSchema({"x", "y"}, x, y));
  return DataSliceOp<internal::EqualOp>()(
      x, y, internal::DataItem(schema::kMask), nullptr);
}

}  // namespace koladata::ops
