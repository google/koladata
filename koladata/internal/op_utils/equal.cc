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
#include "koladata/internal/op_utils/equal.h"

#include <cstdint>
#include <type_traits>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/internal/data_slice.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/logic_ops.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

template <>
struct EqualOp::compatible_types<int, int64_t> : std::true_type {};

template <>
struct EqualOp::compatible_types<int64_t, int> : std::true_type {};

template <>
struct EqualOp::compatible_types<float, double> : std::true_type {};

template <>
struct EqualOp::compatible_types<double, float> : std::true_type {};

absl::StatusOr<DataSliceImpl> EqualOp::operator()(
    const DataSliceImpl& lhs, const DataSliceImpl& rhs) const {
  if (lhs.size() != rhs.size()) {
    return absl::InvalidArgumentError(
        "equal requires input slices to have the same size");
  }
  auto result = arolla::CreateEmptyDenseArray<arolla::Unit>(lhs.size());
  if (rhs.is_empty_and_unknown() || lhs.is_empty_and_unknown()) {
    return DataSliceImpl::Create(result);
  }
  arolla::EvaluationContext ctx;
  RETURN_IF_ERROR(lhs.VisitValues(
      [&]<typename LhsArrayT>(const LhsArrayT& l_array) -> absl::Status {
        using LhsValT = typename LhsArrayT::base_type;
        using LhsViewValT = arolla::view_type_t<LhsValT>;
        return rhs.VisitValues([&]<typename RhsArrayT>(
                                   const RhsArrayT& r_array) -> absl::Status {
          using RhsValT = typename RhsArrayT::base_type;
          using RhsViewValT = arolla::view_type_t<RhsValT>;
          if constexpr (compatible_types<LhsValT, RhsValT>::value) {
            auto op =
                arolla::CreateDenseOp<arolla::DenseOpFlags::kNoSizeValidation |
                                      arolla::DenseOpFlags::kRunOnMissing>(
                    MaskEqualOp<LhsViewValT, RhsViewValT>());
            auto sub_res = op(l_array, r_array);
            if (result.PresentCount() == 0) {
              result = std::move(sub_res);
            } else {
              ASSIGN_OR_RETURN(result, arolla::DenseArrayPresenceOrOp()(
                                           &ctx, result, sub_res));
            }
          }
          return absl::OkStatus();
        });
      }));
  return DataSliceImpl::Create(result);
}

}  // namespace koladata::internal
