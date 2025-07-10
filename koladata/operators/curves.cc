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
#include "koladata/operators/curves.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "koladata/casting.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "arolla/pwlcurve/curves.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

using ::arolla::pwlcurve::CurveType;
using ::koladata::internal::DataItem;

absl::Status VerifyAdjustmentShape(const DataSlice::JaggedShape& shape) {
  if (shape.rank() != 2) {
    return absl::InvalidArgumentError(absl::StrCat(
        "curve adjustments must have rank=2 found: ", shape.rank()));
  }
  const auto& edge = shape.edges().back();
  for (int64_t i = 0; i < edge.parent_size(); ++i) {
    if (int64_t sz = edge.split_size(i); sz != 2) {
      return absl::InvalidArgumentError(absl::StrCat(
          "curve adjustment shape must be regular size=2, found: ", sz));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<arolla::pwlcurve::Curve>> CreateCurve(
    CurveType type, const DataSlice& adjustments) {
  if (adjustments.IsEmpty()) {
    return absl::InvalidArgumentError("curve adjustments must be non empty");
  }
  RETURN_IF_ERROR(VerifyAdjustmentShape(adjustments.GetShape()));
  ASSIGN_OR_RETURN(auto adjustments_double,
                   CastToNarrow(adjustments, DataItem(schema::kFloat64)));
  DCHECK(!adjustments_double.IsEmpty())
      << "casting non empty shouldn't produce missed values";
  const auto& adjustments_da = adjustments_double.slice().values<double>();
  if (!adjustments_da.IsAllPresent()) {
    return absl::InvalidArgumentError("curve adjustments must be all present");
  }
  size_t size = adjustments.size() / 2;
  std::vector<arolla::pwlcurve::Point<double>> ctrl_points(size);
  for (size_t i = 0; i < size; ++i) {
    ctrl_points[i].x = adjustments_da.values[i * 2];
    ctrl_points[i].y = adjustments_da.values[i * 2 + 1];
  }
  return ::arolla::pwlcurve::NewCurve(type, ctrl_points);
}

absl::StatusOr<DataSlice> EvalCurve(CurveType type, const DataSlice& x,
                                    const DataSlice& adjustments) {
  ASSIGN_OR_RETURN(auto curve, CreateCurve(type, adjustments));
  const auto res_schema = internal::DataItem(schema::kFloat64);
  if (x.IsEmpty()) {
    return x.WithSchema(res_schema)->WithBag(nullptr);
  }
  ASSIGN_OR_RETURN(auto x_double, CastToNarrow(x, DataItem(schema::kFloat64)));
  DCHECK(!x_double.IsEmpty())
      << "casting non empty shouldn't produce missed values";
  if (x_double.is_item()) {
    return DataSlice::Create(
        DataItem(curve->Eval(x_double.item().value<double>())), x.GetShape(),
        res_schema, nullptr);
  } else {
    arolla::DenseArray<double> res = arolla::CreateDenseOp([&curve](double p) {
      return curve->Eval(p);
    })(x_double.slice().values<double>());
    return DataSlice::Create(internal::DataSliceImpl::Create(std::move(res)),
                             x.GetShape(), res_schema, nullptr);
  }
}

}  // namespace

// kde.curves.log_pwl_curve
absl::StatusOr<DataSlice> LogPwlCurve(const DataSlice& x,
                                      const DataSlice& adjustments) {
  return EvalCurve(CurveType::LogPWLCurve, x, adjustments);
}

// kde.curves.pwl_curve
absl::StatusOr<DataSlice> PwlCurve(const DataSlice& x,
                                   const DataSlice& adjustments) {
  return EvalCurve(CurveType::PWLCurve, x, adjustments);
}

// kde.curves.log_p1_pwl_curve
absl::StatusOr<DataSlice> LogP1PwlCurve(const DataSlice& x,
                                        const DataSlice& adjustments) {
  return EvalCurve(CurveType::LogP1PWLCurve, x, adjustments);
}

// kde.curves.symmetric_log_p1_pwl_curve
absl::StatusOr<DataSlice> SymmetricLogP1PwlCurve(const DataSlice& x,
                                                 const DataSlice& adjustments) {
  return EvalCurve(CurveType::SymmetricLogP1PWLCurve, x, adjustments);
}

}  // namespace koladata::ops
