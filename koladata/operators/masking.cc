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
#include "koladata/operators/masking.h"

#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/logic_ops.h"
#include "arolla/util/status.h"
#include "arolla/util/unit.h"
#include "koladata/adoption_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_op.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/presence_or.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

absl::Status IntersectionErrorHandler(absl::Status status, const DataSlice& x,
                                      const DataSlice& y) {
  const auto* intersection_error =
      arolla::GetPayload<internal::PresenceOrIntersectionError>(status);
  if (intersection_error == nullptr) {
    return status;
  }
  ASSIGN_OR_RETURN(
      auto x_overlap,
      DataSlice::Create(intersection_error->lhs_overlap,
                        DataSlice::JaggedShape::FlatFromSize(
                            intersection_error->lhs_overlap.size()),
                        x.GetSchemaImpl(), x.GetBag()));
  ASSIGN_OR_RETURN(
      auto y_overlap,
      DataSlice::Create(intersection_error->rhs_overlap,
                        DataSlice::JaggedShape::FlatFromSize(
                            intersection_error->rhs_overlap.size()),
                        y.GetSchemaImpl(), y.GetBag()));
  ASSIGN_OR_RETURN(
      auto indices_overlap,
      DataSlice::Create(intersection_error->indices_overlap,
                        DataSlice::JaggedShape::FlatFromSize(
                            intersection_error->indices_overlap.size()),
                        internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(auto x_str,
                   DataSliceToStr(x_overlap, {.show_attributes = true}));
  ASSIGN_OR_RETURN(auto y_str,
                   DataSliceToStr(y_overlap, {.show_attributes = true}));
  ASSIGN_OR_RETURN(auto indices_str, DataSliceToStr(indices_overlap));
  return absl::InvalidArgumentError(absl::StrFormat(
      "`x` and `y` cannot overlap, but found the following intersecting values "
      "for the flattened and aligned inputs:\n\nintersecting indices: "
      "%s\nintersecting x-values: %s\nintersecting y-values: %s",
      indices_str, x_str, y_str));
}

}  // namespace

absl::StatusOr<DataSlice> DisjointCoalesce(const DataSlice& x,
                                           const DataSlice& y) {
  RETURN_IF_ERROR(ExpectHaveCommonSchema({"x", "y"}, x, y));
  ASSIGN_OR_RETURN(DataBagPtr res_db, WithAdoptedValues(y.GetBag(), x));
  ASSIGN_OR_RETURN(auto aligned_slices, AlignSchemas({x, y}));
  ASSIGN_OR_RETURN(
      auto res,
      DataSliceOp<internal::PresenceOrOp</*disjoint=*/true>>()(
          std::move(aligned_slices.slices[0]),
          std::move(aligned_slices.slices[1]),
          std::move(aligned_slices.common_schema), std::move(res_db)),
      IntersectionErrorHandler(std::move(_), x, y));
  return res;
}

absl::StatusOr<DataSlice> HasNot(arolla::EvaluationContext* ctx,
                                 const DataSlice& x) {
  RETURN_IF_ERROR(ExpectMask("x", x));
  if (x.is_item()) {
    return x.item().has_value()
               ? DataSlice::UnsafeCreate(internal::DataItem(),
                                         internal::DataItem(schema::kMask))
               : DataSlice::CreateFromScalar(arolla::kUnit);
  }
  const auto& slice = x.slice();
  if (slice.is_empty_and_unknown()) {
    auto da = arolla::CreateConstDenseArray<arolla::Unit>(x.GetShape().size(),
                                                          arolla::kUnit);
    return DataSlice::UnsafeCreate(
        internal::DataSliceImpl::Create(std::move(da)), x.GetShape(),
        internal::DataItem(schema::kMask));
  }
  auto da =
      arolla::DenseArrayPresenceNotOp()(ctx, slice.values<arolla::Unit>());
  return DataSlice::UnsafeCreate(internal::DataSliceImpl::Create(std::move(da)),
                                 x.GetShape(),
                                 internal::DataItem(schema::kMask));
}

}  // namespace koladata::ops
