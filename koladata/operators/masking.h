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
#ifndef KOLADATA_OPERATORS_MASKING_H_
#define KOLADATA_OPERATORS_MASKING_H_

#include <utility>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/qexpr/eval_context.h"
#include "koladata/adoption_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_op.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/has.h"
#include "koladata/internal/op_utils/presence_and.h"
#include "koladata/internal/op_utils/presence_or.h"
#include "koladata/operators/arolla_bridge.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

// kd.masking.apply_mask.
inline absl::StatusOr<DataSlice> ApplyMask(const DataSlice& obj,
                                           const DataSlice& mask) {
  RETURN_IF_ERROR(ExpectMask("mask", mask));
  return DataSliceOp<internal::PresenceAndOp>()(obj, mask, obj.GetSchemaImpl(),
                                                obj.GetBag());
}

// kd.masking.coalesce.
inline absl::StatusOr<DataSlice> Coalesce(const DataSlice& x,
                                          const DataSlice& y) {
  RETURN_IF_ERROR(ExpectHaveCommonSchema({"x", "y"}, x, y));
  ASSIGN_OR_RETURN(DataBagPtr res_db, WithAdoptedValues(y.GetBag(), x));
  ASSIGN_OR_RETURN(auto aligned_slices, AlignSchemas({x, y}));
  return DataSliceOp<internal::PresenceOrOp</*disjoint=*/false>>()(
      std::move(aligned_slices.slices[0]), std::move(aligned_slices.slices[1]),
      std::move(aligned_slices.common_schema), std::move(res_db));
}


// kd.masking.disjoint_coalesce.
absl::StatusOr<DataSlice> DisjointCoalesce(const DataSlice& x,
                                           const DataSlice& y);

// kd.masking.has.
inline absl::StatusOr<DataSlice> Has(const DataSlice& obj) {
  return DataSliceOp<internal::HasOp>()(
      obj, obj.GetShape(), internal::DataItem(schema::kMask), nullptr);
}

// kd.masking._has_not.
absl::StatusOr<DataSlice> HasNot(arolla::EvaluationContext* ctx,
                                 const DataSlice& x);

// kd.masking._agg_any.
inline absl::StatusOr<DataSlice> AggAny(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectMask("x", x));
  ASSIGN_OR_RETURN(auto typed_x,
                   CastToNarrow(x, internal::DataItem(schema::kMask)));
  return SimpleAggIntoEval("core.any", {std::move(typed_x)});
}

// kd.masking._agg_all.
inline absl::StatusOr<DataSlice> AggAll(const DataSlice& x) {
  RETURN_IF_ERROR(ExpectMask("x", x));
  ASSIGN_OR_RETURN(auto typed_x,
                   CastToNarrow(x, internal::DataItem(schema::kMask)));
  return SimpleAggIntoEval("core.all", {std::move(typed_x)});
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_MASKING_H_
