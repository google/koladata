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
#ifndef KOLADATA_OPERATORS_MASKING_H_
#define KOLADATA_OPERATORS_MASKING_H_

#include <memory>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
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
#include "koladata/internal/op_utils/utils.h"
#include "koladata/operators/arolla_bridge.h"
#include "koladata/repr_utils.h"
#include "arolla/util/repr.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

// kde.masking.apply_mask.
inline absl::StatusOr<DataSlice> ApplyMask(const DataSlice& obj,
                                           const DataSlice& mask) {
  return DataSliceOp<internal::PresenceAndOp>()(obj, mask, obj.GetSchemaImpl(),
                                                obj.GetBag());
}

// kde.masking.coalesce.
inline absl::StatusOr<DataSlice> Coalesce(const DataSlice& x,
                                          const DataSlice& y) {
  auto res_db = DataBag::CommonDataBag({x.GetBag(), y.GetBag()});
  ASSIGN_OR_RETURN(auto aligned_slices, AlignSchemas({x, y}),
                   AssembleErrorMessage(_, {.db = res_db}));
  return DataSliceOp<internal::PresenceOrOp>()(
      aligned_slices.slices[0], aligned_slices.slices[1],
      aligned_slices.common_schema, std::move(res_db));
}

// kde.masking.has.
inline absl::StatusOr<DataSlice> Has(const DataSlice& obj) {
  return DataSliceOp<internal::HasOp>()(
      obj, obj.GetShape(), internal::DataItem(schema::kMask), nullptr);
}

// kde.masking._has_not.
inline absl::StatusOr<DataSlice> HasNot(const DataSlice& x) {
  // Must be a mask, which is normally guaranteed by `x` being constructed from
  // kde.masking.has. This ensures that M.core.presence_not is always called.
  DCHECK_EQ(x.GetSchemaImpl(), internal::DataItem(schema::kMask));
  return SimplePointwiseEval("core.presence_not", {x},
                             internal::DataItem(schema::kMask));
}

// kde.masking._agg_any.
inline absl::StatusOr<DataSlice> AggAny(const DataSlice& x) {
  ASSIGN_OR_RETURN(
      auto typed_x, CastToNarrow(x, internal::DataItem(schema::kMask)),
      internal::OperatorEvalError(
          std::move(_), "kd.agg_any",
          absl::StrCat("`x` must only contain MASK values, but got ",
                       arolla::Repr(x))));
  return SimpleAggIntoEval("core.any", {std::move(typed_x)});
}

// kde.masking._agg_all.
inline absl::StatusOr<DataSlice> AggAll(const DataSlice& x) {
  ASSIGN_OR_RETURN(
      auto typed_x, CastToNarrow(x, internal::DataItem(schema::kMask)),
      internal::OperatorEvalError(
          std::move(_), "kd.agg_all",
          absl::StrCat("`x` must only contain MASK values, but got ",
                       arolla::Repr(x))));
  return SimpleAggIntoEval("core.all", {std::move(typed_x)});
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_MASKING_H_
