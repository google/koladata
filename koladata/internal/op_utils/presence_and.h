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
#ifndef KOLADATA_INTERNAL_OP_UTILS_PRESENCE_AND_H_
#define KOLADATA_INTERNAL_OP_UTILS_PRESENCE_AND_H_

#include <type_traits>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/logic_ops.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/unit.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

// Elementwise returns the first argument if the second is present, missing
// otherwise.
struct PresenceAndOp {
  absl::StatusOr<DataSliceImpl> operator()(
      const DataSliceImpl& ds, const DataSliceImpl& presence_mask) const {
    arolla::EvaluationContext ctx;
    SliceBuilder bldr(ds.size());
    if (ds.size() != presence_mask.size()) {
      return absl::InvalidArgumentError(
          "ds.size() != presence_mask.size()");
    }
    if (presence_mask.is_empty_and_unknown()) {
      // An empty presence_mask might be represented by empty DataSlice.
      return std::move(bldr).Build();
    }
    if (presence_mask.dtype() != arolla::GetQType<arolla::Unit>()) {
      return absl::InvalidArgumentError(
          "Second argument to operator & (or apply_mask) must have all items "
          "of MASK dtype");
    }
    auto presence_mask_array = presence_mask.values<arolla::Unit>();

    RETURN_IF_ERROR(ds.VisitValues([&](const auto& array) -> absl::Status {
      ASSIGN_OR_RETURN(
          auto masked_array,
          arolla::DenseArrayPresenceAndOp()(&ctx, array, presence_mask_array));
      using T = typename std::decay_t<decltype(array)>::base_type;
      if (!masked_array.IsAllMissing()) {
        bldr.InsertIfNotSet<T>(masked_array.bitmap, {}, masked_array.values);
        if constexpr (std::is_same_v<T, ObjectId>) {
          // TODO: keep only necessary allocation ids.
          bldr.GetMutableAllocationIds() = ds.allocation_ids();
        }
      }
      return absl::OkStatus();
    }));
    return std::move(bldr).Build();
  }

  absl::StatusOr<DataItem> operator()(const DataItem& item,
                                      const DataItem& presence_flag) const {
    if (!presence_flag.has_value()) {
      return DataItem();
    }
    if (!presence_flag.holds_value<arolla::Unit>()) {
      return absl::InvalidArgumentError(
          "Second argument to operator & (or apply_mask) must have all items "
          "of MASK dtype");
    }
    return item;
  }
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_PRESENCE_AND_H_
