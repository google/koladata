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
#ifndef KOLADATA_INTERNAL_OP_UTILS_PRESENCE_OR_H_
#define KOLADATA_INTERNAL_OP_UTILS_PRESENCE_OR_H_

#include <type_traits>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/has.h"
#include "koladata/internal/op_utils/presence_and.h"
#include "koladata/internal/slice_builder.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/logic_ops.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

// Elementwise returns the first argument if it is present and the
// second argument otherwise.
struct PresenceOrOp {
  absl::StatusOr<DataSliceImpl> operator()(
      const DataSliceImpl& lhs, const DataSliceImpl& rhs) const {
    if (lhs.size() != rhs.size()) {
      return absl::InvalidArgumentError(
          "coalesce requires input slices to have the same size");
    }
    if (rhs.present_count() == 0) {
      return lhs;
    }
    if (lhs.is_empty_and_unknown()) {
      // NOTE: Here we do not want to replace `lhs` with `rhs` if `lhs` has
      // underlying type as that would overwrite a type.
      return rhs;
    }
    if (!lhs.is_mixed_dtype() && !rhs.is_mixed_dtype() &&
        lhs.dtype() == rhs.dtype()) {
      return _single_type_case(lhs, rhs);
    }
    arolla::DenseArray<arolla::Unit> lhs_missing_mask;
    arolla::EvaluationContext ctx;
    if (lhs.is_mixed_dtype()) {
      ASSIGN_OR_RETURN(auto lhs_mask_slice, HasOp()(lhs));
      const auto& lhs_mask = lhs_mask_slice.values<arolla::Unit>();
      lhs_missing_mask = arolla::DenseArrayPresenceNotOp()(&ctx, lhs_mask);
    } else {
      // lhs is guaranteed to contain exactly one array.
      lhs.VisitValues([&](const auto& lhs_array) {
        lhs_missing_mask = arolla::DenseArrayPresenceNotOp()(&ctx, lhs_array);
      });
    }
    ASSIGN_OR_RETURN(
        auto rhs_filtered,  // == rhs & ~has(lhs)
        PresenceAndOp()(rhs, DataSliceImpl::Create(lhs_missing_mask)));
    if (rhs_filtered.present_count() == 0) {
      return lhs;
    }
    SliceBuilder bldr(lhs.size(), lhs.allocation_ids());
    bldr.GetMutableAllocationIds().Insert(rhs_filtered.allocation_ids());
    // Add variants present in lhs.
    RETURN_IF_ERROR(lhs.VisitValues([&](const auto& lhs_array) -> absl::Status {
      using T = typename std::decay_t<decltype(lhs_array)>::base_type;
      bool present_in_rhs = false;
      RETURN_IF_ERROR(
          rhs_filtered.VisitValues([&](const auto& rhs_array) -> absl::Status {
            if constexpr (std::is_same_v<decltype(lhs_array),
                                         decltype(rhs_array)>) {
              present_in_rhs = true;
              ASSIGN_OR_RETURN(
                  auto merged_array,
                  arolla::DenseArrayPresenceOrOp()(&ctx, lhs_array, rhs_array));
              bldr.InsertIfNotSet<T>(merged_array.bitmap, {},
                                     merged_array.values);
            }
            return absl::OkStatus();
          }));
      if (!present_in_rhs) {
        bldr.InsertIfNotSet<T>(lhs_array.bitmap, {}, lhs_array.values);
      }
      return absl::OkStatus();
    }));
    // Add variants present only in rhs_filtered.
    rhs_filtered.VisitValues([&](const auto& rhs_array) {
      bool present_in_lhs = false;
      lhs.VisitValues([&](const auto& lhs_array) {
        if (std::is_same_v<decltype(lhs_array), decltype(rhs_array)>) {
          present_in_lhs = true;
        }
      });
      if (!present_in_lhs) {
        using T = typename std::decay_t<decltype(rhs_array)>::base_type;
        bldr.InsertIfNotSet<T>(rhs_array.bitmap, {}, rhs_array.values);
      }
    });
    return std::move(bldr).Build();
  }

  absl::StatusOr<DataSliceImpl> operator()(const DataSliceImpl& lhs,
                                           const DataItem& rhs) const {
    if (lhs.size() == lhs.present_count() || !rhs.has_value()) {
      return lhs;
    }
    return (*this)(lhs, DataSliceImpl::Create(lhs.size(), rhs));
  }

  absl::StatusOr<DataItem> operator()(const DataItem& lhs,
                                      const DataItem& rhs) const {
    if (lhs.has_value()) {
      return lhs;
    }
    return rhs;
  }

 private:
  absl::StatusOr<DataSliceImpl> _single_type_case(
      const DataSliceImpl& lhs, const DataSliceImpl& rhs) const {
    SliceBuilder bldr(lhs.size());

    arolla::EvaluationContext ctx;
    RETURN_IF_ERROR(lhs.VisitValues([&](const auto& lhs_array) -> absl::Status {
      using T = typename std::decay_t<decltype(lhs_array)>::base_type;
      if constexpr (std::is_same_v<T, ObjectId>) {
        bldr.GetMutableAllocationIds() = lhs.allocation_ids();
        // TODO: keep only necessary allocation ids.
        bldr.GetMutableAllocationIds().Insert(rhs.allocation_ids());
      }
      const auto& rhs_array = rhs.values<T>();
      ASSIGN_OR_RETURN(auto merged_array, arolla::DenseArrayPresenceOrOp()(
                                              &ctx, lhs_array, rhs_array));
      bldr.InsertIfNotSet<T>(merged_array.bitmap, {}, merged_array.values);
      return absl::OkStatus();
    }));
    return std::move(bldr).Build();
  }
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_PRESENCE_OR_H_
