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
#ifndef KOLADATA_INTERNAL_OP_UTILS_EXPAND_H_
#define KOLADATA_INTERNAL_OP_UTILS_EXPAND_H_

#include <type_traits>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/edge_ops.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/types.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

// Expands DataSliceImpl / DataItem over an Edge to a DataSliceImpl.
struct ExpandOp {
  absl::StatusOr<DataSliceImpl> operator()(const DataSliceImpl& ds,
                                           arolla::DenseArrayEdge& edge) const {
    DCHECK_EQ(ds.size(), edge.parent_size());  // Ensured by high-level caller.
    arolla::EvaluationContext ctx;
    SliceBuilder bldr(edge.child_size());
    bldr.GetMutableAllocationIds().Insert(ds.allocation_ids());
    RETURN_IF_ERROR(ds.VisitValues([&](const auto& array) -> absl::Status {
      using T = std::decay_t<decltype(array)>::base_type;
      ASSIGN_OR_RETURN(auto expanded_array,
                       arolla::DenseArrayExpandOp()(&ctx, array, edge));
      bldr.InsertIfNotSet<T>(expanded_array.bitmap, arolla::bitmap::Bitmap(),
                             expanded_array.values);
      return absl::OkStatus();
    }));
    return std::move(bldr).Build();
  }

  absl::StatusOr<DataSliceImpl> operator()(const DataItem& item,
                                           arolla::DenseArrayEdge& edge) const {
    DCHECK_EQ(edge.parent_size(), 1);  // Ensured by high-level caller.
    DataSliceImpl res;
    item.VisitValue([&](const auto& val) {
      using T = std::decay_t<decltype(val)>;
      if constexpr (std::is_same_v<T, ObjectId>) {
        auto array = arolla::CreateConstDenseArray<T>(edge.child_size(), val);
        res = DataSliceImpl::CreateWithAllocIds(
            AllocationIdSet(AllocationId(val)), std::move(array));
      } else if constexpr (!std::is_same_v<T, MissingValue>) {
        auto array = arolla::CreateConstDenseArray<T>(edge.child_size(), val);
        res = DataSliceImpl::Create(std::move(array));
      } else {
        res = DataSliceImpl::CreateEmptyAndUnknownType(edge.child_size());
      }
    });
    return res;
  }
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_EXPAND_H_
