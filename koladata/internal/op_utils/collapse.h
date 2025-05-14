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
#ifndef KOLADATA_INTERNAL_OP_UTILS_COLLAPSE_H_
#define KOLADATA_INTERNAL_OP_UTILS_COLLAPSE_H_

#include <strings.h>

#include <cstdint>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_group_ops.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/operators/aggregation/group_op_accumulators.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

// Collapses DataSliceImpl over an Edge to a DataSliceImpl / DataItem.
// If the DataSliceImpl has a single dtype, the result always has a single dtype
// even if all items are empty. If it has mixed dtypes, the result only has
// variants which have at least one present item.
struct CollapseOp {
  absl::StatusOr<DataSliceImpl> operator()(
      const DataSliceImpl& ds, const arolla::DenseArrayEdge& edge) const {
    DCHECK_EQ(ds.size(), edge.child_size());  // Ensured by high-level caller.
    int64_t res_size = edge.parent_size();

    if (ds.is_empty_and_unknown()) {
      return DataSliceImpl::CreateEmptyAndUnknownType(res_size);
    }

    DataSliceImpl res;
    auto process_single_array =
        [&]<typename T>(const arolla::DenseArray<T>& array) -> absl::Status {
      arolla::DenseGroupOps<arolla::CollapseAccumulator<T>> agg(
          arolla::GetHeapBufferFactory());
      ASSIGN_OR_RETURN(auto collapsed_array, agg.Apply(edge, array));
      // TODO: decide if we should copy AllocationIds or create
      // new ones here.
      res = DataSliceImpl::Create(std::move(collapsed_array));
      return absl::OkStatus();
    };

    if (ds.is_single_dtype()) {
      RETURN_IF_ERROR(ds.VisitValues(process_single_array));
    } else {
      RETURN_IF_ERROR(process_single_array(ds.AsDataItemDenseArray()));
    }
    return res;
  }

  // Return an error status as Collapse is not supported on DataItem.
  absl::StatusOr<DataItem> operator()(const DataItem& item,
                                      arolla::DenseArrayEdge& edge) const {
    return absl::InvalidArgumentError(
        "kd.collapse is not supported for DataItem.");
  }
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_COLLAPSE_H_
