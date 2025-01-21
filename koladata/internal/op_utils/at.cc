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
#include "koladata/internal/op_utils/at.h"

#include <strings.h>

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/slice_builder.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/multi_edge_util.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/array_ops.h"
#include "arolla/util/meta.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {
namespace {

// Note SelectWithOffsets is almost the same as Arolla TakeOverOverOp but
// supports negative indices and ignores out-of-bound indices.
template <typename T>
absl::StatusOr<arolla::DenseArray<T>> SelectWithOffsets(
    const arolla::DenseArray<T>& values,
    const arolla::DenseArray<int64_t>& offsets,
    const arolla::DenseArrayEdge& ds_to_common,
    const arolla::DenseArrayEdge& indices_to_common) {
  using OptT = arolla::OptionalValue<T>;
  using ValuesPerGroup = std::vector<arolla::view_type_t<OptT>>;
  std::vector<ValuesPerGroup> groups(ds_to_common.parent_size());
  absl::Span<ValuesPerGroup> groups_span(groups.data(), groups.size());

  auto add_values_fn = [](ValuesPerGroup& values_per_group, int64_t,
                          arolla::view_type_t<OptT> v) {
    values_per_group.push_back(v);
  };
  RETURN_IF_ERROR(arolla::DenseArrayMultiEdgeUtil::ApplyChildArgs(
      add_values_fn, groups_span, ds_to_common, arolla::meta::type_list<OptT>{},
      values));

  auto result_fn = [&](const ValuesPerGroup& values_per_group, int64_t child_id,
                       int64_t offset) -> arolla::view_type_t<OptT> {
    if (offset < 0) {
      offset += values_per_group.size();
    }
    if (offset < 0 || offset >= values_per_group.size()) {
      return std::nullopt;
    }
    return values_per_group[offset];
  };
  auto res = arolla::DenseArrayMultiEdgeUtil::template ProduceResult<T>(
      arolla::GetHeapBufferFactory(), result_fn, groups_span, indices_to_common,
      arolla::meta::type_list<int64_t>{}, offsets);
  return res;
}

}  // namespace

absl::StatusOr<DataSliceImpl> AtOp(
    const DataSliceImpl& ds, const arolla::DenseArray<int64_t>& indices,
    const arolla::DenseArrayEdge& ds_to_common,
    const std::optional<arolla::DenseArrayEdge>& indices_to_common) {
  if (ds.is_empty_and_unknown()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(indices.size());
  }

  auto process_dense_array = [&]<class T>(const arolla::DenseArray<T>& array)
      -> absl::StatusOr<arolla::DenseArray<T>> {
    // Use AtOp if it is scalar edge otherwise TakeOverOverOp.
    if (ds_to_common.parent_size() > 1) {
      if (!indices_to_common.has_value()) {
        return absl::InternalError(
            "indices_to_common must be provided when ds_to_common is not a "
            "scalar edge "
            "for kd.at.");
      }
      // Ensured by the caller.
      DCHECK_EQ(indices_to_common->parent_size(), ds_to_common.parent_size());
      return SelectWithOffsets(array, indices, ds_to_common,
                               *indices_to_common);
    } else {
      // NOTE: out-of-bound errors are reported to the EvaluationContext and
      // ignored here.
      arolla::EvaluationContext ctx;
      return arolla::DenseArrayAtOp()(&ctx, array, indices);
    }
  };

  // TODO: keep only necessary allocation ids.
  if (ABSL_PREDICT_TRUE(ds.is_single_dtype())) {
    DataSliceImpl res_impl;
    RETURN_IF_ERROR(ds.VisitValues(
        [&]<class T>(const arolla::DenseArray<T>& array) -> absl::Status {
          ASSIGN_OR_RETURN(auto res, process_dense_array(array));
          res_impl = DataSliceImpl::CreateWithAllocIds(ds.allocation_ids(),
                                                       std::move(res));
          return absl::OkStatus();
        }));
    return std::move(res_impl);
  } else {
    SliceBuilder builder(indices.size(), ds.allocation_ids());
    RETURN_IF_ERROR(ds.VisitValues(
        [&]<class T>(const arolla::DenseArray<T>& array) -> absl::Status {
          ASSIGN_OR_RETURN(auto res, process_dense_array(array));
          builder.InsertIfNotSet<T>(res.bitmap, {}, res.values);
          return absl::OkStatus();
        }));

    return std::move(builder).Build();
  }
}
}  // namespace koladata::internal
