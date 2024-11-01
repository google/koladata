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
#ifndef KOLADATA_INTERNAL_OP_UTILS_INVERSE_SELECT_H_
#define KOLADATA_INTERNAL_OP_UTILS_INVERSE_SELECT_H_

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/op_utils/utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_group_ops.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/jagged_shape/dense_array/qtype/qtype.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/aggregation/group_op_accumulators.h"
#include "arolla/qexpr/operators/dense_array/array_ops.h"
#include "arolla/qexpr/operators/dense_array/edge_ops.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/repr.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

struct InverseSelectOp {
  static constexpr absl::string_view kInverseSelectOpName = "kd.inverse_select";

  absl::StatusOr<DataSliceImpl> operator()(
      const DataSliceImpl& ds_impl,
      const arolla::JaggedDenseArrayShape& ds_shape,
      const DataSliceImpl& filter,
      const arolla::JaggedDenseArrayShape& filter_shape) const {
    arolla::EvaluationContext ctx;
    std::optional<arolla::DenseArray<arolla::Unit>> presence_mask_array;

    // Check filter's dtype.
    if (filter.present_count() == 0) {
      presence_mask_array =
          arolla::CreateEmptyDenseArray<arolla::Unit>(filter.size());
    } else if (filter.dtype() != arolla::GetQType<arolla::Unit>()) {
      return OperatorEvalError(kInverseSelectOpName,
                               "fltr argument must have all items "
                               "of MASK dtype");
    } else {
      presence_mask_array = filter.values<arolla::Unit>();
    }

    // Check ds_shape is equivalent to the shape after applying the filter.
    auto filter_edges = filter_shape.edges();
    arolla::JaggedDenseArrayShape::EdgeVec filter_select_edges(
        filter_edges.begin(), filter_edges.end());
    arolla::DenseGroupOps<arolla::SimpleCountAggregator> agg(
        arolla::GetHeapBufferFactory());
    ASSIGN_OR_RETURN(auto size,
                     agg.Apply(filter_edges.back(), *presence_mask_array));
    ASSIGN_OR_RETURN(filter_select_edges.back(),
                     arolla::DenseArrayEdgeFromSizesOp()(&ctx, size));

    ASSIGN_OR_RETURN(auto filter_select_shape,
                     arolla::JaggedDenseArrayShape::FromEdges(
                         std::move(filter_select_edges)));
    if (!filter_select_shape.IsEquivalentTo(ds_shape)) {
      return OperatorEvalError(
          kInverseSelectOpName,
          absl::StrCat("the shape of `ds` and the shape of the present elements"
                       " in `fltr` do not match: ",
                       arolla::Repr(ds_shape), " vs ",
                       arolla::Repr(filter_select_shape)));
    }

    arolla::DenseArray<int64_t> present_indices =
        arolla::DenseArrayPresentIndicesOp()(&ctx, *presence_mask_array);

    DataSliceImpl::Builder builder(presence_mask_array->size());
    builder.GetMutableAllocationIds().Insert(ds_impl.allocation_ids());

    RETURN_IF_ERROR(ds_impl.VisitValues([&](const auto& array) -> absl::Status {
      auto res = arolla::DenseArrayFromIndicesAndValues()(
          &ctx, present_indices, array, presence_mask_array->size());
      builder.AddArray(std::move(res));
      return absl::OkStatus();
    }));

    return std::move(builder).Build();
  };

  absl::StatusOr<DataSliceImpl> operator()(
      const DataSliceImpl& ds_impl,
      const arolla::JaggedDenseArrayShape& ds_shape, const DataItem& filter,
      const arolla::JaggedDenseArrayShape& filter_size) const {
    return absl::InternalError("invalid case ensured by the caller");
  };

  absl::StatusOr<DataItem> operator()(
      const DataItem& ds_impl, const arolla::JaggedDenseArrayShape& ds_shape,
      const DataItem& filter,
      const arolla::JaggedDenseArrayShape& filter_shape) const {
    if (filter.has_value() && !filter.holds_value<arolla::Unit>()) {
      return OperatorEvalError(kInverseSelectOpName,
                               "fltr argument must have all items "
                               "of MASK dtype");
    }
    if (ds_impl.has_value() != filter.has_value()) {
      return OperatorEvalError(
          kInverseSelectOpName,
          "the shape of `ds` and the shape of the present elements"
          " in `fltr` do not match: because both are DataItems but have "
          "different presences");
    }
    return ds_impl;
  };

  absl::StatusOr<DataItem> operator()(
      const DataItem& ds_impl, const arolla::JaggedDenseArrayShape& ds_shape,
      const DataSliceImpl& filter,
      const arolla::JaggedDenseArrayShape& filter_shape) const {
    return absl::InternalError("invalid case ensured by the caller");
  }
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_INVERSE_SELECT_H_
