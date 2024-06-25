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
#include "koladata/internal/op_utils/select.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_group_ops.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/aggregation/group_op_accumulators.h"
#include "arolla/qexpr/operators/dense_array/array_ops.h"
#include "arolla/qexpr/operators/dense_array/edge_ops.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

namespace {
using ::arolla::DenseArray;
using ::arolla::DenseArrayAtOp;
using ::arolla::DenseArrayEdge;
using ::arolla::DenseArrayEdgeFromSizesOp;
using ::arolla::DenseArrayEdgeSizesOp;
using ::arolla::DenseArrayPresentIndicesOp;
using ::arolla::EvaluationContext;
using ::arolla::JaggedDenseArrayShape;
using ::arolla::JaggedDenseArrayShapePtr;
using ::arolla::Unit;

// Selects the present groups in the `edge` indicated by the
// presence_mask_array.
absl::StatusOr<DenseArrayEdge> SelectPresentEdge(
    EvaluationContext& ctx, const DenseArrayEdge& edge,
    const DenseArray<Unit>& presence_mask_array) {
  ASSIGN_OR_RETURN(DenseArray<int64_t> edge_sizes,
                   DenseArrayEdgeSizesOp()(&ctx, edge));
  DenseArray<int64_t> present_indices =
      DenseArrayPresentIndicesOp()(&ctx, presence_mask_array);

  DenseArray<int64_t> selected_sizes =
      DenseArrayAtOp()(&ctx, edge_sizes, present_indices);

  return DenseArrayEdgeFromSizesOp()(&ctx, selected_sizes);
}

// Counts the number of presences mask in each group.
absl::StatusOr<DenseArrayEdge> CountPresenceInGroup(
    EvaluationContext& ctx, const DenseArrayEdge& edge,
    const DenseArray<Unit>& presence_mask_array) {
  arolla::DenseGroupOps<arolla::SimpleCountAggregator> agg(
      arolla::GetHeapBufferFactory());
  ASSIGN_OR_RETURN(DenseArray<int64_t> size,
                   agg.Apply(edge, presence_mask_array));
  return DenseArrayEdgeFromSizesOp()(&ctx, size);
}

}  // namespace

absl::StatusOr<SelectOp::Result<DataSliceImpl>> SelectOp::operator()(
    const DataSliceImpl& ds_impl, const JaggedDenseArrayShapePtr& ds_shape,
    const DataSliceImpl& filter, const JaggedDenseArrayShapePtr& filter_shape) {
  if (filter_shape->rank() < 1) {
    return absl::InvalidArgumentError("filter must have at least 1 dimension");
  }
  if (filter_shape->rank() > ds_shape->rank()) {
    return absl::InvalidArgumentError(
        "filter cannot have a higher rank than the DataSlice.");
  }

  if (!filter_shape->IsBroadcastableTo(*ds_shape)) {
    return absl::FailedPreconditionError(
        "filter should be broadcastable to the input DataSlice");
  }

  const absl::Span<const DenseArrayEdge> ds_edges = ds_shape->edges();

  arolla::JaggedDenseArrayShape::EdgeVec new_edges;
  new_edges.reserve(ds_edges.size());

  // The first filter_shape.rank()-1 dimensions are unchanged.
  std::copy_n(ds_edges.begin(), filter_shape->rank() - 1,
              std::back_inserter(new_edges));

  arolla::EvaluationContext ctx;
  DenseArray<arolla::Unit> presence_mask_array;

  if (filter.present_count() == 0) {
    presence_mask_array =
        arolla::CreateEmptyDenseArray<arolla::Unit>(filter.size());
  } else if (filter.dtype() != arolla::GetQType<arolla::Unit>()) {
    return absl::InvalidArgumentError(
        "second argument to operator select must have all items "
        "of MASK dtype");
  } else {
    presence_mask_array = filter.values<arolla::Unit>();
  }

  auto ds_edge_iterator = ds_edges.begin() + filter_shape->rank() - 1;
  {
    // The dimension at filter_shape.rank().
    ASSIGN_OR_RETURN(
        DenseArrayEdge edge,
        CountPresenceInGroup(ctx, *ds_edge_iterator, presence_mask_array));
    new_edges.push_back(std::move(edge));
  }

  // Handles the remaining dimension by moving the iterator first.
  for (++ds_edge_iterator; ds_edge_iterator != ds_edges.end();
       ++ds_edge_iterator) {
    ASSIGN_OR_RETURN(
        DenseArrayEdge edge,
        SelectPresentEdge(ctx, *ds_edge_iterator, presence_mask_array));

    ASSIGN_OR_RETURN(presence_mask_array,
                     arolla::DenseArrayExpandOp()(&ctx, presence_mask_array,
                                                  *ds_edge_iterator));
    if (edge.child_size() == 0) {
      break;
    }
    new_edges.push_back(std::move(edge));
  }

  ASSIGN_OR_RETURN(JaggedDenseArrayShapePtr new_shape,
                   JaggedDenseArrayShape::FromEdges(std::move(new_edges)));

  DataSliceImpl::Builder builder(presence_mask_array.PresentCount());

  // TODO: keep only necessary allocation ids.
  builder.GetMutableAllocationIds().Insert(ds_impl.allocation_ids());

  DenseArray<int64_t> present_indexes =
      DenseArrayPresentIndicesOp()(&ctx, presence_mask_array);

  RETURN_IF_ERROR(ds_impl.VisitValues([&](const auto& array) -> absl::Status {
    using T = typename std::decay_t<decltype(array)>::base_type;
    // Gets elements in `array` at the positions from `present_indexes`.
    DenseArray<T> res = DenseArrayAtOp()(&ctx, array, present_indexes);
    builder.AddArray(std::move(res));
    return absl::OkStatus();
  }));

  return SelectOp::Result<DataSliceImpl>(
      {std::move(builder).Build(), std::move(new_shape)});
}

absl::StatusOr<SelectOp::Result<DataSliceImpl>> SelectOp::operator()(
    const DataSliceImpl& ds_impl, const JaggedDenseArrayShapePtr& ds_shape,
    const DataItem& filter, const JaggedDenseArrayShapePtr& filter_size) const {
  if (!filter.has_value()) {
    if (!ds_impl.is_single_dtype()) {
      DataSliceImpl empty_slice = DataSliceImpl::CreateEmptyAndUnknownType(1);
      return SelectOp::Result<DataSliceImpl>(
          {std::move(empty_slice), JaggedDenseArrayShape::Empty()});
    }
    ASSIGN_OR_RETURN(DataSliceImpl empty_slice,
                     DataSliceImpl::CreateEmptyWithType(1, ds_impl.dtype()));
    return SelectOp::Result<DataSliceImpl>(
        {std::move(empty_slice), JaggedDenseArrayShape::Empty()});
  }
  if (filter.dtype() != arolla::GetQType<arolla::Unit>()) {
    return absl::InvalidArgumentError(
        "second argument to operator select must have all items "
        "of MASK dtype");
  }
  return SelectOp::Result<DataSliceImpl>({ds_impl, ds_shape});
}

absl::StatusOr<SelectOp::Result<DataItem>> SelectOp::operator()(
    const DataItem& ds_impl, const JaggedDenseArrayShapePtr& ds_shape,
    const DataItem& filter,
    const JaggedDenseArrayShapePtr& filter_shape) const {
  if (!filter.has_value()) {
    return SelectOp::Result<DataItem>(
        {DataItem(), JaggedDenseArrayShape::Empty()});
  }
  if (!filter.holds_value<arolla::Unit>()) {
    return absl::InvalidArgumentError(
        "second argument to operator select must have all items "
        "of MASK dtype");
  }
  return SelectOp::Result<DataItem>({ds_impl, ds_shape});
}

absl::StatusOr<SelectOp::Result<DataItem>> SelectOp::operator()(
    const DataItem& ds_impl, const JaggedDenseArrayShapePtr& ds_shape,
    const DataSliceImpl& filter,
    const JaggedDenseArrayShapePtr& filter_shape) const {
  return absl::InternalError("invalid case");
}

}  // namespace koladata::internal
