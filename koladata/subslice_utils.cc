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
#include "koladata/subslice_utils.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/arolla_utils.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/op_utils/at.h"
#include "koladata/shape_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/factory_ops.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::subslice {

namespace {

absl::StatusOr<DataSlice> AtImpl(const DataSlice& x, const DataSlice& indices) {
  if (x.GetShape().rank() != 1) {
    return absl::InternalError("AtImpl should only be called on flat slices");
  }
  ASSIGN_OR_RETURN(auto index_array, ToArollaDenseArray<int64_t>(indices),
                   internal::KodaErrorFromCause(
                       "invalid indices DataSlice is provided", std::move(_)));

  return DataSlice::Create(internal::AtOp(x.slice(), index_array),
                           indices.GetShape(), x.GetSchemaImpl(), x.GetBag());
}

// We pass parent_indices by value since it is not used after calling this
// function, and it helps avoid a copy.
absl::StatusOr<DataSlice> SubsliceChildIndicesForSingle(
    DataSlice parent_indices, DataSlice child_indices,
    const arolla::DenseArrayEdge& edge) {
  ASSIGN_OR_RETURN(auto aligned_slices,
                   shape::Align(std::vector{std::move(parent_indices),
                                            std::move(child_indices)}));
  ASSIGN_OR_RETURN(auto parent_indices_array,
                   ToArollaDenseArray<int64_t>(aligned_slices[0]));
  ASSIGN_OR_RETURN(
      auto arg_array, ToArollaDenseArray<int64_t>(aligned_slices[1]),
      internal::KodaErrorFromCause(
          "the provided indices must contain only integers", std::move(_)));
  ASSIGN_OR_RETURN(
      auto new_chosen_indices_array,
      arolla::CreateDenseOp([&edge](int64_t parent_index, int64_t child_index)
                                -> arolla::OptionalValue<int64_t> {
        int64_t parent_size = edge.split_size(parent_index);
        if (child_index < 0) child_index += parent_size;
        if (child_index < 0 || child_index >= parent_size) {
          return std::nullopt;
        } else {
          return edge.edge_values().values[parent_index] + child_index;
        }
      })(parent_indices_array, arg_array));
  return DataSlice::Create(
      internal::DataSliceImpl::Create(std::move(new_chosen_indices_array)),
      aligned_slices[0].GetShape(), internal::DataItem(schema::kInt64));
}

absl::StatusOr<DataSlice> SubsliceChildIndicesForRange(
    DataSlice parent_indices, Slice child_range,
    const arolla::DenseArrayEdge& edge) {
  std::vector<DataSlice> to_align{std::move(parent_indices)};
  int64_t start_pos = -1;
  int64_t stop_pos = -1;
  if (child_range.start.has_value()) {
    start_pos = to_align.size();
    to_align.push_back(std::move(*child_range.start));
  }
  if (child_range.stop.has_value()) {
    stop_pos = to_align.size();
    to_align.push_back(std::move(*child_range.stop));
  }
  ASSIGN_OR_RETURN(auto aligned_slices, shape::Align(std::move(to_align)));
  ASSIGN_OR_RETURN(auto parent_indices_array,
                   ToArollaDenseArray<int64_t>(aligned_slices[0]));
  std::optional<arolla::DenseArray<int64_t>> start_array;
  std::optional<arolla::DenseArray<int64_t>> stop_array;
  if (start_pos >= 0) {
    ASSIGN_OR_RETURN(
        start_array, ToArollaDenseArray<int64_t>(aligned_slices[start_pos]),
        internal::KodaErrorFromCause(
            "'start' argument of a Slice must contain only integers",
            std::move(_)));
  }
  if (stop_pos >= 0) {
    ASSIGN_OR_RETURN(
        stop_array, ToArollaDenseArray<int64_t>(aligned_slices[stop_pos]),
        internal::KodaErrorFromCause(
            "'stop' argument of a Slice must contain only integers",
            std::move(_)));
  }
  arolla::DenseArrayBuilder<int64_t> new_split_points_builder(
      parent_indices_array.size() + 1);
  std::vector<int64_t> new_chosen_indices;
  parent_indices_array.ForEach(
      [&edge, &start_array, &stop_array, &new_chosen_indices,
       &new_split_points_builder](int64_t id, bool presence,
                                  int64_t parent_index) {
        new_split_points_builder.Set(id, new_chosen_indices.size());
        if (!presence) {
          return;
        }
        int64_t parent_size = edge.split_size(parent_index);
        int64_t start = 0;
        if (start_array.has_value()) {
          if (!start_array->present(id)) {
            return;
          }
          start = start_array->values[id];
        }
        if (start < 0) start += parent_size;
        start = std::clamp<int64_t>(start, 0, parent_size);
        int64_t stop = parent_size;
        if (stop_array.has_value()) {
          if (!stop_array->present(id)) {
            return;
          }
          stop = stop_array->values[id];
        }
        if (stop < 0) stop += parent_size;
        stop = std::clamp<int64_t>(stop, 0, parent_size);
        int64_t offset = edge.edge_values().values[parent_index];
        for (int64_t i = start; i < stop; ++i) {
          new_chosen_indices.push_back(offset + i);
        }
      });
  new_split_points_builder.Set(parent_indices_array.size(),
                               new_chosen_indices.size());
  ASSIGN_OR_RETURN(auto new_shape,
                   aligned_slices[0].GetShape().AddDims(
                       {arolla::DenseArrayEdge::UnsafeFromSplitPoints(
                           std::move(new_split_points_builder).Build())}));
  return DataSlice::Create(
      internal::DataSliceImpl::Create(
          arolla::CreateFullDenseArray(new_chosen_indices)),
      std::move(new_shape), internal::DataItem(schema::kInt64));
}

}  // namespace

absl::StatusOr<DataSlice> Subslice(const DataSlice& x,
                                   std::vector<SlicingArgType> slice_args) {
  if (slice_args.empty()) {
    return x;
  }
  const auto& shape = x.GetShape();
  if (shape.rank() == 1 && slice_args.size() == 1 &&
      std::holds_alternative<DataSlice>(slice_args[0])) {
    // Fast path for the most common case.
    return AtImpl(x, std::get<DataSlice>(slice_args[0]));
  }
  arolla::EvaluationContext ctx;
  ASSIGN_OR_RETURN(
      auto chosen_indices,
      DataSlice::Create(
          internal::DataSliceImpl::Create(arolla::DenseArrayIotaOp()(
              &ctx,
              {shape.edges()[shape.rank() - slice_args.size()].parent_size()})),
          shape.RemoveDims(shape.rank() - slice_args.size()),
          internal::DataItem(schema::kInt64)));
  for (int i = 0; i < slice_args.size(); ++i) {
    const auto& edge = shape.edges()[shape.rank() - slice_args.size() + i];
    auto slice_arg = std::move(slice_args[i]);
    if (std::holds_alternative<DataSlice>(slice_arg)) {
      ASSIGN_OR_RETURN(chosen_indices,
                       SubsliceChildIndicesForSingle(
                           std::move(chosen_indices),
                           std::move(std::get<DataSlice>(slice_arg)), edge));
    } else {
      ASSIGN_OR_RETURN(chosen_indices,
                       SubsliceChildIndicesForRange(
                           std::move(chosen_indices),
                           std::move(std::get<Slice>(slice_arg)), edge));
    }
  }
  ASSIGN_OR_RETURN(auto flat_x, x.Reshape(shape.FlattenDims(0, shape.rank())));
  return AtImpl(flat_x, chosen_indices);
}

}  // namespace koladata::subslice
