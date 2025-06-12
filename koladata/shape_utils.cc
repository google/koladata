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
#include "koladata/shape_utils.h"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/repr.h"
#include "arolla/util/status.h"
#include "koladata/data_slice.h"
#include "koladata/internal/errors.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::shape {

absl::StatusOr<DataSlice::JaggedShape> GetCommonShape(
    absl::Span<const DataSlice> slices) {
  if (slices.empty()) {
    return absl::InvalidArgumentError(
        "computing a common shape requires at least 1 input");
  }
  size_t common_shape_id = 0;
  for (size_t i = 1; i < slices.size(); ++i) {
    if (slices[common_shape_id].GetShape().rank() <
        slices[i].GetShape().rank()) {
      common_shape_id = i;
    }
  }
  const auto& common_shape = slices[common_shape_id].GetShape();
  for (size_t i = 0; i < slices.size(); ++i) {
    const DataSlice::JaggedShape& slice_shape = slices[i].GetShape();
    if (!slice_shape.IsBroadcastableTo(common_shape)) {
      absl::Status status = absl::InvalidArgumentError(absl::StrFormat(
          "shapes are not compatible: %s vs %s", arolla::Repr(slice_shape),
          arolla::Repr(common_shape)));
      return arolla::WithPayload(std::move(status),
                                 internal::ShapeAlignmentError{
                                     .common_shape_id = common_shape_id,
                                     .incompatible_shape_id = i,
                                 });
    }
  }
  return common_shape;
}

absl::StatusOr<std::vector<DataSlice>> Align(std::vector<DataSlice> slices) {
  ASSIGN_OR_RETURN(auto shape, GetCommonShape(slices));
  for (auto& slice : slices) {
    ASSIGN_OR_RETURN(slice, BroadcastToShape(std::move(slice), shape));
  }
  return slices;
}

absl::StatusOr<std::pair<std::vector<DataSlice>, DataSlice::JaggedShape>>
AlignNonScalars(std::vector<DataSlice> slices) {
  ASSIGN_OR_RETURN(auto shape, GetCommonShape(slices));
  for (auto& slice : slices) {
    if (!slice.is_item()) {
      ASSIGN_OR_RETURN(slice, BroadcastToShape(std::move(slice), shape));
    }
  }
  return std::make_pair(std::move(slices), std::move(shape));
}

void ShapeBuilder::Add(int64_t group_size) {
  last_split_ += group_size;
  edge_builder_.Add(i_next_, last_split_);
  ++i_next_;
}

absl::StatusOr<DataSlice::JaggedShape> ShapeBuilder::Build() && {
  auto edge_array = std::move(edge_builder_).Build();
  ASSIGN_OR_RETURN(
      auto last_edge,
      DataSlice::JaggedShape::Edge::FromSplitPoints(std::move(edge_array)));
  return prev_shape_.AddDims({std::move(last_edge)});
}

}  // namespace koladata::shape
