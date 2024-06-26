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
#include "koladata/shape_utils.h"

#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::shape {

absl::StatusOr<typename DataSlice::JaggedShapePtr> GetCommonShape(
    absl::Span<const DataSlice> slices) {
  if (slices.empty()) {
    return absl::InvalidArgumentError(
        "computing a common shape requires at least 1 input");
  }
  DataSlice::JaggedShapePtr shape = nullptr;
  for (const auto& slice : slices) {
    if (shape == nullptr || shape->rank() < slice.GetShape().rank()) {
      shape = slice.GetShapePtr();
    }
  }
  DCHECK_NE(shape, nullptr);
  for (const auto& slice : slices) {
    if (!slice.GetShape().IsBroadcastableTo(*shape)) {
      return absl::InvalidArgumentError("shapes are not compatible");
    }
  }
  return shape;
}

absl::StatusOr<std::vector<DataSlice>> Align(std::vector<DataSlice> slices) {
  ASSIGN_OR_RETURN(auto shape, GetCommonShape(slices));
  for (auto& slice : slices) {
    ASSIGN_OR_RETURN(slice, BroadcastToShape(std::move(slice), shape));
  }
  return slices;
}

absl::StatusOr<std::pair<std::vector<DataSlice>, DataSlice::JaggedShapePtr>>
AlignNonScalars(std::vector<DataSlice> slices) {
  ASSIGN_OR_RETURN(auto shape, GetCommonShape(slices));
  for (auto& slice : slices) {
    if (slice.GetShape().rank() != 0) {
      ASSIGN_OR_RETURN(slice, BroadcastToShape(std::move(slice), shape));
    }
  }
  return std::make_pair(std::move(slices), std::move(shape));
}

}  // namespace koladata::shape