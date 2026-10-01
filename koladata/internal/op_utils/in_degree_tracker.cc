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
#include "koladata/internal/op_utils/in_degree_tracker.h"

#include <cstdint>
#include <type_traits>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype_traits.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {
namespace {

constexpr absl::string_view kInDegreeAttrName = "in";

}  // namespace

DataSliceImpl InDegreeTracker::FilterToObjects(const DataSliceImpl& slice) {
  if (slice.dtype() == arolla::GetQType<ObjectId>()) {
    return slice;
  }
  if (!slice.is_mixed_dtype()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(0);
  }
  DataSliceImpl res = DataSliceImpl::CreateEmptyAndUnknownType(0);
  slice.VisitValues([&](const auto& array) {
    using T = typename std::decay_t<decltype(array)>::base_type;
    if constexpr (std::is_same_v<T, ObjectId>) {
      res = DataSliceImpl::CreateWithAllocIds(slice.allocation_ids(), array);
    }
  });
  return res;
}

absl::StatusOr<DataSliceImpl> InDegreeTracker::ProcessSlice(
    const DataSliceImpl& slice, int64_t delta, int64_t target) {
  DataSliceImpl objects_slice = FilterToObjects(slice);
  if (objects_slice.present_count() == 0) {
    return DataSliceImpl::CreateEmptyAndUnknownType(0);
  }
  return databag_->InternalAddIntAndReturnObjectsWithTargetValue(
      objects_slice, kInDegreeAttrName, delta, target);
}

}  // namespace koladata::internal
