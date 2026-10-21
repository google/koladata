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
#include "koladata/internal/op_utils/group_by_utils.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/qexpr/operators/dense_array/array_ops.h"
#include "arolla/qtype/qtype_traits.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/has.h"
#include "koladata/internal/op_utils/presence_and.h"

namespace koladata::internal {
namespace {

// Restricts `slices` (of the same size) to the items present in all of them.
absl::Status IntersectPresence(absl::Span<DataSliceImpl> slices) {
  std::optional<DataSliceImpl> common_mask;
  for (const DataSliceImpl& slice : slices) {
    DCHECK_EQ(slice.size(), slices[0].size());
    if (slice.present_count() < slice.size()) {
      ASSIGN_OR_RETURN(DataSliceImpl mask, HasOp()(slice));
      if (!common_mask.has_value()) {
        common_mask = std::move(mask);
      } else {
        ASSIGN_OR_RETURN(common_mask, PresenceAndOp()(*common_mask, mask));
      }
    }
  }
  if (!common_mask.has_value()) {
    return absl::OkStatus();
  }
  const size_t present_count = common_mask->present_count();
  for (DataSliceImpl& slice : slices) {
    if (slice.present_count() != present_count) {
      ASSIGN_OR_RETURN(slice, PresenceAndOp()(slice, *common_mask));
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ObjectsGroupBy::ForEachGroup(
    absl::Span<const DataSliceImpl> keys,
    absl::Span<const DataSliceImpl> slices, GroupFn fn) {
  DCHECK(!keys.empty());
  DCHECK(!slices.empty());
  // The slices followed by the keys, restricted to the items present in all of
  // them. Copying DataSliceImpl only increments a reference count.
  absl::InlinedVector<DataSliceImpl, 4> present(slices.begin(), slices.end());
  present.insert(present.end(), keys.begin(), keys.end());
  RETURN_IF_ERROR(IntersectPresence(absl::MakeSpan(present)));
  absl::Span<DataSliceImpl> present_slices =
      absl::MakeSpan(present).first(slices.size());
  absl::Span<const DataSliceImpl> present_keys =
      absl::MakeConstSpan(present).subspan(slices.size());

  const size_t present_count = present_slices[0].present_count();
  if (present_count == 0) {
    return absl::OkStatus();
  }

  // Find keys with all-equal values; they don't need to be grouped.
  absl::InlinedVector<DataItem, 2> group_keys(keys.size());
  absl::InlinedVector<size_t, 2> non_constant_keys;
  for (size_t i = 0; i < keys.size(); ++i) {
    if (std::optional<DataItem> key = GetItemIfAllEqual(present_keys[i]);
        key.has_value()) {
      group_keys[i] = *std::move(key);
    } else {
      non_constant_keys.push_back(i);
    }
  }
  // Fast path for a single group: avoid building an edge.
  if (non_constant_keys.empty()) {
    if (present_count < present_slices[0].size()) {
      for (DataSliceImpl& slice : present_slices) {
        DCHECK(slice.dtype() == arolla::GetQType<ObjectId>());
        slice = DataSliceImpl::Create(arolla::DenseArrayPresentValuesOp()(
            &ctx_, slice.values<ObjectId>()));
      }
    }
    return fn(group_keys, present_slices);
  }

  // Group by the keys that have multiple distinct values.
  ASSIGN_OR_RETURN(arolla::DenseArrayEdge edge,
                   EdgeFromSchemasSlice(present_keys[non_constant_keys[0]]));
  for (size_t i : absl::MakeConstSpan(non_constant_keys).subspan(1)) {
    ASSIGN_OR_RETURN(edge, SplitEdgeBySchemas(edge, present_keys[i]));
  }
  absl::InlinedVector<DataSliceImpl, 2> collapsed_keys(keys.size());
  for (size_t i : non_constant_keys) {
    ASSIGN_OR_RETURN(collapsed_keys[i], CollapseByEdge(edge, present_keys[i]));
  }
  absl::InlinedVector<std::vector<arolla::DenseArray<ObjectId>>, 3>
      split_slices;
  split_slices.reserve(slices.size());
  for (const DataSliceImpl& slice : present_slices) {
    DCHECK(slice.dtype() == arolla::GetQType<ObjectId>());
    ASSIGN_OR_RETURN(std::vector<arolla::DenseArray<ObjectId>> groups,
                     ByEdge(edge, slice.values<ObjectId>()));
    split_slices.push_back(std::move(groups));
  }

  absl::InlinedVector<DataSliceImpl, 3> group_slices(slices.size());
  for (int64_t group = 0; group < edge.parent_size(); ++group) {
    for (size_t i : non_constant_keys) {
      group_keys[i] = collapsed_keys[i][group];
    }
    for (size_t i = 0; i < slices.size(); ++i) {
      // Computes the allocation ids of the group.
      group_slices[i] =
          DataSliceImpl::Create(std::move(split_slices[i][group]));
    }
    RETURN_IF_ERROR(fn(group_keys, absl::MakeSpan(group_slices)));
  }
  return absl::OkStatus();
}

DataSliceImpl ObjectsGroupBy::SchemaAllocationKeys(
    const DataSliceImpl& schemas) {
  DCHECK_GT(schemas.present_count(), 0);
  if (schemas.allocation_ids().empty() ||
      GetItemIfAllEqual(schemas).has_value()) {
    return schemas;
  }
  const arolla::DenseArray<ObjectId>& ids = schemas.values<ObjectId>();
  arolla::DenseArrayBuilder<ObjectId> keys(ids.size());
  ids.ForEachPresent([&](int64_t i, ObjectId schema) {
    keys.Set(i, schema.IsSmallAlloc() ? schema
                                      : AllocationId(schema).ObjectByOffset(0));
  });
  return DataSliceImpl::CreateWithAllocIds(schemas.allocation_ids(),
                                           std::move(keys).Build());
}

}  // namespace koladata::internal
