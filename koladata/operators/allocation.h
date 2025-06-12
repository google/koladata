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
// Operators that allocate new ItemIds without attaching DataBag(s).

#ifndef KOLADATA_OPERATORS_ALLOCATION_H_
#define KOLADATA_OPERATORS_ALLOCATION_H_

#include <cstddef>
#include <cstdint>
#include <utility>

#include "absl/status/statusor.h"
#include "koladata/alloc_utils.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/internal/object_id.h"

namespace koladata::ops {

// kde.allocation.new_itemid_shaped.
// Allocates new ItemIds of the given shape without any DataBag attached.
inline absl::StatusOr<DataSlice> NewItemIdShaped(
    DataSlice::JaggedShape shape, internal::NonDeterministicToken) {
  size_t size = shape.size();
  return DataSlice::Create(internal::DataSliceImpl::AllocateEmptyObjects(size),
                           std::move(shape),
                           internal::DataItem(schema::kItemId));
}

// kd.allocation.new_listid_shaped.
// Allocates new List ItemIds of the given shape.
inline absl::StatusOr<DataSlice> NewListIdShaped(
    DataSlice::JaggedShape shape, internal::NonDeterministicToken) {
  size_t size = shape.size();
  return DataSlice::Create(
      internal::DataSliceImpl::ObjectsFromAllocation(
          internal::AllocateLists(size), size),
      std::move(shape), internal::DataItem(schema::kItemId));
}

// kd.allocation.new_dictid_shaped.
// Allocates new Dict ItemIds of the given shape.
inline absl::StatusOr<DataSlice> NewDictIdShaped(
    DataSlice::JaggedShape shape, internal::NonDeterministicToken) {
  size_t size = shape.size();
  return DataSlice::Create(
      internal::DataSliceImpl::ObjectsFromAllocation(
          internal::AllocateDicts(size), size),
      std::move(shape), internal::DataItem(schema::kItemId));
}

// kd.allocation.new_itemid_like.
// Allocates new ItemIds with the shape and sparsity of shape_and_mask_from.
inline absl::StatusOr<DataSlice> NewItemIdLike(
    const DataSlice& shape_and_mask_from, internal::NonDeterministicToken) {
  return AllocateLike(
      shape_and_mask_from, internal::AllocateSingleObject,
      internal::Allocate, internal::DataItem(schema::kItemId));
}

// kd.allocation.new_listid_like.
// Allocates new List ItemIds with the shape and sparsity of
// shape_and_mask_from.
inline absl::StatusOr<DataSlice> NewListIdLike(
    const DataSlice& shape_and_mask_from, internal::NonDeterministicToken) {
  return AllocateLike(
      shape_and_mask_from, internal::AllocateSingleList,
      internal::AllocateLists, internal::DataItem(schema::kItemId));
}

// kd.allocation.new_dictid_like.
// Allocates new Dict ItemIds with the shape and sparsity of
// shape_and_mask_from.
inline absl::StatusOr<DataSlice> NewDictIdLike(
    const DataSlice& shape_and_mask_from, internal::NonDeterministicToken) {
  return AllocateLike(
      shape_and_mask_from, internal::AllocateSingleDict,
      internal::AllocateDicts, internal::DataItem(schema::kItemId));
}

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_ALLOCATION_H_
