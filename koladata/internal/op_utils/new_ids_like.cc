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
#include "koladata/internal/op_utils/new_ids_like.h"

#include <cstdint>
#include <type_traits>
#include <utility>

#include "arolla/memory/buffer.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal {

DataSliceImpl NewIdsLike(const DataSliceImpl& ds) {
  SliceBuilder bldr(ds.size());
  ds.VisitValues([&](const auto& array) {
    using T = typename std::decay_t<decltype(array)>::base_type;
    if constexpr (std::is_same_v<T, ObjectId>) {
      int64_t count_objects = 0;
      int64_t count_dicts = 0;
      int64_t count_lists = 0;
      int64_t count_schemas = 0;
      array.ForEachPresent([&](int64_t id, const ObjectId& obj_id) {
        if (obj_id.IsDict()) {
          count_dicts += 1;
        } else if (obj_id.IsList()) {
          count_lists += 1;
        } else if (obj_id.IsSchema()) {
          count_schemas += 1;
        } else {
          count_objects += 1;
        }
      });
      AllocationId new_objects = Allocate(count_objects);
      AllocationId new_dicts = AllocateDicts(count_dicts);
      AllocationId new_lists = AllocateLists(count_lists);
      AllocationId new_schemas = AllocateExplicitSchemas(count_schemas);
      if (count_objects > 0) {
        bldr.GetMutableAllocationIds().Insert(new_objects);
      }
      if (count_dicts > 0) {
        bldr.GetMutableAllocationIds().Insert(new_dicts);
      }
      if (count_lists > 0) {
        bldr.GetMutableAllocationIds().Insert(new_lists);
      }
      if (count_schemas > 0) {
        bldr.GetMutableAllocationIds().Insert(new_schemas);
      }
      count_objects = 0;
      count_dicts = 0;
      count_lists = 0;
      count_schemas = 0;
      auto cloned_ids_bldr = arolla::Buffer<ObjectId>::Builder(array.size());
      array.ForEachPresent([&](int64_t id, const ObjectId& obj_id) {
        if (obj_id.IsDict()) {
          cloned_ids_bldr.Set(id, new_dicts.ObjectByOffset(count_dicts++));
        } else if (obj_id.IsList()) {
          cloned_ids_bldr.Set(id, new_lists.ObjectByOffset(count_lists++));
        } else if (obj_id.IsSchema()) {
          cloned_ids_bldr.Set(id, new_schemas.ObjectByOffset(count_schemas++));
        } else {
          cloned_ids_bldr.Set(id, new_objects.ObjectByOffset(count_objects++));
        }
      });
      bldr.InsertIfNotSet<ObjectId>(array.bitmap, {},
                                    std::move(cloned_ids_bldr).Build());
    }
  });
  return std::move(bldr).Build();
}

}  // namespace koladata::internal
