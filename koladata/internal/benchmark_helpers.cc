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
#include "koladata/internal/benchmark_helpers.h"

#include <cstdint>
#include <functional>
#include <utility>

#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"
#include "arolla/dense_array/dense_array.h"

namespace koladata::internal {

DataSliceImpl FilterSingleAllocDataSlice(const DataSliceImpl& ds,
                                         int64_t batch_size,
                                         int64_t skip_size) {
  if (skip_size == 0) {
    return ds;
  }
  int total_size = batch_size * (skip_size + 1);
  AllocationId alloc = AllocationId(ds.values<ObjectId>()[0].value);
  arolla::DenseArrayBuilder<ObjectId> builder(batch_size);
  for (int64_t i = 0, id = 0; i < total_size; i += skip_size + 1) {
    builder.Set(id++, alloc.ObjectByOffset(i));
  }
  return DataSliceImpl::CreateObjectsDataSlice(std::move(builder).Build(),
                                               AllocationIdSet(alloc));
}

DataSliceImpl RemoveItemsIf(
    const DataSliceImpl& ds,
    std::function<bool(const DataItem&)> remove_fn) {
  SliceBuilder builder(ds.size());
  int64_t i = 0;
  for (const DataItem& item : ds) {
    if (remove_fn(item)) {
      ++i;
    } else {
      builder.InsertIfNotSetAndUpdateAllocIds(i++, item);
    }
  }
  return std::move(builder).Build();
}

}  // namespace koladata::internal
