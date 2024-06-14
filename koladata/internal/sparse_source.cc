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
#include "koladata/internal/sparse_source.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/status.h"

namespace koladata::internal {

std::optional<DataItem> SparseSource::Get(ObjectId object) const {
  if (auto it = data_item_map_.find(object); it != data_item_map_.end()) {
    return it->second;
  } else {
    return std::nullopt;
  }
}

DataSliceImpl SparseSource::Get(const ObjectIdArray& objects) const {
  size_t size = objects.size();
  if (data_item_map_.empty() || objects.IsAllMissing()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(size);
  }
  if (size == 1) {
    DCHECK(objects.present(0));
    ObjectId object = objects.values[0];
    if (auto it = data_item_map_.find(object); it != data_item_map_.end()) {
      const auto& item = it->second;
      return item.VisitValue([&](const auto& value) -> DataSliceImpl {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, MissingValue>) {
          return DataSliceImpl::CreateEmptyAndUnknownType(size);
        } else {
          if constexpr (std::is_same_v<T, ObjectId>) {
            return DataSliceImpl::CreateWithAllocIds(
                AllocationIdSet(AllocationId(value)),
                arolla::CreateConstDenseArray<T>(size, value));
          } else {
            return DataSliceImpl::Create(
                arolla::CreateConstDenseArray<T>(size, value));
          }
        }
      });
    }
    return DataSliceImpl::CreateEmptyAndUnknownType(size);
  }

  DataSliceImpl::Builder bldr(size);
  objects.ForEachPresent([&](int64_t id, ObjectId object) {
    if (auto it = data_item_map_.find(object); it != data_item_map_.end()) {
      const auto& item = it->second;
      bldr.Insert(id, item);
    }
  });
  return std::move(bldr).Build();
}

void SparseSource::Get(absl::Span<const ObjectId> objs,
                       DataSliceImpl::Builder& bldr,
                       absl::Span<arolla::bitmap::Word> mask) const {
  size_t size = objs.size();
  for (int64_t id = 0; id < size; ++id) {
    if (!arolla::bitmap::GetBit(mask.data(), id)) {
      continue;
    }
    ObjectId object = objs[id];
    if (auto it = data_item_map_.find(object); it != data_item_map_.end()) {
      const auto& item = it->second;
      arolla::bitmap::UnsetBit(mask.data(), id);
      bldr.Insert(id, item);
    }
  }
}

void SparseSource::Set(ObjectId object, const DataItem& value) {
  if (ObjectBelongs(object)) {
    data_item_map_[object] = value;
  }
}

absl::Status SparseSource::Set(const ObjectIdArray& objects,
                               const DataSliceImpl& values) {
  if (objects.size() != values.size()) {
    return arolla::SizeMismatchError(
        {objects.size(), static_cast<int64_t>(values.size())});
  }
  objects.ForEachPresent([&](int64_t id, ObjectId object) {
    if (ObjectBelongs(object)) {
      data_item_map_[object] = values[id];
    }
  });
  return absl::OkStatus();
}

}  // namespace koladata::internal
