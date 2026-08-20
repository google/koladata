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
#include "koladata/internal/data_list.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/fixed_array.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "arolla/util/meta.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/memory_stats.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal {

DataList::DataList(DataSliceImpl data_slice, int64_t from, int64_t to) {
  if (to == -1) {
    to = data_slice.size();
  }
  DCHECK_LE(0, from);
  DCHECK_LE(from, to);
  DCHECK_LE(to, data_slice.size());
  size_ = to - from;

  if (data_slice.is_mixed_dtype()) {
    std::vector<DataItem> data(size_);
    for (int64_t i = 0; i < size_; ++i) {
      data[i] = data_slice[from + i];
    }
    data_ = std::move(data);
  } else {
    data_slice.VisitValues([&](const auto& array) {
      using T = std::decay_t<decltype(array)>::base_type;
      std::vector<std::optional<T>> data(size_);
      for (int64_t i = 0; i < size_; ++i) {
        auto v = array[from + i];
        if (v.present) {
          data[i] = T(v.value);
        }
      }
      data_ = std::move(data);
    });
  }
}

void DataList::AddToDataSlice(SliceBuilder& bldr, int64_t offset,
                              int64_t from, int64_t to) const {
  if (to == -1) {
    to = size_;
  }
  DCHECK(0 <= from && from <= to && to <= size_);
  DCHECK(0 <= offset && offset + (to - from) <= bldr.size());
  std::visit(
      [&](const auto& vec) {
        if constexpr (std::is_same_v<decltype(vec),
                                     const std::vector<DataItem>&>) {
          for (int64_t i = from; i < to; ++i, ++offset) {
            bldr.InsertIfNotSetAndUpdateAllocIds(offset, vec[i]);
          }
        } else if constexpr (!std::is_same_v<decltype(vec),
                                             const AllMissing&>) {
          using T = arolla::meta::strip_template_t<
              std::optional, typename std::decay_t<decltype(vec)>::value_type>;
          auto typed_bldr = bldr.typed<T>();
          for (int64_t i = from; i < to; ++i, ++offset) {
            const auto& opt_value = vec[i];
            typed_bldr.InsertIfNotSet(offset, opt_value);
            if constexpr (std::is_same_v<T, ObjectId>) {
              if (opt_value.has_value()) {
                bldr.GetMutableAllocationIds().Insert(AllocationId(*opt_value));
              }
            }
          }
        }
      },
      data_);
}

DataItem DataList::Get(int64_t index) const {
  DCHECK(0 <= index && index < size_);
  DataItem res;
  std::visit(
      [&]<typename T>(const T& vec) {
        if constexpr (!std::is_same_v<T, AllMissing>) {
          res = DataItem(vec[index]);
        }
      },
      data_);
  return res;
}

void DataList::SetToMissing(int64_t index) {
  DCHECK(0 <= index && index < size_);
  std::visit([&]<typename T>(T& vec) {
    if constexpr (!std::is_same_v<T, AllMissing>) {
      auto& v = vec[index];
      v = typename std::decay_t<decltype(v)>();
    }
  }, data_);
}

void DataList::SetMissingRange(int64_t index_from, int64_t index_to) {
  DCHECK(0 <= index_from && index_from <= index_to && index_to <= size_);
  std::visit(
      [&]<typename T>(T& vec) {
        if constexpr (!std::is_same_v<T, AllMissing>) {
          for (int64_t index = index_from; index < index_to; ++index) {
            auto& v = vec[index];
            v = typename std::decay_t<decltype(v)>();
          }
        }
      },
      data_);
}

void DataList::Remove(int64_t from, int64_t count) {
  DCHECK(0 <= from && count > 0 && from + count <= size_);
  std::visit(
      [&]<typename T>(T& vec) {
        if constexpr (!std::is_same_v<T, AllMissing>) {
          vec.erase(vec.begin() + from, vec.begin() + from + count);
        }
      },
      data_);
  size_ -= count;
}

void DataList::InsertMissing(int64_t from, int64_t count) {
  DCHECK(0 <= from && count > 0 && from <= size_);
  std::visit(
      [&]<typename T>(T& vec) {
        if constexpr (!std::is_same_v<T, AllMissing>) {
          vec.resize(size_ + count);
          for (int64_t i = size_ - 1; i >= from; --i) {
            vec[i + count] = vec[i];
          }
          for (int64_t i = from; i < from + count; ++i) {
            auto& v = vec[i];
            v = typename std::decay_t<decltype(v)>();
          }
        }
      },
      data_);
  size_ += count;
}

void DataList::Resize(size_t size) {
  std::visit(
      [&]<typename T>(T& vec) {
        if constexpr (!std::is_same_v<T, AllMissing>) {
          vec.resize(size);
        }
      },
      data_);
  size_ = size;
}

void DataList::ConvertToDataItems() {
  std::vector<DataItem> new_data(size_);
  std::visit(
      [&]<typename T>(const T& vec) {
        if constexpr (!std::is_same_v<T, AllMissing>) {
          for (size_t i = 0; i < size_; ++i) {
            new_data[i] = DataItem(vec[i]);
          }
        }
      },
      data_);
  data_ = std::move(new_data);
}

void DataListVector::Map::ConvertToArray(Array& array,
                                         const DataListVector* parent) && {
  if (parent != nullptr) {
    for (size_t i = 0; i < array.size(); ++i) {
      array[i].ptr = parent->Get(i);
    }
  }
  for (auto& [idx, list] : map) {
    array[idx].list = std::move(list);
    array[idx].ptr = &array[idx].list;
  }
}

const DataList* absl_nullable DataListVector::GetFromMap(size_t index) const {
  const auto& map = std::get<Map>(data_).map;
  auto it = map.find(index);
  if (it != map.end()) {
    return &it->second;
  }
  if (parent_ != nullptr) {
    return parent_->Get(index);
  }
  return nullptr;
}

DataList& DataListVector::GetMutableFromArray(size_t index) {
  auto& arr = std::get<Array>(data_);
  ListAndPtr& lp = arr[index];
  if (lp.ptr == nullptr) {
    lp.ptr = &lp.list;
  } else if (!lp.IsMutable()) {
    lp.list = *lp.ptr;
    lp.ptr = &lp.list;
  }
  return lp.list;
}

namespace {
bool ShouldConvertToVector(size_t map_size, size_t total_size) {
  return (map_size + 1) * 10 > total_size * 3;
}
}

DataList& DataListVector::GetMutableFromMap(size_t index) {
  auto& map = std::get<Map>(data_).map;
  auto it = map.find(index);
  if (it != map.end()) {
    return it->second;
  }
  if (ShouldConvertToVector(map.size(), size_)) {
    Map local_map = std::move(std::get<Map>(data_));
    Array& arr = data_.emplace<Array>(size_);
    std::move(local_map).ConvertToArray(arr, parent_.get());
    return GetMutableFromArray(index);
  }
  DataList& list = map[index];
  if (parent_ != nullptr) {
    const DataList* parent_list = parent_->Get(index);
    if (parent_list != nullptr) {
      list = *parent_list;
    }
  }
  return list;
}

void DataListVector::AppendMemoryStats(MemoryStatsEntry& stats) const {
  stats.shallow_size += sizeof(DataListVector);
  if (std::holds_alternative<Array>(data_)) {
    const auto& array = std::get<Array>(data_);
    stats.shallow_size += sizeof(Array::value_type) * array.size();
    for (const ListAndPtr& lptr : array) {
      if (lptr.ptr != &lptr.list) {
        continue;  // link to parent or unset
      }
      stats.shallow_size += lptr.list.size() * sizeof(DataItem);
      for (const auto& v : lptr.list) {
        stats.AppendStringsSize(v);
      }
    }
  } else {
    const auto& map = std::get<Map>(data_).map;
    stats.shallow_size += sizeof(Map::MapType::value_type) * map.size();
    for (const auto& [idx, list] : map) {
      stats.shallow_size += list.size() * sizeof(DataItem);
      for (const auto& v : list) {
        stats.AppendStringsSize(v);
      }
    }
  }
}

}  // namespace koladata::internal
