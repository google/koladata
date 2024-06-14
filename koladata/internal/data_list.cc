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
#include "koladata/internal/data_list.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/log/check.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "arolla/util/meta.h"

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

void DataList::AddToDataSlice(DataSliceImpl::Builder& bldr, int64_t offset,
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
            bldr.Insert(offset, vec[i]);
          }
        } else if constexpr (!std::is_same_v<decltype(vec),
                                             const AllMissing&>) {
          using T = arolla::meta::strip_template_t<
              std::optional, typename std::decay_t<decltype(vec)>::value_type>;
          auto& arr_bldr = bldr.GetArrayBuilder<T>();
          for (int64_t i = from; i < to; ++i, ++offset) {
            const auto& opt_value = vec[i];
            if (!opt_value.has_value()) {
              continue;
            }
            arr_bldr.Set(offset, *opt_value);
            if constexpr (std::is_same_v<T, ObjectId>) {
              bldr.GetMutableAllocationIds().Insert(AllocationId(*opt_value));
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

}  // namespace koladata::internal
