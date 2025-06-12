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
#ifndef KOLADATA_INTERNAL_DATA_LIST_H_
#define KOLADATA_INTERNAL_DATA_LIST_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/quote.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/iterator.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal {

// Represents mutable extensible list of values. Can contain values of different
// types at the same time.
class DataList {
 public:
  DataList() = default;

  // `T` can be either std::vector of optionals of supported types (see types
  // listed in std::variant below, should match types listed in types.h),
  // or std::vector<DataItem>.
  template <typename T>
  explicit DataList(T data) : size_(data.size()), data_(std::move(data)) {}

  explicit DataList(DataSliceImpl data_slice, int64_t from = 0,
                    int64_t to = -1);

  template <typename T>
  explicit DataList(const arolla::DenseArray<T>& array, int64_t from = 0,
                    int64_t to = -1) {
    if (to == -1) {
      to = array.size();
    }
    DCHECK_LE(0, from);
    DCHECK_LE(from, to);
    DCHECK_LE(to, array.size());
    size_ = to - from;
    std::vector<std::optional<T>> data(size_);
    for (int64_t i = 0; i < size_; ++i) {
      auto v = array[from + i];
      if (v.present) {
        data[i] = T(v.value);
      }
    }
    data_ = std::move(data);
  }

  size_t size() const { return size_; }
  bool empty() const { return size() == 0; }

  // Adds value from this list (sliced from `from` to `to`, full list
  // by default) to the data slice builder starting from `offset`.
  // I.e. the value `list[from + i]` will go to `bldr[offset + i]`.
  void AddToDataSlice(SliceBuilder& bldr, int64_t offset,
                      int64_t from = 0, int64_t to = -1) const;

  DataItem Get(int64_t index) const;

  // Removes `count` elements starting from index `from`.
  void Remove(int64_t from, int64_t count);

  // Inserts `count` empty elements starting from index `from`.
  void InsertMissing(int64_t from, int64_t count);

  // If size in increased new elements are initialized to empty (nullopt).
  void Resize(size_t size);

  // `T` can be one of supported types, std::optional of a supported type, or
  // MissingValue.
  template <typename T>
  void Set(int64_t index, T value) {
    DCHECK(0 <= index && index < size_);
    if constexpr (std::is_same_v<T, MissingValue>) {
      SetToMissing(index);
    } else {
      auto fn = [&](auto& vec) {
        if constexpr (!std::is_same_v<T, DataItem> &&
                      std::is_same_v<decltype(vec), std::vector<DataItem>&>) {
          vec[index] = DataItem(std::move(value));
        } else {
          vec[index] = std::move(value);
        }
      };
      ApplyDataItemOrT<T>(fn);
    }
  }

  void Set(int64_t index, DataItem item) {
    DCHECK(0 <= index && index < size_);
    if (std::holds_alternative<std::vector<DataItem>>(data_)) {
      std::get<std::vector<DataItem>>(data_)[index] = std::move(item);
    } else {
      item.VisitValue([&]<typename T>(const T&) {
        Set(index, std::move(item).MoveValue<T>());
      });
    }
  }

  // Set to given values `data.size()` items starting from `start_index`.
  template <typename T>
  void SetN(int64_t start_index, const arolla::DenseArray<T>& data) {
    ApplyDataItemOrT<T>([&](auto& vec) {
      auto* dst = vec.data() + start_index;
      using VT = std::decay_t<decltype(*dst)>;
      if (data.bitmap.empty()) {
        for (auto v : data.values) {
          *dst++ = VT(T(v));
        }
      } else {
        for (auto v : data) {
          *dst++ = v.present ? VT(T(v.value)) : VT();
        }
      }
    });
  }

  // Assign nullopt to the specified index.
  void SetToMissing(int64_t index);

  // Assign nullopt to the given range.
  void SetMissingRange(int64_t index_from, int64_t index_to);

  // `T` can be one of supported types, std::optional of a supported type, or
  // MissingValue.
  template <typename T>
  void Insert(int64_t index, T value) {
    DCHECK(0 <= index && index <= size_);
    if constexpr (std::is_same_v<T, MissingValue>) {
      InsertMissing(index, 1);
    } else {
      auto fn = [&](auto& vec) {
        if constexpr (!std::is_same_v<T, DataItem> &&
                      std::is_same_v<decltype(vec), std::vector<DataItem>&>) {
          vec.insert(vec.begin() + index, DataItem(std::move(value)));
        } else {
          vec.insert(vec.begin() + index, std::move(value));
        }
      };
      ApplyDataItemOrT<T>(fn);
    }
  }

  void Insert(int64_t index, DataItem item) {
    DCHECK(0 <= index && index <= size_);
    if (std::holds_alternative<std::vector<DataItem>>(data_)) {
      std::vector<DataItem>& data = std::get<std::vector<DataItem>>(data_);
      data.insert(data.begin() + index, std::move(item));
      size_ = data.size();
    } else {
      item.VisitValue([&]<typename T>(const T&) {
        Insert(index, std::move(item).MoveValue<T>());
      });
    }
  }

  // Iterators support
  using value_type = DataItem;
  using size_type = size_t;
  using difference_type = int64_t;

  DataItem operator[](int64_t index) const { return Get(index); }

  arolla::ConstArrayIterator<DataList> begin() const {
    return arolla::ConstArrayIterator<DataList>(this, 0);
  }
  arolla::ConstArrayIterator<DataList> end() const {
    return arolla::ConstArrayIterator<DataList>(this, size());
  }

 private:
  void ConvertToDataItems();

  // Calls either `fn(vector<optional<T>>&)` (if the list contains only values
  // of this type) or `fn(vector<DataItem>&)` (otherwise) and uodates `size_`
  // after that. Can trigger conversion of `data_` from
  // `vector<optional<X>>` to `vector<DataItem>`.
  template <typename T, typename Fn>
  void ApplyDataItemOrT(Fn&& fn) {
    using BaseT = arolla::meta::strip_template_t<
        std::optional,
        arolla::meta::strip_template_t<arolla::OptionalValue, T>>;
    using OptT = std::optional<BaseT>;
    if constexpr (!std::is_same_v<T, DataItem>) {
      if (size_ == 0 || std::holds_alternative<AllMissing>(data_)) {
        std::vector<OptT> data(size_);
        fn(data);
        size_ = data.size();
        data_ = std::move(data);
        return;
      } else if (std::holds_alternative<std::vector<OptT>>(data_)) {
        std::vector<OptT>& data = std::get<std::vector<OptT>>(data_);
        fn(data);
        size_ = data.size();
        return;
      }
    }
    if (!std::holds_alternative<std::vector<DataItem>>(data_)) {
      ConvertToDataItems();
    }
    std::vector<DataItem>& data = std::get<std::vector<DataItem>>(data_);
    fn(data);
    size_ = data.size();
  }

  size_t size_ = 0;
  struct AllMissing {};
  std::variant<AllMissing,                                           //
               std::vector<std::optional<ObjectId>>,                 //
               std::vector<std::optional<int32_t>>,                  //
               std::vector<std::optional<int64_t>>,                  //
               std::vector<std::optional<float>>,                    //
               std::vector<std::optional<double>>,                   //
               std::vector<std::optional<bool>>,                     //
               std::vector<std::optional<arolla::Unit>>,             //
               std::vector<std::optional<arolla::Text>>,             //
               std::vector<std::optional<arolla::Bytes>>,            //
               std::vector<std::optional<arolla::expr::ExprQuote>>,  //
               std::vector<std::optional<schema::DType>>,            //
               std::vector<DataItem>>
      data_;
};

// Vector of DataLists. Can be created from shared_ptr to another DataListVector
// and copies content lazily.
class DataListVector {
 public:
  explicit DataListVector(size_t size) : data_(size) {
    for (ListAndPtr& lp : data_) {
      lp.ptr = nullptr;
    }
  }

  explicit DataListVector(std::shared_ptr<const DataListVector> parent)
      : data_(parent->size()), parent_(std::move(parent)) {
    for (size_t i = 0; i < data_.size(); ++i) {
      // raw pointer is safe since `parent_` holds ownership.
      data_[i].ptr = parent_->Get(i);
    }
  }

  DataListVector(const DataListVector&) = delete;
  DataListVector(DataListVector&&) = delete;
  void operator=(const DataListVector&) = delete;
  void operator=(DataListVector&&) = delete;

  size_t size() const { return data_.size(); }

  const DataList* /*absl_nullable*/ Get(size_t index) const {
    DCHECK_LT(index, size());
    return data_[index].ptr;
  }

  DataList& GetMutable(size_t index) {
    DCHECK_LT(index, size());
    ListAndPtr& lp = data_[index];
    if (lp.ptr == nullptr) {
      lp.ptr = &lp.list;
    } else if (!lp.IsMutable()) {
      lp.list = *lp.ptr;
      lp.ptr = &lp.list;
    }
    return lp.list;
  }

 private:
  struct ListAndPtr {
    // ptr_ links either to list_ (mutable) or to a list owned by parent_
    // (immutable since we are not allowed to change parent), or to nullptr
    // (unset).
    bool IsMutable() const { return ptr == &list; }

    DataList list;
    const DataList* ptr;
  };

  std::vector<ListAndPtr> data_;
  std::shared_ptr<const DataListVector> parent_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_DATA_LIST_H_
