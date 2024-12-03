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
#ifndef KOLADATA_INTERNAL_VALUE_ARRAY_H_
#define KOLADATA_INTERNAL_VALUE_ARRAY_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/dynamic_annotations.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"

// It is a private header, part of dense_source implementation.

namespace koladata::internal {
namespace value_array_impl {

using ::arolla::Buffer;
using ::arolla::DenseArray;
using ::arolla::Unit;
using ::arolla::bitmap::AlmostFullBuilder;
using ::arolla::bitmap::Word;

// ValueArray for value with `view_type_t<T>` same as `T` (currently everything
// except Text/Bytes), T != Unit.
template <typename T>
class SimpleValueArray {
 public:
  using base_type = T;

  static_assert(std::is_same_v<T, arolla::view_type_t<T>>);

  explicit SimpleValueArray(DenseArray<T> data) : data_(std::move(data)) {}
  explicit SimpleValueArray(size_t size)
      : mutable_presence_(static_cast<Word*>(
            calloc(arolla::bitmap::BitmapSize(size), sizeof(Word)))),
        mutable_values_(new T[size]),
        data_({Buffer<T>(nullptr, absl::Span<T>(mutable_values_, size)),
               Buffer<Word>(nullptr, absl::Span<Word>(
                                         mutable_presence_,
                                         arolla::bitmap::BitmapSize(size)))}) {
    // For some types (e.g. bool) not all values are valid. In such
    // cases we have to initialize the memory to avoid undefined behavior.
    if constexpr (std::is_enum_v<T> || std::is_same_v<T, bool>) {
      std::memset(mutable_values_, 0, size * sizeof(T));
    } else {
      ABSL_ANNOTATE_MEMORY_IS_INITIALIZED(mutable_values_, size * sizeof(T));
    }
  }

  SimpleValueArray(const SimpleValueArray&) = delete;
  SimpleValueArray(SimpleValueArray&& other)
      : mutable_presence_(other.mutable_presence_),
        mutable_values_(other.mutable_values_),
        data_(std::move(other.data_)) {
    other.mutable_values_ = nullptr;
    other.mutable_presence_ = nullptr;
  }

  ~SimpleValueArray() {
    if (IsMutable()) {
      delete[] mutable_values_;
      free(mutable_presence_);
    }
  }

  size_t size() const { return data_.size(); }
  bool IsMutable() const { return mutable_values_ != nullptr; }

  arolla::OptionalValue<T> Get(int64_t offset) const {
    return data_[offset];
  }

  template <bool CheckAllocId>
  DenseArray<T> Get(const ObjectIdArray& objects,
                    AllocationId obj_allocation_id) const {
    AlmostFullBuilder bitmap_builder(objects.size());
    typename Buffer<T>::Builder values_builder(objects.size());
    const T* values = data_.values.span().data();

    objects.ForEach([&](int64_t id, bool present, ObjectId obj) {
      bool res_present = false;
      int64_t offset = obj.Offset();
      if constexpr (CheckAllocId) {
        if (present && obj_allocation_id.Contains(obj)) {
          res_present = data_.present(offset);
          values_builder.Set(id, values[offset]);
        }
      } else if (present) {
        DCHECK(obj_allocation_id.Contains(obj));
        res_present = data_.present(offset);
        values_builder.Set(id, values[offset]);
      }
      if (!res_present) {
        bitmap_builder.AddMissed(id);
      }
    });
    return DenseArray<T>{std::move(values_builder).Build(),
                         std::move(bitmap_builder).Build()};
  }

  const DenseArray<T>& GetAll() const { return data_; }

  void Set(size_t offset, T value) {
    DCHECK(IsMutable());
    arolla::bitmap::SetBit(mutable_presence_, offset);
    mutable_values_[offset] = value;
  }

  void Unset(size_t offset) {
    DCHECK(IsMutable());
    arolla::bitmap::UnsetBit(mutable_presence_, offset);
  }

  void MergeOverwrite(const DenseArray<T>& vals) {
    DCHECK(IsMutable());
    vals.ForEachPresent([&](int64_t offset, arolla::view_type_t<T> v) {
      mutable_values_[offset] = v;
    });
    UpdatePresenceOr(vals);
  }

  void MergeKeepOriginal(const DenseArray<T>& vals) {
    DCHECK(IsMutable());
    vals.ForEachPresent([&](int64_t offset, arolla::view_type_t<T> v) {
      if (!arolla::bitmap::GetBit(mutable_presence_, offset)) {
        mutable_values_[offset] = v;
      }
    });
    UpdatePresenceOr(vals);
  }

  template <class ConflictFn>
  absl::Status MergeRaiseOnConflict(const DenseArray<T>& vals,
                                    ConflictFn&& conflict) {
    DCHECK(IsMutable());
    absl::Status status = absl::OkStatus();
    vals.ForEachPresent([&](int64_t offset, arolla::view_type_t<T> v) {
      if (!arolla::bitmap::GetBit(mutable_presence_, offset)) {
        mutable_values_[offset] = v;
      } else if (mutable_values_[offset] != v) {
        conflict(status, mutable_values_[offset], v);
      }
    });
    UpdatePresenceOr(vals);
    return status;
  }

  SimpleValueArray<T> CreateMutableCopy() const {
    SimpleValueArray<T> res(data_.size());
    if (data_.bitmap.empty()) {
      std::fill(res.mutable_presence_,
                res.mutable_presence_ + res.data_.bitmap.size(),
                arolla::bitmap::kFullWord);
    } else if (data_.bitmap_bit_offset == 0) {
      std::copy(data_.bitmap.begin(), data_.bitmap.end(),
                res.mutable_presence_);
    } else {
      for (int64_t i = 0; i < res.data_.bitmap.size(); ++i) {
        res.mutable_presence_[i] = arolla::bitmap::GetWordWithOffset(
            data_.bitmap, i, data_.bitmap_bit_offset);
      }
    }
    std::copy(data_.values.begin(), data_.values.end(), res.mutable_values_);
    return res;
  }

  // Applies bitwise or to the arrays's presence and the given bitmap.
  // Stores result back to the `bitmap` argument.
  // ValueArray must be mutable.
  void ReadBitmapOr(Word* bitmap) const {
    DCHECK(mutable_presence_);
    size_t size = arolla::bitmap::BitmapSize(data_.size());
    for (size_t i = 0; i < size; ++i) {
      bitmap[i] |= mutable_presence_[i];
    }
  }

 private:
  // Updates presence bitmap of the array with new values computed from `vals`.
  void UpdatePresenceOr(const DenseArray<T>& vals) {
    size_t bitmap_size = arolla::bitmap::BitmapSize(size());
    if (vals.bitmap.empty()) {
      std::fill(mutable_presence_, mutable_presence_ + bitmap_size,
                arolla::bitmap::kFullWord);
    } else {
      for (size_t i = 0; i < bitmap_size; ++i) {
        mutable_presence_[i] |= arolla::bitmap::GetWordWithOffset(
            vals.bitmap, i, vals.bitmap_bit_offset);
      }
    }
  }

  Word* mutable_presence_ = nullptr;
  T* mutable_values_ = nullptr;
  DenseArray<T> data_;
};

// ValueArray for Unit values.
class MaskValueArray {
 public:
  using base_type = Unit;

  explicit MaskValueArray(DenseArray<Unit> data) : data_(std::move(data)) {}
  explicit MaskValueArray(size_t size)
      : mutable_presence_(static_cast<Word*>(
            calloc(arolla::bitmap::BitmapSize(size), sizeof(Word)))),
        data_({Buffer<Unit>(size),
               Buffer<Word>(nullptr, absl::Span<Word>(
                                         mutable_presence_,
                                         arolla::bitmap::BitmapSize(size)))}) {}

  MaskValueArray(const MaskValueArray&) = delete;
  MaskValueArray(MaskValueArray&& other)
      : mutable_presence_(other.mutable_presence_),
        data_(std::move(other.data_)) {
    other.mutable_presence_ = nullptr;
  }

  ~MaskValueArray() {
    if (IsMutable()) {
      free(mutable_presence_);
    }
  }

  size_t size() const { return data_.size(); }
  bool IsMutable() const { return mutable_presence_ != nullptr; }

  arolla::OptionalValue<Unit> Get(int64_t offset) const {
    return data_[offset];
  }

  template <bool CheckAllocId>
  DenseArray<Unit> Get(const ObjectIdArray& objects,
                       AllocationId obj_allocation_id) const {
    AlmostFullBuilder bitmap_builder(objects.size());
    objects.ForEach([&](int64_t id, bool present, ObjectId obj) {
      int64_t offset = obj.Offset();
      if constexpr (CheckAllocId) {
        if (!present || !obj_allocation_id.Contains(obj) ||
            !data_.present(offset)) {
          bitmap_builder.AddMissed(id);
        }
      } else {
        DCHECK(!present || obj_allocation_id.Contains(obj));
        if (!present || !data_.present(offset)) {
          bitmap_builder.AddMissed(id);
        }
      }
    });
    return DenseArray<Unit>{
      std::move(typename Buffer<Unit>::Builder(objects.size())).Build(),
      std::move(bitmap_builder).Build()};
  }

  const DenseArray<Unit>& GetAll() const { return data_; }

  void Set(size_t offset, Unit value) {
    DCHECK(IsMutable());
    arolla::bitmap::SetBit(mutable_presence_, offset);
  }

  void Unset(size_t offset) {
    DCHECK(IsMutable());
    arolla::bitmap::UnsetBit(mutable_presence_, offset);
  }

  void MergeOverwrite(const DenseArray<Unit>& vals) {
    DCHECK(IsMutable());
    MergeKeepOriginal(vals); }

  void MergeKeepOriginal(const DenseArray<Unit>& vals) {
    DCHECK(IsMutable());
    vals.ForEachPresent([&](int64_t offset, Unit) {
      arolla::bitmap::SetBit(mutable_presence_, offset);
    });
  }

  template <class ConflictFn>
  absl::Status MergeRaiseOnConflict(const DenseArray<Unit>& vals,
                                    ConflictFn&&) {
    DCHECK(IsMutable());
    MergeKeepOriginal(vals);
    return absl::OkStatus();
  }

  MaskValueArray CreateMutableCopy() const {
    MaskValueArray res(data_.size());
    if (data_.bitmap.empty()) {
      std::fill(res.mutable_presence_,
                res.mutable_presence_ + res.data_.bitmap.size(),
                arolla::bitmap::kFullWord);
    } else if (data_.bitmap_bit_offset == 0) {
      std::copy(data_.bitmap.begin(), data_.bitmap.end(),
                res.mutable_presence_);
    } else {
      for (int64_t i = 0; i < res.data_.bitmap.size(); ++i) {
        res.mutable_presence_[i] = arolla::bitmap::GetWordWithOffset(
            data_.bitmap, i, data_.bitmap_bit_offset);
      }
    }
    return res;
  }

  // Applies bitwise or to the arrays's presence and the given bitmap.
  // Stores result back to the `bitmap` argument.
  // ValueArray must be mutable.
  void ReadBitmapOr(Word* bitmap) const {
    DCHECK(mutable_presence_);
    size_t size = arolla::bitmap::BitmapSize(data_.size());
    for (size_t i = 0; i < size; ++i) {
      bitmap[i] |= mutable_presence_[i];
    }
  }

 private:
  Word* mutable_presence_ = nullptr;
  DenseArray<Unit> data_;
};

template <typename T>
class MutableStringArray {
 public:
  using base_type = T;

  static_assert(std::is_same_v<absl::string_view, arolla::view_type_t<T>>);

  explicit MutableStringArray(DenseArray<T> data) = delete;
  explicit MutableStringArray(size_t size) { data_.resize(size); }

  size_t size() const { return data_.size(); }
  bool IsMutable() const { return true; }

  arolla::OptionalValue<absl::string_view> Get(int64_t offset) const {
    const std::optional<std::string>& v = data_[offset];
    if (v) {
      return absl::string_view(*v);
    } else {
      return std::nullopt;
    }
  }

  template <bool CheckAllocId>
  DenseArray<T> Get(const ObjectIdArray& objects,
                    AllocationId obj_allocation_id) const {
    AlmostFullBuilder bitmap_builder(objects.size());
    arolla::StringsBuffer::Builder values_builder(objects.size());

    objects.ForEach([&](int64_t id, bool present, ObjectId obj) {
      bool res_present = false;
      if constexpr (CheckAllocId) {
        if (present && obj_allocation_id.Contains(obj)) {
          const std::optional<std::string>& v = data_[obj.Offset()];
          res_present = v.has_value();
          if (res_present) {
            values_builder.Set(id, *v);
          }
        }
      } else {
        if (present) {
          DCHECK(obj_allocation_id.Contains(obj));
          const std::optional<std::string>& v = data_[obj.Offset()];
          res_present = v.has_value();
          if (res_present) {
            values_builder.Set(id, *v);
          }
        }
      }
      if (!res_present) {
        bitmap_builder.AddMissed(id);
      }
    });
    return DenseArray<T>{std::move(values_builder).Build(),
                         std::move(bitmap_builder).Build()};
  }

  DenseArray<T> GetAll() const {
    AlmostFullBuilder bitmap_builder(data_.size());
    arolla::StringsBuffer::Builder values_builder(data_.size());
    int64_t offset = 0;
    for (const std::optional<std::string>& v : data_) {
      if (v) {
        values_builder.Set(offset, *v);
      } else {
        bitmap_builder.AddMissed(offset);
      }
      offset++;
    }
    return DenseArray<T>{std::move(values_builder).Build(),
                         std::move(bitmap_builder).Build()};
  }

  void Set(size_t offset, absl::string_view value) {
    data_[offset].emplace(value);
  }
  void Unset(size_t offset) { data_[offset] = std::nullopt; }

  void MergeOverwrite(const DenseArray<T>& vals) {
    vals.ForEachPresent([&](int64_t offset, arolla::view_type_t<T> value) {
      Set(offset, value);
    });
  }

  void MergeKeepOriginal(const DenseArray<T>& vals) {
    vals.ForEachPresent([&](int64_t offset, arolla::view_type_t<T> v) {
      auto& dst = data_[offset];
      if (!dst.has_value()) {
        dst.emplace(v);
      }
    });
  }

  template <class ConflictFn>
  absl::Status MergeRaiseOnConflict(const DenseArray<T>& vals,
                                    ConflictFn&& conflict) {
    absl::Status status = absl::OkStatus();
    vals.ForEachPresent([&](int64_t offset, arolla::view_type_t<T> v) {
      auto& dst = data_[offset];
      if (!dst.has_value()) {
        dst.emplace(v);
      } else if (*dst != v) {
        conflict(status, *dst, v);
      }
    });
    return status;
  }

  MutableStringArray<T> CreateMutableCopy() const { return *this; }

  // Applies bitwise or to the arrays's presence and the given bitmap.
  // Stores result back to the `bitmap` argument.
  void ReadBitmapOr(Word* bitmap) const {
    for (size_t i = 0; i < data_.size(); ++i) {
      if (data_[i]) {
        arolla::bitmap::SetBit(bitmap, i);
      }
    }
  }

 private:
  std::vector<std::optional<std::string>> data_;
};

template <typename T>
class ImmutableStringArray {
 public:
  using base_type = T;

  static_assert(std::is_same_v<absl::string_view, arolla::view_type_t<T>>);

  explicit ImmutableStringArray(DenseArray<T> data) : data_(std::move(data)) {}
  explicit ImmutableStringArray(size_t size) = delete;

  size_t size() const { return data_.size(); }
  bool IsMutable() const { return false; }

  arolla::OptionalValue<absl::string_view> Get(int64_t offset) const {
    return data_[offset];
  }

  template <bool CheckAllocId>
  DenseArray<T> Get(const ObjectIdArray& objects,
                    AllocationId obj_allocation_id) const {
    AlmostFullBuilder bitmap_builder(objects.size());
    arolla::StringsBuffer::ReshuffleBuilder values_builder(
        objects.size(), data_.values, std::nullopt);

    objects.ForEach([&](int64_t id, bool present, ObjectId obj) {
      bool res_present = false;
      int64_t offset = obj.Offset();
      if constexpr (CheckAllocId) {
        if (present && obj_allocation_id.Contains(obj)) {
          res_present = data_.present(offset);
          values_builder.CopyValue(id, offset);
        }
      } else if (present) {
        DCHECK(obj_allocation_id.Contains(obj));
        res_present = data_.present(offset);
        values_builder.CopyValue(id, offset);
      }
      if (!res_present) {
        bitmap_builder.AddMissed(id);
      }
    });
    return DenseArray<T>{std::move(values_builder).Build(),
                         std::move(bitmap_builder).Build()};
  }

  const DenseArray<T>& GetAll() const { return data_; }

  void Set(size_t offset, absl::string_view value) {
    LOG(FATAL) << "ImmutableStringArray::Set is not allowed";
  }
  void Unset(size_t offset) {
    LOG(FATAL) << "ImmutableStringArray::Unset is not allowed";
  }
  void MergeOverwrite(const DenseArray<T>& vals) {
    LOG(FATAL) << "ImmutableStringArray::MergeOverwrite is not allowed";
  }
  void MergeKeepOriginal(const DenseArray<T>& vals) {
    LOG(FATAL) << "ImmutableStringArray::MergeKeepOriginal is not allowed";
  }
  template <class ConflictFn>
  absl::Status MergeRaiseOnConflict(const DenseArray<T>& vals, ConflictFn&&) {
    return absl::FailedPreconditionError(
        "ImmutableStringArray::MergeRaiseOnConflict is not allowed");
  }

  MutableStringArray<T> CreateMutableCopy() const {
    MutableStringArray<T> res(data_.size());
    data_.ForEachPresent([&res](int64_t offset, absl::string_view value) {
      res.Set(offset, value);
    });
    return res;
  }

 private:
  DenseArray<T> data_;
};

}  // namespace value_array_impl

// ValueArray is used as an internal storage in MultitypeDenseSource and
// TypedDenseSource.
template <class T, bool can_be_mutable>
using ValueArray = std::conditional_t<
    std::is_same_v<arolla::view_type_t<T>, absl::string_view>,
    std::conditional_t<can_be_mutable, value_array_impl::MutableStringArray<T>,
                       value_array_impl::ImmutableStringArray<T>>,
    std::conditional_t<std::is_same_v<T, arolla::Unit>,
                       value_array_impl::MaskValueArray,
                       value_array_impl::SimpleValueArray<T>>>;

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_VALUE_ARRAY_H_
