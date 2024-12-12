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
#include <type_traits>
#include <utility>

#include "absl/base/dynamic_annotations.h"
#include "absl/container/fixed_array.h"
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
using ::arolla::bitmap::Bitmap;
using ::arolla::bitmap::Word;

// Filters and remaps bitmap by given array of ObjectId.
// The result is
//   [presence[objs[i].Offset()] if objs[i] else 0 for i in range(objs.size())]
// If CheckAllocId=false then all ObjectId must be from given allocation.
// If CheckAllocId=true then objects from other allocations will be considered
// missing.
// CallbackFn is called for every present bit in the result. Arguments are
// indices in the result and in the source bitmap.
template <bool CheckAllocId, class CallbackFn>
Bitmap BitmapByObjOffsets(const Bitmap& presence, const ObjectIdArray& objects,
                          AllocationId obj_allocation_id, CallbackFn&& fn) {
  AlmostFullBuilder bitmap_builder(objects.size());
  objects.ForEach([&](int64_t id, bool present, ObjectId obj) {
    bool res_present = false;
    int64_t offset = obj.Offset();
    if constexpr (CheckAllocId) {
      if (present && obj_allocation_id.Contains(obj)) {
        res_present = arolla::bitmap::GetBit(presence, offset);
      }
    } else if (present) {
      DCHECK(obj_allocation_id.Contains(obj));
      res_present = arolla::bitmap::GetBit(presence, offset);
    }
    if (res_present) {
      fn(id, offset);
    } else {
      bitmap_builder.AddMissed(id);
    }
  });
  return std::move(bitmap_builder).Build();
}

template <bool CheckAllocId, class T>
arolla::DenseArray<T> GetByObjOffsets(const DenseArray<T>& data,
                                      const ObjectIdArray& objects,
                                      AllocationId obj_allocation_id) {
  if constexpr (std::is_same_v<arolla::view_type_t<T>, absl::string_view>) {
    arolla::StringsBuffer::ReshuffleBuilder values_builder(
        objects.size(), data.values, std::nullopt);
    auto bitmap = BitmapByObjOffsets<CheckAllocId>(
        data.bitmap, objects, obj_allocation_id,
        [&](int64_t id, int64_t offset) {
          values_builder.CopyValue(id, offset);
        });
    return DenseArray<T>{std::move(values_builder).Build(), std::move(bitmap)};
  } else if constexpr (std::is_same_v<T, Unit>) {
    return DenseArray<Unit>{arolla::VoidBuffer(objects.size()),
                            BitmapByObjOffsets<CheckAllocId>(
                                data.bitmap, objects, obj_allocation_id,
                                [&](int64_t id, int64_t offset) {})};
  } else {
    typename Buffer<T>::Builder values_builder(objects.size());
    const T* values = data.values.span().data();
    auto bitmap = BitmapByObjOffsets<CheckAllocId>(
        data.bitmap, objects, obj_allocation_id,
        [&](int64_t id, int64_t offset) {
          values_builder.Set(id, values[offset]);
        });
    return DenseArray<T>{std::move(values_builder).Build(), std::move(bitmap)};
  }
}

// ValueArray for T != Unit.
template <typename T>
class SimpleValueArray {
 public:
  using base_type = T;

  explicit SimpleValueArray(const DenseArray<T>& data)
      : presence_(arolla::bitmap::BitmapSize(data.size())),
        values_(new T[data.size()], data.size()) {
    if (data.bitmap.empty()) {
      std::fill(presence_.begin(), presence_.end(), arolla::bitmap::kFullWord);
    } else {
      DCHECK_EQ(data.bitmap_bit_offset, 0);
      std::copy(data.bitmap.begin(), data.bitmap.end(), presence_.begin());
    }
    if constexpr (std::is_same_v<arolla::view_type_t<T>, T>) {
      std::copy(data.values.begin(), data.values.end(), values_.begin());
    } else {
      for (int64_t i = 0; i < data.size(); ++i) {
        values_[i] = T(data.values[i]);
      }
    }
  }

  explicit SimpleValueArray(size_t size)
      : presence_(arolla::bitmap::BitmapSize(size)),
        values_(new T[size], size) {
    std::memset(presence_.data(), 0, presence_.size() * sizeof(Word));
    // For some types (e.g. bool) not all values are valid. In such
    // cases we have to initialize the memory to avoid undefined behavior.
    if constexpr (std::is_enum_v<T> || std::is_same_v<T, bool>) {
      std::memset(values_.data(), 0, size * sizeof(T));
    } else {
      ABSL_ANNOTATE_MEMORY_IS_INITIALIZED(values_.data(), size * sizeof(T));
    }
  }

  SimpleValueArray(const SimpleValueArray&) = delete;
  SimpleValueArray(SimpleValueArray&& other)
      : presence_(std::move(other.presence_)), values_(other.values_) {
    other.values_ = absl::Span<T>(nullptr, 0);
  }

  ~SimpleValueArray() {
    delete[] values_.data();
  }

  size_t size() const { return values_.size(); }

  arolla::OptionalValue<arolla::view_type_t<T>> Get(int64_t offset) const {
    return {arolla::bitmap::GetBit(presence_.data(), offset), values_[offset]};
  }

  template <bool CheckAllocId>
  DenseArray<T> Get(const ObjectIdArray& objects,
                    AllocationId obj_allocation_id) const {
    typename Buffer<T>::Builder values_builder(objects.size());
    auto bitmap = BitmapByObjOffsets<CheckAllocId>(
        Bitmap(nullptr, presence_), objects, obj_allocation_id,
        [&](int64_t id, int64_t offset) {
          values_builder.Set(id, values_[offset]);
        });
    return DenseArray<T>{std::move(values_builder).Build(), std::move(bitmap)};
  }

  DenseArray<T> GetAll() const {
    if constexpr (std::is_same_v<arolla::view_type_t<T>, absl::string_view>) {
      return DenseArray<T>{Buffer<T>::Create(values_.begin(), values_.end()),
                           Buffer<Word>(nullptr, presence_)};
    } else {
      return {Buffer<T>(nullptr, values_), Buffer<Word>(nullptr, presence_)};
    }
  }

  void Set(size_t offset, arolla::view_type_t<T> value) {
    arolla::bitmap::SetBit(presence_.data(), offset);
    values_[offset] = value;
  }

  void Unset(size_t offset) {
    arolla::bitmap::UnsetBit(presence_.data(), offset);
    if constexpr (!std::is_same_v<arolla::view_type_t<T>, T>) {
      // Needed to free memory
      values_[offset] = T();
    }
  }

  void MergeOverwrite(const DenseArray<T>& vals) {
    vals.ForEachPresent([&](int64_t offset, arolla::view_type_t<T> v) {
      values_[offset] = v;
    });
    UpdatePresenceOr(vals);
  }

  void MergeKeepOriginal(const DenseArray<T>& vals) {
    vals.ForEachPresent([&](int64_t offset, arolla::view_type_t<T> v) {
      if (!arolla::bitmap::GetBit(presence_.data(), offset)) {
        values_[offset] = v;
      }
    });
    UpdatePresenceOr(vals);
  }

  template <class ConflictFn>
  absl::Status MergeRaiseOnConflict(const DenseArray<T>& vals,
                                    ConflictFn&& conflict) {
    absl::Status status = absl::OkStatus();
    vals.ForEachPresent([&](int64_t offset, arolla::view_type_t<T> v) {
      if (!arolla::bitmap::GetBit(presence_.data(), offset)) {
        values_[offset] = v;
      } else if (values_[offset] != v) {
        conflict(status, values_[offset], v);
      }
    });
    UpdatePresenceOr(vals);
    return status;
  }

  SimpleValueArray<T> Copy() const {
    SimpleValueArray<T> res(size());
    std::copy(presence_.begin(), presence_.end(), res.presence_.begin());
    std::copy(values_.begin(), values_.end(), res.values_.begin());
    return res;
  }

  // Applies bitwise or to the arrays's presence and the given bitmap.
  // Stores result back to the `bitmap` argument.
  // ValueArray must be mutable.
  void ReadBitmapOr(Word* bitmap) const {
    for (size_t i = 0; i < presence_.size(); ++i) {
      bitmap[i] |= presence_[i];
    }
  }

 private:
  // Updates presence bitmap of the array with new values computed from `vals`.
  void UpdatePresenceOr(const DenseArray<T>& vals) {
    if (vals.bitmap.empty()) {
      std::fill(presence_.begin(), presence_.end(), arolla::bitmap::kFullWord);
    } else {
      DCHECK_EQ(vals.bitmap_bit_offset, 0);
      for (size_t i = 0; i < presence_.size(); ++i) {
        presence_[i] |= arolla::bitmap::GetWord(vals.bitmap, i);
      }
    }
  }

  absl::FixedArray<Word> presence_;
  absl::Span<T> values_;
};

// ValueArray for Unit values.
class MaskValueArray {
 public:
  using base_type = Unit;

  explicit MaskValueArray(const DenseArray<Unit>& data)
      : mutable_presence_(static_cast<Word*>(
            calloc(arolla::bitmap::BitmapSize(data.size()), sizeof(Word)))),
        data_({data.values,
               Buffer<Word>(nullptr,
                            absl::Span<Word>(
                                mutable_presence_,
                                arolla::bitmap::BitmapSize(data.size())))}) {
    if (data.bitmap.empty()) {
      std::fill(mutable_presence_, mutable_presence_ + data_.bitmap.size(),
                arolla::bitmap::kFullWord);
    } else {
      DCHECK_EQ(data.bitmap_bit_offset, 0);
      std::copy(data.bitmap.begin(), data.bitmap.end(), mutable_presence_);
    }
  }

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

  ~MaskValueArray() { free(mutable_presence_); }

  size_t size() const { return data_.size(); }

  arolla::OptionalValue<Unit> Get(int64_t offset) const {
    return data_[offset];
  }

  template <bool CheckAllocId>
  DenseArray<Unit> Get(const ObjectIdArray& objects,
                       AllocationId obj_allocation_id) const {
    return GetByObjOffsets<CheckAllocId>(data_, objects, obj_allocation_id);
  }

  const DenseArray<Unit>& GetAll() const { return data_; }

  void Set(size_t offset, Unit value) {
    arolla::bitmap::SetBit(mutable_presence_, offset);
  }

  void Unset(size_t offset) {
    arolla::bitmap::UnsetBit(mutable_presence_, offset);
  }

  void MergeOverwrite(const DenseArray<Unit>& vals) { MergeKeepOriginal(vals); }

  void MergeKeepOriginal(const DenseArray<Unit>& vals) {
    vals.ForEachPresent([&](int64_t offset, Unit) {
      arolla::bitmap::SetBit(mutable_presence_, offset);
    });
  }

  template <class ConflictFn>
  absl::Status MergeRaiseOnConflict(const DenseArray<Unit>& vals,
                                    ConflictFn&&) {
    MergeKeepOriginal(vals);
    return absl::OkStatus();
  }

  MaskValueArray Copy() const { return MaskValueArray(data_); }

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

}  // namespace value_array_impl

// ValueArray is used as an internal storage in MultitypeDenseSource and
// TypedDenseSource.
template <class T>
using ValueArray = std::conditional_t<std::is_same_v<T, arolla::Unit>,
                                      value_array_impl::MaskValueArray,
                                      value_array_impl::SimpleValueArray<T>>;

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_VALUE_ARRAY_H_
