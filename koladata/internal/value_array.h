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
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"
#include "koladata/internal/object_id.h"

// It is a private header, part of dense_source implementation.

namespace koladata::internal {
namespace value_array_impl {

using ::arolla::Buffer;
using ::arolla::DenseArray;
using ::arolla::Unit;
using ::arolla::bitmap::AlmostFullBuilder;
using ::arolla::bitmap::Bitmap;
using ::arolla::bitmap::Word;
using ::arolla::view_type_t;

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

template <typename T>
class ValueBuffer : public absl::Span<T> {
 public:
  explicit ValueBuffer(int64_t size) : absl::Span<T>(new T[size], size) {
    // For some types (e.g. bool) not all values are valid. In such
    // cases we have to initialize the memory to avoid undefined behavior.
    if constexpr (std::is_enum_v<T> || std::is_same_v<T, bool>) {
      std::memset(this->data(), 0, size * sizeof(T));
    } else if constexpr (!std::is_same_v<T, Unit>) {
      ABSL_ANNOTATE_MEMORY_IS_INITIALIZED(this->data(), size * sizeof(T));
    }
  }
  ValueBuffer(const ValueBuffer& other) = delete;
  ValueBuffer& operator=(const ValueBuffer&) = delete;
  ValueBuffer(ValueBuffer&& other) : absl::Span<T>(other) {
    static_cast<absl::Span<T>&>(other) = absl::Span<T>(nullptr, 0);
  }

  explicit ValueBuffer(const arolla::Buffer<T>& buf) : ValueBuffer(buf.size()) {
    if constexpr (!std::is_same_v<view_type_t<T>, T>) {
      for (int64_t i = 0; i < buf.size(); ++i) {
        (*this)[i] = buf[i];
      }
    } else {
      std::copy(buf.begin(), buf.end(), this->begin());
    }
  }

  ~ValueBuffer() { delete [] this->data(); }
};

template <>
class ValueBuffer<Unit> {
 public:
  explicit ValueBuffer(int64_t size) : size_(size) {}
  explicit ValueBuffer(const arolla::Buffer<Unit>& buf) : size_(buf.size()) {}
  int64_t size() const { return size_; }
  Unit operator[](int64_t offset) const { return Unit{}; }
 private:
  int64_t size_;
};

// ValueArray is used as an internal storage in MultitypeDenseSource and
// TypedDenseSource.
template <typename T>
class ValueArray {
 public:
  using base_type = T;

  explicit ValueArray(const DenseArray<T>& data)
      : presence_(arolla::bitmap::BitmapSize(data.size())),
        values_(data.size()) {
    if (data.bitmap.empty()) {
      std::fill(presence_.begin(), presence_.end(), arolla::bitmap::kFullWord);
    } else {
      DCHECK_EQ(data.bitmap_bit_offset, 0);
      std::copy(data.bitmap.begin(), data.bitmap.end(), presence_.begin());
    }
    if constexpr (std::is_same_v<T, Unit>) {
      // skip
    } else if constexpr (std::is_same_v<arolla::view_type_t<T>, T>) {
      std::copy(data.values.begin(), data.values.end(), values_.begin());
    } else {
      for (int64_t i = 0; i < data.size(); ++i) {
        values_[i] = T(data.values[i]);
      }
    }
  }

  explicit ValueArray(size_t size)
      : presence_(arolla::bitmap::BitmapSize(size)), values_(size) {
    std::memset(presence_.data(), 0, presence_.size() * sizeof(Word));
  }

  ValueArray(const ValueArray&) = delete;
  ValueArray& operator=(const ValueArray&) = delete;
  ValueArray(ValueArray&& other)
      : presence_(std::move(other.presence_)),
        values_(std::move(other.values_)) {}

  ValueBuffer<T> MoveValues() && { return std::move(values_); }
  absl::Span<const Word> presence() const { return presence_; }

  size_t size() const { return values_.size(); }

  arolla::OptionalValue<arolla::view_type_t<T>> Get(int64_t offset) const {
    if constexpr (std::is_same_v<T, Unit>) {
      return arolla::OptionalUnit(
          arolla::bitmap::GetBit(presence_.data(), offset));
    } else {
      return {arolla::bitmap::GetBit(presence_.data(), offset),
              values_[offset]};
    }
  }

  template <bool CheckAllocId>
  DenseArray<T> Get(const ObjectIdArray& objects,
                    AllocationId obj_allocation_id) const {
    if constexpr (std::is_same_v<T, Unit>) {
      return DenseArray<Unit>{
          arolla::VoidBuffer(objects.size()),
          BitmapByObjOffsets<CheckAllocId>(Bitmap(nullptr, presence_), objects,
                                           obj_allocation_id,
                                           [&](int64_t id, int64_t offset) {})};
    } else {
      typename Buffer<T>::Builder values_builder(objects.size());
      auto bitmap = BitmapByObjOffsets<CheckAllocId>(
          Bitmap(nullptr, presence_), objects, obj_allocation_id,
          [&](int64_t id, int64_t offset) {
            values_builder.Set(id, values_[offset]);
          });
      return DenseArray<T>{std::move(values_builder).Build(),
                           std::move(bitmap)};
    }
  }

  DenseArray<T> GetAll() const {
    if constexpr (std::is_same_v<arolla::view_type_t<T>, absl::string_view>) {
      return DenseArray<T>{Buffer<T>::Create(values_.begin(), values_.end()),
                           Buffer<Word>(nullptr, presence_)};
    } else if constexpr (std::is_same_v<T, Unit>) {
      return {arolla::VoidBuffer(size()), Buffer<Word>(nullptr, presence_)};
    } else {
      return {Buffer<T>(nullptr, values_), Buffer<Word>(nullptr, presence_)};
    }
  }

  void Set(size_t offset, arolla::view_type_t<T> value) {
    arolla::bitmap::SetBit(presence_.data(), offset);
    if constexpr (!std::is_same_v<T, Unit>) {
      values_[offset] = value;
    }
  }

  void Unset(size_t offset) {
    arolla::bitmap::UnsetBit(presence_.data(), offset);
    if constexpr (!std::is_same_v<arolla::view_type_t<T>, T>) {
      // Needed to free memory
      values_[offset] = T();
    }
  }

  ValueArray<T> Copy() const {
    ValueArray<T> res(size());
    std::copy(presence_.begin(), presence_.end(), res.presence_.begin());
    if constexpr (!std::is_same_v<T, Unit>) {
      std::copy(values_.begin(), values_.end(), res.values_.begin());
    }
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
  ValueBuffer<T> values_;
};

}  // namespace value_array_impl

using value_array_impl::ValueBuffer;
using value_array_impl::ValueArray;

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_VALUE_ARRAY_H_
