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
#ifndef KOLADATA_INTERNAL_SLICE_BUILDER_H_
#define KOLADATA_INTERNAL_SLICE_BUILDER_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <variant>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/types.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/quote.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"

namespace koladata::internal {

// Stores one byte per element of DataSliceImpl that encodes its type and
// present/missing/removed state.
// TODO: move into DataSliceImpl::TypesBuffer (or maybe to
//                    a separate file).
struct TypesBuffer {
  // Special values in id_to_typeidx.
  static constexpr uint8_t kUnset = 0xff;
  static constexpr uint8_t kRemoved = 0xfe;  // explicitly set to missing

  // Index in `types` (or kUnset/kRemoved) per element.
  arolla::Buffer<uint8_t> id_to_typeidx;

  // ScalarTypeId<T>() of used types.
  absl::InlinedVector<KodaTypeId, 4> types;

  int64_t size() const { return id_to_typeidx.size(); }
  size_t type_count() const { return types.size(); }

  KodaTypeId id_to_scalar_typeid(int64_t id) const {
    DCHECK(0 <= id && id < size());
    uint8_t index = id_to_typeidx[id];
    if (index == kRemoved || index == kUnset) {
      return ScalarTypeId<MissingValue>();  // unset or removed
    }
    DCHECK_LT(index, types.size());
    return types[index];
  }

  // Returns QType for given id. `nullptr` if not set or removed.
  absl::Nullable<const arolla::QType*> id_to_type(int64_t id) const {
    return ScalarTypeIdToQType(id_to_scalar_typeid(id));
  }

  // Creates bitmap of (id_to_typeidx[i] == type_idx) per element.
  arolla::bitmap::Bitmap ToBitmap(uint8_t type_idx) const;

  // Creates bitmap of (id_to_typeidx[i] not in [kUnset, kRemoved]) per element.
  arolla::bitmap::Bitmap ToPresenceBitmap() const;
};

// DataSliceImpl builder.
class SliceBuilder {
 public:
  explicit SliceBuilder(size_t size);

  size_t size() const { return id_to_typeidx_.size(); }
  bool is_finalized() const { return unset_count_ == 0; }

  // Returns true also if was set to anything (missing or present).
  bool IsSet(int64_t id) const {
    DCHECK(0 <= id && id < size());
    return id_to_typeidx_[id] != TypesBuffer::kUnset;
  }

  // Returns read-only TypesBuffer, but the underlying data is changing when
  // new values are inserted into the builder.
  const TypesBuffer& types_buffer() const { return types_buffer_; }

  // Sets all items that are missing in the mask to TypesBuffer::kRemoved,
  // so the builder will never assign values to them.
  void ApplyMask(const arolla::DenseArray<arolla::Unit>& mask) {
    mask.ForEach([&](int64_t id, bool present, arolla::Unit) {
      if (!present && id_to_typeidx_[id] == TypesBuffer::kUnset) {
        id_to_typeidx_[id] = TypesBuffer::kRemoved;
        unset_count_--;
      }
    });
  }

  // Set value with given id to `v`. Value can be DataItem, DataItem::View, any
  // type that can be stored in DataItem, or OptionalValue of such type. Each id
  // can be inserted only once. Does nothing if was previously called with the
  // same id (even if the value was missing).
  // AllocationIdSet (relevant only when adding ObjectId) must be updated
  // separately using GetMutableAllocationIds().
  template <typename OptionalOrT>
  void InsertIfNotSet(int64_t id, const OptionalOrT& v);
  void InsertIfNotSet(int64_t id, const DataItem& v);

  // It calls InsertIfNotSet and updates allocation ids if `v` is an ObjectId.
  void InsertIfNotSetAndUpdateAllocIds(int64_t id, const DataItem& v) {
    if (v.holds_value<ObjectId>()) {
      InsertIfNotSet(id, v.value<ObjectId>());
      GetMutableAllocationIds().Insert(AllocationId(v.value<ObjectId>()));
    } {
      InsertIfNotSet(id, v);
    }
  }

  // Batch version of InsertIfNotSet. Equivalent to:
  //   for (int i = 0; i < size; ++i) {
  //     if (GetBit(mask, i)) {
  //       InsertIfNotSet(i, OptionalValue<T>{GetBit(presence, i), values[i]});
  //     }
  //   }
  // values.size() must match this->size(). Can reuse `values` buffer if no
  // other values of this type were added to the builder.
  // AllocationIdSet (relevant only if T == ObjectId) must be updated separately
  // using GetMutableAllocationIds().
  template <typename T>
  void InsertIfNotSet(const arolla::bitmap::Bitmap& mask,
                      const arolla::bitmap::Bitmap& presence,
                      const arolla::Buffer<T>& values);

  template <typename T>
  class TypedBuilder {
   public:
    bool IsSet(int64_t id) const {
      DCHECK(0 <= id && id < id_to_typeidx_.size());
      return id_to_typeidx_[id] != TypesBuffer::kUnset;
    }

    template <typename ValueT>  // T, or optional of T, or nullopt
    void InsertIfNotSet(int64_t id, const ValueT& v);

   private:
    friend class SliceBuilder;
    explicit TypedBuilder(SliceBuilder& base)
        : id_to_typeidx_(base.id_to_typeidx_),
          bldr_(base.GetBufferBuilder<T>()),
          base_(base),
          typeidx_(base.current_typeidx_) {}

    absl::Span<uint8_t> id_to_typeidx_;
    arolla::Buffer<T>::Builder& bldr_;
    SliceBuilder& base_;
    uint8_t typeidx_;
  };

  // Returns typed view of the builder with more efficient implementation of
  // scalar InsertIfNotSet. Subsequent calls of `this->typed()` and
  // `this->InsertIfNotSet()` invalidate the view.
  template <typename T>
  TypedBuilder<T> typed() { return TypedBuilder<T>(*this); }

  AllocationIdSet& GetMutableAllocationIds() {
    return allocation_ids_;
  }

  // Usage: DataSliceImpl slice = std::move(builder).Build();
  DataSliceImpl Build() &&;

 private:
  template <typename T>
  struct TypedStorage {
    uint8_t type_index;  // index in types_buffer_.types
    std::variant<arolla::Buffer<T>, typename arolla::Buffer<T>::Builder> data;

    arolla::Buffer<T> Build() && {
      return std::visit([]<typename B>(B& b) {
        if constexpr (std::is_same_v<B, arolla::Buffer<T>>) {
          return std::move(b);
        } else {
          return std::move(b).Build();
        }
      }, data);
    }
  };
  using StorageVariant = std::variant<        //
      std::monostate,                         //
      TypedStorage<ObjectId>,                 //
      TypedStorage<int32_t>,                  //
      TypedStorage<int64_t>,                  //
      TypedStorage<float>,                    //
      TypedStorage<double>,                   //
      TypedStorage<bool>,                     //
      TypedStorage<arolla::Unit>,             //
      TypedStorage<arolla::Text>,             //
      TypedStorage<arolla::Bytes>,            //
      TypedStorage<arolla::expr::ExprQuote>,  //
      TypedStorage<schema::DType>>;

  // Moves data out of StorageVariant, builds DenseArray of the corresponding
  // type. Returns true and DataSliceImpl::Variant containing the DenseArray if
  // the data is not empty. Returns false if all the data is missing.
  std::pair<bool, DataSliceImpl::Variant> BuildDataSliceVariant(
      StorageVariant& sv);

  // Sets `current_type_id_` and makes `current_storage_` pointing to
  // StorageVariant for the given type. If it is a new type the a new
  // StorageVariant is created in std::monostate state.
  // Note: this function doesn't update `current_typeidx_` because it requires
  // extracting or initializing TypedStorage<T>::type_index that is more
  // convenient to do in the templated code that calls ChangeCurrentType.
  void ChangeCurrentType(KodaTypeId type_id);

  template <typename T>
  typename arolla::Buffer<T>::Builder& GetBufferBuilder();

  // Part of GetBufferBuilder implementation for the case
  // `ScalarTypeId<T>() != current_type_id_`. Extracted to a separate
  // non-inlinable function to improve inlining of the fast codepath in
  // `GetBufferBuilder`.
  template <typename T>
  typename arolla::Buffer<T>::Builder& GetBufferBuilderWithTypeChange();

  template <typename T>
  static bool IsMissing(const T& v) {
    if constexpr (std::is_same_v<T, MissingValue> ||
                  std::is_same_v<T, std::nullopt_t>) {
      return true;
    } else if constexpr (arolla::is_optional_v<T>) {
      return !v.present;
    } else {
      return false;
    }
  }

  // Map from ScalarTypeId<T>() to StorageVariant
  absl::flat_hash_map<KodaTypeId, StorageVariant> storage_;

  // Used to avoid having a single element in storage_.
  StorageVariant first_storage_;

  // Pointer to storage_[current_type_id_] or first_storage_.
  StorageVariant* current_storage_ = &first_storage_;
  KodaTypeId current_type_id_ = ScalarTypeId<MissingValue>();
  uint8_t current_typeidx_;

  TypesBuffer types_buffer_;

  // Mutable underlying data of types_buffer_.id_to_typeidx.
  absl::Span<uint8_t> id_to_typeidx_;

  // The number of elements where `id_to_typeidx_[i] == kUnset`. Can only be
  // decreased. When reaches zero `is_finalized()` becomes true.
  size_t unset_count_;

  AllocationIdSet allocation_ids_;
};

// *** Implementation

template <typename T>
typename arolla::Buffer<T>::Builder& SliceBuilder::GetBufferBuilder() {
  if (ScalarTypeId<T>() == current_type_id_) {
    return std::get<typename arolla::Buffer<T>::Builder>(
        std::get<TypedStorage<T>>(*current_storage_).data);
  } else {
    return GetBufferBuilderWithTypeChange<T>();
  }
}

template <typename T>
ABSL_ATTRIBUTE_NOINLINE typename arolla::Buffer<T>::Builder&
SliceBuilder::GetBufferBuilderWithTypeChange() {
  DCHECK(ScalarTypeId<T>() != current_type_id_);
  ChangeCurrentType(ScalarTypeId<T>());
  if (std::holds_alternative<std::monostate>(*current_storage_)) {
    current_typeidx_ = types_buffer_.type_count();
    *current_storage_ = TypedStorage<T>{
        current_typeidx_, typename arolla::Buffer<T>::Builder(size())};
    types_buffer_.types.push_back(ScalarTypeId<T>());
    return std::get<typename arolla::Buffer<T>::Builder>(
        std::get<TypedStorage<T>>(*current_storage_).data);
  }
  DCHECK(std::holds_alternative<TypedStorage<T>>(*current_storage_));
  TypedStorage<T>& tstorage = std::get<TypedStorage<T>>(*current_storage_);
  current_typeidx_ = tstorage.type_index;
  if (std::holds_alternative<arolla::Buffer<T>>(tstorage.data)) {
    // Note: currently can't happen because we don't create it yet.
    // TODO: implement conversion Buffer -> Buffer::Builder.
    LOG(FATAL) << "Not implemented yet";
  }
  DCHECK(std::holds_alternative<typename arolla::Buffer<T>::Builder>(
      tstorage.data));
  return std::get<typename arolla::Buffer<T>::Builder>(tstorage.data);
}

template <typename T>
void SliceBuilder::InsertIfNotSet(int64_t id, const T& v) {
  static_assert(!std::is_same_v<T, DataItem>);
  if (IsSet(id)) {
    return;
  }
  unset_count_--;
  if (SliceBuilder::IsMissing(v)) {
    id_to_typeidx_[id] = TypesBuffer::kRemoved;
    return;
  }
  if constexpr (arolla::is_optional_v<T>) {
    GetBufferBuilder<arolla::strip_optional_t<T>>().Set(id, v.value);
  } else if constexpr (arolla::meta::is_wrapped_with_v<DataItem::View, T>) {
    GetBufferBuilder<typename T::value_type>().Set(id, v.view);
  } else if constexpr (!std::is_same_v<T, MissingValue> &&
                       !std::is_same_v<T, std::nullopt_t>) {
    GetBufferBuilder<T>().Set(id, v);
  } else {
    LOG(FATAL) << "Unexpected missing value";
  }
  id_to_typeidx_[id] = current_typeidx_;
}

template <typename T>
template <typename OptionalOrT>
void SliceBuilder::TypedBuilder<T>::InsertIfNotSet(int64_t id,
                                                   const OptionalOrT& v) {
  if (IsSet(id)) {
    return;
  }
  base_.unset_count_--;
  if (SliceBuilder::IsMissing(v)) {
    id_to_typeidx_[id] = TypesBuffer::kRemoved;
    return;
  }
  if constexpr (arolla::is_optional_v<OptionalOrT>) {
    static_assert(
        std::is_same_v<OptionalOrT, arolla::OptionalValue<T>> ||
        std::is_same_v<OptionalOrT,
                       arolla::OptionalValue<arolla::view_type_t<T>>>);
    bldr_.Set(id, v.value);
  } else if constexpr (std::is_same_v<OptionalOrT, DataItem::View<T>>) {
    bldr_.Set(id, v.view);
  } else if constexpr (!std::is_same_v<T, std::nullopt_t>) {
    static_assert(std::is_same_v<OptionalOrT, T> ||
                  std::is_same_v<OptionalOrT, arolla::view_type_t<T>>);
    bldr_.Set(id, v);
  } else {
    LOG(FATAL) << "Unexpected missing value";
  }
  id_to_typeidx_[id] = typeidx_;
}

// Dummy implementation.
// TODO: optimize.
template <typename T>
void SliceBuilder::InsertIfNotSet(const arolla::bitmap::Bitmap& mask,
                                  const arolla::bitmap::Bitmap& presence,
                                  const arolla::Buffer<T>& values) {
  DCHECK_EQ(values.size(), size());
  for (size_t i = 0; i < size(); ++i) {
    if (arolla::bitmap::GetBit(mask, i)) {
      if (arolla::bitmap::GetBit(presence, i)) {
        InsertIfNotSet(i, DataItem::View<T>(values[i]));
      } else {
        InsertIfNotSet(i, std::nullopt);
      }
    }
  }
}

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_SLICE_BUILDER_H_
