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
#include <tuple>
#include <utility>
#include <variant>

#include "absl/base/attributes.h"
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
#include "koladata/internal/types_buffer.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/expr/quote.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"

namespace koladata::internal {

// DataSliceImpl builder.
class SliceBuilder {
 public:
  explicit SliceBuilder(size_t size);
  explicit SliceBuilder(size_t size, AllocationIdSet alloc_ids)
      : SliceBuilder(size) {
    allocation_ids_ = std::move(alloc_ids);
  }

  // SliceBuilder has pointers to its static data, so it can't be moved or
  // copied.
  SliceBuilder(const SliceBuilder&) = delete;
  SliceBuilder(SliceBuilder&&) = delete;
  SliceBuilder& operator=(const SliceBuilder&) = delete;
  SliceBuilder& operator=(SliceBuilder&&) = delete;

  size_t size() const { return types_buffer_.id_to_typeidx.size(); }
  bool is_finalized() const { return unset_count_ == 0; }

  // Returns true also if was set to anything (missing or present).
  bool IsSet(int64_t id) const {
    DCHECK(0 <= id && id < size());
    return types_buffer_.id_to_typeidx[id] != TypesBuffer::kUnset;
  }

  // Returns read-only TypesBuffer, but the underlying data is changing when
  // new values are inserted into the builder.
  const TypesBuffer& types_buffer() const { return types_buffer_; }

  // Sets all items that are missing in the mask to TypesBuffer::kRemoved,
  // so the builder will never assign values to them.
  void ApplyMask(const arolla::DenseArray<arolla::Unit>& mask) {
    mask.ForEach([&](int64_t id, bool present, arolla::Unit) {
      if (!present && types_buffer_.id_to_typeidx[id] == TypesBuffer::kUnset) {
        types_buffer_.id_to_typeidx[id] = TypesBuffer::kRemoved;
        unset_count_--;
      }
    });
  }

  void ConvertMaybeRemovedToUnset() {
    for (uint8_t& t : types_buffer_.id_to_typeidx) {
      if (t == TypesBuffer::kMaybeRemoved) {
        t = TypesBuffer::kUnset;
        unset_count_++;
      }
    }
  }

  void ConvertMaybeRemovedToRemoved() {
    for (uint8_t& t : types_buffer_.id_to_typeidx) {
      if (t == TypesBuffer::kMaybeRemoved) {
        t = TypesBuffer::kRemoved;
      }
    }
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
  void InsertIfNotSetAndUpdateAllocIds(int64_t id, const DataItem& v);

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
        : id_to_typeidx_(base.types_buffer_.id_to_typeidx),
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
  // type. Returns true, TypedStorage<T>::type_index and DataSliceImpl::Variant
  // containing the DenseArray if the data is not empty. Returns false and
  // TypedStorage<T>::type_index if all the data is missing.
  std::tuple<bool, uint8_t, DataSliceImpl::Variant> BuildDataSliceVariant(
      StorageVariant& sv);

  // Used during DataSliceImpl construction. Removes StorageVariants with
  // specified idx and dchecks that they were empty.
  static void RemoveEmptyTypes(DataSliceImpl::Internal& impl,
                               absl::Span<uint8_t> idx_to_remove);

  // Sets `current_type_id_` and makes `current_storage_` pointing to
  // StorageVariant for the given type. If it is a new type the a new
  // StorageVariant is created in std::monostate state.
  // Note: this function doesn't update `current_typeidx_` because it requires
  // extracting or initializing TypedStorage<T>::type_index that is more
  // convenient to do in the templated code that calls ChangeCurrentType.
  void ChangeCurrentType(KodaTypeId type_id);

  // Sets current type to MissingValue and switches storage_state_ to
  // kMapStorage (in order to not to lose type info of current_storage_).
  void UnsetCurrentType();

  template <typename T>
  typename arolla::Buffer<T>::Builder& GetBufferBuilder();

  // Part of GetBufferBuilder implementation for the case
  // `ScalarTypeId<T>() != current_type_id_`. Extracted to a separate
  // non-inlinable function to improve inlining of the fast codepath in
  // `GetBufferBuilder`.
  template <typename T>
  typename arolla::Buffer<T>::Builder& GetBufferBuilderWithTypeChange();

  // Implementation detail of GetBufferBuilder. Expects that current_storage_
  // holds TypedStorage<T> with matching type.
  template <typename T>
  typename arolla::Buffer<T>::Builder& GetBufferBuilderFromCurrentStorage();

  template <typename T>
  static bool IsMissing(const T& v) {
    if constexpr (std::is_same_v<T, MissingValue> ||
                  std::is_same_v<T, std::nullopt_t>) {
      return true;
    } else if constexpr (arolla::is_optional_v<T>) {
      return !v.present;
    } else if constexpr (arolla::meta::is_wrapped_with_v<std::optional, T>) {
      return !v.has_value();
    } else {
      return false;
    }
  }

  // Updates unset items types_buffer_.id_to_typeidx for ids that are present in
  // `mask`. For these ids sets the type to the given `typeidx` if
  // `presence==true`, or to kRemoved otherwise. Calls `add_value_fn` for each
  // id changing type from kUnset to typeidx.
  template <typename Fn>
  void UpdateTypesBuffer(uint8_t typeidx, const arolla::bitmap::Bitmap& mask,
                         const arolla::bitmap::Bitmap& presence,
                         Fn&& add_value_fn);

  // Map from ScalarTypeId<T>() to StorageVariant.
  absl::flat_hash_map<KodaTypeId, StorageVariant> storage_;

  // Used to avoid having a single element in storage_.
  // first_storage_ may only contain BufferBuilder or monostate.
  StorageVariant first_storage_;

  enum {
    kStorageEmpty,  // nothing added to the builder yet
    kFirstStorage,  // first_storage_ is used, storage_ is empty
    kMapStorage     // first_storage_ is not used, storage_ is not empty
  } storage_state_ = kStorageEmpty;

  // Pointer to storage_[current_type_id_] or first_storage_.
  StorageVariant* current_storage_ = &first_storage_;

  // Before and after any public function call we have the following invariant:
  // if `current_type_id_` is set (i.e. not type id of MissingValue),
  // current_storage_ should point to StorageVariant containing
  // Buffer<T>::Builder of the corresponding type; current_typeidx_ is index
  // of the corresponding type in types_buffer_. If `current_type_id_` is unset,
  // current_typeidx_ is unspecified and shouldn't be used.
  //
  // Note: This invariant can be temporarily violated in internal functions.
  // E.g. `ChangeCurrentType` changes the type without creating the builder or
  // updating current_typeidx_, and expects that the invariant will be restored
  // by the caller.
  KodaTypeId current_type_id_ = ScalarTypeId<MissingValue>();
  uint8_t current_typeidx_;

  TypesBuffer types_buffer_;

  // The number of elements where `types_buffer_.id_to_typeidx[i] == kUnset`.
  // Can only be decreased. When reaches zero `is_finalized()` becomes true.
  size_t unset_count_;

  AllocationIdSet allocation_ids_;
};

// *** Implementation

template <typename T>
typename arolla::Buffer<T>::Builder& SliceBuilder::GetBufferBuilder() {
  if (ScalarTypeId<T>() == current_type_id_) {
    // See comment at `current_type_id_` definition.
    return std::get<typename arolla::Buffer<T>::Builder>(
        std::get<TypedStorage<T>>(*current_storage_).data);
  } else {
    return GetBufferBuilderWithTypeChange<T>();
  }
}

template <typename T>
typename arolla::Buffer<T>::Builder&
SliceBuilder::GetBufferBuilderFromCurrentStorage() {
  DCHECK(std::holds_alternative<TypedStorage<T>>(*current_storage_));
  TypedStorage<T>& tstorage = std::get<TypedStorage<T>>(*current_storage_);
  current_typeidx_ = tstorage.type_index;
  if (!std::holds_alternative<arolla::Buffer<T>>(tstorage.data)) {
    return std::get<typename arolla::Buffer<T>::Builder>(tstorage.data);
  }
  const arolla::Buffer<T>& buf = std::get<arolla::Buffer<T>>(tstorage.data);
  typename arolla::Buffer<T>::Builder bldr(size());
  for (int64_t i = 0; i < size(); ++i) {
    if (types_buffer_.id_to_typeidx[i] == current_typeidx_) {
      bldr.Set(i, buf[i]);
    }
  }
  tstorage.data = std::move(bldr);
  return std::get<typename arolla::Buffer<T>::Builder>(tstorage.data);
}

template <typename T>
ABSL_ATTRIBUTE_NOINLINE typename arolla::Buffer<T>::Builder&
SliceBuilder::GetBufferBuilderWithTypeChange() {
  DCHECK(ScalarTypeId<T>() != current_type_id_);

  // It can break `current_type_id_` invariant
  // (see comment at `current_type_id_` definition).
  ChangeCurrentType(ScalarTypeId<T>());

  // Here we restore the invariant.
  if (std::holds_alternative<std::monostate>(*current_storage_)) {
    current_typeidx_ = types_buffer_.type_count();
    *current_storage_ = TypedStorage<T>{
        current_typeidx_, typename arolla::Buffer<T>::Builder(size())};
    types_buffer_.types.push_back(ScalarTypeId<T>());
    return std::get<typename arolla::Buffer<T>::Builder>(
        std::get<TypedStorage<T>>(*current_storage_).data);
  }
  // The invariant can still be violated (storage can contain Buffer rather
  // than Builder), but GetBufferBuilderFromCurrentStorage handles it.
  return GetBufferBuilderFromCurrentStorage<T>();
}

template <typename T>
void SliceBuilder::InsertIfNotSet(int64_t id, const T& v) {
  static_assert(!std::is_same_v<T, DataItem>);
  if (IsSet(id)) {
    return;
  }
  unset_count_--;
  if (SliceBuilder::IsMissing(v)) {
    types_buffer_.id_to_typeidx[id] = TypesBuffer::kMaybeRemoved;
    return;
  }
  if constexpr (arolla::is_optional_v<T>) {
    GetBufferBuilder<arolla::strip_optional_t<T>>().Set(id, v.value);
  } else if constexpr (arolla::meta::is_wrapped_with_v<std::optional, T>) {
    GetBufferBuilder<typename T::value_type>().Set(id, *v);
  } else if constexpr (arolla::meta::is_wrapped_with_v<DataItem::View, T>) {
    GetBufferBuilder<typename T::value_type>().Set(id, v.view);
  } else if constexpr (!std::is_same_v<T, MissingValue> &&
                       !std::is_same_v<T, std::nullopt_t>) {
    GetBufferBuilder<T>().Set(id, v);
  } else {
    LOG(FATAL) << "Unexpected missing value";
  }
  types_buffer_.id_to_typeidx[id] = current_typeidx_;
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
    id_to_typeidx_[id] = TypesBuffer::kMaybeRemoved;
    return;
  }
  if constexpr (arolla::is_optional_v<OptionalOrT>) {
    static_assert(
        std::is_same_v<OptionalOrT, arolla::OptionalValue<T>> ||
        std::is_same_v<OptionalOrT,
                       arolla::OptionalValue<arolla::view_type_t<T>>>);
    bldr_.Set(id, v.value);
  } else if constexpr (arolla::meta::is_wrapped_with_v<std::optional,
                                                       OptionalOrT>) {
    bldr_.Set(id, *v);
  } else if constexpr (std::is_same_v<OptionalOrT, DataItem::View<T>>) {
    bldr_.Set(id, v.view);
  } else if constexpr (!std::is_same_v<OptionalOrT, std::nullopt_t>) {
    static_assert(std::is_same_v<OptionalOrT, T> ||
                  std::is_same_v<OptionalOrT, arolla::view_type_t<T>>);
    bldr_.Set(id, v);
  } else {
    LOG(FATAL) << "Unexpected missing value";
  }
  id_to_typeidx_[id] = typeidx_;
}

template <typename Fn>
void SliceBuilder::UpdateTypesBuffer(uint8_t typeidx,
                                     const arolla::bitmap::Bitmap& mask,
                                     const arolla::bitmap::Bitmap& presence,
                                     Fn&& add_value_fn) {
  arolla::VoidBuffer vb(size());
  arolla::DenseArraysForEach(
      [&](int64_t id, bool valid, arolla::Unit, arolla::OptionalUnit presence) {
        if (valid && !IsSet(id)) {
          types_buffer_.id_to_typeidx[id] =
              presence ? typeidx : TypesBuffer::kMaybeRemoved;
          unset_count_--;
          if (presence) {
            add_value_fn(id);
          }
        }
      },
      arolla::DenseArray<arolla::Unit>{vb, mask},
      arolla::DenseArray<arolla::Unit>{vb, presence})
      .IgnoreError();  // we create the DenseArray ourselves
}

template <typename T>
void SliceBuilder::InsertIfNotSet(const arolla::bitmap::Bitmap& mask,
                                  const arolla::bitmap::Bitmap& presence,
                                  const arolla::Buffer<T>& values) {
  DCHECK_EQ(values.size(), size());
  if (ScalarTypeId<T>() != current_type_id_) {
    // It breaks `current_type_id_` invariant
    // (see comment at `current_type_id_` definition).
    ChangeCurrentType(ScalarTypeId<T>());

    if (std::holds_alternative<std::monostate>(*current_storage_)) {
      uint8_t typeidx = types_buffer_.type_count();
      *current_storage_ = TypedStorage<T>{typeidx, values};
      types_buffer_.types.push_back(ScalarTypeId<T>());

      UpdateTypesBuffer(typeidx, mask, presence, [](int64_t id) {});

      // Restores `current_type_id_` invariant by unsetting the type.
      UnsetCurrentType();

      return;
    }

    // Here the invariant still can be violated.
  }

  // Restores `current_type_id_` invariant by enforcing buffer builder.
  typename arolla::Buffer<T>::Builder& bldr =
      GetBufferBuilderFromCurrentStorage<T>();

  UpdateTypesBuffer(current_typeidx_, mask, presence,
                    [&](int64_t id) { bldr.Set(id, values[id]); });
}

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_SLICE_BUILDER_H_
