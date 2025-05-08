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
#ifndef KOLADATA_INTERNAL_DATA_SLICE_H_
#define KOLADATA_INTERNAL_DATA_SLICE_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/types_buffer.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/iterator.h"
#include "arolla/util/refcount_ptr.h"
#include "arolla/util/repr.h"
#include "arolla/util/status.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

class SliceBuilder;

// Multidimensional Jagged Array storing ObjectId's or primitives.
class DataSliceImpl {
 public:
  // Creates empty DataSliceImpl with `size` equal to zero and unknown type.
  // TODO: avoid allocation in default constructor and create a
  // private constructor for Builder and factory methods.
  DataSliceImpl() = default;

  // Returns DataSliceImpl with `size` newly allocated objects
  // in a single AllocationId.
  static DataSliceImpl AllocateEmptyObjects(size_t size);

  // Returns DataSliceImpl with `size` objects from the provided AllocationId.
  static DataSliceImpl ObjectsFromAllocation(AllocationId alloc_id,
                                             size_t size);

  // Returns all-removed DataSliceImpl with given size and unknown type
  static DataSliceImpl CreateEmptyAndUnknownType(size_t size);

  // Returns empty DataSliceImpl with given `types_buffer`.
  // TypesBuffer is used to distinguish missing and removed values.
  // Only kRemoved and kUnset are allowed.
  static DataSliceImpl CreateEmptyAndUnknownType(TypesBuffer types_buffer);

  // Returns DataSliceImpl with specified ObjectId's and
  // AllocationId's.
  static DataSliceImpl CreateObjectsDataSlice(ObjectIdArray objects,
                                              AllocationIdSet allocation_ids) {
    return CreateWithAllocIds(allocation_ids, std::move(objects));
  }

  // Returns DataSliceImpl with specified values and allocation ids.
  template <class T, class... Ts>
  static DataSliceImpl CreateWithAllocIds(AllocationIdSet allocation_ids,
                                          arolla::DenseArray<T> main_values,
                                          arolla::DenseArray<Ts>... values);

  // Creates a single-type DataSliceImpl with provided TypesBuffer. TypesBuffer
  // in single-type slice is optional, used in DenseSource to distinguish
  // missing and removed values.
  template <class T>
  static DataSliceImpl CreateWithTypesBuffer(TypesBuffer types_buffer,
                                             AllocationIdSet allocation_ids,
                                             arolla::DenseArray<T> values);

  static DataSliceImpl Create(const arolla::DenseArray<DataItem>& items);

  // Returns DataSliceImpl with specified values. In case of
  // ObjectId arrays for better performance prefer CreateWithAllocIds if
  // alloc ids are already known.
  template <class T, class... Ts>
  static DataSliceImpl Create(arolla::DenseArray<T> main_values,
                              arolla::DenseArray<Ts>... values);

  static DataSliceImpl Create(absl::Span<const DataItem> items);

  static DataSliceImpl Create(size_t size, const DataItem& item);

  // Returns DataSliceImpl with specified values.
  // Returns error if type is not supported.
  static absl::StatusOr<DataSliceImpl> Create(arolla::TypedRef values);

  // Returns number of elements in the flat array.
  size_t size() const { return internal_->size; }

  // Returns number of present elements in the flat array.
  size_t present_count() const;

  // Returns true if there are values of different types in a slice.
  bool is_mixed_dtype() const { return internal_->values.size() > 1; }

  // Returns true if all values have the same type and the type is not unknown.
  bool is_single_dtype() const { return internal_->values.size() == 1; }

  // Returns true on all empty slices without known type.
  bool is_empty_and_unknown() const { return internal_->values.empty(); }

  // Returns QType of the content of the DataSliceImpl if all elements has the
  // same type or `GetNothingQType` otherwise. I.e. dtype() == NOTHING, when
  // either is_mixed_dtype() or is_empty_and_unknown() is true.
  arolla::QTypePtr dtype() const { return internal_->dtype; }

  // Returns true iff any present value is a primitive.
  bool ContainsAnyPrimitives() const;

  // Returns true iff all present values are lists.
  bool ContainsOnlyLists() const;

  // Returns true iff all present values are dicts.
  bool ContainsOnlyDicts() const;

  // Returns true iff all present values are entities.
  bool ContainsOnlyEntities() const;

  // Get values from the data slice.
  // Can be used only if dtype() == GetQType<T>().
  template <class T>
  const arolla::DenseArray<T>& values() const {
    DCHECK_EQ(internal_->values.size(), 1);
    DCHECK_EQ(internal_->dtype, arolla::GetQType<T>());
    return std::get<arolla::DenseArray<T>>(internal_->values[0]);
  }

  // Get values from the data slice for given type_idx (see TypesBuffer).
  template <class T>
  const arolla::DenseArray<T>& values(uint8_t type_idx) const {
    DCHECK_GT(internal_->values.size(), type_idx);
    return std::get<arolla::DenseArray<T>>(internal_->values[type_idx]);
  }

  // Struct that holds type indices for each element. Can be empty for empty or
  // single-type slices. These indices can be used e.g. in
  // `DataSlice::values(type_idx)` to get the underlying DenseArray holding
  // values of the corresponding type.
  const TypesBuffer& types_buffer() const { return internal_->types_buffer; }

  // Call `visitor` on each internal DenseArray of values. Each item is a
  // variant with a value of DenseArray of some of supported scalar types
  // (INT32, FLOAT32, STRING, etc.). All DenseArrays have the same size, with
  // non-intersecting presence ids. A value at index `i` of the DataSliceImpl is
  // considered missing iff it is missing in all DenseArrays, and one can
  // therefore not consider the DenseArrays in isolation.
  //
  // `visitor` should either return `absl::Status` or `void`. In case,
  // `absl::Status` is the return type, in case of error, the error is returned
  // without calling a visitor on the rest of the items.
  template <class Visitor>
  auto VisitValues(Visitor&& visitor) const {
    const auto& values = internal_->values;
    if constexpr (std::is_same_v<decltype(std::visit(visitor, Variant())),
                                 absl::Status>) {
      for (const Variant& vals : values) {
        if (absl::Status s = std::visit(visitor, vals); !s.ok()) {
          return s;
        }
      }
      return absl::OkStatus();
    } else {
      static_assert(
          std::is_same_v<decltype(std::visit(visitor, Variant())), void>);
      for (const Variant& vals : values) {
        std::visit(visitor, vals);
      }
    }
  }

  // Transforms values of the DataSliceImpl.
  //
  // `result_size` is the size of the result DataSliceImpl.
  //
  // `allocation_ids` is the set of allocation ids that are allowed to be
  // present in the result DataSliceImpl. If `std::nullopt`, any allocation id
  // is allowed.
  //
  // `transform` is a functor that takes a DenseArray of values of the
  // DataSliceImpl and returns a DenseArray of values of the result
  // DataSliceImpl.
  //
  // All DenseArrays in the result DataSliceImpl must have the same size and
  // non-intersecting presence ids.
  //
  // All resulted DenseArrays must have different types.
  template <class DenseArrayTransformer>
  absl::StatusOr<DataSliceImpl> TransformValues(
      size_t result_size, std::optional<AllocationIdSet> allocation_ids,
      DenseArrayTransformer&& transform) const;

  // Returns DataItem with given offset.
  DataItem operator[](int64_t offset) const;

  bool present(int64_t offset) const;

  // Returns a DenseArray of DataItems for DataSlice's items.
  // Missing items in the DataSlice are converted to missings in the DenseArray
  // rather than empty DataItems.
  arolla::DenseArray<DataItem> AsDataItemDenseArray() const;

  // Support for testing::ElementsAre
  using value_type = DataItem;
  using size_type = int64_t;
  using difference_type = int64_t;
  arolla::ConstArrayIterator<DataSliceImpl> begin() const {
    return arolla::ConstArrayIterator<DataSliceImpl>(this, 0);
  }
  arolla::ConstArrayIterator<DataSliceImpl> end() const {
    return arolla::ConstArrayIterator<DataSliceImpl>(this, size());
  }

  // Returns list of uniquified allocation ids of all ObjectId's.
  const AllocationIdSet& allocation_ids() const {
    return internal_->allocation_ids;
  }

  // Computes the fingerprint.
  void ArollaFingerprint(arolla::FingerprintHasher* hasher) const;

  // Returns true iff `other` represents the same data.
  // NOTE: `IsEquivalent` can return true even if dtype() != other.dtype().
  // For example:
  //   slice1 = DataSliceImpl::Create(IntArray({1, 2, 3}))
  //   slice2 = DataSliceImpl::Create(IntArray({1, 2, 3}),
  //                                  FloatArray({nullopt, nullopt, nullopt}))
  //   slice1.dtype() -> INT32      slice1.is_mixed_type() -> false
  //   slice2.dtype() -> NOTHING    slice2.is_mixed_type() -> true
  // But
  //   slice1.IsEquivalent(slice2) -> true.
  bool IsEquivalentTo(const DataSliceImpl& other) const;

  template <typename Sink>
  friend void AbslStringify(Sink& sink, const DataSliceImpl& slice) {
    constexpr size_t kSufficientStringLength = 1000;

    std::string result;
    absl::StrAppend(&result, "[");
    for (size_t i = 0; i < slice.size(); ++i) {
      if (i != 0) {
        absl::StrAppend(&result, ", ");
      }
      if (result.size() > kSufficientStringLength) {
        absl::StrAppend(&result, "... (", slice.size(), " elements total)");
        break;
      }
      absl::StrAppend(&result, slice[i]);
    }
    absl::StrAppend(&result, "]");

    sink.Append(result);
  }

 private:
  friend class SliceBuilder;

  template <class T>
  void AddAllocIds(const arolla::DenseArray<T>& array) {
    if constexpr (std::is_same_v<T, ObjectId>) {
      AllocationIdSet& id_set = internal_->allocation_ids;
      array.ForEachPresent(
          [&](int64_t id, ObjectId obj) { id_set.Insert(AllocationId(obj)); });
    }
  }

  using Variant = std::variant<arolla::DenseArray<ObjectId>,                 //
                               arolla::DenseArray<int32_t>,                  //
                               arolla::DenseArray<float>,                    //
                               arolla::DenseArray<int64_t>,                  //
                               arolla::DenseArray<double>,                   //
                               arolla::DenseArray<bool>,                     //
                               arolla::DenseArray<arolla::Unit>,             //
                               arolla::DenseArray<arolla::Text>,             //
                               arolla::DenseArray<arolla::Bytes>,            //
                               arolla::DenseArray<arolla::expr::ExprQuote>,  //
                               arolla::DenseArray<schema::DType>             //
                               >;
  struct Internal : public arolla::RefcountedBase {
    size_t size = 0;
    arolla::QTypePtr dtype = arolla::GetNothingQType();

    // List of uniquified allocation ids of all objects.
    // Empty if there are no ObjectId in this DataSliceImpl.
    AllocationIdSet allocation_ids;

    // Typically 1 element.
    // More are used to represent mixed types.
    //
    // All arrays must have the same size and non intersected present ids. The
    // element index `i` of the DataSliceImpl is given by the (only) present
    // element at index `i` in one of the underlying DenseArrays. If no such
    // element is present, the DataSliceImpl element is considered missing.
    absl::InlinedVector<Variant, 1> values;

    TypesBuffer types_buffer;
  };

  // Removes all values with all non present items.
  // Sets dtype to dtype of single value or GetNothingQType otherwise.
  void RemoveEmptyValues();

  static void InitTypesBuffer(Internal& impl);

  // Verify that all ObjectId's are in the allocation ids.
  // It is slow and shouldn't be used in opt mode.
  bool VerifyAllocIdsConsistency() const;

  template <class T, class... Ts>
  static void CreateImpl(DataSliceImpl& res, arolla::DenseArray<T> main_values,
                         arolla::DenseArray<Ts>... values);

  arolla::RefcountPtr<Internal> internal_ =
      arolla::RefcountPtr<Internal>::Make();
};

namespace data_slice_impl {

class NonIntersectingIdsChecker {
 public:
  explicit NonIntersectingIdsChecker(size_t size)
      : size_(size),
        bitmap_size_(arolla::bitmap::BitmapSize(size)),
        present_ids_(bitmap_size_) {}

  template <class T>
  bool Verify(const arolla::DenseArray<T>& array) {
    if (array.size() != size_) {
      return false;
    }
    for (size_t i = 0; i < bitmap_size_; ++i) {
      arolla::bitmap::Word word = arolla::bitmap::GetWordWithOffset(
          array.bitmap, i, array.bitmap_bit_offset);
      if (present_ids_[i] & word) {
        return false;
      }
      present_ids_[i] |= word;
    }
    return true;
  }

 private:
  size_t size_;
  size_t bitmap_size_;
  std::vector<arolla::bitmap::Word> present_ids_;
};

template <class T, class... Ts>
bool VerifyNonIntersectingIds(const arolla::DenseArray<T>& main_values,
                              const arolla::DenseArray<Ts>&... values) {
  NonIntersectingIdsChecker checker(main_values.size());
  return checker.Verify(main_values) && (checker.Verify(values) && ...);
}

template <class T>
bool VerifyAllocIds(AllocationIdSet alloc_ids,
                    const arolla::DenseArray<T>& values) {
  if constexpr (std::is_same_v<T, ObjectId>) {
    bool res = true;
    values.ForEachPresent([&](int64_t /*id*/, ObjectId obj_id) {
      res = res && !alloc_ids.Insert(AllocationId(obj_id));
    });
    return res;
  } else {
    return true;
  }
}

template <class T>
constexpr bool AreAllTypesDistinct(std::type_identity<T>) {
  return true;
}

template <class T, class... Ts>
constexpr bool AreAllTypesDistinct(std::type_identity<T>,
                                   std::type_identity<Ts>...) {
  return (!std::is_same_v<T, Ts> && ...) &&
         AreAllTypesDistinct(std::type_identity<Ts>()...);
}

}  // namespace data_slice_impl

template <class DenseArrayTransformer>
absl::StatusOr<DataSliceImpl> DataSliceImpl::TransformValues(
    size_t result_size, std::optional<AllocationIdSet> allocation_ids,
    DenseArrayTransformer&& transform) const {
  const auto& values = internal_->values;
  DataSliceImpl res;

  auto& res_impl = *res.internal_;
  res_impl.size = result_size;

  if (allocation_ids.has_value()) {
    res_impl.allocation_ids = std::move(*allocation_ids);
  }

#ifndef NDEBUG
  absl::flat_hash_set<arolla::QTypePtr> qtypes;
  qtypes.reserve(values.size());
  data_slice_impl::NonIntersectingIdsChecker checker(result_size);
#endif

  auto add_array = [&]<class T>(arolla::DenseArray<T> transformed_array) {
#ifndef NDEBUG
    DCHECK(qtypes.insert(arolla::GetQType<T>()).second)
        << "duplicated type: " << arolla::GetQType<T>();
    DCHECK_EQ(transformed_array.size(), result_size);
    DCHECK_EQ(transformed_array.bitmap_bit_offset, 0);
    DCHECK(checker.Verify(transformed_array)) << "ids are intersecting";
#endif
    if (!allocation_ids.has_value()) {
      res.AddAllocIds(transformed_array);
    }
    if (!transformed_array.IsAllMissing()) {
      res_impl.dtype = res_impl.values.empty() ? arolla::GetQType<T>()
                                               : arolla::GetNothingQType();
      res_impl.values.push_back(std::move(transformed_array));
    }
  };

  res_impl.values.reserve(values.size());

  for (const Variant& vals : values) {
    RETURN_IF_ERROR(std::visit(
        [&](const auto& array) -> absl::Status {
          auto s = transform(array);
          if constexpr (arolla::IsStatusOrT<decltype(s)>::value) {
            if (!s.ok()) {
              return s.status();
            }
            add_array(*std::move(s));
          } else {
            add_array(std::move(s));
          }
          return absl::OkStatus();
        },
        vals));
  }

  if (res_impl.values.size() > 1) {
    InitTypesBuffer(res_impl);
  }
  DCHECK(res.VerifyAllocIdsConsistency());
  return res;
}

template <class T, class... Ts>
void DataSliceImpl::CreateImpl(DataSliceImpl& res,
                               arolla::DenseArray<T> main_values,
                               arolla::DenseArray<Ts>... values) {
  static_assert(data_slice_impl::AreAllTypesDistinct(
                    std::type_identity<T>(), std::type_identity<Ts>()...),
                "All DenseArray's must have different types");
  DCHECK_EQ(main_values.bitmap_bit_offset, 0);
  if constexpr (sizeof...(values) > 0) {
    DCHECK((values.size() == main_values.size()) && ...);
    DCHECK((values.bitmap_bit_offset == 0) && ...);
    DCHECK(data_slice_impl::VerifyNonIntersectingIds(main_values, values...));
  }
  auto& impl = *res.internal_;
  impl.size = main_values.size();

  if constexpr (sizeof...(values) > 0) {
    impl.values.reserve(sizeof...(values) + 1);
    DCHECK(((values.size() == main_values.size()) && ...));
  }
  // We avoid calling RemoveEmptyValues for single type to minimize
  // overhead in runtime and the binary size.
  if (!main_values.IsAllMissing()) {
    impl.values.emplace_back(std::move(main_values));
    impl.dtype = arolla::GetQType<T>();
  }
  if constexpr (sizeof...(values) > 0) {
    (impl.values.emplace_back(std::move(values)), ...);
    res.RemoveEmptyValues();
    if (impl.values.size() > 1) {
      InitTypesBuffer(impl);
    }
  }
}

template <class T, class... Ts>
DataSliceImpl DataSliceImpl::Create(arolla::DenseArray<T> main_values,
                                    arolla::DenseArray<Ts>... values) {
  DataSliceImpl res;
  res.AddAllocIds(main_values);
  (res.AddAllocIds(values), ...);
  CreateImpl(res, std::move(main_values), std::move(values)...);
  return res;
}

template <class T, class... Ts>
DataSliceImpl DataSliceImpl::CreateWithAllocIds(
    AllocationIdSet allocation_ids, arolla::DenseArray<T> main_values,
    arolla::DenseArray<Ts>... values) {
  DataSliceImpl res;
  if constexpr ((std::is_same_v<T, ObjectId> || ... ||
                 std::is_same_v<Ts, ObjectId>)) {
    DCHECK(data_slice_impl::VerifyAllocIds(allocation_ids, main_values));
    DCHECK((data_slice_impl::VerifyAllocIds(allocation_ids, values) && ...));
    res.internal_->allocation_ids = std::move(allocation_ids);
  } else {
    (void)allocation_ids;
  }
  CreateImpl(res, std::move(main_values), std::move(values)...);
  return res;
}

template <class T>
DataSliceImpl DataSliceImpl::CreateWithTypesBuffer(TypesBuffer types_buffer,
    AllocationIdSet allocation_ids, arolla::DenseArray<T> values) {
  DataSliceImpl res;
  res.internal_->types_buffer = std::move(types_buffer);
  if constexpr (std::is_same_v<T, ObjectId>) {
    DCHECK(data_slice_impl::VerifyAllocIds(allocation_ids, values));
    res.internal_->allocation_ids = std::move(allocation_ids);
  } else {
    (void)allocation_ids;
  }
  CreateImpl(res, std::move(values));
  return res;
}


using DataSlicePtr = std::unique_ptr<DataSliceImpl>;

}  // namespace koladata::internal

namespace arolla {

AROLLA_DECLARE_REPR(::koladata::internal::DataSliceImpl);
AROLLA_DECLARE_SIMPLE_QTYPE(INTERNAL_DATA_SLICE,
                            ::koladata::internal::DataSliceImpl);

}  // namespace arolla

#endif  // KOLADATA_INTERNAL_DATA_SLICE_H_
