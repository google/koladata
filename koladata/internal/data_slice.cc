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
#include "koladata/internal/data_slice.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/types.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"

namespace koladata::internal {

namespace {

using ::arolla::Buffer;
using ::arolla::CreateEmptyDenseArray;
using ::arolla::GetQType;
using ::arolla::QTypePtr;

// Meta function to convert variant args to arolla type_list.
struct VariantArgsMetaFn {
  template <class... Args>
  arolla::meta::type_list<Args...> operator()(std::variant<Args...>);
};

}  // namespace

DataSliceImpl DataSliceImpl::ObjectsFromAllocation(AllocationId alloc_id,
                                                   size_t size) {
  DataSliceImpl result;
  auto& impl = *result.internal_;
  impl.size = size;
  impl.dtype = GetQType<ObjectId>();

  Buffer<ObjectId>::Builder values_builder(size);
  if (size > 0) {
    impl.allocation_ids = AllocationIdSet(alloc_id);
    for (int64_t i = 0; i < size; ++i) {
      values_builder.Set(i, alloc_id.ObjectByOffset(i));
    }
  }
  impl.values.emplace_back(ObjectIdArray{std::move(values_builder).Build()});
  return result;
}

DataSliceImpl DataSliceImpl::AllocateEmptyObjects(size_t size) {
  return ObjectsFromAllocation(Allocate(size), size);
}

DataSliceImpl DataSliceImpl::CreateAllMissingObjectDataSlice(size_t size) {
  DataSliceImpl result;
  auto& impl = *result.internal_;
  impl.size = size;
  impl.dtype = GetQType<ObjectId>();
  impl.values.emplace_back(CreateEmptyDenseArray<ObjectId>(size));
  return result;
}

absl::StatusOr<DataSliceImpl> DataSliceImpl::CreateEmptyWithType(
    size_t size, QTypePtr dtype) {
  std::optional<DataSliceImpl> result;
  arolla::meta::foreach_type<std::invoke_result_t<VariantArgsMetaFn, Variant>>(
      [&](auto meta_type) {
        using ValT = typename decltype(meta_type)::type::base_type;
        if (dtype == GetQType<ValT>()) {
          result = Create(CreateEmptyDenseArray<ValT>(size));
        }
      });
  if (result.has_value()) {
    return *result;
  }
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported type: ", dtype->name()));
}

DataSliceImpl DataSliceImpl::CreateEmptyAndUnknownType(size_t size) {
  DataSliceImpl result;
  result.internal_->size = size;
  return result;
}

DataSliceImpl DataSliceImpl::Create(const arolla::DenseArray<DataItem>& items) {
  if (items.empty()) {
    return DataSliceImpl();
  }
  DataSliceImpl::Builder bldr(items.size());
  items.ForEachPresent(
      [&](int64_t id, const auto& value) { bldr.Set(id, value); });
  return std::move(bldr).Build();
}

DataSliceImpl DataSliceImpl::Create(absl::Span<const DataItem> items) {
  if (items.empty()) {
    return DataSliceImpl();
  }
  DataSliceImpl::Builder bldr(items.size());
  for (int64_t i = 0; i < items.size(); ++i) {
    bldr.Set(i, items[i]);
  }
  return std::move(bldr).Build();
}

DataSliceImpl DataSliceImpl::Create(size_t size, const DataItem& item) {
  return item.VisitValue([&]<typename T>(const T& val) {
    if constexpr (std::is_same_v<T, MissingValue>) {
      return DataSliceImpl::CreateEmptyAndUnknownType(size);
    } else {
      auto arr = arolla::CreateConstDenseArray<T>(size, val);
      if constexpr (std::is_same_v<T, ObjectId>) {
        return DataSliceImpl::CreateObjectsDataSlice(
            arr, AllocationIdSet(AllocationId(val)));
      } else {
        return DataSliceImpl::Create(arr);
      }
    }
  });
}

absl::StatusOr<DataSliceImpl> DataSliceImpl::Create(arolla::TypedRef values) {
  std::optional<DataSliceImpl> result;
  QTypePtr array_type = values.GetType();
  arolla::meta::foreach_type<std::invoke_result_t<VariantArgsMetaFn, Variant>>(
      [&](auto meta_type) {
        using ArrayT = typename decltype(meta_type)::type;
        if constexpr (!std::is_same_v<ArrayT, ObjectIdArray>) {
          if (array_type == GetQType<ArrayT>()) {
            result = Create(values.UnsafeAs<ArrayT>());
          }
        }
      });
  if (!result.has_value()) {
    if (array_type->value_qtype() != nullptr) {
      return absl::InvalidArgumentError(
          absl::StrCat("unsupported array element type: ",
                       array_type->value_qtype()->name()));
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("unsupported type: ", array_type->name()));
    }
  }
  return std::move(*result);
}

size_t DataSliceImpl::present_count() const {
  size_t res = 0;
  VisitValues([&](const auto& array) {
    res += array.PresentCount();
  });
  return res;
}

DataItem DataSliceImpl::operator[](int64_t offset) const {
  DCHECK_LT(offset, internal_->size);
  DataItem result;
  for (const auto& value : internal_->values) {
    std::visit(
        [offset, &result](const auto& array) {
          using T = typename std::decay_t<decltype(array)>::base_type;
          if (array.present(offset)) {
            result = DataItem(T(array.values[offset]));
          }
        },
        value);
    if (result.has_value()) {
      break;
    }
  }
  return result;
}

arolla::DenseArray<DataItem> DataSliceImpl::AsDataItemDenseArray() const {
  auto size = internal_->size;
  if (is_empty_and_unknown()) {
    return CreateEmptyDenseArray<DataItem>(size);
  }

  arolla::DenseArrayBuilder<DataItem> builder(size);
  VisitValues([&]<typename T>(const arolla::DenseArray<T>& array) {
    array.ForEachPresent([&](int64_t id, const auto& value) {
      builder.Set(id, DataItem(T(value)));
    });
  });
  return std::move(builder).Build();
}

void DataSliceImpl::ArollaFingerprint(arolla::FingerprintHasher* hasher) const {
  // NOTE: If the DataSlice is empty, regardless of how it is created, it should
  // have the same fingerprint as other empty DataSlices of that size. Internal
  // DenseArrays contain historical information on how the slice was created and
  // that is not necessarily important for the fingerprint computation.
  //
  // Higher-level DataSlice includes Schema in its fingerprint computation, so
  // the type information will still be included in the high-level DataSlice's
  // final fingerprint:
  //
  // kd.slice(arolla.dense_array_int64([None, None]))
  //     <=>
  // kd.slice([None, None], kd.INT64)
  if (present_count() == 0) {
    hasher->Combine(size());
    return;
  }
  // allocation_ids is also not needed as it is a redundant information on what
  // is present in one of the value arrays (i.e. array of ObjectIds).
  auto combine = [&](const auto& value) {
    std::visit([&](const auto& array) {
      if (!array.empty()) {
        using T = typename std::decay_t<decltype(array)>::base_type;
        hasher->Combine(array);
        hasher->Combine(GetQType<T>());
      }
    }, value);
  };

  if (internal_->values.size() == 1) {
    combine(internal_->values[0]);
    return;
  }

  // Iterate over `values` always in the same order, regardless of the order of
  // DenseArrays during DataSliceImpl creation.
  auto values_copy = internal_->values;
  std::sort(values_copy.begin(), values_copy.end(),
            [&](const auto& l, const auto& r) {
              return l.index() < r.index();
            });
  for (const auto& val : values_copy) {
    combine(val);
  }
}

void DataSliceImpl::RemoveEmptyValues() {
  auto end = std::remove_if(
      internal_->values.begin(), internal_->values.end(),
      [](const auto& variant) {
        return std::visit(
            [](const auto& array) { return array.IsAllMissing(); }, variant);
      });
  internal_->values.erase(end, internal_->values.end());
  if (internal_->values.size() != 1) {
    internal_->dtype = arolla::GetNothingQType();
  } else {
    internal_->dtype = std::visit(
        [](const auto& array) {
          return GetQType<typename std::decay_t<decltype(array)>::base_type>();
        },
        internal_->values[0]);
  }
}

void DataSliceImpl::Builder::ChangeCurrentType(int8_t type_id) {
  DCHECK_NE(current_type_id_, type_id);
  if (current_type_id_ == ScalarTypeId<MissingValue>()) {
    current_type_id_ = type_id;
    return;
  }
  if (bldrs_.empty()) {
    bldrs_.reserve(2);
    bldrs_.emplace(current_type_id_, std::move(first_bldr_));
  }
  current_bldr_ = &bldrs_[type_id];
  current_type_id_ = type_id;
}

DataSliceImpl DataSliceImpl::Builder::Build() && {
  auto& impl = *slice_.internal_;
#ifndef NDEBUG
  size_t bitmap_size = arolla::bitmap::BitmapSize(size());
  std::vector<arolla::bitmap::Word> present_ids(bitmap_size);
  auto check_duplicated_ids = [&](const auto& array) {
    for (size_t i = 0; i < bitmap_size; ++i) {
      arolla::bitmap::Word word = arolla::bitmap::GetWordWithOffset(
          array.bitmap, i, array.bitmap_bit_offset);
      if (present_ids[i] & word) {
        return true;
      }
      present_ids[i] |= word;
    }
    return false;
  };
  uint32_t used_type_ids_mask = 0;
  for (const auto& var : impl.values) {
    std::visit(
        [&](const auto& array) {
          constexpr int8_t type_id =
              ScalarTypeId<typename std::decay_t<decltype(array)>::base_type>();
          DCHECK_EQ((used_type_ids_mask >> type_id) & 1U, 0);
          used_type_ids_mask |= (1U << type_id);
          DCHECK_NE(type_id, current_type_id_);
          DCHECK(!bldrs_.contains(type_id));
          DCHECK(!check_duplicated_ids(array));
        },
        var);
  }
#endif  // NDEBUG
  auto visitor = [&](auto bldr) {
    if constexpr (std::is_same_v<std::decay_t<decltype(bldr)>,
                                 std::monostate>) {
      DCHECK(false);
    } else {
#ifndef NDEBUG
      auto array = std::move(bldr).Build();
      DCHECK(!check_duplicated_ids(array));
      impl.values.emplace_back(std::move(array));
#else
      impl.values.emplace_back(std::move(bldr).Build());
#endif  // NDEBUG
      impl.dtype = arolla::GetQType<typename decltype(bldr)::base_type>();
    }
  };
  if (current_type_id_ != ScalarTypeId<MissingValue>()) {
    DCHECK(!std::holds_alternative<std::monostate>(*current_bldr_));
    std::visit(visitor, std::move(*current_bldr_));
    if (!bldrs_.empty()) {
      for (auto& [type_id, bldr] : bldrs_) {
        if (type_id != current_type_id_) {
          std::visit(visitor, std::move(bldr));
        }
      }
    }
  }
  if (impl.values.size() != 1) {
    slice_.RemoveEmptyValues();
  }
  return std::move(slice_);
}

bool DataSliceImpl::IsEquivalentTo(const DataSliceImpl& other) const {
  if (this == &other) {
    return true;
  }
  if (size() != other.size()) {
    return false;
  }
  if (!is_mixed_dtype() && !is_empty_and_unknown() &&
      dtype() == other.dtype()) {
    return std::visit(
        [&](const auto& arr) {
          using ArrT = std::decay_t<decltype(arr)>;
          return arolla::ArraysAreEquivalent(
              arr, std::get<ArrT>(other.internal_->values[0]));
        },
        internal_->values[0]);
  }
  // Compare pointwise because `internal_->values` and
  // `other.internal->values` can have different order.
  for (int64_t i = 0; i < size(); ++i) {
    if (!(*this)[i].IsEquivalentTo(other[i])) return false;
  }
  return true;
}

}  // namespace koladata::internal

namespace arolla {

ReprToken ReprTraits<::koladata::internal::DataSliceImpl>::operator()(
    const ::koladata::internal::DataSliceImpl& value) const {
  return ReprToken{absl::StrCat(value)};
}

AROLLA_DEFINE_SIMPLE_QTYPE(INTERNAL_DATA_SLICE,
                           ::koladata::internal::DataSliceImpl);

}  // namespace arolla
