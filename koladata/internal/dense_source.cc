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
#include "koladata/internal/dense_source.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/types.h"
#include "koladata/internal/value_array.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/quote.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"  // IWYU pragma: keep
#include "arolla/util/meta.h"
#include "arolla/util/status.h"
#include "arolla/util/text.h"  // IWYU pragma: keep
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"

namespace koladata::internal {
namespace {

using ::arolla::DenseArray;

ABSL_ATTRIBUTE_NOINLINE void UpdateMergeConflictStatusWithDataItem(
    absl::Status& status, const DataItem& value, const DataItem& other_value) {
  if (!status.ok()) {
    return;
  }
  status = absl::FailedPreconditionError(
      absl::StrCat("merge conflict: ", value, " != ", other_value));
}

template <class T>
ABSL_ATTRIBUTE_NOINLINE void UpdateMergeConflictStatus(
    absl::Status& status, arolla::view_type_t<T> value,
    arolla::view_type_t<T> other_value) {
  if (!status.ok()) {
    return;
  }
  UpdateMergeConflictStatusWithDataItem(status, DataItem(T(value)),
                                        DataItem(T(other_value)));
}

// It is batch version of ValueArray::Set. Implemented as a free function
// because it is common for all ValueArray implementations.
// `objects` and `values` must have the same size.
template <typename ValueArray, typename T>
void ValueArraySet(AllocationId alloc_id, ValueArray& data,
                   const ObjectIdArray& objects, const DenseArray<T>& values) {
  DCHECK_EQ(objects.size(), values.size());
  if (values.bitmap.empty()) {
    auto values_iter = values.values.begin();
    objects.ForEach([&](int64_t id, bool present, ObjectId object) {
      if (present && alloc_id.Contains(object)) {
        data.Set(object.Offset(), values_iter[id]);
      }
    });
    return;
  }

  using view_type = arolla::view_type_t<T>;
  arolla::DenseArraysForEachPresent(
      [&](int64_t id, ObjectId object, arolla::OptionalValue<view_type> value) {
        if (alloc_id.Contains(object)) {
          int64_t offset = object.Offset();
          if (value.present) {
            data.Set(offset, value.value);
          } else {
            data.Unset(offset);
          }
        }
      },
      objects, values)
      .IgnoreError();  // we DCHECK object.size() == values.size() above.
}

template <typename ValueArray>
void ValueArrayUnset(AllocationId alloc_id, ValueArray& data,
                     const ObjectIdArray& objects) {
  objects.ForEach([&](int64_t id, bool present, ObjectId obj) {
    if (present && alloc_id.Contains(obj)) {
      data.Unset(obj.Offset());
    }
  });
}

template <bool can_be_mutable>
class MultitypeDenseSource : public DenseSource {
 public:
  using ValueArrayVariant = std::variant<                   //
      ValueArray<ObjectId, can_be_mutable>,                 //
      ValueArray<int32_t, can_be_mutable>,                  //
      ValueArray<int64_t, can_be_mutable>,                  //
      ValueArray<float, can_be_mutable>,                    //
      ValueArray<double, can_be_mutable>,                   //
      ValueArray<bool, can_be_mutable>,                     //
      ValueArray<arolla::Unit, can_be_mutable>,             //
      ValueArray<arolla::Text, can_be_mutable>,             //
      ValueArray<arolla::Bytes, can_be_mutable>,            //
      ValueArray<arolla::expr::ExprQuote, can_be_mutable>,  //
      ValueArray<schema::DType, can_be_mutable>>;

  // Constructs immutable DenseSource
  MultitypeDenseSource(AllocationId obj_allocation_id,
                       const DataSliceImpl& values)
      : is_mutable_(false),
        obj_allocation_id_(obj_allocation_id),
        size_(values.size()),
        attr_allocation_ids_(values.allocation_ids()) {
    DCHECK_GT(size_, 0);
    DCHECK_LE(values.size(), obj_allocation_id_.Capacity());
    if constexpr (!can_be_mutable) {
      values.VisitValues([&](const auto& array) {
        using T = typename std::decay_t<decltype(array)>::base_type;
        values_.emplace_back(ValueArray<T, /*can_be_mutable=*/false>(array));
      });
    }
  }

  // Constructs mutable DenseSource
  explicit MultitypeDenseSource(AllocationId obj_allocation_id, int64_t size)
      : is_mutable_(true),
        obj_allocation_id_(obj_allocation_id),
        size_(size),
        attr_allocation_ids_() {
    DCHECK_GT(size, 0);
    DCHECK_LE(size, obj_allocation_id_.Capacity());
  }

  AllocationId allocation_id() const final { return obj_allocation_id_; }
  int64_t size() const final { return size_; }

  DataItem Get(ObjectId object) const final {
    DCHECK(obj_allocation_id_.Contains(object));
    int64_t offset = object.Offset();
    DataItem res;
    for (const ValueArrayVariant& var : values_) {
      std::visit(
          [&](const auto& value_array) {
            if (auto v = value_array.Get(offset); v.present) {
              using T = typename std::decay_t<decltype(value_array)>::base_type;
              res = DataItem(T(v.value));
            }
          },
          var);
    }
    return res;
  }

  DataSliceImpl Get(const ObjectIdArray& objects,
                    bool check_alloc_id) const final {
    SliceBuilder bldr(objects.size());
    bldr.GetMutableAllocationIds().Insert(attr_allocation_ids_);
    for (const ValueArrayVariant& var : values_) {
      std::visit(
          [&](const auto& value_array) {
            using T = typename std::decay_t<decltype(value_array)>::base_type;
            auto array = check_alloc_id ? value_array.template Get<true>(
                                               objects, obj_allocation_id_)
                                         : value_array.template Get<false>(
                                               objects, obj_allocation_id_);
            bldr.InsertIfNotSet<T>(array.bitmap, arolla::bitmap::Bitmap(),
                                   array.values);
          },
          var);
    }
    return std::move(bldr).Build();
  }

  void Get(absl::Span<const ObjectId> objects, SliceBuilder& bldr) const final {
    bldr.GetMutableAllocationIds().Insert(attr_allocation_ids_);
    for (const ValueArrayVariant& var : values_) {
      std::visit(
          [&](const auto& value_array) {
            using T = typename std::decay_t<decltype(value_array)>::base_type;
            auto typed_bldr = bldr.typed<T>();
            for (int64_t i = 0; i < objects.size(); ++i) {
              if (typed_bldr.IsSet(i)) {
                continue;
              }
              ObjectId id = objects[i];
              if (obj_allocation_id_.Contains(id)) {
                auto v = value_array.Get(id.Offset());
                if (v.present) {
                  typed_bldr.InsertIfNotSet(i, v.value);
                }
              }
            }
          },
          var);
    }
  }

  DataSliceImpl GetAll() const final {
    SliceBuilder bldr(size_);
    bldr.GetMutableAllocationIds().Insert(attr_allocation_ids_);
    for (const ValueArrayVariant& var : values_) {
      std::visit(
          [&](const auto& value_array) {
            using T = typename std::decay_t<decltype(value_array)>::base_type;
            auto&& data = value_array.GetAll();
            bldr.InsertIfNotSet<T>(data.bitmap, arolla::bitmap::Bitmap(),
                                   data.values);
          },
          var);
    }
    return std::move(bldr).Build();
  }

  bool IsMutable() const final { return is_mutable_; }

  absl::Status Set(ObjectId object, const DataItem& value) final {
    if (!IsMutable()) {
      return absl::FailedPreconditionError(
          "SetAttr is not allowed for an immutable DenseSource.");
    }
    if (!obj_allocation_id_.Contains(object)) {
      return absl::OkStatus();
    }
    if (value.holds_value<ObjectId>()) {
      attr_allocation_ids_.Insert(AllocationId(value.value<ObjectId>()));
    }
    if constexpr (can_be_mutable) {
      int64_t offset = object.Offset();
      bool found = false;
      for (ValueArrayVariant& var : values_) {
        std::visit(
            [&](auto& value_array) {
              using DstT =
                  typename std::decay_t<decltype(value_array)>::base_type;
              value.VisitValue([&](const auto& v) {
                using SrcT = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<DstT, SrcT>) {
                  found = true;
                  value_array.Set(offset, v);
                } else {
                  value_array.Unset(offset);
                }
              });
            },
            var);
      }
      if (!found) {
        value.VisitValue([&](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (!std::is_same_v<T, MissingValue>) {
            ValueArray<T, /*can_be_mutable=*/true> arr(size_);
            arr.Set(offset, v);
            values_.emplace_back(std::move(arr));
          }
        });
      }
    }
    return absl::OkStatus();
  }

  absl::Status Set(const ObjectIdArray& objects,
                   const DataSliceImpl& values) final {
    if (!IsMutable()) {
      return absl::FailedPreconditionError(
          "SetAttr is not allowed for an immutable DenseSource.");
    }
    if constexpr (can_be_mutable) {
      if (objects.size() != values.size()) {
        return arolla::SizeMismatchError(
            {objects.size(), static_cast<int64_t>(values.size())});
      }
      attr_allocation_ids_.Insert(values.allocation_ids());

      std::vector<bool> updated(values_.size(), false);
      values.VisitValues([&](const auto& array) {
        using T = typename std::decay_t<decltype(array)>::base_type;
        bool found = false;
        for (int i = 0; i < updated.size(); ++i) {
          if (std::holds_alternative<ValueArray<T, true>>(values_[i])) {
            ValueArraySet(obj_allocation_id_,
                          std::get<ValueArray<T, true>>(values_[i]), objects,
                          array);
            updated[i] = true;
            found = true;
          }
        }
        if (!found) {
          ValueArray<T, /*can_be_mutable=*/true> arr(size_);
          ValueArraySet(obj_allocation_id_, arr, objects, array);
          values_.emplace_back(std::move(arr));
        }
      });
      for (int i = 0; i < updated.size(); ++i) {
        if (!updated[i]) {
          std::visit(
              [&](auto& value_array) {
                ValueArrayUnset(obj_allocation_id_, value_array, objects);
              },
              values_[i]);
        }
      }
    }
    return absl::OkStatus();
  }

  absl::Status SetUnitAndUpdateMissingObjects(
      const ObjectIdArray& objects,
      std::vector<ObjectId>& missing_objects) final {
    return absl::FailedPreconditionError(
        "SetUnitAndUpdateMissingObjects is not allowed for a multitype "
        "DenseSource.");
  }

  absl::Status SetAllSkipMissing(const DataSliceImpl& values,
                                 ConflictHandlingOption option) final {
    if (!IsMutable()) {
      return absl::FailedPreconditionError(
          "SetAttr is not allowed for an immutable DenseSource.");
    }
    absl::Status status = absl::OkStatus();
    if constexpr (can_be_mutable) {
      if (option == ConflictHandlingOption::kOverwrite) {
        OverwriteAllSkipMissing(values);
        return absl::OkStatus();
      }

      std::vector<arolla::bitmap::Word> presence_vec(
          arolla::bitmap::BitmapSize(size_));
      arolla::bitmap::Word* presence = presence_vec.data();
      for (ValueArrayVariant& vals : values_) {
        std::visit([&](const auto& av) { av.ReadBitmapOr(presence); }, vals);
      }
      attr_allocation_ids_.Insert(values.allocation_ids());
      values.VisitValues([&](const auto& array) {
        auto sliced_array = array.MakeUnowned().Slice(
            0, std::min<int64_t>(array.size(), size_));
        using T = typename std::decay_t<decltype(array)>::base_type;
        bool found_the_same_type = false;
        for (ValueArrayVariant& vals : values_) {
          if (std::holds_alternative<ValueArray<T, true>>(vals)) {
            auto& dst_vals = std::get<ValueArray<T, true>>(vals);
            if (option == ConflictHandlingOption::kKeepOriginal) {
              sliced_array.ForEachPresent(
                  [&](int64_t offset, arolla::view_type_t<T> value) {
                    if (!arolla::bitmap::GetBit(presence, offset)) {
                      dst_vals.Set(offset, value);
                    }
                  });
            } else {  // kRaiseOnConflict
              sliced_array.ForEachPresent(
                  [&](int64_t offset, arolla::view_type_t<T> value) {
                    if (!arolla::bitmap::GetBit(presence, offset)) {
                      dst_vals.Set(offset, value);
                    } else if (value != dst_vals.Get(offset)) {
                      UpdateMergeConflictStatus<T>(status, value,
                                                   dst_vals.Get(offset).value);
                    }
                  });
            }
            found_the_same_type = true;
            continue;
          }
          if (option == ConflictHandlingOption::kRaiseOnConflict) {
            // `vals` and `array` have different type, so if some index present
            // in both, it is merge conflict.
            std::visit(
                [&](auto& dst_vals) {
                  using VT =
                      typename std::decay_t<decltype(dst_vals)>::base_type;
                  sliced_array.ForEachPresent(
                      [&](int64_t offset, arolla::view_type_t<T> value) {
                        if (dst_vals.Get(offset).present) {
                          UpdateMergeConflictStatusWithDataItem(
                              status, DataItem(T(value)),
                              DataItem(VT(dst_vals.Get(offset).value)));
                        }
                      });
                },
                vals);
          }
        }
        if (!found_the_same_type) {
          bool non_empty = false;
          ValueArray<T, /*can_be_mutable=*/true> dst_vals(size_);
          sliced_array.ForEachPresent(
              [&](int64_t offset, arolla::view_type_t<T> value) {
                if (!arolla::bitmap::GetBit(presence, offset)) {
                  dst_vals.Set(offset, value);
                  non_empty = true;
                }
              });
          if (non_empty) {
            values_.emplace_back(std::move(dst_vals));
          }
        }
      });
    }
    return status;
  }

  std::shared_ptr<DenseSource> CreateMutableCopy() const final {
    auto res = std::make_shared<MultitypeDenseSource</*can_be_mutable=*/true>>(
        obj_allocation_id_, size_);
    res->attr_allocation_ids_ = attr_allocation_ids_;
    for (const auto& val_arr_variant : values_) {
      std::visit(
          [&res](const auto& val_arr) {
            res->values_.emplace_back(val_arr.CreateMutableCopy());
          },
          val_arr_variant);
    }
    return res;
  }

 private:
  friend class MultitypeDenseSource</*can_be_mutable=*/false>;

  template <class T, class ValueArray>
  friend class TypedDenseSource;

  void OverwriteAllSkipMissing(const DataSliceImpl& values) {
    if constexpr (can_be_mutable) {
      attr_allocation_ids_.Insert(values.allocation_ids());
      values.VisitValues([&](const auto& array) {
        auto sliced_array = array.MakeUnowned().Slice(
            0, std::min<int64_t>(array.size(), size_));
        using T = typename std::decay_t<decltype(array)>::base_type;
        bool found_the_same_type = false;
        for (ValueArrayVariant& vals : values_) {
          if (std::holds_alternative<ValueArray<T, true>>(vals)) {
            auto& dst_vals = std::get<ValueArray<T, true>>(vals);
            sliced_array.ForEachPresent(
                [&](int64_t offset, arolla::view_type_t<T> value) {
                  dst_vals.Set(offset, value);
                });
            found_the_same_type = true;
          } else {
            std::visit(
                [&](auto& dst_vals) {
                  sliced_array.ForEachPresent(
                      [&](int64_t offset, arolla::view_type_t<T> value) {
                        dst_vals.Unset(offset);
                      });
                },
                vals);
          }
        }
        if (!found_the_same_type) {
          ValueArray<T, /*can_be_mutable=*/true> dst_vals(size_);
          sliced_array.ForEachPresent(
              [&](int64_t offset, arolla::view_type_t<T> value) {
                dst_vals.Set(offset, value);
              });
          values_.emplace_back(std::move(dst_vals));
        }
      });
    }
  }

  bool is_mutable_;

  // AllocationId of `objects` for which this DenseSource has values.
  AllocationId obj_allocation_id_;
  int64_t size_;

  // List of AllocationId of ObjectIds that are (if any) are stored in this
  // DenseSource as values. When we remove a value we don't recalculate
  // the list, so it potentially can contain more AllocationIds that are
  // actually used.
  AllocationIdSet attr_allocation_ids_;

  // `values_` contain one ValueArray per value type. We reserve size 2, because
  // for a single type there is an optimized class `TypedDenseSource`.
  absl::InlinedVector<ValueArrayVariant, 2> values_;
};

template <class T, class ValueArray>
class TypedDenseSource final : public DenseSource {
 public:
  using view_type = arolla::view_type_t<T>;

  // Constructs immutable DenseSource
  TypedDenseSource(AllocationId obj_allocation_id,
                   AllocationIdSet attr_allocation_ids, DenseArray<T> values)
      : obj_allocation_id_(obj_allocation_id), values_(std::move(values)) {
    if constexpr (std::is_same_v<T, ObjectId>) {
      attr_allocation_ids_ = std::move(attr_allocation_ids);
    }
  }

  // Constructs mutable DenseSource
  explicit TypedDenseSource(AllocationId obj_allocation_id, int64_t size)
      : obj_allocation_id_(obj_allocation_id),
        attr_allocation_ids_(),
        values_(size) {
    DCHECK_LE(size, obj_allocation_id_.Capacity());
  }

  TypedDenseSource(AllocationId obj_allocation_id,
                   AllocationIdSet attr_allocation_ids, ValueArray values)
      : obj_allocation_id_(obj_allocation_id), values_(std::move(values)) {
    if constexpr (std::is_same_v<T, ObjectId>) {
      attr_allocation_ids_ = std::move(attr_allocation_ids);
    }
  }

  AllocationId allocation_id() const final { return obj_allocation_id_; }
  int64_t size() const final { return values_.size(); }

  DataItem Get(ObjectId object) const final {
    if (multitype_) {
      return multitype_->Get(object);
    }
    DCHECK(obj_allocation_id_.Contains(object));
    int64_t offset = object.Offset();
    arolla::OptionalValue<view_type> v = values_.Get(offset);
    if (v.present) {
      return DataItem(T(v.value));
    } else {
      return DataItem();
    }
  }

  DataSliceImpl Get(const ObjectIdArray& objects,
                    bool check_alloc_id) const final {
    if (multitype_) {
      return multitype_->Get(objects, check_alloc_id);
    }
    auto res = check_alloc_id
                   ? values_.template Get<true>(objects, obj_allocation_id_)
                   : values_.template Get<false>(objects, obj_allocation_id_);
    if constexpr (std::is_same_v<T, ObjectId>) {
      return DataSliceImpl::CreateWithAllocIds(attr_allocation_ids_,
                                               std::move(res));
    } else {
      return DataSliceImpl::Create(std::move(res));
    }
  }

  void Get(absl::Span<const ObjectId> objects, SliceBuilder& bldr) const final {
    if (multitype_) {
      return multitype_->Get(objects, bldr);
    }
    if constexpr (std::is_same_v<T, ObjectId>) {
      bldr.GetMutableAllocationIds().Insert(attr_allocation_ids_);
    }
    auto typed_bldr = bldr.typed<T>();
    for (int64_t i = 0; i < objects.size(); ++i) {
      if (typed_bldr.IsSet(i)) {
        continue;
      }
      ObjectId id = objects[i];
      if (obj_allocation_id_.Contains(id)) {
        typed_bldr.InsertIfNotSet(i, values_.Get(id.Offset()));
      }
    }
  }

  DataSliceImpl GetAll() const final {
    if (multitype_) {
      return multitype_->GetAll();
    }
    return DataSliceImpl::CreateWithAllocIds(attr_allocation_ids_,
                                             values_.GetAll());
  }

  bool IsMutable() const final { return values_.IsMutable() || multitype_; }

  absl::Status Set(ObjectId object, const DataItem& value) final {
    if (multitype_) {
      return multitype_->Set(object, value);
    }
    if (!IsMutable()) {
      return absl::FailedPreconditionError(
          "SetAttr is not allowed for an immutable DenseSource.");
    }
    if (!obj_allocation_id_.Contains(object)) {
      return absl::OkStatus();
    }
    int64_t offset = object.Offset();
    if (value.has_value()) {
      if (!value.holds_value<T>()) {
        CreateMultitype();
        return multitype_->Set(object, value);
      }
      const T& v = value.value<T>();
      if constexpr (std::is_same_v<T, ObjectId>) {
        attr_allocation_ids_.Insert(AllocationId(v));
      }
      values_.Set(offset, v);
    } else {
      values_.Unset(offset);
    }
    return absl::OkStatus();
  }

  absl::Status Set(const ObjectIdArray& objects,
                   const DataSliceImpl& values) final {
    if (!IsMutable() && !multitype_) {
      return absl::FailedPreconditionError(
          "SetAttr is not allowed for an immutable DenseSource.");
    }
    if (objects.size() != values.size()) {
      return arolla::SizeMismatchError(
          {objects.size(), static_cast<int64_t>(values.size())});
    }
    if (multitype_) {
      return multitype_->Set(objects, values);
    }
    if (values.is_empty_and_unknown()) {
      ValueArrayUnset(obj_allocation_id_, values_, objects);
      return absl::OkStatus();
    }
    if (values.is_mixed_dtype() || values.dtype() != arolla::GetQType<T>()) {
      CreateMultitype();
      return multitype_->Set(objects, values);
    }
    if constexpr (std::is_same_v<T, ObjectId>) {
      attr_allocation_ids_.Insert(values.allocation_ids());
    }
    const DenseArray<T>& values_array = values.values<T>();

    ValueArraySet(obj_allocation_id_, values_, objects, values_array);
    return absl::OkStatus();
  }

  absl::Status SetUnitAndUpdateMissingObjects(
      const ObjectIdArray& objects,
      std::vector<ObjectId>& missing_objects) final {
    // TODO: support a flag to mark that all objects are from the
    // current allocation.
    if (!IsMutable()) {
      return absl::FailedPreconditionError(
          "SetUnitAndUpdateMissingObjects is not allowed for an immutable "
          "DenseSource.");
    }
    if (multitype_) {
      return absl::FailedPreconditionError(
          "SetUnitAndUpdateMissingObjects is not allowed for a multitype "
          "DenseSource.");
    }
    if (!std::is_same_v<T, arolla::Unit>) {
      return absl::FailedPreconditionError(
          "SetUnitAndUpdateMissingObjects is only allowed for OptionalUnit "
          "DenseSource.");
    }
    if constexpr (std::is_same_v<T, arolla::Unit>) {
      objects.ForEach([&](int64_t id, bool present, ObjectId object) {
        if (present && obj_allocation_id_.Contains(object) &&
            !values_.Get(object.Offset()).present) {
          values_.Set(object.Offset(), arolla::kUnit);
          missing_objects.push_back(object);
        }
      });
    }
    return absl::OkStatus();
  }

  absl::Status SetAllSkipMissing(const DataSliceImpl& values,
                                 ConflictHandlingOption option) final {
    if (!IsMutable() && !multitype_) {
      return absl::FailedPreconditionError(
          "SetAttr is not allowed for an immutable DenseSource.");
    }
    if (multitype_ || values.is_mixed_dtype() ||
        values.dtype() != arolla::GetQType<T>()) {
      if (!multitype_) {
        CreateMultitype();
      }
      return multitype_->SetAllSkipMissing(values, option);
    }
    if constexpr (std::is_same_v<T, ObjectId>) {
      attr_allocation_ids_.Insert(values.allocation_ids());
    }
    const DenseArray<T>& values_array = values.values<T>();
    auto sliced_array = values_array.MakeUnowned().Slice(
        0, std::min<int64_t>(values_array.size(), values_.size()));
    switch (option) {
      case DenseSource::ConflictHandlingOption::kOverwrite:
        values_.MergeOverwrite(sliced_array);
        return absl::OkStatus();
      case DenseSource::ConflictHandlingOption::kKeepOriginal:
        values_.MergeKeepOriginal(sliced_array);
        return absl::OkStatus();
      case DenseSource::ConflictHandlingOption::kRaiseOnConflict:
        return values_.MergeRaiseOnConflict(sliced_array,
                                            UpdateMergeConflictStatus<T>);
    }
    ABSL_UNREACHABLE();
  }

  std::shared_ptr<DenseSource> CreateMutableCopy() const final;

 private:
  template <class OtherT, class OtherValueArray>
  friend class TypedDenseSource;

  void CreateMultitype() {
    if constexpr (std::is_same_v<ValueArray,
                                 decltype(values_.CreateMutableCopy())>) {
      multitype_ =
          std::make_unique<MultitypeDenseSource</*can_be_mutable=*/true>>(
              obj_allocation_id_, size());
      multitype_->attr_allocation_ids_ = std::move(attr_allocation_ids_);
      multitype_->values_.emplace_back(std::move(values_));
    }
  }

  AllocationId obj_allocation_id_;
  AllocationIdSet attr_allocation_ids_;
  ValueArray values_;
  std::unique_ptr<MultitypeDenseSource</*can_be_mutable=*/true>> multitype_;
};

template <class T, class ValueArray>
std::shared_ptr<DenseSource>
TypedDenseSource<T, ValueArray>::CreateMutableCopy() const {
  if (multitype_) {
    return multitype_->CreateMutableCopy();
  } else {
    using can_be_mutableValueArray = decltype(values_.CreateMutableCopy());
    return std::make_shared<TypedDenseSource<T, can_be_mutableValueArray>>(
        obj_allocation_id_, attr_allocation_ids_, values_.CreateMutableCopy());
  }
}

}  // namespace

absl::StatusOr<std::shared_ptr<DenseSource>> DenseSource::CreateReadonly(
    AllocationId alloc, const DataSliceImpl& data) {
  if (data.size() > alloc.Capacity()) {
    return absl::FailedPreconditionError(absl::StrCat(
        "data slice exceed capacity: ", data.size(), " > ", alloc.Capacity()));
  }
  if (data.is_empty_and_unknown()) {
    return absl::FailedPreconditionError(
        "Empty and unknown slices should be handled at a higher-level: "
        "DataBagImpl::SourceCollection");
  }
  if (data.is_mixed_dtype()) {
    return std::make_shared<MultitypeDenseSource<false>>(alloc, data);
  }
  std::shared_ptr<DenseSource> res = nullptr;
  data.VisitValues([&](const auto& array) {
    using T = typename std::decay_t<decltype(array)>::base_type;
    res = std::make_shared<
        TypedDenseSource<T, ValueArray<T, /*can_be_mutable=*/false>>>(
        alloc, data.allocation_ids(), array);
  });
  if (!res) {
    return absl::UnimplementedError(
        absl::StrCat("Unsupported type: ", data.dtype()->name()));
  }
  return res;
}

absl::StatusOr<std::shared_ptr<DenseSource>> DenseSource::CreateMutable(
    AllocationId alloc, int64_t size,
    absl::Nullable<const arolla::QType*> main_type) {
  if (main_type == nullptr) {
    return std::make_shared<MultitypeDenseSource<true>>(alloc, size);
  }
  std::shared_ptr<DenseSource> res;
  arolla::meta::foreach_type<supported_types_list>([&](auto meta_type) {
    using T = typename decltype(meta_type)::type;
    if (main_type == arolla::GetQType<T>()) {
      res = std::make_shared<
          TypedDenseSource<T, ValueArray<T, /*can_be_mutable=*/true>>>(alloc,
                                                                       size);
    }
  });
  if (!res) {
    return absl::UnimplementedError(
        absl::StrCat("Unsupported type: ", main_type->name()));
  }
  return res;
}

}  // namespace koladata::internal
