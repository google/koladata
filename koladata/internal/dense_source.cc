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
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/types.h"
#include "koladata/internal/types_buffer.h"
#include "koladata/internal/value_array.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/quote.h"
#include "arolla/memory/buffer.h"
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
using ::arolla::OptionalValue;
using ::arolla::view_type_t;

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
    absl::Status& status, view_type_t<T> value,
    view_type_t<T> other_value) {
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

  using view_type = view_type_t<T>;
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

class MultitypeDenseSource : public DenseSource {
 public:
  using ValueBufferVariant = std::variant<   //
      ValueBuffer<ObjectId>,                 //
      ValueBuffer<int32_t>,                  //
      ValueBuffer<int64_t>,                  //
      ValueBuffer<float>,                    //
      ValueBuffer<double>,                   //
      ValueBuffer<bool>,                     //
      ValueBuffer<arolla::Unit>,             //
      ValueBuffer<arolla::Text>,             //
      ValueBuffer<arolla::Bytes>,            //
      ValueBuffer<arolla::expr::ExprQuote>,  //
      ValueBuffer<schema::DType>>;

  explicit MultitypeDenseSource(AllocationId obj_allocation_id, int64_t size)
      : obj_allocation_id_(obj_allocation_id),
        size_(size),
        attr_allocation_ids_() {
    DCHECK_GT(size, 0);
    DCHECK_LE(size, obj_allocation_id_.Capacity());
    types_buffer_.InitAllUnset(size);
  }

  AllocationId allocation_id() const final { return obj_allocation_id_; }
  int64_t size() const final { return size_; }

  DataItem Get(ObjectId object) const final {
    DCHECK(obj_allocation_id_.Contains(object) && object.Offset() < size_);
    int64_t offset = object.Offset();
    uint8_t tidx = types_buffer_.id_to_typeidx[offset];
    if (!TypesBuffer::is_present_type_idx(tidx)) {
      return DataItem();
    }
    return std::visit([&](const auto& buf) { return DataItem(buf[offset]); },
                      values_[tidx]);
  }

  DataSliceImpl Get(const ObjectIdArray& objects,
                    bool check_alloc_id) const final {
    SliceBuilder bldr(objects.size());
    bldr.ApplyMask(objects.ToMask());
    Get(objects.values.span(), bldr);
    return std::move(bldr).Build();
  }

  void Get(absl::Span<const ObjectId> objects, SliceBuilder& bldr) const final {
    bldr.GetMutableAllocationIds().Insert(attr_allocation_ids_);
    int idx = 0;
    for (const ValueBufferVariant& var : values_) {
      std::visit(
          [&]<class T>(const ValueBuffer<T>& buf) {
            auto typed_bldr = bldr.typed<T>();
            for (int64_t i = 0; i < objects.size(); ++i) {
              ObjectId id = objects[i];
              if (typed_bldr.IsSet(i) || !obj_allocation_id_.Contains(id)) {
                continue;
              }
              int64_t offset = id.Offset();
              if (types_buffer_.id_to_typeidx[offset] == idx) {
                typed_bldr.InsertIfNotSet(i, buf[offset]);
              }
            }
          },
          var);
      idx++;
    }
  }

  DataSliceImpl GetAll() const final {
    // TODO: Instead of DataSliceImpl return a struct with all
    // data for merging. Should be O(type_count).
    SliceBuilder bldr(size_);
    bldr.GetMutableAllocationIds().Insert(attr_allocation_ids_);
    uint8_t idx = 0;
    for (const ValueBufferVariant& var : values_) {
      arolla::bitmap::Bitmap bitmap = types_buffer_.ToBitmap(idx++);
      std::visit(
          [&]<class T>(const ValueBuffer<T>& buf) {
            if constexpr (std::is_same_v<T, arolla::Unit>) {
              bldr.InsertIfNotSet<T>(bitmap, arolla::bitmap::Bitmap(),
                                     arolla::VoidBuffer(size_));
            } else if constexpr (std::is_same_v<view_type_t<T>,
                                                absl::string_view>) {
              bldr.InsertIfNotSet<T>(
                  bitmap, arolla::bitmap::Bitmap(),
                  arolla::StringsBuffer::Create(buf.begin(), buf.end()));
            } else {
              bldr.InsertIfNotSet<T>(bitmap, arolla::bitmap::Bitmap(),
                                     arolla::Buffer<T>(nullptr, buf));
            }
          },
          var);
    }
    return std::move(bldr).Build();
  }

  bool IsMutable() const final { return true; }

  absl::Status Set(ObjectId object, const DataItem& value) final {
    if (!obj_allocation_id_.Contains(object)) {
      return absl::OkStatus();
    }
    if (value.holds_value<ObjectId>()) {
      attr_allocation_ids_.Insert(AllocationId(value.value<ObjectId>()));
    }
    int64_t offset = object.Offset();
    if (!value.has_value()) {
      types_buffer_.id_to_typeidx[offset] = TypesBuffer::kRemoved;
      return absl::OkStatus();
    }
    uint8_t typeidx = types_buffer_.get_or_add_typeidx(value.type_id());
    types_buffer_.id_to_typeidx[offset] = typeidx;
    DCHECK_LE(typeidx, values_.size());
    value.VisitValue([&]<class T>(const T& v) {
      if constexpr (!std::is_same_v<T, MissingValue>) {
        if (typeidx == values_.size()) {
          values_.emplace_back(ValueBuffer<T>(size_));
        }
        if constexpr (!std::is_same_v<T, arolla::Unit>) {
          std::get<ValueBuffer<T>>(values_[typeidx])[offset] = v;
        }
      }
    });
    return absl::OkStatus();
  }

  absl::Status Set(const ObjectIdArray& objects,
                   const DataSliceImpl& values) final {
    if (objects.size() != values.size()) {
      return arolla::SizeMismatchError(
          {objects.size(), static_cast<int64_t>(values.size())});
    }
    if (values.is_empty_and_unknown()) {
      objects.ForEachPresent([&](int64_t id, ObjectId object) {
        if (obj_allocation_id_.Contains(object)) {
          types_buffer_.id_to_typeidx[object.Offset()] = TypesBuffer::kRemoved;
        }
      });
      return absl::OkStatus();
    }
    attr_allocation_ids_.Insert(values.allocation_ids());

    if (values.is_single_dtype()) {
      values.VisitValues([&]<class T>(const DenseArray<T>& array) {
        SetImpl</*RemoveMissing=*/true>(objects, array);
      });
      return absl::OkStatus();
    }

    values.VisitValues([&]<class T>(const DenseArray<T>& array) {
      SetImpl</*RemoveMissing=*/false>(objects, array);
    });
    const uint8_t* val_typeidx = values.types_buffer().id_to_typeidx.data();
    objects.ForEachPresent([&](int64_t val_offset, ObjectId id) {
      if (obj_allocation_id_.Contains(id) &&
          !TypesBuffer::is_present_type_idx(val_typeidx[val_offset])) {
        types_buffer_.id_to_typeidx[id.Offset()] = TypesBuffer::kRemoved;
      }
    });

    return absl::OkStatus();
  }

  absl::Status SetUnitAndUpdateMissingObjects(
      const ObjectIdArray& objects,
      std::vector<ObjectId>& missing_objects) final {
    return absl::FailedPreconditionError(
        "SetUnitAndUpdateMissingObjects is not allowed for a multitype "
        "DenseSource.");
  }

  absl::Status MergeImpl(const DataSliceImpl& values,
                         ConflictHandlingOption option) final {
    if (values.size() > size_) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "MergeImpl: source size exceeds destination size: %d vs %d",
          values.size(), size_));
    }
    absl::Status status = absl::OkStatus();
    if (values.is_empty_and_unknown()) {
      return status;
    }
    values.VisitValues([&]<class T>(const DenseArray<T>& array) {
      MergeArrayImpl(status, array, option);
    });
    if (values.types_buffer().types.empty()) {
      DCHECK(values.is_single_dtype());
      return status;
    }
    for (int64_t offset = 0; offset < values.size(); ++offset) {
      if (values.types_buffer().id_to_typeidx[offset] !=
          TypesBuffer::kRemoved) {
        continue;
      }
      if (option == ConflictHandlingOption::kOverwrite ||
          !TypesBuffer::is_present_type_idx(
              types_buffer_.id_to_typeidx[offset])) {
        // Note: replacing kUnset with kRemoved is allowed for any `option`.
        types_buffer_.id_to_typeidx[offset] = TypesBuffer::kRemoved;
      } else if (status.ok() &&
                  option == ConflictHandlingOption::kOverwrite) {
        UpdateMergeConflictStatusWithDataItem(
            status, DataItem(),
            Get(obj_allocation_id_.ObjectByOffset(offset)));
      }
    }
    return status;
  }

  std::shared_ptr<DenseSource> CreateMutableCopy() const final {
    auto res =
        std::make_shared<MultitypeDenseSource>(obj_allocation_id_, size_);
    res->attr_allocation_ids_ = attr_allocation_ids_;
    res->types_buffer_ = types_buffer_;
    for (const auto& val_variant : values_) {
      std::visit(
          [&res]<class T>(const ValueBuffer<T>& old_buf) {
            ValueBuffer<T> new_buf(old_buf.size());
            if constexpr (!std::is_same_v<T, arolla::Unit>) {
              std::copy(old_buf.begin(), old_buf.end(), new_buf.begin());
            }
            res->values_.emplace_back(std::move(new_buf));
          },
          val_variant);
    }
    return res;
  }

 private:
  template <class T>
  friend class TypedDenseSource;

  friend class ReadOnlyDenseSource;

  template <class T>
  uint8_t GetOrAddTypeIdx() {
    uint8_t typeidx = types_buffer_.get_or_add_typeidx(ScalarTypeId<T>());
    DCHECK_LE(typeidx, values_.size());
    if (typeidx == values_.size()) {
      values_.emplace_back(ValueBuffer<T>(size_));
    }
    return typeidx;
  }

  template <bool RemoveMissing, class T>
  void SetImpl(const ObjectIdArray& objects, const DenseArray<T>& array) {
    uint8_t typeidx = GetOrAddTypeIdx<T>();
    ValueBuffer<T>& dst_vals = std::get<ValueBuffer<T>>(values_[typeidx]);
    DCHECK_EQ(objects.size(), array.size());
    arolla::DenseArraysForEach(
        [&](int64_t, bool valid, ObjectId id, OptionalValue<view_type_t<T>> v) {
          if (!valid || !obj_allocation_id_.Contains(id)) {
            return;
          }
          int64_t offset = id.Offset();
          if constexpr (RemoveMissing) {
            types_buffer_.id_to_typeidx[offset] =
                v.present ? typeidx : TypesBuffer::kRemoved;
          } else if (v.present) {
            types_buffer_.id_to_typeidx[offset] = typeidx;
          }
          if constexpr (!std::is_same_v<T, view_type_t<T>>) {
            dst_vals[offset] = v.present ? T(v.value) : T();
          } else if constexpr (!std::is_same_v<T, arolla::Unit>) {
            if (v.present) {
              dst_vals[offset] = v.value;
            }
          }
        },
        objects, array)
        .IgnoreError();  // size mismatch should be checked by the caller
  }

  template <class T>
  void MergeArrayImpl(absl::Status& status, const DenseArray<T>& array,
                      ConflictHandlingOption option) {
    uint8_t typeidx = GetOrAddTypeIdx<T>();
    ValueBuffer<T>& dst_vals = std::get<ValueBuffer<T>>(values_[typeidx]);
    if (option == ConflictHandlingOption::kKeepOriginal) {
      array.ForEachPresent([&](int64_t offset, view_type_t<T> v) {
        uint8_t& tidx = types_buffer_.id_to_typeidx[offset];
        if (tidx == TypesBuffer::kUnset) {
          tidx = typeidx;
          dst_vals[offset] = T(v);
        }
      });
    } else if (option == ConflictHandlingOption::kOverwrite) {
      array.ForEachPresent([&](int64_t offset, view_type_t<T> v) {
        types_buffer_.id_to_typeidx[offset] = typeidx;
        dst_vals[offset] = T(v);
      });
    } else {
      DCHECK(option == ConflictHandlingOption::kRaiseOnConflict);
      array.ForEachPresent([&](int64_t offset, view_type_t<T> v) {
        uint8_t& tidx = types_buffer_.id_to_typeidx[offset];
        if (tidx == TypesBuffer::kUnset) {
          tidx = typeidx;
          dst_vals[offset] = T(v);
        } else if (status.ok() && (tidx != typeidx || dst_vals[offset] != v)) {
          UpdateMergeConflictStatusWithDataItem(
              status, DataItem(T(v)),
              Get(obj_allocation_id_.ObjectByOffset(offset)));
        }
      });
    }
  }

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
  absl::InlinedVector<ValueBufferVariant, 2> values_;
  TypesBuffer types_buffer_;
};

template <class T>
class TypedDenseSource final : public DenseSource {
 public:
  using view_type = view_type_t<T>;

  explicit TypedDenseSource(AllocationId obj_allocation_id, int64_t size)
      : obj_allocation_id_(obj_allocation_id),
        attr_allocation_ids_(),
        values_(size) {
    DCHECK_LE(size, obj_allocation_id_.Capacity());
  }

  TypedDenseSource(AllocationId obj_allocation_id,
                   AllocationIdSet attr_allocation_ids, ValueArray<T> values)
      : obj_allocation_id_(obj_allocation_id), values_(std::move(values)) {
    if constexpr (std::is_same_v<T, ObjectId>) {
      attr_allocation_ids_ = std::move(attr_allocation_ids);
    }
  }

  AllocationId allocation_id() const final { return obj_allocation_id_; }
  int64_t size() const final {
    return multitype_ ? multitype_->size() : values_.size();
  }

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

  bool IsMutable() const final { return true; }

  absl::Status Set(ObjectId object, const DataItem& value) final {
    if (multitype_) {
      return multitype_->Set(object, value);
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

  absl::Status MergeImpl(const DataSliceImpl& values,
                         ConflictHandlingOption option) final {
    if (multitype_ || values.is_mixed_dtype() ||
        values.dtype() != arolla::GetQType<T>()) {
      if (!multitype_) {
        CreateMultitype();
      }
      return multitype_->MergeImpl(values, option);
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

  std::shared_ptr<DenseSource> CreateMutableCopy() const final {
    if (multitype_) {
      return multitype_->CreateMutableCopy();
    } else {
      return std::make_shared<TypedDenseSource<T>>(
          obj_allocation_id_, attr_allocation_ids_, values_.Copy());
    }
  }

 private:
  template <class OtherT>
  friend class TypedDenseSource;

  void CreateMultitype() {
    multitype_ =
        std::make_unique<MultitypeDenseSource>(obj_allocation_id_, size());
    multitype_->attr_allocation_ids_ = std::move(attr_allocation_ids_);
    multitype_->types_buffer_.types.push_back(ScalarTypeId<T>());
    multitype_->types_buffer_.id_to_typeidx.resize(values_.size());
    uint8_t* id_to_typeidx = multitype_->types_buffer_.id_to_typeidx.data();
    arolla::bitmap::IterateByGroups(
        values_.presence().data(), 0, values_.size(), [&](int64_t offset) {
          return [id_to_typeidx, offset](int i, bool present) {
            id_to_typeidx[offset + i] = present ? 0 : TypesBuffer::kUnset;
          };
        });
    multitype_->values_.emplace_back(std::move(values_).MoveValues());
  }

  AllocationId obj_allocation_id_;
  AllocationIdSet attr_allocation_ids_;
  ValueArray<T> values_;
  std::unique_ptr<MultitypeDenseSource> multitype_;
};

// Uses DataSliceImpl as data storage. Can be created from existing
// DataSliceImpl without copying data.
class ReadOnlyDenseSource : public DenseSource {
 public:
  ReadOnlyDenseSource(AllocationId alloc, const DataSliceImpl& data)
      : obj_allocation_id_(alloc), data_(data) {}

  AllocationId allocation_id() const final { return obj_allocation_id_; }

  int64_t size() const final { return data_.size(); }

  DataItem Get(ObjectId object) const override {
    DCHECK(data_.is_mixed_dtype())
        << "for single type use TypedReadOnlyDenseSource";
    DCHECK(obj_allocation_id_.Contains(object));
    return data_[object.Offset()];
  }

  DataSliceImpl Get(const ObjectIdArray& objects,
                    bool check_alloc_id) const override {
    SliceBuilder bldr(objects.size());
    bldr.ApplyMask(objects.ToMask());
    Get(objects.values.span(), bldr);
    return std::move(bldr).Build();
  }

  void Get(absl::Span<const ObjectId> objects,
           SliceBuilder& bldr) const override {
    DCHECK(data_.is_mixed_dtype())
        << "for single type use TypedReadOnlyDenseSource";
    DCHECK_EQ(bldr.size(), objects.size());
    bldr.GetMutableAllocationIds().Insert(data_.allocation_ids());
    data_.VisitValues([&]<class T>(const DenseArray<T>& arr) {
      auto typed_bldr = bldr.typed<T>();
      for (int64_t i = 0; i < objects.size(); ++i) {
        ObjectId obj = objects[i];
        if (typed_bldr.IsSet(i) || !obj_allocation_id_.Contains(obj)) {
          continue;
        }
        int64_t offset = obj.Offset();
        if (arr.present(offset)) {
          typed_bldr.InsertIfNotSet(i, arr.values[offset]);
        }
      }
    });
  }

  bool IsMutable() const final { return false; }

  absl::Status Set(ObjectId, const DataItem&) final {
    return absl::FailedPreconditionError(
        "SetAttr is not allowed for an immutable DenseSource.");
  }

  absl::Status Set(const ObjectIdArray&, const DataSliceImpl&) final {
    return absl::FailedPreconditionError(
        "SetAttr is not allowed for an immutable DenseSource.");
  }

  absl::Status SetUnitAndUpdateMissingObjects(const ObjectIdArray&,
                                              std::vector<ObjectId>&) final {
    return absl::FailedPreconditionError(
        "SetAttr is not allowed for an immutable DenseSource.");
  }

  std::shared_ptr<DenseSource> CreateMutableCopy() const override {
    auto res =
        std::make_shared<MultitypeDenseSource>(obj_allocation_id_, size());
    res->attr_allocation_ids_ = data_.allocation_ids();
    res->types_buffer_ = data_.types_buffer();
    data_.VisitValues([&]<class T>(const DenseArray<T>& arr) {
      ValueBuffer<T> buf(arr.size());
      if constexpr (!std::is_same_v<view_type_t<T>, T>) {
        for (int64_t i = 0; i < arr.size(); ++i) {
          buf[i] = arr.values[i];
        }
      } else if constexpr (!std::is_same_v<T, arolla::Unit>) {
        std::copy(arr.values.begin(), arr.values.end(), buf.begin());
      }
      res->values_.emplace_back(std::move(buf));
    });
    return res;
  }

 protected:
  const AllocationIdSet& attr_allocation_ids() const {
    return data_.allocation_ids();
  }

 private:
  DataSliceImpl GetAll() const final { return data_; }

  absl::Status MergeImpl(const DataSliceImpl&, ConflictHandlingOption) final {
    return absl::FailedPreconditionError(
        "Merge is not allowed for an immutable DenseSource.");
  }

  AllocationId obj_allocation_id_;
  DataSliceImpl data_;
};

// ReadOnlyDenseSource that can hold only single-type slices. Its implementation
// of `Get` has less overhead as it works only with `T` and gets data directly
// from `DenseArray<T>`.
template <class T>
class TypedReadOnlyDenseSource final : public ReadOnlyDenseSource {
 public:
  TypedReadOnlyDenseSource(AllocationId alloc, const DataSliceImpl& data)
      : ReadOnlyDenseSource(alloc, data),
        data_(data.values<T>()) {}

  DataItem Get(ObjectId object) const override {
    DCHECK(allocation_id().Contains(object));
    int64_t offset = object.Offset();
    if (data_.present(offset)) {
      return DataItem(T(data_.values[offset]));
    } else {
      return DataItem();
    }
  }

  DataSliceImpl Get(const ObjectIdArray& objects,
                    bool check_alloc_id) const override {
    auto res = check_alloc_id ? value_array_impl::GetByObjOffsets<true>(
                                    data_, objects, allocation_id())
                              : value_array_impl::GetByObjOffsets<false>(
                                    data_, objects, allocation_id());
    if constexpr (std::is_same_v<T, ObjectId>) {
      return DataSliceImpl::CreateWithAllocIds(attr_allocation_ids(),
                                               std::move(res));
    } else {
      return DataSliceImpl::Create(std::move(res));
    }
  }

  void Get(absl::Span<const ObjectId> objects,
           SliceBuilder& bldr) const override {
    DCHECK_EQ(bldr.size(), objects.size());
    bldr.GetMutableAllocationIds().Insert(attr_allocation_ids());
    auto typed_bldr = bldr.typed<T>();
    for (int64_t i = 0; i < objects.size(); ++i) {
      ObjectId obj = objects[i];
      if (bldr.IsSet(i) || !allocation_id().Contains(obj)) {
        continue;
      }
      typed_bldr.InsertIfNotSet(i, data_[obj.Offset()]);
    }
  }

  std::shared_ptr<DenseSource> CreateMutableCopy() const override {
    return std::make_shared<TypedDenseSource<T>>(
        allocation_id(), attr_allocation_ids(), ValueArray<T>(data_));
  }

 private:
  DenseArray<T> data_;
};

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
  if (data.is_single_dtype()) {
    std::shared_ptr<DenseSource> res = nullptr;
    data.VisitValues([&](const auto& array) {
      using T = typename std::decay_t<decltype(array)>::base_type;
      res = std::make_shared<TypedReadOnlyDenseSource<T>>(alloc, data);
    });
    DCHECK(res);
    if (res) {
      return res;
    }
  }
  return std::make_shared<ReadOnlyDenseSource>(alloc, data);
}

absl::StatusOr<std::shared_ptr<DenseSource>> DenseSource::CreateMutable(
    AllocationId alloc, int64_t size,
    absl::Nullable<const arolla::QType*> main_type) {
  if (main_type == nullptr) {
    return std::make_shared<MultitypeDenseSource>(alloc, size);
  }
  std::shared_ptr<DenseSource> res;
  arolla::meta::foreach_type<supported_types_list>([&](auto meta_type) {
    using T = typename decltype(meta_type)::type;
    if (main_type == arolla::GetQType<T>()) {
      res = std::make_shared<TypedDenseSource<T>>(alloc, size);
    }
  });
  if (!res) {
    return absl::UnimplementedError(
        absl::StrCat("Unsupported type: ", main_type->name()));
  }
  return res;
}

}  // namespace koladata::internal
