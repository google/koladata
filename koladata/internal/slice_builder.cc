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
#include "koladata/internal/slice_builder.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <tuple>
#include <utility>
#include <variant>

#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/types/span.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/types.h"
#include "koladata/internal/types_buffer.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/meta.h"

namespace koladata::internal {

SliceBuilder::SliceBuilder(size_t size) {
  types_buffer_.id_to_typeidx.resize(size);
  std::fill(types_buffer_.id_to_typeidx.begin(),
            types_buffer_.id_to_typeidx.end(), TypesBuffer::kUnset);
  unset_count_ = size;
}

std::tuple<bool, uint8_t, DataSliceImpl::Variant>
SliceBuilder::BuildDataSliceVariant(StorageVariant& sv) {
  using Res = std::tuple<bool, uint8_t, DataSliceImpl::Variant>;
  return std::visit(
      [this]<typename TS>(TS& ts) {
        if constexpr (std::is_same_v<TS, std::monostate>) {
          LOG(FATAL) << "Unexpected std::monostate in "
                         "SliceBuilder::BuildDataSliceVariant";
          return Res{false, 0, DataSliceImpl::Variant()};
        } else {
          using T = arolla::meta::strip_template_t<TypedStorage, TS>;
          uint8_t ti = ts.type_index;
          auto bitmap = types_buffer_.ToBitmap(ti);
          if (arolla::bitmap::AreAllBitsUnset(bitmap.begin(), size())) {
            return Res{false, ti, DataSliceImpl::Variant()};
          }
          return Res{
              true, ti,
              arolla::DenseArray<T>{std::move(ts).Build(), std::move(bitmap)}};
        }
      },
      sv);
}

void SliceBuilder::RemoveEmptyTypes(DataSliceImpl::Internal& impl,
                                    absl::Span<uint8_t> idx_to_remove) {
  std::sort(idx_to_remove.begin(), idx_to_remove.end());
  absl::InlinedVector<uint8_t, 8> new_idx(impl.types_buffer.type_count());
  auto to_remove_it = idx_to_remove.begin();
  uint8_t new_tcount = 0;
  for (uint8_t idx = 0; idx < impl.types_buffer.type_count(); ++idx) {
    if (to_remove_it != idx_to_remove.end() && *to_remove_it == idx) {
      new_idx[idx] = TypesBuffer::kUnset;
      to_remove_it++;
    } else {
      new_idx[idx] = new_tcount++;
    }
  }
  for (uint8_t& v : impl.types_buffer.id_to_typeidx) {
    if (v <= impl.types_buffer.type_count()) {
      DCHECK_NE(new_idx[v], TypesBuffer::kUnset);
      v = new_idx[v];
    }
  }
  for (int i = 0; i < impl.types_buffer.type_count(); ++i) {
    if (uint8_t ni = new_idx[i]; ni != TypesBuffer::kUnset) {
      impl.types_buffer.types[ni] = impl.types_buffer.types[i];
      impl.values[ni] = std::move(impl.values[i]);
    }
  }
  impl.types_buffer.types.resize(new_tcount);
  impl.values.resize(new_tcount);
}

DataSliceImpl SliceBuilder::Build() && {
  if (storage_state_ == kStorageEmpty) {
    return DataSliceImpl::CreateEmptyAndUnknownType(std::move(types_buffer_));
  }
  if (storage_state_ == kFirstStorage) {
    auto [present, idx, array_variant] = BuildDataSliceVariant(first_storage_);
    DCHECK_EQ(idx, 0);
    if (!present) {
      return DataSliceImpl::CreateEmptyAndUnknownType(std::move(types_buffer_));
    }
    DataSliceImpl res;
    res.internal_->allocation_ids = std::move(allocation_ids_);
    res.internal_->size = size();
    res.internal_->values.push_back(std::move(array_variant));
    res.internal_->dtype = ScalarTypeIdToQType(types_buffer_.types[0]);
    res.internal_->types_buffer = std::move(types_buffer_);
    DCHECK(res.VerifyAllocIdsConsistency());
    return res;
  }
  DataSliceImpl res;
  res.internal_->allocation_ids = std::move(allocation_ids_);
  res.internal_->size = size();
  KodaTypeId last_present_type_id = 0;
  res.internal_->values.resize(storage_.size());
  absl::InlinedVector<uint8_t, 8> idx_to_remove;
  for (auto& [tid, storage] : storage_) {
    auto [present, idx, array_variant] = BuildDataSliceVariant(storage);
    if (present) {
      res.internal_->values[idx] = std::move(array_variant);
      last_present_type_id = tid;
    } else {
      idx_to_remove.emplace_back(idx);
    }
  }
  res.internal_->types_buffer = std::move(types_buffer_);
  if (!idx_to_remove.empty()) {
    RemoveEmptyTypes(*res.internal_, absl::Span<uint8_t>(idx_to_remove));
  }
  if (res.internal_->values.size() == 1) {
    res.internal_->dtype = ScalarTypeIdToQType(last_present_type_id);
  }
  DCHECK(res.VerifyAllocIdsConsistency());
  return res;
}

void SliceBuilder::ChangeCurrentType(KodaTypeId type_id) {
  DCHECK_NE(current_type_id_, type_id);
  if (storage_state_ == kStorageEmpty) {
    storage_state_ = kFirstStorage;
    current_type_id_ = type_id;
    return;
  }
  if (storage_state_ == kFirstStorage) {
    storage_.reserve(2);
    storage_.emplace(current_type_id_, std::move(first_storage_));
    storage_state_ = kMapStorage;
  }
  current_storage_ = &storage_[type_id];
  current_type_id_ = type_id;
}

void SliceBuilder::UnsetCurrentType() {
  DCHECK_NE(storage_state_, kStorageEmpty);
  if (storage_state_ == kFirstStorage) {
    storage_.reserve(2);
    storage_.emplace(current_type_id_, std::move(first_storage_));
    current_storage_ = nullptr;
  }
  storage_state_ = kMapStorage;
  current_type_id_ = ScalarTypeId<MissingValue>();
}

void SliceBuilder::InsertGuaranteedNotSet(int64_t id, const DataItem& v) {
  DCHECK(!IsSet(id));
  v.VisitValue([&](const auto& v) { InsertGuaranteedNotSet(id, v); });
}

void SliceBuilder::InsertIfNotSet(int64_t id, const DataItem& v) {
  if (IsSet(id)) {
    return;
  }
  InsertGuaranteedNotSet(id, v);
}

void SliceBuilder::InsertGuaranteedNotSetAndUpdateAllocIds(int64_t id,
                                                           const DataItem& v) {
  DCHECK(!IsSet(id));
  v.VisitValue([&]<typename T>(const T& v) {
    InsertGuaranteedNotSet(id, v);
    if constexpr (std::is_same_v<T, ObjectId>) {
      GetMutableAllocationIds().Insert(AllocationId(v));
    }
  });
}

void SliceBuilder::InsertIfNotSetAndUpdateAllocIds(int64_t id,
                                                   const DataItem& v) {
  if (IsSet(id)) {
    return;
  }
  InsertGuaranteedNotSetAndUpdateAllocIds(id, v);
}

}  // namespace koladata::internal
