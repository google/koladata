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
#include <utility>
#include <variant>

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/types.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/util/meta.h"

namespace koladata::internal {

arolla::bitmap::Bitmap TypesBuffer::ToBitmap(uint8_t type_idx) const {
  arolla::bitmap::Builder bldr(id_to_typeidx.size());
  bldr.AddForEach(id_to_typeidx, [&](uint8_t t) { return t == type_idx; });
  return std::move(bldr).Build();
}

arolla::bitmap::Bitmap TypesBuffer::ToPresenceBitmap() const {
  arolla::bitmap::Builder bldr(id_to_typeidx.size());
  bldr.AddForEach(id_to_typeidx,
                  [&](uint8_t t) { return t != kRemoved && t != kUnset; });
  return std::move(bldr).Build();
}

SliceBuilder::SliceBuilder(size_t size) {
  arolla::Buffer<uint8_t>::Builder tidx_bldr(size);
  id_to_typeidx_ = tidx_bldr.GetMutableSpan();
  types_buffer_.id_to_typeidx = std::move(tidx_bldr).Build();
  std::fill(id_to_typeidx_.begin(), id_to_typeidx_.end(), TypesBuffer::kUnset);
  unset_count_ = size;
}

std::pair<uint8_t, DataSliceImpl::Variant> SliceBuilder::BuildDataSliceVariant(
    StorageVariant& sv) {
  using Res = std::pair<uint8_t, DataSliceImpl::Variant>;
  return std::visit([this]<typename TS>(TS& ts) {
    if constexpr (std::is_same_v<TS, std::monostate>) {
      LOG(FATAL)
          << "Unexpected std::monostate in SliceBuilder::BuildDataSliceVariant";
      return Res(0, DataSliceImpl::Variant());
    } else {
      using T = arolla::meta::strip_template_t<TypedStorage, TS>;
      uint8_t ti = ts.type_index;
      return Res(ti, arolla::DenseArray<T>{std::move(ts).Build(),
                                           types_buffer_.ToBitmap(ti)});
    }
  }, sv);
}

DataSliceImpl SliceBuilder::Build() && {
  if (storage_.empty()) {
    if (current_type_id_ == ScalarTypeId<MissingValue>()) {
      return DataSliceImpl::CreateEmptyAndUnknownType(size());
    }
    DataSliceImpl res;
    res.internal_->allocation_ids = std::move(allocation_ids_);
    res.internal_->size = size();
    res.internal_->dtype = ScalarTypeIdToQType(types_buffer_.types[0]);
    res.internal_->values.push_back(
        BuildDataSliceVariant(first_storage_).second);
    return res;
  }
  DataSliceImpl res;
  res.internal_->allocation_ids = std::move(allocation_ids_);
  res.internal_->size = size();
  if (types_buffer_.type_count() == 1) {
    res.internal_->dtype = ScalarTypeIdToQType(types_buffer_.types[0]);
  }
  res.internal_->values.resize(types_buffer_.type_count());
  for (auto& [tid, storage] : storage_) {
    auto [idx, array_variant] = BuildDataSliceVariant(storage);
    res.internal_->values[idx] = std::move(array_variant);
  }
  return res;
}

void SliceBuilder::ChangeCurrentType(KodaTypeId type_id) {
  DCHECK_NE(current_type_id_, type_id);
  if (storage_.empty()) {
    if (current_type_id_ == ScalarTypeId<MissingValue>()) {
      current_type_id_ = type_id;
      return;
    }
    storage_.reserve(2);
    storage_.emplace(current_type_id_, std::move(first_storage_));
  }
  current_storage_ = &storage_[type_id];
  current_type_id_ = type_id;
}

template <>
void SliceBuilder::InsertIfNotSet<DataItem>(int64_t id, const DataItem& v) {
  v.VisitValue([&](const auto& v) { InsertIfNotSet(id, v); });
}

}  // namespace koladata::internal
