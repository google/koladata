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
#include <utility>
#include <variant>

#include "arolla/qtype/qtype.h"

#if defined(__SSE2__)
#include <emmintrin.h>
#endif

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

namespace {

arolla::bitmap::Word GetEqualityMask16(const uint8_t* data, uint8_t type_idx) {
#if defined(__SSE2__)
  const auto match = _mm_set1_epi8(static_cast<char>(type_idx));
  auto ctrl = _mm_loadu_si128(reinterpret_cast<const __m128i*>(data));
  return static_cast<arolla::bitmap::Word>(
      _mm_movemask_epi8(_mm_cmpeq_epi8(match, ctrl)));
#else
  arolla::bitmap::Word mask = 0;
  for (int64_t i = 0; i < 16; ++i) {
    mask |= ((data[i] == type_idx) << i);
  }
  return mask;
#endif
}

}  // namespace

arolla::bitmap::Bitmap TypesBuffer::ToBitmap(uint8_t type_idx) const {
  static_assert(arolla::bitmap::kWordBitCount == 32);
  arolla::bitmap::Bitmap::Builder bldr(
      arolla::bitmap::BitmapSize(id_to_typeidx.size()));
  auto bitmask = bldr.GetMutableSpan();

  int64_t offset = 0;
  const int64_t limit32 = static_cast<int64_t>(id_to_typeidx.size()) - 31;
  for (; offset < limit32; offset += 32) {
    arolla::bitmap::Word mask0 =
        GetEqualityMask16(id_to_typeidx.begin() + offset, type_idx);
    arolla::bitmap::Word mask1 =
        GetEqualityMask16(id_to_typeidx.begin() + offset + 16, type_idx);
    bitmask[offset / arolla::bitmap::kWordBitCount] = mask0 | (mask1 << 16);
  }

  int64_t limit = static_cast<int64_t>(id_to_typeidx.size()) - offset;
  if (limit > 0) {
    arolla::bitmap::Word mask = 0;
    for (int64_t i = 0; i < limit; ++i) {
      mask |= static_cast<arolla::bitmap::Word>(id_to_typeidx[offset + i] ==
                                                type_idx)
              << i;
    }
    bitmask.back() = mask;
  }
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

std::pair<bool, DataSliceImpl::Variant> SliceBuilder::BuildDataSliceVariant(
    StorageVariant& sv) {
  using Res = std::pair<bool, DataSliceImpl::Variant>;
  return std::visit([this]<typename TS>(TS& ts) {
    if constexpr (std::is_same_v<TS, std::monostate>) {
      LOG(FATAL)
          << "Unexpected std::monostate in SliceBuilder::BuildDataSliceVariant";
      return Res{false, DataSliceImpl::Variant()};
    } else {
      using T = arolla::meta::strip_template_t<TypedStorage, TS>;
      uint8_t ti = ts.type_index;
      auto bitmap = types_buffer_.ToBitmap(ti);
      if (arolla::bitmap::AreAllBitsUnset(bitmap.begin(), size())) {
        return Res{false, DataSliceImpl::Variant()};
      }
      return Res{true, arolla::DenseArray<T>{std::move(ts).Build(),
                                             std::move(bitmap)}};
    }
  }, sv);
}

DataSliceImpl SliceBuilder::Build() && {
  if (storage_.empty()) {
    if (current_type_id_ == ScalarTypeId<MissingValue>()) {
      return DataSliceImpl::CreateEmptyAndUnknownType(size());
    }
    auto [present, array_variant] = BuildDataSliceVariant(first_storage_);
    if (!present) {
      return DataSliceImpl::CreateEmptyAndUnknownType(size());
    }
    DataSliceImpl res;
    res.internal_->allocation_ids = std::move(allocation_ids_);
    res.internal_->size = size();
    res.internal_->values.push_back(std::move(array_variant));
    res.internal_->dtype = ScalarTypeIdToQType(types_buffer_.types[0]);
    return res;
  }
  DataSliceImpl res;
  res.internal_->allocation_ids = std::move(allocation_ids_);
  res.internal_->size = size();
  KodaTypeId last_present_type_id = 0;
  for (auto& [tid, storage] : storage_) {
    auto [present, array_variant] = BuildDataSliceVariant(storage);
    if (present) {
      res.internal_->values.push_back(std::move(array_variant));
      last_present_type_id = tid;
    }
  }
  if (res.internal_->values.size() == 1) {
    res.internal_->dtype = ScalarTypeIdToQType(last_present_type_id);
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

void SliceBuilder::InsertIfNotSet(int64_t id, const DataItem& v) {
  v.VisitValue([&](const auto& v) { InsertIfNotSet(id, v); });
}

}  // namespace koladata::internal
