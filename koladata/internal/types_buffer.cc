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
#include "koladata/internal/types_buffer.h"

#include <cstdint>
#include <utility>

#if defined(__SSE2__)
#include <emmintrin.h>
#endif

#include "arolla/dense_array/bitmap.h"

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
                  [&](uint8_t t) { return is_present_type_idx(t); });
  return std::move(bldr).Build();
}

arolla::bitmap::Bitmap TypesBuffer::ToNotRemovedBitmap() const {
  arolla::bitmap::Builder bldr(id_to_typeidx.size());
  bldr.AddForEach(id_to_typeidx, [&](uint8_t t) { return t != kRemoved; });
  return std::move(bldr).Build();
}

}  // namespace koladata::internal
