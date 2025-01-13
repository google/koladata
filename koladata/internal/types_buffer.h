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
#ifndef KOLADATA_INTERNAL_TYPES_BUFFER_H_
#define KOLADATA_INTERNAL_TYPES_BUFFER_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "absl/base/nullability.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/types.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/qtype/qtype.h"

namespace koladata::internal {

// Type information for a multitype array. Holds list of types and an index in
// this list (1 byte) for each array element. Used in DataSliceImpl and
// DenseSource.
struct TypesBuffer {
  // Special values in id_to_typeidx.
  static constexpr uint8_t kUnset = 0xff;
  static constexpr uint8_t kRemoved = 0xfe;  // explicitly set to missing
  static constexpr uint8_t kMaybeRemoved = 0xfd;

  static constexpr bool is_present_type_idx(uint8_t idx) {
    return (idx & 0x80) == 0;
  }

  // Index in `types` (or kUnset/kRemoved) per element.
  absl::InlinedVector<uint8_t, 16> id_to_typeidx;

  // ScalarTypeId<T>() of used types.
  absl::InlinedVector<KodaTypeId, 4> types;

  int64_t size() const { return id_to_typeidx.size(); }
  size_t type_count() const { return types.size(); }

  void InitAllUnset(int64_t size) {
    id_to_typeidx.resize(size);
    std::fill(id_to_typeidx.begin(), id_to_typeidx.end(), kUnset);
  }

  KodaTypeId id_to_scalar_typeid(int64_t id) const {
    DCHECK(0 <= id && id < size());
    uint8_t index = id_to_typeidx[id];
    if (!is_present_type_idx(index)) {
      return ScalarTypeId<MissingValue>();  // unset or removed
    }
    DCHECK_LT(index, types.size());
    return types[index];
  }

  // Returns QType for given id. `nullptr` if not set or removed.
  absl::Nullable<const arolla::QType*> id_to_type(int64_t id) const {
    return ScalarTypeIdToQType(id_to_scalar_typeid(id));
  }

  // Returns index of given type in `types` (i.e. typeidx). If not found, adds
  // it at the end of `types` and returns its index.
  uint8_t get_or_add_typeidx(KodaTypeId type_id) {
    for (uint8_t i = 0; i < types.size(); ++i) {
      if (types[i] == type_id) {
        return i;
      }
    }
    types.push_back(type_id);
    return types.size() - 1;
  }

  // Creates bitmap of (id_to_typeidx[i] == type_idx) per element.
  arolla::bitmap::Bitmap ToBitmap(uint8_t type_idx) const;

  // Creates bitmap of (id_to_typeidx[i] not in [kUnset, kRemoved]) per element.
  arolla::bitmap::Bitmap ToPresenceBitmap() const;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_TYPES_BUFFER_H_
