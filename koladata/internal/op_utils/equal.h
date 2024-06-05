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
#ifndef KOLADATA_INTERNAL_OP_UTILS_EQUAL_H_
#define KOLADATA_INTERNAL_OP_UTILS_EQUAL_H_

#include <type_traits>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "arolla/memory/optional_value.h"

namespace koladata::internal {

// Returns present if the values are present and equal, missing otherwise.
struct EqualOp {
  absl::StatusOr<DataSliceImpl> operator()(const DataSliceImpl& lhs,
                                           const DataSliceImpl& rhs) const;

  DataItem operator()(const DataItem& lhs, const DataItem& rhs) const {
    if (lhs.has_value() && lhs == rhs) {
      return DataItem(arolla::kPresent);
    }
    return DataItem();
  }

 private:
  // Supports comparing int to int64_t and float to double.
  template <typename T1, typename T2>
  struct MaskEqualOp {
    arolla::OptionalUnit operator()(T1 l, T2 r) const {
      return arolla::OptionalUnit(l == r);
    }
  };

  template <typename T1, typename T2>
  struct compatible_types : std::false_type {};

  template <typename T1>
  struct compatible_types<T1, T1> : std::true_type {};
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_EQUAL_H_
