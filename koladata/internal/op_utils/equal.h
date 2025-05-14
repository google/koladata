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

#include <cstdint>
#include <type_traits>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/meta.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

// Returns present if the values are present and equal, missing otherwise.
struct EqualOp {
  absl::StatusOr<DataSliceImpl> operator()(const DataSliceImpl& lhs,
                                           const DataSliceImpl& rhs) const;

  DataItem operator()(const DataItem& lhs, const DataItem& rhs) const;

  // Returns true if the types are comparable (supported using MaskEqualOp).
  template <typename T1, typename T2>
  static constexpr bool Comparable() {
    using numerics = arolla::meta::type_list<int, int64_t, float, double>;
    return std::is_same_v<T1, T2> || (arolla::meta::contains_v<numerics, T1> &&
                                      arolla::meta::contains_v<numerics, T2>);
  }

 private:
  // Returns present if the values are present and equal, missing otherwise.
  //
  // Requires: Comparable<T1, T2>()
  template <typename T1, typename T2>
  struct MaskEqualOp {
    static_assert(EqualOp::Comparable<T1, T2>());

    arolla::OptionalUnit operator()(T1 l, T2 r) const {
      return arolla::OptionalUnit(l == r);
    }
  };
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_EQUAL_H_
