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
#ifndef KOLADATA_OPERATORS_LISTS_H_
#define KOLADATA_OPERATORS_LISTS_H_

// List operator implementations.

#include <cstdint>

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/internal/non_deterministic_token.h"

namespace koladata::ops {

// kde.core._get_list_item_by_range.
inline absl::StatusOr<DataSlice> GetListItemByRange(const DataSlice& ds,
                                                    int64_t start,
                                                    int64_t stop) {
  return ds.ExplodeList(start, stop);
}

// kde.lists._explode
absl::StatusOr<DataSlice> Explode(const DataSlice& x, int64_t ndim);

// kde.lists._implode
absl::StatusOr<DataSlice> Implode(const DataSlice& x, int64_t ndim,
                                  internal::NonDeterministicToken);

// kde.lists.size.
absl::StatusOr<DataSlice> ListSize(const DataSlice& lists);

// kde.lists.create operator.
absl::StatusOr<DataSlice> List(
    const DataSlice& items, const DataSlice& item_schema,
    const DataSlice& schema, const DataSlice& itemid,
    internal::NonDeterministicToken);

// kde.lists._list_like operator.
absl::StatusOr<DataSlice> ListLike(
    const DataSlice& shape_and_mask_from, const DataSlice& items,
    const DataSlice& item_schema, const DataSlice& schema,
    const DataSlice& itemid,
    internal::NonDeterministicToken);

// kde.lists._list_shaped operator.
absl::StatusOr<DataSlice> ListShaped(
    const DataSlice::JaggedShape& shape, const DataSlice& items,
    const DataSlice& item_schema, const DataSlice& schema,
    const DataSlice& itemid,
    internal::NonDeterministicToken);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_LISTS_H_