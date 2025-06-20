// Copyright 2025 Google LLC
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
#ifndef KOLADATA_OPERATORS_DICTS_H_
#define KOLADATA_OPERATORS_DICTS_H_

// Dict operators implementations.

#include <cstdint>

#include "absl/status/statusor.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/non_deterministic_token.h"

namespace koladata::ops {

// kd.dicts.dict_size.
absl::StatusOr<DataSlice> DictSize(const DataSlice& dicts);

// kd.dicts.get_keys.
inline absl::StatusOr<DataSlice> GetKeys(const DataSlice& dict_ds) {
  return dict_ds.GetDictKeys();
}

// kd.dicts._get_values.
inline absl::StatusOr<DataSlice> GetValues(const DataSlice& dict_ds) {
  return dict_ds.GetDictValues();
}

// kd.dicts._get_values_by_keys.
inline absl::StatusOr<DataSlice> GetValuesByKeys(const DataSlice& dict_ds,
                                                 const DataSlice& key_ds) {
  return dict_ds.GetFromDict(key_ds);
}

// kd.dicts._dict_update
absl::StatusOr<DataBagPtr> DictUpdate(const DataSlice& x, const DataSlice& keys,
                                      const DataSlice& values);

// kd.dicts._shaped operator.
absl::StatusOr<DataSlice> DictShaped(
    const DataSlice::JaggedShape& shape, const DataSlice& keys,
    const DataSlice& values, const DataSlice& key_schema,
    const DataSlice& value_schema, const DataSlice& schema,
    const DataSlice& itemid,
    internal::NonDeterministicToken);

// kd.dicts._like operator.
absl::StatusOr<DataSlice> DictLike(
    const DataSlice& shape_and_mask_from, const DataSlice& keys,
    const DataSlice& values, const DataSlice& key_schema,
    const DataSlice& value_schema, const DataSlice& schema,
    const DataSlice& itemid,
    internal::NonDeterministicToken);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_DICTS_H_
