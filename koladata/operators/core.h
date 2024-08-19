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
#ifndef KOLADATA_OPERATORS_CORE_H_
#define KOLADATA_OPERATORS_CORE_H_

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata::ops {

// kde.core._add.
absl::StatusOr<DataSlice> Add(const DataSlice& x, const DataSlice& y);

// kde.core.no_db.
DataSlice NoDb(const DataSlice& ds);

// kde.core.get_db.
absl::StatusOr<DataBagPtr> GetDb(const DataSlice& ds);

// kde.core.with_db.
DataSlice WithDb(const DataSlice& ds, const DataBagPtr& db);

// kde.core._agg_any.
absl::StatusOr<DataSlice> AggAny(const DataSlice& x);

// kde.core._agg_all.
absl::StatusOr<DataSlice> AggAll(const DataSlice& x);

// kde.core._inverse_mapping.
absl::StatusOr<DataSlice> InverseMapping(const DataSlice& x);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_CORE_H_
