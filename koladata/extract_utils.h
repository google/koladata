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
#ifndef KOLADATA_EXTRACT_UTILS_H_
#define KOLADATA_EXTRACT_UTILS_H_

#include <optional>

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/internal/op_utils/extract.h"

namespace koladata::extract_utils_internal {

// Returns a copy of `ds` that has the schema `schema`, and uses a new DataBag
// that contains only triples reachable from `ds` (when interpreted as having
// the schema `schema`) or from `schema`.
absl::StatusOr<DataSlice> ExtractWithSchema(
    const DataSlice& ds, const DataSlice& schema, int max_depth = -1,
    const std::optional<internal::LeafCallback>& leaf_callback = std::nullopt);

// Returns a copy of `ds` that uses a new DataBag that contains only triples
// reachable from `ds`.
absl::StatusOr<DataSlice> Extract(const DataSlice& ds);

}  // namespace koladata::extract_utils_internal

#endif  // KOLADATA_EXTRACT_UTILS_H_
