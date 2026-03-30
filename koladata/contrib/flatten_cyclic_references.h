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
#ifndef KOLADATA_CONTRIB_FLATTEN_CYCLIC_REFERENCES_H_
#define KOLADATA_CONTRIB_FLATTEN_CYCLIC_REFERENCES_H_

#include <cstdint>

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"
#include "koladata/internal/non_deterministic_token.h"

namespace koladata::contrib {

// kd_ext.contrib.flatten_cyclic_references
absl::StatusOr<DataSlice> FlattenCyclicReferences(
    const DataSlice& ds, int64_t max_recursion_depth,
    internal::NonDeterministicToken = {});

}  // namespace koladata::contrib

#endif  // KOLADATA_CONTRIB_FLATTEN_CYCLIC_REFERENCES_H_
