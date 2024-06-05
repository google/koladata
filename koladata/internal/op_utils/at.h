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
#ifndef KOLADATA_INTERNAL_OP_UTILS_AT_H_
#define KOLADATA_INTERNAL_OP_UTILS_AT_H_

#include <strings.h>

#include <cstdint>
#include <optional>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "koladata/internal/data_slice.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"

namespace koladata::internal {

// Return a new DataSliceImpl with items in 'ds' at provided indices.
// Indices are based on the dimension represented by 'ds_to_common'.
// Out-of-bound indices result in missing items and negative indices are
// supported. If ds_to_common is scalar, indices_to_common is not used and can
// be std::nullopt. Otherwise, indices_to_common must be provided and have the
// same parent size as ds_to_common.
absl::StatusOr<DataSliceImpl> AtOp(
    const DataSliceImpl& ds, const arolla::DenseArray<int64_t>& indices,
    const arolla::DenseArrayEdge& ds_to_common,
    const std::optional<arolla::DenseArrayEdge>& indices_to_common);
}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_AT_H_
