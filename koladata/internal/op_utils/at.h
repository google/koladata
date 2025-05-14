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

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "arolla/dense_array/dense_array.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

// Return a new DataSliceImpl with items in 'ds' at provided indices.
// Out-of-bound indices result in missing items and negative indices are
// supported.
absl::StatusOr<DataSliceImpl> AtOp(const DataSliceImpl& ds,
                                   const arolla::DenseArray<int64_t>& indices);
}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_AT_H_
