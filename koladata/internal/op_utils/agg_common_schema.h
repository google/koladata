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
#ifndef KOLADATA_INTERNAL_OP_UTILS_AGG_COMMON_SCHEMA_H_
#define KOLADATA_INTERNAL_OP_UTILS_AGG_COMMON_SCHEMA_H_

#include "absl/status/statusor.h"
#include "arolla/dense_array/edge.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

// Returns the common schema within each group of schemas `ds`.
absl::StatusOr<DataSliceImpl> AggCommonSchemaOp(
    const DataSliceImpl& ds, const arolla::DenseArrayEdge& edge);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_AGG_COMMON_SCHEMA_H_
