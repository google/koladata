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
#ifndef KOLADATA_INTERNAL_OP_UTILS_VECTORIZED_TRAVERSER_UTILS_H_
#define KOLADATA_INTERNAL_OP_UTILS_VECTORIZED_TRAVERSER_UTILS_H_

#include <cstdint>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/edge.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal::vectorized_traverser_internal {

// Validates `tokens` returned by the visitor's `method` for `ds`: they must
// have the same size as `ds` and hold only ObjectIds.
absl::Status ValidatePrevisitTokens(absl::string_view method,
                                    const DataSliceImpl& ds,
                                    const DataSliceImpl& tokens);

// Validates that `item` (if present) has the primitive type specified by
// `schema` (which must be a primitive schema).
absl::Status ValidatePrimitiveType(const DataItem& item,
                                   const DataItem& schema);

// Validates that all present items of `ds` have the primitive type specified by
// `schema` (which must be a primitive schema).
absl::Status ValidatePrimitiveTypes(const DataSliceImpl& ds,
                                    const DataItem& schema);

// Validates items of `ds` with primitive `schemas` (aligned with `ds`), and
// returns true iff all present items of `ds` are valid primitives (with
// primitive or OBJECT `schemas`).
absl::StatusOr<bool> ContainsOnlyValidPrimitives(const DataSliceImpl& ds,
                                                 const DataSliceImpl& schemas);

// Returns true iff objects reached with `schema` are traversed further.
inline bool IsTraversableSchema(const DataItem& schema) {
  return schema == schema::kSchema || schema == schema::kObject ||
         schema.holds_value<ObjectId>();
}

// Returns the items of the `i`-th parent, where `edge` maps `items` to their
// parents (e.g. list items to lists).
DataSliceImpl ChildItems(const DataSliceImpl& items,
                         const arolla::DenseArrayEdge& edge, int64_t i);

}  // namespace koladata::internal::vectorized_traverser_internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_VECTORIZED_TRAVERSER_UTILS_H_
