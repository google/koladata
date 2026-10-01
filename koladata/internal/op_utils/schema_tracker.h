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
#ifndef KOLADATA_INTERNAL_OP_UTILS_SCHEMA_TRACKER_H_
#define KOLADATA_INTERNAL_OP_UTILS_SCHEMA_TRACKER_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

// A tracker for schemas of reached ObjectIds used during graph traversal.
//
// Wraps a DataBag storing the schema of objects as an attribute and provides
// operations to set schema, record whether objects were reached with
// `schema::kObject`, and ensure that no object is reached with multiple
// conflicting schemas.
class SchemaTracker {
 public:
  SchemaTracker() : databag_(DataBagImpl::CreateEmptyDatabag()) {}

  // Sets schema for ObjectIds in `ds`. Verifies that no object in `ds` has a
  // conflicting schema (returns InvalidArgumentError on conflict). If
  // `is_object_schema` is true, also marks the objects in `ds` as having been
  // reached with `schema::kObject`.
  absl::Status SetSchemaCheckNoConflicts(const DataSliceImpl& ds,
                                         const DataItem& schema,
                                         bool is_object_schema = false);

  // Sets per-object schemas `schemas` for ObjectIds in `ds`. Verifies that no
  // object in `ds` conflicts with a previously recorded schema (inner-slice
  // conflicts for duplicate ObjectIds within `ds` are not checked).
  absl::Status SetSchemaCheckNoConflicts(const DataSliceImpl& ds,
                                         const DataSliceImpl& schemas,
                                         bool is_object_schema = false);

  // Sets schema for ObjectId in `item`. Verifies that `item` does not have a
  // conflicting schema (returns InvalidArgumentError on conflict).
  absl::Status SetSchemaCheckNoConflicts(const DataItem& item,
                                         const DataItem& schema);

  // Returns the recorded schemas for ObjectIds in `ds`.
  absl::StatusOr<DataSliceImpl> GetSchemas(const DataSliceImpl& ds) const;

  // Returns a mask slice (unit values) indicating which ObjectIds in `ds` were
  // marked with `is_object_schema = true` in prior calls to
  // `SetSchemaCheckNoConflicts`.
  absl::StatusOr<DataSliceImpl> GetObjectSchemaMask(
      const DataSliceImpl& ds) const;

 private:
  static bool IsFullAlloc(const DataSliceImpl& objects);

  DataBagImplPtr databag_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_SCHEMA_TRACKER_H_
