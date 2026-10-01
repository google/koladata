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
#include "koladata/internal/op_utils/schema_tracker.h"

#include <cstdint>
#include <utility>

#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/qtype/qtype_traits.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"

namespace koladata::internal {
namespace {

constexpr absl::string_view kObjectSchemaAttr = "o";

}  // namespace

bool SchemaTracker::IsFullAlloc(const DataSliceImpl& objects) {
  if (objects.is_empty_and_unknown() ||
      objects.dtype() != arolla::GetQType<ObjectId>()) {
    return false;
  }
  const auto& alloc_ids = objects.allocation_ids();
  if (alloc_ids.contains_small_allocation_id() || alloc_ids.size() != 1) {
    return false;
  }
  AllocationId alloc_id = alloc_ids.ids()[0];
  if (objects.size() != alloc_id.Capacity()) {
    return false;
  }
  const auto& objs = objects.values<ObjectId>();
  if (!objs.bitmap.empty()) {
    return false;
  }
  bool is_full_alloc = true;
  objs.ForEachPresent([&](int64_t i, ObjectId obj) {
    if (obj != alloc_id.ObjectByOffset(i)) {
      is_full_alloc = false;
    }
  });
  return is_full_alloc;
}

absl::Status SchemaTracker::SetSchemaCheckNoConflicts(const DataSliceImpl& ds,
                                                      const DataItem& schema,
                                                      bool is_object_schema) {
  if (ds.present_count() == 0) {
    return absl::OkStatus();
  }
  if (ds.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected ObjectId slice, got %v", ds.dtype()->name()));
  }
  if (!schema.holds_value<ObjectId>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected ObjectId schema, got %v", schema));
  }

  if (is_object_schema) {
    ASSIGN_OR_RETURN(auto _,
                     databag_->InternalSetUnitAttrAndReturnMissingObjects(
                         ds, kObjectSchemaAttr));
  }

  ASSIGN_OR_RETURN(auto existing_schemas,
                   databag_->GetAttr(ds, schema::kSchemaAttr));
  if (!existing_schemas.is_empty_and_unknown()) {
    if (existing_schemas.dtype() != arolla::GetQType<ObjectId>()) {
      return absl::InternalError(absl::StrFormat(
          "expected ObjectId slice for existing schemas, got %v",
          existing_schemas.dtype()->name()));
    }
    ObjectId schema_obj = schema.value<ObjectId>();
    absl::Status status = absl::OkStatus();
    RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
        [&](int64_t /*id*/, ObjectId obj, ObjectId existing_schema) {
          if (status.ok() && existing_schema != schema_obj) {
            status = absl::InvalidArgumentError(absl::StrFormat(
                "multiple schemas found for object %v: %v vs %v", obj,
                existing_schema, schema_obj));
          }
        },
        ds.values<ObjectId>(), existing_schemas.values<ObjectId>()));
    RETURN_IF_ERROR(std::move(status));
  }

  DataSliceImpl values = DataSliceImpl::Create(ds.size(), schema);
  if (IsFullAlloc(ds)) {
    AllocationId alloc_id = ds.allocation_ids().ids()[0];
    return databag_->SetAttrFullAlloc(alloc_id, schema::kSchemaAttr, values);
  }
  return databag_->SetAttr(ds, schema::kSchemaAttr, values);
}

absl::Status SchemaTracker::SetSchemaCheckNoConflicts(
    const DataSliceImpl& ds, const DataSliceImpl& schemas,
    bool is_object_schema) {
  if (ds.size() != schemas.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("ds and schemas size mismatch: %d vs %d",
                        ds.size(), schemas.size()));
  }
  if (ds.present_count() == 0) {
    return absl::OkStatus();
  }
  if (ds.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected ObjectId slice, got %v", ds.dtype()->name()));
  }
  if (schemas.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected ObjectId schemas slice, got %v", schemas.dtype()->name()));
  }

  if (is_object_schema) {
    ASSIGN_OR_RETURN(auto _,
                     databag_->InternalSetUnitAttrAndReturnMissingObjects(
                         ds, kObjectSchemaAttr));
  }

  ASSIGN_OR_RETURN(auto existing_schemas,
                   databag_->GetAttr(ds, schema::kSchemaAttr));
  if (!existing_schemas.is_empty_and_unknown()) {
    if (existing_schemas.dtype() != arolla::GetQType<ObjectId>()) {
      return absl::InternalError(absl::StrFormat(
          "expected ObjectId slice for existing schemas, got %v",
          existing_schemas.dtype()->name()));
    }
    absl::Status status = absl::OkStatus();
    RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
        [&](int64_t /*id*/, ObjectId obj, ObjectId existing_schema,
            ObjectId new_schema) {
          if (status.ok() && existing_schema != new_schema) {
            status = absl::InvalidArgumentError(absl::StrFormat(
                "multiple schemas found for object %v: %v vs %v", obj,
                existing_schema, new_schema));
          }
        },
        ds.values<ObjectId>(), existing_schemas.values<ObjectId>(),
        schemas.values<ObjectId>()));
    RETURN_IF_ERROR(std::move(status));
  }

  if (IsFullAlloc(ds)) {
    AllocationId alloc_id = ds.allocation_ids().ids()[0];
    return databag_->SetAttrFullAlloc(alloc_id, schema::kSchemaAttr, schemas);
  }
  return databag_->SetAttr(ds, schema::kSchemaAttr, schemas);
}

absl::Status SchemaTracker::SetSchemaCheckNoConflicts(const DataItem& item,
                                                      const DataItem& schema) {
  if (!item.has_value()) {
    return absl::OkStatus();
  }
  if (!item.holds_value<ObjectId>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected ObjectId, got %v", item));
  }
  if (!schema.holds_value<ObjectId>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected ObjectId schema, got %v", schema));
  }

  ASSIGN_OR_RETURN(auto existing, databag_->GetAttr(item, schema::kSchemaAttr));
  if (existing.has_value()) {
    if (existing != schema) {
      return absl::InvalidArgumentError(
          absl::StrFormat("multiple schemas found for object %v: %v vs %v",
                          item, existing, schema));
    }
    return absl::OkStatus();
  }
  return databag_->SetAttr(item, schema::kSchemaAttr, schema);
}

absl::StatusOr<DataSliceImpl> SchemaTracker::GetSchemas(
    const DataSliceImpl& ds) const {
  return databag_->GetAttr(ds, schema::kSchemaAttr);
}

absl::StatusOr<DataSliceImpl> SchemaTracker::GetObjectSchemaMask(
    const DataSliceImpl& ds) const {
  return databag_->GetAttr(ds, kObjectSchemaAttr);
}

}  // namespace koladata::internal
