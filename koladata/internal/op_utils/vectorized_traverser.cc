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
#include "koladata/internal/op_utils/vectorized_traverser.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/cancellation.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/abstract_visitor.h"
#include "koladata/internal/op_utils/group_by_utils.h"
#include "koladata/internal/op_utils/in_degree_tracker.h"
#include "koladata/internal/op_utils/vectorized_traverse_helper.h"

namespace koladata::internal {
namespace {

using ::koladata::internal::vectorized_traverser_internal::
    VectorizedTraverseHelper;

// Validates `tokens` returned by the visitor's `method` for `ds`: they must
// have the same size as `ds` and hold only ObjectIds.
absl::Status ValidatePrevisitTokens(absl::string_view method,
                                    const DataSliceImpl& ds,
                                    const DataSliceImpl& tokens) {
  if (tokens.size() != ds.size()) {
    return absl::InternalError(
        absl::StrFormat("expected %d tokens from %s, got %d", ds.size(), method,
                        tokens.size()));
  }
  if (tokens.present_count() > 0 &&
      tokens.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::InternalError(absl::StrFormat(
        "expected ObjectId tokens from %s, got %s", method,
        tokens.is_mixed_dtype() ? "mixed types" : tokens.dtype()->name()));
  }
  return absl::OkStatus();
}

absl::Status ValidatePrimitiveTypes(const DataSliceImpl& ds,
                                    const DataItem& schema) {
  DCHECK(schema.is_primitive_schema());
  auto schema_dtype = schema.value<schema::DType>();
  if (ds.is_empty_and_unknown() || ds.dtype() == schema_dtype.qtype()) {
    return absl::OkStatus();
  }
  arolla::QTypePtr actual_qtype = ds.dtype();
  if (ds.is_mixed_dtype()) {
    ds.VisitValues([&](const auto& array) {
      using T = typename std::decay_t<decltype(array)>::base_type;
      if (arolla::GetQType<T>() != schema_dtype.qtype()) {
        actual_qtype = arolla::GetQType<T>();
      }
    });
  }
  if (auto actual_dtype = schema::DType::FromQType(actual_qtype);
      actual_dtype.ok()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "during traversal, got a slice with primitive type ", schema_dtype,
        " while the actual content has type ", actual_dtype->name()));
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "during traversal, got a slice with primitive type ", schema_dtype,
      " while the actual content is not a primitive"));
}

}  // namespace

VectorizedTraverser::VectorizedTraverser(
    const DataBagImpl& databag, DataBagImpl::FallbackSpan fallbacks,
    std::shared_ptr<AbstractVisitor> visitor)
    : databag_(databag),
      fallbacks_(fallbacks),
      visitor_(std::move(visitor)),
      traverse_helper_(databag, fallbacks) {
  DCHECK_NE(visitor_, nullptr);
}

absl::Status VectorizedTraverser::TraverseSlice(const DataSliceImpl& ds,
                                                const DataItem& schema) {
  return PrevisitAndCalculateInDegrees(ds, schema);
}

absl::StatusOr<DataSliceImpl> VectorizedTraverser::PrevisitSlice(
    const DataSliceImpl& values, const DataItem& schema,
    const std::optional<TransitionKey>& key, const DataItem& parent_token) {
  ASSIGN_OR_RETURN(DataSliceImpl tokens,
                   visitor_->PrevisitSlice(values, schema, key, parent_token));
  RETURN_IF_ERROR(ValidatePrevisitTokens("PrevisitSlice", values, tokens));
  return tokens;
}

absl::StatusOr<DataSliceImpl> VectorizedTraverser::PrevisitObjectSchemas(
    const DataSliceImpl& objects, const DataSliceImpl& schemas,
    const DataItem& token) {
  ASSIGN_OR_RETURN(DataSliceImpl tokens,
                   visitor_->PrevisitObjectSchemas(objects, schemas, token));
  RETURN_IF_ERROR(
      ValidatePrevisitTokens("PrevisitObjectSchemas", objects, tokens));
  return tokens;
}

VectorizedTraverser::QueuedSlice VectorizedTraverser::MakeObjectsSlice(
    DataSliceImpl objects, DataSliceImpl schemas, const DataItem& token) {
  // Use an entity slice when all objects share the same schema (always true for
  // small allocations, including list/dict schemas).
  if (std::optional<DataItem> schema =
          ObjectsGroupBy::GetItemIfAllEqual(schemas);
      schema.has_value()) {
    return {.slice = std::move(objects),
            .schema_item = *std::move(schema),
            .is_object_schema = true,
            .token = token};
  }
  return {.slice = std::move(objects),
          .schema_item = DataItem(schema::kObject),
          .is_object_schema = true,
          .object_schemas = std::move(schemas),
          .token = token};
}

absl::Status VectorizedTraverser::TrackSchemas(const QueuedSlice& slice) {
  if (slice.schema_item == schema::kSchema) {
    return absl::OkStatus();
  }
  if (slice.schema_item == schema::kObject) {
    return schema_tracker_.SetSchemaCheckNoConflicts(
        slice.slice, slice.object_schemas, /*is_object_schema=*/true);
  }
  return schema_tracker_.SetSchemaCheckNoConflicts(
      slice.slice, slice.schema_item, slice.is_object_schema);
}

absl::Status VectorizedTraverser::Enqueue(QueuedSlice slice) {
  RETURN_IF_ERROR(TrackSchemas(slice));
  ASSIGN_OR_RETURN(DataSliceImpl new_objects,
                   in_degree_tracker_.IncrementAndGetNew(slice.slice));
  if (new_objects.present_count() == 0) {
    return absl::OkStatus();
  }
  // Update `slice` and `object_schemas` only if some objects were already seen.
  if (new_objects.size() != slice.slice.size()) {
    if (slice.schema_item == schema::kObject) {
      ASSIGN_OR_RETURN(slice.object_schemas,
                       schema_tracker_.GetSchemas(new_objects));
    }
    slice.slice = std::move(new_objects);
  }
  reachable_slices_.push_back(std::move(slice));
  return absl::OkStatus();
}

absl::Status VectorizedTraverser::ForEachChild(const QueuedSlice& parent,
                                               TransitionCallback callback) {
  if (parent.schema_item == schema::kSchema) {
    return traverse_helper_.ForEachChildOfSchemas(parent.slice, callback);
  }
  if (parent.schema_item == schema::kObject) {
    return traverse_helper_.ForEachChildOfObjects(
        parent.slice, parent.object_schemas, callback);
  }
  return traverse_helper_.ForEachChildOfEntities(parent.slice,
                                                 parent.schema_item, callback);
}

absl::Status VectorizedTraverser::PrevisitAndEnqueue(
    const DataItem& parent_token, const Transition& transition) {
  const DataItem& schema = transition.schema;
  if (schema.is_primitive_schema()) {
    return ValidatePrimitiveTypes(transition.values, schema);
  }
  if (schema == schema::kSchema) {
    return PrevisitAndEnqueueSchemas(parent_token, transition.key,
                                     transition.values);
  }
  DataSliceImpl objects = InDegreeTracker::FilterToObjects(transition.values);
  if (objects.present_count() == 0) {
    return absl::OkStatus();
  }
  if (schema == schema::kObject) {
    return PrevisitAndEnqueueObjects(parent_token, transition.key, objects);
  }
  if (schema == schema::kItemId) {
    // ItemIds are previsited, but not traversed further.
    return PrevisitSlice(objects, schema, transition.key, parent_token)
        .status();
  }
  return PrevisitAndEnqueueEntities(parent_token, transition.key, objects,
                                    schema);
}

absl::Status VectorizedTraverser::PrevisitAndEnqueueSchemas(
    const DataItem& parent_token, const std::optional<TransitionKey>& key,
    const DataSliceImpl& values) {
  if (values.present_count() == 0) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(
      DataSliceImpl tokens,
      PrevisitSlice(values, DataItem(schema::kSchema), key, parent_token));
  DataSliceImpl schemas = InDegreeTracker::FilterToObjects(values);
  if (schemas.present_count() == 0) {
    return absl::OkStatus();
  }
  DataSliceImpl allocation_keys = ObjectsGroupBy::SchemaAllocationKeys(schemas);
  return group_by_.ByKeys(std::move(tokens), std::move(allocation_keys))
      .Split(std::move(schemas))
      .ForEach([&](const DataItem& token, const DataItem& /*allocation_key*/,
                   DataSliceImpl allocation_schemas) {
        return Enqueue({.slice = std::move(allocation_schemas),
                        .schema_item = DataItem(schema::kSchema),
                        .token = token});
      });
}

absl::Status VectorizedTraverser::PrevisitAndEnqueueEntities(
    const DataItem& parent_token, const std::optional<TransitionKey>& key,
    const DataSliceImpl& objects, const DataItem& schema) {
  ASSIGN_OR_RETURN(DataSliceImpl tokens,
                   PrevisitSlice(objects, schema, key, parent_token));
  return group_by_.ByKeys(std::move(tokens))
      .Split(objects)
      .ForEach([&](const DataItem& token, DataSliceImpl objects_group) {
        return Enqueue({.slice = std::move(objects_group),
                        .schema_item = schema,
                        .token = token});
      });
}

absl::Status VectorizedTraverser::PrevisitAndEnqueueObjects(
    const DataItem& parent_token, const std::optional<TransitionKey>& key,
    const DataSliceImpl& objects) {
  ASSIGN_OR_RETURN(
      DataSliceImpl tokens,
      PrevisitSlice(objects, DataItem(schema::kObject), key, parent_token));
  return group_by_.ByKeys(std::move(tokens))
      .Split(objects)
      .ForEach([&](const DataItem& token, DataSliceImpl objects_group) {
        return PrevisitAndEnqueueObjectSchemas(token, objects_group);
      });
}

absl::Status VectorizedTraverser::PrevisitAndEnqueueObjectSchemas(
    const DataItem& token, const DataSliceImpl& objects) {
  ASSIGN_OR_RETURN(DataSliceImpl schemas,
                   databag_.GetObjSchemaAttr(objects, fallbacks_));
  if (schemas.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("unsupported schema found during traversal among %v in "
                        "__schema__ attribute of slice %v",
                        schemas, objects));
  }
  ASSIGN_OR_RETURN(DataSliceImpl tokens,
                   PrevisitObjectSchemas(objects, schemas, token));
  DataSliceImpl allocation_keys = ObjectsGroupBy::SchemaAllocationKeys(schemas);
  return group_by_.ByKeys(std::move(tokens), std::move(allocation_keys))
      .Split(objects, std::move(schemas))
      .ForEach([&](const DataItem& new_token,
                   const DataItem& /*allocation_key*/,
                   DataSliceImpl allocation_objects,
                   DataSliceImpl allocation_schemas) {
        return Enqueue(MakeObjectsSlice(std::move(allocation_objects),
                                        std::move(allocation_schemas),
                                        new_token));
      });
}

absl::Status VectorizedTraverser::PrevisitAndCalculateInDegrees(
    const DataSliceImpl& ds, const DataItem& schema) {
  RETURN_IF_ERROR(VectorizedTraverseHelper::ForEachRootTransition(
      ds, schema, [&](const Transition& transition) {
        return PrevisitAndEnqueue(/*parent_token=*/DataItem(), transition);
      }));
  for (size_t head = 0; head < reachable_slices_.size(); ++head) {
    RETURN_IF_ERROR(arolla::CheckCancellation());
    // Copy, since `reachable_slices_` grows during the iteration.
    QueuedSlice parent = reachable_slices_[head];
    RETURN_IF_ERROR(ForEachChild(parent, [&](const Transition& transition) {
      return PrevisitAndEnqueue(parent.token, transition);
    }));
  }
  return absl::OkStatus();
}

}  // namespace koladata::internal
