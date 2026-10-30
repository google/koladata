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
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/abstract_visitor.h"
#include "koladata/internal/op_utils/group_by_utils.h"
#include "koladata/internal/op_utils/has.h"
#include "koladata/internal/op_utils/in_degree_tracker.h"
#include "koladata/internal/op_utils/presence_and.h"
#include "koladata/internal/op_utils/schema_info.h"
#include "koladata/internal/op_utils/slice_attribute_table.h"
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

absl::Status PrimitiveTypeError(schema::DType schema_dtype,
                                arolla::QTypePtr actual_qtype) {
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

absl::Status ValidatePrimitiveType(const DataItem& item,
                                   const DataItem& schema) {
  DCHECK(schema.is_primitive_schema());
  auto schema_dtype = schema.value<schema::DType>();
  if (!item.has_value() || item.dtype() == schema_dtype.qtype()) {
    return absl::OkStatus();
  }
  return PrimitiveTypeError(schema_dtype, item.dtype());
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
  return PrimitiveTypeError(schema_dtype, actual_qtype);
}

// Validates items of `ds` with primitive `schemas`, and returns true iff all
// present items of `ds` are valid primitives (with primitive or OBJECT
// `schemas`).
absl::StatusOr<bool> ContainsOnlyValidPrimitives(const DataSliceImpl& ds,
                                                 const DataSliceImpl& schemas) {
  if (!schemas.ContainsAnyPrimitives()) {
    return false;
  }
  bool only_primitives = true;
  for (int64_t i = 0; i < ds.size(); ++i) {
    if (!ds.present(i)) {
      continue;
    }
    const DataItem item = ds[i];
    const DataItem schema = schemas[i];
    if (schema.is_primitive_schema()) {
      RETURN_IF_ERROR(ValidatePrimitiveType(item, schema));
    } else if (schema != schema::kObject || item.holds_value<ObjectId>()) {
      only_primitives = false;
    }
  }
  return only_primitives;
}

// Returns true iff objects reached with `schema` are traversed further.
bool IsTraversableSchema(const DataItem& schema) {
  return schema == schema::kSchema || schema == schema::kObject ||
         schema.holds_value<ObjectId>();
}

// Returns the items of the `i`-th parent, where `edge` maps `items` to their
// parents (e.g. list items to lists).
DataSliceImpl ChildItems(const DataSliceImpl& items,
                         const arolla::DenseArrayEdge& edge, int64_t i) {
  absl::Span<const int64_t> split_points = edge.edge_values().values.span();
  int64_t from = split_points[i];
  int64_t to = split_points[i + 1];
  if (from == to) {
    return DataSliceImpl::CreateEmptyAndUnknownType(0);
  }
  if (from == 0 && to == items.size()) {
    return items;
  }
  return items.SubSlice(from, to - from);
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
  RETURN_IF_ERROR(PrevisitAndCalculateInDegrees(ds, schema));
  RETURN_IF_ERROR(BuildTopologicalOrderFromInDegrees(ds, schema));
  return VisitInReverseTopologicalOrder();
}

absl::StatusOr<DataSliceImpl> VectorizedTraverser::PrevisitSlice(
    const DataSliceImpl& values, const DataItem& schema,
    const std::optional<TransitionKey>& key, const DataItem& parent_token) {
  ASSIGN_OR_RETURN(DataSliceImpl tokens,
                   visitor_->PrevisitSlice(values, schema, key, parent_token));
  RETURN_IF_ERROR(ValidatePrevisitTokens("PrevisitSlice", values, tokens));
  return tokens;
}

absl::Status VectorizedTraverser::PrevisitByToken(
    const DataSliceImpl& values, const DataItem& schema,
    const std::optional<TransitionKey>& key, const DataItem& parent_token,
    TokenGroupCallback callback) {
  ASSIGN_OR_RETURN(DataSliceImpl tokens,
                   PrevisitSlice(values, schema, key, parent_token));
  DataSliceImpl objects = InDegreeTracker::FilterToObjects(values);
  if (objects.present_count() == 0) {
    return absl::OkStatus();
  }
  return group_by_.ByKeys(std::move(tokens))
      .Split(std::move(objects))
      .ForEach(callback);
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

absl::Status VectorizedTraverser::ForEachSchemaSlice(
    DataSliceImpl schemas, const DataItem& token,
    QueuedSliceCallback callback) {
  DataSliceImpl allocation_keys = ObjectsGroupBy::SchemaAllocationKeys(schemas);
  return group_by_.ByKeys(std::move(allocation_keys))
      .Split(std::move(schemas))
      .ForEach([&](const DataItem& /*allocation_key*/,
                   DataSliceImpl allocation_schemas) {
        return callback({.slice = std::move(allocation_schemas),
                         .schema_item = DataItem(schema::kSchema),
                         .token = token});
      });
}

absl::Status VectorizedTraverser::ForEachObjectSlice(
    DataSliceImpl objects, DataSliceImpl schemas, const DataItem& token,
    QueuedSliceCallback callback) {
  DataSliceImpl allocation_keys = ObjectsGroupBy::SchemaAllocationKeys(schemas);
  return group_by_.ByKeys(std::move(allocation_keys))
      .Split(std::move(objects), std::move(schemas))
      .ForEach([&](const DataItem& /*allocation_key*/,
                   DataSliceImpl allocation_objects,
                   DataSliceImpl allocation_schemas) {
        return callback(MakeObjectsSlice(std::move(allocation_objects),
                                         std::move(allocation_schemas), token));
      });
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
  // For SCHEMA, all values (including DTypes) are previsited, and filtered to
  // ObjectIds afterwards.
  DataSliceImpl values =
      schema == schema::kSchema
          ? transition.values
          : InDegreeTracker::FilterToObjects(transition.values);
  if (values.present_count() == 0) {
    return absl::OkStatus();
  }
  if (schema == schema::kItemId) {
    // ItemIds are previsited, but not traversed further.
    return PrevisitSlice(values, schema, transition.key, parent_token).status();
  }
  return PrevisitByToken(
      values, schema, transition.key, parent_token,
      [&](const DataItem& token, DataSliceImpl objects) {
        if (schema == schema::kSchema) {
          return ForEachSchemaSlice(
              std::move(objects), token,
              [&](QueuedSlice slice) { return Enqueue(std::move(slice)); });
        }
        if (schema == schema::kObject) {
          return PrevisitAndEnqueueObjectSchemas(token, objects);
        }
        return Enqueue({.slice = std::move(objects),
                        .schema_item = schema,
                        .token = token});
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
  return group_by_.ByKeys(std::move(tokens))
      .Split(objects, std::move(schemas))
      .ForEach([&](const DataItem& new_token, DataSliceImpl token_objects,
                   DataSliceImpl token_schemas) {
        return ForEachObjectSlice(
            std::move(token_objects), std::move(token_schemas), new_token,
            [&](QueuedSlice slice) { return Enqueue(std::move(slice)); });
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

absl::Status VectorizedTraverser::AppendToOrder(DataSliceImpl values,
                                                const DataItem& schema) {
  auto append = [&](QueuedSlice slice) {
    topological_order_.push_back(std::move(slice));
    return absl::OkStatus();
  };
  if (schema == schema::kSchema) {
    return ForEachSchemaSlice(std::move(values), /*token=*/DataItem(), append);
  }
  if (schema == schema::kObject) {
    ASSIGN_OR_RETURN(DataSliceImpl schemas, schema_tracker_.GetSchemas(values));
    if (schemas.present_count() != values.size()) {
      return absl::InternalError(
          "schemas of objects reached with OBJECT schema are not tracked");
    }
    return ForEachObjectSlice(std::move(values), std::move(schemas),
                              /*token=*/DataItem(), append);
  }
  return append({.slice = std::move(values), .schema_item = schema});
}

absl::Status VectorizedTraverser::DecrementAndAppendToOrder(
    const DataSliceImpl& values, const DataItem& schema) {
  if (!IsTraversableSchema(schema)) {
    return absl::OkStatus();
  }
  DataSliceImpl objects = InDegreeTracker::FilterToObjects(values);
  if (objects.present_count() == 0) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(DataSliceImpl free_objects,
                   in_degree_tracker_.DecrementAndGetLast(objects));
  if (free_objects.present_count() == 0) {
    return absl::OkStatus();
  }
  return AppendToOrder(std::move(free_objects), schema);
}

absl::Status VectorizedTraverser::BreakCycle(size_t& slice_idx) {
  for (; slice_idx < reachable_slices_.size(); ++slice_idx) {
    RETURN_IF_ERROR(arolla::CheckCancellation());
    const QueuedSlice& candidate = reachable_slices_[slice_idx];
    ASSIGN_OR_RETURN(DataSliceImpl objects,
                     in_degree_tracker_.FilterToPositive(candidate.slice));
    if (objects.present_count() == 0) {
      continue;
    }
    DataSliceImpl free_objects;
    while (free_objects.present_count() == 0) {
      RETURN_IF_ERROR(arolla::CheckCancellation());
      ASSIGN_OR_RETURN(free_objects,
                       in_degree_tracker_.DecrementAndGetLast(objects));
    }
    return AppendToOrder(std::move(free_objects), candidate.schema_item);
  }
  return absl::InternalError(
      "no slices with positive in-degree found while breaking a cycle");
}

absl::Status VectorizedTraverser::BuildTopologicalOrderFromInDegrees(
    const DataSliceImpl& ds, const DataItem& schema) {
  topological_order_.reserve(reachable_slices_.size());
  auto decrement_callback = [&](const Transition& transition) {
    return DecrementAndAppendToOrder(transition.values, transition.schema);
  };
  RETURN_IF_ERROR(VectorizedTraverseHelper::ForEachRootTransition(
      ds, schema, decrement_callback));

  int64_t num_objects_to_process = 0;
  for (const QueuedSlice& slice : reachable_slices_) {
    num_objects_to_process += slice.slice.size();
  }
  size_t head = 0;
  size_t cycle_breaking_idx = 0;
  while (num_objects_to_process > 0) {
    RETURN_IF_ERROR(arolla::CheckCancellation());
    if (head == topological_order_.size()) {
      RETURN_IF_ERROR(BreakCycle(cycle_breaking_idx));
    }
    DCHECK_LT(head, topological_order_.size());
    // Copy, since `topological_order_` grows during the iteration.
    QueuedSlice parent = topological_order_[head++];
    num_objects_to_process -= parent.slice.size();
    RETURN_IF_ERROR(ForEachChild(parent, decrement_callback));
  }
  return absl::OkStatus();
}

absl::StatusOr<DataSliceImpl> VectorizedTraverser::GetValues(
    const DataSliceImpl& values, const DataItem& schema) {
  if (schema.is_primitive_schema()) {
    RETURN_IF_ERROR(ValidatePrimitiveTypes(values, schema));
    return values;
  }
  if (schema == schema::kObject &&
      InDegreeTracker::FilterToObjects(values).present_count() == 0) {
    return values;
  }
  return visitor_->GetValues(values, schema);
}

absl::StatusOr<DataSliceImpl> VectorizedTraverser::GetValues(
    const DataSliceImpl& values, const DataSliceImpl& schemas) {
  DataSliceImpl present_values = values;
  if (schemas.present_count() < schemas.size() && values.present_count() > 0) {
    ASSIGN_OR_RETURN(DataSliceImpl mask, HasOp()(schemas));
    ASSIGN_OR_RETURN(present_values, PresenceAndOp()(values, mask));
  }
  if (present_values.present_count() == 0) {
    return DataSliceImpl::CreateEmptyAndUnknownType(values.size());
  }
  if (std::optional<DataItem> schema =
          ObjectsGroupBy::GetItemIfAllEqual(schemas);
      schema.has_value()) {
    return GetValues(present_values, *schema);
  }
  ASSIGN_OR_RETURN(bool only_primitives,
                   ContainsOnlyValidPrimitives(present_values, schemas));
  if (only_primitives) {
    return present_values;
  }
  return visitor_->GetValues(present_values, schemas);
}

absl::Status VectorizedTraverser::Visit(const QueuedSlice& slice) {
  if (slice.schema_item == schema::kSchema) {
    return VisitSchemas(slice.slice);
  }
  if (slice.schema_item == schema::kObject) {
    // A small allocation means a single schema, so a slice with OBJECT
    // schema has `object_schemas` from a single big allocation.
    return VisitObjectsWithSchemasInBigAllocation(slice.slice,
                                                  slice.object_schemas);
  }
  ASSIGN_OR_RETURN(ObjectSchemaFlags is_object_schema,
                   GetObjectSchemaFlags(slice));
  return VisitEntities(slice.slice, slice.schema_item, is_object_schema);
}

absl::StatusOr<VectorizedTraverser::ObjectSchemaFlags>
VectorizedTraverser::GetObjectSchemaFlags(const QueuedSlice& slice) const {
  if (slice.is_object_schema) {
    return ObjectSchemaFlags{.all = true};
  }
  ASSIGN_OR_RETURN(DataSliceImpl mask,
                   schema_tracker_.GetObjectSchemaMask(slice.slice));
  return ObjectSchemaFlags{.mask = std::move(mask)};
}

absl::Status VectorizedTraverser::VisitSchemas(const DataSliceImpl& schemas) {
  ASSIGN_OR_RETURN(std::vector<DataItem> attr_names,
                   GetSchemasAttrNames(schemas, databag_, fallbacks_));
  SliceAttributeTable::Builder builder(schemas.size(), attr_names.size());
  for (const DataItem& attr_name_item : attr_names) {
    absl::string_view attr_name = attr_name_item.value<arolla::Text>().view();
    ASSIGN_OR_RETURN(
        DataSliceImpl attr_values,
        databag_.GetSchemaAttrAllowMissing(schemas, attr_name, fallbacks_));
    if (attr_values.present_count() == 0) {
      continue;
    }
    ASSIGN_OR_RETURN(DataSliceImpl converted_values,
                     GetValues(attr_values, GetSchemaAttrSchema(attr_name)));
    builder.Add(attr_name, std::move(converted_values), std::move(attr_values));
  }
  SliceAttributeTable attrs = std::move(builder).Build();
  for (int64_t i = 0; i < schemas.size(); ++i) {
    RETURN_IF_ERROR(visitor_->VisitSchema(schemas[i], DataItem(schema::kSchema),
                                          /*is_object_schema=*/false,
                                          attrs.NamesOf(i), attrs.ValuesOf(i)));
  }
  return absl::OkStatus();
}

absl::Status VectorizedTraverser::VisitEntities(
    const DataSliceImpl& entities, const DataItem& schema,
    const ObjectSchemaFlags& is_object_schema) {
  ASSIGN_OR_RETURN(EntitySchemaInfo info,
                   GetEntitySchemaInfo(schema, databag_, fallbacks_));
  if (const auto* list_info = std::get_if<ListSchemaInfo>(&info)) {
    return VisitLists(entities, schema, list_info->item_schema,
                      is_object_schema);
  }
  if (const auto* dict_info = std::get_if<DictSchemaInfo>(&info)) {
    return VisitDicts(entities, schema, dict_info->keys_schema,
                      dict_info->values_schema, is_object_schema);
  }
  const auto& object_info = std::get<ObjectSchemaInfo>(info);
  SliceAttributeTable::Builder builder(entities.size(),
                                       object_info.attr_names.size());
  for (const DataItem& attr_name_item : object_info.attr_names) {
    absl::string_view attr_name = attr_name_item.value<arolla::Text>().view();
    ASSIGN_OR_RETURN(DataItem attr_schema,
                     databag_.GetSchemaAttr(schema, attr_name, fallbacks_));
    ASSIGN_OR_RETURN(DataSliceImpl values,
                     databag_.GetAttr(entities, attr_name, fallbacks_));
    ASSIGN_OR_RETURN(values, GetValues(values, attr_schema));
    builder.Add(attr_name, std::move(values));
  }
  SliceAttributeTable attrs = std::move(builder).Build();
  for (int64_t i = 0; i < entities.size(); ++i) {
    RETURN_IF_ERROR(visitor_->VisitObject(entities[i], schema,
                                          is_object_schema[i], attrs.NamesOf(i),
                                          attrs.ValuesOf(i)));
  }
  return absl::OkStatus();
}

absl::Status VectorizedTraverser::VisitLists(
    const DataSliceImpl& lists, const DataItem& schema,
    const DataItem& item_schema, const ObjectSchemaFlags& is_object_schema) {
  ASSIGN_OR_RETURN(
      (auto [items, edge]),
      databag_.ExplodeLists(lists, DataBagImpl::ListRange(), fallbacks_));
  ASSIGN_OR_RETURN(DataSliceImpl converted_items,
                   GetValues(items, item_schema));
  for (int64_t i = 0; i < lists.size(); ++i) {
    RETURN_IF_ERROR(visitor_->VisitList(lists[i], schema, is_object_schema[i],
                                        ChildItems(converted_items, edge, i)));
  }
  return absl::OkStatus();
}

absl::Status VectorizedTraverser::VisitDicts(
    const DataSliceImpl& dicts, const DataItem& schema,
    const DataItem& keys_schema, const DataItem& values_schema,
    const ObjectSchemaFlags& is_object_schema) {
  ASSIGN_OR_RETURN((auto [keys, edge]),
                   databag_.GetDictKeys(dicts, fallbacks_));
  // Values are returned in the same order as keys.
  ASSIGN_OR_RETURN(auto values_and_edge,
                   databag_.GetDictValues(dicts, fallbacks_));
  ASSIGN_OR_RETURN(DataSliceImpl converted_keys, GetValues(keys, keys_schema));
  ASSIGN_OR_RETURN(DataSliceImpl converted_values,
                   GetValues(values_and_edge.first, values_schema));
  for (int64_t i = 0; i < dicts.size(); ++i) {
    RETURN_IF_ERROR(visitor_->VisitDict(dicts[i], schema, is_object_schema[i],
                                        ChildItems(converted_keys, edge, i),
                                        ChildItems(converted_values, edge, i)));
  }
  return absl::OkStatus();
}

absl::Status VectorizedTraverser::VisitObjectsWithSchemasInBigAllocation(
    const DataSliceImpl& objects, const DataSliceImpl& schemas) {
  ASSIGN_OR_RETURN(std::vector<DataItem> attr_names,
                   GetSchemasAttrNames(schemas, databag_, fallbacks_));
  SliceAttributeTable::Builder builder(objects.size(), attr_names.size());
  for (const DataItem& attr_name_item : attr_names) {
    DCHECK(!IsContainerSchemaAttrName(attr_name_item));
    if (IsSchemaAnnotationAttrName(attr_name_item)) {
      continue;
    }
    absl::string_view attr_name = attr_name_item.value<arolla::Text>().view();
    ASSIGN_OR_RETURN(
        DataSliceImpl attr_schemas,
        databag_.GetSchemaAttrAllowMissing(schemas, attr_name, fallbacks_));
    if (attr_schemas.present_count() == 0) {
      continue;
    }
    ASSIGN_OR_RETURN(DataSliceImpl values,
                     databag_.GetAttr(objects, attr_name, fallbacks_));
    ASSIGN_OR_RETURN(values, GetValues(values, attr_schemas));
    builder.Add(attr_name, std::move(values), std::move(attr_schemas));
  }
  SliceAttributeTable attrs = std::move(builder).Build();
  for (int64_t i = 0; i < objects.size(); ++i) {
    RETURN_IF_ERROR(visitor_->VisitObject(objects[i], schemas[i],
                                          /*is_object_schema=*/true,
                                          attrs.NamesOf(i), attrs.ValuesOf(i)));
  }
  return absl::OkStatus();
}

absl::Status VectorizedTraverser::VisitInReverseTopologicalOrder() {
  for (auto it = topological_order_.rbegin(); it != topological_order_.rend();
       ++it) {
    RETURN_IF_ERROR(arolla::CheckCancellation());
    RETURN_IF_ERROR(Visit(*it));
  }
  return absl::OkStatus();
}

}  // namespace koladata::internal
