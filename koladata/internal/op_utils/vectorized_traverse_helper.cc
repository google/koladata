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
#include "koladata/internal/op_utils/vectorized_traverse_helper.h"

#include <optional>
#include <utility>
#include <variant>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/strings/string_view.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/in_degree_tracker.h"
#include "koladata/internal/op_utils/schema_info.h"
#include "koladata/internal/op_utils/traverse_helper.h"

namespace koladata::internal::vectorized_traverser_internal {
namespace {

using TransitionKey = TraverseHelper::TransitionKey;
using TransitionType = TraverseHelper::TransitionType;

}  // namespace

absl::Status VectorizedTraverseHelper::ForEachRootTransition(
    const DataSliceImpl& ds, const DataItem& schema,
    TransitionCallback callback) {
  RETURN_IF_ERROR(callback(Transition{
      TransitionKey{.type = TransitionType::kSliceItem}, ds, schema}));
  return callback(Transition{std::nullopt, DataSliceImpl::Create(1, schema),
                             DataItem(schema::kSchema)});
}

absl::Status VectorizedTraverseHelper::ForEachChildOfSchemas(
    const DataSliceImpl& schemas, TransitionCallback callback) {
  RETURN_IF_ERROR(
      callback(Transition{TransitionKey{.type = TransitionType::kSchema},
                          DataSliceImpl::Create(1, DataItem(schema::kSchema)),
                          DataItem(schema::kSchema)}));
  ASSIGN_OR_RETURN(std::vector<DataItem> attr_names,
                   GetSchemasAttrNames(schemas, databag_, fallbacks_));
  for (const DataItem& attr_name : attr_names) {
    RETURN_IF_ERROR(ForEachSchemaAttributeValue(schemas, attr_name, callback));
  }
  return absl::OkStatus();
}

absl::Status VectorizedTraverseHelper::ForEachSchemaAttributeValue(
    const DataSliceImpl& schemas, const DataItem& attr_name,
    TransitionCallback callback) {
  absl::string_view attr_name_view = attr_name.value<arolla::Text>().view();
  DataItem values_schema = GetSchemaAttrSchema(attr_name_view);
  if (values_schema.is_primitive_schema()) {
    // `__schema_name__`.
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(
      DataSliceImpl attr_values,
      databag_.GetSchemaAttrAllowMissing(schemas, attr_name_view, fallbacks_));
  return callback(
      Transition{TransitionKey{.type = TransitionType::kSchemaAttributeName,
                               .value = attr_name},
                 std::move(attr_values), std::move(values_schema)});
}

absl::Status VectorizedTraverseHelper::ForEachChildOfEntities(
    const DataSliceImpl& entities, const DataItem& schema,
    TransitionCallback callback) {
  RETURN_IF_ERROR(callback(
      Transition{TransitionKey{.type = TransitionType::kSchema},
                 DataSliceImpl::Create(1, schema), DataItem(schema::kSchema)}));
  ASSIGN_OR_RETURN(EntitySchemaInfo info,
                   GetEntitySchemaInfo(schema, databag_, fallbacks_));
  if (const auto* list_info = std::get_if<ListSchemaInfo>(&info)) {
    return ForEachListItem(entities, list_info->item_schema, callback);
  }
  if (const auto* dict_info = std::get_if<DictSchemaInfo>(&info)) {
    return ForEachDictKeyAndValue(entities, dict_info->keys_schema,
                                  dict_info->values_schema, callback);
  }
  for (const DataItem& attr_name :
       std::get<ObjectSchemaInfo>(info).attr_names) {
    ASSIGN_OR_RETURN(
        DataItem attr_schema,
        databag_.GetSchemaAttr(schema, attr_name.value<arolla::Text>().view(),
                               fallbacks_));
    RETURN_IF_ERROR(
        ForEachAttributeValue(entities, attr_name, attr_schema, callback));
  }
  return absl::OkStatus();
}

absl::Status VectorizedTraverseHelper::ForEachChildOfObjects(
    const DataSliceImpl& objects, const DataSliceImpl& object_schemas,
    TransitionCallback callback) {
  ASSIGN_OR_RETURN(std::vector<DataItem> attr_names,
                   GetSchemasAttrNames(object_schemas, databag_, fallbacks_));
  if (absl::c_any_of(attr_names, IsContainerSchemaAttrName)) {
    return group_by_.ByKeys(object_schemas)
        .Split(objects)
        .ForEach([&](const DataItem& schema, DataSliceImpl entities) {
          return ForEachChildOfEntities(entities, schema, callback);
        });
  }
  RETURN_IF_ERROR(
      callback(Transition{TransitionKey{.type = TransitionType::kSchema},
                          object_schemas, DataItem(schema::kSchema)}));
  for (const DataItem& attr_name : attr_names) {
    if (!IsSchemaAnnotationAttrName(attr_name)) {
      RETURN_IF_ERROR(ForEachObjectsAttributeValue(objects, object_schemas,
                                                   attr_name, callback));
    }
  }
  return absl::OkStatus();
}

absl::Status VectorizedTraverseHelper::ForEachObjectsAttributeValue(
    const DataSliceImpl& objects, const DataSliceImpl& object_schemas,
    const DataItem& attr_name, TransitionCallback callback) {
  ASSIGN_OR_RETURN(
      DataSliceImpl attr_schemas,
      databag_.GetSchemaAttrAllowMissing(
          object_schemas, attr_name.value<arolla::Text>().view(), fallbacks_));
  return group_by_.ByKeys(std::move(attr_schemas))
      .Split(objects)
      .ForEach([&](const DataItem& attr_schema, DataSliceImpl entities) {
        return ForEachAttributeValue(entities, attr_name, attr_schema,
                                     callback);
      });
}

absl::Status VectorizedTraverseHelper::ForEachAttributeValue(
    const DataSliceImpl& entities, const DataItem& attr_name,
    const DataItem& attr_schema, TransitionCallback callback) {
  if (attr_schema.is_primitive_schema()) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(
      DataSliceImpl attr_values,
      databag_.GetAttr(entities, attr_name.value<arolla::Text>().view(),
                       fallbacks_));
  return callback(Transition{
      TransitionKey{.type = TransitionType::kAttributeName, .value = attr_name},
      std::move(attr_values), attr_schema});
}

absl::Status VectorizedTraverseHelper::ForEachListItem(
    const DataSliceImpl& lists, const DataItem& item_schema,
    TransitionCallback callback) {
  if (item_schema.is_primitive_schema()) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(
      auto items_and_edge,
      databag_.ExplodeLists(lists, DataBagImpl::ListRange(), fallbacks_));
  return callback(Transition{TransitionKey{.type = TransitionType::kListItem},
                             std::move(items_and_edge.first), item_schema});
}

absl::Status VectorizedTraverseHelper::ForEachDictKeyAndValue(
    const DataSliceImpl& dicts, const DataItem& keys_schema,
    const DataItem& values_schema, TransitionCallback callback) {
  if (keys_schema.is_primitive_schema() &&
      values_schema.is_primitive_schema()) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(auto keys_and_edge, databag_.GetDictKeys(dicts, fallbacks_));
  const DataSliceImpl& keys = keys_and_edge.first;
  if (!keys_schema.is_primitive_schema()) {
    RETURN_IF_ERROR(callback(Transition{
        TransitionKey{.type = TransitionType::kDictKey}, keys, keys_schema}));
  }
  if (values_schema.is_primitive_schema()) {
    return absl::OkStatus();
  }
  // Values are returned in the same order as keys.
  ASSIGN_OR_RETURN(auto values_and_edge,
                   databag_.GetDictValues(dicts, fallbacks_));
  return ForEachDictValueGroupedByKey(keys, values_and_edge.first,
                                      values_schema, callback);
}

absl::Status VectorizedTraverseHelper::ForEachDictValueGroupedByKey(
    const DataSliceImpl& keys, const DataSliceImpl& values,
    const DataItem& values_schema, TransitionCallback callback) {
  DataSliceImpl value_objects = InDegreeTracker::FilterToObjects(values);
  if (value_objects.present_count() == 0) {
    return absl::OkStatus();
  }
  return group_by_.ByKeys(keys)
      .Split(std::move(value_objects))
      .ForEach([&](const DataItem& key, DataSliceImpl key_values) {
        return callback(Transition{
            TransitionKey{.type = TransitionType::kDictValue, .value = key},
            std::move(key_values), values_schema});
      });
}

}  // namespace koladata::internal::vectorized_traverser_internal
