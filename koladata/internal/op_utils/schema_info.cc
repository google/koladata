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
#include "koladata/internal/op_utils/schema_info.h"

#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"

namespace koladata::internal {

absl::StatusOr<EntitySchemaInfo> GetEntitySchemaInfo(
    const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks) {
  DCHECK(schema.holds_value<ObjectId>());
  if (schema.value<ObjectId>().IsNoFollowSchema()) {
    // Attributes of NoFollow schemas are not traversed.
    return ObjectSchemaInfo();
  }
  ASSIGN_OR_RETURN(std::vector<DataItem> attr_names,
                   databag.GetSchemaAttrsAsVector(schema, fallbacks,
                                                  /*filter_removed=*/true));
  if (attr_names.empty()) {
    return ObjectSchemaInfo();
  }
  bool has_list_items_attr = false;
  bool has_dict_keys_attr = false;
  bool has_dict_values_attr = false;
  for (const DataItem& attr_name_item : attr_names) {
    absl::string_view attr_name = attr_name_item.value<arolla::Text>().view();
    if (attr_name == schema::kListItemsSchemaAttr) {
      has_list_items_attr = true;
    } else if (attr_name == schema::kDictKeysSchemaAttr) {
      has_dict_keys_attr = true;
    } else if (attr_name == schema::kDictValuesSchemaAttr) {
      has_dict_values_attr = true;
    }
  }
  if (has_list_items_attr) {
    if (attr_names.size() != 1) {
      return absl::InvalidArgumentError(
          absl::StrFormat("list schema %v has unexpected attributes", schema));
    }
    ASSIGN_OR_RETURN(
        DataItem item_schema,
        databag.GetSchemaAttr(schema, schema::kListItemsSchemaAttr, fallbacks));
    return ListSchemaInfo{.item_schema = std::move(item_schema)};
  }
  if (has_dict_keys_attr || has_dict_values_attr) {
    if (attr_names.size() != 2 || !has_dict_keys_attr ||
        !has_dict_values_attr) {
      return absl::InvalidArgumentError(
          absl::StrFormat("dict schema %v has unexpected attributes", schema));
    }
    ASSIGN_OR_RETURN(
        DataItem keys_schema,
        databag.GetSchemaAttr(schema, schema::kDictKeysSchemaAttr, fallbacks));
    ASSIGN_OR_RETURN(DataItem values_schema,
                     databag.GetSchemaAttr(
                         schema, schema::kDictValuesSchemaAttr, fallbacks));
    return DictSchemaInfo{.keys_schema = std::move(keys_schema),
                          .values_schema = std::move(values_schema)};
  }
  std::erase_if(attr_names, IsSchemaAnnotationAttrName);
  return ObjectSchemaInfo{.attr_names = std::move(attr_names)};
}

absl::StatusOr<std::vector<DataItem>> GetSchemasAttrNames(
    const DataSliceImpl& schemas, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks) {
  DCHECK_GT(schemas.size(), 0);
  DCHECK(schemas.present(0));
  ObjectId schema = schemas.values<ObjectId>().values[0];
  if (schema.IsNoFollowSchema()) {
    return std::vector<DataItem>();
  }
  if (schema.IsSmallAlloc()) {
    return databag.GetSchemaAttrsAsVector(DataItem(schema), fallbacks);
  }
  return databag.GetSchemaAttrsForBigAllocationAsVector(AllocationId(schema),
                                                        fallbacks);
}

}  // namespace koladata::internal
