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
#ifndef KOLADATA_INTERNAL_OP_UTILS_SCHEMA_INFO_H_
#define KOLADATA_INTERNAL_OP_UTILS_SCHEMA_INFO_H_

#include <variant>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_attrs.h"

namespace koladata::internal {

struct ListSchemaInfo {
  DataItem item_schema;
};

struct DictSchemaInfo {
  DataItem keys_schema;
  DataItem values_schema;
};

struct ObjectSchemaInfo {
  // Present attribute names of the schema, excluding schema annotation
  // attributes (see schema::IsSchemaAnnotationAttr). Empty for NoFollow
  // schemas.
  std::vector<DataItem> attr_names;
};

using EntitySchemaInfo =
    std::variant<ObjectSchemaInfo, ListSchemaInfo, DictSchemaInfo>;

// Classifies the entity schema `schema` as a list, dict or object/entity
// schema, validates it and reads list item / dict keys and values schemas.
// A list schema must have only the list items schema attribute, and a dict
// schema must have only the dict keys and values schema attributes.
absl::StatusOr<EntitySchemaInfo> GetEntitySchemaInfo(
    const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks = {});

// Returns attribute names of `schemas` (dense ObjectIds from a single
// allocation; a superset for big allocations, which may include removed
// attributes). NoFollow schemas have no attributes to follow.
absl::StatusOr<std::vector<DataItem>> GetSchemasAttrNames(
    const DataSliceImpl& schemas, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks = {});

// Returns the schema of the values of the schema attribute `attr_name`.
inline DataItem GetSchemaAttrSchema(absl::string_view attr_name) {
  if (attr_name == schema::kSchemaNameAttr) {
    return DataItem(schema::kString);
  } else if (attr_name == schema::kSchemaMetadataAttr) {
    return DataItem(schema::kObject);
  }
  return DataItem(schema::kSchema);
}

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_SCHEMA_INFO_H_
