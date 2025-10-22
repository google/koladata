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
#include "koladata/metadata_utils.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/dense_array/util/concat.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_attrs.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

namespace {

absl::StatusOr<DataSlice> GetSchemaAttrsOrder(const DataSlice& ds) {
  ASSIGN_OR_RETURN(auto metadata, GetMetadataForSchemaSlice(ds));
  if (metadata.GetSchemaImpl() != schema::kObject) {
    return absl::InvalidArgumentError(absl::StrCat(
        "failed to get attrs order; metadata has unexpected schema: ",
        SchemaToStr(metadata.GetSchema())));
  }
  ASSIGN_OR_RETURN(auto attrs_order_list,
                   metadata.GetAttr(schema::kMetadataAttrsOrderAttr));
  return attrs_order_list.ExplodeList(0, std::nullopt);
}

absl::StatusOr<std::vector<std::string>> ReadStringValues(
    const DataSlice& ds) {
  if (ds.is_item()) {
    return absl::InvalidArgumentError("expected a slice, got an item");
  }
  if (ds.slice().is_empty_and_unknown()) {
    return std::vector<std::string>();
  }
  if (ds.slice().dtype() != arolla::GetQType<arolla::Text>()) {
    return absl::InvalidArgumentError(
        "DataSlice should be a DataSlice of TEXT");
  }
  auto ds_array = ds.slice().values<arolla::Text>();
  if (!ds_array.IsAllPresent()) {
    return absl::InvalidArgumentError("DataSlice should be present");
  }
  std::vector<std::string> result;
  result.reserve(ds_array.size());
  ds_array.ForEachPresent(
      [&](int64_t _, auto value) { result.emplace_back(value); });
  return result;
}

}  // namespace

absl::StatusOr<DataSlice> GetMetadataForSchemaSlice(
    const DataSlice& schema_ds) {
  auto schema = schema_ds.GetSchemaImpl();
  if (schema == schema::kSchema) {
    return schema_ds.GetAttr(schema::kSchemaMetadataAttr);
  }
  return absl::InvalidArgumentError(
      absl::StrCat("failed to get metadata; cannot get for a DataSlice with ",
                   SchemaToStr(schema_ds.GetSchema()), " schema"));
}

absl::StatusOr<DataSlice> GetOrderedAttrNames(const DataSlice& ds) {
  auto schema = ds.GetSchemaImpl();
  if (schema.is_struct_schema()) {
    return GetSchemaAttrsOrder(ds.GetSchema());
  } else if (schema.is_object_schema()) {
    if (ds.is_item()) {
      ASSIGN_OR_RETURN(auto schema_slice, ds.GetObjSchema());
      return GetSchemaAttrsOrder(schema_slice);
    }
    return absl::InvalidArgumentError(
        "cannot get attrs order for a DataSlice with OBJECT schema");
  } else if (schema.is_schema_schema()) {
    return GetSchemaAttrsOrder(ds);
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "failed to get attrs order; cannot get for a DataSlice with ",
      SchemaToStr(ds.GetSchema()), " schema"));
}

absl::StatusOr<std::vector<std::string>> GetOrderedOrLexicographicAttrNames(
    const DataSlice& ds, bool assert_order_specified) {
  auto attr_names_or = GetOrderedAttrNames(ds);
  ASSIGN_OR_RETURN(auto attr_names_set, ds.GetAttrNames());
  if (!attr_names_or.ok()) {
    if (assert_order_specified) {
      return attr_names_or.status();
    }
    return std::vector<std::string>(attr_names_set.begin(),
                                    attr_names_set.end());
  }
  ASSIGN_OR_RETURN(auto ordered_attr_names, ReadStringValues(*attr_names_or));
  if (ordered_attr_names.size() != attr_names_set.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("ordered attr names are inconsistent with the schema; "
                        "number of attributes is different %d vs %d",
                        ordered_attr_names.size(), attr_names_set.size()));
  }
  for (const auto& attr_name : ordered_attr_names) {
    if (!attr_names_set.contains(attr_name)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("ordered attr names are inconsistent with the "
                          "schema: %s is not present in the schema",
                          attr_name));
    }
  }
  return ordered_attr_names;
}

}  // namespace koladata
