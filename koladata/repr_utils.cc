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
#include "koladata/repr_utils.h"

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/btree_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/jagged_shape/dense_array/jagged_shape.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

absl::StatusOr<std::string> DataItemToStr(const DataSlice& ds);

// Returns the string representation of list schema. `schema` must be schema
// type and DataItem. Returns empty string if it doesn't contain list item
// schema attr.
absl::StatusOr<std::string> ListSchemaStr(const DataSlice& schema) {
  ASSIGN_OR_RETURN(DataSlice empty,
                   DataSlice::Create(internal::DataItem(std::nullopt),
                                     schema.GetSchema().item()));
  ASSIGN_OR_RETURN(DataSlice attr, schema.GetAttrWithDefault(
                                       schema::kListItemsSchemaAttr, empty));
  if (attr.impl_empty_and_unknown()) {
    return "";
  }
  ASSIGN_OR_RETURN(std::string str, DataItemToStr(attr));
  return absl::StrCat("LIST[", str, "]");
}

// Returns the string representation of list schema. `schema` must be schema
// type and DataItem. Returns empty string if it doesn't contain list item
// schema attr.
absl::StatusOr<std::string> DictSchemaStr(const DataSlice& schema) {
  ASSIGN_OR_RETURN(DataSlice empty,
                   DataSlice::Create(internal::DataItem(std::nullopt),
                                     schema.GetSchema().item()));
  ASSIGN_OR_RETURN(DataSlice key_attr, schema.GetAttrWithDefault(
                                           schema::kDictKeysSchemaAttr, empty));
  ASSIGN_OR_RETURN(
      DataSlice value_attr,
      schema.GetAttrWithDefault(schema::kDictValuesSchemaAttr, empty));
  if (key_attr.impl_empty_and_unknown() ||
      value_attr.impl_empty_and_unknown()) {
    return "";
  }
  ASSIGN_OR_RETURN(std::string key_attr_str, DataItemToStr(key_attr));
  ASSIGN_OR_RETURN(std::string value_attr_str, DataItemToStr(value_attr));
  return absl::StrCat("DICT{", key_attr_str, ", ", value_attr_str, "}");
}

// Returns the string representation of list item.
absl::StatusOr<std::string> ListToStr(const DataSlice& ds) {
  ASSIGN_OR_RETURN(const DataSlice list, ds.ExplodeList(0, std::nullopt));
  ASSIGN_OR_RETURN(const std::string str, DataSliceToStr(list));
  return absl::StrCat("List", str);
}

// Returns the string representation of dict item.
absl::StatusOr<std::string> DictToStr(const DataSlice& ds) {
  ASSIGN_OR_RETURN(const DataSlice keys, ds.GetDictKeys());
  const internal::DataSliceImpl& key_slice = keys.slice();
  std::vector<std::string> elements;
  elements.reserve(key_slice.size());
  for (const internal::DataItem& item : key_slice) {
    ASSIGN_OR_RETURN(DataSlice key,
                     DataSlice::Create(item, keys.GetSchemaImpl(), ds.GetDb()));
    ASSIGN_OR_RETURN(DataSlice value, ds.GetFromDict(key));
    ASSIGN_OR_RETURN(std::string key_str, DataItemToStr(key));
    ASSIGN_OR_RETURN(std::string value_str, DataItemToStr(value));
    elements.emplace_back(absl::StrCat(key_str, "=", value_str));
  }
  return absl::StrCat("Dict{", absl::StrJoin(elements, ", "), "}");
}

// Returns the string representation of schema item.
absl::StatusOr<std::string> SchemaToStr(const DataSlice& ds) {
  ASSIGN_OR_RETURN(absl::btree_set<arolla::Text> attr_names, ds.GetAttrNames());
  std::vector<std::string> parts;
  parts.reserve(attr_names.size());
  for (const arolla::Text& attr_name : attr_names) {
    ASSIGN_OR_RETURN(DataSlice value, ds.GetAttr(attr_name));
    ASSIGN_OR_RETURN(std::string value_str, DataItemToStr(value));

    parts.emplace_back(
        absl::StrCat(absl::StripPrefix(absl::StripSuffix(attr_name, "'"), "'"),
                     "=", value_str));
  }
  return absl::StrJoin(parts, ", ");
}

absl::StatusOr<std::string> DataItemToStr(const DataSlice& ds) {
  const internal::DataItem& data_item = ds.item();

  if (data_item.template holds_value<internal::ObjectId>()) {
    const internal::ObjectId& obj =
        data_item.template value<internal::ObjectId>();
    if (obj.IsList()) {
      return ListToStr(ds);
    }
    if (obj.IsDict()) {
      return DictToStr(ds);
    }
    absl::string_view prefix = "Entity(";
    if (obj.IsExplicitSchema()) {
      ASSIGN_OR_RETURN(std::string list_schema_str, ListSchemaStr(ds));
      if (!list_schema_str.empty()) {
        return list_schema_str;
      }
      ASSIGN_OR_RETURN(std::string dict_schema_str, DictSchemaStr(ds));
      if (!dict_schema_str.empty()) {
        return dict_schema_str;
      }
      prefix = "SCHEMA(";
    } else if (obj.IsImplicitSchema()) {
      prefix = "IMPLICIT_SCHEMA(";
    } else if (ds.GetSchemaImpl() == schema::kObject) {
      prefix = "Obj(";
    }
    ASSIGN_OR_RETURN(std::string schema_str, SchemaToStr(ds));
    return absl::StrCat(prefix, schema_str, ")");
  }
  return absl::StrCat(data_item);
}

struct FormatOptions {
  absl::string_view prefix = "";
  absl::string_view suffix = "";
  bool enable_multiline = true;
  int max_width = 90;
};

// Returns the string format of DataSlice content with proper (multiline)
// layout and separators.
std::string PrettyFormatStr(const std::vector<std::string>& parts,
                            const FormatOptions& options) {
  bool parts_multilined =
      std::find_if(parts.begin(), parts.end(), [](const std::string& str) {
        return absl::StrContains(str, '\n');
      }) != parts.end();
  int total_len = std::accumulate(
      parts.begin(), parts.end(), 0, [](int sum, const std::string& str) {
        return sum + str.size() + 2 /*separator has length 2*/;
      });

  bool use_multiline = options.enable_multiline &&
                       (parts_multilined || total_len > options.max_width);

  absl::string_view sep = use_multiline ? ",\n" : ", ";
  absl::string_view indent = "\n";
  std::string prefix(options.prefix);
  std::string suffix(options.suffix);
  if (use_multiline) {
    indent = "\n  ";
    prefix = prefix.empty() ? prefix : absl::StrCat(prefix, "\n");
    suffix = suffix.empty() ? suffix : absl::StrCat(",\n", suffix);
  }
  std::string joined_parts = absl::StrCat(prefix, absl::StrJoin(parts, sep));
  absl::StrReplaceAll({{"\n", indent}}, &joined_parts);
  return absl::StrCat(joined_parts, suffix);
}

// Returns the string representation of the element in each edge group.
absl::StatusOr<std::vector<std::string>> StringifyGroup(
    const arolla::DenseArrayEdge& edge, const std::vector<std::string>& parts) {
  std::vector<std::string> result;
  result.reserve(edge.child_size());
  const arolla::DenseArray<int64_t>& edge_values = edge.edge_values();
  if (!edge_values.IsFull()) {
    return absl::InternalError("Edge contains missing value.");
  }
  for (int64_t i = 0; i < edge_values.size() - 1; ++i) {
    arolla::OptionalValue<int64_t> start = edge_values[i];
    arolla::OptionalValue<int64_t> end = edge_values[i + 1];
    std::vector<std::string> elements;
    elements.reserve(end.value - start.value);
    for (int64_t offset = start.value; offset < end.value; ++offset) {
      elements.emplace_back(parts[offset]);
    }
    result.emplace_back(
        PrettyFormatStr(elements, {.prefix = "[", .suffix = "]"}));
  }
  return result;
}

absl::StatusOr<std::vector<std::string>> StringifyByDimension(
    const internal::DataSliceImpl& slice,
    absl::Span<const arolla::DenseArrayEdge> edges, const DataBagPtr& data_bag,
    int64_t dimension) {
  const arolla::DenseArrayEdge& edge = edges[dimension];
  if (dimension == edges.size() - 1) {
    // Turns each items in slice into a string.
    std::vector<std::string> parts;
    parts.reserve(slice.size());
    for (const internal::DataItem& item : slice) {
      // print dict/list content when they are elements of DataSlice.
      if (item.is_list() || item.is_dict()) {
        ASSIGN_OR_RETURN(DataSlice item_slice,
                         DataSlice::Create(
                             item, internal::DataItem(schema::kAny), data_bag));
        ASSIGN_OR_RETURN(std::string item_str, DataItemToStr(item_slice));
        parts.emplace_back(std::move(item_str));
      } else {
        parts.emplace_back(absl::StrCat(item));
      }
    }
    return StringifyGroup(edge, parts);
  }
  ASSIGN_OR_RETURN(std::vector<std::string> parts,
                   StringifyByDimension(slice, edges, data_bag, dimension + 1));
  return StringifyGroup(edge, parts);
}

// Returns the string for python __str__ and part of __repr__.
// This method requires the DataSlice contains DataSliceImpl.
// TODO: do truncation when ds is too large.
absl::StatusOr<std::string> DataSliceImplToStr(const DataSlice& ds) {
  const arolla::JaggedDenseArrayShape& shape = ds.GetShape();
  ASSIGN_OR_RETURN(
      std::vector<std::string> parts,
      StringifyByDimension(ds.slice(), shape.edges(), ds.GetDb(), 0));
  return PrettyFormatStr(
      parts, {.prefix = "", .suffix = "", .enable_multiline = false});
}

}  // namespace

absl::StatusOr<std::string> DataSliceToStr(const DataSlice& ds) {
  return ds.VisitImpl([&ds]<typename T>(const T& impl) {
    return std::is_same_v<T, internal::DataItem> ? DataItemToStr(ds)
                                                 : DataSliceImplToStr(ds);
  });
}

}  // namespace koladata
