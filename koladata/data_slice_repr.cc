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
#include "koladata/data_slice_repr.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/types/span.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

using ::koladata::internal::DataItem;
using ::koladata::internal::DataItemRepr;
using ::koladata::internal::ObjectId;

constexpr absl::string_view kEllipsis = "...";

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
    const arolla::DenseArrayEdge& edge, const std::vector<std::string>& parts,
    int64_t item_limit) {
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
    size_t item_count = 0;
    for (int64_t offset = start.value; offset < end.value; ++offset) {
      if (item_count >= item_limit) {
        elements.emplace_back(kEllipsis);
        break;
      }
      elements.emplace_back(parts[offset]);
      ++item_count;
    }
    result.emplace_back(
        PrettyFormatStr(elements, {.prefix = "[", .suffix = "]"}));
  }
  return result;
}

// Returns the string representation for the DataSlice. It requires the
// DataSlice contains only DataItem.
absl::StatusOr<std::string> DataItemToStr(const DataSlice& ds,
                                          const ReprOption& option);

absl::StatusOr<std::vector<std::string>> StringifyByDimension(
    const DataSlice& slice, int64_t dimension, const ReprOption& option) {
  const internal::DataSliceImpl& slice_impl = slice.slice();
  const absl::Span<const arolla::DenseArrayEdge> edges =
      slice.GetShape().edges();
  const arolla::DenseArrayEdge& edge = edges[dimension];
  const DataItem& schema = slice.GetSchemaImpl();
  if (dimension == edges.size() - 1) {
    // Turns each items in slice into a string.
    std::vector<std::string> parts;
    parts.reserve(slice.size());
    bool obj_or_any_schema =
        schema == schema::kObject || schema == schema::kAny;
    for (const DataItem& item : slice_impl) {
      if (item.holds_value<ObjectId>()) {
        absl::string_view item_prefix = "";
        if (item.is_dict()) {
          item_prefix = "Dict:";
        } else if (item.is_list()) {
          item_prefix = "List:";
        } else if (slice.GetSchemaImpl() == schema::kObject) {
          item_prefix = "Obj:";
        } else if (!item.is_schema()) {
          item_prefix = "Entity:";
        }
        parts.push_back(absl::StrCat(item_prefix, DataItemRepr(item)));
      } else {
        parts.push_back(DataItemRepr(item, {.show_dtype = obj_or_any_schema}));
      }
    }
    return StringifyGroup(edge, parts, option.item_limit);
  }
  ASSIGN_OR_RETURN(std::vector<std::string> parts,
                   StringifyByDimension(slice, dimension + 1,
                                        {.depth = option.depth - 1,
                                         .item_limit = option.item_limit}));
  return StringifyGroup(edge, parts, option.item_limit);
}

// Returns the string for python __str__ and part of __repr__.
// The DataSlice must have at least 1 dimension.
// TODO: do truncation when ds is too large.
absl::StatusOr<std::string> DataSliceImplToStr(
    const DataSlice& ds, const ReprOption& option = ReprOption{}) {
  ASSIGN_OR_RETURN(std::vector<std::string> parts,
                   StringifyByDimension(ds, 0, option));
  return PrettyFormatStr(
      parts, {.prefix = "", .suffix = "", .enable_multiline = false});
}

// Returns the string representation of list schema. `schema` must be schema
// type and DataItem. Returns empty string if it doesn't contain list item
// schema attr.
absl::StatusOr<std::string> ListSchemaStr(const DataSlice& schema,
                                          int64_t depth) {
  ASSIGN_OR_RETURN(
      DataSlice empty,
      DataSlice::Create(DataItem(std::nullopt), schema.GetSchema().item()));
  ASSIGN_OR_RETURN(DataSlice attr, schema.GetAttrWithDefault(
                                       schema::kListItemsSchemaAttr, empty));
  if (attr.impl_empty_and_unknown()) {
    return "";
  }
  ASSIGN_OR_RETURN(std::string str, DataItemToStr(attr, {.depth = depth}));
  return absl::StrCat("LIST[", str, "]");
}

// Returns the string representation of list schema. `schema` must be schema
// type and DataItem. Returns empty string if it doesn't contain list item
// schema attr.
absl::StatusOr<std::string> DictSchemaStr(const DataSlice& schema,
                                          int64_t depth) {
  ASSIGN_OR_RETURN(
      DataSlice empty,
      DataSlice::Create(DataItem(std::nullopt), schema.GetSchema().item()));
  ASSIGN_OR_RETURN(DataSlice key_attr, schema.GetAttrWithDefault(
                                           schema::kDictKeysSchemaAttr, empty));
  ASSIGN_OR_RETURN(
      DataSlice value_attr,
      schema.GetAttrWithDefault(schema::kDictValuesSchemaAttr, empty));
  if (key_attr.impl_empty_and_unknown() ||
      value_attr.impl_empty_and_unknown()) {
    return "";
  }
  ASSIGN_OR_RETURN(std::string key_attr_str,
                   DataItemToStr(key_attr, {.depth = depth}));
  ASSIGN_OR_RETURN(std::string value_attr_str,
                   DataItemToStr(value_attr, {.depth = depth}));
  return absl::StrCat("DICT{", key_attr_str, ", ", value_attr_str, "}");
}

// Returns the string representation of list item.
absl::StatusOr<std::string> ListToStr(const DataSlice& ds,
                                      const ReprOption& option) {
  ASSIGN_OR_RETURN(const DataSlice list, ds.ExplodeList(0, std::nullopt));

  auto stringfy_list_items =
      [&option, &list](const internal::DataSliceImpl& list_impl)
      -> absl::StatusOr<std::string> {
    std::vector<std::string> elements;
    elements.reserve(list_impl.size());
    size_t item_count = 0;
    for (const internal::DataItem& item : list_impl) {
      if (item_count >= option.item_limit) {
        elements.emplace_back(kEllipsis);
        break;
      }
      auto item_schema = list.GetSchema();
      ASSIGN_OR_RETURN(
          DataSlice item_slice,
          DataSlice::Create(item, item_schema.item(), list.GetBag()));
      ASSIGN_OR_RETURN(std::string item_str, DataItemToStr(item_slice, option));
      elements.emplace_back(std::move(item_str));
      ++item_count;
    }
    return PrettyFormatStr(elements, {.prefix = "[", .suffix = "]"});
  };
  ASSIGN_OR_RETURN(const std::string str, stringfy_list_items(list.slice()));
  return absl::StrCat("List", str);
}

// Returns the string representation of dict item.
absl::StatusOr<std::string> DictToStr(const DataSlice& ds,
                                      const ReprOption& option) {
  ASSIGN_OR_RETURN(const DataSlice keys, ds.GetDictKeys());
  const internal::DataSliceImpl& key_slice = keys.slice();
  std::vector<std::string> elements;
  elements.reserve(key_slice.size());
  size_t item_count = 0;
  for (const DataItem& item : key_slice) {
    if (item_count >= option.item_limit) {
      elements.emplace_back(kEllipsis);
      break;
    }
    ASSIGN_OR_RETURN(
        DataSlice key,
        DataSlice::Create(item, keys.GetSchemaImpl(), ds.GetBag()));
    ASSIGN_OR_RETURN(DataSlice value, ds.GetFromDict(key));
    ASSIGN_OR_RETURN(std::string key_str, DataItemToStr(key, option));
    ASSIGN_OR_RETURN(std::string value_str, DataItemToStr(value, option));
    elements.emplace_back(absl::StrCat(key_str, "=", value_str));
    ++item_count;
  }
  return absl::StrCat("Dict{", absl::StrJoin(elements, ", "), "}");
}

// Returns the string representation of schema item.
absl::StatusOr<std::string> SchemaToStr(const DataSlice& ds,
                                        const ReprOption& option) {
  ASSIGN_OR_RETURN(DataSlice::AttrNamesSet attr_names, ds.GetAttrNames());
  std::vector<std::string> parts;
  parts.reserve(attr_names.size());
  size_t item_count = 0;
  for (const std::string& attr_name : attr_names) {
    // Keeps all attributes from schema.
    if (!ds.item().is_schema() && item_count >= option.item_limit) {
      parts.emplace_back(kEllipsis);
      break;
    }
    ASSIGN_OR_RETURN(DataSlice value, ds.GetAttr(attr_name));
    ASSIGN_OR_RETURN(std::string value_str, DataItemToStr(value, option));

    parts.emplace_back(
        absl::StrCat(absl::StripPrefix(absl::StripSuffix(attr_name, "'"), "'"),
                     "=", value_str));
    ++item_count;
  }
  return absl::StrJoin(parts, ", ");
}

absl::StatusOr<std::string> DataItemToStr(const DataSlice& ds,
                                          const ReprOption& option) {
  const DataItem& data_item = ds.item();
  DCHECK_GE(option.depth, 0);
  if (option.depth == 0) {
    return DataItemRepr(data_item);
  }

  ReprOption next_option = option;
  --next_option.depth;

  const DataItem& schema = ds.GetSchemaImpl();
  if (data_item.template holds_value<ObjectId>()) {
    // STRING items inside Lists and Dicts are quoted.
    next_option.strip_quotes = false;
    if (ds.GetBag() == nullptr) {
      return DataItemRepr(data_item);
    }

    const ObjectId& obj = data_item.template value<ObjectId>();
    if (schema.holds_value<ObjectId>() &&
        schema.template value<ObjectId>().IsNoFollowSchema()) {
      return absl::StrCat("Nofollow(Entity:", DataItemRepr(data_item), ")");
    }
    if (obj.IsList()) {
      return ListToStr(ds, next_option);
    }
    if (obj.IsDict()) {
      return DictToStr(ds, next_option);
    }
    absl::string_view prefix = "Entity(";
    if (obj.IsNoFollowSchema()) {
      if (obj == ObjectId::NoFollowObjectSchemaId()) {
        return "NOFOLLOW(OBJECT)";
      }
      const DataItem original =
          DataItem(internal::GetOriginalFromNoFollow(obj));
      return absl::StrCat("NOFOLLOW(", DataItemRepr(original), ")");
    } else if (obj.IsExplicitSchema()) {
      ASSIGN_OR_RETURN(std::string list_schema_str,
                       ListSchemaStr(ds, next_option.depth));
      if (!list_schema_str.empty()) {
        return list_schema_str;
      }
      ASSIGN_OR_RETURN(std::string dict_schema_str,
                       DictSchemaStr(ds, next_option.depth));
      if (!dict_schema_str.empty()) {
        return dict_schema_str;
      }
      prefix = "SCHEMA(";
    } else if (obj.IsImplicitSchema()) {
      prefix = "IMPLICIT_SCHEMA(";
    } else if (schema == schema::kObject) {
      prefix = "Obj(";
    }
    ASSIGN_OR_RETURN(std::string schema_str, SchemaToStr(ds, next_option));
    if (schema_str.empty() && !obj.IsSchema()) {
      return absl::StrCat(prefix, "):", DataItemRepr(data_item));
    }
    return absl::StrCat(prefix, schema_str, ")");
  }
  bool obj_or_any_schema = schema == schema::kObject || schema == schema::kAny;
  return DataItemRepr(data_item, {.strip_quotes = option.strip_quotes,
                                  .show_dtype = obj_or_any_schema});
}

}  // namespace

absl::StatusOr<std::string> DataSliceToStr(const DataSlice& ds,
                                           const ReprOption& option) {
  DCHECK_GE(option.depth, 0);
  return ds.VisitImpl([&ds, &option]<typename T>(const T& impl) {
    return std::is_same_v<T, DataItem> ? DataItemToStr(ds, option)
                                       : DataSliceImplToStr(ds, option);
  });
}

}  // namespace koladata
