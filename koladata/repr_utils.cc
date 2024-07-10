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
#include <functional>
#include <numeric>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/triples.h"
#include "koladata/s11n/codec.pb.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

using DataItemProto = ::koladata::s11n::KodaV1Proto::DataItemProto;
using ::koladata::DataBagPtr;
using ::koladata::internal::DataBagContent;
using ::koladata::internal::DataItem;
using ::koladata::internal::Error;
using ::koladata::internal::GetErrorPayload;
using ::koladata::internal::debug::AttrTriple;
using ::koladata::internal::debug::DictItemTriple;
using ::koladata::internal::debug::Triples;

constexpr int kTwoSpaceIndentation = 2;

constexpr absl::string_view kDictValuesNameReplacement = "<dict value>";
constexpr absl::string_view kListItemsNameReplacement = "<list items>";
constexpr absl::string_view kSchemaNameReplacement = "<object schemas>";

absl::StatusOr<std::string> DataItemToStr(const DataSlice& ds);

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

// Returns the string representation for the ObjectId. UUID has prefix 'k',
// others are '$'.
std::string ObjectIdStr(const internal::ObjectId& id) {
  absl::string_view prefix = id.IsUuid() ? "k" : "$";
  return absl::StrCat(prefix, id);
}

// Returns the string representation for the DataItem.
std::string DataItemStr(const internal::DataItem& item,
                        bool strip_text = false) {
  if (item.holds_value<internal::ObjectId>()) {
    return ObjectIdStr(item.value<internal::ObjectId>());
  }
  if (item.holds_value<arolla::Text>() && strip_text) {
    return absl::StrCat(
        absl::StripPrefix(absl::StripSuffix(absl::StrCat(item), "'"), "'"));
  }
  return absl::StrCat(item);
}

absl::StatusOr<std::vector<std::string>> StringifyByDimension(
    const DataSlice& slice, int64_t dimension, bool show_content) {
  const internal::DataSliceImpl& slice_impl = slice.slice();
  const absl::Span<const arolla::DenseArrayEdge> edges =
      slice.GetShape().edges();
  const arolla::DenseArrayEdge& edge = edges[dimension];
  if (dimension == edges.size() - 1) {
    // Turns each items in slice into a string.
    std::vector<std::string> parts;
    parts.reserve(slice.size());
    for (const internal::DataItem& item : slice_impl) {
      // print item content when they are in List.
      if (show_content) {
        ASSIGN_OR_RETURN(
            DataSlice item_slice,
            DataSlice::Create(item, slice.GetSchemaImpl(), slice.GetDb()));
        ASSIGN_OR_RETURN(std::string item_str, DataItemToStr(item_slice));
        parts.push_back(std::move(item_str));
      } else {
        if (item.holds_value<internal::ObjectId>()) {
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
          parts.push_back(absl::StrCat(item_prefix, DataItemStr(item)));
        } else {
          parts.push_back(absl::StrCat(item));
        }
      }
    }
    return StringifyGroup(edge, parts);
  }
  ASSIGN_OR_RETURN(std::vector<std::string> parts,
                   StringifyByDimension(slice, dimension + 1, show_content));
  return StringifyGroup(edge, parts);
}

// Returns the string for python __str__ and part of __repr__.
// The DataSlice must have at least 1 dimension. If `show_content` is true, the
// content of List, Dict, Entity and Object will be printed to the string
// instead of ItemId representation.
// TODO: Add recursion depth limit and cycle prevention.
// TODO: do truncation when ds is too large.
absl::StatusOr<std::string> DataSliceImplToStr(const DataSlice& ds,
                                               bool show_content = false) {
  ASSIGN_OR_RETURN(std::vector<std::string> parts,
                   StringifyByDimension(ds, 0, show_content));
  return PrettyFormatStr(
      parts, {.prefix = "", .suffix = "", .enable_multiline = false});
}

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
  ASSIGN_OR_RETURN(const std::string str,
                   DataSliceImplToStr(list, /*show_content=*/true));
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
    if (schema_str.empty() && !obj.IsSchema()) {
      return absl::StrCat(prefix, "):", DataItemStr(data_item));
    }
    return absl::StrCat(prefix, schema_str, ")");
  }
  return absl::StrCat(data_item);
}

// Returns true if a DataItem holds '__items__', '__keys__', '__values__'
// These attributes will be hidden when printing the DataBag.
bool IsInternalAttribute(const internal::DataItem& item) {
  if (!item.holds_value<arolla::Text>()) {
    return false;
  }
  absl::string_view attr = item.value<arolla::Text>().view();
  return attr == schema::kListItemsSchemaAttr ||
         attr == schema::kDictKeysSchemaAttr ||
         attr == schema::kDictValuesSchemaAttr;
}

absl::StatusOr<internal::DataItem> DecodeDataItem(
    const DataItemProto& item_proto) {
  switch (item_proto.value_case()) {
    case s11n::KodaV1Proto::DataItemProto::kDtype:
      return internal::DataItem(schema::DType(item_proto.dtype()));
    case s11n::KodaV1Proto::DataItemProto::kObjectId:
      return internal::DataItem(
          internal::ObjectId::UnsafeCreateFromInternalHighLow(
              item_proto.object_id().hi(), item_proto.object_id().lo()));
    default:
      return absl::InvalidArgumentError("Unsupported proto");
  }
}

absl::StatusOr<Error> SetNoCommonSchemaError(
    Error cause, absl::Span<const koladata::DataBagPtr> dbs) {
  DataBagPtr db = DataBag::ImmutableEmptyWithFallbacks(dbs);
  ASSIGN_OR_RETURN(internal::DataItem common_schema_item,
                   DecodeDataItem(cause.no_common_schema().common_schema()));
  ASSIGN_OR_RETURN(
      internal::DataItem conflict_schema_item,
      DecodeDataItem(cause.no_common_schema().conflicting_schema()));

  ASSIGN_OR_RETURN(DataSlice common_schema,
                   DataSlice::Create(common_schema_item,
                                     internal::DataItem(schema::kSchema), db));
  ASSIGN_OR_RETURN(DataSlice conflict_schema,
                   DataSlice::Create(conflict_schema_item,
                                     internal::DataItem(schema::kSchema), db));

  ASSIGN_OR_RETURN(std::string common_schema_str,
                   DataSliceToStr(common_schema));
  ASSIGN_OR_RETURN(std::string conflict_schema_str,
                   DataSliceToStr(conflict_schema));

  Error error;
  error.set_error_message(
      absl::StrFormat("\ncannot find a common schema for provided schemas\n\n"
                      " the common schema(s) %s: %s\n"
                      " the first conflicting schema %s: %s",
                      common_schema_item.DebugString(), common_schema_str,
                      conflict_schema_item.DebugString(), conflict_schema_str));
  *error.mutable_cause() = std::move(cause);
  return error;
}

struct DataBagFormatOption {
  int indentation = 0;
  std::optional<int> fallback_index;
};

absl::StatusOr<std::string> DataBagToStrInternal(
    const DataBagPtr& db, absl::flat_hash_set<const DataBag*>& seen_db,
    const DataBagFormatOption& format_opt) {
  std::string line_indent(format_opt.indentation * kTwoSpaceIndentation, ' ');
  if (seen_db.contains(db.get())) {
    return absl::StrCat(line_indent, "fallback #", *format_opt.fallback_index,
                        " duplicated, see db with id: ", GetBagIdRepr(db));
  }
  seen_db.emplace(db.get());

  std::string res =
      format_opt.fallback_index
          ? absl::StrCat(line_indent, "fallback #", *format_opt.fallback_index,
                         " ", GetBagIdRepr(db), ":\n", line_indent,
                         "DataBag:\n")
          : absl::StrCat(line_indent, "DataBag ", GetBagIdRepr(db), ":\n");
  ASSIGN_OR_RETURN(DataBagContent content, db->GetImpl().ExtractContent());
  Triples main_triples(content);
  for (const AttrTriple& attr : main_triples.attributes()) {
    absl::StrAppend(
        &res, line_indent,
        absl::StrFormat("%s.%s => %s\n", ObjectIdStr(attr.object),
                        attr.attribute,
                        DataItemStr(attr.value, /*strip_text=*/true)));
  }
  for (const auto& [list_id, values] : main_triples.lists()) {
    absl::StrAppend(
        &res, line_indent,
        absl::StrFormat(
            "%s[:] => [%s]\n", ObjectIdStr(list_id),
            absl::StrJoin(values.begin(), values.end(), ", ",
                          [](std::string* out, const internal::DataItem& item) {
                            out->append(DataItemStr(item));
                          })));
  }
  for (const DictItemTriple& dict : main_triples.dicts()) {
    if (dict.object.IsDict()) {
      absl::StrAppend(
          &res, line_indent,
          absl::StrFormat("%s[%s] => %s\n", ObjectIdStr(dict.object),
                          DataItemStr(dict.key), DataItemStr(dict.value)));
    }
  }
  absl::StrAppend(&res, "\n", line_indent, "SchemaBag:\n");
  for (const DictItemTriple& dict : main_triples.dicts()) {
    if (dict.object.IsSchema() && !IsInternalAttribute(dict.key)) {
      absl::StrAppend(
          &res, line_indent,
          absl::StrFormat("%s.%s => %s\n", ObjectIdStr(dict.object),
                          DataItemStr(dict.key, /*strip_text=*/true),
                          DataItemStr(dict.value)));
    }
  }
  const std::vector<DataBagPtr>& fallbacks = db->GetFallbacks();
  if (!fallbacks.empty()) {
    absl::StrAppend(&res, "\n", line_indent, fallbacks.size(),
                    " fallback DataBag(s):\n");
  }
  absl::string_view sep = "";
  for (int i = 0; i < fallbacks.size(); ++i) {
    ASSIGN_OR_RETURN(
        std::string content,
        DataBagToStrInternal(
            fallbacks.at(i), seen_db,
            {.indentation = format_opt.indentation + 1, .fallback_index = i}));
    absl::StrAppend(&res, sep, content);
    sep = "\n";
  }
  return res;
}

}  // namespace

absl::StatusOr<std::string> DataSliceToStr(const DataSlice& ds) {
  return ds.VisitImpl([&ds]<typename T>(const T& impl) {
    return std::is_same_v<T, internal::DataItem> ? DataItemToStr(ds)
                                                 : DataSliceImplToStr(ds);
  });
}

absl::StatusOr<std::string> DataBagToStr(const DataBagPtr& db) {
  absl::flat_hash_set<const DataBag*> seen_db;
  ASSIGN_OR_RETURN(std::string res,
                   DataBagToStrInternal(db, seen_db, {.indentation = 0}));
  return res;
}

absl::Status AssembleErrorMessage(const absl::Status& status,
                                  absl::Span<const koladata::DataBagPtr> dbs) {
  std::optional<Error> cause = GetErrorPayload(status);
  if (!cause) {
    return status;
  }
  if (cause->has_no_common_schema()) {
    ASSIGN_OR_RETURN(Error error,
                     SetNoCommonSchemaError(std::move(*cause), dbs));
    return WithErrorPayload(status, error);
  }
  return status;
}

template <typename Map>
void UpdateCountMap(const typename Map::key_type& val, Map& count_dict) {
  static_assert(std::is_same<typename Map::mapped_type, int64_t>::value,
                "mapped_type must be int64_t");
  auto [it, inserted] = count_dict.emplace(val, 1);
  if (!inserted) {
    ++it->second;
  }
}

absl::StatusOr<std::string> DataBagStatistics(const DataBagPtr& db,
                                              int64_t top_attr_limit) {
  ASSIGN_OR_RETURN(DataBagContent content, db->GetImpl().ExtractContent());
  Triples main_triples(content);

  std::vector<std::pair<int, std::string>> top_attrs;

  // counts the number of attrs.
  {
    absl::flat_hash_map<std::string, int64_t> attribute_count;
    for (const AttrTriple& triple : main_triples.attributes()) {
      if (triple.attribute == schema::kSchemaAttr) {
        UpdateCountMap(std::string(kSchemaNameReplacement), attribute_count);
      } else {
        UpdateCountMap(triple.attribute, attribute_count);
      }
    }
    for (const auto& [attr, count] : attribute_count) {
      top_attrs.emplace_back(count, attr);
    }
  }

  // counts the number of lists.
  {
    int64_t list_item_count = std::accumulate(
        main_triples.lists().begin(), main_triples.lists().end(), 0,
        [](int64_t acc,
           const std::pair<const internal::ObjectId,
                           std::vector<internal::DataItem>>& list) {
          return acc + list.second.size();
        });
    if (list_item_count > 0) {
      top_attrs.emplace_back(list_item_count, kListItemsNameReplacement);
    }
  }

  // counts the number of keys in dicts.
  {
    absl::flat_hash_map<DataItem, int64_t, DataItem::Hash, DataItem::Eq>
        key_count;

    for (const DictItemTriple& dict_triple : main_triples.dicts()) {
      if (!dict_triple.object.IsDict()) {
        continue;
      }
      UpdateCountMap(dict_triple.key, key_count);
    }

    if (!key_count.empty()) {
      for (const auto& [key, count] : key_count) {
        top_attrs.emplace_back(count, kDictValuesNameReplacement);
      }
    }
  }

  int64_t schema_count = std::count_if(
      main_triples.dicts().begin(), main_triples.dicts().end(),
      [](const DictItemTriple& item) { return item.object.IsSchema(); });

  int64_t value_count = std::accumulate(
      top_attrs.begin(), top_attrs.end(), 0,
      [](int64_t acc, const std::pair<int, std::string>& attr_count) {
        return acc + attr_count.first;
      });

  std::string res = absl::StrFormat(
      "DataBag %s with %d values in %d attrs, plus %d schema values and %d "
      "fallbacks. Top attrs:\n",
      GetBagIdRepr(db), value_count, top_attrs.size(), schema_count,
      db->GetFallbacks().size());

  std::sort(top_attrs.begin(), top_attrs.end(),
            std::greater<std::pair<int64_t, std::string>>());

  if (top_attrs.size() > top_attr_limit) {
    top_attrs.resize(top_attr_limit);
  }

  for (const auto& [count, attr] : top_attrs) {
    absl::StrAppend(&res, absl::StrFormat("  %s: %d values\n", attr, count));
  }
  absl::StrAppend(&res, "Use db.contents_repr() to see the actual values.");

  return res;
}

}  // namespace koladata
