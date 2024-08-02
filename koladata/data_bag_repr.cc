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
#include "koladata/data_bag_repr.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <numeric>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/triples.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

using ::koladata::internal::DataBagContent;
using ::koladata::internal::DataItem;
using ::koladata::internal::ObjectId;
using ::koladata::internal::debug::AttrTriple;
using ::koladata::internal::debug::DictItemTriple;
using ::koladata::internal::debug::Triples;

using AttrMap =
    absl::flat_hash_map<DataItem, DataItem, DataItem::Hash, DataItem::Eq>;

constexpr int kTwoSpaceIndentation = 2;

constexpr absl::string_view kDictValuesNameReplacement = "<dict value>";
constexpr absl::string_view kListItemsNameReplacement = "<list items>";
constexpr absl::string_view kSchemaNameReplacement = "<object schemas>";

struct DataBagFormatOption {
  int indentation = 0;
  std::optional<int> fallback_index;
};

// Builds the schema attr triples into a map.
absl::flat_hash_map<ObjectId, AttrMap> BuildSchemaAttrMap(
    absl::Span<const DictItemTriple> schemas) {
  absl::flat_hash_map<ObjectId, AttrMap> result;
  for (const DictItemTriple& triple : schemas) {
    if (!triple.object.IsSchema()) {
      continue;
    }
    auto [it, _] = result.try_emplace(triple.object, AttrMap());
    it->second.emplace(triple.key, triple.value);
  }
  return result;
}

// Returns the string representation of the schema. The schema is
// recursively expanded if it's nested list or dict schema.
absl::StatusOr<std::string> SchemaToStr(
    const DataItem& schema_item,
    const absl::flat_hash_map<ObjectId, AttrMap>& triple_map,
    int64_t depth = 5);

// Returns the string representation of the schema attribute value. Returns
// empty string if the attribute is not found.
absl::StatusOr<std::string> AttrValueToStr(
    absl::string_view attr, const AttrMap& attr_map,
    const absl::flat_hash_map<ObjectId, AttrMap>& triple_map, int64_t depth) {
  auto it = attr_map.find(DataItem(arolla::Text(attr)));
  if (it == attr_map.end()) {
    return "";
  }
  ASSIGN_OR_RETURN(std::string res,
                   SchemaToStr(it->second, triple_map, depth - 1));
  return res;
}

absl::StatusOr<std::string> SchemaToStr(
    const DataItem& schema_item,
    const absl::flat_hash_map<ObjectId, AttrMap>& triple_map, int64_t depth) {
  if (!schema_item.holds_value<ObjectId>()) {
    return absl::StrCat(schema_item);
  }
  DCHECK_GE(depth, 0);
  if (depth == 0) {
    return "...";
  }
  const ObjectId& schema = schema_item.value<ObjectId>();
  auto it = triple_map.find(schema);
  if (it == triple_map.end()) {
    return absl::InternalError(
        absl::StrCat("schema is not in the triple map: ", schema));
  }
  ASSIGN_OR_RETURN(std::string list_schema_str,
                   AttrValueToStr(schema::kListItemsSchemaAttr, it->second,
                                  triple_map, depth));
  if (!list_schema_str.empty()) {
    return absl::StrCat("list<", list_schema_str, ">");
  }
  ASSIGN_OR_RETURN(std::string key_schema_str,
                   AttrValueToStr(schema::kDictKeysSchemaAttr, it->second,
                                  triple_map, depth));
  ASSIGN_OR_RETURN(std::string value_schema_str,
                   AttrValueToStr(schema::kDictValuesSchemaAttr, it->second,
                                  triple_map, depth));
  if (!key_schema_str.empty() && !value_schema_str.empty()) {
    return absl::StrCat(internal::DataItemRepr(schema_item), "[dict<",
                        key_schema_str, ", ", value_schema_str, ">]");
  }
  return DataItemRepr(schema_item);
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
        absl::StrFormat(
            "%s.%s => %s\n", ObjectIdStr(attr.object), attr.attribute,
            internal::DataItemRepr(attr.value, /*strip_text=*/true)));
  }
  for (const auto& [list_id, values] : main_triples.lists()) {
    absl::StrAppend(
        &res, line_indent,
        absl::StrFormat(
            "%s[:] => [%s]\n", ObjectIdStr(list_id),
            absl::StrJoin(values.begin(), values.end(), ", ",
                          [](std::string* out, const internal::DataItem& item) {
                            out->append(internal::DataItemRepr(item));
                          })));
  }
  for (const DictItemTriple& dict : main_triples.dicts()) {
    if (dict.object.IsDict()) {
      absl::StrAppend(
          &res, line_indent,
          absl::StrFormat("%s[%s] => %s\n", ObjectIdStr(dict.object),
                          internal::DataItemRepr(dict.key),
                          internal::DataItemRepr(dict.value)));
    }
  }
  absl::StrAppend(&res, "\n", line_indent, "SchemaBag:\n");

  absl::flat_hash_map<ObjectId, AttrMap> schema_triple_map =
      BuildSchemaAttrMap(main_triples.dicts());
  for (const DictItemTriple& dict : main_triples.dicts()) {
    if (dict.object.IsSchema() && !IsInternalAttribute(dict.key)) {
      ASSIGN_OR_RETURN(std::string value_str,
                       SchemaToStr(dict.value, schema_triple_map));
      absl::StrAppend(
          &res, line_indent,
          absl::StrFormat("%s.%s => %s\n", ObjectIdStr(dict.object),
                          internal::DataItemRepr(dict.key, /*strip_text=*/true),
                          value_str));
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

template <typename Map>
void UpdateCountMap(const typename Map::key_type& val, Map& count_dict) {
  static_assert(std::is_same<typename Map::mapped_type, int64_t>::value,
                "mapped_type must be int64_t");
  auto [it, inserted] = count_dict.emplace(val, 1);
  if (!inserted) {
    ++it->second;
  }
}

}  // namespace

absl::StatusOr<std::string> DataBagToStr(const DataBagPtr& db) {
  absl::flat_hash_set<const DataBag*> seen_db;
  ASSIGN_OR_RETURN(std::string res,
                   DataBagToStrInternal(db, seen_db, {.indentation = 0}));
  return res;
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
           const std::pair<const ObjectId, std::vector<internal::DataItem>>&
               list) { return acc + list.second.size(); });
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
