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
#include <functional>
#include <numeric>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
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
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

using ::koladata::internal::DataItem;
using ::koladata::internal::DataItemRepr;
using ::koladata::internal::ObjectId;

// This is the suffix we expect when DataItemRepr truncates a string. Note
// the additional single quote at the end.
constexpr absl::string_view kTruncationSuffix = "...'";
constexpr absl::string_view kAttrTemplate = "%s=%s";
constexpr absl::string_view kAttrHtmlTemplate =
    "<span class=\"attr\">%s</span>=%s";

struct FormatOptions {
  absl::string_view prefix = "";
  absl::string_view suffix = "";
  bool enable_multiline = true;
  int max_width = 90;
};

// Escape the standard reserved HTML characters. Unfortunately it doesn't seem
// like there is a standard version of this in absl.
std::string EscapeHtml(std::string value) {
  return absl::StrReplaceAll(value, {
      {"&", "&amp;"},
      {"<", "&lt;"},
      {">", "&gt;"},
      {"\"", "&quot;"},
  });
}

// Wraps a DataItem repr string with HTML tags if requested. Although this class
// purely handles HTML, it is named `WrapBehavior` because we may be able to
// generalize to other representations in the future if necessary. Note that
// this can not be merged with ReprOption because it needs to track state.
//
// It is passed by mutable reference everywhere because it must track the number
// of HTML characters added. This allows us to know the number of content
// characters in a string when considering multiline formatting.
//
// In the following discussion an "access path" refers to the sequence of
// operations on a DataSlice object that is required to access something from
// the DataSlice. For example, if we have a list of objects with ObjectIds in
// an `abc` attribute, the access path to the ObjectId in the 2nd item in the
// list would be `[1].abc`.
//
// There are several types of tags:
// 1) Object ids are wrapped in <span class="object-id"> tags.
// 2) Access path wrappers wrap sections of the repr string with HTML tags
//    that include metadata on how to access the corresponding data in the
//    DataSlice. For example, <span list-index="1"> indicates that everything
//    in that span belongs to the 2nd item in the list.
// 3) Attribute names are wrapped in <span class="attr">.
struct WrappingBehavior {
  bool format_html = false;
  // Number of HTML characters added by wrapping with HTML.
  size_t html_char_count = 0;

  std::string MaybeWrapObjectId(const DataItem& item, std::string item_repr) {
    if (format_html && item.holds_value<ObjectId>()) {
      std::string result = absl::StrFormat(
          "<span class=\"object-id\">%s</span>", item_repr);
      UpdateHtmlCharCount(result, item_repr);
      return result;
    }
    return item_repr;
  }

  std::string MaybeAnnotateSliceIndex(std::string repr, size_t index) {
    return MaybeAnnotateAccess("slice-index", index, std::move(repr));
  }
  std::string MaybeAnnotateListIndex(std::string repr, size_t index) {
    return MaybeAnnotateAccess("list-index", index, std::move(repr));
  }
  // The attr_name argument should not be escaped.
  std::string MaybeAnnotateSchemaAttr(
      std::string repr, absl::string_view attr_name) {
    return MaybeAnnotateAccess("schema-attr", attr_name, std::move(repr));
  }
  // This is an access path that attempts to access the key of a dict using
  // the index of the key in the dict's keys DataSlice.
  std::string MaybeAnnotateDictKeyIndex(
      std::string repr, size_t key_index) {
    return MaybeAnnotateAccess("dict-key-index", key_index, std::move(repr));
  }
  // This is an access path that attempts to access the value of a dict using
  // the index of the value in the dict's values DataSlice.
  std::string MaybeAnnotateDictValueIndex(
      std::string repr, size_t key_index) {
    return MaybeAnnotateAccess("dict-value-index",
                               key_index, std::move(repr));
  }

  // A schema access path is a single attribute name with an empty value. These
  // indicate an access path into a SCHEMA DataItem.
  std::string MaybeAnnotateSchemaAccess(
      absl::string_view schema_access, std::string repr) {
    return MaybeAnnotateAccess(schema_access, "", std::move(repr));
  }

  // The attr argument should not be escaped.
  std::string FormatSchemaAttrAndValue(absl::string_view attr,
                                       absl::string_view value_str,
                                       bool is_list) {
    absl::string_view stripped_attr =
        absl::StripPrefix(absl::StripSuffix(attr, "'"), "'");

    if (format_html) {
      // The inner StrFormat wraps the visible attribute name and the outer
      // MaybeAnnotateSchemaAttr annotates both attr and value with metadata
      // to reconstruct the access path.
      std::string escaped_attr = MaybeEscape(std::string(stripped_attr));
      std::string attr_str = absl::StrFormat(
          kAttrHtmlTemplate, escaped_attr, value_str);
      // The base size of the content is the sum of the value, stripped
      // attribute name, and the '=' in the template.
      UpdateHtmlCharCount(attr_str.size(),
                          value_str.size() + escaped_attr.size() + 1);
      return MaybeAnnotateSchemaAttr(std::move(attr_str), stripped_attr);
    } else {
      return absl::StrFormat(kAttrTemplate, stripped_attr, value_str);
    }
  }

  // These are the ellipsis that appear when we run into an item limit.
  std::string_view ItemLimitEllipsis() {
    if (format_html) {
      return "<span class=\"limited\">...</span>";
    }
    return "...";
  }

  // This must be invoked around all raw repr strings. The assumption of
  // other methods in this class is that their string arguments have already
  // been escaped if necessary.
  std::string MaybeEscape(std::string value) {
    if (format_html) {
      // We can't use UpdateHtmlCharCount here because the value is moved.
      size_t initial_value_size = value.size();
      std::string result = EscapeHtml(std::move(value));

      // Wrap truncation suffix in a span to make it interactive. We can not
      // do this before escaping (e.g. by passing a truncation suffix to
      // DataItemRepr) because it would be escaped by the line above.
      if (result.ends_with(kTruncationSuffix)) {
        result = absl::StrCat(
            result.substr(0, result.size() - kTruncationSuffix.length()),
            "<span class=\"truncated\">...</span>'");
      }

      UpdateHtmlCharCount(result.size(), initial_value_size);
      return result;
    }
    return value;
  }

 private:
  // Annotates an access attribute. We escape the access attribute value in
  // because the public MaybeEscape counts the additional characters, but
  // the entire attribute value is not visible content.
  template <typename T>
  std::string MaybeAnnotateAccess(
      absl::string_view access_type, T access_value, std::string repr) {
    if (format_html) {
      std::string result = absl::StrFormat(
          "<span %s=\"%s\">%s</span>", access_type,
          EscapeHtml(absl::StrCat(access_value)), repr);
      UpdateHtmlCharCount(result, repr);
      return result;
    }
    return repr;
  }

  void UpdateHtmlCharCount(
      absl::string_view new_str, absl::string_view old_str) {
    UpdateHtmlCharCount(new_str.size(), old_str.size());
  }

  void UpdateHtmlCharCount(size_t new_size, size_t old_size) {
    html_char_count += new_size - old_size;
  }
};

// Returns the string format of DataSlice content with proper (multiline)
// layout and separators. non_content_char_count is the number of characters
// added by wrappers that will not be visible (e.g. HTML tags).
std::string PrettyFormatStr(const std::vector<std::string>& parts,
                            const FormatOptions& options,
                            size_t non_content_char_count) {
  bool parts_multilined =
      std::find_if(parts.begin(), parts.end(), [](const std::string& str) {
        return absl::StrContains(str, '\n');
      }) != parts.end();
  int total_len = std::accumulate(
      parts.begin(), parts.end(), 0, [](int sum, const std::string& str) {
        return sum + str.size() + 2 /*separator has length 2*/;
      });

  bool use_multiline =
      options.enable_multiline
      && (parts_multilined
          || total_len - non_content_char_count > options.max_width);

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

// Returns the string representation for the DataSlice. It requires the
// DataSlice contains only DataItem.
absl::StatusOr<std::string> DataItemToStr(const DataItem& data_item,
                                          const DataItem& schema,
                                          absl::Nullable<const DataBagPtr>& db,
                                          const ReprOption& option,
                                          WrappingBehavior& wrapping);
// Helper function for calling above function. Requires `item` to hold a
// DataItem.
absl::StatusOr<std::string> DataItemToStr(const DataSlice& item,
                                          const ReprOption& option,
                                          WrappingBehavior& wrapping) {
  DCHECK(item.is_item());
  return DataItemToStr(item.item(), item.GetSchemaImpl(), item.GetBag(),
                       option, wrapping);
}

std::string DataSliceItemRepr(const DataItem& item, const DataItem& schema,
                              const DataBagPtr& bag, const ReprOption& option,
                              WrappingBehavior& wrapping) {
  if (option.show_attributes) {
    ReprOption next_option = option;
    // Show quotes on Text for non item slice.
    next_option.strip_quotes = false;
    if (auto content = DataItemToStr(item, schema, bag, next_option, wrapping);
        content.ok()) {
      return std::move(content.value());
    }
  }
  if (item.holds_value<ObjectId>()) {
    const ObjectId& obj = item.value<ObjectId>();
    if (schema == schema::kItemId) {
      return ObjectIdStr(obj, /*show_flag_prefix=*/true);
    }
    if (obj == ObjectId::NoFollowObjectSchemaId()) {
      return "NOFOLLOW(OBJECT)";
    }
    absl::string_view item_prefix = "";
    if (item.is_dict()) {
      item_prefix = "Dict:";
    } else if (item.is_list()) {
      item_prefix = "List:";
    } else if (schema == schema::kObject) {
      item_prefix = "Obj:";
    } else if (!item.is_schema()) {
      item_prefix = "Entity:";
    }
    return absl::StrCat(item_prefix,
                        wrapping.MaybeWrapObjectId(item, DataItemRepr(item)));
  }
  bool is_obj_schema = schema == schema::kObject;
  bool is_mask_schema = schema == schema::kMask;
  return wrapping.MaybeEscape(DataItemRepr(
      item,
      {.show_dtype = is_obj_schema,
        .show_missing = is_mask_schema,
        .unbounded_type_max_len = option.unbounded_type_max_len}));
}

// Returns the string for python __str__ and part of __repr__.
// The DataSlice must have at least 1 dimension.
std::string DataSliceImplToStr(const DataSlice& ds, const ReprOption& option,
                               WrappingBehavior& wrapping) {
  const auto& shape = ds.GetShape();
  size_t total_item_limit = option.item_limit;
  bool should_enforce_item_limit = ds.size() > total_item_limit;

  // Returns the string representations for the value(s) of the
  // `included_groups` in the dimension `dim`. The returned vector has the same
  // size as `included_groups`.
  std::function<std::vector<std::string>(size_t, absl::Span<const int64_t>)>
      stringify_dimension;
  stringify_dimension = [&](size_t dim,
                            absl::Span<const int64_t> included_groups)
      -> std::vector<std::string> {
    // We're at the last dimension. Print all the items.
    if (dim >= shape.rank()) {
      std::vector<std::string> result;
      result.reserve(included_groups.size());
      for (int64_t group : included_groups) {
        result.push_back(DataSliceItemRepr(ds.slice()[group],
                                           ds.GetSchemaImpl(), ds.GetBag(),
                                           option, wrapping));
      }
      total_item_limit -= result.size();
      DCHECK_GE(total_item_limit, 0);
      return result;
    }

    // We have more dimensions to go: recurse.
    const auto& edge = shape.edges()[dim];
    const auto& split_points = edge.edge_values().values.span();

    // We resize this vector to be empty on each iteration, but reserve enough
    // space for the largest group using the upper bound of edge.child_size().
    std::vector<int64_t> group_indices;
    group_indices.reserve(edge.child_size());

    // We add one to the reserved size to avoid a reallocation in the case that
    // we need to append an ellipsis after returning from an recursive call.
    std::vector<std::string> group_reprs;
    group_reprs.reserve(included_groups.size() + 1);
    for (int64_t group : included_groups) {
      int64_t group_size = split_points[group + 1] - split_points[group];
      size_t size = group_size;
      if (should_enforce_item_limit) {
        size_t item_limit = option.item_limit_per_dimension;
        if (dim + 1 == shape.rank()) {
          item_limit =
              std::min(option.item_limit_per_dimension, total_item_limit);
        }
        size = std::min(size, item_limit);
      }
      size_t initial_html_char_count = wrapping.html_char_count;

      // Get the string representations of the elements. Note that the resize
      // here never reduces capacity and should stay within allocated capacity.
      group_indices.resize(size);
      std::iota(group_indices.begin(), group_indices.end(),
                split_points[group]);
      auto elem_reprs = stringify_dimension(dim + 1, group_indices);

      for (int64_t i = 0; i < size; ++i) {
        elem_reprs[i] =
            wrapping.MaybeAnnotateSliceIndex(std::move(elem_reprs[i]), i);
      }

      // Append ellipsis if we are hiding elements.
      if (group_size > size) {
        elem_reprs.emplace_back(wrapping.ItemLimitEllipsis());
      }

      // Compose final presentation of the group.
      group_reprs.push_back(
          PrettyFormatStr(elem_reprs, {.prefix = "[", .suffix = "]"},
                          wrapping.html_char_count - initial_html_char_count));
    }
    return group_reprs;
  };

  std::vector<std::string> result = stringify_dimension(0, {0});
  DCHECK_EQ(result.size(), 1);
  return std::move(result[0]);
}

// Returns the string representation of list schema. `schema` must be schema
// type and DataItem. Returns empty string if it doesn't contain list item
// schema attr.
absl::StatusOr<std::string> ListSchemaStr(const DataSlice& schema,
                                          const ReprOption& option,
                                          WrappingBehavior& wrapping) {
  ASSIGN_OR_RETURN(DataSlice attr,
                   schema.GetAttrOrMissing(schema::kListItemsSchemaAttr));
  if (attr.IsEmpty()) {
    return "";
  }

  ASSIGN_OR_RETURN(std::string str, DataItemToStr(attr, option, wrapping));
  return absl::StrCat(
      "LIST[", wrapping.MaybeAnnotateSchemaAccess(
          "item-schema", std::move(str)), "]");
}

// Returns the string representation of list schema. `schema` must be schema
// type and DataItem. Returns empty string if it doesn't contain list item
// schema attr.
absl::StatusOr<std::string> DictSchemaStr(const DataSlice& schema,
                                          const ReprOption& option,
                                          WrappingBehavior& wrapping) {
  ASSIGN_OR_RETURN(DataSlice key_attr,
                   schema.GetAttrOrMissing(schema::kDictKeysSchemaAttr));
  ASSIGN_OR_RETURN(DataSlice value_attr,
                   schema.GetAttrOrMissing(schema::kDictValuesSchemaAttr));
  if (key_attr.IsEmpty() || value_attr.IsEmpty()) {
    return "";
  }

  ASSIGN_OR_RETURN(std::string key_attr_str,
                   DataItemToStr(key_attr, option, wrapping));
  ASSIGN_OR_RETURN(std::string value_attr_str,
                   DataItemToStr(value_attr, option, wrapping));
  return absl::StrCat(
      "DICT{",
      wrapping.MaybeAnnotateSchemaAccess("key-schema", std::move(key_attr_str)),
      ", ",
      wrapping.MaybeAnnotateSchemaAccess("value-schema",
                                         std::move(value_attr_str)),
      "}");
}

// Returns the string representation of list item.
absl::StatusOr<std::string> ListToStr(const DataItem& item,
                                      const DataItem& schema,
                                      const DataBagPtr& db,
                                      const ReprOption& option,
                                      WrappingBehavior& wrapping) {
  DCHECK(db != nullptr);
  auto get_list_and_schema =
      [&]() -> absl::StatusOr<std::pair<internal::DataSliceImpl, DataItem>> {
    if (schema.has_value()) {
      ASSIGN_OR_RETURN(auto ds, DataSlice::Create(item, schema, db));
      ASSIGN_OR_RETURN(auto items, ds.ExplodeList(0, std::nullopt));
      auto item_schema = items.GetSchemaImpl();
      return {{std::move(items).slice(), std::move(item_schema)}};
    } else {
      FlattenFallbackFinder fb_finder(*db);
      ASSIGN_OR_RETURN(
          auto list,
          db->GetImpl().ExplodeList(
              item, internal::DataBagImpl::ListRange(0, std::nullopt),
              fb_finder.GetFlattenFallbacks()));
      return {{std::move(list), internal::DataItem()}};
    }
  };

  auto stringfy_list_items = [&]() -> absl::StatusOr<std::string> {
    size_t initial_html_char_count = wrapping.html_char_count;

    ASSIGN_OR_RETURN((auto [list_impl, item_schema]), get_list_and_schema());
    std::vector<std::string> elements;
    elements.reserve(list_impl.size());
    size_t item_count = 0;
    for (size_t i = 0; i < list_impl.size(); ++i) {
      const internal::DataItem& item = list_impl[i];
      if (item_count >= option.item_limit) {
        elements.emplace_back(wrapping.ItemLimitEllipsis());
        break;
      }
      ASSIGN_OR_RETURN(std::string item_str,
                       DataItemToStr(item, item_schema, db, option, wrapping));
      elements.emplace_back(
          wrapping.MaybeAnnotateListIndex(std::move(item_str), i));
      ++item_count;
    }
    return PrettyFormatStr(
        elements, {.prefix = "[", .suffix = "]"},
        wrapping.html_char_count - initial_html_char_count);
  };
  ASSIGN_OR_RETURN(const std::string str, stringfy_list_items());
  return absl::StrCat("List", str);
}

// Returns the string representation of dict item.
absl::StatusOr<std::string> DictToStr(const DataItem& item,
                                      const DataItem& schema,
                                      const DataBagPtr& db,
                                      const ReprOption& option,
                                      WrappingBehavior& wrapping) {
  DCHECK(db != nullptr);
  auto get_keys_values_and_schemas =
      [&]() -> absl::StatusOr<
                std::tuple<internal::DataSliceImpl, internal::DataSliceImpl,
                           DataItem, DataItem>> {
    if (schema.has_value()) {
      ASSIGN_OR_RETURN(auto ds, DataSlice::Create(item, schema, db));
      ASSIGN_OR_RETURN(auto keys, ds.GetDictKeys());
      ASSIGN_OR_RETURN(auto values, ds.GetDictValues());
      auto key_schema = keys.GetSchemaImpl();
      auto value_schema = values.GetSchemaImpl();
      return {{std::move(keys).slice(), std::move(values).slice(),
               std::move(key_schema), std::move(value_schema)}};
    } else {
      const auto& db_impl = db->GetImpl();
      FlattenFallbackFinder fb_finder(*db);
      auto fallbacks = fb_finder.GetFlattenFallbacks();
      ASSIGN_OR_RETURN((auto [keys, _]), db_impl.GetDictKeys(item, fallbacks));
      ASSIGN_OR_RETURN((auto [values, _unused]),
                       db_impl.GetDictValues(item, fallbacks));
      return {{std::move(keys), std::move(values), internal::DataItem(),
               internal::DataItem()}};
    }
  };
  ASSIGN_OR_RETURN((auto [keys_impl, values_impl, key_schema, value_schema]),
                   get_keys_values_and_schemas());
  std::vector<std::string> elements;
  elements.reserve(keys_impl.size());
  size_t initial_html_char_count = wrapping.html_char_count;
  size_t item_count = 0;

  ReprOption key_option = option;
  key_option.depth = 0;

  for (size_t i = 0; i < keys_impl.size(); ++i) {
    if (item_count >= option.item_limit) {
      elements.emplace_back(wrapping.ItemLimitEllipsis());
      break;
    }
    const DataItem& key = keys_impl[i];
    const DataItem& value = values_impl[i];
    ASSIGN_OR_RETURN(std::string key_str,
                     DataItemToStr(key, key_schema, db, key_option, wrapping));
    ASSIGN_OR_RETURN(std::string value_str,
                     DataItemToStr(value, value_schema, db, option, wrapping));

    elements.emplace_back(absl::StrCat(
        wrapping.MaybeAnnotateDictKeyIndex(std::move(key_str), i),
        "=",
        wrapping.MaybeAnnotateDictValueIndex(
            std::move(value_str), i)));
    ++item_count;
  }

  return PrettyFormatStr(
      elements, {.prefix = "Dict{", .suffix = "}"},
      wrapping.html_char_count - initial_html_char_count);
}

// Returns the string representation of schema items or objects.
absl::StatusOr<std::vector<std::string>> AttrsToStrParts(
    const DataItem& item,
    const DataItem& schema,
    const DataBagPtr& db,
    const ReprOption& option,
    WrappingBehavior& wrapping) {
  if (!schema.has_value()) {
    return std::vector<std::string>();
  }
  ASSIGN_OR_RETURN(DataSlice ds, DataSlice::Create(item, schema, db));
  ASSIGN_OR_RETURN(DataSlice::AttrNamesSet attr_names, ds.GetAttrNames());
  std::vector<std::string> parts;
  parts.reserve(attr_names.size());
  size_t item_count = 0;
  for (const std::string& attr_name : attr_names) {
    // Keeps all attributes from schema.
    if (!ds.item().is_schema() && item_count >= option.item_limit) {
      parts.emplace_back(wrapping.ItemLimitEllipsis());
      break;
    }
    ASSIGN_OR_RETURN(DataSlice value, ds.GetAttr(attr_name));
    ASSIGN_OR_RETURN(std::string value_str,
                     DataItemToStr(value.item(), value.GetSchemaImpl(), db,
                                   option, wrapping));
    parts.emplace_back(wrapping.FormatSchemaAttrAndValue(
        attr_name, value_str, value.item().is_list()));
    ++item_count;
  }

  return parts;
}

// Returns the schema name from __schema_name__ attribute. The schema must hold
// an ObjectId with schema flag and bag must not be null.
std::string GetSchemaNameOrEmpty(const DataBagPtr& bag,
                                 const DataItem& schema) {
  DCHECK(bag != nullptr);
  DCHECK(schema.holds_value<ObjectId>());
  const internal::DataBagImpl & bag_impl = bag->GetImpl();
  FlattenFallbackFinder finder(*bag);
  ASSIGN_OR_RETURN(
      DataItem schema_name,
      bag_impl.GetSchemaAttrAllowMissing(schema, schema::kSchemaNameAttr,
                                         finder.GetFlattenFallbacks()),
      [](const absl::Status& status) { return ""; }(_));
  if (!schema_name.has_value()) {
    return "";
  }
  return std::string(schema_name.value<arolla::Text>().view());
}

absl::StatusOr<std::string> DataItemToStr(const DataItem& data_item,
                                          const DataItem& schema,
                                          absl::Nullable<const DataBagPtr>& db,
                                          const ReprOption& option,
                                          WrappingBehavior& wrapping) {
  // Helper that applies the wrapping to DataItemRepr to be used when
  // the item holds an ObjectId.
  auto repr_with_wrapping = [&option, &wrapping](const DataItem& item) {
    return wrapping.MaybeWrapObjectId(
        item, wrapping.MaybeEscape(
            DataItemRepr(item, {
              .unbounded_type_max_len = option.unbounded_type_max_len})));
  };

  if (!data_item.has_value()) {
    return DataItemRepr(data_item, {.show_missing = (schema == schema::kMask)});
  }

  DCHECK_GE(option.depth, 0);
  if (option.depth == 0) {
    return repr_with_wrapping(data_item);
  }

  ReprOption next_option = option;
  --next_option.depth;

  // When DataSlice holds a schema DataItem.
  if (schema == schema::kSchema ||
      (data_item.is_schema() && !schema.has_value())) {
    if (data_item.holds_value<schema::DType>()) {
      return absl::StrCat(data_item.value<schema::DType>());
    }
    DCHECK(data_item.holds_value<ObjectId>());
    const ObjectId& obj = data_item.value<ObjectId>();
    if (db == nullptr) {
      return ObjectIdStr(obj);
    }
    if (obj.IsNoFollowSchema()) {
      if (obj == ObjectId::NoFollowObjectSchemaId()) {
        return "NOFOLLOW(OBJECT)";
      }
      const ObjectId original = internal::GetOriginalFromNoFollow(obj);
      return absl::StrCat("NOFOLLOW(", ObjectIdStr(original), ")");
    }
    ASSIGN_OR_RETURN(
        auto ds,
        DataSlice::Create(data_item, internal::DataItem(schema::kSchema), db));
    if (ds.IsListSchema()) {
      return ListSchemaStr(ds, next_option, wrapping);
    }
    if (ds.IsDictSchema()) {
      return DictSchemaStr(ds, next_option, wrapping);
    }
    std::string prefix = "";
    std::string schema_name = GetSchemaNameOrEmpty(db, data_item);
    if (!schema_name.empty()) {
      absl::StrAppend(&prefix, schema_name, "(");
    } else if (obj.IsExplicitSchema()) {
      absl::StrAppend(&prefix, "SCHEMA(");
    } else if (obj.IsImplicitSchema()) {
      absl::StrAppend(&prefix, "IMPLICIT_SCHEMA(");
    }
    size_t initial_html_char_count = wrapping.html_char_count;
    ASSIGN_OR_RETURN(
        std::vector<std::string> schema_parts,
        AttrsToStrParts(data_item, internal::DataItem(schema::kSchema), db,
                        next_option, wrapping));
    return PrettyFormatStr(
      schema_parts, {.prefix = prefix, .suffix = ")"},
      wrapping.html_char_count - initial_html_char_count);
  }

  // Handle ITEMID schema explicitly. We should not show any additional
  // detail about the ObjectId in this case.
  if (schema.is_itemid_schema()) {
    DCHECK(data_item.holds_value<ObjectId>());
    return ObjectIdStr(data_item.value<ObjectId>(),
                                  /*show_flag_prefix=*/true);
  }

  if (data_item.holds_value<ObjectId>()) {
    // STRING items inside Lists and Dicts are quoted.
    next_option.strip_quotes = false;
    if (db == nullptr) {
      return repr_with_wrapping(data_item);
    }

    const ObjectId& obj = data_item.value<ObjectId>();
    if (schema.holds_value<ObjectId>() &&
        schema.value<ObjectId>().IsNoFollowSchema()) {
      return absl::StrCat("Nofollow(",
                          ObjectIdStr(obj, /*show_flag_prefix=*/true), ")");
    }
    if (obj.IsList()) {
      return ListToStr(data_item, schema, db, next_option, wrapping);
    }
    if (obj.IsDict()) {
      return DictToStr(data_item, schema, db, next_option, wrapping);
    }

    absl::string_view prefix = (schema == schema::kObject) ? "Obj(" : "Entity(";
    size_t initial_html_char_count = wrapping.html_char_count;
    ASSIGN_OR_RETURN(
        std::vector<std::string> attr_parts,
        AttrsToStrParts(data_item, schema, db, next_option, wrapping));
    // Append ItemId if there is no attribute
    if (attr_parts.empty()) {
      return absl::StrCat(prefix, "):", ObjectIdStr(obj));
    }
    return PrettyFormatStr(attr_parts, {.prefix = prefix, .suffix = ")"},
                           wrapping.html_char_count - initial_html_char_count);
  }
  bool is_obj_schema = schema == schema::kObject;
  return wrapping.MaybeEscape(
      DataItemRepr(data_item, {
        .strip_quotes = option.strip_quotes,
        .show_dtype = is_obj_schema,
        .unbounded_type_max_len = option.unbounded_type_max_len}));
}

}  // namespace

absl::StatusOr<std::string> DataItemToStr(const internal::DataItem& data_item,
                                          const internal::DataItem& schema,
                                          absl::Nullable<const DataBagPtr>& db,
                                          const ReprOption& option) {
  DCHECK_GE(option.depth, 0);
  WrappingBehavior wrapping{.format_html = option.format_html};
  return DataItemToStr(data_item, schema, db, option, wrapping);
}

absl::StatusOr<std::string> DataSliceToStr(const DataSlice& ds,
                                           const ReprOption& option) {
  DCHECK_GE(option.depth, 0);
  WrappingBehavior wrapping{.format_html = option.format_html};
  return ds.VisitImpl([&ds, &option, &wrapping]<typename T>(
      const T& impl) {
    return std::is_same_v<T, DataItem>
        ? DataItemToStr(ds, option, wrapping)
        : DataSliceImplToStr(ds, option, wrapping);
  });
}

// Returns the string representation of the attribute names of the DataSlice.
// Returns empty string if failed to get the attribute names.
std::string AttrNamesOrEmpty(const DataSlice& ds) {
  if (auto attr_names = ds.GetAttrNames(/*union_object_attrs=*/true);
      attr_names.ok()) {
    return absl::StrCat("[", absl::StrJoin(*attr_names, ", "), "]");
  }
  return "";
}

std::string DataSliceRepr(const DataSlice& ds, const ReprOption& option) {
  std::string result;
  absl::StrAppend(&result, ds.is_item() ? "DataItem(" : "DataSlice(");
  bool only_print_attr_names =
      ds.size() >= option.item_limit && option.show_attributes && ds.IsEntity();
  // If the data slice is too large, we will not print the
  // whole data slice.
  if (only_print_attr_names) {
    absl::StrAppend(&result, "attrs: ", AttrNamesOrEmpty(ds));
  } else if (auto content = DataSliceToStr(ds, option); content.ok()) {
    absl::StrAppend(&result, *content);
  } else {
    ds.VisitImpl(
        [&](const auto& impl) { return absl::StrAppend(&result, impl); });
  }
  absl::StrAppend(&result, ", schema: ");
  if (auto schema = DataSliceToStr(ds.GetSchema(), option); schema.ok()) {
    absl::StrAppend(&result, *schema);
  } else {
    absl::StrAppend(&result, ds.GetSchemaImpl());
  }
  if (!ds.is_item()) {
    if (option.show_shape) {
      absl::StrAppend(&result, ", shape: ", arolla::Repr(ds.GetShape()));
    } else {
      absl::StrAppend(&result, ", ndims: ", ds.GetShape().rank(),
                      ", size: ", ds.GetShape().size());
    }
  }
  if (option.show_databag_id && ds.GetBag() != nullptr) {
    absl::StrAppend(&result, ", bag_id: ", GetBagIdRepr(ds.GetBag()));
  }
  absl::StrAppend(&result, ")");
  return result;
}

}  // namespace koladata
