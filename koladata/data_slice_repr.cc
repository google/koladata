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
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
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
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

using ::koladata::internal::DataItem;
using ::koladata::internal::DataItemRepr;
using ::koladata::internal::ObjectId;

constexpr absl::string_view kEllipsis = "...";
constexpr absl::string_view kAttrTemplate = "%s=%s";
constexpr absl::string_view kAttrHtmlTemplate =
    "<span class=\"attr %s\">%s</span>=%s";

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
// this can not be merged with ReprOption because some contexts need to set
// object_ids_clickable differently.
//
// In the following discussion an "access path" refers to the sequence of
// operations on a DataSlice object that is required to access something from
// the DataSlice. For example, if we have a list of objects with ObjectIds in
// an `abc` attribute, the access path to the ObjectId in the 2nd item in the
// list would be `[1].abc`.
//
// There are several types of tags:
// 1) Object ids are wrapped in <span class="object-id"> tags. These will
//    have a "clickable" class if object_ids_clickable is true. Object ids
//    should only be clickable in contexts where it is possible to construct
//    the exact access path.
// 2) Access path wrappers wrap sections of the repr string with HTML tags
//    that include metadata on how to access the corresponding data in the
//    DataSlice. For example, <span list-index="1"> indicates that everything
//    in that span belongs to the 2nd item in the list.
// 3) Attribute names are wrapped in <span class="attr"> and have a
//    "clickable" class if the corresponding value is a list. This enables
//    an interaction to explode the list in the interactive repr.
struct WrappingBehavior {
  bool format_html = false;
  // True in contexts where object ids are clickable.
  bool object_ids_clickable = false;

  std::string MaybeWrapObjectId(
      const DataItem& item, std::string item_repr) const {
    if (format_html && item.holds_value<ObjectId>()) {
      return absl::StrFormat(
          "<span class=\"object-id %s\">%s</span>",
          object_ids_clickable ? "clickable" : "", item_repr);
    }
    return item_repr;
  }

  // Annotates an access attribute.
  template <typename T>
  std::string MaybeAnnotateAccess(
      absl::string_view access_type, T access_value, std::string repr) const {
    if (format_html) {
      return absl::StrFormat(
          "<span %s=\"%s\">%s</span>", access_type,
          absl::StrCat(access_value), repr);
    }
    return repr;
  }

  std::string MaybeAnnotateSliceIndex(std::string repr, size_t index) const {
    return MaybeAnnotateAccess("slice-index", index, std::move(repr));
  }
  std::string MaybeAnnotateListIndex(std::string repr, size_t index) const {
    return MaybeAnnotateAccess("list-index", index, std::move(repr));
  }
  std::string MaybeAnnotateSchemaAttr(
      std::string repr, absl::string_view attr_name) const {
    return MaybeAnnotateAccess("schema-attr", attr_name, std::move(repr));
  }
  std::string MaybeAnnotateDictKey(
      std::string repr, absl::string_view key_name) const {
    return MaybeAnnotateAccess("dict-key", key_name, std::move(repr));
  }

  std::string FormatSchemaAttrAndValue(absl::string_view attr,
                                       absl::string_view value_str,
                                       bool is_list) const {
    absl::string_view stripped_attr =
        absl::StripPrefix(absl::StripSuffix(attr, "'"), "'");

    if (format_html) {
      absl::string_view clickable_class = is_list ? "clickable" : "";
      // The inner StrFormat wraps the visible attribute name and the outer
      // MaybeAnnotateSchemaAttr annotates both attr and value with metadata
      // to reconstruct the access path.
      std::string attr_str = absl::StrFormat(
          kAttrHtmlTemplate, clickable_class, stripped_attr, value_str);
      return MaybeAnnotateSchemaAttr(std::move(attr_str), attr);
    } else {
      return absl::StrFormat(kAttrTemplate, stripped_attr, value_str);
    }
  }

  // This must be invoked around all raw repr strings. The assumption of
  // other methods in this class is that their string arguments have already
  // been escaped if necessary.
  std::string MaybeEscape(std::string value) const {
    if (format_html) {
      return EscapeHtml(std::move(value));
    }
    return value;
  }
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

// Returns the string representation for the DataSlice. It requires the
// DataSlice contains only DataItem.
absl::StatusOr<std::string> DataItemToStr(const DataSlice& ds,
                                          const ReprOption& option,
                                          const WrappingBehavior& wrapping);

std::string DataSliceItemRepr(const DataItem& item, const DataItem& schema,
                              const ReprOption& option) {
  WrappingBehavior wrapping{.format_html = option.format_html,
                            .object_ids_clickable = true};
  if (item.holds_value<ObjectId>()) {
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
                        wrapping.MaybeWrapObjectId(
                            item, wrapping.MaybeEscape(DataItemRepr(item))));
  } else {
    bool is_obj_or_any_schema =
        schema == schema::kObject || schema == schema::kAny;
    bool is_mask_schema = schema == schema::kMask;
    return wrapping.MaybeEscape(DataItemRepr(
        item,
        {.show_dtype = is_obj_or_any_schema, .show_missing = is_mask_schema}));
  }
}

// Returns the string representations for the value(s) of the `included_groups`
// in the dimension `dim`. The returned vector has the same size as
// `included_groups`.
//
// TODO: Support max-depth.
std::vector<std::string> StringifyDimension(
    const DataSlice& ds, size_t dim, absl::Span<const int64_t> included_groups,
    const ReprOption& option) {
  const auto& shape = ds.GetShape();
  // We're at the last dimension. Print all the items.
  if (dim >= shape.rank()) {
    std::vector<std::string> result;
    result.reserve(included_groups.size());
    for (int64_t group : included_groups) {
      result.push_back(
          DataSliceItemRepr(ds.slice()[group], ds.GetSchemaImpl(), option));
    }
    return result;
  }
  // We have more dimensions to go: recurse.
  //
  // Create a list of at most `option.item_limit` children per group that are
  // within the `included_groups`.
  const auto& edge = shape.edges()[dim];
  const auto& split_points = edge.edge_values().values.span();
  std::vector<int64_t> next_groups;
  next_groups.reserve(edge.child_size());  // Upper bound.
  for (int64_t group : included_groups) {
    int64_t size = std::min(split_points[group + 1] - split_points[group],
                            static_cast<int64_t>(option.item_limit));
    for (int64_t i = 0; i < size; ++i) {
      next_groups.push_back(split_points[group] + i);
    }
  }
  // Get the string representations of the children.
  auto next_group_reprs = StringifyDimension(ds, dim + 1, next_groups, option);
  // Group the representations of the children within a pair of brackets.
  int group_start = 0;
  std::vector<std::string> group_reprs;
  group_reprs.reserve(included_groups.size());
  WrappingBehavior wrapping{.format_html = option.format_html};
  for (int64_t group : included_groups) {
    int64_t group_size = split_points[group + 1] - split_points[group];
    int64_t size =
        std::min(group_size, static_cast<int64_t>(option.item_limit));
    // A vector is created where each child repr has additional information
    // about the current group context added. An ellipsis is also added to the
    // data if truncation was done.
    std::vector<std::string> current_group_reprs;
    current_group_reprs.reserve(size + 1);
    for (int64_t i = 0; i < size; ++i) {
      current_group_reprs.push_back(wrapping.MaybeAnnotateSliceIndex(
          std::move(next_group_reprs[group_start + i]), i));
    }
    if (group_size > size) {
      current_group_reprs.emplace_back(kEllipsis);
    }
    group_reprs.push_back(
        PrettyFormatStr(current_group_reprs, {.prefix = "[", .suffix = "]"}));
    group_start += size;
  }
  return group_reprs;
}

// Returns the string for python __str__ and part of __repr__.
// The DataSlice must have at least 1 dimension.
std::string DataSliceImplToStr(const DataSlice& ds,
                               const ReprOption& option = ReprOption{}) {
  std::vector<std::string> result =
      StringifyDimension(ds, /*dim=*/0, /*included_groups=*/{0}, option);
  DCHECK_EQ(result.size(), 1);
  return std::move(result[0]);
}

// Returns the string representation of list schema. `schema` must be schema
// type and DataItem. Returns empty string if it doesn't contain list item
// schema attr.
absl::StatusOr<std::string> ListSchemaStr(const DataSlice& schema,
                                          const ReprOption& option) {
  ASSIGN_OR_RETURN(DataSlice attr,
                   schema.GetAttrOrMissing(schema::kListItemsSchemaAttr));
  if (attr.IsEmpty()) {
    return "";
  }
  ASSIGN_OR_RETURN(std::string str, DataItemToStr(
      attr, option, {.format_html = option.format_html}));
  return absl::StrCat("LIST[", str, "]");
}

// Returns the string representation of list schema. `schema` must be schema
// type and DataItem. Returns empty string if it doesn't contain list item
// schema attr.
absl::StatusOr<std::string> DictSchemaStr(const DataSlice& schema,
                                          const ReprOption& option) {
  ASSIGN_OR_RETURN(DataSlice key_attr,
                   schema.GetAttrOrMissing(schema::kDictKeysSchemaAttr));
  ASSIGN_OR_RETURN(DataSlice value_attr,
                   schema.GetAttrOrMissing(schema::kDictValuesSchemaAttr));
  if (key_attr.IsEmpty() || value_attr.IsEmpty()) {
    return "";
  }

  WrappingBehavior wrapping{.format_html = option.format_html};
  ASSIGN_OR_RETURN(std::string key_attr_str,
                   DataItemToStr(key_attr, option, wrapping));
  ASSIGN_OR_RETURN(std::string value_attr_str,
                   DataItemToStr(value_attr, option, wrapping));
  return absl::StrCat("DICT{", key_attr_str, ", ", value_attr_str, "}");
}

// Returns the string representation of list item.
absl::StatusOr<std::string> ListToStr(const DataSlice& ds,
                                      const ReprOption& option) {
  ASSIGN_OR_RETURN(const DataSlice list, ds.ExplodeList(0, std::nullopt));

  auto stringfy_list_items =
      [&option, &list](const internal::DataSliceImpl& list_impl)
      -> absl::StatusOr<std::string> {
    WrappingBehavior wrapping{.format_html = option.format_html,
                              .object_ids_clickable = true};
    std::vector<std::string> elements;
    elements.reserve(list_impl.size());
    size_t item_count = 0;
    for (size_t i = 0; i < list_impl.size(); ++i) {
      const internal::DataItem& item = list_impl[i];
      if (item_count >= option.item_limit) {
        elements.emplace_back(kEllipsis);
        break;
      }
      auto item_schema = list.GetSchema();
      ASSIGN_OR_RETURN(
          DataSlice item_slice,
          DataSlice::Create(item, item_schema.item(), list.GetBag()));
      ASSIGN_OR_RETURN(std::string item_str,
                       DataItemToStr(item_slice, option, wrapping));
      elements.emplace_back(
          wrapping.MaybeAnnotateListIndex(std::move(item_str), i));
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
  WrappingBehavior wrapping{.format_html = option.format_html,
                            .object_ids_clickable = true};
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
    ASSIGN_OR_RETURN(std::string key_str,
                     DataItemToStr(key, option, wrapping));
    ASSIGN_OR_RETURN(std::string value_str,
                     DataItemToStr(value, option, wrapping));
    elements.emplace_back(
        wrapping.MaybeAnnotateDictKey(
            absl::StrCat(key_str, "=", value_str),
            wrapping.MaybeEscape(
                DataItemRepr(key.item(), {.strip_quotes = true}))));
    ++item_count;
  }
  return absl::StrCat("Dict{", absl::StrJoin(elements, ", "), "}");
}

// Returns the string representation of schema item.
absl::StatusOr<std::string> SchemaToStr(const DataSlice& ds,
                                        const ReprOption& option) {
  ASSIGN_OR_RETURN(DataSlice::AttrNamesSet attr_names, ds.GetAttrNames());
  WrappingBehavior wrapping{.format_html = option.format_html,
                            .object_ids_clickable = true};
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
    ASSIGN_OR_RETURN(std::string value_str,
                     DataItemToStr(value, option, wrapping));
    parts.emplace_back(wrapping.FormatSchemaAttrAndValue(
        wrapping.MaybeEscape(attr_name), value_str, value.item().is_list()));
    ++item_count;
  }
  return absl::StrJoin(parts, ", ");
}

absl::StatusOr<std::string> DataItemToStr(const DataSlice& ds,
                                          const ReprOption& option,
                                          const WrappingBehavior& wrapping) {
  // Helper that applies the wrapping to DataItemRepr to be used when
  // the item holds an ObjectId.
  auto repr_with_wrapping = [&wrapping](const DataItem& item) {
    return wrapping.MaybeWrapObjectId(
        item, wrapping.MaybeEscape(DataItemRepr(item)));
  };

  const DataItem& data_item = ds.item();
  DCHECK_GE(option.depth, 0);
  if (option.depth == 0) {
    return repr_with_wrapping(data_item);
  }

  ReprOption next_option = option;
  --next_option.depth;

  const DataItem& schema = ds.GetSchemaImpl();
  if (data_item.holds_value<ObjectId>()) {
    // STRING items inside Lists and Dicts are quoted.
    next_option.strip_quotes = false;
    if (ds.GetBag() == nullptr) {
      return repr_with_wrapping(data_item);
    }

    const ObjectId& obj = data_item.value<ObjectId>();
    if (schema.holds_value<ObjectId>() &&
        schema.value<ObjectId>().IsNoFollowSchema()) {
      return absl::StrCat("Nofollow(Entity:",
                          repr_with_wrapping(data_item), ")");
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
      return absl::StrCat(
          "NOFOLLOW(", wrapping.MaybeEscape(DataItemRepr(original)), ")");
    } else if (obj.IsExplicitSchema()) {
      ASSIGN_OR_RETURN(std::string list_schema_str,
                       ListSchemaStr(ds, next_option));
      if (!list_schema_str.empty()) {
        return list_schema_str;
      }
      ASSIGN_OR_RETURN(std::string dict_schema_str,
                       DictSchemaStr(ds, next_option));
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
      return absl::StrCat(prefix, "):", wrapping.MaybeEscape(
          DataItemRepr(data_item)));
    }
    return absl::StrCat(prefix, schema_str, ")");
  }
  bool is_obj_or_any_schema =
      schema == schema::kObject || schema == schema::kAny;
  bool is_mask_schema = schema == schema::kMask;
  return wrapping.MaybeEscape(
      DataItemRepr(data_item, {.strip_quotes = option.strip_quotes,
                               .show_dtype = is_obj_or_any_schema,
                               .show_missing = is_mask_schema}));
}

}  // namespace

absl::StatusOr<std::string> DataSliceToStr(const DataSlice& ds,
                                           const ReprOption& option) {
  DCHECK_GE(option.depth, 0);
  WrappingBehavior wrapping{.format_html = option.format_html,
                            .object_ids_clickable = true};
  return ds.VisitImpl([&ds, &option, &wrapping]<typename T>(
      const T& impl) {
    return std::is_same_v<T, DataItem>
        ? DataItemToStr(ds, option, wrapping)
        : DataSliceImplToStr(ds, option);
  });
}

}  // namespace koladata
