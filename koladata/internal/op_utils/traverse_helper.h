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
#ifndef KOLADATA_INTERNAL_OP_UTILS_TRAVERSE_HELPER_H_
#define KOLADATA_INTERNAL_OP_UTILS_TRAVERSE_HELPER_H_

#include <sys/types.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "arolla/util/view_types.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

// A helper class for traversing a databag.
//
// It is used to hide the details of databag accessing to get attributes and
// schemas for different types of items.
class TraverseHelper {
 public:
  enum class TransitionType {
    kListItem,
    kDictKey,
    kDictValue,
    kAttributeName,
    kSchemaAttributeName,
    kObjectSchema,
    kSchema,
    kSliceItem,
  };

  struct Transition {
    DataItem item;
    DataItem schema;
  };

  // Information needed to identify a single transition.
  struct TransitionKey {
    TransitionType type;
    // Index of the list item, or index in TransitionSet for dict keys/values.
    int64_t index = -1;
    // Attribute name of dict key for dict keys/values.
    DataItem value = DataItem();
  };

  static std::string TransitionKeyToAccessString(
      const TransitionKey& key,
      absl::string_view ignore_attr_name_prefix = "") {
    if (key.type == TransitionType::kListItem) {
      return absl::StrFormat("[%d]", key.index);
    } else if (key.type == TransitionType::kDictKey) {
      return absl::StrFormat(".get_keys().S[%d]", key.index);
    } else if (key.type == TransitionType::kDictValue) {
      return absl::StrFormat("[%s]", DataItemRepr(key.value));
    } else if (key.type == TransitionType::kSchemaAttributeName) {
      auto attr_name = key.value.value<arolla::Text>().view();
      if (!ignore_attr_name_prefix.empty() &&
          attr_name.starts_with(ignore_attr_name_prefix)) {
        attr_name = attr_name.substr(ignore_attr_name_prefix.size());
      }
      if (attr_name == schema::kSchemaNameAttr) {
        return absl::StrFormat(".get_attr(%s)", schema::kSchemaNameAttr);
      } else if (attr_name == schema::kSchemaMetadataAttr) {
        return absl::StrFormat(".get_attr(%s)", schema::kSchemaMetadataAttr);
      } else if (attr_name == schema::kListItemsSchemaAttr) {
        return ".get_item_schema()";
      } else if (attr_name == schema::kDictKeysSchemaAttr) {
        return ".get_key_schema()";
      } else if (attr_name == schema::kDictValuesSchemaAttr) {
        return ".get_value_schema()";
      } else {
        return absl::StrFormat(".%s", attr_name);
      }
    } else if (key.type == TransitionType::kAttributeName) {
      auto attr_name = key.value.value<arolla::Text>().view();
      if (!ignore_attr_name_prefix.empty() &&
          attr_name.starts_with(ignore_attr_name_prefix)) {
        attr_name = attr_name.substr(ignore_attr_name_prefix.size());
      }
      if (attr_name == schema::kSchemaAttr) {
        return ".get_obj_schema()";
      } else {
        return absl::StrFormat(".%s", attr_name);
      }
    } else if (key.type == TransitionType::kObjectSchema) {
      return ".get_obj_schema()";
    } else if (key.type == TransitionType::kSchema) {
      return ".get_schema()";
    } else if (key.type == TransitionType::kSliceItem) {
      if (key.index == -1) {
        return "";
      }
      return absl::StrFormat(".S[%d]", key.index);
    }
    ABSL_UNREACHABLE();
  }

  static std::string TransitionKeySequenceToAccessPath(
      absl::Span<const TransitionKey> seq,
      absl::string_view ignore_attr_name_prefix = "") {
    std::string result;
    for (const auto& key : seq) {
      absl::StrAppend(
          &result, TransitionKeyToAccessString(key, ignore_attr_name_prefix));
    }
    return result;
  }

  // Returns a copy of the sequence of transition keys, where attribute names
  // starting with the given prefix have this prefix removed.
  static std::vector<TransitionKey> WithIgnoringPrefix(
      absl::Span<const TransitionKey> seq,
      absl::string_view ignore_attr_name_prefix) {
    std::vector<TransitionKey> result;
    for (const auto& key : seq) {
      if (key.type == TransitionType::kSchemaAttributeName ||
          key.type == TransitionType::kAttributeName) {
        auto attr_name = key.value.value<arolla::Text>().view();
        if (!ignore_attr_name_prefix.empty() &&
            attr_name.starts_with(ignore_attr_name_prefix)) {
          attr_name = attr_name.substr(ignore_attr_name_prefix.size());
        }
        result.push_back(
            {.type = key.type,
             .index = key.index,
             .value = DataItem(arolla::Text(std::move(attr_name)))});
      } else {
        result.push_back(key);
      }
    }
    return result;
  }

  // A view of a set of transitions for a given item with a given schema.
  //
  // This class is used to make a clear dispatch logic for traversing
  // different types of items. And to avoid repeating databag accesses.
  class TransitionsSet {
   public:
    static TransitionsSet CreateForList(DataItem list_item_schema,
                                        DataSliceImpl list_items) {
      return TransitionsSet(
          /*keys_or_names=*/std::nullopt,
          /*values=*/std::move(list_items),
          /*list_item_schema=*/std::move(list_item_schema),
          /*dict_keys_schema=*/std::nullopt,
          /*dict_values_schema=*/std::nullopt,
          /*is_schema=*/false);
    }

    static TransitionsSet CreateForDict(DataItem dict_keys_schema,
                                        DataItem dict_values_schema,
                                        DataSliceImpl dict_keys,
                                        DataSliceImpl dict_values) {
      return TransitionsSet(
          /*keys_or_names=*/std::move(dict_keys),
          /*values=*/std::move(dict_values),
          /*list_item_schema=*/std::nullopt,
          /*dict_keys_schema=*/std::move(dict_keys_schema),
          /*dict_values_schema=*/std::move(dict_values_schema),
          /*is_schema=*/false);
    }

    static TransitionsSet CreateForEntity(DataSliceImpl attr_names) {
      return TransitionsSet(
          /*keys_or_names=*/std::move(attr_names),
          /*values=*/std::nullopt,
          /*list_item_schema=*/std::nullopt,
          /*dict_keys_schema=*/std::nullopt,
          /*dict_values_schema=*/std::nullopt,
          /*is_schema=*/false);
    }

    static TransitionsSet CreateForSchema(DataSliceImpl attr_names) {
      return TransitionsSet(
          /*keys_or_names=*/std::move(attr_names),
          /*values=*/std::nullopt,
          /*list_item_schema=*/std::nullopt,
          /*dict_keys_schema=*/std::nullopt,
          /*dict_values_schema=*/std::nullopt,
          /*is_schema=*/true);
    }

    static TransitionsSet CreateForObject() {
      return TransitionsSet(
          /*keys_or_names=*/std::nullopt,
          /*values=*/std::nullopt,
          /*list_item_schema=*/std::nullopt,
          /*dict_keys_schema=*/std::nullopt,
          /*dict_values_schema=*/std::nullopt,
          /*is_schema=*/false);
    }

    static TransitionsSet CreateEmpty() {
      return TransitionsSet(
          /*keys_or_names=*/std::nullopt,
          /*values=*/std::nullopt,
          /*list_item_schema=*/std::nullopt,
          /*dict_keys_schema=*/std::nullopt,
          /*dict_values_schema=*/std::nullopt,
          /*is_schema=*/false);
    }

    bool is_list() const { return list_item_schema_.has_value(); }
    bool is_dict() const { return dict_keys_schema_.has_value(); }
    bool is_schema() const { return is_schema_; }
    bool has_attrs() const { return keys_or_names_.has_value(); }

    const DataItem& dict_keys_schema() const {
      DCHECK(dict_keys_schema_.has_value());
      return *dict_keys_schema_;
    }

    const DataItem& dict_values_schema() const {
      DCHECK(dict_values_schema_.has_value());
      return *dict_values_schema_;
    }

    const DataItem& list_item_schema() const {
      DCHECK(list_item_schema_.has_value());
      return *list_item_schema_;
    }

    const DataSliceImpl& dict_keys() const {
      DCHECK(dict_keys_schema_.has_value());
      return *keys_or_names_;
    }

    const DataSliceImpl& dict_values() const {
      DCHECK(dict_values_schema_.has_value());
      return *values_;
    }

    const DataSliceImpl& list_items() const {
      DCHECK(list_item_schema_.has_value());
      return *values_;
    }

    const arolla::DenseArray<arolla::Text>& attr_names() const {
      DCHECK(keys_or_names_.has_value());
      DCHECK(!keys_or_names_->is_empty_and_unknown());
      return keys_or_names_->values<arolla::Text>();
    }

    // Converts the transitions set to a vector of TransitionKeys.
    std::vector<TransitionKey> GetTransitionKeys() const {
      std::vector<TransitionKey> transition_keys;
      if (is_list()) {
        for (int64_t i = 0; i < list_items().size(); ++i) {
          transition_keys.push_back(
              {.type = TransitionType::kListItem, .index = i});
        }
      } else if (is_dict()) {
        for (int64_t i = 0; i < dict_keys().size(); ++i) {
          transition_keys.push_back({.type = TransitionType::kDictKey,
                                     .index = i,
                                     .value = dict_keys()[i]});
          transition_keys.push_back({.type = TransitionType::kDictValue,
                                     .index = i,
                                     .value = dict_keys()[i]});
        }
      } else if (has_attrs()) {
        TransitionType type = is_schema() ? TransitionType::kSchemaAttributeName
                                          : TransitionType::kAttributeName;
        attr_names().ForEach(
            [&](int64_t i, bool presence, std::string_view attr_name) {
              DCHECK(presence);
              transition_keys.push_back(
                  {.type = type, .value = DataItem(arolla::Text(attr_name))});
            });
      }
      return transition_keys;
    }

   private:
    TransitionsSet(std::optional<DataSliceImpl> keys_or_names,
                   std::optional<DataSliceImpl> values,
                   std::optional<DataItem> list_item_schema,
                   std::optional<DataItem> dict_keys_schema,
                   std::optional<DataItem> dict_values_schema,
                   bool is_schema)
        : keys_or_names_(std::move(keys_or_names)),
          values_(std::move(values)),
          list_item_schema_(std::move(list_item_schema)),
          dict_keys_schema_(std::move(dict_keys_schema)),
          dict_values_schema_(std::move(dict_values_schema)),
          is_schema_(is_schema) {}

    std::optional<DataSliceImpl> keys_or_names_;
    std::optional<DataSliceImpl> values_;
    std::optional<DataItem> list_item_schema_;
    std::optional<DataItem> dict_keys_schema_;
    std::optional<DataItem> dict_values_schema_;
    bool is_schema_;
  };

  TraverseHelper(const DataBagImpl& databag,
                 DataBagImpl::FallbackSpan fallbacks)
      : databag_(databag), fallbacks_(fallbacks) {}

  // Returns view of all transitions for the given item and schema.
  absl::StatusOr<TransitionsSet> GetTransitions(
      DataItem item, DataItem schema, bool remove_special_attrs = true) {
    if (schema.holds_value<ObjectId>()) {
      // Entity schema.
      return EntityTransitions(item, schema, remove_special_attrs);
    } else if (schema.holds_value<schema::DType>()) {
      if (schema == schema::kObject) {
        // Object schema.
        return TransitionsSet::CreateForObject();
      } else if (schema == schema::kSchema) {
        return SchemaTransitions(item);
      }
      // Primitive types and ItemId have no transitions.
      return TransitionsSet::CreateEmpty();
    }
    return absl::InternalError("unsupported schema type");
  }

  // Calls the given function for each directly reachable object.
  template <typename Fn>
  absl::Status ForEachObject(const DataItem& item, const DataItem& schema,
                     const TransitionsSet& transitions_set, Fn&& fn) {
    static_assert(std::is_same_v<decltype(fn(arolla::view_type_t<DataItem>{},
                                             arolla::view_type_t<DataItem>(),
                                             absl::string_view{})),
                                 void>,
                  "Callback shouldn't return value");
    if (schema == schema::kObject) {
      ASSIGN_OR_RETURN(DataItem object_schema, GetObjectSchema(item));
      if (object_schema.holds_value<ObjectId>()) {
        fn(item, object_schema, std::nullopt);
      }
    } else if (transitions_set.is_list()) {
      ForEachListItemObject(transitions_set, fn);
    } else if (transitions_set.is_dict()) {
      ForEachDictElementObject(transitions_set, fn);
    } else if (schema == schema::kSchema && transitions_set.has_attrs()) {
      return ForEachSchemaAttributeObject(item, transitions_set, fn);
    } else if (transitions_set.has_attrs()) {
      return ForEachAttributeObject(item, schema, transitions_set, fn);
    }
    return absl::OkStatus();
  }

  // Returns transition for the given key.
  absl::StatusOr<Transition> TransitionByKey(
      const DataItem& item, const DataItem& schema,
      const TransitionsSet& transitions_set, const TransitionKey& key) {
    if (key.type == TransitionType::kListItem) {
      return Transition(
          {.item = transitions_set.list_items()[key.index],
           .schema = transitions_set.list_item_schema()});
    } else if (key.type == TransitionType::kDictKey) {
      DCHECK_EQ(transitions_set.dict_keys()[key.index], key.value);
      return Transition(
          {.item = key.value, .schema = transitions_set.dict_keys_schema()});
    } else if (key.type == TransitionType::kDictValue) {
      DCHECK_EQ(transitions_set.dict_keys()[key.index], key.value);
      return Transition({.item = transitions_set.dict_values()[key.index],
                         .schema = transitions_set.dict_values_schema()});
    } else if (key.type == TransitionType::kAttributeName) {
      return AttributeTransition(item, schema, key.value.value<arolla::Text>());
    } else if (key.type == TransitionType::kSchemaAttributeName) {
      return SchemaAttributeTransition(item, key.value.value<arolla::Text>());
    } else if (key.type == TransitionType::kObjectSchema) {
      ASSIGN_OR_RETURN(auto object_schema, GetObjectSchema(item));
      return Transition({.item = std::move(object_schema),
                         .schema = DataItem(schema::kSchema)});
    } else if (key.type == TransitionType::kSchema) {
      return Transition({.item = schema, .schema = DataItem(schema::kSchema)});
    } else {
      return absl::InternalError("unsupported transition key type");
    }
  }

  // Returns transition for the given attribute.
  absl::StatusOr<Transition> AttributeTransition(const DataItem& item,
                                                 const DataItem& schema,
                                                 std::string_view attr_name) {
    ASSIGN_OR_RETURN(DataItem attr_value,
                     databag_.GetAttr(item, attr_name, fallbacks_));
    ASSIGN_OR_RETURN(DataItem attr_schema,
                     databag_.GetSchemaAttr(schema, attr_name, fallbacks_));
    return Transition(
        {.item = std::move(attr_value), .schema = std::move(attr_schema)});
  }

  // Returns transition for the given schema attribute.
  absl::StatusOr<Transition> SchemaAttributeTransition(
      const DataItem& schema, std::string_view attr_name) {
    ASSIGN_OR_RETURN(DataItem attr_schema,
                     databag_.GetSchemaAttr(schema, attr_name, fallbacks_));
    schema::DType schema_dtype = schema::kSchema;
    if (attr_name == schema::kSchemaNameAttr) {
      schema_dtype = schema::kString;
    } else if (attr_name == schema::kSchemaMetadataAttr) {
      schema_dtype = schema::kObject;
    }
    return Transition(
        {.item = std::move(attr_schema), .schema = DataItem(schema_dtype)});
  }

  // Returns the schema for the given object.
  absl::StatusOr<DataItem> GetObjectSchema(const DataItem& item) {
    return databag_.GetObjSchemaAttr(item, fallbacks_);
  }

 private:
  template <typename Fn>
  void ForEachListItemObject(const TransitionsSet& transitions_set, Fn&& fn) {
    const DataSliceImpl& list_items = transitions_set.list_items();
    auto item_schema = transitions_set.list_item_schema();
    if (item_schema.is_primitive_schema()) {
      return;
    }
    for (int64_t i = 0; i < list_items.size(); ++i) {
      const DataItem& item = list_items[i];
      if (item.holds_value<ObjectId>()) {
        fn(item, item_schema, schema::kListItemsSchemaAttr);
      }
    }
  }

  template <typename Fn>
  void ForEachDictElementObject(const TransitionsSet& transitions_set,
                                Fn&& fn) {
    if (!transitions_set.dict_keys_schema().is_primitive_schema()) {
      const DataSliceImpl& dict_keys = transitions_set.dict_keys();
      for (int64_t i = 0; i < dict_keys.size(); ++i) {
        const DataItem& key = dict_keys[i];
        if (key.holds_value<ObjectId>()) {
          fn(key, transitions_set.dict_keys_schema(),
             schema::kDictKeysSchemaAttr);
        }
      }
    }
    if (!transitions_set.dict_values_schema().is_primitive_schema()) {
      const DataSliceImpl& dict_values = transitions_set.dict_values();
      for (int64_t i = 0; i < dict_values.size(); ++i) {
        const DataItem& value = dict_values[i];
        if (value.holds_value<ObjectId>()) {
          fn(value, transitions_set.dict_values_schema(),
             schema::kDictValuesSchemaAttr);
        }
      }
    }
  }

  template <typename Fn>
  absl::Status ForEachSchemaAttributeObject(
      const DataItem& item, const TransitionsSet& transitions_set, Fn&& fn) {
    absl::Status status = absl::OkStatus();
    transitions_set.attr_names().ForEach(
        [&](int64_t i, bool presence, std::string_view attr_name) {
          DCHECK(presence);
          auto transition_or = SchemaAttributeTransition(item, attr_name);
          if (!transition_or.ok()) {
            status = transition_or.status();
            return;
          }
          if (transition_or->item.holds_value<ObjectId>()) {
            fn(transition_or->item, transition_or->schema, std::nullopt);
          }
        });
    return status;
  }

  template <typename Fn>
  absl::Status ForEachAttributeObject(const DataItem& item,
                                      const DataItem& schema,
                                      const TransitionsSet& transitions_set,
                                      Fn&& fn) {
    absl::Status status = absl::OkStatus();
    transitions_set.attr_names().ForEach(
        [&](int64_t i, bool presence, std::string_view attr_name) {
          DCHECK(presence);
          if (attr_name == schema::kSchemaNameAttr ||
              attr_name == schema::kSchemaMetadataAttr) {
            return;
          }
          auto transition_or = AttributeTransition(item, schema, attr_name);
          if (!transition_or.ok()) {
            status = transition_or.status();
            return;
          }
          if (transition_or->item.holds_value<ObjectId>()) {
            fn(transition_or->item, transition_or->schema, attr_name);
          }
        });
    return status;
  }

  absl::StatusOr<TransitionsSet> ListTransitions(const DataItem& item,
                                                 const DataItem& schema) {
    ASSIGN_OR_RETURN(DataItem list_item_schema,
                     databag_.GetSchemaAttr(
                         schema, schema::kListItemsSchemaAttr, fallbacks_));
    ASSIGN_OR_RETURN(
        DataSliceImpl list_items,
        databag_.ExplodeList(item, DataBagImpl::ListRange(), fallbacks_));
    return TransitionsSet::CreateForList(std::move(list_item_schema),
                                         std::move(list_items));
  }

  absl::StatusOr<TransitionsSet> DictTransitions(const DataItem& item,
                                                 const DataItem& schema) {
    ASSIGN_OR_RETURN(DataItem dict_keys_schema,
                     databag_.GetSchemaAttr(schema, schema::kDictKeysSchemaAttr,
                                            fallbacks_));
    ASSIGN_OR_RETURN(DataItem dict_values_schema,
                     databag_.GetSchemaAttr(
                         schema, schema::kDictValuesSchemaAttr, fallbacks_));
    ASSIGN_OR_RETURN(auto dict_keys, databag_.GetDictKeys(item, fallbacks_));
    ASSIGN_OR_RETURN(auto dict_values,
                     databag_.GetDictValues(item, fallbacks_));
    DCHECK_EQ(dict_keys.first.size(), dict_values.first.size());
    return TransitionsSet::CreateForDict(
        std::move(dict_keys_schema), std::move(dict_values_schema),
        std::move(dict_keys.first), std::move(dict_values.first));
  }

  absl::StatusOr<TransitionsSet> EntityTransitions(const DataItem& item,
                                                   const DataItem& schema,
                                                   bool remove_special_attrs) {
    if (schema.template value<ObjectId>().IsNoFollowSchema()) {
      // No processing needed for NoFollowSchema.
      return TransitionsSet::CreateEmpty();
    }
    ASSIGN_OR_RETURN(DataSliceImpl attr_names_slice,
                     databag_.GetSchemaAttrs(schema, fallbacks_));
    if (attr_names_slice.size() == 0) {
      return TransitionsSet::CreateEmpty();
    }
    if (attr_names_slice.present_count() != attr_names_slice.size()) {
      return absl::InternalError("schema attribute names should be present");
    }
    const auto& attr_names = attr_names_slice.values<arolla::Text>();

    auto is_special_attr = [](std::string_view attr_name) {
      return attr_name == schema::kSchemaNameAttr ||
             attr_name == schema::kSchemaMetadataAttr;
    };

    bool has_list_items_attr = false;
    bool has_dict_keys_attr = false;
    bool has_dict_values_attr = false;
    int64_t special_attrs_count = 0;
    // Schema attributes are previsited when the item is being visited.
    attr_names.ForEach(
        [&](int64_t id, bool presence, std::string_view attr_name) {
          DCHECK(presence);
          if (attr_name == schema::kListItemsSchemaAttr) {
            has_list_items_attr = true;
          } else if (attr_name == schema::kDictKeysSchemaAttr) {
            has_dict_keys_attr = true;
          } else if (attr_name == schema::kDictValuesSchemaAttr) {
            has_dict_values_attr = true;
          } else if (is_special_attr(attr_name)) {
            ++special_attrs_count;
          }
        });
    if (has_list_items_attr) {
      if (attr_names.size() != 1) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "list schema %v has unexpected attributes", schema));
      }
      return ListTransitions(item, schema);
    } else if (has_dict_keys_attr || has_dict_values_attr) {
      if (attr_names.size() != 2 || !has_dict_keys_attr ||
          !has_dict_values_attr) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "dict schema %v has unexpected attributes", schema));
      }
      return DictTransitions(item, schema);
    }
    if (!remove_special_attrs || !special_attrs_count) {
      return TransitionsSet::CreateForEntity(std::move(attr_names_slice));
    }
    if (special_attrs_count == attr_names.size()) {
      return TransitionsSet::CreateEmpty();
    }
    arolla::DenseArrayBuilder<arolla::Text> filtered_attr_names(
        attr_names.size() - special_attrs_count);
    int64_t new_id = 0;
    attr_names.ForEach(
        [&](int64_t id, bool presence, std::string_view attr_name) {
          if (!is_special_attr(attr_name)) {
            filtered_attr_names.Set(new_id, attr_name);
            ++new_id;
          }
        });
    return TransitionsSet::CreateForEntity(
        DataSliceImpl::Create(std::move(filtered_attr_names).Build()));
  }

  absl::StatusOr<TransitionsSet> SchemaTransitions(const DataItem& item) {
    if (!item.template holds_value<ObjectId>()) {
      return TransitionsSet::CreateEmpty();
    }
    if (item.template value<ObjectId>().IsNoFollowSchema()) {
      // No processing needed for NoFollowSchema.
      return TransitionsSet::CreateEmpty();
    }
    ASSIGN_OR_RETURN(DataSliceImpl attr_names_slice,
                     databag_.GetSchemaAttrs(item, fallbacks_));
    if (attr_names_slice.size() == 0) {
      return TransitionsSet::CreateEmpty();
    }
    if (attr_names_slice.present_count() != attr_names_slice.size()) {
      return absl::InternalError("schema attribute names should be present");
    }
    return TransitionsSet::CreateForSchema(std::move(attr_names_slice));
  }

  const DataBagImpl& databag_;
  const DataBagImpl::FallbackSpan fallbacks_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_TRAVERSE_HELPER_H_
