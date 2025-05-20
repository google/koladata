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
#ifndef KOLADATA_INTERNAL_OP_UTILS_DEEP_COMPARATOR_H_
#define KOLADATA_INTERNAL_OP_UTILS_DEEP_COMPARATOR_H_

#include <sys/types.h>

#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
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

class TraverseHelper {
 public:
  struct Transition {
    DataItem item;
    DataItem schema;
  };

  class TransitionsSet {
   public:
    static TransitionsSet CreateForList(DataItem list_item_schema,
                                        DataSliceImpl list_items) {
      return TransitionsSet(std::nullopt, std::move(list_items),
                            std::move(list_item_schema), std::nullopt,
                            std::nullopt);
    }

    static TransitionsSet CreateForDict(DataItem dict_keys_schema,
                                        DataItem dict_values_schema,
                                        DataSliceImpl dict_keys,
                                        DataSliceImpl dict_values) {
      return TransitionsSet(std::move(dict_keys), std::move(dict_values),
                            std::nullopt, std::move(dict_keys_schema),
                            std::move(dict_values_schema));
    }

    static TransitionsSet CreateForEntity(DataSliceImpl attr_names) {
      return TransitionsSet(std::move(attr_names), std::nullopt, std::nullopt,
                            std::nullopt, std::nullopt);
    }

    static TransitionsSet CreateForSchema(DataSliceImpl attr_names) {
      return TransitionsSet(std::move(attr_names), std::nullopt, std::nullopt,
                            std::nullopt, std::nullopt);
    }

    static TransitionsSet CreateForObject() {
      return TransitionsSet(std::nullopt, std::nullopt, std::nullopt,
                            std::nullopt, std::nullopt);
    }

    static TransitionsSet CreateEmpty() {
      return TransitionsSet(std::nullopt, std::nullopt, std::nullopt,
                            std::nullopt, std::nullopt);
    }

    bool is_list() const { return list_item_schema_.has_value(); }
    bool is_dict() const { return dict_keys_schema_.has_value(); }
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

   private:
    TransitionsSet(std::optional<DataSliceImpl> keys_or_names,
                   std::optional<DataSliceImpl> values,
                   std::optional<DataItem> list_item_schema,
                   std::optional<DataItem> dict_keys_schema,
                   std::optional<DataItem> dict_values_schema)
        : keys_or_names_(std::move(keys_or_names)),
          values_(std::move(values)),
          list_item_schema_(std::move(list_item_schema)),
          dict_keys_schema_(std::move(dict_keys_schema)),
          dict_values_schema_(std::move(dict_values_schema)) {}

    std::optional<DataSliceImpl> keys_or_names_;
    std::optional<DataSliceImpl> values_;
    std::optional<DataItem> list_item_schema_;
    std::optional<DataItem> dict_keys_schema_;
    std::optional<DataItem> dict_values_schema_;
  };

  TraverseHelper(const DataBagImpl& databag,
                 DataBagImpl::FallbackSpan fallbacks)
      : databag_(databag), fallbacks_(fallbacks) {}

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

  template <typename Fn>
  absl::Status ForEachObject(const DataItem& item, const DataItem& schema,
                     const TransitionsSet& transitions_set, Fn&& fn) {
    static_assert(std::is_same_v<decltype(fn(arolla::view_type_t<DataItem>{},
                                             arolla::view_type_t<DataItem>())),
                                 void>,
                  "Callback shouldn't return value");
    if (schema == schema::kObject) {
      ASSIGN_OR_RETURN(DataItem object_schema, GetObjectSchema(item));
      if (object_schema.holds_value<ObjectId>()) {
        fn(item, object_schema);
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

  absl::StatusOr<DataItem> GetObjectSchema(const DataItem& item) {
    if (!item.has_value() || !item.template holds_value<ObjectId>()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("object %v is expected to be an object", item));
    }
    ASSIGN_OR_RETURN(DataItem schema,
                     databag_.GetAttr(item, schema::kSchemaAttr, fallbacks_));
    if (!schema.is_struct_schema()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("object %v is expected to have a schema in "
                          "%s attribute, got %v",
                          item, schema::kSchemaAttr, schema));
    }
    return schema;
  }

 private:
  template <typename Fn>
  void ForEachListItemObject(const TransitionsSet& transitions_set, Fn&& fn) {
    const DataSliceImpl& list_items = transitions_set.list_items();
    for (int64_t i = 0; i < list_items.size(); ++i) {
      const DataItem& item = list_items[i];
      if (item.holds_value<ObjectId>()) {
        fn(item, transitions_set.list_item_schema());
      }
    }
  }

  template <typename Fn>
  void ForEachDictElementObject(const TransitionsSet& transitions_set,
                                Fn&& fn) {
    const DataSliceImpl& dict_keys = transitions_set.dict_keys();
    for (int64_t i = 0; i < dict_keys.size(); ++i) {
      const DataItem& key = dict_keys[i];
      if (key.holds_value<ObjectId>()) {
        fn(key, transitions_set.dict_keys_schema());
      }
    }
    const DataSliceImpl& dict_values = transitions_set.dict_values();
    for (int64_t i = 0; i < dict_values.size(); ++i) {
      const DataItem& value = dict_values[i];
      if (value.holds_value<ObjectId>()) {
        fn(value, transitions_set.dict_values_schema());
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
          fn(transition_or->item, transition_or->schema);
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
          fn(transition_or->item, transition_or->schema);
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

#endif  // KOLADATA_INTERNAL_OP_UTILS_TRAVERSER_H_
