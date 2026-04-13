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
#include "koladata/internal/op_utils/apply_filter.h"

#include <cstdint>
#include <stack>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/extract.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/util/status_macros_backport.h"  // NOLINT

namespace koladata::internal {

using schema_filters::AnyPrimitiveFilter;
using schema_filters::AnySchemaFilter;

namespace {

class ApplyFilterTraverser {
  // Traverser for applying filter to a DataSlice.
  //
  // The result is stored in a new DataBag, and contains a subset of the
  // attributes of the original DataSlice. While the filter is a schema that
  // defines which attributes to keep.
  //
  // The filter is a schema that might contain special schemas:
  // * kAnyPrimitive: matches any primitive schema.
  // * kAnySchema: matches any schema.
 public:
  ApplyFilterTraverser(TraverseHelper ds_traverse_helper,
                       TraverseHelper filter_traverse_helper,
                       DataBagImpl& new_databag)
      : ds_traverse_helper_(std::move(ds_traverse_helper)),
        filter_traverse_helper_(std::move(filter_traverse_helper)),
        new_databag_(new_databag),
        visited_items_(),
        to_visit_() {}

  absl::Status ApplyFilter(const DataSliceImpl& ds, const DataItem& schema,
                           const DataItem& filter) {
    for (const DataItem& item : ds) {
      RETURN_IF_ERROR(Visit({item, schema, filter}));
    }
    return ProcessStack();
  }

  DataSliceImpl GetCloneFilterMetadataSlice() {
    return DataSliceImpl::Create(clone_filter_metadata_);
  }

  DataSliceImpl GetCloneFilterMetadataIdsSlice() {
    return DataSliceImpl::Create(clone_filter_metadata_ids_);
  }

 private:
  struct ItemWithSchemaAndFilter {
    DataItem item;
    DataItem schema;
    DataItem filter;
  };

  absl::Status ValidatePrimitiveType(const DataItem& item,
                                     const DataItem& schema) {
    DCHECK(schema.value<schema::DType>().is_primitive());
    if (!item.has_value()) {
      return absl::OkStatus();
    }
    auto dtype = item.dtype();
    if (auto schema_dtype = schema.value<schema::DType>();
        schema_dtype.qtype() != dtype) {
      return absl::InvalidArgumentError(
          absl::StrFormat("Item %v does not match schema %v", item, schema));
    }
    return absl::OkStatus();
  }

  absl::Status Visit(ItemWithSchemaAndFilter item) {
    auto [it, inserted] = visited_items_.try_emplace(
        std::make_pair(item.item, item.schema), item.filter);
    if (!inserted) {
      if (it->second != item.filter) {
        if (!item.schema.is_struct_schema()) {
          // We can visit the same schema with different filters.
          // It have to be supported for primitive schemas, or List/Dict schemas
          // with primitive items.
          return absl::OkStatus();
        }
        return absl::InvalidArgumentError(
            absl::StrFormat("Item %v with schema %v is already visited with a "
                            "different filter %v, got %v",
                            item.item, item.schema, it->second, item.filter));
      }
      return absl::OkStatus();
    }
    to_visit_.push(std::move(item));
    return absl::OkStatus();
  }

  // Process schema with no filter.
  // Copies the schema and recursively process the schema attributes.
  absl::Status ProcessSchema(const DataItem& schema) {
    DCHECK(schema.is_schema());
    if (!schema.is_struct_schema()) {
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(
        auto schema_transitions_set,
        ds_traverse_helper_.GetTransitions(schema, DataItem(schema::kSchema)));
    auto schema_transition_keys = schema_transitions_set.GetTransitionKeys();

    for (const auto& key : schema_transition_keys) {
      ASSIGN_OR_RETURN(
          auto schema_transition,
          ds_traverse_helper_.TransitionByKey(schema, DataItem(schema::kSchema),
                                              schema_transitions_set, key));
      std::string_view attr_name = key.value.value<arolla::Text>();
      RETURN_IF_ERROR(new_databag_.SetSchemaAttr(schema, attr_name,
                                                 schema_transition.item));
      if (attr_name == schema::kSchemaNameAttr) {
        continue;
      } else if (attr_name == schema::kSchemaMetadataAttr) {
        RETURN_IF_ERROR(Visit({std::move(schema_transition.item),
                               DataItem(schema::kObject), AnySchemaFilter()}));
        continue;
      }
      RETURN_IF_ERROR(Visit({std::move(schema_transition.item),
                             DataItem(schema::kSchema), AnySchemaFilter()}));
    }
    return absl::OkStatus();
  }

  // Clone the metadata from the filter to the schema metadata.
  absl::Status CloneMetadataFromFilter(const DataItem& schema,
                                       const DataItem& filter_metadata) {
    ASSIGN_OR_RETURN(auto schema_metadata, internal::CreateUuidWithMainObject(
                                               schema, schema::kMetadataSeed));
    RETURN_IF_ERROR(new_databag_.SetSchemaAttr(
        schema, schema::kSchemaMetadataAttr, schema_metadata));
    clone_filter_metadata_.push_back(filter_metadata);
    clone_filter_metadata_ids_.push_back(std::move(schema_metadata));
    return absl::OkStatus();
  }

  // Process schema with an entity filter.
  // Copies and recursively process the schema attributes listed in the filter.
  absl::Status ProcessSchemaWithEntityFilter(const DataItem& schema,
                                             const DataItem& filter) {
    DCHECK(schema.is_schema());
    if (!schema.is_struct_schema()) {
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(auto filter_transitions_set,
                     filter_traverse_helper_.GetTransitions(
                         filter, DataItem(schema::kSchema)));
    ASSIGN_OR_RETURN(
        auto schema_transitions_set,
        ds_traverse_helper_.GetTransitions(schema, DataItem(schema::kSchema)));
    ASSIGN_OR_RETURN(auto transition,
                     ds_traverse_helper_.SchemaAttributeTransitionAllowMissing(
                         schema, schema::kSchemaNameAttr));
    if (transition.item.has_value()) {
      RETURN_IF_ERROR(new_databag_.SetSchemaAttr(
          schema, schema::kSchemaNameAttr, transition.item));
    }
    auto filter_transition_keys = filter_transitions_set.GetTransitionKeys();
    for (const auto& key : filter_transition_keys) {
      ASSIGN_OR_RETURN(
          auto filter_transition,
          filter_traverse_helper_.TransitionByKey(
              filter, DataItem(schema::kSchema), filter_transitions_set, key));
      if (key.value.value<arolla::Text>() == schema::kSchemaNameAttr) {
        continue;
      } else if (key.value.value<arolla::Text>() ==
                 schema::kSchemaMetadataAttr) {
        RETURN_IF_ERROR(
            CloneMetadataFromFilter(schema, filter_transition.item));
        continue;
      }
      ASSIGN_OR_RETURN(
          auto schema_transition,
          ds_traverse_helper_.SchemaAttributeTransitionAllowMissing(
              schema, key.value.value<arolla::Text>()));
      if (!schema_transition.item.has_value()) {
        return absl::InvalidArgumentError(
            absl::StrFormat("the attribute '%v' defined by filter %v is "
                            "missing on the schema %v",
                            key.value.value<arolla::Text>(), filter, schema));
      }
      RETURN_IF_ERROR(Visit({schema_transition.item, DataItem(schema::kSchema),
                             std::move(filter_transition.item)}));
      RETURN_IF_ERROR(
          new_databag_.SetSchemaAttr(schema, key.value.value<arolla::Text>(),
                                     std::move(schema_transition.item)));
    }
    // We need to extract the schema name and metadata despite of the filter.
    ASSIGN_OR_RETURN(auto schema_name_transition,
                     ds_traverse_helper_.SchemaAttributeTransitionAllowMissing(
                         schema, schema::kSchemaNameAttr));
    if (schema_name_transition.item.has_value()) {
      RETURN_IF_ERROR(new_databag_.SetSchemaAttr(
          schema, schema::kSchemaNameAttr, schema_name_transition.item));
    }
    ASSIGN_OR_RETURN(auto schema_metadata_transition,
                     ds_traverse_helper_.SchemaAttributeTransitionAllowMissing(
                         schema, schema::kSchemaMetadataAttr));
    if (schema_metadata_transition.item.has_value()) {
      RETURN_IF_ERROR(
          new_databag_.SetSchemaAttr(schema, schema::kSchemaMetadataAttr,
                                     schema_metadata_transition.item));
      RETURN_IF_ERROR(Visit({std::move(schema_metadata_transition.item),
                             DataItem(schema::kObject), AnySchemaFilter()}));
    }
    return absl::OkStatus();
  }

  // Copies the list items and recursively process them with the corresponding
  // schema and filter.
  absl::Status ProcessListWithTransitions(
      const DataItem& list, const DataItem& schema, const DataItem& filter,
      const TraverseHelper::TransitionsSet& transitions_set) {
    DCHECK(list.holds_value<ObjectId>() && list.value<ObjectId>().IsList());
    RETURN_IF_ERROR(
        new_databag_.ExtendList(list, transitions_set.list_items()));
    ASSIGN_OR_RETURN(auto list_item_schema,
                     ds_traverse_helper_.SchemaAttributeTransition(
                         schema, schema::kListItemsSchemaAttr));
    auto filter_list_item_schema = TraverseHelper::Transition(
        AnySchemaFilter(), DataItem(schema::kSchema));
    if (filter != AnySchemaFilter()) {
      ASSIGN_OR_RETURN(filter_list_item_schema,
                       filter_traverse_helper_.SchemaAttributeTransition(
                           filter, schema::kListItemsSchemaAttr));
    }
    const auto& items = transitions_set.list_items();
    for (int64_t i = 0; i < items.size(); ++i) {
      RETURN_IF_ERROR(Visit(
          {items[i], list_item_schema.item, filter_list_item_schema.item}));
    }
    return absl::OkStatus();
  }

  // Copies the dict key and values and recursively process them with the
  // corresponding schema and filter.
  absl::Status ProcessDictWithTransitions(
      const DataItem& dict, const DataItem& schema, const DataItem& filter,
      const TraverseHelper::TransitionsSet& transitions_set) {
    DCHECK(dict.holds_value<ObjectId>() && dict.value<ObjectId>().IsDict());
    auto keys = transitions_set.dict_keys();
    auto values = transitions_set.dict_values();
    DCHECK(keys.size() == values.size());
    RETURN_IF_ERROR(new_databag_.SetInDict(
        DataSliceImpl::Create(keys.size(), dict), keys, values));
    ASSIGN_OR_RETURN(auto keys_schema,
                     ds_traverse_helper_.SchemaAttributeTransition(
                         schema, schema::kDictKeysSchemaAttr));
    ASSIGN_OR_RETURN(auto values_schema,
                     ds_traverse_helper_.SchemaAttributeTransition(
                         schema, schema::kDictValuesSchemaAttr));
    auto filter_keys_schema = TraverseHelper::Transition(
        AnySchemaFilter(), DataItem(schema::kSchema));
    auto filter_values_schema = TraverseHelper::Transition(
        AnySchemaFilter(), DataItem(schema::kSchema));
    if (filter != AnySchemaFilter()) {
      ASSIGN_OR_RETURN(filter_keys_schema,
                       filter_traverse_helper_.SchemaAttributeTransition(
                           filter, schema::kDictKeysSchemaAttr));
      ASSIGN_OR_RETURN(filter_values_schema,
                       filter_traverse_helper_.SchemaAttributeTransition(
                           filter, schema::kDictValuesSchemaAttr));
    }
    for (int64_t i = 0; i < keys.size(); ++i) {
      RETURN_IF_ERROR(
          Visit({keys[i], keys_schema.item, filter_keys_schema.item}));
      RETURN_IF_ERROR(
          Visit({values[i], values_schema.item, filter_values_schema.item}));
    }
    return absl::OkStatus();
  }

  // Process item with an entity schema and an entity or no filter.
  // Copies the attributes listed in the filter and recursively process them
  // with the corresponding schema and filter.
  absl::Status ProcessEntityWithEntityFilter(const DataItem& item,
                                             const DataItem& schema,
                                             const DataItem& filter) {
    DCHECK(filter.holds_value<ObjectId>());
    RETURN_IF_ERROR(Visit({schema, DataItem(schema::kSchema), filter}));
    if (!item.has_value()) {
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(auto transitions_set,
                     ds_traverse_helper_.GetTransitions(item, schema));
    if (transitions_set.is_list()) {
      return ProcessListWithTransitions(item, schema, filter, transitions_set);
    }
    if (transitions_set.is_dict()) {
      return ProcessDictWithTransitions(item, schema, filter, transitions_set);
    }
    if (filter != AnySchemaFilter()) {
      ASSIGN_OR_RETURN(transitions_set, filter_traverse_helper_.GetTransitions(
                                            filter, DataItem(schema::kSchema)));
    }
    if (!transitions_set.has_attrs()) {
      return absl::OkStatus();
    }
    const auto& attr_names = transitions_set.attr_names();
    arolla::DenseArrayBuilder<DataItem> attr_values(attr_names.size());

    absl::Status status = absl::OkStatus();
    attr_names.ForEach(
        [&](int64_t i, bool presence, const std::string_view attr_name) {
          DCHECK(presence);
          if (!status.ok()) {
            return;
          }
          if (attr_name == schema::kSchemaNameAttr) {
            return;
          } else if (attr_name == schema::kSchemaMetadataAttr) {
            // Metadata would be extracted in ProcessSchema.
            return;
          }
          auto transition_or =
              ds_traverse_helper_.AttributeTransition(item, schema, attr_name);
          if (!transition_or.ok()) {
            status = transition_or.status();
            return;
          }
          attr_values.Set(i, transition_or->item);
          auto filter_attr = AnySchemaFilter();
          if (filter != AnySchemaFilter()) {
            auto filter_transition_or =
                filter_traverse_helper_.SchemaAttributeTransition(filter,
                                                                  attr_name);
            if (!filter_transition_or.ok()) {
              status = filter_transition_or.status();
              return;
            }
            filter_attr = std::move(filter_transition_or->item);
          }
          if (item.holds_value<ObjectId>()) {
            status = new_databag_.SetAttr(item, attr_name,
                                          std::move(transition_or->item));
            if (!status.ok()) {
              return;
            }
          }
          status =
              Visit({std::move(transition_or->item),
                     std::move(transition_or->schema), std::move(filter_attr)});
        });
    return status;
  }

  // Processes an arbitrary item and schema with no filter.
  //
  // Copies the attributes and recursively process them with the corresponding
  // schema and no filter.
  absl::Status ProcessItem(const DataItem& item, const DataItem& schema) {
    if (schema.holds_value<ObjectId>()) {
      return ProcessEntityWithEntityFilter(item, schema, AnySchemaFilter());
    }
    if (schema.holds_value<schema::DType>()) {
      if (schema == schema::kObject) {
        if (!item.holds_value<ObjectId>()) {
          return absl::OkStatus();
        }
        ASSIGN_OR_RETURN(auto obj_schema,
                         ds_traverse_helper_.GetObjectSchema(item));
        RETURN_IF_ERROR(
            new_databag_.SetAttr(item, schema::kSchemaAttr, obj_schema));
        return Visit({item, std::move(obj_schema), AnySchemaFilter()});
      }
      if (schema == schema::kSchema) {
        return ProcessSchema(item);
      }
      if (schema.is_primitive_schema()) {
        return ValidatePrimitiveType(item, schema);
      }
    }
    return absl::OkStatus();
  }

  // Processes an arbitrary item and schema with an ANY_PRIMITIVE filter.
  //
  // Checks that the item is a primitive.
  // Or if the item is a schema, that matches the ANY_PRIMITIVE filter.
  absl::Status ProcessItemWithAnyPrimitiveFilter(const DataItem& item,
                                                 const DataItem& schema) {
    if (schema.is_primitive_schema() || schema.is_itemid_schema()) {
      return absl::OkStatus();
    }
    if (schema.is_object_schema() && !item.holds_value<ObjectId>()) {
      return absl::OkStatus();
    }
    if (schema.is_schema_schema()) {
      if (item.is_primitive_schema() || item.is_object_schema()) {
        return absl::OkStatus();
      }
    }
    return absl::InvalidArgumentError(absl::StrFormat(
        "Item %v with schema %v does not match ANY_PRIMITIVE filter", item,
        schema));
  }

  // Processes an arbitrary item and schema with a primitive filter.
  //
  // Checks that the item is a primitive and that it matches the filter.
  // Or if the item is schema, that it matches a primitive filter.
  absl::Status ProcessItemWithPrimitiveFilter(const DataItem& item,
                                              const DataItem& schema,
                                              const DataItem& filter) {
    if (schema.is_primitive_schema()) {
      if (filter == schema) {
        return ValidatePrimitiveType(item, schema);
      }
      return absl::InvalidArgumentError(
          absl::StrFormat("Item %v with schema %v does not match filter %v",
                          item, schema, filter));
    }
    if (schema.is_object_schema()) {
      if (item.ContainsAnyPrimitives()) {
        return ValidatePrimitiveType(item, filter);
      }
      return absl::InvalidArgumentError(
          absl::StrFormat("Item %v with schema %v does not match filter %v",
                          item, schema, filter));
    }
    if (schema.is_schema_schema()) {
      if (item == filter) {
        return absl::OkStatus();
      }
      return absl::InvalidArgumentError(
          absl::StrFormat("Schema %v does not match filter %v", item, filter));
    }
    return absl::InvalidArgumentError(absl::StrFormat(
        "Item %v with schema %v does not have not a primitive schema", item,
        schema));
  }

  // Processes an arbitrary item and schema with a OBJECT filter.
  //
  // Checks that the item has OBJECT schema. Or if the item is OBJECT schema.
  absl::Status ProcessItemWithObjectFilter(const DataItem& item,
                                           const DataItem& schema) {
    if (schema.is_object_schema()) {
      if (!item.holds_value<ObjectId>()) {
        return absl::OkStatus();
      }
      ASSIGN_OR_RETURN(auto obj_schema,
                       ds_traverse_helper_.GetObjectSchema(item));
      RETURN_IF_ERROR(
          new_databag_.SetAttr(item, schema::kSchemaAttr, obj_schema));
      return Visit({item, std::move(obj_schema), AnySchemaFilter()});
    }
    if (schema.is_schema_schema()) {
      if (item == schema::kObject) {
        return absl::OkStatus();
      }
      return absl::InvalidArgumentError(absl::StrFormat(
          "Schema %v does not match filter %v", item, schema::kObject));
    }
    return absl::InvalidArgumentError(absl::StrFormat(
        "Item %v with schema %v does not match an OBJECT filter", item,
        schema));
  }

  // Processes an arbitrary item and schema with an entity filter.
  //
  // Checks that the item has an OBJECT of an entity schema.
  // Or if the item is an OBJECT or an entity schema.
  absl::Status ProcessItemWithEntityFilter(const DataItem& item,
                                           const DataItem& schema,
                                           const DataItem& filter) {
    if (schema.is_object_schema()) {
      if (!item.has_value()) {
        return absl::OkStatus();
      }
      if (!item.holds_value<ObjectId>()) {
        return absl::InvalidArgumentError(
            absl::StrFormat("Item %v with schema %v does not match filter %v",
                            item, schema, filter));
      }
      ASSIGN_OR_RETURN(auto obj_schema,
                       ds_traverse_helper_.GetObjectSchema(item));
      RETURN_IF_ERROR(
          new_databag_.SetAttr(item, schema::kSchemaAttr, obj_schema));
      return Visit({item, std::move(obj_schema), filter});
    }
    if (schema.holds_value<ObjectId>()) {
      return ProcessEntityWithEntityFilter(item, schema, filter);
    }
    if (schema.is_schema_schema()) {
      return ProcessSchemaWithEntityFilter(item, filter);
    }
    return absl::InvalidArgumentError(absl::StrFormat(
        "Item %v with schema %v does not have not match filter %v", item,
        schema, filter));
  }

  // Processes the stack of items that are waiting to be copied.
  absl::Status ProcessStack() {
    while (!to_visit_.empty()) {
      auto [item, schema, filter] = std::move(to_visit_.top());
      to_visit_.pop();
      if (filter == AnyPrimitiveFilter()) {
        RETURN_IF_ERROR(ProcessItemWithAnyPrimitiveFilter(item, schema));
      } else if (filter == AnySchemaFilter()) {
        RETURN_IF_ERROR(ProcessItem(item, schema));
      } else if (filter.holds_value<schema::DType>()) {
        if (filter.is_schema_schema()) {
          RETURN_IF_ERROR(ProcessSchema(item));
        } else if (filter.is_object_schema()) {
          RETURN_IF_ERROR(ProcessItemWithObjectFilter(item, schema));
        } else if (filter.is_primitive_schema()) {
          RETURN_IF_ERROR(ProcessItemWithPrimitiveFilter(item, schema, filter));
        }
      } else if (filter.holds_value<ObjectId>()) {
        RETURN_IF_ERROR(ProcessItemWithEntityFilter(item, schema, filter));
      } else {
        return absl::InvalidArgumentError(
            absl::StrFormat("Unsupported filter: %v", filter));
      }
    }
    return absl::OkStatus();
  }

 private:
  TraverseHelper ds_traverse_helper_;
  TraverseHelper filter_traverse_helper_;
  DataBagImpl& new_databag_;
  absl::flat_hash_map<std::pair<DataItem, DataItem>, DataItem,
                      DataItem::HashOfPair>
      visited_items_;
  std::stack<ItemWithSchemaAndFilter> to_visit_;
  std::vector<DataItem> clone_filter_metadata_;
  std::vector<DataItem> clone_filter_metadata_ids_;
};

}  // namespace

absl::Status ApplyFilterOp::operator()(
    const DataSliceImpl& ds, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks, const DataItem& filter,
    const DataBagImpl& filter_databag,
    DataBagImpl::FallbackSpan filter_fallbacks) const {
  auto ds_traverse_helper = TraverseHelper(databag, fallbacks);
  auto filter_traverse_helper =
      TraverseHelper(filter_databag, filter_fallbacks);
  ApplyFilterTraverser apply_filter_traverser(std::move(ds_traverse_helper),
                                              std::move(filter_traverse_helper),
                                              new_databag_);
  RETURN_IF_ERROR(apply_filter_traverser.ApplyFilter(ds, schema, filter));

  {
    // Clone metadata from the filter.
    auto clone_filter_metadata_slice =
        apply_filter_traverser.GetCloneFilterMetadataSlice();
    auto clone_filter_metadata_ids_slice =
        apply_filter_traverser.GetCloneFilterMetadataIdsSlice();
    auto shallow_clone_databag = DataBagImpl::CreateEmptyDatabag();
    auto shallow_clone_op = ShallowCloneOp(&*shallow_clone_databag);
    ASSIGN_OR_RETURN((auto [shallow_cloned_metadata_, _]),
                     shallow_clone_op(clone_filter_metadata_slice,
                                      clone_filter_metadata_ids_slice,
                                      DataItem(schema::kObject), filter_databag,
                                      filter_fallbacks, nullptr, {}));

    std::vector<const DataBagImpl*> filter_databag_and_fallbacks = {
        &filter_databag};
    filter_databag_and_fallbacks.insert(filter_databag_and_fallbacks.end(),
                                        filter_fallbacks.begin(),
                                        filter_fallbacks.end());

    auto extract_op = ExtractOp(&new_databag_);
    RETURN_IF_ERROR(extract_op(
        shallow_cloned_metadata_, DataItem(schema::kObject),
        *shallow_clone_databag, filter_databag_and_fallbacks, nullptr, {}));
  }
  return absl::OkStatus();
}

absl::Status ApplyFilterOp::operator()(
    const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks, const DataItem& filter,
    const DataBagImpl& filter_databag,
    DataBagImpl::FallbackSpan filter_fallbacks) const {
  return ApplyFilterOp(new_databag_)(DataSliceImpl::Create(1, item), schema,
                                     databag, fallbacks, filter, filter_databag,
                                     filter_fallbacks);
}

}  // namespace koladata::internal
