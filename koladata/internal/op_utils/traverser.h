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
#ifndef KOLADATA_INTERNAL_OP_UTILS_TRAVERSER_H_
#define KOLADATA_INTERNAL_OP_UTILS_TRAVERSER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stack>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/slice_builder.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

// An interface for a visitor that is used in Traverser.
class AbstractVisitor {
 public:
  virtual ~AbstractVisitor() = default;

  // Returns a value for the given item and schema.
  //
  // GetValue would only be called for an (item, schema) after Previsit was
  // called for the same (item, schema). If topological ordering of reachable
  // objects exists, then GetValue would be called for an (item, schema) only
  // after corresponding Visit* method was called for the same (item, schema).
  //
  // Result of GetValue is used in Visit* methods as values for attributes,
  // list items, dict keys and values.
  virtual absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                            const DataItem& schema) = 0;

  // Called for each reachable item and schema before any calls to Visit* or
  // GetValue methods.
  // For objects Previsit is called twice:
  // - first time with schema::kObject.
  // - second time with the schema written in kSchemaAttr attribute.
  virtual absl::Status Previsit(const DataItem& item,
                                const DataItem& schema) = 0;

  // Called for each reachable list.
  // Args:
  // - list: contains ObjectId of the list.
  // - schema: contains ObjectId of the list schema.
  // - is_object_schema: true iff schema was taken from kSchemaAttr attribute.
  // - items: contains values of the list items.
  virtual absl::Status VisitList(const DataItem& list, const DataItem& schema,
                                 bool is_object_schema,
                                 const DataSliceImpl& items) = 0;

  // Called for each reachable dict.
  // Args:
  // - dict: contains ObjectId of the dict.
  // - schema: contains ObjectId of the dict schema.
  // - is_object_schema: true iff schema was taken from kSchema attribute.
  // - keys: contains values of the dict keys.
  // - values: contains values of the dict values in the same order as
  // corresponding keys.
  virtual absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                                 bool is_object_schema,
                                 const DataSliceImpl& keys,
                                 const DataSliceImpl& values) = 0;

  // Called for each reachable object.
  // Args:
  // - list: contains ObjectId.
  // - schema: contains schema.
  // - is_object_schema: true iff schema was taken from kSchema attribute.
  // - attr_names: contains names of the attributes, including special names
  //     like kSchemaAttr, kListItemsSchemaAttr, kDictKeysSchemaAttr,
  //     kDictValuesSchemaAttr.
  // - attr_values: contains values of the attributes in the same order as
  //     corresponding attr_names.
  virtual absl::Status VisitObject(
      const DataItem& object, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_values) = 0;

  // Called for each reachable schema.
  // Args:
  // - item: contains schema.
  // - schema: is DataItem(schema::kSchema).
  // - is_object_schema: true iff schema was taken from kSchema attribute.
  // - attr_names: contains schema attribute names.
  // - attr_schema: contains values for attribute schemas in the same order as
  //     corresponding attr_names.
  virtual absl::Status VisitSchema(
      const DataItem& item, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_schema) = 0;

  // Call for each reachable primitive.
  // Args:
  // - item: contains primitive.
  // - schema: contains schema.
  virtual absl::Status VisitPrimitive(const DataItem& item,
                                      const DataItem& schema) = 0;
};

template <typename VisitorT>
class Traverser {
  // Traverses a DataBag starting from the given slice.
  //
  // The traversal is initialized with an AbstractVisitor implementation.
  //
  // The processing is organized in stages:
  // 1. Depth first search (DepthFirstPrevisitItemsAndSchemas):
  //    - calls visitor.Previsit for each reachable item and schema.
  //    - create a post order (topological order if it exists) of the reachable
  //    items and schemas.
  // 2. Iteration over the post order with the calls to visitor.Visit*
  // (VisitInPostOrder).
  //    - calls visitor.GetValue on attributes, list items, dict keys and values
  //    and use the results for arguments in visitor.Visit* calls. Except for
  //    the currently visited item and it's schema.
 public:
  Traverser(const DataBagImpl& databag, DataBagImpl::FallbackSpan fallbacks,
            std::shared_ptr<VisitorT> visitor)
      : databag_(databag),
        fallbacks_(fallbacks),
        previsit_stack_(),
        topological_order_(),
        visitor_(std::move(visitor)) {
    static_assert(std::is_base_of<AbstractVisitor, VisitorT>());
  }

  absl::Status TraverseSlice(const DataSliceImpl& ds, const DataItem& schema) {
    absl::Status status = absl::OkStatus();
    RETURN_IF_ERROR(
        Previsit({.item = schema, .schema = DataItem(schema::kSchema)}));
    for (const DataItem& item : ds) {
      RETURN_IF_ERROR(Previsit({.item = item, .schema = schema}));
    }
    if (!status.ok()) {
      return status;
    }
    RETURN_IF_ERROR(DepthFirstPrevisitItemsAndSchemas());
    RETURN_IF_ERROR(VisitInPostOrder());
    return absl::OkStatus();
  }

 private:
  struct ItemWithSchema {
    DataItem item;
    DataItem schema;
  };

  absl::Status Previsit(const ItemWithSchema& item) {
    if (item.item.has_value()) {
      previsit_stack_.push({.item = item.item, .schema = item.schema});
    }
    return visitor_->VisitorT::Previsit(item.item, item.schema);
  }

  absl::Status PrevisitAttribute(const ItemWithSchema& item,
                                 std::string_view attr_name) {
    DataItem attr_schema;
    if (attr_name == schema::kSchemaAttr) {
      attr_schema = DataItem(schema::kSchema);
    } else {
      ASSIGN_OR_RETURN(attr_schema, databag_.GetSchemaAttr(
                                        item.schema, attr_name, fallbacks_));
    }
    ASSIGN_OR_RETURN(auto attr_item,
                     databag_.GetAttr(item.item, attr_name, fallbacks_));
    RETURN_IF_ERROR(Previsit({.item = attr_item, .schema = attr_schema}));
    return absl::OkStatus();
  }

  absl::Status PrevisitListItems(const ItemWithSchema& item) {
    ASSIGN_OR_RETURN(
        auto list_item_schema,
        databag_.GetSchemaAttr(item.schema, schema::kListItemsSchemaAttr,
                               fallbacks_));
    ASSIGN_OR_RETURN(
        auto list_items,
        databag_.ExplodeList(item.item, DataBagImpl::ListRange(), fallbacks_));
    for (const DataItem& list_item : list_items) {
      RETURN_IF_ERROR(
          Previsit({.item = list_item, .schema = list_item_schema}));
    }
    return absl::OkStatus();
  }

  absl::Status PrevisitDictKeysAndValues(const ItemWithSchema& item) {
    ASSIGN_OR_RETURN(auto dict_keys_schema,
                     databag_.GetSchemaAttr(
                         item.schema, schema::kDictKeysSchemaAttr, fallbacks_));
    ASSIGN_OR_RETURN(
        auto dict_values_schema,
        databag_.GetSchemaAttr(item.schema, schema::kDictValuesSchemaAttr,
                               fallbacks_));
    ASSIGN_OR_RETURN(auto dict_keys,
                     databag_.GetDictKeys(item.item, fallbacks_));
    ASSIGN_OR_RETURN(auto dict_values,
                     databag_.GetDictValues(item.item, fallbacks_));
    for (const DataItem& dict_key : dict_keys.first) {
      RETURN_IF_ERROR(Previsit({.item = dict_key, .schema = dict_keys_schema}));
    }
    for (const DataItem& dict_value : dict_values.first) {
      RETURN_IF_ERROR(
          Previsit({.item = dict_value, .schema = dict_values_schema}));
    }
    return absl::OkStatus();
  }

  absl::Status PrevisitEntityAttributes(const ItemWithSchema& item) {
    if (item.schema.template value<ObjectId>().IsNoFollowSchema()) {
      // No processing needed for NoFollowSchema.
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(DataSliceImpl attr_names_slice,
                     databag_.GetSchemaAttrs(item.schema, fallbacks_));
    if (attr_names_slice.size() == 0) {
      return absl::OkStatus();
    }
    if (attr_names_slice.present_count() != attr_names_slice.size()) {
      return absl::InternalError("schema attribute names should be present");
    }
    const auto& attr_names = attr_names_slice.values<arolla::Text>();

    bool has_list_items_attr = false;
    bool has_dict_keys_attr = false;
    bool has_dict_values_attr = false;
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
          }
        });
    if (has_list_items_attr) {
      if (attr_names.size() != 1) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "list schema %v has unexpected attributes", item.schema));
      }
      return PrevisitListItems(item);
    } else if (has_dict_keys_attr || has_dict_values_attr) {
      if (attr_names.size() != 2 || !has_dict_keys_attr ||
          !has_dict_values_attr) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "dict schema %v has unexpected attributes", item.schema));
      }
      return PrevisitDictKeysAndValues(item);
    }
    absl::Status status = absl::OkStatus();
    attr_names.ForEach(
        [&](int64_t id, bool presence, std::string_view attr_name) {
          DCHECK(presence);
          if (!status.ok()) {
            return;
          }
          if (attr_name == schema::kSchemaNameAttr) {
            return;
          }
          status = PrevisitAttribute(item, attr_name);
        });
    return status;
  }

  absl::Status PrevisitObjectAttributes(const ItemWithSchema& item) {
    if (!item.item.has_value() || !item.item.template holds_value<ObjectId>()) {
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(
        auto schema,
        databag_.GetAttr(item.item, schema::kSchemaAttr, fallbacks_));
    if (!schema.is_struct_schema()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("object %v is expected to have a schema in "
                          "%s attribute, got %v",
                          item.item, schema::kSchemaAttr, schema));
    }
    RETURN_IF_ERROR(visitor_->VisitorT::Previsit(item.item, schema));
    RETURN_IF_ERROR(PrevisitAttribute(item, schema::kSchemaAttr));
    return PrevisitEntityAttributes({item.item, schema});
  }

  absl::Status PrevisitSchemaAttributes(const ItemWithSchema& item) {
    if (!item.item.template holds_value<ObjectId>()) {
      return absl::OkStatus();
    }
    if (item.item.template value<ObjectId>().IsNoFollowSchema()) {
      // No processing needed for NoFollowSchema.
      return absl::OkStatus();
    }
    DCHECK_EQ(item.schema, schema::kSchema);
    ASSIGN_OR_RETURN(DataSliceImpl attr_names_slice,
                     databag_.GetSchemaAttrs(item.item, fallbacks_));
    if (attr_names_slice.size() == 0) {
      return absl::OkStatus();
    }
    if (attr_names_slice.present_count() != attr_names_slice.size()) {
      return absl::InternalError("schema attribute names should be present");
    }
    const auto& attr_names = attr_names_slice.values<arolla::Text>();
    absl::Status status = absl::OkStatus();
    attr_names.ForEach(
        [&](int64_t id, bool presence, std::string_view attr_name) {
          DCHECK(presence);
          if (!status.ok()) {
            return;
          }
          auto attr_schema_or =
              databag_.GetSchemaAttr(item.item, attr_name, fallbacks_);
          if (!attr_schema_or.ok()) {
            status = attr_schema_or.status();
            return;
          }
          auto schema_dtype = (attr_name == schema::kSchemaNameAttr)
                                  ? schema::kString
                                  : schema::kSchema;
          status = Previsit(
              {.item = *attr_schema_or, .schema = DataItem(schema_dtype)});
        });
    return status;
  }

  absl::Status DepthFirstPrevisitItemsAndSchemas() {
    // We do a depth-first traversal, with unrolled recursion. For that we keep
    // track of the stack size when we started visiting the current item. If the
    // stack would reach the same size again it would mean that all the items
    // reachable from the current item have been visited.
    std::stack<std::pair<int64_t, ItemWithSchema>> objects_on_stack;
    // TODO: Mark pairs (item, schema) as used.
    auto used_items = absl::flat_hash_set<DataItem, DataItem::Hash>();

    while (!previsit_stack_.empty()) {
      ItemWithSchema item = std::move(previsit_stack_.top());
      previsit_stack_.pop();
      if (!used_items.contains(item.item)) {
        objects_on_stack.emplace(previsit_stack_.size(), item);
        used_items.insert(item.item);
        if (item.item != DataItem(schema::kSchema) ||
            item.schema != DataItem(schema::kSchema)) {
          // Always call Previsit on the schema, unless it would be a loop.
          RETURN_IF_ERROR(Previsit(
              {.item = item.schema, .schema = DataItem(schema::kSchema)}));
        }
        if (item.schema.template holds_value<ObjectId>()) {
          // Entity schema.
          RETURN_IF_ERROR(PrevisitEntityAttributes(item));
        } else if (item.schema.template holds_value<schema::DType>()) {
          if (item.schema == schema::kObject) {
            // Object schema.
            RETURN_IF_ERROR(PrevisitObjectAttributes(item));
          } else if (item.schema == schema::kSchema) {
            RETURN_IF_ERROR(PrevisitSchemaAttributes(item));
          }
        } else {
          return absl::InternalError("unsupported schema type");
        }
      }
      // Put the items for which we have already visited all the reachable items
      // in the topological order for the visitor->Visit* calls.
      while (!objects_on_stack.empty() &&
             objects_on_stack.top().first == previsit_stack_.size()) {
        topological_order_.push_back(objects_on_stack.top().second);
        objects_on_stack.pop();
      }
    }
    return absl::OkStatus();
  }

  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) {
    return visitor_->VisitorT::GetValue(item, schema);
  }

  absl::Status VisitList(const ItemWithSchema& item, bool is_object) {
    ASSIGN_OR_RETURN(
        auto list_item_schema,
        databag_.GetSchemaAttr(item.schema, schema::kListItemsSchemaAttr,
                               fallbacks_));
    ASSIGN_OR_RETURN(
        auto list_items,
        databag_.ExplodeList(item.item, DataBagImpl::ListRange(), fallbacks_));
    SliceBuilder list_items_values(list_items.size());
    for (size_t i = 0; i < list_items.size(); ++i) {
      ASSIGN_OR_RETURN(auto value, GetValue(list_items[i], list_item_schema));
      list_items_values.InsertIfNotSetAndUpdateAllocIds(i, value);
    }
    return visitor_->VisitorT::VisitList(item.item, item.schema, is_object,
                                          std::move(list_items_values).Build());
  }

  absl::Status VisitDict(const ItemWithSchema& item, bool is_object) {
    ASSIGN_OR_RETURN(auto dict_keys_schema,
                     databag_.GetSchemaAttr(
                         item.schema, schema::kDictKeysSchemaAttr, fallbacks_));
    ASSIGN_OR_RETURN(
        auto dict_values_schema,
        databag_.GetSchemaAttr(item.schema, schema::kDictValuesSchemaAttr,
                               fallbacks_));
    ASSIGN_OR_RETURN((auto [dict_keys, dict_keys_edge]),
                     databag_.GetDictKeys(item.item, fallbacks_));
    ASSIGN_OR_RETURN((auto [dict_values, dict_values_edge]),
                     databag_.GetDictValues(item.item, fallbacks_));
    SliceBuilder dict_keys_values(dict_keys.size());
    SliceBuilder dict_values_values(dict_values.size());
    for (size_t i = 0; i < dict_keys.size(); ++i) {
      ASSIGN_OR_RETURN(DataItem value,
                       GetValue(dict_keys[i], dict_keys_schema));
      dict_keys_values.InsertIfNotSetAndUpdateAllocIds(i, value);
    }
    for (size_t i = 0; i < dict_values.size(); ++i) {
      ASSIGN_OR_RETURN(DataItem value,
                       GetValue(dict_values[i], dict_values_schema));
      dict_values_values.InsertIfNotSetAndUpdateAllocIds(i, value);
    }
    return visitor_->VisitorT::VisitDict(
        item.item, item.schema, is_object, std::move(dict_keys_values).Build(),
        std::move(dict_values_values).Build());
  }

  absl::Status VisitSchema(const DataItem& schema,
                           const arolla::DenseArray<arolla::Text>& attr_names) {
    absl::Status status = absl::OkStatus();
    std::vector<std::tuple<absl::string_view, DataItem>> schema_attrs;
    arolla::DenseArrayBuilder<DataItem> attr_values(attr_names.size());
    DCHECK(attr_names.IsAllPresent());
    attr_names.ForEachPresent([&](int64_t id, std::string_view attr_name) {
      if (!status.ok()) {
        return;
      }
      auto attr_schema_or =
          databag_.GetSchemaAttr(schema, attr_name, fallbacks_);
      if (!attr_schema_or.ok()) {
        status = attr_schema_or.status();
        return;
      }
      auto attr_schema = *attr_schema_or;
      DataItem attr_schema_schema = DataItem(schema::kSchema);
      if (attr_name == schema::kSchemaNameAttr) {
        attr_schema_schema = DataItem(schema::kString);
      } else if (!attr_schema.is_schema()) {
        status = absl::InvalidArgumentError(absl::StrFormat(
            "schema %v has unexpected attribute %s", schema, attr_name));
        return;
      }
      auto attr_value_or = GetValue(attr_schema, attr_schema_schema);
      if (!attr_value_or.ok()) {
        status = attr_value_or.status();
        return;
      }
      auto attr_value = attr_value_or.value();
      attr_values.Set(id, attr_value);
    });
    if (!status.ok()) {
      return status;
    }
    return visitor_->VisitorT::VisitSchema(
        schema, DataItem(schema::kSchema),
        /*is_object_schema=*/false, attr_names, std::move(attr_values).Build());
  }

  absl::Status VisitSchemaItem(const ItemWithSchema& item) {
    if (!item.item.has_value()) {
      return absl::OkStatus();
    }
    if (item.item.template holds_value<ObjectId>()) {
      ASSIGN_OR_RETURN(DataSliceImpl attr_names_slice,
                       databag_.GetSchemaAttrs(item.item, fallbacks_));
      if (attr_names_slice.present_count() != attr_names_slice.size()) {
        return absl::InternalError("schema attribute names should be present");
      }
      arolla::DenseArray<arolla::Text> attr_names;
      if (!attr_names_slice.is_empty_and_unknown()) {
        attr_names = attr_names_slice.values<arolla::Text>();
      }
      return VisitSchema(item.item, attr_names);
    }
    return VisitPrimitive(item);
  }

  absl::Status VisitEntity(const ItemWithSchema& item, bool is_object) {
    ASSIGN_OR_RETURN(DataSliceImpl attr_names_slice,
                     databag_.GetSchemaAttrs(item.schema, fallbacks_));
    if (attr_names_slice.present_count() != attr_names_slice.size()) {
      return absl::InternalError("schema attribute names should be present");
    }
    arolla::DenseArray<arolla::Text> attr_names;
    if (!attr_names_slice.is_empty_and_unknown()) {
      attr_names = attr_names_slice.values<arolla::Text>();
    }
    bool has_list_items_attr = false;
    bool has_dict_keys_attr = false;
    bool has_dict_values_attr = false;
    DCHECK(attr_names.IsAllPresent());
    attr_names.ForEachPresent([&](int64_t id, std::string_view attr_name) {
      if (attr_name == schema::kListItemsSchemaAttr) {
        has_list_items_attr = true;
      } else if (attr_name == schema::kDictKeysSchemaAttr) {
        has_dict_keys_attr = true;
      } else if (attr_name == schema::kDictValuesSchemaAttr) {
        has_dict_values_attr = true;
      }
    });
    RETURN_IF_ERROR(VisitSchema(item.schema, attr_names));
    if (has_list_items_attr) {
      if (attr_names.size() != 1) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "list schema %v has unexpected attributes", item.schema));
      }
      return VisitList(item, is_object);
    } else if (has_dict_keys_attr || has_dict_values_attr) {
      if (attr_names.size() != 2 || !has_dict_keys_attr ||
          !has_dict_values_attr) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "dict schema %v has unexpected attributes", item.schema));
      }
      return VisitDict(item, is_object);
    }
    arolla::DenseArrayBuilder<DataItem> attr_values(attr_names.size());
    arolla::DenseArrayBuilder<arolla::Text> actual_attr_names(
        attr_names.size());
    absl::Status status = absl::OkStatus();
    size_t attr_count = 0;
    attr_names.ForEach([&](int64_t id, bool presence,
                           std::string_view attr_name) {
      DCHECK(presence);
      if (!status.ok()) {
        return;
      }
      if (attr_name == schema::kSchemaNameAttr) {
        return;
      }
      auto attr_item_or = databag_.GetAttr(item.item, attr_name, fallbacks_);
      if (!attr_item_or.ok()) {
        status = attr_item_or.status();
        return;
      }
      auto attr_schema_or =
          databag_.GetSchemaAttr(item.schema, attr_name, fallbacks_);
      if (!attr_schema_or.ok()) {
        status = attr_item_or.status();
        return;
      }
      auto value_or = GetValue(*attr_item_or, *attr_schema_or);
      if (!value_or.ok()) {
        status = value_or.status();
        return;
      }
      attr_values.Set(attr_count, *value_or);
      actual_attr_names.Set(attr_count, attr_name);
      ++attr_count;
    });
    if (!status.ok()) {
      return status;
    }
    return visitor_->VisitorT::VisitObject(
        item.item, item.schema, is_object,
        std::move(actual_attr_names).Build(attr_count),
        std::move(attr_values).Build(attr_count));
  }

  absl::Status VisitObject(const ItemWithSchema& item) {
    if (!item.item.has_value()) {
      return absl::OkStatus();
    }
    if (!item.item.template holds_value<ObjectId>()) {
      ASSIGN_OR_RETURN(auto dtype, schema::DType::FromQType(item.item.dtype()));
      return VisitPrimitive({.item = item.item, .schema = DataItem(dtype)});
    }
    ASSIGN_OR_RETURN(
        auto schema,
        databag_.GetAttr(item.item, schema::kSchemaAttr, fallbacks_));
    if (!schema.is_schema()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "object %v is expected to have a schema in %s attribute, got %v",
          item.item, schema::kSchemaAttr, schema));
    }
    return VisitEntity({item.item, schema}, /*is_object=*/true);
  }

  absl::Status VisitPrimitive(const ItemWithSchema& item) {
    return visitor_->VisitorT::VisitPrimitive(item.item, item.schema);
  }

  absl::Status VisitInPostOrder() {
    for (auto item : topological_order_) {
      if (item.schema.template holds_value<ObjectId>()) {
        // Entity schema.
        RETURN_IF_ERROR(VisitEntity(item, /*is_object=*/false));
      } else if (item.schema.template holds_value<schema::DType>()) {
        if (item.schema == schema::kObject) {
          // Object schema.
          RETURN_IF_ERROR(VisitObject(item));
        } else if (item.schema == schema::kSchema) {
          RETURN_IF_ERROR(VisitSchemaItem(item));
        } else {
          RETURN_IF_ERROR(VisitPrimitive(item));
        }
      }
    }
    return absl::OkStatus();
  }

 private:
  const DataBagImpl& databag_;
  const DataBagImpl::FallbackSpan fallbacks_;
  std::stack<ItemWithSchema> previsit_stack_;
  std::vector<ItemWithSchema> topological_order_;
  std::shared_ptr<VisitorT> visitor_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_TRAVERSER_H_
