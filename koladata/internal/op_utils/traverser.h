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
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/slice_builder.h"
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
      : traverse_helper_(databag, fallbacks),
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

  absl::Status ValidatePrimitiveType(const ItemWithSchema& item) {
    if (!item.item.has_value()) {
      return absl::OkStatus();
    }
    DCHECK(item.schema.is_primitive_schema());
    auto dtype = item.item.dtype();
    if (auto schema_dtype = item.schema.template value<schema::DType>();
        schema_dtype.qtype() != dtype) {
      auto item_qtype_or = schema::DType::FromQType(dtype);
      if (!item_qtype_or.ok()) {
        return absl::InvalidArgumentError(absl::StrCat(
            "during traversal, got a slice with primitive type ", schema_dtype,
            " while the actual content is not a primitive"));
      }
      return absl::InvalidArgumentError(absl::StrCat(
          "during traversal, got a slice with primitive type ", schema_dtype,
          " while the actual content has type ", item_qtype_or->name()));
    }
    return absl::OkStatus();
  }

  absl::Status Previsit(const ItemWithSchema& item) {
    if (item.schema.is_primitive_schema()) {
      RETURN_IF_ERROR(ValidatePrimitiveType(item));
    }
    if (item.item.has_value() && !item.item.ContainsAnyPrimitives()) {
      previsit_stack_.push({.item = item.item, .schema = item.schema});
    }
    return visitor_->VisitorT::Previsit(item.item, item.schema);
  }

  struct PairOfItemsHash {
    size_t operator()(const std::pair<DataItem, DataItem>& item) const {
      return absl::HashOf(DataItem::Hash()(item.first),
                          DataItem::Hash()(item.second));
    }
  };

  absl::Status DepthFirstPrevisitItemsAndSchemas() {
    // We do a depth-first traversal, with unrolled recursion. For that we keep
    // track of the stack size when we started visiting the current item. If the
    // stack would reach the same size again it would mean that all the items
    // reachable from the current item have been visited.
    std::stack<std::pair<int64_t, ItemWithSchema>> objects_on_stack;
    auto used_items =
        absl::flat_hash_set<std::pair<DataItem, DataItem>, PairOfItemsHash>();

    while (!previsit_stack_.empty()) {
      ItemWithSchema item = std::move(previsit_stack_.top());
      previsit_stack_.pop();
      if (!used_items.contains({item.item, item.schema})) {
        // This item would be later added with actual schema.
        objects_on_stack.emplace(previsit_stack_.size(), item);
        // Please note that we cannot insert NaN values into the set (see
        // b/395020189); one way to avoid this is not to insert primitives at
        // all, but we do check for that in Previsit.

        used_items.insert({item.item, item.schema});
        if (item.schema == schema::kObject) {
          ASSIGN_OR_RETURN(item.schema,
                           traverse_helper_.GetObjectSchema(item.item));
          RETURN_IF_ERROR(visitor_->VisitorT::Previsit(item.item, item.schema));
        }
        if (item.item != DataItem(schema::kSchema) ||
            item.schema != DataItem(schema::kSchema)) {
          // Always call Previsit on the schema, unless it would be a loop.
          RETURN_IF_ERROR(Previsit(
              {.item = item.schema, .schema = DataItem(schema::kSchema)}));
        }
        ASSIGN_OR_RETURN(
            TraverseHelper::TransitionsSet transitions_set,
            traverse_helper_.GetTransitions(item.item, item.schema,
                                            /*remove_special_attrs=*/false));
        absl::Status status = absl::OkStatus();
        RETURN_IF_ERROR(traverse_helper_.ForEachObject(
            item.item, item.schema, transitions_set,
            [&](const DataItem& item, const DataItem& schema) {
              if (!status.ok()) {
                return;
              }
              status = Previsit({.item = item, .schema = schema});
            }));
        if (!status.ok()) {
          return status;
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
    if (schema.is_primitive_schema()) {
      RETURN_IF_ERROR(ValidatePrimitiveType({.item = item, .schema = schema}));
    }
    return visitor_->VisitorT::GetValue(item, schema);
  }

  absl::Status VisitSchemaItem(const ItemWithSchema& item) {
    if (!item.item.has_value() || !item.item.template holds_value<ObjectId>()) {
      return absl::OkStatus();
    }
    DCHECK(item.schema == schema::kSchema);
    ASSIGN_OR_RETURN(auto transitions_set,
                     traverse_helper_.GetTransitions(item.item, item.schema));
    if (!transitions_set.has_attrs()) {
      return visitor_->VisitorT::VisitSchema(item.item, item.schema,
                                             /*is_object_schema=*/false, {},
                                             {});
    }
    const auto& attr_names = transitions_set.attr_names();
    arolla::DenseArrayBuilder<DataItem> attr_values(attr_names.size());
    absl::Status status = absl::OkStatus();
    attr_names.ForEach([&](int64_t i, bool presence,
                           const std::string_view attr_name) {
      DCHECK(presence);
      if (!status.ok()) {
        return;
      }
      auto transition_or =
          traverse_helper_.SchemaAttributeTransition(item.item, attr_name);
      if (!transition_or.ok()) {
        status = transition_or.status();
        return;
      }
      auto value_or = GetValue(transition_or->item, transition_or->schema);
      if (!value_or.ok()) {
        status = value_or.status();
        return;
      }
      attr_values.Set(i, *value_or);
    });
    if (!status.ok()) {
      return status;
    }
    return visitor_->VisitorT::VisitSchema(item.item, item.schema,
                                           /*is_object_schema=*/false,
                                           attr_names,
                                           std::move(attr_values).Build());
  }

  absl::Status VisitList(
      const ItemWithSchema& item, bool is_object,
      const TraverseHelper::TransitionsSet& transitions) {
    const auto& list_items = transitions.list_items();
    const auto& list_item_schema = transitions.list_item_schema();
    SliceBuilder list_items_values(list_items.size());
    for (int64_t i = 0; i < list_items.size(); ++i) {
      ASSIGN_OR_RETURN(auto value, GetValue(list_items[i], list_item_schema));
      list_items_values.InsertIfNotSetAndUpdateAllocIds(i, value);
    }
    return visitor_->VisitorT::VisitList(item.item, item.schema, is_object,
                                         std::move(list_items_values).Build());
  }

  absl::Status VisitDict(
      const ItemWithSchema& item, bool is_object,
      const TraverseHelper::TransitionsSet& transitions_set) {
    const DataSliceImpl& keys = transitions_set.dict_keys();
    const DataItem& keys_schema = transitions_set.dict_keys_schema();
    const DataSliceImpl& values = transitions_set.dict_values();
    const DataItem& values_schema = transitions_set.dict_values_schema();
    SliceBuilder keys_values(keys.size());
    SliceBuilder values_values(values.size());
    for (int64_t i = 0; i < keys.size(); ++i) {
      ASSIGN_OR_RETURN(DataItem key_value, GetValue(keys[i], keys_schema));
      ASSIGN_OR_RETURN(DataItem value_value,
                       GetValue(values[i], values_schema));
      keys_values.InsertIfNotSetAndUpdateAllocIds(i, key_value);
      values_values.InsertIfNotSetAndUpdateAllocIds(i, value_value);
    }
    return visitor_->VisitorT::VisitDict(item.item, item.schema, is_object,
                                         std::move(keys_values).Build(),
                                         std::move(values_values).Build());
  }

  absl::Status VisitEntity(const ItemWithSchema& item, bool is_object) {
    ASSIGN_OR_RETURN(auto transitions_set,
                     traverse_helper_.GetTransitions(item.item, item.schema));
    if (transitions_set.is_list()) {
      return VisitList(item, is_object, transitions_set);
    } else if (transitions_set.is_dict()) {
      return VisitDict(item, is_object, transitions_set);
    } else if (!transitions_set.has_attrs()) {
      return visitor_->VisitorT::VisitObject(item.item, item.schema, is_object,
                                             {}, {});
    }
    const auto& attr_names = transitions_set.attr_names();
    arolla::DenseArrayBuilder<DataItem> attr_values(attr_names.size());

    absl::Status status = absl::OkStatus();
    attr_names.ForEach([&](int64_t i, bool presence,
                           const std::string_view attr_name) {
      DCHECK(presence);
      if (!status.ok()) {
        return;
      }
      auto transition_or = traverse_helper_.AttributeTransition(
                                            item.item, item.schema, attr_name);
      if (!transition_or.ok()) {
        status = transition_or.status();
        return;
      }
      auto value_or = GetValue(transition_or->item, transition_or->schema);
      if (!value_or.ok()) {
        status = value_or.status();
        return;
      }
      attr_values.Set(i, *value_or);
    });
    if (!status.ok()) {
      return status;
    }
    return visitor_->VisitorT::VisitObject(item.item, item.schema, is_object,
                                           std::move(attr_names),
                                           std::move(attr_values).Build());
  }

  absl::Status VisitObject(const ItemWithSchema& item) {
    if (!item.item.has_value() || !item.item.template holds_value<ObjectId>()) {
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(auto schema, traverse_helper_.GetObjectSchema(item.item));
    return VisitEntity({item.item, schema}, /*is_object=*/true);
  }

  absl::Status VisitInPostOrder() {
    for (const ItemWithSchema& item : topological_order_) {
      if (item.schema.template holds_value<ObjectId>()) {
        // Entity schema.
        RETURN_IF_ERROR(VisitEntity(item, /*is_object=*/false));
      } else if (item.schema.template holds_value<schema::DType>()) {
        if (item.schema == schema::kObject) {
          // Object schema.
          RETURN_IF_ERROR(VisitObject(item));
        } else if (item.schema == schema::kSchema) {
          RETURN_IF_ERROR(VisitSchemaItem(item));
        }
      }
    }
    return absl::OkStatus();
  }

 private:
  TraverseHelper traverse_helper_;
  std::stack<ItemWithSchema> previsit_stack_;
  std::vector<ItemWithSchema> topological_order_;
  std::shared_ptr<VisitorT> visitor_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_TRAVERSER_H_
