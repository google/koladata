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
#ifndef KOLADATA_INTERNAL_OP_UTILS_OBJECT_FINDER_H_
#define KOLADATA_INTERNAL_OP_UTILS_OBJECT_FINDER_H_

#include <cstddef>
#include <cstdint>
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

class ObjectFinder {
  // Finds paths to objects in a DataBag, starting from a given slice.
  //
  // The visitor is called for each object found (including primitives).
  // The visitor can request the path to the object.
 public:
  using ObjectPathCallback = absl::FunctionRef<absl::Status(
      const DataItem&, const DataItem&,
      absl::FunctionRef<std::vector<TraverseHelper::TransitionKey>()> path)>;

  ObjectFinder(const DataBagImpl& databag, DataBagImpl::FallbackSpan fallbacks,
               absl::string_view ignore_attr_name_prefix = "")
      : traverse_helper_(databag, fallbacks),
        previsit_stack_(),
        used_items_(),
        ignore_attr_name_prefix_(ignore_attr_name_prefix) {}

  absl::Status TraverseSlice(const DataSliceImpl& ds, const DataItem& schema,
                             ObjectPathCallback callback) {
    RETURN_IF_ERROR(
        Previsit({.item = schema, .schema = DataItem(schema::kSchema)},
                 TraverseHelper::TransitionKey(
                     {.type = TraverseHelper::TransitionType::kSchema}),
                 callback));
    for (int64_t i = 0; i < ds.size(); ++i) {
      RETURN_IF_ERROR(Previsit(
          {.item = ds[i], .schema = schema},
          TraverseHelper::TransitionKey(
              {.type = TraverseHelper::TransitionType::kSliceItem, .index = i}),
          callback));
    }
    return DepthFirstPrevisitItemsAndSchemas(callback);
  }

  absl::Status TraverseSlice(const DataItem& item, const DataItem& schema,
                             ObjectPathCallback callback) {
    RETURN_IF_ERROR(
        Previsit({.item = schema, .schema = DataItem(schema::kSchema)},
                 TraverseHelper::TransitionKey(
                     {.type = TraverseHelper::TransitionType::kSchema}),
                 callback));
    RETURN_IF_ERROR(
        Previsit({.item = item, .schema = schema},
                 TraverseHelper::TransitionKey(
                     {.type = TraverseHelper::TransitionType::kSliceItem}),
                 callback));
    return DepthFirstPrevisitItemsAndSchemas(callback);
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

  absl::Status Previsit(const ItemWithSchema& item,
                        const TraverseHelper::TransitionKey& key,
                        ObjectPathCallback callback) {
    if (item.schema.is_primitive_schema()) {
      RETURN_IF_ERROR(ValidatePrimitiveType(item));
    }
    if (item.item.has_value() && !item.item.ContainsAnyPrimitives()) {
      auto [_, inserted] = used_items_.insert({item.item, item.schema});
      if (!inserted) {
        // If non-primitive item was already visited, we don't need to visit it
        // again or to call the callback on it.
        return absl::OkStatus();
      }
      previsit_stack_.push({item, key});
    }
    access_stack_.push_back(key);
    RETURN_IF_ERROR(callback(item.item, item.schema, [&]() {
      if (ignore_attr_name_prefix_.empty()) {
        return access_stack_;
      }
      return TraverseHelper::WithIgnoringPrefix(
          absl::MakeSpan(access_stack_), ignore_attr_name_prefix_);
    }));
    access_stack_.pop_back();
    return absl::OkStatus();
  }

  struct PairOfItemsHash {
    size_t operator()(const std::pair<DataItem, DataItem>& item) const {
      DCHECK(!item.first.ContainsAnyPrimitives());
      return absl::HashOf(DataItem::Hash()(item.first),
                          DataItem::Hash()(item.second));
    }
  };

  absl::Status DepthFirstPrevisitItemsAndSchemas(ObjectPathCallback callback) {
    // We do a databag graph traversal, with unrolled recursion. For that we
    // keep track of the stack size when we started visiting the current item.
    // If the stack would reach the same size again it would mean that visit of
    // the current item is done.

    while (!previsit_stack_.empty()) {
      auto& stack_top = previsit_stack_.top();
      ItemWithSchema item = stack_top.first;
      // By setting invalid schema, we mark that we already processed the item.
      // When it would become top of the stack again - we completed the
      // processing of the item.
      stack_top.first.schema = DataItem();
      access_stack_.push_back(stack_top.second);

      if (item.schema == schema::kObject) {
        ASSIGN_OR_RETURN(item.schema,
                         traverse_helper_.GetObjectSchema(item.item));
        RETURN_IF_ERROR(Previsit(
            {.item = item.schema, .schema = DataItem(schema::kSchema)},
            TraverseHelper::TransitionKey(
                {.type = TraverseHelper::TransitionType::kObjectSchema}),
            callback));
        RETURN_IF_ERROR(callback(item.item, item.schema, [&]() {
          if (ignore_attr_name_prefix_.empty()) {
            return access_stack_;
          }
          return TraverseHelper::WithIgnoringPrefix(
              absl::MakeSpan(access_stack_), ignore_attr_name_prefix_);
        }));
      } else if (item.item != DataItem(schema::kSchema) ||
                 item.schema != DataItem(schema::kSchema)) {
        RETURN_IF_ERROR(
            Previsit({.item = item.schema, .schema = DataItem(schema::kSchema)},
                     TraverseHelper::TransitionKey(
                         {.type = TraverseHelper::TransitionType::kSchema}),
                     callback));
      }
      ASSIGN_OR_RETURN(
          TraverseHelper::TransitionsSet transitions_set,
          traverse_helper_.GetTransitions(item.item, item.schema,
                                          /*remove_special_attrs=*/false));
      std::vector<TraverseHelper::TransitionKey> transition_keys =
          transitions_set.GetTransitionKeys();
      for (const auto& transition_key : transition_keys) {
        ASSIGN_OR_RETURN(auto transition, traverse_helper_.TransitionByKey(
                                              item.item, item.schema,
                                              transitions_set, transition_key));
        RETURN_IF_ERROR(Previsit({.item = std::move(transition.item),
                                  .schema = std::move(transition.schema)},
                                 transition_key, callback));
      }
      // Here we are removing from the access stack the transitions to the items
      // for which the processing is finished. These transitions should not be
      // on the access stack any longer.
      while (!previsit_stack_.empty() &&
             previsit_stack_.top().first.schema == DataItem()) {
        previsit_stack_.pop();
        access_stack_.pop_back();
      }
    }
    return absl::OkStatus();
  }

 private:
  TraverseHelper traverse_helper_;
  std::vector<TraverseHelper::TransitionKey> access_stack_;
  std::stack<std::pair<ItemWithSchema, TraverseHelper::TransitionKey>>
      previsit_stack_;
  absl::flat_hash_set<std::pair<DataItem, DataItem>, PairOfItemsHash>
      used_items_;
  std::string ignore_attr_name_prefix_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_OBJECT_FINDER_H_
