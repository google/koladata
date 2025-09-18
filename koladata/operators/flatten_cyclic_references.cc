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
#include "koladata/operators/flatten_cyclic_references.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stack>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/jagged_shape/dense_array/util/concat.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/non_deterministic_token.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/slice_builder.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::ops {

namespace {

using internal::DataBagImpl;
using internal::DataItem;
using internal::DataSliceImpl;
using internal::TraverseHelper;

struct ItemWithSchema {
  DataItem item;
  DataItem schema;

  bool operator==(const ItemWithSchema& other) const {
    auto equal_or_nan = [](const DataItem& a, const DataItem& b) {
      return (a.is_nan() && b.is_nan()) || a == b;
    };
    return equal_or_nan(item, other.item) && schema == other.schema;
  }

  struct Hash {
    using is_transparent = void;

    size_t operator()(const ItemWithSchema& item) const {
      return absl::HashOf(DataItem::Hash()(item.item),
                          DataItem::Hash()(item.schema));
    }
  };

  using absl_container_hash = Hash;
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

DataItem CloneItem(const DataItem& item) {
  if (item.is_list()) {
    return DataItem(internal::AllocateSingleList());
  }
  if (item.is_dict()) {
    return DataItem(internal::AllocateSingleDict());
  }
  if (item.is_entity()) {
    return DataItem(internal::AllocateSingleObject());
  }
  if (item.is_schema()) {
    return DataItem(internal::AllocateExplicitSchema());
  }
  return item;
}

// Converts data, reachable from the given slice into a rooted tree.
//
// Traversal is bounded by the `max_recursion_depth`, which indicates the
// maximum number of times the same item can be encountered during traversal.
// Note that the size of the resulting tree may be exponential of the original
// data size.
//
// The result is stored in the `new_databag`.
class FlattenCyclicReferencesOp {
 public:
  explicit FlattenCyclicReferencesOp(DataBagImpl* new_databag,
                                     int64_t max_recursion_depth)
      : new_databag_(new_databag),
        traverse_helper_(std::nullopt),
        max_recursion_depth_(max_recursion_depth),
        access_stack_counts_(),
        previsit_stack_() {}

  absl::StatusOr<DataSliceImpl> operator()(
      const DataSliceImpl& ds, const DataItem& schema,
      const DataBagImpl& databag, DataBagImpl::FallbackSpan fallbacks = {}) {
    traverse_helper_.emplace(databag, fallbacks);
    return TraverseSlice(ds, schema);
  }

  absl::StatusOr<DataItem> operator()(
      const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
      DataBagImpl::FallbackSpan fallbacks = {}) {
    ASSIGN_OR_RETURN(auto result_slice,
                     (*this)(DataSliceImpl::Create(/*size=*/1, item), schema,
                             databag, fallbacks));
    DCHECK_EQ(result_slice.size(), 1);
    return result_slice[0];
  }

 private:
  absl::StatusOr<DataSliceImpl> TraverseSlice(const DataSliceImpl& ds,
                                              const DataItem& schema) {
    internal::SliceBuilder result_builder(ds.size());
    for (int64_t i = 0; i < ds.size(); ++i) {
      ASSIGN_OR_RETURN(auto result_item,
                       Previsit({.item = ds[i], .schema = schema}));
      result_builder.InsertIfNotSetAndUpdateAllocIds(i, result_item);
    }
    RETURN_IF_ERROR(DepthFirstPrevisitItemsAndSchemas());
    return std::move(result_builder).Build();
  }

  absl::Status DepthFirstPrevisitItemsAndSchemas() {
    // We do a databag graph traversal, with unrolled recursion. For that we
    // keep track of the stack size when we started visiting the current item.
    // If the stack would reach the same size again it would mean that visit of
    // the current item is done.

    while (!previsit_stack_.empty()) {
      auto& stack_top = previsit_stack_.top();
      ItemWithSchema item = stack_top.first;
      ItemWithSchema cloned_item = stack_top.second;
      // We mark that we already processed the item.
      // When it would become top of the stack again - we completed the
      // processing of the item.
      stack_top.second.schema = DataItem();
      auto [it, _] = access_stack_counts_.insert({item, 0});
      it->second += 1;
      if (it->second <= max_recursion_depth_ + 1) {
        if (item.schema == schema::kObject) {
          ASSIGN_OR_RETURN(item.schema,
                           traverse_helper_->GetObjectSchema(item.item));
          ASSIGN_OR_RETURN(auto cloned_schema,
                           Previsit({.item = item.schema,
                                     .schema = DataItem(schema::kSchema)}));
          RETURN_IF_ERROR(new_databag_->SetAttr(
              cloned_item.item, schema::kSchemaAttr, cloned_schema));
          cloned_item.schema = std::move(cloned_schema);
        }
        ASSIGN_OR_RETURN(
            TraverseHelper::TransitionsSet transitions_set,
            traverse_helper_->GetTransitions(item.item, item.schema,
                                             /*remove_special_attrs=*/false));
        std::vector<TraverseHelper::TransitionKey> transition_keys =
            transitions_set.GetTransitionKeys();
        for (const auto& transition_key : transition_keys) {
          ASSIGN_OR_RETURN(
              auto transition,
              traverse_helper_->TransitionByKey(
                  item.item, item.schema, transitions_set, transition_key));
          ASSIGN_OR_RETURN(auto cloned_child_item,
                           Previsit({.item = std::move(transition.item),
                                     .schema = std::move(transition.schema)}));
          RETURN_IF_ERROR(
              SaveTransition(cloned_item, transition_key,
                             {.item = std::move(cloned_child_item),
                              .schema = std::move(transition.schema)}));
        }
      }
      // Here we are removing from the access stack the transitions to the items
      // for which the processing is finished. These transitions should not be
      // on the access stack any longer.
      while (!previsit_stack_.empty() &&
             previsit_stack_.top().second.schema == DataItem()) {
        access_stack_counts_[previsit_stack_.top().first] -= 1;
        previsit_stack_.pop();
      }
    }
    return absl::OkStatus();
  }

  absl::StatusOr<DataItem> Previsit(const ItemWithSchema& item) {
    if (item.schema.is_primitive_schema()) {
      RETURN_IF_ERROR(ValidatePrimitiveType(item));
    }
    if (item.item.has_value() && !item.item.ContainsAnyPrimitives()) {
      auto it = access_stack_counts_.find(item);
      if (it != access_stack_counts_.end() &&
          it->second > max_recursion_depth_) {
        return DataItem();
      }
      DataItem cloned_item = CloneItem(item.item);
      previsit_stack_.push(
          {item, {.item = cloned_item, .schema = item.schema}});
      return cloned_item;
    }
    return item.item;
  }

  absl::Status SaveTransition(ItemWithSchema item,
                              TraverseHelper::TransitionKey key,
                              ItemWithSchema value) {
    if (key.type == TraverseHelper::TransitionType::kDictKey) {
      // We process dict keys when we process dict values.
      if (value.item.holds_value<internal::ObjectId>()) {
        return absl::InvalidArgumentError(
            "non-primitives dict keys are not supported");
      }
      return new_databag_->SetSchemaAttr(
          item.schema, schema::kDictKeysSchemaAttr, value.schema);
    }
    if (key.type == TraverseHelper::TransitionType::kAttributeName) {
      DCHECK(key.value.holds_value<arolla::Text>());
      auto attr_name = key.value.value<arolla::Text>();
      RETURN_IF_ERROR(
          new_databag_->SetSchemaAttr(item.schema, attr_name, value.schema));
      RETURN_IF_ERROR(
          new_databag_->SetAttr(item.item, attr_name, std::move(value.item)));
      return absl::OkStatus();
    } else if (key.type ==
               TraverseHelper::TransitionType::kSchemaAttributeName) {
      DCHECK(key.value.holds_value<arolla::Text>());
      auto attr_name = key.value.value<arolla::Text>();
      RETURN_IF_ERROR(
          new_databag_->SetSchemaAttr(item.item, attr_name, value.item));
    } else if (key.type == TraverseHelper::TransitionType::kListItem) {
      RETURN_IF_ERROR(new_databag_->SetSchemaAttr(
          item.schema, schema::kListItemsSchemaAttr, value.schema));
      ASSIGN_OR_RETURN(auto list_size, new_databag_->GetListSize(item.item));
      if (list_size == DataItem()) {
        list_size = DataItem(0);
      }
      auto extend_size =
          std::max(int64_t{0}, key.index + 1 - list_size.value<int64_t>());
      RETURN_IF_ERROR(new_databag_->ExtendList(
          item.item, DataSliceImpl::CreateEmptyAndUnknownType(extend_size)));
      RETURN_IF_ERROR(
          new_databag_->SetInList(item.item, key.index, std::move(value.item)));
    } else if (key.type == TraverseHelper::TransitionType::kDictValue) {
      RETURN_IF_ERROR(new_databag_->SetSchemaAttr(
          item.schema, schema::kDictValuesSchemaAttr, value.schema));
      RETURN_IF_ERROR(
          new_databag_->SetInDict(item.item, key.value, value.item));
    } else {
      return absl::InternalError("unsupported transition type");
    }
    return absl::OkStatus();
  }

  DataBagImpl* new_databag_;
  std::optional<TraverseHelper> traverse_helper_;
  int64_t max_recursion_depth_;
  absl::flat_hash_map<ItemWithSchema, int64_t> access_stack_counts_;
  std::stack<std::pair<ItemWithSchema, ItemWithSchema>> previsit_stack_;
};

}  // namespace

absl::StatusOr<DataSlice> FlattenCyclicReferences(
    const DataSlice& ds, int64_t max_recursion_depth,
    internal::NonDeterministicToken) {
  const auto& db = ds.GetBag();
  if (db == nullptr) {
    return absl::InvalidArgumentError("cannot unroll without a DataBag");
  }
  auto schema = ds.GetSchema();
  const auto& schema_impl = schema.impl<DataItem>();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  return ds.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    auto result_db = DataBag::EmptyMutable();
    ASSIGN_OR_RETURN(auto result_db_impl, result_db->GetMutableImpl());
    FlattenCyclicReferencesOp flatten_cyclic_references_op(
        &result_db_impl.get(), max_recursion_depth);
    ASSIGN_OR_RETURN(auto result_slice_impl,
                     flatten_cyclic_references_op(
                         impl, schema_impl, db->GetImpl(), fallbacks_span));
    result_db->UnsafeMakeImmutable();
    return DataSlice::Create(std::move(result_slice_impl), ds.GetShape(),
                             schema_impl, std::move(result_db));
  });
}


}  // namespace koladata::ops
