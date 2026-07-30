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
#include "koladata/contrib/flatten_cyclic_references.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stack>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
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
#include "koladata/internal/uuid_object.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::contrib {

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
  // Schemas should not be cloned via CloneItem. Explicit schemas keep their
  // original IDs, and implicit schemas derive IDs from their parent objects.
  DCHECK(!item.is_schema());
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
        times_in_current_access_path_(),
        previsit_stack_(),
        visited_schemas_() {}

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
  struct ClonedItemOnStack {
    ItemWithSchema original;
    DataItem cloned_item;
    bool is_visited = false;
  };

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
    // We do a databag graph traversal, with unrolled recursion.
    // We keep track of the access path to the current item as items in
    // `previsit_stack_` with `is_visited == true`.
    // We also count the number of times each item is accessed in
    // `times_in_current_access_path_`, so we can stop the recursion when the
    // repetition (depth) limit is reached.
    // Note, that we keep the old schemas whenever possible (we have to clone
    // the implicit schemas of the cloned objects). Thus, all schema attributes
    // are copied during the first visit, and we don't need to revisit the
    // schemas.

    while (!previsit_stack_.empty()) {
      auto& stack_top = previsit_stack_.top();
      ItemWithSchema original = stack_top.original;
      DataItem cloned_item = stack_top.cloned_item;
      // We mark that we already processed the item.
      // When it would become top of the stack again - we completed the
      // processing of the subtree under this item.
      stack_top.is_visited = true;
      auto [it, _] = times_in_current_access_path_.insert({original, 0});
      it->second += 1;
      DataItem cloned_schema = original.schema;
      if (it->second <= max_recursion_depth_ + 1) {
        if (original.schema == schema::kObject) {
          ASSIGN_OR_RETURN(original.schema,
                           traverse_helper_->GetObjectSchema(original.item));
          ASSIGN_OR_RETURN(cloned_schema,
                           GetClonedSchema(original.schema, cloned_item));
          RETURN_IF_ERROR(new_databag_->SetAttr(
              cloned_item, schema::kSchemaAttr, cloned_schema));
        } else if (original.schema.holds_value<internal::ObjectId>()) {
          // Entity with explicit schema. Push the schema onto the stack
          // to copy its attrs (including metadata) to the new databag.
          RETURN_IF_ERROR(VisitSchemaIfNeeded(original.schema));
        }
        ASSIGN_OR_RETURN(
            TraverseHelper::TransitionsSet transitions_set,
            traverse_helper_->GetTransitions(original.item, original.schema,
                                             /*remove_special_attrs=*/false));
        std::vector<TraverseHelper::TransitionKey> transition_keys =
            transitions_set.GetTransitionKeys();
        for (const auto& transition_key : transition_keys) {
          ASSIGN_OR_RETURN(auto transition,
                           traverse_helper_->TransitionByKey(
                               original.item, original.schema, transitions_set,
                               transition_key));
          if (transition_key.type ==
              TraverseHelper::TransitionType::kSchemaAttributeName) {
            // Schema attribute values (schema types, metadata ObjectIds,
            // schema names) should not be cloned. Pass the original values
            // directly to SaveTransition.
            RETURN_IF_ERROR(SaveTransition(
                {.item = cloned_item, .schema = cloned_schema}, transition_key,
                {.item = std::move(transition.item),
                 .schema = std::move(transition.schema)}));
          } else {
            ASSIGN_OR_RETURN(auto cloned_child_item,
                             Previsit({.item = std::move(transition.item),
                                       .schema = transition.schema}));
            RETURN_IF_ERROR(SaveTransition(
                {.item = cloned_item, .schema = cloned_schema}, transition_key,
                {.item = std::move(cloned_child_item),
                 .schema = std::move(transition.schema)}));
          }
        }
      }
      // Here we are removing from the access stack the transitions to the items
      // for which the processing is finished. These transitions should not be
      // on the access stack any longer.
      while (!previsit_stack_.empty() && previsit_stack_.top().is_visited) {
        times_in_current_access_path_[previsit_stack_.top().original] -= 1;
        previsit_stack_.pop();
      }
    }
    return absl::OkStatus();
  }

  // Returns the schema ID to use in the new databag for the given original
  // schema. Explicit schemas keep their original IDs. Implicit schemas get new
  // IDs derived from the cloned parent object.
  absl::StatusOr<DataItem> GetClonedSchema(const DataItem& schema,
                                           const DataItem& cloned_object) {
    if (schema.is_implicit_schema()) {
      ASSIGN_OR_RETURN(auto cloned_schema,
                       internal::CreateUuidWithMainObject<
                           internal::ObjectId::kUuidImplicitSchemaFlag>(
                           cloned_object, schema::kImplicitSchemaSeed));
      // Push the implicit schema onto the stack to copy its attrs.
      previsit_stack_.push(
          {.original = {.item = schema, .schema = DataItem(schema::kSchema)},
           .cloned_item = cloned_schema});
      return cloned_schema;
    }
    // Explicit schema: keep original ID, visit to copy attrs.
    RETURN_IF_ERROR(VisitSchemaIfNeeded(schema));
    return schema;
  }

  // Pushes an explicit schema onto the stack to copy its attrs to the new
  // databag. Only visits each schema once.
  absl::Status VisitSchemaIfNeeded(const DataItem& schema) {
    DCHECK(schema.holds_value<internal::ObjectId>());
    if (visited_schemas_.insert(schema).second) {
      previsit_stack_.push(
          {.original = {.item = schema, .schema = DataItem(schema::kSchema)},
           .cloned_item = schema});
    }
    return absl::OkStatus();
  }

  absl::StatusOr<DataItem> Previsit(const ItemWithSchema& item) {
    if (item.schema.is_primitive_schema()) {
      RETURN_IF_ERROR(ValidatePrimitiveType(item));
    }
    if (item.item.has_value() && !item.item.ContainsAnyPrimitives()) {
      auto it = times_in_current_access_path_.find(item);
      if (it != times_in_current_access_path_.end() &&
          it->second > max_recursion_depth_) {
        return DataItem();
      }
      if (item.schema == schema::kItemId) {
        return item.item;
      }
      if (item.item.is_schema()) {
        // Schemas are not cloned — keep original IDs. But ensure the schema's
        // attrs are copied to the new databag.
        if (!item.item.is_implicit_schema()) {
          RETURN_IF_ERROR(VisitSchemaIfNeeded(item.item));
        }
        return item.item;
      }
      DataItem cloned_item = CloneItem(item.item);
      previsit_stack_.push({.original = item, .cloned_item = cloned_item});
      return cloned_item;
    }
    return item.item;
  }

  absl::Status SaveAttributeTransition(ItemWithSchema item,
                                       absl::string_view attr_name,
                                       ItemWithSchema value) {
    RETURN_IF_ERROR(
        new_databag_->SetSchemaAttr(item.schema, attr_name, value.schema));
    RETURN_IF_ERROR(
        new_databag_->SetAttr(item.item, attr_name, std::move(value.item)));
    return absl::OkStatus();
  }

  absl::Status SaveMetadataTransition(DataItem schema, DataItem value) {
    // Metadata ObjectId is derived from the schema's ObjectId.
    ASSIGN_OR_RETURN(
        auto metadata_id,
        internal::CreateUuidWithMainObject(
            schema, schema::kMetadataSeed));
    RETURN_IF_ERROR(
        new_databag_->SetSchemaAttr(schema, schema::kSchemaMetadataAttr,
                                    metadata_id));
    // Push the metadata object onto the stack to copy its attrs.
    if (value.has_value()) {
      previsit_stack_.push({.original = {.item = std::move(value),
                                         .schema = DataItem(schema::kObject)},
                            .cloned_item = std::move(metadata_id)});
    }
    return absl::OkStatus();
  }

  absl::Status SaveSchemaAttributeTransition(DataItem schema,
                                             absl::string_view attr_name,
                                             DataItem attr_schema) {
    RETURN_IF_ERROR(
        new_databag_->SetSchemaAttr(schema, attr_name, attr_schema));
    if (attr_schema.holds_value<internal::ObjectId>() &&
        attr_schema.is_schema() && !attr_schema.is_implicit_schema()) {
      RETURN_IF_ERROR(VisitSchemaIfNeeded(attr_schema));
    }
    return absl::OkStatus();
  }

  absl::Status SaveListItemTransition(ItemWithSchema item,
                                      TraverseHelper::TransitionKey key,
                                      ItemWithSchema value) {
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
    return absl::OkStatus();
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
      if (attr_name == schema::kSchemaMetadataAttr) {
        // Skip metadata here — it's a schema-level attribute that appears as
        // kAttributeName on entities (via EntityTransitions), but is properly
        // handled via kSchemaAttributeName when the schema is visited.
        return absl::OkStatus();
      }
      return SaveAttributeTransition(std::move(item), attr_name,
                                     std::move(value));
    } else if (key.type ==
               TraverseHelper::TransitionType::kSchemaAttributeName) {
      DCHECK_EQ(item.schema, schema::kSchema);
      DCHECK(key.value.holds_value<arolla::Text>());
      auto attr_name = key.value.value<arolla::Text>();
      if (attr_name == schema::kSchemaMetadataAttr) {
        DCHECK_EQ(value.schema, schema::kObject);
        return SaveMetadataTransition(std::move(item.item),
                                      std::move(value.item));
      }
      return SaveSchemaAttributeTransition(std::move(item.item), attr_name,
                                           std::move(value.item));
    } else if (key.type == TraverseHelper::TransitionType::kListItem) {
      return SaveListItemTransition(std::move(item), std::move(key),
                                    std::move(value));
    } else if (key.type == TraverseHelper::TransitionType::kDictValue) {
      RETURN_IF_ERROR(new_databag_->SetSchemaAttr(
          item.schema, schema::kDictValuesSchemaAttr, value.schema));
      RETURN_IF_ERROR(
          new_databag_->SetInDict(item.item, key.value, value.item));
      return absl::OkStatus();
    } else if (key.type == TraverseHelper::TransitionType::kListNoItems) {
      RETURN_IF_ERROR(new_databag_->SetSchemaAttr(
          item.schema, schema::kListItemsSchemaAttr, value.schema));
    } else if (key.type == TraverseHelper::TransitionType::kDictNoKeys) {
      RETURN_IF_ERROR(new_databag_->SetSchemaAttr(
          item.schema, schema::kDictKeysSchemaAttr, value.schema));
    } else if (key.type == TraverseHelper::TransitionType::kDictNoValues) {
      RETURN_IF_ERROR(new_databag_->SetSchemaAttr(
          item.schema, schema::kDictValuesSchemaAttr, value.schema));
    } else {
      return absl::InternalError(
          absl::StrCat("unsupported transition type ", key.type));
    }
    // Ensure that any entity schema referenced as value.schema has its attrs
    // copied to the new databag. This handles cases like empty lists with
    // entity item schemas.
    if (value.schema.holds_value<internal::ObjectId>() &&
        value.schema.is_schema() && !value.schema.is_implicit_schema()) {
      RETURN_IF_ERROR(VisitSchemaIfNeeded(value.schema));
    }
    return absl::OkStatus();
  }

  DataBagImpl* new_databag_;
  std::optional<TraverseHelper> traverse_helper_;
  int64_t max_recursion_depth_;
  absl::flat_hash_map<ItemWithSchema, int64_t> times_in_current_access_path_;
  // Stack of items to visit. Each item is paired with a boolean indicating
  // whether it has been visited yet, or if it kept on stack after visiting to
  // track when the subtree rooted at the item is fully visited.
  std::stack<ClonedItemOnStack> previsit_stack_;
  // Tracks explicit schemas that have already been visited to avoid
  // copying their attrs multiple times.
  absl::flat_hash_set<DataItem, DataItem::Hash> visited_schemas_;
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
  if (schema_impl == schema::kSchema) {
    return absl::InvalidArgumentError(
        "cannot flatten cyclic references for a DataSlice of schemas");
  }
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  return ds.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    auto result_db = DataBag::EmptyMutable();
    ASSIGN_OR_RETURN(auto& result_db_impl, result_db->GetMutableImpl());
    FlattenCyclicReferencesOp flatten_cyclic_references_op(&result_db_impl,
                                                           max_recursion_depth);
    ASSIGN_OR_RETURN(auto result_slice_impl,
                     flatten_cyclic_references_op(
                         impl, schema_impl, db->GetImpl(), fallbacks_span));
    result_db->UnsafeMakeImmutable();
    return DataSlice::Create(std::move(result_slice_impl), ds.GetShape(),
                             schema_impl, std::move(result_db));
  });
}

}  // namespace koladata::contrib
