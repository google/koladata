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
#include "koladata/internal/op_utils/deep_clone.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
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
#include "koladata/internal/op_utils/traverser.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/uuid_object.h"
#include "koladata/internal/uuid_schemas.h"

namespace koladata::internal {

namespace {

// Visitor used in a secondary Traverser to resolve derived list and dict schema
// ObjectIds in topological post-order.
class DerivedIdVisitor : public AbstractVisitor {
 public:
  explicit DerivedIdVisitor(
      absl::flat_hash_map<AllocationId, AllocationId>& allocation_tracker)
      : allocation_tracker_(allocation_tracker) {}

  absl::StatusOr<bool> Previsit(
      const DataItem& from_item, const DataItem& from_schema,
      const std::optional<AbstractVisitor::TransitionKey>& transition_key,
      const DataItem& item, const DataItem& schema) override {
    if (schema != schema::kSchema) {
      return false;
    }
    if (transition_key.has_value() &&
        transition_key->type == TransitionType::kSliceItem) {
      return true;
    }
    if (transition_key.has_value() &&
        transition_key->type == TransitionType::kSchemaAttributeName) {
      DCHECK(transition_key->value.holds_value<arolla::Text>());
      absl::string_view attr_name =
          transition_key->value.value<arolla::Text>().view();
      if (attr_name == schema::kListItemsSchemaAttr ||
          attr_name == schema::kDictKeysSchemaAttr ||
          attr_name == schema::kDictValuesSchemaAttr) {
        return true;
      }
    }
    return false;
  }

  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) override {
    if (!item.holds_value<ObjectId>() || schema != schema::kSchema) {
      return item;
    }
    if (item.value<ObjectId>().IsNoFollowSchema()) {
      ASSIGN_OR_RETURN(
          auto original_item_clone,
          GetValue(
              DataItem(GetOriginalFromNoFollow(item.value<ObjectId>())),
              schema));
      return DataItem(
          CreateNoFollowWithMainObject(original_item_clone.value<ObjectId>()));
    }
    auto item_it =
        allocation_tracker_.find(AllocationId(item.value<ObjectId>()));
    if (item_it != allocation_tracker_.end() &&
        item_it->second != AllocationId()) {
      return DataItem(
          item_it->second.ObjectByOffset(item.value<ObjectId>().Offset()));
    }
    return item;
  }

  absl::Status VisitSchema(
      const DataItem& item, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_schemas) override {
    if (!item.holds_value<ObjectId>()) {
      return absl::OkStatus();
    }
    DataItem orig_item =
        item.value<ObjectId>().IsNoFollowSchema()
            ? DataItem(GetOriginalFromNoFollow(item.value<ObjectId>()))
            : item;
    AllocationId old_alloc = AllocationId(orig_item.value<ObjectId>());

    std::optional<DataItem> list_items_schema;
    std::optional<DataItem> dict_keys_schema;
    std::optional<DataItem> dict_values_schema;

    for (size_t i = 0; i < attr_names.size(); ++i) {
      if (!attr_names.present(i) || !attr_schemas.present(i)) {
        continue;
      }
      absl::string_view attr_name = attr_names[i].value;
      if (attr_name == schema::kListItemsSchemaAttr) {
        list_items_schema = attr_schemas.values[i];
      } else if (attr_name == schema::kDictKeysSchemaAttr) {
        dict_keys_schema = attr_schemas.values[i];
      } else if (attr_name == schema::kDictValuesSchemaAttr) {
        dict_values_schema = attr_schemas.values[i];
      }
    }

    if (list_items_schema.has_value()) {
      DataItem new_schema_item = CreateListSchemaId(*list_items_schema);
      allocation_tracker_[old_alloc] =
          AllocationId(new_schema_item.value<ObjectId>());
    } else if (dict_keys_schema.has_value() && dict_values_schema.has_value()) {
      DataItem new_schema_item =
          CreateDictSchemaId(*dict_keys_schema, *dict_values_schema);
      allocation_tracker_[old_alloc] =
          AllocationId(new_schema_item.value<ObjectId>());
    }
    return absl::OkStatus();
  }

  absl::Status VisitList(const DataItem&, const DataItem&, bool,
                         const DataSliceImpl&) override {
    return absl::OkStatus();
  }

  absl::Status VisitDict(const DataItem&, const DataItem&, bool,
                         const DataSliceImpl&, const DataSliceImpl&) override {
    return absl::OkStatus();
  }

  absl::Status VisitObject(
      const DataItem&, const DataItem&, bool,
      const arolla::DenseArray<arolla::Text>&,
      const arolla::DenseArray<DataItem>&) override {
    return absl::OkStatus();
  }

 private:
  absl::flat_hash_map<AllocationId, AllocationId>& allocation_tracker_;
};

class DeepCloneVisitor : AbstractVisitor {
 public:
  explicit DeepCloneVisitor(DataBagImplPtr new_databag, bool is_schema_slice,
                            const DataBagImpl& databag,
                            DataBagImpl::FallbackSpan fallbacks)
      : new_databag_(std::move(new_databag)),
        is_schema_slice_(is_schema_slice),
        derived_id_traverser_(
            databag, fallbacks,
            std::make_shared<DerivedIdVisitor>(allocation_tracker_)) {}

  absl::StatusOr<bool> Previsit(
      const DataItem& from_item, const DataItem& from_schema,
      const std::optional<AbstractVisitor::TransitionKey>& transition_key,
      const DataItem& item, const DataItem& schema) override {
    if (schema == schema::kSchema) {
      if (is_schema_slice_ && from_schema == schema::kSchema &&
          transition_key.has_value() &&
          transition_key->type == TransitionType::kSchemaAttributeName) {
        DCHECK(transition_key->value.holds_value<arolla::Text>());
        absl::string_view attr_name =
            transition_key->value.value<arolla::Text>().view();
        if (attr_name == schema::kListItemsSchemaAttr ||
            attr_name == schema::kDictKeysSchemaAttr) {
          schemas_with_derived_ids_.push_back(from_item);
        }
      }
      return PrevisitSchema(item);
    }

    if (schema == schema::kObject && from_schema == schema::kSchema) {
      // The `item` is schema_metadata for `from_item`.
      RETURN_IF_ERROR(PrevisitSchemaMetadata(from_item, item));
      return true;
    }
    if (schema.holds_value<ObjectId>()) {
      // Entity schema.
      if (schema.is_implicit_schema()) {
        // The item was already previsited with `schema::kObject`, thus we only
        // need to "clone" the implicit schema.
        RETURN_IF_ERROR(PrevisitItemWithImplicitSchema(item, schema));
        return true;
      }
      RETURN_IF_ERROR(PrevisitObject(item));
      return true;
    } else if (schema.holds_value<schema::DType>()) {
      if (schema == schema::kObject) {
        RETURN_IF_ERROR(PrevisitObject(item));
        return true;
      }
      return true;
    }
    return absl::InternalError("unsupported schema type");
  }

  // Reassigns metadata ids for all allocations that have metadata.
  //
  // The metadata ids are derived from the parent schema ids. Thus if the
  // schemas are cloned, the metadata ids should also be updated.
  //
  // Note, that for explicit schemas, new ids are also derived from the cloned
  // object ids. And we can have a long chains of implicit schemas and metadata
  // objects, where each next object id is derived from the previous one.
  //
  // To handle this, we first find the set of schemas that have metadata, but
  // which ids are not derived from the metadata objects. These schemas are the
  // starting points of the chains of dependent ids.
  //
  // Then we go through all these starting schemas, and for each of them we
  // create a chain of new ids metadata objects and implicit schemas.
  absl::Status AssignMetadataIds() {
    if (allocations_with_metadata_.empty()) {
      return absl::OkStatus();
    }
    std::vector<AllocationId> derived_allocations;
    for (const AllocationId& schema_allocation :
         allocations_with_metadata_) {
      ASSIGN_OR_RETURN(DataItem metadata,
                       CreateUuidWithMainObject(
                           DataItem(schema_allocation.ObjectByOffset(0)),
                           schema::kMetadataSeed));
      ASSIGN_OR_RETURN(
          DataItem next_schema,
          CreateUuidWithMainObject<internal::ObjectId::kUuidImplicitSchemaFlag>(
              metadata, schema::kImplicitSchemaSeed));
      auto next_allocation = AllocationId(next_schema.value<ObjectId>());
      if (allocations_with_metadata_.contains(next_allocation)) {
        derived_allocations.push_back(next_allocation);
      }
    }
    // Remove allocations that are derived from the others.
    for (const AllocationId& schema_allocation : derived_allocations) {
      allocations_with_metadata_.erase(schema_allocation);
    }
    for (AllocationId schema_allocation : allocations_with_metadata_) {
      // For each starting schema allocation, we create a chain of alternating
      // metadata and implicit schema allocations.
      ASSIGN_OR_RETURN(
          DataItem cloned_schema_starting_chain,
          GetValueImpl(DataItem(schema_allocation.ObjectByOffset(0)),
                       DataItem(schema::kSchema)));
      AllocationId cloned_schema_allocation =
          AllocationId(cloned_schema_starting_chain.value<ObjectId>());
      while (true) {
        allocation_tracker_[schema_allocation] = cloned_schema_allocation;
        ASSIGN_OR_RETURN(DataItem metadata,
                         CreateUuidWithMainObject(
                             DataItem(schema_allocation.ObjectByOffset(0)),
                             schema::kMetadataSeed));
        ASSIGN_OR_RETURN(
            DataItem cloned_metadata,
            CreateUuidWithMainObject(
                DataItem(cloned_schema_allocation.ObjectByOffset(0)),
                schema::kMetadataSeed));
        allocation_tracker_[AllocationId(metadata.value<ObjectId>())] =
            AllocationId(cloned_metadata.value<ObjectId>());
        ASSIGN_OR_RETURN(DataItem next_schema,
                         CreateUuidWithMainObject<
                             internal::ObjectId::kUuidImplicitSchemaFlag>(
                             metadata, schema::kImplicitSchemaSeed));
        schema_allocation = AllocationId(next_schema.value<ObjectId>());
        auto it = allocation_tracker_.find(schema_allocation);
        if (it == allocation_tracker_.end()) {
          break;
        }
        ASSIGN_OR_RETURN(DataItem next_cloned_schema,
                         CreateUuidWithMainObject<
                             internal::ObjectId::kUuidImplicitSchemaFlag>(
                             cloned_metadata, schema::kImplicitSchemaSeed));
        cloned_schema_allocation =
            AllocationId(next_cloned_schema.value<ObjectId>());
        it->second = cloned_schema_allocation;
      }
    }
    allocations_with_metadata_.clear();
    return absl::OkStatus();
  }

  absl::StatusOr<DataItem> GetValue(const DataItem& item,
                                    const DataItem& schema) override {
    if (!resolved_derived_ids_) {
      // On first GetValue or Visit* call, we reassign metadata and list/dict
      // schema ids.
      //
      // GetValue is called only after all Previsits are done. And we call
      // GetValue (and not GetValueImpl) from each of the Visit* methods, thus
      // ensuring that we would reassign derived ids once, after all Previsits
      // are done, and before we start using cloned ids to store in the new
      // databag.
      RETURN_IF_ERROR(ResolveDerivedObjectIds());
      resolved_derived_ids_ = true;
    }
    return GetValueImpl(item, schema);
  }

  absl::Status VisitList(const DataItem& list, const DataItem& schema,
                         bool is_object_schema,
                         const DataSliceImpl& items) override {
    DCHECK(list.holds_value<ObjectId>() && list.value<ObjectId>().IsList());
    ASSIGN_OR_RETURN(auto new_list, GetValue(list, schema));
    if (is_object_schema) {
      RETURN_IF_ERROR(SetSchemaAttr(new_list, schema));
    }
    ASSIGN_OR_RETURN(auto list_size, new_databag_->GetListSize(new_list));
    DCHECK(list_size.holds_value<int64_t>());
    if (list_size.value<int64_t>() != 0) {
      if (items.size() != list_size.value<int64_t>()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "Different numbers of items provided for the list %v: %d vs %d",
            list, list_size.value<int64_t>(), items.size()));
      }
      return absl::OkStatus();
    }
    RETURN_IF_ERROR(new_databag_->ExtendList(new_list, items));
    return absl::OkStatus();
  }

  absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                         bool is_object_schema, const DataSliceImpl& keys,
                         const DataSliceImpl& values) override {
    DCHECK(dict.holds_value<ObjectId>() && dict.value<ObjectId>().IsDict());
    DCHECK_EQ(keys.size(), values.size());
    ASSIGN_OR_RETURN(auto new_dict, GetValue(dict, schema));
    if (is_object_schema) {
      RETURN_IF_ERROR(SetSchemaAttr(new_dict, schema));
    }
    RETURN_IF_ERROR(new_databag_->SetInDict(
        DataSliceImpl::Create(keys.size(), new_dict), keys, values));
    return absl::OkStatus();
  }

  absl::Status VisitObject(
      const DataItem& object, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_values) override {
    DCHECK(object.holds_value<ObjectId>());
    ASSIGN_OR_RETURN(auto new_object, GetValue(object, schema));
    if (is_object_schema) {
      RETURN_IF_ERROR(SetSchemaAttr(new_object, schema));
    }
    DCHECK_EQ(attr_names.size(), attr_values.size());
    DCHECK(attr_names.IsAllPresent());
    for (size_t i = 0; i < attr_names.size(); ++i) {
      if (attr_values.present(i)) {
        auto attr_name = attr_names[i].value;
        const DataItem& value = attr_values[i].value;
        if (schema == schema::kSchema) {
          if (object != new_object && attr_name == schema::kSchemaNameAttr) {
            continue;
          }
          RETURN_IF_ERROR(
              new_databag_->SetSchemaAttr(new_object, attr_name, value));
        } else {
          RETURN_IF_ERROR(new_databag_->SetAttr(new_object, attr_name, value));
        }
      }
    }
    return absl::OkStatus();
  }

  absl::Status VisitSchema(
      const DataItem& item, const DataItem& schema, bool is_object_schema,
      const arolla::DenseArray<arolla::Text>& attr_names,
      const arolla::DenseArray<DataItem>& attr_schema) override {
    return VisitObject(item, schema, is_object_schema, attr_names, attr_schema);
  }

  std::vector<ObjectId> get_explicit_schemas() { return explicit_schemas_; }

 private:
  DataItem GetValueFromTrackedAllocation(const DataItem& item) {
    DCHECK(item.holds_value<ObjectId>());
    auto item_it =
        allocation_tracker_.find(AllocationId(item.value<ObjectId>()));
    if (item_it == allocation_tracker_.end()) {
      return DataItem();
    }
    return DataItem(
        item_it->second.ObjectByOffset(item.value<ObjectId>().Offset()));
  }

  absl::StatusOr<DataItem> GetValueImpl(const DataItem& item,
                                        const DataItem& schema) {
    if (!item.holds_value<ObjectId>() || schema == schema::kItemId) {
      return item;
    }
    if (item.is_schema() && !is_schema_slice_ && !item.is_implicit_schema()) {
      // We keep explicit schemas as is, unless we `deep_clone` a schema slice.
      // However, we keep implicit schemas in sync with parent objects.
      return item;
    }
    if (item.value<ObjectId>().IsNoFollowSchema()) {
      ASSIGN_OR_RETURN(
          auto original_item_clone,
          GetValueImpl(
              DataItem(GetOriginalFromNoFollow(item.value<ObjectId>())),
              schema));
      return DataItem(
          CreateNoFollowWithMainObject(original_item_clone.value<ObjectId>()));
    }
    DataItem new_item = GetValueFromTrackedAllocation(item);
    if (new_item.has_value()) {
      return std::move(new_item);
    }
    if (item.is_implicit_schema()) {
      // No object with implicit schema in `item`'s AllocationId was cloned.
      // Thus, we cannot determine new AllocationId for the implicit schemas
      // and create an ExplicitSchemaAllocationId instead.
      RETURN_IF_ERROR(CloneAsExplicitSchema(item));
      return GetValueImpl(item, schema);
    }
    return absl::InvalidArgumentError(
        absl::StrFormat("new allocation for object %v is not found", item));
  }

  absl::Status PrevisitObject(const DataItem& item) {
    if (!item.holds_value<ObjectId>()) {
      return absl::OkStatus();
    }
    const auto allocation_id = AllocationId(item.value<ObjectId>());
    if (auto [it, inserted] =
            allocation_tracker_.emplace(allocation_id, AllocationId());
        inserted) {
      it->second = NewAllocationIdLike(allocation_id);
    }
    return absl::OkStatus();
  }

  absl::Status PrevisitSchemaMetadata(const DataItem& from_item,
                                      const DataItem& item) {
    DCHECK(item.holds_value<ObjectId>());
    DCHECK(from_item.holds_value<ObjectId>());
    allocation_tracker_.emplace(AllocationId(item.value<ObjectId>()),
                                AllocationId());
    allocations_with_metadata_.insert(
        AllocationId(from_item.value<ObjectId>()));
    return absl::OkStatus();
  }

  absl::Status PrevisitItemWithImplicitSchema(const DataItem& item,
                                              const DataItem& schema) {
    DCHECK(schema.is_implicit_schema());
    auto [alloc_it, inserted] = allocation_tracker_.emplace(
        AllocationId(schema.value<ObjectId>()), AllocationId());
    if (!inserted) {
      return absl::OkStatus();
    }
    // Don't call `GetValueImpl` here, because it may invalidate `alloc_it` by
    // `allocation_tracker_` updates.
    auto new_item = GetValueFromTrackedAllocation(item);
    if (!new_item.has_value()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("new allocation for object %v is not found", item));
    }
    ASSIGN_OR_RETURN(
        auto new_schema,
        CreateUuidWithMainObject<internal::ObjectId::kUuidImplicitSchemaFlag>(
            new_item, schema::kImplicitSchemaSeed));
    alloc_it->second = AllocationId(new_schema.value<ObjectId>());
    return absl::OkStatus();
  }

  // Returns true if the schema should be traversed further.
  absl::StatusOr<bool> PrevisitSchema(const DataItem& schema) {
    if (!schema.holds_value<ObjectId>() || schema.is_implicit_schema()) {
      // For implicit schemas we create a "clone" when encounter them in
      // `schema::kSchemaAttr`, or in GetValue after all Previsits are done.
      return true;
    }
    if (is_schema_slice_) {
      // If deep_clone is called on a schema slice, we clone explicit schemas.
      if (schema.value<ObjectId>().IsNoFollowSchema()) {
        RETURN_IF_ERROR(CloneAsExplicitSchema(
            DataItem(GetOriginalFromNoFollow(schema.value<ObjectId>()))));
      } else {
        RETURN_IF_ERROR(CloneAsExplicitSchema(schema));
      }
      return true;
    } else {
      // We stop the traversal on explicit schemas. Instead, we will extract
      // them later.
      explicit_schemas_.push_back(schema.value<ObjectId>());
      return false;
    }
  }

  absl::Status CloneAsExplicitSchema(const DataItem& schema) {
    const auto allocation_id = AllocationId(schema.value<ObjectId>());
    auto [alloc_it, inserted] =
        allocation_tracker_.emplace(allocation_id, AllocationId());
    if (!inserted) {
      return absl::OkStatus();
    }
    alloc_it->second = AllocateExplicitSchemas(allocation_id.Capacity());
    return absl::OkStatus();
  }

  absl::Status ResolveListDictSchemaIds() {
    if (schemas_with_derived_ids_.empty()) {
      return absl::OkStatus();
    }
    auto schemas_slice = DataSliceImpl::Create(
        arolla::CreateFullDenseArray<DataItem>(schemas_with_derived_ids_));
    RETURN_IF_ERROR(derived_id_traverser_.TraverseSlice(
        schemas_slice, DataItem(schema::kSchema)));
    schemas_with_derived_ids_.clear();
    return absl::OkStatus();
  }

  absl::Status ResolveDerivedObjectIds() {
    RETURN_IF_ERROR(ResolveListDictSchemaIds());
    RETURN_IF_ERROR(AssignMetadataIds());
    return absl::OkStatus();
  }

  absl::Status SetSchemaAttr(const DataItem& item, const DataItem& schema) {
    ASSIGN_OR_RETURN(auto explicit_schema_value,
                     GetValue(schema, DataItem(schema::kSchema)));
    RETURN_IF_ERROR(new_databag_->SetAttr(item, schema::kSchemaAttr,
                                          std::move(explicit_schema_value)));
    return absl::OkStatus();
  }

  DataBagImplPtr new_databag_;
  bool is_schema_slice_;
  absl::flat_hash_map<AllocationId, AllocationId> allocation_tracker_;
  Traverser<DerivedIdVisitor> derived_id_traverser_;
  absl::flat_hash_set<AllocationId> allocations_with_metadata_;
  std::vector<ObjectId> explicit_schemas_;
  std::vector<DataItem> schemas_with_derived_ids_;
  bool resolved_derived_ids_ = false;
};

}  // namespace

absl::StatusOr<DataSliceImpl> DeepCloneOp::operator()(
    const DataSliceImpl& ds, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks) const {
  auto visitor = std::make_shared<DeepCloneVisitor>(
      DataBagImplPtr::NewRef(new_databag_),
      /*is_schema_slice=*/schema == schema::kSchema, databag, fallbacks);
  auto traverse_op = Traverser<DeepCloneVisitor>(databag, fallbacks, visitor);
  RETURN_IF_ERROR(traverse_op.TraverseSlice(ds, schema));
  SliceBuilder result_items(ds.size());
  for (size_t i = 0; i < ds.size(); ++i) {
    ASSIGN_OR_RETURN(auto value,
                     visitor->DeepCloneVisitor::GetValue(ds[i], schema));
    result_items.InsertIfNotSetAndUpdateAllocIds(i, value);
  }
  auto explicit_schemas = visitor->get_explicit_schemas();
  auto explicit_schemas_slice = DataSliceImpl::Create(
      arolla::CreateFullDenseArray<ObjectId>(std::move(explicit_schemas)));
  auto extract_op = ExtractOp(new_databag_);
  // We need to extract all the content that is reachable from the explicit
  // schemas. Some of these items might be already cloned (if reached through
  // different paths), in which case they would have two versions in the new
  // databag (extracted and cloned).
  //
  // DeepCloneVisitor doesn't set attributes for ObjectIds from the initial
  // databag. Thus, we can extract any part of the initial databag, and existing
  // content of the `new_databag_` would not interfere with the extraction.
  RETURN_IF_ERROR(extract_op(explicit_schemas_slice, DataItem(schema::kSchema),
                             databag, fallbacks, nullptr, {}));
  return std::move(result_items).Build();
}

absl::StatusOr<DataItem> DeepCloneOp::operator()(
    const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks) const {
  ASSIGN_OR_RETURN(auto result_slice,
                   (*this)(DataSliceImpl::Create(/*size=*/1, item), schema,
                           databag, fallbacks));
  DCHECK_EQ(result_slice.size(), 1);
  return result_slice[0];
}

}  // namespace koladata::internal
