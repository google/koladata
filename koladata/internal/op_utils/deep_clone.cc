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
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/traverser.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

namespace {

class DeepCloneVisitor : AbstractVisitor {
 public:
  explicit DeepCloneVisitor(DataBagImplPtr new_databag, bool is_schema_slice)
      : new_databag_(std::move(new_databag)),
        is_schema_slice_(is_schema_slice),
        allocation_tracker_(),
        allocations_with_metadata_() {}

  absl::Status Previsit(const DataItem& from_item, const DataItem& from_schema,
                        const DataItem& item, const DataItem& schema) override {
    if (schema == schema::kObject && from_schema == schema::kSchema) {
      // The `item` is schema_metadata for `from_item`.
      return PrevisitSchemaMetadata(from_item, item);
    }
    if (schema.holds_value<ObjectId>()) {
      // Entity schema.
      if (schema.is_implicit_schema()) {
        // The item was already previsited with `schema::kObject`, thus we only
        // need to "clone" the implicit schema.
        return PrevisitItemWithImplicitSchema(item, schema);
      }
      return PrevisitObject(item);
    } else if (schema.holds_value<schema::DType>()) {
      if (schema == schema::kObject) {
        return PrevisitObject(item);
      } else if (schema == schema::kSchema) {
        return PrevisitSchema(item);
      }
      return absl::OkStatus();
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
    std::vector<AllocationId> derived_allocations;
    for (const AllocationId& schema_allocation : allocations_with_metadata_) {
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
    if (!allocations_with_metadata_.empty()) {
      // On first GetValue or Visit* call, we reassign metadata ids.
      //
      // GetValue is called only after all Previsits are done. And we call
      // GetValue (and not GetValueImpl) from each of the Visit* methods, thus
      // ensuring that we would reassign derived ids once, after all Previsits
      // are done, and before we start using cloned ids to srore in the new
      // databag.
      RETURN_IF_ERROR(AssignMetadataIds());
      DCHECK(allocations_with_metadata_.empty());
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
    RETURN_IF_ERROR(new_databag_->ExtendList(new_list, items));
    return absl::OkStatus();
  }

  absl::Status VisitDict(const DataItem& dict, const DataItem& schema,
                         bool is_object_schema, const DataSliceImpl& keys,
                         const DataSliceImpl& values) override {
    DCHECK(dict.holds_value<ObjectId>() && dict.value<ObjectId>().IsDict());
    DCHECK(keys.size() == values.size());
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
    DCHECK(attr_names.size() == attr_values.size());
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
    if (!item.holds_value<ObjectId>()) {
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
    AllocationId item_allocation = AllocationId(item.value<ObjectId>());
    allocation_tracker_[item_allocation] = item_allocation;
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

  absl::Status PrevisitSchema(const DataItem& schema) {
    // We create a "clone" for all explicit schemas, and skip implicit schemas.
    // For implicit schemas we create a "clone" when encounter them in
    // `schema::kSchemaAttr`, or in GetValue after all Previsits are done.
    if (schema.holds_value<ObjectId>() && !schema.is_implicit_schema() &&
        is_schema_slice_) {
      if (schema.value<ObjectId>().IsNoFollowSchema()) {
        return CloneAsExplicitSchema(
            DataItem(GetOriginalFromNoFollow(schema.value<ObjectId>())));
      }
      return CloneAsExplicitSchema(schema);
    }
    return absl::OkStatus();
  }

  absl::Status CloneAsExplicitSchema(const DataItem& schema) {
    const auto allocation_id = AllocationId(schema.value<ObjectId>());
    auto [alloc_it, inserted] =
        allocation_tracker_.emplace(allocation_id, AllocationId());
    if (!inserted) {
      return absl::OkStatus();
    }
    AllocationId new_allocation_id =
        AllocateExplicitSchemas(allocation_id.Capacity());
    alloc_it->second = new_allocation_id;
    return absl::OkStatus();
  }

  absl::Status SetSchemaAttr(const DataItem& item, const DataItem& schema) {
    ASSIGN_OR_RETURN(auto explicit_schema_value,
                     GetValue(schema, DataItem(schema::kSchema)));
    RETURN_IF_ERROR(new_databag_->SetAttr(item, schema::kSchemaAttr,
                                          std::move(explicit_schema_value)));
    return absl::OkStatus();
  }

 private:
  DataBagImplPtr new_databag_;
  bool is_schema_slice_;
  absl::flat_hash_map<AllocationId, AllocationId> allocation_tracker_;
  absl::flat_hash_set<AllocationId> allocations_with_metadata_;
};

}  // namespace

absl::StatusOr<DataSliceImpl> DeepCloneOp::operator()(
    const DataSliceImpl& ds, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks) const {
  auto visitor = std::make_shared<DeepCloneVisitor>(
      DataBagImplPtr::NewRef(new_databag_),
      /*is_schema_slice=*/schema == schema::kSchema);
  auto traverse_op = Traverser<DeepCloneVisitor>(databag, fallbacks, visitor);
  RETURN_IF_ERROR(traverse_op.TraverseSlice(ds, schema));
  SliceBuilder result_items(ds.size());
  for (size_t i = 0; i < ds.size(); ++i) {
    ASSIGN_OR_RETURN(auto value,
                     visitor->DeepCloneVisitor::GetValue(ds[i], schema));
    result_items.InsertIfNotSetAndUpdateAllocIds(i, value);
  }
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
