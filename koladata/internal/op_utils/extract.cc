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
#include "koladata/internal/op_utils/extract.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/equal.h"
#include "koladata/internal/op_utils/group_by_utils.h"
#include "koladata/internal/op_utils/has.h"
#include "koladata/internal/op_utils/presence_and.h"
#include "koladata/internal/op_utils/presence_or.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/types_buffer.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/edge_ops.h"
#include "arolla/qexpr/operators/dense_array/logic_ops.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/cancellation.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

namespace {

// Attribute name for storing information about previous visits of ObjectId.
const std::string_view kVisitedAttrName = "u";

// Attribute name for storing information about previous visits of ObjectId
// with kObject schema. This is used to ensure that kSchemaAttr is extracted
// for the object, even if it was already visited as entity.
const std::string_view kObjectVisitedAttrName = "o";

// Attribute name for storing information about previous visits of schema
// ObjectId.
const std::string_view kSchemaVisitedAttrName = "s";

// Attribute name for storing mapping from the ObjectId in result to the
// original ObjectId.
const std::string_view kMappingAttrName = "m";

enum class SchemaSource { kSchemaDatabag = 1, kDataDatabag = 2 };

struct QueuedSlice {
  DataSliceImpl slice;
  DataItem schema;
  // schema_source indicates which (schema or data) DataBag we are using to
  // retrieve schema for the current slice.
  SchemaSource schema_source;
  int depth;
  // extract_kschemaattr_only indicates that we should only extract kSchemaAttr
  // for the slice.
  bool extract_kschemaattr_only = false;
};

class CopyingProcessor {
 public:
  CopyingProcessor(
      const DataBagImpl& databag, DataBagImpl::FallbackSpan fallbacks,
      absl::Nullable<const DataBagImpl*> schema_databag,
      DataBagImpl::FallbackSpan schema_fallbacks,
      const DataBagImplPtr& new_databag, bool is_shallow_clone = false,
      int max_depth = -1,
      const std::optional<LeafCallback>& leaf_callback = std::nullopt)
      : group_by_(),
        queued_slices_(),
        databag_(databag),
        fallbacks_(std::move(fallbacks)),
        schema_databag_(schema_databag),
        schema_fallbacks_(std::move(schema_fallbacks)),
        new_databag_(new_databag),
        objects_tracker_(DataBagImpl::CreateEmptyDatabag()),
        is_shallow_clone_(is_shallow_clone),
        max_depth_(max_depth),
        leaf_callback_(leaf_callback) {}

  absl::Status ExtractSlice(const QueuedSlice& slice) {
    RETURN_IF_ERROR(VisitImpl(slice, /*push_front=*/false));
    return ProcessQueue();
  }

  template <class ImplT>
  absl::Status SetMappingToInitialIds(const ImplT& new_ids,
                                      const ImplT& old_ids) {
    return objects_tracker_->SetAttr(new_ids, kMappingAttrName, old_ids);
  }

 private:
  // Push slice to the end of the queue with incremented depth, or to the front
  // of the queue with the same depth.
  void UpdateDepthAndAddToQueue(QueuedSlice slice, bool push_front) {
    if (push_front) {
      queued_slices_.push_front(std::move(slice));
    } else {
      slice.depth++;
      queued_slices_.push_back(std::move(slice));
    }
  }

  absl::Status ValidatePrimitiveTypes(const QueuedSlice& slice) {
    if (slice.slice.is_empty_and_unknown()) {
      return absl::OkStatus();
    }
    DCHECK(slice.schema.value<schema::DType>().is_primitive());
    auto dtype = slice.slice.dtype();
    if (auto schema_dtype = slice.schema.value<schema::DType>();
        schema_dtype.qtype() != dtype) {
      auto slice_qtype_or = schema::DType::FromQType(dtype);
      if (!slice_qtype_or.ok()) {
        return absl::InvalidArgumentError(absl::StrCat(
            "during extract/clone, got a slice with primitive type ",
            schema_dtype,
            " while the actual content is mixed or not a primitive"));
      }
      return absl::InvalidArgumentError(
          absl::StrCat("during extract/clone, got a slice with primitive type ",
                       schema_dtype, " while the actual content has type ",
                       slice_qtype_or->name()));
    }
    return absl::OkStatus();
  }

  DataSliceImpl FilterToObjects(const DataSliceImpl& ds) {
    DataSliceImpl res = DataSliceImpl::CreateEmptyAndUnknownType(ds.size());
    ds.VisitValues([&](const auto& array) {
      using T = typename std::decay_t<decltype(array)>::base_type;
      if constexpr (std::is_same_v<T, ObjectId>) {
        res = DataSliceImpl::CreateWithAllocIds(ds.allocation_ids(), array);
      }
    });
    return res;
  }

  absl::StatusOr<DataSliceImpl> MarkEntitiesAsVisited(
    const DataSliceImpl& slice) {
    if (slice.is_empty_and_unknown()) {
      return slice;
    }
    if (slice.dtype() != arolla::GetQType<ObjectId>()) {
      return absl::InternalError("Expected a slice of ObjectIds");
    }
    return objects_tracker_->InternalSetUnitAttrAndReturnMissingObjects(
        slice, kVisitedAttrName);
  }

  absl::StatusOr<DataSliceImpl> MarkObjectsAsVisited(
      const DataSliceImpl& slice) {
    if (slice.is_empty_and_unknown()) {
      return slice;
    }
    if (slice.dtype() != arolla::GetQType<ObjectId>()) {
      return absl::InternalError("Expected a slice of ObjectIds");
    }
    return objects_tracker_->InternalSetUnitAttrAndReturnMissingObjects(
        slice, kObjectVisitedAttrName);
  }

  absl::Status MarkSchemaAsVisited(const DataItem& schema_item,
                                   SchemaSource schema_source) {
    ASSIGN_OR_RETURN(auto visited, objects_tracker_->GetAttr(
                                       schema_item, kSchemaVisitedAttrName));
    // schema_source indicates in which (schema or data) DataBag we are visiting
    // the current `schema_item`.
    int visited_mask = static_cast<int>(schema_source);
    if (visited.has_value()) {
      if (!visited.holds_value<int>()) {
        return absl::InternalError("visited attribute should be an int");
      }
      visited_mask |= visited.value<int>();
    }
    RETURN_IF_ERROR(objects_tracker_->SetAttr(
        schema_item, kSchemaVisitedAttrName, DataItem(visited_mask)));
    return absl::OkStatus();
  }

  absl::Status VisitObjects(const QueuedSlice& slice, bool push_front) {
    // Filter out objectIds, as Object slice may also contain primitive types.
    DataSliceImpl objects_slice = FilterToObjects(slice.slice);
    ASSIGN_OR_RETURN(auto partial_update_slice,
                     MarkObjectsAsVisited(objects_slice));
    if (partial_update_slice.size() == 0) {
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(auto update_slice,
                     MarkEntitiesAsVisited(partial_update_slice));
    if (update_slice.size() != partial_update_slice.size()) {
      // We need to copy kSchemaAttr for the ObjectIds that were previously
      // visited as entities. We assume that the schemas are the same, so we
      // would not need a deep extraction.
      UpdateDepthAndAddToQueue(
          QueuedSlice{.slice = std::move(partial_update_slice),
                      .schema = slice.schema,
                      .schema_source = SchemaSource::kDataDatabag,
                      .depth = slice.depth,
                      .extract_kschemaattr_only = true},
          push_front);
    }
    if (update_slice.size() > 0) {
      UpdateDepthAndAddToQueue(
          QueuedSlice{.slice = std::move(update_slice),
                      .schema = slice.schema,
                      .schema_source = SchemaSource::kDataDatabag,
                      .depth = slice.depth},
          push_front);
    }
    return absl::OkStatus();
  }

  absl::Status VisitEntities(const QueuedSlice& slice, bool push_front) {
    ASSIGN_OR_RETURN(auto update_slice, MarkEntitiesAsVisited(slice.slice));
    if (update_slice.present_count() == 0) {
      ASSIGN_OR_RETURN(
          auto schema_is_copied,
          objects_tracker_->GetAttr(slice.schema, kSchemaVisitedAttrName));
      if (schema_is_copied.has_value() &&
          (schema_is_copied.value<int>() &
           static_cast<int>(slice.schema_source))) {
        // Data is copied and Schema is copied from the appropriate DataBag -
        // schema_source.
        return absl::OkStatus();
      }
    }
    RETURN_IF_ERROR(MarkSchemaAsVisited(slice.schema, slice.schema_source));
    UpdateDepthAndAddToQueue(QueuedSlice{.slice = std::move(update_slice),
                                         .schema = slice.schema,
                                         .schema_source = slice.schema_source,
                                         .depth = slice.depth},
                             push_front);
    return absl::OkStatus();
  }

  absl::Status VisitImpl(const QueuedSlice& slice, bool push_front) {
    // TODO: Decide on the behavior, when we come to the same
    // object with the different schemas.
    if (slice.schema.holds_value<ObjectId>()) {
      return VisitEntities(slice, push_front);
    } else if (slice.schema.holds_value<schema::DType>()) {
      if (slice.schema == schema::kObject || slice.schema == schema::kSchema) {
        return VisitObjects(slice, push_front);
      } else if (slice.schema.value<schema::DType>().is_primitive()) {
        RETURN_IF_ERROR(ValidatePrimitiveTypes(slice));
      }
      // ItemId need no processing.
    } else {
      return absl::InternalError("unsupported schema type");
    }
    return absl::OkStatus();
  }

  absl::Status NotifyLeaf(const QueuedSlice& slice) {
    if (leaf_callback_.has_value()) {
      RETURN_IF_ERROR(leaf_callback_.value()(slice.slice, slice.schema));
    }
    return absl::OkStatus();
  }

  absl::Status Visit(const QueuedSlice& slice, bool push_front = false) {
    if (is_shallow_clone_) {
      return absl::OkStatus();
    }
    return VisitImpl(slice, push_front);
  }

  // Sets attribute to the new_data_bag_ that are not unset in the `attr_ds`.
  // It is useful utility to copy attributes from one DataBag to another.
  // Use GetAttrWithRemoved to get `attr_ds`.
  absl::Status SetAttrToNewDatabagSkipUnset(DataSliceImpl ds,
                                            const std::string_view attr_name,
                                            const DataSliceImpl& attr_ds) {
    if (attr_ds.types_buffer().size() != 0) {
      auto set_mask =
          attr_ds.types_buffer().ToInvertedBitmap(TypesBuffer::kUnset);
      if (!set_mask.empty() &&
          !arolla::bitmap::AreAllBitsSet(set_mask.begin(), ds.size())) {
        const auto& objects_array = ds.values<ObjectId>();
        ds = DataSliceImpl::CreateWithAllocIds(
            ds.allocation_ids(),
            ObjectIdArray{objects_array.values, std::move(set_mask)});
      }
    }
    return new_databag_->SetAttr(ds, attr_name, attr_ds);
  }

  absl::Status SetSchemaAttrToNewDatabagSkipMissing(
      DataSliceImpl ds, const std::string_view attr_name,
      const DataSliceImpl& attr_ds) {
    // TODO: We need to differentiate between removed and unset
    // schema attributes, and to set them accordingly in the new_databag_.
    // For now we assume that all of them are unset.
    if (attr_ds.present_count() < ds.present_count()) {
      ASSIGN_OR_RETURN(auto mask, HasOp()(attr_ds));
      ASSIGN_OR_RETURN(ds, PresenceAndOp()(ds, mask));
    }
    return new_databag_->SetSchemaAttr(ds, attr_name, attr_ds);
  }

  absl::Status ProcessAttribute(const QueuedSlice& slice,
                                const std::string_view attr_name,
                                const DataItem& attr_schema) {
    auto ds = slice.slice;
    DataSliceImpl old_ds;
    if (is_shallow_clone_) {
      ASSIGN_OR_RETURN(old_ds, objects_tracker_->GetAttr(ds, kMappingAttrName));
    } else {
      old_ds = ds;
    }
    ASSIGN_OR_RETURN(auto attr_ds, databag_.GetAttrWithRemoved(
                                       old_ds, attr_name, fallbacks_));
    if (max_depth_ == -1 || slice.depth < max_depth_) {
      RETURN_IF_ERROR(
          SetAttrToNewDatabagSkipUnset(std::move(ds), attr_name, attr_ds));
    }
    RETURN_IF_ERROR(Visit(
        {std::move(attr_ds), attr_schema, slice.schema_source, slice.depth}));
    return absl::OkStatus();
  }

  absl::Status ProcessDictKeysAndValues(const QueuedSlice& slice,
                                        const DataItem& keys_schema,
                                        const DataItem& values_schema) {
    const auto& ds = slice.slice;
    DataSliceImpl old_ds;
    if (is_shallow_clone_) {
      ASSIGN_OR_RETURN(old_ds, objects_tracker_->GetAttr(ds, kMappingAttrName));
    } else {
      old_ds = ds;
    }
    ASSIGN_OR_RETURN((auto [keys_ds, keys_edge]),
                     databag_.GetDictKeys(old_ds, fallbacks_));
    if (ds.present_count() == 0) {
      RETURN_IF_ERROR(Visit(
          {DataSliceImpl(), keys_schema, slice.schema_source, slice.depth}));
      RETURN_IF_ERROR(Visit(
          {DataSliceImpl(), values_schema, slice.schema_source, slice.depth}));
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(
        auto dicts_expanded,
        arolla::DenseArrayExpandOp()(&ctx_, ds.values<ObjectId>(), keys_edge));
    auto dict_expanded_ds = DataSliceImpl::Create(dicts_expanded);
    DataSliceImpl values_ds;
    if (is_shallow_clone_) {
      ASSIGN_OR_RETURN(auto old_dicts_expanded,
                       arolla::DenseArrayExpandOp()(
                           &ctx_, old_ds.values<ObjectId>(), keys_edge));
      ASSIGN_OR_RETURN(values_ds,
                       databag_.GetFromDict(
                           DataSliceImpl::Create(std::move(old_dicts_expanded)),
                           keys_ds, fallbacks_));
    } else {
      ASSIGN_OR_RETURN(values_ds, databag_.GetFromDict(dict_expanded_ds,
                                                       keys_ds, fallbacks_));
    }
    if (max_depth_ == -1 || slice.depth < max_depth_) {
      RETURN_IF_ERROR(
          new_databag_->SetInDict(dict_expanded_ds, keys_ds, values_ds));
    }
    RETURN_IF_ERROR(Visit(
        {std::move(keys_ds), keys_schema, slice.schema_source, slice.depth}));
    RETURN_IF_ERROR(Visit({std::move(values_ds), values_schema,
                           slice.schema_source, slice.depth}));
    return absl::OkStatus();
  }

  absl::Status ProcessListItems(const QueuedSlice& slice,
                                const DataItem& attr_schema) {
    const auto& ds = slice.slice;
    DataSliceImpl old_ds;
    if (is_shallow_clone_) {
      ASSIGN_OR_RETURN(old_ds, objects_tracker_->GetAttr(ds, kMappingAttrName));
    } else {
      old_ds = ds;
    }
    ASSIGN_OR_RETURN(
        auto list_items,
        databag_.ExplodeLists(old_ds, DataBagImpl::ListRange(), fallbacks_));
    const auto& [list_items_ds, list_items_edge] = list_items;
    if (max_depth_ == -1 || slice.depth < max_depth_) {
      RETURN_IF_ERROR(
          new_databag_->ExtendLists(ds, list_items_ds, list_items_edge));
    }
    RETURN_IF_ERROR(
        Visit({list_items_ds, attr_schema, slice.schema_source, slice.depth}));
    return absl::OkStatus();
  }

  // Copies schema for attribute `attr_name` of the given `schema_item` from
  // provided databag `db` (with `fallbacks`) into `new_databag_`.
  // Returns copied schema and a boolean value indicating if the `new_databag_`
  // was changed.
  absl::StatusOr<std::pair<DataItem, bool>> CopyAttrSchema(
      const DataItem& schema_item, const DataBagImpl& db,
      const DataBagImpl::FallbackSpan fallbacks,
      const std::string_view attr_name) {
    DataItem old_schema_item;
    if (is_shallow_clone_) {
      ASSIGN_OR_RETURN(old_schema_item, objects_tracker_->GetAttr(
                                            schema_item, kMappingAttrName));
      if (!old_schema_item.has_value()) {
        return std::make_pair(DataItem(), false);
      }
    } else {
      old_schema_item = schema_item;
    }
    ASSIGN_OR_RETURN(DataItem attr_schema,
                     db.GetSchemaAttr(old_schema_item, attr_name, fallbacks));
    ASSIGN_OR_RETURN(
        auto copied_schema,
        new_databag_->GetSchemaAttrAllowMissing(schema_item, attr_name));
    if (copied_schema.has_value()) {
      if (copied_schema != attr_schema) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "conflicting values for schema %v attribute %s: %v != %v",
            schema_item, attr_name, copied_schema, attr_schema));
      }
      return std::make_pair(std::move(attr_schema), false);
    }
    if ((attr_name == schema::kSchemaNameAttr ||
         attr_name == schema::kSchemaMetadataAttr) &&
        old_schema_item != schema_item) {
      return std::make_pair(DataItem(), false);
    }
    RETURN_IF_ERROR(
        new_databag_->SetSchemaAttr(schema_item, attr_name, attr_schema));
    return std::make_pair(std::move(attr_schema), true);
  }

  // Copies schemas for attribute `attr_name` of the given `schemas` from
  // provided databag `databag_` (with `fallbacks_`) into `new_databag_`.
  // Returns copied schemas and a boolean value indicating if the `new_databag_`
  // was changed.
  absl::StatusOr<std::pair<DataSliceImpl, bool>> CopyAttrSchemas(
      const DataSliceImpl& new_schemas, const DataSliceImpl& old_schemas,
      const std::string_view attr_name) {
    ASSIGN_OR_RETURN(
        DataSliceImpl attr_schemas,
        databag_.GetSchemaAttrAllowMissing(old_schemas, attr_name, fallbacks_));
    if (attr_schemas.is_empty_and_unknown()) {
      return std::make_pair(std::move(attr_schemas), false);
    }
    ASSIGN_OR_RETURN(
        DataSliceImpl copied_schemas,
        new_databag_->GetSchemaAttrAllowMissing(new_schemas, attr_name));
    if (copied_schemas.is_empty_and_unknown()) {
      RETURN_IF_ERROR(SetSchemaAttrToNewDatabagSkipMissing(
          new_schemas, attr_name, attr_schemas));
      return std::make_pair(std::move(attr_schemas), true);
    }
    ASSIGN_OR_RETURN(auto eq_schemas, EqualOp()(copied_schemas, attr_schemas));
    if (eq_schemas.present_count() == attr_schemas.present_count()) {
      return std::make_pair(std::move(attr_schemas), false);
    }
    if (eq_schemas.present_count() == copied_schemas.present_count()) {
      RETURN_IF_ERROR(SetSchemaAttrToNewDatabagSkipMissing(
          new_schemas, attr_name, attr_schemas));
      return std::make_pair(std::move(attr_schemas), true);
    }
    ASSIGN_OR_RETURN(auto attr_schemas_mask, HasOp()(attr_schemas));
    ASSIGN_OR_RETURN(auto schemas_overwritten,
                     PresenceAndOp()(copied_schemas, attr_schemas_mask));
    if (eq_schemas.present_count() != schemas_overwritten.present_count()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "conflicting values for some of schemas %v attribute %s: %v != "
          "%v",
          old_schemas, attr_name, copied_schemas, attr_schemas));
    }
    // If there are previously set attributes, that are missing in
    // attr_schemas, we shouldn't overwrite them.
    ASSIGN_OR_RETURN(auto new_schemas_overwrite,
                     PresenceAndOp()(new_schemas, attr_schemas_mask));
    RETURN_IF_ERROR(SetSchemaAttrToNewDatabagSkipMissing(
        std::move(new_schemas_overwrite), attr_name, attr_schemas));
    return std::make_pair(std::move(attr_schemas), true);
  }

  // Process slice of dicts with entity schema.
  absl::Status ProcessDicts(const QueuedSlice& ds, const DataBagImpl& db,
                            const DataBagImpl::FallbackSpan fallbacks) {
    ASSIGN_OR_RETURN(
        (auto [keys_schema, was_key_schema_updated]),
        CopyAttrSchema(ds.schema, db, fallbacks, schema::kDictKeysSchemaAttr));
    ASSIGN_OR_RETURN((auto [values_schema, was_value_schema_updated]),
                     CopyAttrSchema(ds.schema, db, fallbacks,
                                    schema::kDictValuesSchemaAttr));
    bool was_schema_updated =
        was_key_schema_updated || was_value_schema_updated;
    if (was_schema_updated || ds.slice.present_count() > 0) {
      // Data or schema are not yet copied.
      return ProcessDictKeysAndValues(ds, keys_schema, values_schema);
    }
    return absl::OkStatus();
  }

  // Process slice of lists with entity schema.
  absl::Status ProcessLists(const QueuedSlice& ds, const DataBagImpl& db,
                            const DataBagImpl::FallbackSpan fallbacks) {
    ASSIGN_OR_RETURN(
        (auto [attr_schema, was_schema_updated]),
        CopyAttrSchema(ds.schema, db, fallbacks, schema::kListItemsSchemaAttr));
    if (was_schema_updated || ds.slice.present_count() > 0) {
      // Data or schema are not yet copied.
      return ProcessListItems(ds, attr_schema);
    }
    return absl::OkStatus();
  }

  // Process attributes for the slice with entity schema.
  absl::Status ProcessEntitySliceAttrs(
      const QueuedSlice& ds, const DataBagImpl& db,
      const DataBagImpl::FallbackSpan fallbacks) {
    DataItem old_schema;
    if (is_shallow_clone_ && ds.schema.holds_value<ObjectId>()) {
      ASSIGN_OR_RETURN(old_schema,
                       objects_tracker_->GetAttr(ds.schema, kMappingAttrName));
    } else {
      old_schema = ds.schema;
    }
    ASSIGN_OR_RETURN(auto attr_names,
                     db.GetSchemaAttrsAsVector(old_schema, fallbacks));
    if (attr_names.empty()) {
      return absl::OkStatus();
    }

    bool has_list_items_attr = false;
    bool has_dict_keys_attr = false;
    bool has_dict_values_attr = false;
    bool has_regular_attr = false;
    for (const auto& attr_name_item : attr_names) {
      const std::string_view attr_name = attr_name_item.value<arolla::Text>();
      if (attr_name == schema::kListItemsSchemaAttr) {
        has_list_items_attr = true;
        continue;
      }
      if (attr_name == schema::kDictKeysSchemaAttr) {
        has_dict_keys_attr = true;
        continue;
      }
      if (attr_name == schema::kDictValuesSchemaAttr) {
        has_dict_values_attr = true;
        continue;
      }
      ASSIGN_OR_RETURN((auto [attr_schema, was_schema_updated]),
                       CopyAttrSchema(ds.schema, db, fallbacks, attr_name));
      if (attr_name == schema::kSchemaNameAttr) {
        continue;
      }
      if (attr_name == schema::kSchemaMetadataAttr) {
        if (was_schema_updated) {
          RETURN_IF_ERROR(Visit(
              {DataSliceImpl::Create({std::move(attr_schema)}),
               DataItem(schema::kObject), SchemaSource::kDataDatabag, ds.depth},
              /*push_front=*/true));
        }
        continue;
      }
      has_regular_attr = true;
      if (was_schema_updated || ds.slice.present_count() > 0) {
        // Data or schema are not yet copied.
        RETURN_IF_ERROR(
            ProcessAttribute(ds, attr_name, std::move(attr_schema)));
      }
    }
    if (has_list_items_attr) {
      if (has_regular_attr || has_dict_keys_attr || has_dict_values_attr) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "list schema %v has unexpected attributes", old_schema));
      }
      return ProcessLists(ds, db, fallbacks);
    } else if (has_dict_keys_attr || has_dict_values_attr) {
      if (has_regular_attr || has_list_items_attr || !has_dict_keys_attr ||
          !has_dict_values_attr) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "dict schema %v has unexpected attributes", old_schema));
      }
      return ProcessDicts(ds, db, fallbacks);
    }
    return absl::OkStatus();
  }

  // Process slice of objects with entity schema.
  absl::Status ProcessEntitySlice(const QueuedSlice& ds) {
    if (!ds.schema.is_struct_schema()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("schema object is expected, got %v", ds.schema));
    }
    if (ds.schema.value<ObjectId>().IsNoFollowSchema()) {
      // no processing needed for NoFollowSchema.
      return absl::OkStatus();
    }
    if (ds.schema_source == SchemaSource::kDataDatabag) {
      return ProcessEntitySliceAttrs(ds, databag_, fallbacks_);
    } else if (ds.schema_source == SchemaSource::kSchemaDatabag) {
      DCHECK(schema_databag_ != nullptr);
      return ProcessEntitySliceAttrs(ds, *schema_databag_, schema_fallbacks_);
    }
    return absl::InternalError("unsupported schema source");
  }

  // Process slice of schemas.
  absl::Status ProcessSchemaSlice(const QueuedSlice& ds) {
    if (ds.schema != schema::kSchema) {
      return absl::InvalidArgumentError("slice of schemas is expected");
    }
    DCHECK(ds.schema_source == SchemaSource::kDataDatabag);
    std::optional<DataSliceImpl> old_schemas;
    if (is_shallow_clone_) {
      ASSIGN_OR_RETURN(old_schemas,
                       objects_tracker_->GetAttr(ds.slice, kMappingAttrName));
    } else {
      old_schemas = ds.slice;
    }
    for (size_t idx = 0; idx < ds.slice.size(); ++idx) {
      const DataItem& item = ds.slice[idx];
      if (!item.has_value() || !item.holds_value<ObjectId>()) {
        continue;
      }
      auto item_slice =
          QueuedSlice{.slice = DataSliceImpl::CreateEmptyAndUnknownType(0),
                      .schema = item,
                      .schema_source = SchemaSource::kDataDatabag,
                      .depth = ds.depth};
      RETURN_IF_ERROR(ProcessEntitySlice(item_slice));
    }
    return absl::OkStatus();
  }

  absl::Status ProcessObjectsListItemsAttribute(
      const DataSliceImpl& new_ds, const DataSliceImpl& new_schemas,
      const DataSliceImpl& old_schemas, int depth) {
    ASSIGN_OR_RETURN((auto [items_schemas, was_schema_updated]),
                     CopyAttrSchemas(new_schemas, old_schemas,
                                     schema::kListItemsSchemaAttr));
    if (!was_schema_updated && new_ds.present_count() == 0) {
      return absl::OkStatus();
    }
    // Data or schema are not yet copied.
    std::optional<DataItem> single_schema =
        group_by_.GetItemIfAllEqual(items_schemas);
    if (single_schema.has_value()) {
      return ProcessListItems({.slice = new_ds,
                               .schema = DataItem(schema::kObject),
                               .schema_source = SchemaSource::kDataDatabag,
                               .depth = depth},
                              std::move(*single_schema));
    }
    ASSIGN_OR_RETURN((auto [group_schemas, new_ds_grouped]),
                     group_by_.BySchemas(items_schemas, new_ds));
    auto status = absl::OkStatus();
    group_schemas.VisitValues(
        [&]<class T>(const arolla::DenseArray<T>& groups_schemas_T) {
          if constexpr (std::is_same_v<T, ObjectId> ||
                        std::is_same_v<T, schema::DType>) {
            groups_schemas_T.ForEachPresent([&](size_t idx, T item_schema) {
              if (!status.ok()) {
                return;
              }
              status = ProcessListItems(
                  {.slice =
                       DataSliceImpl::Create(std::move(new_ds_grouped[idx])),
                   .schema = DataItem(schema::kObject),
                   .schema_source = SchemaSource::kDataDatabag,
                   .depth = depth},
                  DataItem(item_schema));
            });
          } else {
            DCHECK(false);
          }
        });
    return status;
  }

  absl::Status ProcessObjectsDictKeysAndValuesAttributes(
      const DataSliceImpl& new_ds, const DataSliceImpl& new_schemas,
      const DataSliceImpl& old_schemas, int depth) {
    ASSIGN_OR_RETURN(
        (auto [keys_schemas, was_keys_schema_updated]),
        CopyAttrSchemas(new_schemas, old_schemas, schema::kDictKeysSchemaAttr));
    ASSIGN_OR_RETURN((auto [values_schemas, was_values_schema_updated]),
                     CopyAttrSchemas(new_schemas, old_schemas,
                                     schema::kDictValuesSchemaAttr));
    if (!was_keys_schema_updated && !was_values_schema_updated &&
        new_ds.present_count() == 0) {
      return absl::OkStatus();
    }
    // Data or schema are not yet copied.
    std::optional<DataItem> single_keys_schema =
        group_by_.GetItemIfAllEqual(keys_schemas);
    std::optional<DataItem> single_values_schema =
        group_by_.GetItemIfAllEqual(values_schemas);
    if (single_keys_schema.has_value() && single_values_schema.has_value()) {
      return ProcessDictKeysAndValues(
          {.slice = new_ds,
           .schema = DataItem(schema::kObject),
           .schema_source = SchemaSource::kDataDatabag,
           .depth = depth},
          std::move(*single_keys_schema), std::move(*single_values_schema));
    }
    ASSIGN_OR_RETURN(
        auto edge, group_by_.EdgeFromSchemaPairs(keys_schemas, values_schemas));
    ASSIGN_OR_RETURN(auto group_keys_schemas,
                     group_by_.CollapseByEdge(edge, keys_schemas));
    ASSIGN_OR_RETURN(auto group_values_schemas,
                     group_by_.CollapseByEdge(edge, values_schemas));
    ASSIGN_OR_RETURN(
        auto new_ds_grouped,
        group_by_.ByEdge(std::move(edge), new_ds.values<ObjectId>()));
    auto status = absl::OkStatus();
    group_keys_schemas.VisitValues(
        [&]<class key_T>(
            const arolla::DenseArray<key_T>& group_keys_schemas_T) {
          group_values_schemas.VisitValues(
              [&]<class value_T>(
                  const arolla::DenseArray<value_T>& group_values_schemas_T) {
                if constexpr ((std::is_same_v<key_T, ObjectId> ||
                               std::is_same_v<key_T, schema::DType>) &&
                              (std::is_same_v<value_T, ObjectId> ||
                               std::is_same_v<value_T, schema::DType>)) {
                  auto for_each_status = arolla::DenseArraysForEachPresent(
                      [&](size_t idx, key_T key_schema, value_T value_schema) {
                        if (!status.ok()) {
                          return;
                        }
                        status = ProcessDictKeysAndValues(
                            {.slice = DataSliceImpl::Create(
                                 std::move(new_ds_grouped[idx])),
                             .schema = DataItem(schema::kObject),
                             .schema_source = SchemaSource::kDataDatabag,
                             .depth = depth},
                            DataItem(key_schema), DataItem(value_schema));
                      },
                      group_keys_schemas_T, group_values_schemas_T);
                  if (status.ok() && !for_each_status.ok()) {
                    status = std::move(for_each_status);
                  }
                } else {
                  DCHECK(false);
                }
              });
        });
    return status;
  }

  absl::Status ProcessObjectsAttribute(const DataSliceImpl& new_ds,
                                       const DataSliceImpl& new_schemas,
                                       const DataSliceImpl& old_schemas,
                                       std::string_view attr_name, int depth) {
    ASSIGN_OR_RETURN((auto [attr_schemas, was_schema_updated]),
                     CopyAttrSchemas(new_schemas, old_schemas, attr_name));
    if (attr_name == schema::kSchemaMetadataAttr) {
      return Visit({std::move(attr_schemas), DataItem(schema::kObject),
                    SchemaSource::kDataDatabag, depth},
                   /*push_front=*/true);
    }
    if (attr_name == schema::kSchemaNameAttr) {
      return absl::OkStatus();
    }
    if (!was_schema_updated && new_ds.present_count() == 0) {
      return absl::OkStatus();
    }
    // Data or schema are not yet copied.
    if (attr_schemas.is_empty_and_unknown()) {
      return absl::OkStatus();
    }
    std::optional<DataItem> single_schema =
        group_by_.GetItemIfAllEqual(attr_schemas);
    if (single_schema.has_value()) {
      return ProcessAttribute({.slice = new_ds,
                               .schema = DataItem(schema::kObject),
                               .schema_source = SchemaSource::kDataDatabag,
                               .depth = depth},
                              attr_name, std::move(*single_schema));
    }
    ASSIGN_OR_RETURN((auto [group_schemas, new_ds_grouped]),
                     group_by_.BySchemas(attr_schemas, new_ds));
    auto status = absl::OkStatus();
    group_schemas.VisitValues(
        [&]<class T>(const arolla::DenseArray<T>& group_schemas_T) {
          if constexpr (std::is_same_v<T, ObjectId> ||
                        std::is_same_v<T, schema::DType>) {
            group_schemas_T.ForEachPresent([&](size_t idx, T attr_schema) {
              if (!status.ok()) {
                return;
              }
              status = ProcessAttribute(
                  {.slice =
                       DataSliceImpl::Create(std::move(new_ds_grouped[idx])),
                   .schema = DataItem(schema::kObject),
                   .schema_source = SchemaSource::kDataDatabag,
                   .depth = depth},
                  attr_name, DataItem(attr_schema));
            });
          } else {
            DCHECK(false);
          }
        });
    return status;
  }

  // Process slice of objects with ObjectId schema, where all schemas are either
  // in a single big allocation or are in small allocation and are equal.
  absl::Status ProcessObjectsWithSchemasInSingleAllocation(
      ObjectId schemas_allocation_id, const DataSliceImpl& new_ds,
      const DataSliceImpl& new_schemas, const DataSliceImpl& old_schemas,
      int depth) {
    std::vector<DataItem> attr_names;
    if (schemas_allocation_id.IsSmallAlloc()) {
      ASSIGN_OR_RETURN(attr_names,
                       databag_.GetSchemaAttrsAsVector(
                           DataItem(schemas_allocation_id), fallbacks_));
    } else {
      attr_names = databag_.GetSchemaAttrsForBigAllocationAsVector(
          AllocationId(schemas_allocation_id), fallbacks_);
    }
    bool has_lists = false;
    bool has_dicts = false;
    for (const auto& attr_name : attr_names) {
      auto attr_name_str = attr_name.value<arolla::Text>();
      if (attr_name_str == schema::kListItemsSchemaAttr) {
        has_lists = true;
      } else if (attr_name_str == schema::kDictKeysSchemaAttr ||
                 attr_name_str == schema::kDictValuesSchemaAttr) {
        has_dicts = true;
      } else {
        RETURN_IF_ERROR(ProcessObjectsAttribute(
            new_ds, new_schemas, old_schemas, attr_name_str, depth));
      }
    }
    if (has_lists) {
      RETURN_IF_ERROR(ProcessObjectsListItemsAttribute(new_ds, new_schemas,
                                                       old_schemas, depth));
    }
    if (has_dicts) {
      RETURN_IF_ERROR(ProcessObjectsDictKeysAndValuesAttributes(
          new_ds, new_schemas, old_schemas, depth));
    }
    return absl::OkStatus();
  }

  // Process slice of objects with ObjectId schema.
  absl::Status ProcessObjectsWithSchemas(const DataSliceImpl& new_ds,
                                         const DataSliceImpl& new_schemas,
                                         const DataSliceImpl& old_schemas,
                                         int depth) {
    DCHECK(new_ds.dtype() == arolla::GetQType<ObjectId>());
    DCHECK(new_schemas.dtype() == arolla::GetQType<ObjectId>());
    DCHECK(old_schemas.dtype() == arolla::GetQType<ObjectId>());

    arolla::DenseArrayBuilder<ObjectId> schema_allocs_bldr(old_schemas.size());
    bool is_single_allocation = true;
    std::optional<ObjectId> schema_alloc = std::nullopt;
    old_schemas.values<ObjectId>().ForEachPresent(
        [&](size_t idx, const ObjectId& schema) {
          if (schema.IsNoFollowSchema()) {
            return;
          }
          ObjectId schema_repr = schema;
          if (!schema.IsSmallAlloc()) {
            schema_repr = AllocationId(schema).ObjectByOffset(0);
          }
          schema_allocs_bldr.Set(idx, schema_repr);
          if (schema_alloc.has_value()) {
            is_single_allocation &= *schema_alloc == schema_repr;
          } else {
            schema_alloc = schema_repr;
          }
        });
    if (!schema_alloc.has_value()) {
      return absl::OkStatus();
    }
    if (is_single_allocation) {
      return ProcessObjectsWithSchemasInSingleAllocation(
          *schema_alloc, new_ds, new_schemas, old_schemas, depth);
    }
    const auto schema_allocs = std::move(schema_allocs_bldr).Build();
    ASSIGN_OR_RETURN(auto edge, group_by_.EdgeFromSchemasArray(schema_allocs));
    ASSIGN_OR_RETURN(auto group_schema_allocs,
                     group_by_.CollapseByEdge(edge, schema_allocs));
    ASSIGN_OR_RETURN(auto new_ds_grouped,
                     group_by_.ByEdge(edge, new_ds.values<ObjectId>()));
    ASSIGN_OR_RETURN(auto new_schemas_grouped,
                     group_by_.ByEdge(edge, new_schemas.values<ObjectId>()));
    ASSIGN_OR_RETURN(
        auto old_schemas_grouped,
        group_by_.ByEdge(std::move(edge), old_schemas.values<ObjectId>()));
    auto status = absl::OkStatus();
    group_schema_allocs.ForEachPresent([&](size_t idx, ObjectId schema_alloc) {
      if (!status.ok()) {
        return;
      }
      status = ProcessObjectsWithSchemasInSingleAllocation(
          schema_alloc, DataSliceImpl::Create(std::move(new_ds_grouped[idx])),
          DataSliceImpl::Create(std::move(new_schemas_grouped[idx])),
          DataSliceImpl::Create(std::move(old_schemas_grouped[idx])), depth);
    });
    return status;
  }

  // Process slice of objects with object schema.
  absl::Status ProcessObjectSlice(const QueuedSlice& ds) {
    DataSliceImpl old_ds;
    if (is_shallow_clone_) {
      ASSIGN_OR_RETURN(old_ds,
                       objects_tracker_->GetAttr(ds.slice, kMappingAttrName));
    } else {
      old_ds = ds.slice;
    }
    ASSIGN_OR_RETURN(
        auto old_schemas,
        databag_.GetAttrWithRemoved(old_ds, schema::kSchemaAttr, fallbacks_));
    bool has_schemas = !old_schemas.is_empty_and_unknown();
    if (has_schemas && old_schemas.dtype() != arolla::GetQType<ObjectId>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "unsupported schema found during extract/clone among %v in "
          "__schema__ attribute of slice %v",
          old_schemas, old_ds));
    }
    if (!is_shallow_clone_) {
      RETURN_IF_ERROR(SetAttrToNewDatabagSkipUnset(
          ds.slice, schema::kSchemaAttr, old_schemas));
      if (!has_schemas || ds.extract_kschemaattr_only) {
        return absl::OkStatus();
      }
      return ProcessObjectsWithSchemas(ds.slice, old_schemas, old_schemas,
                                       ds.depth);
    }
    // TODO: what do we do with removed schemas in shallow clone?
    if (!has_schemas) {
      return absl::OkStatus();
    }
    // Create new implicit schemas and keep allocated explicit schemas unchanged
    arolla::DenseArrayBuilder<arolla::Unit> implicit_mask_bldr(ds.slice.size());
    old_schemas.values<ObjectId>().ForEachPresent(
        [&](size_t idx, const ObjectId& schema) {
          if (schema.IsImplicitSchema()) {
            implicit_mask_bldr.Set(idx, arolla::kPresent);
          }
        });
    ASSIGN_OR_RETURN(
        auto implicit_slice,
        PresenceAndOp()(ds.slice, DataSliceImpl::Create(
                                      std::move(implicit_mask_bldr).Build())));
    ASSIGN_OR_RETURN(
        auto new_implicit_schemas,
        CreateUuidWithMainObject<internal::ObjectId::kUuidImplicitSchemaFlag>(
            implicit_slice, schema::kImplicitSchemaSeed));
    ASSIGN_OR_RETURN(auto new_schemas,
                     PresenceOrOp()(new_implicit_schemas, old_schemas));
    RETURN_IF_ERROR(
        new_databag_->SetAttr(ds.slice, schema::kSchemaAttr, new_schemas));
    RETURN_IF_ERROR(SetMappingToInitialIds(new_schemas, old_schemas));
    if (ds.extract_kschemaattr_only) {
      return absl::OkStatus();
    }
    return ProcessObjectsWithSchemas(ds.slice, new_schemas, old_schemas,
                                     ds.depth);
  }

  absl::Status ProcessQueue() {
    while (!queued_slices_.empty()) {
      RETURN_IF_ERROR(arolla::CheckCancellation());
      QueuedSlice slice = std::move(queued_slices_.front());
      queued_slices_.pop_front();
      if (max_depth_ >= 0 && slice.depth > max_depth_) {
        continue;
      } else if (max_depth_ >= 0 && slice.depth == max_depth_ &&
                 !slice.extract_kschemaattr_only) {
        // We notify the leaf, but keep processing slice.
        RETURN_IF_ERROR(NotifyLeaf(slice));
      }
      if (slice.schema.holds_value<ObjectId>()) {
        // Entity schema.
        RETURN_IF_ERROR(ProcessEntitySlice(slice));
      } else if (slice.schema.holds_value<schema::DType>()) {
        if (slice.schema == schema::kObject) {
          // Object schema.
          RETURN_IF_ERROR(ProcessObjectSlice(slice));
        } else if (slice.schema == schema::kSchema) {
          RETURN_IF_ERROR(ProcessSchemaSlice(slice));
        }
        // Primitive types and ItemId need no processing.
      } else {
        return absl::InternalError("unsupported schema type");
      }
    }
    return absl::OkStatus();
  }

 private:
  arolla::EvaluationContext ctx_;
  ObjectsGroupBy group_by_;
  std::deque<QueuedSlice> queued_slices_;
  const DataBagImpl& databag_;
  const DataBagImpl::FallbackSpan fallbacks_;
  const absl::Nullable<const DataBagImpl*> schema_databag_;
  const DataBagImpl::FallbackSpan schema_fallbacks_;
  const DataBagImplPtr new_databag_;
  const DataBagImplPtr objects_tracker_;
  const bool is_shallow_clone_;
  const int max_depth_;
  const std::optional<LeafCallback>& leaf_callback_;
};

absl::StatusOr<DataSliceImpl> ValidateCompatibilityAndFilterItemid(
    const DataSliceImpl& ds, const DataSliceImpl& itemid) {
  if (!itemid.is_empty_and_unknown() &&
      itemid.dtype() != arolla::GetQType<ObjectId>()) {
    return absl::InvalidArgumentError("itemid must contain ObjectIds");
  }
  std::optional<arolla::DenseArray<ObjectId>> filtered_itemid;
  RETURN_IF_ERROR(ds.VisitValues([&]<class T>(const arolla::DenseArray<T>&
                                                  array) -> absl::Status {
    if constexpr (std::is_same_v<T, ObjectId>) {
      auto itemid_array = itemid.values<ObjectId>();
      absl::Status status = absl::OkStatus();
      RETURN_IF_ERROR(arolla::DenseArraysForEach(
          [&](int64_t offset, bool is_valid,
              const arolla::OptionalValue<ObjectId> obj,
              const arolla::OptionalValue<ObjectId> item) {
            DCHECK(is_valid);
            if (status.ok() && obj.present) {
              if (!item.present) {
                status = absl::InvalidArgumentError(
                    "itemid must have an objectId for each item present in ds");
              } else if ((obj.value.IsDict() != item.value.IsDict()) ||
                         (obj.value.IsList() != item.value.IsList()) ||
                         (obj.value.IsSchema() != item.value.IsSchema())) {
                status = absl::InvalidArgumentError(
                    "itemid must be of the same type as respective ObjectId "
                    "from ds");
              }
            }
          },
          array, itemid_array));
      arolla::EvaluationContext ctx;
      ASSIGN_OR_RETURN(filtered_itemid,
                       arolla::DenseArrayPresenceAndOp()(&ctx, itemid_array,
                                                         array.ToMask()));
      return status;
    }
    return absl::OkStatus();
  }));
  if (!filtered_itemid.has_value()) {
    return DataSliceImpl::CreateEmptyAndUnknownType(itemid.size());
  }
  return DataSliceImpl::Create(std::move(*filtered_itemid));
}

absl::StatusOr<DataSliceImpl> WithReplacedObjectIds(
    const DataSliceImpl& ds, const DataSliceImpl& itemid) {
  SliceBuilder bldr(ds.size(), itemid.allocation_ids());
  ds.VisitValues([&]<class T>(const arolla::DenseArray<T>& array) {
    if constexpr (!std::is_same_v<T, ObjectId>) {
      bldr.InsertIfNotSet<T>(array.bitmap, {}, array.values);
    }
  });
  RETURN_IF_ERROR(
      itemid.VisitValues([&]<class T>(const arolla::DenseArray<T>& array) {
        if constexpr (std::is_same_v<T, ObjectId>) {
          bldr.InsertIfNotSet<T>(array.bitmap, {}, array.values);
          return absl::OkStatus();
        } else {
          return absl::InvalidArgumentError("itemid must contain ObjectIds");
        }
      }));
  return std::move(bldr).Build();
}

}  // namespace

absl::Status ExtractOp::operator()(
    const DataSliceImpl& ds, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks,
    absl::Nullable<const DataBagImpl*> schema_databag,
    DataBagImpl::FallbackSpan schema_fallbacks, int max_depth,
    const std::optional<LeafCallback>& leaf_callback) const {
  SchemaSource schema_source = schema_databag == nullptr
                                   ? SchemaSource::kDataDatabag
                                   : SchemaSource::kSchemaDatabag;
  auto slice = QueuedSlice{.slice = ds,
                           .schema = schema,
                           .schema_source = schema_source,
                           .depth = -1};
  auto processor = CopyingProcessor(databag, std::move(fallbacks),
                                    schema_databag, std::move(schema_fallbacks),
                                    DataBagImplPtr::NewRef(new_databag_), false,
                                    max_depth, leaf_callback);
  RETURN_IF_ERROR(processor.ExtractSlice(slice));
  return absl::OkStatus();
}

absl::Status ExtractOp::operator()(
    const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks,
    absl::Nullable<const DataBagImpl*> schema_databag,
    DataBagImpl::FallbackSpan schema_fallbacks, int max_depth,
    const std::optional<LeafCallback>& leaf_callback) const {
  return (*this)(DataSliceImpl::Create(/*size=*/1, item), schema, databag,
                 std::move(fallbacks), schema_databag,
                 std::move(schema_fallbacks), max_depth, leaf_callback);
}

absl::StatusOr<std::pair<DataSliceImpl, DataItem>> ShallowCloneOp::operator()(
    const DataSliceImpl& ds, const DataSliceImpl& itemid,
    const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks,
    absl::Nullable<const DataBagImpl*> schema_databag,
    DataBagImpl::FallbackSpan schema_fallbacks) const {
  if ((schema_databag == nullptr) && !schema_fallbacks.empty()) {
    return absl::InternalError(
        "schema_databag and schema_fallbacks must be both present or both "
        "absent");
  }
  // TODO: check for itemid and ds compatibility with a high level
  // API instead.
  ASSIGN_OR_RETURN(auto filtered_itemid,
                   ValidateCompatibilityAndFilterItemid(ds, itemid));
  auto processor = CopyingProcessor(databag, std::move(fallbacks),
                                    schema_databag, std::move(schema_fallbacks),
                                    DataBagImplPtr::NewRef(new_databag_),
                                    /*is_shallow_clone=*/true);
  RETURN_IF_ERROR(processor.SetMappingToInitialIds(filtered_itemid, ds));
  if (schema.holds_value<ObjectId>()) {
    RETURN_IF_ERROR(processor.SetMappingToInitialIds(schema, schema));
  }
  SchemaSource schema_source = schema_databag == nullptr
                                   ? SchemaSource::kDataDatabag
                                   : SchemaSource::kSchemaDatabag;
  auto slice = QueuedSlice{.slice = filtered_itemid,
                           .schema = schema,
                           .schema_source = schema_source,
                           .depth = -1};
  RETURN_IF_ERROR(processor.ExtractSlice(slice));
  ASSIGN_OR_RETURN(auto result_slice,
                   WithReplacedObjectIds(ds, filtered_itemid));
  return std::make_pair(std::move(result_slice), schema);
}

absl::StatusOr<std::pair<DataItem, DataItem>> ShallowCloneOp::operator()(
    const DataItem& item, const DataItem& itemid, const DataItem& schema,
    const DataBagImpl& databag, DataBagImpl::FallbackSpan fallbacks,
    absl::Nullable<const DataBagImpl*> schema_databag,
    DataBagImpl::FallbackSpan schema_fallbacks) const {
  internal::ShallowCloneOp clone_op(new_databag_);
  ASSIGN_OR_RETURN((auto [result_slice_impl, result_schema_impl]),
                   clone_op(/*size=*/DataSliceImpl::Create(1, item),
                            DataSliceImpl::Create(/*size=*/1, itemid), schema,
                            databag, std::move(fallbacks), schema_databag,
                            std::move(schema_fallbacks)));
  return std::make_pair(result_slice_impl[0], std::move(result_schema_impl));
}

}  // namespace koladata::internal
