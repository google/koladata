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

#include <cstddef>
#include <cstdint>
#include <queue>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/edge_ops.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

namespace {

// Attribute name for storing information about previous visits of ObjectId.
const std::string_view kVisitedAttrName = "u";

// Attribute name for storing information about previous visits of schema
// ObjectId.
const std::string_view kSchemaVisitedAttrName = "s";

// Attribute name for storing mapping from the ObjectId in result to the
// original ObjectId.
const std::string_view kMappingAttrName = "m";

enum class SchemaSource { kSchemaDatabag = 1, kDataDatabag = 2 };

struct QueuedSlice {
  const DataSliceImpl slice;
  const DataItem schema;
  const SchemaSource
      schema_source;  // schema_source indicates which (schema or data) DataBag
                      // we are using to retrieve schema for the current slice.
};

class CopyingProcessor {
  DataSliceImpl FilterToObjects(const DataSliceImpl& ds) {
    auto bldr = DataSliceImpl::Builder(ds.size());
    bldr.GetMutableAllocationIds().Insert(ds.allocation_ids());
    ds.VisitValues([&](const auto& array) {
      using T = typename std::decay_t<decltype(array)>::base_type;
      if constexpr (std::is_same_v<T, ObjectId>) {
        bldr.AddArray(std::move(array));
      }
    });
    return std::move(bldr).Build();
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
        slice, kVisitedAttrName);
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

  absl::Status VisitObjects(const QueuedSlice& slice) {
    // Filter out objectIds, as Object slice may also contain primitive types.
    DataSliceImpl objects_slice = FilterToObjects(slice.slice);
    ASSIGN_OR_RETURN(auto update_slice, MarkObjectsAsVisited(objects_slice));
    if (update_slice.present_count() == 0) {
      return absl::OkStatus();
    }
    queued_slices_.push(
        QueuedSlice{.slice = update_slice,
                    .schema = slice.schema,
                    .schema_source = SchemaSource::kDataDatabag});
    return absl::OkStatus();
  }

  absl::Status VisitEntities(const QueuedSlice& slice) {
    ASSIGN_OR_RETURN(auto update_slice, MarkObjectsAsVisited(slice.slice));
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
    queued_slices_.push(QueuedSlice{.slice = update_slice,
                                    .schema = slice.schema,
                                    .schema_source = slice.schema_source});
    return absl::OkStatus();
  }

  absl::Status VisitImpl(const QueuedSlice& slice) {
    // TODO: Decide on the behavior, when we come to the same
    // object with the different schemas.
    if (slice.schema.holds_value<ObjectId>()) {
      return VisitEntities(slice);
    } else if (slice.schema.holds_value<schema::DType>()) {
      if (slice.schema == schema::kObject || slice.schema == schema::kSchema) {
        return VisitObjects(slice);
      } else if (slice.schema == schema::kAny) {
        return absl::InternalError(
            "clone/extract not supported for kAny schema");
      }
      // Primitive types, Any and ItemId need no processing.
    } else {
      return absl::InternalError("unsupported schema type");
    }
    return absl::OkStatus();
  }

  absl::Status Visit(const QueuedSlice& slice) {
    if (is_shallow_clone_) {
      return absl::OkStatus();
    }
    return VisitImpl(slice);
  }

  absl::Status ProcessAttribute(const QueuedSlice& slice,
                                const std::string_view attr_name,
                                const DataItem& attr_schema) {
    const auto& ds = slice.slice;
    DataSliceImpl old_ds;
    if (is_shallow_clone_) {
      ASSIGN_OR_RETURN(old_ds, objects_tracker_->GetAttr(ds, kMappingAttrName));
    } else {
      old_ds = ds;
    }
    ASSIGN_OR_RETURN(auto attr_ds,
                     databag_.GetAttr(old_ds, attr_name, fallbacks_));
    RETURN_IF_ERROR(new_databag_->SetAttr(ds, attr_name, attr_ds));
    RETURN_IF_ERROR(Visit({attr_ds, attr_schema, slice.schema_source}));
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
    arolla::EvaluationContext ctx;
    if (ds.present_count() == 0) {
      RETURN_IF_ERROR(
          Visit({DataSliceImpl(), keys_schema, slice.schema_source}));
      RETURN_IF_ERROR(
          Visit({DataSliceImpl(), values_schema, slice.schema_source}));
      return absl::OkStatus();
    }
    ASSIGN_OR_RETURN(
        auto dicts_expanded,
        arolla::DenseArrayExpandOp()(&ctx, ds.values<ObjectId>(), keys_edge));
    auto dict_expanded_ds = DataSliceImpl::Create(dicts_expanded);
    DataSliceImpl values_ds;
    if (is_shallow_clone_) {
      ASSIGN_OR_RETURN(auto old_dicts_expanded,
                       arolla::DenseArrayExpandOp()(
                           &ctx, old_ds.values<ObjectId>(), keys_edge));
      ASSIGN_OR_RETURN(values_ds, databag_.GetFromDict(
                                      DataSliceImpl::Create(old_dicts_expanded),
                                      keys_ds, fallbacks_));
    } else {
      ASSIGN_OR_RETURN(values_ds, databag_.GetFromDict(dict_expanded_ds,
                                                       keys_ds, fallbacks_));
    }
    RETURN_IF_ERROR(
        new_databag_->SetInDict(dict_expanded_ds, keys_ds, values_ds));
    RETURN_IF_ERROR(Visit({keys_ds, keys_schema, slice.schema_source}));
    RETURN_IF_ERROR(Visit({values_ds, values_schema, slice.schema_source}));
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
    RETURN_IF_ERROR(
        new_databag_->ExtendLists(ds, list_items_ds, list_items_edge));
    RETURN_IF_ERROR(Visit({list_items_ds, attr_schema, slice.schema_source}));
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
        // In case of Object slice we don't copy schemas in ShallowClone.
        return std::make_pair(schema_item, false);
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
    RETURN_IF_ERROR(
        new_databag_->SetSchemaAttr(schema_item, attr_name, attr_schema));
    return std::make_pair(std::move(attr_schema), true);
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
      if (!old_schema.has_value()) {
        // In case of Object slice we will have schemaId from the old databag.
        old_schema = ds.schema;
      }
    } else {
      old_schema = ds.schema;
    }
    ASSIGN_OR_RETURN(DataSliceImpl attr_names_slice,
                     db.GetSchemaAttrs(old_schema, fallbacks));
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
    attr_names.ForEach(
        [&](int64_t id, bool presence, const std::string_view attr_name) {
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
            "list schema %v has unexpected attributes", old_schema));
      }
      return ProcessLists(ds, db, fallbacks);
    } else if (has_dict_keys_attr || has_dict_values_attr) {
      if (attr_names.size() != 2 || !has_dict_keys_attr ||
          !has_dict_values_attr) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "dict schema %v has unexpected attributes", old_schema));
      }
      return ProcessDicts(ds, db, fallbacks);
    }
    absl::Status status = absl::OkStatus();
    attr_names.ForEach(
        [&](int64_t id, bool presence, const std::string_view attr_name) {
          DCHECK(presence);
          if (!status.ok()) {
            return;
          }
          auto status_or_attr_schema =
              CopyAttrSchema(ds.schema, db, fallbacks, attr_name);
          if (!status_or_attr_schema.ok()) {
            status = status_or_attr_schema.status();
            return;
          }
          auto [attr_schema, was_schema_updated] = *status_or_attr_schema;
          if (was_schema_updated || ds.slice.present_count() > 0) {
            // Data or schema are not yet copied.
            status = ProcessAttribute(ds, attr_name, attr_schema);
          }
        });
    return status;
  }

  // Process slice of objects with entity schema.
  absl::Status ProcessEntitySlice(const QueuedSlice& ds) {
    if (!ds.schema.is_entity_schema()) {
      return absl::InvalidArgumentError("schema object is expected");
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
    for (size_t idx = 0; idx < ds.slice.size(); ++idx) {
      const DataItem& item = ds.slice[idx];
      if (!item.has_value() || !item.holds_value<ObjectId>()) {
        continue;
      }
      auto item_slice =
          QueuedSlice{.slice = DataSliceImpl::CreateEmptyAndUnknownType(0),
                      .schema = item,
                      .schema_source = SchemaSource::kDataDatabag};
      RETURN_IF_ERROR(ProcessEntitySlice(item_slice));
    }
    return absl::OkStatus();
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
    ASSIGN_OR_RETURN(auto slice_schemas,
                     databag_.GetAttr(old_ds, schema::kSchemaAttr, fallbacks_));
    RETURN_IF_ERROR(
        new_databag_->SetAttr(ds.slice, schema::kSchemaAttr, slice_schemas));
    for (size_t idx = 0; idx < ds.slice.size(); ++idx) {
      const DataItem& item = ds.slice[idx];
      const DataItem& schema = slice_schemas[idx];
      if (!schema.is_schema()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "object %v is expected to have a schema in %s attribute, got %v",
            old_ds[idx], schema::kSchemaAttr, schema));
      }
      auto item_slice =
          QueuedSlice{.slice = DataSliceImpl::Create(1, item),
                      .schema = schema,
                      .schema_source = SchemaSource::kDataDatabag};
      if (schema.holds_value<ObjectId>()) {
        // TODO: group items with the same schema into slices.
        RETURN_IF_ERROR(ProcessEntitySlice(item_slice));
      } else if (schema == schema::kSchema) {
        RETURN_IF_ERROR(ProcessSchemaSlice(item_slice));
      } else {
        return absl::InternalError("unsupported schema type");
      }
    }
    return absl::OkStatus();
  }

  absl::Status ProcessQueue() {
    // TODO: support implicit schemas.
    while (!queued_slices_.empty()) {
      QueuedSlice slice = std::move(queued_slices_.front());
      queued_slices_.pop();
      if (slice.schema.holds_value<ObjectId>()) {
        // Entity schema.
        RETURN_IF_ERROR(ProcessEntitySlice(slice));
      } else if (slice.schema.holds_value<schema::DType>()) {
        if (slice.schema == schema::kObject) {
          // Object schema.
          RETURN_IF_ERROR(ProcessObjectSlice(slice));
        } else if (slice.schema == schema::kAny) {
          return absl::InternalError(
              "clone/extract not supported for kAny schema");
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

 public:
  CopyingProcessor(const DataBagImpl& databag,
                   const DataBagImpl::FallbackSpan& fallbacks,
                   const DataBagImplPtr& new_databag,
                   bool is_shallow_clone = false)
      : queued_slices_(),
        databag_(databag),
        fallbacks_(fallbacks),
        schema_databag_(nullptr),
        schema_fallbacks_(),
        new_databag_(new_databag),
        objects_tracker_(DataBagImpl::CreateEmptyDatabag()),
        is_shallow_clone_(is_shallow_clone) {}

  CopyingProcessor(const DataBagImpl& databag,
                   const DataBagImpl::FallbackSpan& fallbacks,
                   const DataBagImpl* schema_databag,
                   const DataBagImpl::FallbackSpan schema_fallbacks,
                   const DataBagImplPtr& new_databag,
                   bool is_shallow_clone = false)
      : queued_slices_(),
        databag_(databag),
        fallbacks_(fallbacks),
        schema_databag_(schema_databag),
        schema_fallbacks_(schema_fallbacks),
        new_databag_(new_databag),
        objects_tracker_(DataBagImpl::CreateEmptyDatabag()),
        is_shallow_clone_(is_shallow_clone) {}

  absl::Status ExtractSlice(QueuedSlice slice) {
    RETURN_IF_ERROR(VisitImpl(slice));
    return ProcessQueue();
  }

  // Create a new ObjectId for each ObjectId in the ds. If some ObjectId is
  // present in ds multiple times, new ObjectId will be created for each
  // occurrence.
  absl::StatusOr<DataSliceImpl> CloneObjects(const DataSliceImpl& ds) {
    DataSliceImpl::Builder bldr(ds.size());
    RETURN_IF_ERROR(ds.VisitValues([&](const auto& array) -> absl::Status {
      using T = typename std::decay_t<decltype(array)>::base_type;
      if constexpr (std::is_same_v<T, ObjectId>) {
        int64_t count_objects = 0;
        int64_t count_dicts = 0;
        int64_t count_lists = 0;
        int64_t count_schemas = 0;
        array.ForEachPresent([&](int64_t id, const ObjectId& obj_id) {
          if (obj_id.IsDict()) {
            count_dicts += 1;
          } else if (obj_id.IsList()) {
            count_lists += 1;
          } else if (obj_id.IsExplicitSchema() && !obj_id.IsUuid()){
            count_schemas += 1;
          } else {
            count_objects += 1;
          }
        });
        AllocationId new_objects = Allocate(count_objects);
        AllocationId new_dicts = AllocateDicts(count_dicts);
        AllocationId new_lists = AllocateLists(count_lists);
        AllocationId new_schemas = AllocateExplicitSchemas(count_schemas);
        if (count_objects > 0) {
          bldr.GetMutableAllocationIds().Insert(new_objects);
        }
        if (count_dicts > 0) {
          bldr.GetMutableAllocationIds().Insert(new_dicts);
        }
        if (count_lists > 0) {
          bldr.GetMutableAllocationIds().Insert(new_lists);
        }
        if (count_schemas > 0) {
          bldr.GetMutableAllocationIds().Insert(new_schemas);
        }
        count_objects = 0;
        count_dicts = 0;
        count_lists = 0;
        count_schemas = 0;
        auto cloned_ids_bldr =
            arolla::DenseArrayBuilder<ObjectId>(array.size());
        array.ForEachPresent([&](int64_t id, const ObjectId& obj_id) {
          if (obj_id.IsDict()) {
            cloned_ids_bldr.Set(id, new_dicts.ObjectByOffset(count_dicts++));
          } else if (obj_id.IsList()) {
            cloned_ids_bldr.Set(id, new_lists.ObjectByOffset(count_lists++));
          } else if (obj_id.IsExplicitSchema()) {
            if (obj_id.IsUuid()) {
              cloned_ids_bldr.Set(id, obj_id);
            } else {
              cloned_ids_bldr.Set(id,
                                  new_schemas.ObjectByOffset(count_schemas++));
            }
          } else {
            cloned_ids_bldr.Set(id,
                                new_objects.ObjectByOffset(count_objects++));
          }
        });
        arolla::DenseArray<ObjectId> cloned_ids =
            std::move(cloned_ids_bldr).Build();
        // TODO: Use ds and result if they are not mixed.
        RETURN_IF_ERROR(objects_tracker_->SetAttr(
            DataSliceImpl::CreateObjectsDataSlice(
                cloned_ids, bldr.GetMutableAllocationIds()),
            kMappingAttrName, DataSliceImpl::Create(array)));
        bldr.AddArray(std::move(cloned_ids));
      } else {
        bldr.AddArray(std::move(array));
      }
      return absl::OkStatus();
    }));
    return std::move(bldr).Build();
  }

  // Set the mapping attribute of the schema to the schema itself.
  absl::StatusOr<DataItem> ReflectSchema(const DataItem& schema) {
    if (schema.holds_value<ObjectId>()) {
      RETURN_IF_ERROR(
          objects_tracker_->SetAttr(schema, kMappingAttrName, schema));
      return schema;
    }
    return schema;
  }

 private:
  std::queue<QueuedSlice> queued_slices_;
  const DataBagImpl& databag_;
  const DataBagImpl::FallbackSpan fallbacks_;
  const DataBagImpl* schema_databag_;
  const DataBagImpl::FallbackSpan schema_fallbacks_;
  const DataBagImplPtr new_databag_;
  const DataBagImplPtr objects_tracker_;
  const bool is_shallow_clone_;
};

}  // namespace

absl::Status ExtractOp::operator()(const DataSliceImpl& ds,
                                   const DataItem& schema,
                                   const DataBagImpl& databag,
                                   DataBagImpl::FallbackSpan fallbacks) const {
  auto slice = QueuedSlice{.slice = ds,
                           .schema = schema,
                           .schema_source = SchemaSource::kDataDatabag};
  auto processor = CopyingProcessor(databag, fallbacks,
                                    DataBagImplPtr::NewRef(new_databag_));
  RETURN_IF_ERROR(processor.ExtractSlice(slice));
  return absl::OkStatus();
}

absl::Status ExtractOp::operator()(const DataItem& item, const DataItem& schema,
                                   const DataBagImpl& databag,
                                   DataBagImpl::FallbackSpan fallbacks) const {
  return (*this)(DataSliceImpl::Create(1, item), schema, databag, fallbacks);
}

absl::Status ExtractOp::operator()(
    const DataSliceImpl& ds, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks, const DataBagImpl& schema_databag,
    DataBagImpl::FallbackSpan schema_fallbacks) const {
  auto slice = QueuedSlice{.slice = ds,
                           .schema = schema,
                           .schema_source = SchemaSource::kSchemaDatabag};
  auto processor =
      CopyingProcessor(databag, fallbacks, &schema_databag, schema_fallbacks,
                       DataBagImplPtr::NewRef(new_databag_));
  RETURN_IF_ERROR(processor.ExtractSlice(slice));
  return absl::OkStatus();
}

absl::Status ExtractOp::operator()(
    const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks, const DataBagImpl& schema_databag,
    DataBagImpl::FallbackSpan schema_fallbacks) const {
  return (*this)(DataSliceImpl::Create(/*size=*/1, item), schema, databag,
                 fallbacks, schema_databag, schema_fallbacks);
}

absl::StatusOr<std::pair<DataSliceImpl, DataItem>> ShallowCloneOp::operator()(
    const DataSliceImpl& ds, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks) const {
  auto processor =
      CopyingProcessor(databag, fallbacks, DataBagImplPtr::NewRef(new_databag_),
                       /*is_shallow_clone=*/true);
  ASSIGN_OR_RETURN(auto result_ds, processor.CloneObjects(ds));
  ASSIGN_OR_RETURN(auto result_schema, processor.ReflectSchema(schema));
  auto slice = QueuedSlice{.slice = result_ds,
                           .schema = result_schema,
                           .schema_source = SchemaSource::kDataDatabag};
  RETURN_IF_ERROR(processor.ExtractSlice(slice));
  return std::make_pair(std::move(result_ds), std::move(result_schema));
}

absl::StatusOr<std::pair<DataItem, DataItem>> ShallowCloneOp::operator()(
    const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks) const {
  internal::ShallowCloneOp clone_op(new_databag_);
  ASSIGN_OR_RETURN(
      (auto [result_slice_impl, result_schema_impl]),
      clone_op(DataSliceImpl::Create(1, item), schema, databag, fallbacks));
  return std::make_pair(result_slice_impl[0], std::move(result_schema_impl));
}

absl::StatusOr<std::pair<DataSliceImpl, DataItem>> ShallowCloneOp::operator()(
    const DataSliceImpl& ds, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks, const DataBagImpl& schema_databag,
    DataBagImpl::FallbackSpan schema_fallbacks) const {
  auto processor =
      CopyingProcessor(databag, fallbacks, &schema_databag, schema_fallbacks,
                       DataBagImplPtr::NewRef(new_databag_),
                       /*is_shallow_clone=*/true);
  ASSIGN_OR_RETURN(auto result_ds, processor.CloneObjects(ds));
  ASSIGN_OR_RETURN(auto result_schema, processor.ReflectSchema(schema));
  auto slice = QueuedSlice{.slice = result_ds,
                           .schema = result_schema,
                           .schema_source = SchemaSource::kSchemaDatabag};
  RETURN_IF_ERROR(processor.ExtractSlice(slice));
  return std::make_pair(std::move(result_ds), std::move(result_schema));
}

absl::StatusOr<std::pair<DataItem, DataItem>> ShallowCloneOp::operator()(
    const DataItem& item, const DataItem& schema, const DataBagImpl& databag,
    DataBagImpl::FallbackSpan fallbacks, const DataBagImpl& schema_databag,
    DataBagImpl::FallbackSpan schema_fallbacks) const {
  internal::ShallowCloneOp clone_op(new_databag_);
  ASSIGN_OR_RETURN((auto [result_slice_impl, result_schema_impl]),
                   clone_op(DataSliceImpl::Create(1, item), schema, databag,
                            fallbacks, schema_databag, schema_fallbacks));
  return std::make_pair(result_slice_impl[0], std::move(result_schema_impl));
}

}  // namespace koladata::internal
