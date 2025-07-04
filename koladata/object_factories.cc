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
#include "koladata/object_factories.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/array_ops.h"
#include "arolla/qexpr/operators/dense_array/logic_ops.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "koladata/adoption_utils.h"
#include "koladata/alloc_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/error_repr_utils.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/error.h"
#include "koladata/internal/op_utils/has.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/uuid_object.h"
#include "koladata/shape_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

namespace {

using ::koladata::internal::AllocationId;
using ::koladata::internal::DataItem;
using ::koladata::internal::DataSliceImpl;
using ::koladata::internal::ObjectId;

constexpr absl::string_view kListSchemaSeed = "__list_schema__";
constexpr absl::string_view kDictSchemaSeed = "__dict_schema__";

absl::Status VerifyNoSchemaArg(absl::Span<const absl::string_view> attr_names) {
  if (std::find(attr_names.begin(), attr_names.end(), "schema") !=
      attr_names.end()) {
    return absl::InvalidArgumentError(
        "please use new_...() instead of obj_...() to create items with a given"
        " schema");
  }
  return absl::OkStatus();
}

// Returns an entity (with `db` attached) that can be created either from
// DataSliceImpl(s) or DataItem(s).
template <class ImplT>
absl::StatusOr<DataSlice> CreateEntitiesFromFields(
    const DataBagPtr& db,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> aligned_values, internal::DataItem schema,
    internal::DataBagImpl& db_mutable_impl) {
  DCHECK(&db->GetImpl() == &db_mutable_impl);
  std::vector<std::reference_wrapper<const ImplT>> aligned_values_impl;
  aligned_values_impl.reserve(aligned_values.size());
  for (const auto& val : aligned_values) {
    aligned_values_impl.push_back(std::cref(val.impl<ImplT>()));
  }
  ASSIGN_OR_RETURN(auto ds_impl, db_mutable_impl.CreateObjectsFromFields(
                                     attr_names, aligned_values_impl));
  return DataSlice::Create(std::move(ds_impl),
                           aligned_values.begin()->GetShape(),
                           std::move(schema),
                           db);
}

template <class ImplT>
absl::Status SetObjectSchema(
    internal::DataBagImpl& db_mutable_impl, const ImplT& ds_impl,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const DataItem>> schemas) {
  ASSIGN_OR_RETURN(
      auto schema_impl,
      CreateUuidWithMainObject<internal::ObjectId::kUuidImplicitSchemaFlag>(
          ds_impl, schema::kImplicitSchemaSeed));
  RETURN_IF_ERROR(
      db_mutable_impl.SetSchemaFields<ImplT>(schema_impl, attr_names, schemas));
  RETURN_IF_ERROR(
      db_mutable_impl.SetAttr(ds_impl, schema::kSchemaAttr, schema_impl));
  return absl::OkStatus();
}

// Specialization of SetObjectSchema to overwrite fields schema fields for the
// entire allocation.
absl::Status OverwriteObjectSchemaForEntireAllocation(
    internal::DataBagImpl& db_mutable_impl, const DataSliceImpl& ds_impl,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const std::reference_wrapper<const internal::DataItem>>
        schemas) {
  if (ds_impl.is_empty_and_unknown()) {
    return absl::OkStatus();
  }
  // sanity checks
  DCHECK_EQ(ds_impl.dtype(), arolla::GetQType<ObjectId>());
  DCHECK_EQ(ds_impl.present_count(), ds_impl.size());
  DCHECK((ds_impl.allocation_ids().empty() &&
          ds_impl.allocation_ids().contains_small_allocation_id()) ||
         (ds_impl.allocation_ids().size() == 1 &&
          !ds_impl.allocation_ids().contains_small_allocation_id()))
      << "DataSlice must be an entire allocation";
  ASSIGN_OR_RETURN(auto schema_first_obj,
                   CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
                       ds_impl[0], schema::kImplicitSchemaSeed));
  auto schema_alloc = AllocationId(schema_first_obj.value<ObjectId>());

  RETURN_IF_ERROR(db_mutable_impl.SetSchemaFieldsForEntireAllocation(
      schema_alloc, ds_impl.size(), attr_names, schemas));
  auto schema_impl =
      DataSliceImpl::ObjectsFromAllocation(schema_alloc, ds_impl.size());
  RETURN_IF_ERROR(
      db_mutable_impl.SetAttr(ds_impl, schema::kSchemaAttr, schema_impl));
  return absl::OkStatus();
}

// Returns an object (with `db` attached) that can be created either from
// DataSliceImpl(s) or DataItem(s). Compared to CreateEntitiesFromFields it
// creates Implicit schema for each allocated object and sets normal attribute
// `__schema__` to these newly allocated implicit schemas.
//
// DataSlice-level schema is set to `OBJECT`.
template <class ImplT>
absl::StatusOr<DataSlice> CreateObjectsFromFields(
    const DataBagPtr& db, absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> aligned_values) {
  std::vector<std::reference_wrapper<const ImplT>> aligned_values_impl;
  aligned_values_impl.reserve(aligned_values.size());
  std::vector<std::reference_wrapper<const internal::DataItem>> schemas;
  schemas.reserve(aligned_values.size());
  for (const auto& val : aligned_values) {
    aligned_values_impl.push_back(std::cref(val.impl<ImplT>()));
    schemas.push_back(std::cref(val.GetSchemaImpl()));
  }
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  ASSIGN_OR_RETURN(auto ds_impl, db_mutable_impl.CreateObjectsFromFields(
                                     attr_names, aligned_values_impl));

  if constexpr (std::is_same_v<ImplT, internal::DataSliceImpl>) {
    RETURN_IF_ERROR(OverwriteObjectSchemaForEntireAllocation(
        db_mutable_impl, ds_impl, attr_names, schemas));
  } else {
    RETURN_IF_ERROR(
        SetObjectSchema(db_mutable_impl, ds_impl, attr_names, schemas));
  }

  return DataSlice::Create(std::move(ds_impl),
                           aligned_values.begin()->GetShape(),
                           internal::DataItem(schema::kObject), db);
}

absl::Status DefaultInitItemIdType(const DataSlice& itemid,
                                   const DataBagPtr& db) {
  return absl::OkStatus();
}

absl::Status InitItemIdsForLists(const DataSlice& itemid,
                                 const DataBagPtr& db) {
  // itemid is guaranteed to have ITEMID schema.
  bool contains_only_lists =
      itemid.VisitImpl([]<typename T>(const T& impl) -> bool {
        return impl.ContainsOnlyLists();
      });
  if (!contains_only_lists) {
    return absl::InvalidArgumentError(
        "itemid argument to list creation, requires List ItemIds");
  }
  return itemid.WithBag(db).ClearDictOrList();
}

absl::Status InitItemIdsForDicts(const DataSlice& itemid,
                                 const DataBagPtr& db) {
  // itemid is guaranteed to have ITEMID schema.
  bool contains_only_dicts =
      itemid.VisitImpl([]<typename T>(const T& impl) -> bool {
        return impl.ContainsOnlyDicts();
      });
  if (!contains_only_dicts) {
    return absl::InvalidArgumentError(
        "itemid argument to dict creation, requires Dict ItemIds");
  }
  return itemid.WithBag(db).ClearDictOrList();
}

// Verifies that the given itemid is valid for creating Koda Items, optionally
// filtering it to the given mask first. Returns the filtered itemid,
// which is guaranteed to be full when required_mask == std::nullopt, or have
// exactly the same sparsity as required_mask otherwise. It is also guaranteed
// to not have duplicates.
//
// NOTE: itemid's attached DataBag is ignored if present.
absl::StatusOr<DataSlice> VerifyAndFilterItemId(
    const DataSlice& itemid, const DataSlice::JaggedShape& shape,
    const std::optional<DataSlice>& required_mask) {
  if (itemid.GetSchemaImpl() != schema::kItemId) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "`itemid` expected ITEMID schema, got %v", itemid.GetSchemaImpl()));
  }
  if (!itemid.GetShape().IsEquivalentTo(shape)) {
    return absl::InvalidArgumentError(
        absl::StrFormat("the shape of `itemid` %s is different from the shape "
                        "of the resulting DataSlice: %s",
                        arolla::Repr(itemid), arolla::Repr(shape)));
  }
  int64_t required_count = required_mask.has_value()
                               ? required_mask->present_count()
                               : itemid.size();
  if (itemid.present_count() < required_count) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "`itemid` only has %d present items but %d are required",
        itemid.present_count(), required_count));
  }
  if (itemid.present_count() == 0) {
    return itemid;
  }
  if (itemid.dtype() != arolla::GetQType<internal::ObjectId>()) {
    return absl::InternalError("`itemid` must have ItemIds");
  }
  if (itemid.is_item()) {
    return required_count == 0
               ? DataSlice::Create(internal::DataItem(), itemid.GetSchemaImpl())
               : itemid;
  }
  arolla::EvaluationContext ctx;
  auto itemid_array = itemid.slice().values<internal::ObjectId>();
  if (required_count < itemid.size()) {
    DCHECK(required_mask.has_value());
    ASSIGN_OR_RETURN(itemid_array,
                     arolla::DenseArrayPresenceAndOp()(
                         &ctx, itemid_array,
                         internal::PresenceDenseArray(required_mask->slice())));
    RETURN_IF_ERROR(ctx.status());
    if (itemid_array.PresentCount() != required_count) {
      return absl::InvalidArgumentError(
          "`itemid` and `shape_and_mask_from` must have the same sparsity");
    }
  }
  auto unique_itemid = arolla::DenseArrayUniqueOp()(&ctx, itemid_array);
  RETURN_IF_ERROR(ctx.status());
  if (unique_itemid.PresentCount() != required_count) {
    return absl::InvalidArgumentError("`itemid` cannot have duplicate ItemIds");
  }
  // TODO: keep only necessary allocation ids.
  return DataSlice::Create(
      internal::DataSliceImpl::CreateObjectsDataSlice(
          std::move(itemid_array), itemid.slice().allocation_ids()),
      itemid.GetShape(), itemid.GetSchemaImpl());
}

// Creates a DataSlice with objects constructed by allocate_single_fn /
// allocate_many_fn and shape + sparsity of shape_and_mask_from.
template <typename AllocateSingleFn, typename AllocateManyFn,
          typename InitItemIdFn>
absl::StatusOr<DataSlice> CreateLike(const DataBagPtr& db,
                                     const DataSlice& shape_and_mask_from,
                                     const internal::DataItem& schema,
                                     AllocateSingleFn allocate_single_fn,
                                     AllocateManyFn allocate_many_fn,
                                     const std::optional<DataSlice>& itemid,
                                     InitItemIdFn init_itemid_fn) {
  if (itemid) {
    ASSIGN_OR_RETURN(
        auto filtered_itemid,
        VerifyAndFilterItemId(*itemid, shape_and_mask_from.GetShape(),
                              shape_and_mask_from));
    RETURN_IF_ERROR(init_itemid_fn(filtered_itemid, db));
    return filtered_itemid.VisitImpl([&](const auto& impl) {
      return DataSlice::Create(impl, filtered_itemid.GetShape(), schema, db);
    });
  } else {
    return AllocateLike(shape_and_mask_from, allocate_single_fn,
                        allocate_many_fn, schema, db);
  }
}

// Creates a DataSlice with objects constructed by allocate_single_fn /
// allocate_many_fn and shape.
template <typename AllocateSingleFn, typename AllocateManyFn,
          typename InitItemIdFn>
absl::StatusOr<DataSlice> CreateShaped(const DataBagPtr& db,
                                       DataSlice::JaggedShape shape,
                                       const internal::DataItem& schema,
                                       AllocateSingleFn allocate_single_fn,
                                       AllocateManyFn allocate_many_fn,
                                       const std::optional<DataSlice>& itemid,
                                       InitItemIdFn init_itemid_fn) {
  if (itemid) {
    ASSIGN_OR_RETURN(auto filtered_itemid,
                     VerifyAndFilterItemId(*itemid, shape, std::nullopt));
    // There is no filtering happening in the line above, but we keep using
    // filtered_itemid below for consistency with CreateLike implementation.
    RETURN_IF_ERROR(init_itemid_fn(filtered_itemid, db));
    return filtered_itemid.VisitImpl([&](const auto& impl) {
      return DataSlice::Create(impl, filtered_itemid.GetShape(), schema, db);
    });
  }
  if (shape.rank() == 0) {
    return DataSlice::Create(
        internal::DataItem(allocate_single_fn()),
        std::move(shape), schema, db);
  } else {
    size_t size = shape.size();
    return DataSlice::Create(
        internal::DataSliceImpl::ObjectsFromAllocation(
            allocate_many_fn(size), size),
        std::move(shape), schema, db);
  }
}

// Deduces result item schema for the factory functions that accept `values` and
// `item_schema`.
// NOTE: the function does not verify that `values` and `item_schema` are
// compatible, it is done later during assignment.
absl::StatusOr<DataSlice> DeduceItemSchema(
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& item_schema) {
  if (item_schema.has_value()) {
    return *item_schema;
  }
  if (values.has_value()) {
    return values->GetSchema();
  }
  return DataSlice::Create(internal::DataItem(schema::kObject),
                           internal::DataItem(schema::kSchema));
}

// Creates list schema with the given item schema.
absl::StatusOr<internal::DataItem> CreateListSchemaItem(
    const DataBagPtr& db, const DataSlice& item_schema) {
  RETURN_IF_ERROR(item_schema.VerifyIsSchema());
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  return db_mutable_impl.CreateUuSchemaFromFields(
      kListSchemaSeed,
      {"__items__"}, {item_schema.item()});
}

// Creates dict schema with the given keys and values schemas.
absl::StatusOr<internal::DataItem> CreateDictSchemaItem(
    const DataBagPtr& db, const DataSlice& key_schema,
    const DataSlice& value_schema) {
  RETURN_IF_ERROR(key_schema.VerifyIsSchema());
  RETURN_IF_ERROR(value_schema.VerifyIsSchema());
  RETURN_IF_ERROR(schema::VerifyDictKeySchema(key_schema.item()));
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  return db_mutable_impl.CreateUuSchemaFromFields(
      kDictSchemaSeed, {"__keys__", "__values__"},
      {key_schema.item(), value_schema.item()});
}

// Implementation of CreateDictLike and CreateDictShaped that handles schema
// deduction and keys/values assignment. `create_dicts_fn` must create a
// DataSlice with the provided schema.
template <typename CreateDictsFn>
absl::StatusOr<DataSlice> CreateDictImpl(
    const DataBagPtr& db,
    const CreateDictsFn& create_dicts_fn,
    const std::optional<DataSlice>& keys,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& key_schema,
    const std::optional<DataSlice>& value_schema) {
  internal::DataItem dict_schema;
  AdoptionQueue schema_adoption_queue;
  if (schema) {
    if (key_schema.has_value() || value_schema.has_value()) {
      return absl::InvalidArgumentError(
          "creating dicts with schema accepts either a dict schema or key/value"
          " schemas, but not both");
    }
    RETURN_IF_ERROR(schema->VerifyIsDictSchema());
    dict_schema = schema->item();
    if (schema->GetBag() != db) {
      schema_adoption_queue.Add(*schema);
    }
  } else {
    ASSIGN_OR_RETURN(auto deduced_key_schema,
                     DeduceItemSchema(keys, key_schema));
    ASSIGN_OR_RETURN(auto deduced_value_schema,
                     DeduceItemSchema(values, value_schema));
    ASSIGN_OR_RETURN(dict_schema, CreateDictSchemaItem(db, deduced_key_schema,
                                                       deduced_value_schema));
    if (key_schema && key_schema->GetBag() != db) {
      schema_adoption_queue.Add(*key_schema);
    }
    if (value_schema && value_schema->GetBag() != db) {
      schema_adoption_queue.Add(*value_schema);
    }
  }
  RETURN_IF_ERROR(schema_adoption_queue.AdoptInto(*db));
  ASSIGN_OR_RETURN(DataSlice res, create_dicts_fn(dict_schema));
  if (keys.has_value() && values.has_value()) {
    RETURN_IF_ERROR(res.SetInDict(*keys, *values));
  } else if (keys.has_value()) {
    return absl::InvalidArgumentError(
        "creating a dict requires both keys and values, got only keys");
  } else if (values.has_value()) {
    return absl::InvalidArgumentError(
        "creating a dict requires both keys and values, got only values");
  }
  return res;
}

// Adopts elements of first arg into `db`.
absl::Status AdoptValuesInto(absl::Span<const DataSlice> values,
                             DataBag& db) {
  AdoptionQueue adoption_queue;
  for (size_t i = 0; i < values.size(); ++i) {
    adoption_queue.Add(values[i]);
  }
  return adoption_queue.AdoptInto(db);
}

auto KodaErrorCausedByIncompableSchemaError(const DataBagPtr& lhs_bag,
                                            absl::Span<const DataSlice> slices,
                                            const DataSlice& result_ds) {
  return [&lhs_bag, slices, &result_ds](auto&& status_like) {
    return KodaErrorCausedByIncompableSchemaError(
        std::forward<decltype(status_like)>(status_like), lhs_bag, slices,
        result_ds);
  };
}

auto KodaErrorCausedByIncompableSchemaError(const DataBagPtr& lhs_bag,
                                            const DataBagPtr& rhs_bag,
                                            const DataSlice& ds) {
  return [&lhs_bag, &rhs_bag, &ds](auto&& status_like) {
    return KodaErrorCausedByIncompableSchemaError(
        std::forward<decltype(status_like)>(status_like), lhs_bag, rhs_bag, ds);
  };
}

// Implementation of EntityCreator -Shaped and -Like that handles assignment of
// attributes and provided schema. `create_entities_fn` must create a DataSlice
// with appropriate shape and sparsity with the provided schema item.
template <typename CreateEntitiesFn>
absl::StatusOr<DataSlice> CreateEntitiesImpl(
    const DataBagPtr& db,
    const CreateEntitiesFn& create_entities_fn,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values,
    const std::optional<DataSlice>& schema,
    bool overwrite_schema) {
  internal::DataItem schema_item;
  if (schema) {
    // Copy schema into db before setting attributes for proper casting /
    // error reporting.
    if (schema->GetBag() != db) {
      AdoptionQueue schema_adoption_queue;
      schema_adoption_queue.Add(*schema);
      RETURN_IF_ERROR(schema_adoption_queue.AdoptInto(*db));
    }
    RETURN_IF_ERROR(schema->WithBag(db).VerifyIsEntitySchema());
    schema_item = schema->item();
  } else {
    // New schema is allocated, but `SetAttrs` updates empty schema based on
    // attribute schemas.
    schema_item = internal::DataItem(internal::AllocateExplicitSchema());
  }
  ASSIGN_OR_RETURN(DataSlice res, create_entities_fn(schema_item));
  RETURN_IF_ERROR(res.SetAttrs(attr_names, values, overwrite_schema))
      .With(KodaErrorCausedByIncompableSchemaError(db, values, res));
  // Adopt into the databag only at the end to avoid garbage in the databag in
  // case of error.
  // NOTE: This will cause 2 merges of the same DataBag, if schema comes from
  // the same DataBag as values.
  RETURN_IF_ERROR(AdoptValuesInto(values, *db));
  return res;
}

// Implementation of ObjectCreator -Shaped and -Like that handles assignment of
// attributes. `create_objects_fn` must create a DataSlice with appropriate
// shape and sparsity with OBJECT schema.
template <typename CreateObjectsFn>
absl::StatusOr<DataSlice> CreateObjectsImpl(
    const DataBagPtr& db,
    const CreateObjectsFn& create_objects_fn,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values) {
  RETURN_IF_ERROR(VerifyNoSchemaArg(attr_names));
  ASSIGN_OR_RETURN(DataSlice res, create_objects_fn());
  RETURN_IF_ERROR(res.VisitImpl([&](const auto& impl) -> absl::Status {
    ASSIGN_OR_RETURN(
        auto schema_impl,
        CreateUuidWithMainObject<internal::ObjectId::kUuidImplicitSchemaFlag>(
            impl, schema::kImplicitSchemaSeed));
    ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                     db->GetMutableImpl());
    RETURN_IF_ERROR(
        db_mutable_impl.SetAttr(impl, schema::kSchemaAttr, schema_impl));
    return absl::OkStatus();
  }));
  RETURN_IF_ERROR(res.SetAttrs(attr_names, values))
      .With(KodaErrorCausedByIncompableSchemaError(db, values, res));
  // Adopt into the databag only at the end to avoid garbage in the databag
  // in case of error.
  RETURN_IF_ERROR(AdoptValuesInto(values, *db));
  return res;
}

}  // namespace

// TODO: When DataSlice::SetAttrs is fast enough keep only -Shaped
// and -Like creation and forward to -Shaped here.
absl::StatusOr<DataSlice> EntityCreator::FromAttrs(
    const DataBagPtr& db,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values,
    const std::optional<DataSlice>& schema,
    bool overwrite_schema,
    const std::optional<DataSlice>& itemid) {
  if (itemid) {
    return EntityCreator::Shaped(db, itemid->GetShape(), attr_names, values,
                                 schema, overwrite_schema, itemid);
  }
  DCHECK_EQ(attr_names.size(), values.size());
  internal::DataItem schema_item;
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  std::vector<DataSlice> aligned_values;
  if (schema) {
    // Copy schema into db before setting attributes for proper casting /
    // error reporting.
    AdoptionQueue schema_adoption_queue;
    schema_adoption_queue.Add(*schema);
    RETURN_IF_ERROR(schema_adoption_queue.AdoptInto(*db));
    RETURN_IF_ERROR(schema->WithBag(db).VerifyIsEntitySchema());
    schema_item = schema->item();
  }
  if (values.empty()) {
    if (!schema_item.has_value()) {
      schema_item = internal::DataItem(internal::AllocateExplicitSchema());
    }
    return DataSlice::Create(
        internal::DataItem(internal::AllocateSingleObject()),
        std::move(schema_item), db);
  }
  if (schema_item.has_value()) {
    std::vector<DataSlice> casted_values;
    casted_values.reserve(values.size());
    for (int i = 0; i < values.size(); ++i) {
      ASSIGN_OR_RETURN(
          casted_values.emplace_back(),
          CastOrUpdateSchema(values[i], schema_item, attr_names[i],
                             overwrite_schema, db_mutable_impl),
          // Adds the db from schema to assemble readable error message.
          KodaErrorCausedByIncompableSchemaError(_, db, values[i].GetBag(),
                                                 values[i]));
    }
    ASSIGN_OR_RETURN(
        aligned_values, shape::Align(std::move(casted_values)),
        KodaErrorCausedByShapeAlignmentError(std::move(_), attr_names, values));
  } else {
    std::vector<std::reference_wrapper<const internal::DataItem>> schemas;
    schemas.reserve(values.size());
    for (const auto& val : values) {
      schemas.push_back(std::cref(val.GetSchemaImpl()));
    }
    if (!schema_item.has_value()) {
      ASSIGN_OR_RETURN(
          schema_item,
          db_mutable_impl.CreateExplicitSchemaFromFields(attr_names, schemas));
    }
    ASSIGN_OR_RETURN(
        aligned_values, shape::Align(values),
        KodaErrorCausedByShapeAlignmentError(std::move(_), attr_names, values));
  }
  std::optional<DataSlice> res;
  // All DataSlices have the same shape at this point and thus the same internal
  // representation, so we pick any of them to dispatch the object creation by
  // internal implementation type.
  RETURN_IF_ERROR(aligned_values.begin()->VisitImpl(
      [&]<class T>(const T& impl) -> absl::Status {
        ASSIGN_OR_RETURN(res, CreateEntitiesFromFields<T>(
                                  db, attr_names, aligned_values,
                                  std::move(schema_item), db_mutable_impl));
        return absl::OkStatus();
      }));
  // Adopt into the databag only at the end to avoid garbage in the databag in
  // case of error.
  // NOTE: This will cause 2 merges of the same DataBag, if schema comes from
  // the same DataBag as values.
  RETURN_IF_ERROR(AdoptValuesInto(values, *db));
  return *std::move(res);
}

absl::StatusOr<DataSlice> EntityCreator::Shaped(
    const DataBagPtr& db, DataSlice::JaggedShape shape,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values,
    const std::optional<DataSlice>& schema,
    bool overwrite_schema,
    const std::optional<DataSlice>& itemid) {
  return CreateEntitiesImpl(
      db,
      [&](const internal::DataItem& schema_item) {
        return CreateShaped(db, std::move(shape), schema_item,
                            internal::AllocateSingleObject,
                            internal::Allocate,
                            itemid,
                            DefaultInitItemIdType);
      },
      attr_names, values, schema, overwrite_schema);
}

absl::StatusOr<DataSlice> EntityCreator::Like(
    const DataBagPtr& db, const DataSlice& shape_and_mask_from,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values,
    const std::optional<DataSlice>& schema,
    bool overwrite_schema,
    const std::optional<DataSlice>& itemid) {
  return CreateEntitiesImpl(
      db,
      [&](const internal::DataItem& schema_item) {
        return CreateLike(db, shape_and_mask_from, schema_item,
                          internal::AllocateSingleObject,
                          internal::Allocate,
                          itemid,
                          DefaultInitItemIdType);
      }, attr_names, values, schema, overwrite_schema);
}

// TODO: When DataSlice::SetAttrs is fast enough keep only -Shaped
// and -Like creation and forward to -Shaped here.
absl::StatusOr<DataSlice> ObjectCreator::FromAttrs(
    const DataBagPtr& db, absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values,
    const std::optional<DataSlice>& itemid) {
  DCHECK_EQ(attr_names.size(), values.size());
  if (itemid) {
    return ObjectCreator::Shaped(db, itemid->GetShape(), attr_names, values,
                                 itemid);
  }
  if (values.empty()) {
    return ObjectCreator::Shaped(db, DataSlice::JaggedShape::Empty(), {}, {});
  }
  RETURN_IF_ERROR(VerifyNoSchemaArg(attr_names));
  ASSIGN_OR_RETURN(
      auto aligned_values, shape::Align(values),
      KodaErrorCausedByShapeAlignmentError(std::move(_), attr_names, values));
  std::optional<DataSlice> res;
  // All DataSlices have the same shape at this point and thus the same internal
  // representation, so we pick any of them to dispatch the object creation by
  // internal implementation type.
  RETURN_IF_ERROR(aligned_values.begin()->VisitImpl(
      [&]<class T>(const T& impl) -> absl::Status {
        ASSIGN_OR_RETURN(
            res, CreateObjectsFromFields<T>(db, attr_names, aligned_values));
        return absl::OkStatus();
      }));
  // Adopt into the databag only at the end to avoid garbage in the databag in
  // case of error.
  RETURN_IF_ERROR(AdoptValuesInto(values, *db));
  return *std::move(res);
}

absl::StatusOr<DataSlice> ObjectCreator::Shaped(
    const DataBagPtr& db, DataSlice::JaggedShape shape,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values,
    const std::optional<DataSlice>& itemid) {
  return CreateObjectsImpl(
      db,
      [&]() {
        return CreateShaped(db, std::move(shape),
                            internal::DataItem(schema::kObject),
                            internal::AllocateSingleObject,
                            internal::Allocate,
                            itemid,
                            DefaultInitItemIdType);
      }, attr_names, values);
}

absl::StatusOr<DataSlice> ObjectCreator::Like(
    const DataBagPtr& db, const DataSlice& shape_and_mask_from,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values,
    const std::optional<DataSlice>& itemid) {
  return CreateObjectsImpl(
      db,
      [&]() {
        return CreateLike(db, shape_and_mask_from,
                          internal::DataItem(schema::kObject),
                          internal::AllocateSingleObject,
                          internal::Allocate,
                          itemid,
                          DefaultInitItemIdType);
      }, attr_names, values);
}

absl::StatusOr<DataSlice> ObjectCreator::ConvertWithoutAdopt(
    const DataBagPtr& db, const DataSlice& value) {
  if (!value.GetSchemaImpl().is_primitive_schema() &&
      // TODO: NONE schema is a primitive schema.
      value.GetSchemaImpl() != schema::kNone &&
      value.GetSchemaImpl() != schema::kObject) {
    return value.WithBag(db).EmbedSchema();
  }
  return value.WithSchema(internal::DataItem(schema::kObject));
}

absl::StatusOr<DataSlice> CreateUu(
    const DataBagPtr& db, absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values,
    const std::optional<DataSlice>& schema, bool overwrite_schema) {
  DCHECK_EQ(attr_names.size(), values.size());
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  internal::DataItem schema_item;
  if (schema) {
    RETURN_IF_ERROR(schema->VerifyIsSchema());
    schema_item = schema->item();
    if (!schema_item.is_struct_schema()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "processing Entity attributes requires Entity schema, got %v",
          schema_item));
    }
    // Copy schema into db before setting attributes for proper casting /
    // error reporting.
    AdoptionQueue schema_adoption_queue;
    schema_adoption_queue.Add(*schema);
    RETURN_IF_ERROR(schema_adoption_queue.AdoptInto(*db));
  }
  if (!schema_item.has_value()) {
    // Construct schema_item from values.
    std::vector<std::reference_wrapper<const internal::DataItem>> schemas;
    schemas.reserve(values.size());
    for (const auto& val : values) {
      schemas.push_back(std::cref(val.GetSchemaImpl()));
    }
    ASSIGN_OR_RETURN(schema_item, db_mutable_impl.CreateUuSchemaFromFields(
                                      seed, attr_names, schemas));
  }
  // schema_item is finalized at this point.
  if (values.empty()) {
    auto uuid = internal::CreateUuidFromFields(
        seed, {},
        std::vector<std::reference_wrapper<const internal::DataItem>>{});
    return DataSlice::Create(uuid, std::move(schema_item), db);
  }
  ASSIGN_OR_RETURN(
      auto aligned_values, shape::Align(values),
      KodaErrorCausedByShapeAlignmentError(std::move(_), attr_names, values));
  // All DataSlices have the same shape at this point and thus the same internal
  // representation, so we pick any of them to dispatch the object creation by
  // internal implementation type.
  return aligned_values.begin()->VisitImpl([&]<class ImplT>(const ImplT& impl)
                                               -> absl::StatusOr<DataSlice> {
    std::vector<std::reference_wrapper<const ImplT>> aligned_values_impl;
    aligned_values_impl.reserve(aligned_values.size());
    for (int i = 0; i < attr_names.size(); ++i) {
      aligned_values_impl.push_back(std::cref(aligned_values[i].impl<ImplT>()));
    }

    std::optional<ImplT> impl_res;
    if constexpr (std::is_same_v<internal::DataItem, ImplT>) {
      impl_res =
          internal::CreateUuidFromFields(seed, attr_names, aligned_values_impl);
    } else {
      ASSIGN_OR_RETURN(impl_res, internal::CreateUuidFromFields(
                                     seed, attr_names, aligned_values_impl));
    }
    ASSIGN_OR_RETURN(auto ds, DataSlice::Create(
                                  impl_res.value(),
                                  aligned_values.begin()->GetShape(),
                                  std::move(schema_item), db));
    RETURN_IF_ERROR(ds.SetAttrs(attr_names, aligned_values, overwrite_schema))
        .With(KodaErrorCausedByIncompableSchemaError(ds.GetBag(), values, ds));
    // Adopt into the databag only at the end to avoid garbage in the
    // databag in case of error. NOTE: This will cause 2 merges of the
    // same DataBag, if schema comes from the same DataBag as values.
    RETURN_IF_ERROR(AdoptValuesInto(values, *db));
    return ds;
  });
}

absl::StatusOr<DataSlice> CreateUuObject(
    const DataBagPtr& db, absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values) {
  DCHECK_EQ(attr_names.size(), values.size());
  DataSlice ds;
  if (values.empty()) {
    auto uuid = internal::CreateUuidFromFields(
        seed, {},
        std::vector<std::reference_wrapper<const internal::DataItem>>{});
    ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                     db->GetMutableImpl());
    RETURN_IF_ERROR(SetObjectSchema(db_mutable_impl, uuid, {}, {}));
    return DataSlice::Create(uuid, internal::DataItem(schema::kObject), db);
  }
  ASSIGN_OR_RETURN(
      auto aligned_values, shape::Align(values),
      KodaErrorCausedByShapeAlignmentError(std::move(_), attr_names, values));

  std::vector<std::reference_wrapper<const internal::DataItem>> schemas;
  schemas.reserve(aligned_values.size());
  for (const auto& slice : aligned_values) {
    schemas.push_back(std::cref(slice.GetSchemaImpl()));
  }

  return aligned_values.begin()->VisitImpl(
      [&]<class ImplT>(ImplT) -> absl::StatusOr<DataSlice> {
        std::vector<std::reference_wrapper<const ImplT>> aligned_values_impl;
        aligned_values_impl.reserve(aligned_values.size());
        for (int i = 0; i < attr_names.size(); ++i) {
          aligned_values_impl.push_back(
              std::cref(aligned_values[i].impl<ImplT>()));
        }

        std::optional<ImplT> impl_res;
        if constexpr (std::is_same_v<internal::DataItem, ImplT>) {
          impl_res = internal::CreateUuidFromFields(seed, attr_names,
                                                    aligned_values_impl);
        } else {
          ASSIGN_OR_RETURN(impl_res,
                           internal::CreateUuidFromFields(seed, attr_names,
                                                          aligned_values_impl));
        }
        ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                         db->GetMutableImpl());
        for (int i = 0; i < attr_names.size(); ++i) {
          RETURN_IF_ERROR(db_mutable_impl.SetAttr(*impl_res, attr_names[i],
                                                  aligned_values_impl[i]));
        }
        RETURN_IF_ERROR(SetObjectSchema(db_mutable_impl, impl_res.value(),
                                        attr_names, schemas));
        // Adopt into the databag only at the end to avoid garbage in the
        // databag in case of error.
        RETURN_IF_ERROR(AdoptValuesInto(values, *db));
        return DataSlice::Create(
            impl_res.value(), aligned_values.begin()->GetShape(),
            internal::DataItem(schema::kObject), db);
      });
}

absl::StatusOr<DataSlice> CreateEntitySchema(
    const DataBagPtr& db,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> schemas) {
  DCHECK_EQ(attr_names.size(), schemas.size());
  std::vector<std::reference_wrapper<const internal::DataItem>> schema_items;
  schema_items.reserve(schemas.size());
  for (const DataSlice& schema : schemas) {
    RETURN_IF_ERROR(schema.VerifyIsSchema());
    schema_items.push_back(std::cref(schema.item()));
  }
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  ASSIGN_OR_RETURN(
      auto schema_id,
      db_mutable_impl.CreateExplicitSchemaFromFields(attr_names, schema_items));
  return DataSlice::Create(schema_id, internal::DataItem(schema::kSchema), db);
}

absl::StatusOr<DataSlice> CreateUuSchema(
    const DataBagPtr& db,
    absl::string_view seed,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> schemas) {
  DCHECK_EQ(attr_names.size(), schemas.size());
  std::vector<std::reference_wrapper<const internal::DataItem>> schema_items;
  schema_items.reserve(schemas.size());
  for (const DataSlice& schema : schemas) {
    RETURN_IF_ERROR(schema.VerifyIsSchema());
    schema_items.push_back(std::cref(schema.item()));
  }
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  ASSIGN_OR_RETURN(auto schema_id, db_mutable_impl.CreateUuSchemaFromFields(
                                       seed, attr_names, schema_items));
  ASSIGN_OR_RETURN(
      auto result,
      DataSlice::Create(schema_id, internal::DataItem(schema::kSchema), db));
  RETURN_IF_ERROR(AdoptValuesInto(schemas, *db));
  return result;
}

absl::StatusOr<DataSlice> CreateNamedSchema(
    const DataBagPtr& db, absl::string_view name,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> schemas) {
  // Note that we do not pass attributes here since we do not want the schema
  // id to depend on them.
  ASSIGN_OR_RETURN(
      auto res,
      CreateUuSchema(db, absl::StrCat("__named_schema__", name), {}, {}));

  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  RETURN_IF_ERROR(db_mutable_impl.SetSchemaAttr(
      res.item(), schema::kSchemaNameAttr, DataItem(arolla::Text(name))));

  RETURN_IF_ERROR(res.SetAttrs(attr_names, schemas,
                               /*overwrite_schema=*/false));
  RETURN_IF_ERROR(AdoptValuesInto(schemas, *db));
  return res;
}

absl::StatusOr<DataSlice> CreateMetadata(const DataBagPtr& db,
                                         const DataSlice& slice) {
  if (!slice.GetSchemaImpl().is_schema_schema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("failed to create metadata; cannot create for a "
                        "DataSlice with %v schema",
                        SchemaToStr(slice.GetSchema())));
  }
  if (db == nullptr) {
    return absl::InvalidArgumentError(
        absl::StrFormat("failed to create metadata; the DataSlice "
                        "is a reference without a bag"));
  }
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  ASSIGN_OR_RETURN(
      auto metadata,
      slice.VisitImpl([&]<class T>(
                          const T& ds_impl) -> absl::StatusOr<DataSlice> {
        ASSIGN_OR_RETURN(T metadata_impl, internal::CreateUuidWithMainObject(
                                              ds_impl, schema::kMetadataSeed));
        RETURN_IF_ERROR(db_mutable_impl.SetSchemaAttr(
            ds_impl, schema::kSchemaMetadataAttr, metadata_impl));
        return DataSlice::Create(std::move(metadata_impl), slice.GetShape(),
                                 internal::DataItem(schema::kItemId), db);
      }));
  return ObjectCreator::Like(db, slice, {}, {}, std::move(metadata));
}

absl::StatusOr<DataSlice> CreateSchema(
    const DataBagPtr& db,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> schemas) {
  ASSIGN_OR_RETURN(auto result, CreateEntitySchema(db, attr_names, schemas));
  RETURN_IF_ERROR(AdoptValuesInto(schemas, *db));
  return result;
}

absl::StatusOr<DataSlice> CreateListSchema(const DataBagPtr& db,
                                           const DataSlice& item_schema) {
  return CreateUuSchema(db, kListSchemaSeed, {"__items__"}, {item_schema});
}

absl::StatusOr<DataSlice> CreateDictSchema(const DataBagPtr& db,
                                           const DataSlice& key_schema,
                                           const DataSlice& value_schema) {
  RETURN_IF_ERROR(key_schema.VerifyIsSchema());
  RETURN_IF_ERROR(schema::VerifyDictKeySchema(key_schema.item()));
  return CreateUuSchema(db, kDictSchemaSeed, {"__keys__", "__values__"},
                        {key_schema, value_schema});
}

absl::StatusOr<DataSlice> CreateDictLike(
    const DataBagPtr& db, const DataSlice& shape_and_mask_from,
    const std::optional<DataSlice>& keys,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& key_schema,
    const std::optional<DataSlice>& value_schema,
    const std::optional<DataSlice>& itemid) {
  return CreateDictImpl(
      db,
      [&](const auto& schema) {
        return CreateLike(db, shape_and_mask_from, schema,
                          internal::AllocateSingleDict,
                          internal::AllocateDicts,
                          itemid,
                          InitItemIdsForDicts);
      },
      keys, values, schema, key_schema, value_schema);
}

absl::StatusOr<DataSlice> CreateDictShaped(
    const DataBagPtr& db, DataSlice::JaggedShape shape,
    const std::optional<DataSlice>& keys,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& key_schema,
    const std::optional<DataSlice>& value_schema,
    const std::optional<DataSlice>& itemid) {
  return CreateDictImpl(
      db,
      [&](const auto& schema) {
        return CreateShaped(db, std::move(shape), schema,
                            internal::AllocateSingleDict,
                            internal::AllocateDicts,
                            itemid,
                            InitItemIdsForDicts);
      },
      keys, values, schema, key_schema, value_schema);
}

absl::StatusOr<DataSlice> CreateEmptyList(
    const DataBagPtr& db, const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& item_schema,
    const std::optional<DataSlice>& itemid) {
  auto shape = itemid ? itemid->GetShape() : DataSlice::JaggedShape::Empty();
  return CreateListShaped(db, std::move(shape), /*values=*/std::nullopt, schema,
                          item_schema, itemid);
}

absl::StatusOr<DataSlice> CreateListsFromLastDimension(
    const DataBagPtr& db, const DataSlice& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& item_schema,
    const std::optional<DataSlice>& itemid) {
  size_t rank = values.GetShape().rank();
  if (rank == 0) {
    return absl::InvalidArgumentError(
        "creating a list from values requires at least one dimension");
  }
  return CreateListShaped(db, values.GetShape().RemoveDims(rank - 1), values,
                          schema, item_schema, itemid);
}

absl::StatusOr<DataSlice> CreateNestedList(
    const DataBagPtr& db, const DataSlice& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& item_schema,
    const std::optional<DataSlice>& itemid) {
  // NOTE: CreateListShaped deals with consistency of values and passed schema
  // args (called from CreateListsFromLastDimension below).
  ASSIGN_OR_RETURN(
      DataSlice res,
      CreateListsFromLastDimension(
          db, values, schema, item_schema,
          values.GetShape().rank() <= 1 && itemid ? itemid : std::nullopt));
  for (size_t rank = res.GetShape().rank(); rank > 0;
       rank = res.GetShape().rank()) {
    // NOTE: If `itemid` is present, the last "implosion" of a list needs to
    // happen to `itemid` ObjectIds.
    ASSIGN_OR_RETURN(
        res,
        CreateListsFromLastDimension(
            db, res, /*schema=*/std::nullopt, /*item_schema=*/std::nullopt,
            // TODO: When itemid is provided by the user, a proper
            // nested / child itemid should be created.
            rank == 1 && itemid ? itemid : std::nullopt));
  }
  return std::move(res);
}

absl::StatusOr<DataSlice> Implode(
    const DataBagPtr& db, const DataSlice& values, int ndim,
    const std::optional<DataSlice>& itemid) {
  constexpr absl::string_view kOperatorName = "kd.implode";
  const size_t rank = values.GetShape().rank();
  if (ndim < 0) {
    ndim = values.GetShape().rank();  // ndim < 0 means implode all dimensions.
  }
  if (rank < ndim) {
    return internal::OperatorEvalError(
        kOperatorName,
        absl::StrFormat("cannot implode 'x' to fold the last %d dimension(s) "
                        "because 'x' only has %d dimensions",
                        ndim, rank));
  }

  // Adopt `values` into `db`. This covers the case when ndim == 0 when
  // CreateListsFromLastDimension is not invoked.
  AdoptionQueue adoption_queue;
  adoption_queue.Add(values);
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db)).With([&](auto&& status) {
    return internal::OperatorEvalError(
        status, kOperatorName,
        "cannot adopt input DataSlice into the new DataBag");
  });

  if (ndim == 0) {
    if (itemid.has_value()) {
      return internal::OperatorEvalError(
          kOperatorName, "does not accept 'itemid' argument when ndim==0");
    }
    return values.WithBag(db);
  }
  // Changing the `db`, because CreateListsFromLastDimension also adopts.
  std::optional<DataSlice> result = values.WithBag(db);
  for (int i = 1; i < ndim; ++i) {
    // TODO: When itemid is provided by the user, a proper nested /
    // child itemid should be created.
    ASSIGN_OR_RETURN(result, CreateListsFromLastDimension(db, *result));
  }
  ASSIGN_OR_RETURN(
      result,
      CreateListsFromLastDimension(db, *result, /*schema=*/std::nullopt,
                                   /*item_schema=*/std::nullopt, itemid));
  return *std::move(result);
}

absl::StatusOr<DataSlice> ConcatLists(const DataBagPtr& db,
                                      std::vector<DataSlice> inputs) {
  if (inputs.empty()) {
    return CreateEmptyList(db);
  }

  for (const DataSlice& input_slice : inputs) {
    if (!input_slice.IsList()) {
      return absl::InvalidArgumentError(
          "concat_lists expects all input slices to contain lists");
    }
  }

  ASSIGN_OR_RETURN(inputs, shape::Align(std::move(inputs)));
  auto result_shape = inputs[0].GetShape();
  const auto result_rank = result_shape.rank();

  // Explode each input slice once, because DataBagImpl::ExtendLists expects
  // values to come from the last DataSlice dimension, not from list objects.
  for (DataSlice& input_slice : inputs) {
    ASSIGN_OR_RETURN(input_slice, input_slice.ExplodeList(0, std::nullopt));
  }

  // Align input schemas. Because we exploded once first, this aligns the item
  // schema in the result.
  ASSIGN_OR_RETURN(auto aligned_inputs, AlignSchemas(std::move(inputs)));
  inputs = std::move(aligned_inputs.slices);

  AdoptionQueue adoption_queue;
  for (DataSlice& input_slice : inputs) {
    adoption_queue.Add(input_slice);
  }
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db));

  ASSIGN_OR_RETURN(const std::optional<DataSlice> item_schema,
                   DataSlice::Create(aligned_inputs.common_schema,
                                     internal::DataItem(schema::kSchema), db));
  ASSIGN_OR_RETURN(
      DataSlice result,
      CreateListShaped(db, std::move(result_shape), /*values=*/std::nullopt,
                       /*schema=*/std::nullopt,
                       /*item_schema=*/item_schema));

  // Note: Ideally, this would preallocate the backing vectors in the result
  // lists based on the type and size information we can get from the inputs.
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_impl, db->GetMutableImpl());
  if (result_rank > 0) {
    for (const auto& input_slice : inputs) {
      const auto& input_exploded_edge = input_slice.GetShape().edges().back();
      RETURN_IF_ERROR(db_impl.ExtendLists(result.slice(), input_slice.slice(),
                                          input_exploded_edge));
    }
  } else {
    for (const auto& input_slice : inputs) {
      RETURN_IF_ERROR(db_impl.ExtendList(result.item(), input_slice.slice()));
    }
  }

  return result;
}

absl::StatusOr<DataSlice> CreateListShaped(
    const DataBagPtr& db, DataSlice::JaggedShape shape,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& item_schema,
    const std::optional<DataSlice>& itemid) {
  internal::DataItem list_schema;
  AdoptionQueue schema_adoption_queue;
  if (schema) {
    if (item_schema.has_value()) {
      return absl::InvalidArgumentError(
          "creating lists with schema accepts either a list schema or item "
          "schema, but not both");
    }
    RETURN_IF_ERROR(schema->VerifyIsListSchema());
    list_schema = schema->item();
    if (schema->GetBag() != db) {
      schema_adoption_queue.Add(*schema);
    }
  } else {
    ASSIGN_OR_RETURN(auto deduced_item_schema,
                     DeduceItemSchema(values, item_schema));
    ASSIGN_OR_RETURN(list_schema,
                     CreateListSchemaItem(db, deduced_item_schema));
    if (item_schema && item_schema->GetBag() != db) {
      schema_adoption_queue.Add(*item_schema);
    }
  }
  RETURN_IF_ERROR(schema_adoption_queue.AdoptInto(*db));
  ASSIGN_OR_RETURN(auto result, CreateShaped(db, std::move(shape), list_schema,
                                             internal::AllocateSingleList,
                                             internal::AllocateLists,
                                             itemid,
                                             InitItemIdsForLists));
  if (values.has_value()) {
    RETURN_IF_ERROR(result.AppendToList(*values))
        .With(KodaErrorCausedByIncompableSchemaError(result.GetBag(),
                                                     values->GetBag(), result));
  }
  return std::move(result);
}

absl::StatusOr<DataSlice> CreateListLike(
    const DataBagPtr& db, const DataSlice& shape_and_mask_from,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& item_schema,
    const std::optional<DataSlice>& itemid) {
  internal::DataItem list_schema;
  AdoptionQueue schema_adoption_queue;
  if (schema) {
    if (item_schema.has_value()) {
      return absl::InvalidArgumentError(
          "creating lists with schema accepts either a list schema or item "
          "schema, but not both");
    }
    RETURN_IF_ERROR(schema->VerifyIsListSchema());
    list_schema = schema->item();
    if (schema->GetBag() != db) {
      schema_adoption_queue.Add(*schema);
    }
  } else {
    ASSIGN_OR_RETURN(auto deduced_item_schema,
                     DeduceItemSchema(values, item_schema));
    ASSIGN_OR_RETURN(list_schema,
                     CreateListSchemaItem(db, deduced_item_schema));
    if (item_schema && item_schema->GetBag() != db) {
      schema_adoption_queue.Add(*item_schema);
    }
  }
  RETURN_IF_ERROR(schema_adoption_queue.AdoptInto(*db));
  ASSIGN_OR_RETURN(auto result, CreateLike(db, shape_and_mask_from, list_schema,
                                           internal::AllocateSingleList,
                                           internal::AllocateLists,
                                           itemid,
                                           InitItemIdsForLists));
  if (values.has_value()) {
    RETURN_IF_ERROR(result.AppendToList(*values))
        .With(KodaErrorCausedByIncompableSchemaError(result.GetBag(),
                                                     values->GetBag(), result));
  }
  return result;
}

absl::StatusOr<DataSlice> CreateNoFollowSchema(const DataSlice& target_schema) {
  RETURN_IF_ERROR(target_schema.VerifyIsSchema());
  ASSIGN_OR_RETURN(auto no_follow_schema_item,
                   schema::NoFollowSchemaItem(target_schema.item()));
  return DataSlice::Create(std::move(no_follow_schema_item),
                           internal::DataItem(schema::kSchema),
                           target_schema.GetBag());
}

absl::StatusOr<DataSlice> NoFollow(const DataSlice& target) {
  ASSIGN_OR_RETURN(auto no_follow_schema_item,
                   schema::NoFollowSchemaItem(target.GetSchemaImpl()));
  return target.WithSchema(no_follow_schema_item);
}

}  // namespace koladata
