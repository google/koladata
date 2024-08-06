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
#include "koladata/object_factories.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/adoption_utils.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/uuid_object.h"
#include "koladata/repr_utils.h"
#include "koladata/shape_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

namespace {

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
    const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& aligned_values, internal::DataItem schema,
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

// Copies all schema attributes for schema `schema_item` from `schema_db` to
// `db_mutable_impl`. Returns an Error if schema is not Any or Entity or in case
// internal invariants are broken.
// TODO: This solution is mid-term, as we will either optimize it
// to rely on fast copying of dictionaries from one DataBag to another or will
// apply merging before invoking `EntityCreator`. Invoking merging will allow
// the full functionality of `update_schema=true`, which is equivalent to the
// functionality of SetAttrWithUpdateSchema.
absl::Status CopyEntitySchema(const DataBagPtr& schema_db,
                              const internal::DataItem& schema_item,
                              internal::DataBagImpl& db_mutable_impl) {
  if (schema_item == schema::kAny) {
    return absl::OkStatus();
  }
  if (!schema_item.is_entity_schema()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "processing Entity attributes requires Entity schema, got %v",
        schema_item));
  }
  if (schema_db == nullptr) {
    // NOTE: If `update_schema=true`, the attribute schemas will just be set at
    // the point of creating Entities. If `update_schema=false`, the error will
    // be returned from the caller, as attributes will have missing schema in
    // the DataBag the caller is creating Entity into.
    return absl::OkStatus();
  }
  const internal::DataBagImpl& schema_db_impl = schema_db->GetImpl();
  FlattenFallbackFinder fb_finder = FlattenFallbackFinder(*schema_db);
  auto fallbacks = fb_finder.GetFlattenFallbacks();
  ASSIGN_OR_RETURN(internal::DataSliceImpl attr_names_slice,
                   schema_db_impl.GetSchemaAttrs(schema_item, fallbacks));

  std::vector<absl::string_view> attr_names;
  attr_names.reserve(attr_names_slice.size());
  // All are present.
  attr_names_slice.values<arolla::Text>().ForEachPresent(
      [&](int64_t, absl::string_view attr_name) {
        attr_names.push_back(attr_name);
      });
  std::deque<internal::DataItem> attr_schema_owners;
  std::vector<std::reference_wrapper<const internal::DataItem>> attr_schemas;
  attr_schemas.reserve(attr_names_slice.size());
  for (const auto& attr_name : attr_names) {
    ASSIGN_OR_RETURN(
        attr_schema_owners.emplace_back(),
        schema_db_impl.GetSchemaAttrAllowMissing(schema_item, attr_name,
                                                 fallbacks));
    attr_schemas.push_back(std::cref(attr_schema_owners.back()));
  }
  return db_mutable_impl.OverwriteSchemaFields(
      schema_item, attr_names, attr_schemas);
}

absl::Status CopyListSchema(const DataSlice& list_schema,
                            const DataBagPtr& db) {
  RETURN_IF_ERROR(list_schema.VerifyIsListSchema());
  if (list_schema.GetDb() == db) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  return CopyEntitySchema(list_schema.GetDb(), list_schema.item(),
                          db_mutable_impl);
}

absl::Status CopyDictSchema(const DataSlice& dict_schema,
                            const DataBagPtr& db) {
  RETURN_IF_ERROR(dict_schema.VerifyIsDictSchema());
  if (dict_schema.GetDb() == db) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  return CopyEntitySchema(dict_schema.GetDb(), dict_schema.item(),
                          db_mutable_impl);
}

template <class ImplT>
absl::Status SetObjectSchema(
    internal::DataBagImpl& db_mutable_impl, const ImplT& ds_impl,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<std::reference_wrapper<const internal::DataItem>>&
        schemas,
    bool overwrite_schemas = true) {
  ASSIGN_OR_RETURN(
      auto schema_impl,
      CreateUuidWithMainObject<internal::ObjectId::kUuidImplicitSchemaFlag>(
          ds_impl, schema::kImplicitSchemaSeed));
  if (overwrite_schemas) {
    RETURN_IF_ERROR(db_mutable_impl.OverwriteSchemaFields<ImplT>(
        schema_impl, attr_names, schemas));
  } else {
    RETURN_IF_ERROR(db_mutable_impl.SetSchemaFields<ImplT>(
        schema_impl, attr_names, schemas));
  }
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
    const DataBagPtr& db, const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& aligned_values) {
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

  RETURN_IF_ERROR(
      SetObjectSchema(db_mutable_impl, ds_impl, attr_names, schemas));

  return DataSlice::Create(std::move(ds_impl),
                           aligned_values.begin()->GetShape(),
                           internal::DataItem(schema::kObject), db);
}

// Creates a DataSlice with objects constructed by allocate_single_func /
// allocate_many_func and shape + sparsity of shape_and_mask_from.
template <typename AllocateSingleFunc, typename AllocateManyFunc>
absl::StatusOr<DataSlice> CreateLike(const std::shared_ptr<DataBag>& db,
                                     const DataSlice& shape_and_mask_from,
                                     const internal::DataItem& schema,
                                     AllocateSingleFunc allocate_single_func,
                                     AllocateManyFunc allocate_many_func) {
  return shape_and_mask_from.VisitImpl([&]<class T>(const T& impl) {
    if constexpr (std::is_same_v<T, internal::DataItem>) {
      return DataSlice::Create(impl.has_value()
                                   ? internal::DataItem(allocate_single_func())
                                   : internal::DataItem(),
                               schema, db);
    } else {
      auto alloc_id = allocate_many_func(impl.present_count());
      arolla::DenseArrayBuilder<internal::ObjectId> result_impl_builder(
          impl.size());
      int64_t i = 0;
      impl.VisitValues([&](const auto& array) {
        array.ForEachPresent([&](int64_t id, auto _) {
          result_impl_builder.Set(id, alloc_id.ObjectByOffset(i++));
        });
      });
      return DataSlice::Create(internal::DataSliceImpl::CreateObjectsDataSlice(
                                   std::move(result_impl_builder).Build(),
                                   internal::AllocationIdSet(alloc_id)),
                               shape_and_mask_from.GetShape(), schema, db);
    }
  });
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

// Implementation of CreateDictLike and CreateDictShaped that handles schema
// deduction and keys/values assignment. `create_dicts_fn` must create a
// DataSlice with the provided schema.
template <typename CreateDictsFn>
absl::StatusOr<DataSlice> CreateDictImpl(
    const std::shared_ptr<DataBag>& db, const CreateDictsFn& create_dicts_fn,
    const std::optional<DataSlice>& keys,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& key_schema,
    const std::optional<DataSlice>& value_schema) {
  internal::DataItem dict_schema;
  if (schema) {
    if (key_schema.has_value() || value_schema.has_value()) {
      return absl::InvalidArgumentError(
          "creating dicts with schema accepts either a dict schema or key/value"
          " schemas, but not both");
    }
    RETURN_IF_ERROR(CopyDictSchema(*schema, db));
    dict_schema = schema->item();
  } else {
    ASSIGN_OR_RETURN(auto deduced_key_schema,
                     DeduceItemSchema(keys, key_schema));
    ASSIGN_OR_RETURN(auto deduced_value_schema,
                     DeduceItemSchema(values, value_schema));
    ASSIGN_OR_RETURN(dict_schema, CreateDictSchema(db, deduced_key_schema,
                                                   deduced_value_schema));
  }
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

// Implementation of EntityCreator -Shaped and -Like that handles assignment of
// attributes and provided schema. `create_entities_fn` must create a DataSlice
// with appropriate shape and sparsity with the provided schema item.
template <typename CreateEntitiesFn>
absl::StatusOr<DataSlice> CreateEntitiesImpl(
    const std::shared_ptr<DataBag>& db,
    const CreateEntitiesFn& create_entities_fn,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values,
    const std::optional<DataSlice>& schema,
    bool update_schema) {
  internal::DataItem schema_item;
  if (schema) {
    RETURN_IF_ERROR(schema->VerifyIsSchema());
    schema_item = schema->item();
    ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                     db->GetMutableImpl());
    RETURN_IF_ERROR(
        CopyEntitySchema(schema->GetDb(), schema_item, db_mutable_impl));
  } else {
    schema_item = internal::DataItem(internal::AllocateExplicitSchema());
    // New schema is allocated, so attributes should be written to it
    // successfully below.
    update_schema = true;
  }
  ASSIGN_OR_RETURN(DataSlice res, create_entities_fn(schema_item));
  RETURN_IF_ERROR(res.SetAttrs(attr_names, values, update_schema));
  return res;
}

// Implementation of ObjectCreator -Shaped and -Like that handles assignment of
// attributes. `create_objects_fn` must create a DataSlice with appropriate
// shape and sparsity with OBJECT schema.
template <typename CreateObjectsFn>
absl::StatusOr<DataSlice> CreateObjectsImpl(
    const std::shared_ptr<DataBag>& db,
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
  RETURN_IF_ERROR(res.SetAttrs(attr_names, values));
  return res;
}

}  // namespace

absl::StatusOr<DataSlice> CreateEntitySchema(
    const DataBagPtr& db, const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& schemas) {
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

absl::StatusOr<DataSlice> UuSchemaCreator::operator()(
    const DataBagPtr& db,
    absl::string_view seed,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& schemas) const {
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
      db_mutable_impl.CreateUuSchemaFromFields(seed, attr_names, schema_items));
  return DataSlice::Create(schema_id, internal::DataItem(schema::kSchema), db);
}

absl::StatusOr<DataSlice> SchemaCreator::operator()(
    const DataBagPtr& db,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& schemas) const {
  return CreateEntitySchema(db, attr_names, schemas);
}

absl::StatusOr<DataSlice> ListSchemaCreator::operator()(
    const DataBagPtr& db,
    const DataSlice& item_schema) const {
  ASSIGN_OR_RETURN(auto schema_item, CreateListSchema(db, item_schema));
  return DataSlice::Create(schema_item, internal::DataItem(schema::kSchema),
                           db);
}

absl::StatusOr<DataSlice> DictSchemaCreator::operator()(
    const DataBagPtr& db,
    const DataSlice& key_schema, const DataSlice& value_schema) const {
  ASSIGN_OR_RETURN(auto schema_item,
                   CreateDictSchema(db, key_schema, value_schema));
  return DataSlice::Create(schema_item, internal::DataItem(schema::kSchema),
                           db);
}

// TODO: When DataSlice::SetAttrs is fast enough keep only -Shaped
// and -Like creation and forward to -Shaped here.
absl::StatusOr<DataSlice> EntityCreator::FromAttrs(
    const DataBagPtr& db,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& values,
    const std::optional<DataSlice>& schema,
    bool update_schema) {
  DCHECK_EQ(attr_names.size(), values.size());
  internal::DataItem schema_item;
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  std::vector<DataSlice> aligned_values;
  if (schema) {
    RETURN_IF_ERROR(schema->VerifyIsSchema());
    schema_item = schema->item();
    RETURN_IF_ERROR(
        CopyEntitySchema(schema->GetDb(), schema_item, db_mutable_impl));
  }
  if (values.empty()) {
    if (!schema_item.has_value()) {
      schema_item = internal::DataItem(internal::AllocateExplicitSchema());
    }
    return DataSlice::Create(
        internal::DataItem(internal::AllocateSingleObject()),
        std::move(schema_item), db);
  }
  if (schema_item.has_value() && schema_item != schema::kAny) {
    std::vector<DataSlice> casted_values;
    casted_values.reserve(values.size());
    for (int i = 0; i < values.size(); ++i) {
      ASSIGN_OR_RETURN(
          casted_values.emplace_back(),
          CastOrUpdateSchema(values[i], schema_item, attr_names[i],
                             update_schema, db_mutable_impl),
          // Adds the db from schema to assemble readable error message.
          AssembleErrorMessage(_, {.db = DataBag::ImmutableEmptyWithFallbacks(
                                       {schema ? schema->GetDb() : nullptr,
                                        values[i].GetDb()})}));
    }
    ASSIGN_OR_RETURN(aligned_values, shape::Align(std::move(casted_values)));
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
    ASSIGN_OR_RETURN(aligned_values, shape::Align(values));
  }
  // All DataSlices have the same shape at this point and thus the same internal
  // representation, so we pick any of them to dispatch the object creation by
  // internal implementation type.
  return aligned_values.begin()->VisitImpl([&]<class T>(const T& impl) {
    return CreateEntitiesFromFields<T>(db, attr_names, aligned_values,
                                       std::move(schema_item), db_mutable_impl);
  });
}

absl::StatusOr<DataSlice> EntityCreator::Shaped(
    const DataBagPtr& db, DataSlice::JaggedShape shape,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values,
    const std::optional<DataSlice>& schema,
    bool update_schema) {
  return CreateEntitiesImpl(
      db,
      [&](const internal::DataItem& schema_item) {
        size_t size = shape.size();
        return DataSlice::Create(
            internal::DataSliceImpl::AllocateEmptyObjects(size),
            std::move(shape), schema_item, db);
      }, attr_names, values, schema, update_schema);
}

absl::StatusOr<DataSlice> EntityCreator::Like(
    const DataBagPtr& db, const DataSlice& shape_and_mask_from,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values,
    const std::optional<DataSlice>& schema,
    bool update_schema) {
  return CreateEntitiesImpl(
      db,
      [&](const internal::DataItem& schema_item) {
        return CreateLike(db, shape_and_mask_from, schema_item,
                          internal::AllocateSingleObject,
                          internal::Allocate);
      }, attr_names, values, schema, update_schema);
}

// TODO: When DataSlice::SetAttrs is fast enough keep only -Shaped
// and -Like creation and forward to -Shaped here.
absl::StatusOr<DataSlice> ObjectCreator::FromAttrs(
    const DataBagPtr& db, const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& values) {
  DCHECK_EQ(attr_names.size(), values.size());
  if (values.empty()) {
    return ObjectCreator::Shaped(db, DataSlice::JaggedShape::Empty(), {}, {});
  }
  RETURN_IF_ERROR(VerifyNoSchemaArg(attr_names));
  ASSIGN_OR_RETURN(auto aligned_values, shape::Align(values));
  // All DataSlices have the same shape at this point and thus the same internal
  // representation, so we pick any of them to dispatch the object creation by
  // internal implementation type.
  return aligned_values.begin()->VisitImpl([&]<class T>(const T& impl) {
    return CreateObjectsFromFields<T>(db, attr_names, aligned_values);
  });
}

absl::StatusOr<DataSlice> ObjectCreator::Shaped(
    const DataBagPtr& db, DataSlice::JaggedShape shape,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values) {
  return CreateObjectsImpl(
      db,
      [&]() {
        size_t size = shape.size();
        return DataSlice::Create(
            internal::DataSliceImpl::AllocateEmptyObjects(size),
            std::move(shape), internal::DataItem(schema::kObject), db);
      }, attr_names, values);
}

absl::StatusOr<DataSlice> ObjectCreator::Like(
    const DataBagPtr& db, const DataSlice& shape_and_mask_from,
    absl::Span<const absl::string_view> attr_names,
    absl::Span<const DataSlice> values) {
  return CreateObjectsImpl(
      db,
      [&]() {
        return CreateLike(db, shape_and_mask_from,
                          internal::DataItem(schema::kObject),
                          internal::AllocateSingleObject,
                          internal::Allocate);
      }, attr_names, values);
}

absl::StatusOr<DataSlice> ObjectCreator::Convert(const DataBagPtr& db,
                                                 const DataSlice& value) {
  if (value.GetSchemaImpl() == schema::kObject) {
    return value.WithDb(db);
  }
  return value.WithDb(db).EmbedSchema();
}

absl::StatusOr<DataSlice> CreateUuidFromFields(
    absl::string_view seed,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& values) {
  DCHECK_EQ(attr_names.size(), values.size());
  if (values.empty()) {
    return DataSlice::Create(
        CreateUuidFromFields(
            seed, {},
            std::vector<std::reference_wrapper<const internal::DataItem>>{}),
        internal::DataItem(schema::kItemId));
  }
  ASSIGN_OR_RETURN(auto aligned_values, shape::Align(values));
  return aligned_values.begin()->VisitImpl([&]<class T>(const T& impl) {
    std::vector<std::reference_wrapper<const T>> values_impl;
    values_impl.reserve(values.size());
    for (int i = 0; i < attr_names.size(); ++i) {
      values_impl.push_back(std::cref(aligned_values[i].impl<T>()));
    }
    return DataSlice::Create(
        internal::CreateUuidFromFields(seed, attr_names, values_impl),
        aligned_values.begin()->GetShape(),
        internal::DataItem(schema::kItemId),
        nullptr);
  });
}

absl::StatusOr<DataSlice> UuObjectCreator::operator()(
    const DataBagPtr& db, absl::string_view seed,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& values) const {
  DCHECK_EQ(attr_names.size(), values.size());
  DataSlice ds;
  if (values.empty()) {
    auto uuid = internal::CreateUuidFromFields(
        seed, {},
        std::vector<std::reference_wrapper<const internal::DataItem>>{});
    ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                     db->GetMutableImpl());
    RETURN_IF_ERROR(SetObjectSchema(db_mutable_impl, uuid, {}, {},
                                    /*overwrite_schemas=*/false));
    return DataSlice::Create(uuid, internal::DataItem(schema::kObject));
  }
  ASSIGN_OR_RETURN(auto aligned_values, shape::Align(values));

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
                                        attr_names, schemas,
                                  /*overwrite_schemas=*/false));
        return DataSlice::Create(
            impl_res.value(), aligned_values.begin()->GetShape(),
            internal::DataItem(schema::kObject), db);
      });
}

absl::StatusOr<internal::DataItem> CreateDictSchema(
    const DataBagPtr& db, const DataSlice& key_schema,
    const DataSlice& value_schema) {
  RETURN_IF_ERROR(key_schema.VerifyIsSchema());
  RETURN_IF_ERROR(value_schema.VerifyIsSchema());
  RETURN_IF_ERROR(schema::VerifyDictKeySchema(key_schema.item()));
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  return db_mutable_impl.CreateUuSchemaFromFields(
      "::koladata::::CreateDictSchema", {"__keys__", "__values__"},
      {key_schema.item(), value_schema.item()});
}

absl::StatusOr<DataSlice> CreateDictLike(
    const std::shared_ptr<DataBag>& db, const DataSlice& shape_and_mask_from,
    const std::optional<DataSlice>& keys,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& key_schema,
    const std::optional<DataSlice>& value_schema) {
  return CreateDictImpl(
      db,
      [&](const auto& schema) {
        return CreateLike(db, shape_and_mask_from, schema,
                          internal::AllocateSingleDict,
                          internal::AllocateDicts);
      },
      keys, values, schema, key_schema, value_schema);
}

absl::StatusOr<DataSlice> CreateDictShaped(
    const std::shared_ptr<DataBag>& db, DataSlice::JaggedShape shape,
    const std::optional<DataSlice>& keys,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& key_schema,
    const std::optional<DataSlice>& value_schema) {
  return CreateDictImpl(
      db,
      [&](const auto& schema) {
        if (shape.rank() == 0) {
          return DataSlice::Create(
              internal::DataItem(internal::AllocateSingleDict()),
              std::move(shape), schema, db);
        } else {
          size_t size = shape.size();
          return DataSlice::Create(
              internal::DataSliceImpl::ObjectsFromAllocation(
                  internal::AllocateDicts(size), size),
              std::move(shape), schema, db);
        }
      },
      keys, values, schema, key_schema, value_schema);
}

absl::StatusOr<internal::DataItem> CreateListSchema(
    const DataBagPtr& db, const DataSlice& item_schema) {
  RETURN_IF_ERROR(item_schema.VerifyIsSchema());
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  return db_mutable_impl.CreateUuSchemaFromFields(
      "::koladata::::CreateListSchema",
      {"__items__"}, {item_schema.item()});
}

absl::StatusOr<DataSlice> CreateEmptyList(
    const std::shared_ptr<DataBag>& db, const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& item_schema) {
  return CreateListShaped(db, DataSlice::JaggedShape::Empty(),
                          /*values=*/std::nullopt, schema, item_schema);
}

absl::StatusOr<DataSlice> CreateListsFromLastDimension(
    const std::shared_ptr<DataBag>& db, const DataSlice& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& item_schema) {
  size_t rank = values.GetShape().rank();
  if (rank == 0) {
    return absl::InvalidArgumentError(
        "creating a list from values requires at least one dimension");
  }
  return CreateListShaped(db, values.GetShape().RemoveDims(rank - 1), values,
                          schema, item_schema);
}

absl::StatusOr<DataSlice> CreateNestedList(
    const std::shared_ptr<DataBag>& db, const DataSlice& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& item_schema) {
  ASSIGN_OR_RETURN(DataSlice res, CreateListsFromLastDimension(
                                      db, values, schema, item_schema));
  while (res.GetShape().rank() > 0) {
    ASSIGN_OR_RETURN(res, CreateListsFromLastDimension(db, res));
  }
  return res;
}

absl::StatusOr<DataSlice> Implode(const std::shared_ptr<DataBag>& db,
                                  const DataSlice& values, int ndim) {
  const size_t rank = values.GetShape().rank();
  if (ndim < 0) {
    ndim = values.GetShape().rank();  // ndim < 0 means implode all dimensions.
  }
  if (rank < ndim) {
    return absl::InvalidArgumentError(
        absl::StrFormat("cannot implode 'x' to fold the last %d dimension(s) "
                        "because 'x' only has %d dimensions",
                        ndim, rank));
  }

  // Adopt `values` into `db`.
  AdoptionQueue adoption_queue;
  adoption_queue.Add(values);
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*db));

  if (ndim == 0) {
    return values.WithDb(db);
  }
  ASSIGN_OR_RETURN(DataSlice result, CreateListsFromLastDimension(db, values));
  for (int i = 1; i < ndim; ++i) {
    ASSIGN_OR_RETURN(result, CreateListsFromLastDimension(db, result));
  }
  return result;
}

absl::StatusOr<DataSlice> CreateListShaped(
    const std::shared_ptr<DataBag>& db, DataSlice::JaggedShape shape,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& item_schema) {
  internal::DataItem list_schema;
  if (schema) {
    if (item_schema.has_value()) {
      return absl::InvalidArgumentError(
          "creating lists with schema accepts either a list schema or item "
          "schema, but not both");
    }
    RETURN_IF_ERROR(CopyListSchema(*schema, db));
    list_schema = schema->item();
  } else {
    ASSIGN_OR_RETURN(auto deduced_item_schema,
                     DeduceItemSchema(values, item_schema));
    ASSIGN_OR_RETURN(list_schema, CreateListSchema(db, deduced_item_schema));
  }
  size_t size = shape.size();

  std::optional<DataSlice> result;
  if (shape.rank() == 0) {
    ASSIGN_OR_RETURN(
        result,
        DataSlice::Create(internal::DataItem(internal::AllocateSingleList()),
                          list_schema, db));
  } else {
    ASSIGN_OR_RETURN(result, DataSlice::Create(
                                 internal::DataSliceImpl::ObjectsFromAllocation(
                                     internal::AllocateLists(size), size),
                                 std::move(shape), list_schema, db));
  }
  if (values.has_value()) {
    RETURN_IF_ERROR(result->AppendToList(*values));
  }
  return *std::move(result);
}

absl::StatusOr<DataSlice> CreateListLike(
    const std::shared_ptr<DataBag>& db, const DataSlice& shape_and_mask_from,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& schema,
    const std::optional<DataSlice>& item_schema) {
  internal::DataItem list_schema;
  if (schema) {
    if (item_schema.has_value()) {
      return absl::InvalidArgumentError(
          "creating lists with schema accepts either a list schema or item "
          "schema, but not both");
    }
    RETURN_IF_ERROR(CopyListSchema(*schema, db));
    list_schema = schema->item();
  } else {
    ASSIGN_OR_RETURN(auto deduced_item_schema,
                     DeduceItemSchema(values, item_schema));
    ASSIGN_OR_RETURN(list_schema, CreateListSchema(db, deduced_item_schema));
  }
  ASSIGN_OR_RETURN(auto result, CreateLike(db, shape_and_mask_from, list_schema,
                                           internal::AllocateSingleList,
                                           internal::AllocateLists));
  if (values.has_value()) {
    RETURN_IF_ERROR(result.AppendToList(*values));
  }
  return result;
}

absl::StatusOr<DataSlice> CreateNoFollowSchema(const DataSlice& target_schema) {
  RETURN_IF_ERROR(target_schema.VerifyIsSchema());
  ASSIGN_OR_RETURN(auto no_follow_schema_item,
                   schema::NoFollowSchemaItem(target_schema.item()));
  return DataSlice::Create(std::move(no_follow_schema_item),
                           internal::DataItem(schema::kSchema),
                           target_schema.GetDb());
}

absl::StatusOr<DataSlice> NoFollow(const DataSlice& target) {
  ASSIGN_OR_RETURN(auto no_follow_schema_item,
                   schema::NoFollowSchemaItem(target.GetSchemaImpl()));
  return target.WithSchema(no_follow_schema_item);
}

absl::StatusOr<DataSlice> CreateEmptyShaped(const DataSlice::JaggedShape& shape,
                                            const DataSlice& schema,
                                            absl::Nullable<DataBagPtr> db) {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  if (db == nullptr && schema.IsEntitySchema()) {
    db = DataBag::Empty();
  }
  ASSIGN_OR_RETURN(
      auto ds, DataSlice::Create(internal::DataItem(internal::MissingValue()),
                                 internal::DataItem(schema::kMask), db));
  ASSIGN_OR_RETURN(ds, BroadcastToShape(ds, shape));
  return ds.SetSchema(schema);
}

}  // namespace koladata
