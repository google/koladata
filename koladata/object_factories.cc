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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/uuid_object.h"
#include "koladata/shape/shape_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

namespace {

// Returns an entity (with `db` attached) that can be created either from
// DataSliceImpl(s) or DataItem(s). The returned Entity has explicit schema
// whose attributes are set to schemas of `values`.
template <class ImplT>
absl::StatusOr<DataSlice> CreateEntitiesFromFields(
    const DataBagPtr& db,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& aligned_values) {
  std::vector<std::reference_wrapper<const ImplT>> aligned_values_impl;
  aligned_values_impl.reserve(aligned_values.size());
  for (const auto& val : aligned_values) {
    aligned_values_impl.push_back(std::cref(val.impl<ImplT>()));
  }
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  ASSIGN_OR_RETURN(auto ds_impl, db_mutable_impl.CreateObjectsFromFields(
                                     attr_names, aligned_values_impl));
  std::vector<std::reference_wrapper<const internal::DataItem>> schemas;
  schemas.reserve(aligned_values.size());
  for (const auto& val : aligned_values) {
    schemas.push_back(std::cref(val.GetSchemaImpl()));
  }
  ASSIGN_OR_RETURN(auto schema, db_mutable_impl.CreateExplicitSchemaFromFields(
                                    attr_names, schemas));
  return DataSlice::Create(std::move(ds_impl),
                           std::move(aligned_values.begin()->GetShapePtr()),
                           std::move(schema),
                           db);
}

template <class ImplT>
absl::Status SetSchema(
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
    const DataBagPtr& db,
    const std::vector<absl::string_view>& attr_names,
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
      SetSchema(db_mutable_impl, ds_impl, attr_names, schemas));

  return DataSlice::Create(std::move(ds_impl),
                           std::move(aligned_values.begin()->GetShapePtr()),
                           internal::DataItem(schema::kObject),
                           db);
}

// Creates a DataSlice with objects constructed by allocate_single_func /
// allocate_many_func and shape + sparsity of shape_and_mask.
template <typename AllocateSingleFunc, typename AllocateManyFunc>
absl::StatusOr<DataSlice> CreateLike(const std::shared_ptr<DataBag>& db,
                                     const DataSlice& shape_and_mask,
                                     const internal::DataItem& schema,
                                     AllocateSingleFunc allocate_single_func,
                                     AllocateManyFunc allocate_many_func) {
  return shape_and_mask.VisitImpl([&]<class T>(const T& impl) {
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
                               shape_and_mask.GetShapePtr(), schema, db);
    }
  });
}

// Creates dict schema with the given key and value schemas.
// TODO: Make this function public. Will be useful for
// kd.dict_schema.
absl::StatusOr<internal::DataItem> CreateDictSchema(
    const DataBagPtr& db, const DataSlice& key_schema,
    const DataSlice& value_schema) {
  RETURN_IF_ERROR(key_schema.VerifyIsSchema());
  RETURN_IF_ERROR(value_schema.VerifyIsSchema());
  RETURN_IF_ERROR(schema::VerifyDictKeySchema(key_schema.item()));
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  return db_mutable_impl.CreateUuSchemaFromFields(
      "::koladata::::CreateDictSchema",
      {"__keys__", "__values__"},
      {key_schema.item(), value_schema.item()});
}

}  // namespace

absl::StatusOr<DataSlice> CreateEntitySchema(
    const DataBagPtr& db,
    const std::vector<absl::string_view>& attr_names,
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

absl::StatusOr<DataSlice> EntityCreator::operator()(
    const DataBagPtr& db,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& values) const {
  DCHECK_EQ(attr_names.size(), values.size());
  if (values.empty()) {
    return DataSlice::Create(
        internal::DataItem(internal::AllocateSingleObject()),
        internal::DataItem(internal::AllocateExplicitSchema()),
        db);
  }
  // TODO: Add merging of DataBags if values have references to
  // DataBags.
  ASSIGN_OR_RETURN(auto aligned_values, shape::Align<DataSlice>(values));
  // All DataSlices have the same shape at this point and thus the same internal
  // representation, so we pick any of them to dispatch the object creation by
  // internal implementation type.
  return aligned_values.begin()->VisitImpl([&]<class T>(const T& impl) {
    return CreateEntitiesFromFields<T>(db, attr_names, aligned_values);
  });
}

absl::StatusOr<DataSlice> EntityCreator::operator()(
    const DataBagPtr& db, DataSlice::JaggedShapePtr shape) const {
  auto ds_impl = internal::DataSliceImpl::AllocateEmptyObjects(shape->size());
  return DataSlice::Create(
      std::move(ds_impl), std::move(shape),
      internal::DataItem(internal::AllocateExplicitSchema()), db);
}

absl::StatusOr<DataSlice> ObjectCreator::operator()(
    const DataBagPtr& db,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& values) const {
  DCHECK_EQ(attr_names.size(), values.size());
  if (values.empty()) {
    return (*this)(db, DataSlice::JaggedShape::Empty());
  }
  // TODO: Add merging of DataBags if values have references to
  // DataBags.
  ASSIGN_OR_RETURN(auto aligned_values, shape::Align<DataSlice>(values));
  // All DataSlices have the same shape at this point and thus the same internal
  // representation, so we pick any of them to dispatch the object creation by
  // internal implementation type.
  return aligned_values.begin()->VisitImpl([&]<class T>(const T& impl) {
    return CreateObjectsFromFields<T>(db, attr_names, aligned_values);
  });
}

absl::StatusOr<DataSlice> ObjectCreator::operator()(
    const DataBagPtr& db, DataSlice::JaggedShapePtr shape) const {
  auto ds_impl = internal::DataSliceImpl::AllocateEmptyObjects(shape->size());
  ASSIGN_OR_RETURN(
      auto schema_impl,
      CreateUuidWithMainObject<internal::ObjectId::kUuidImplicitSchemaFlag>(
          ds_impl, schema::kImplicitSchemaSeed));
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   db->GetMutableImpl());
  RETURN_IF_ERROR(
      db_mutable_impl.SetAttr(ds_impl, schema::kSchemaAttr, schema_impl));
  return DataSlice::Create(std::move(ds_impl), std::move(shape),
                           internal::DataItem(schema::kObject), db);
}

absl::StatusOr<DataSlice> ObjectCreator::operator()(
    const DataBagPtr& db, const DataSlice& value) const {
  if (value.GetSchemaImpl() == schema::kObject) {
    return value.WithDb(db);
  }
  return value.WithDb(db).EmbedSchema();
}

absl::StatusOr<DataSlice> UuObjectCreator::operator()(
    const DataBagPtr& db, absl::string_view seed,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& values) const {
  DCHECK_EQ(attr_names.size(), values.size());
  DataSlice ds;
  if (values.empty()) {
    auto uuid = internal::CreateUuidFromFields(
        seed, absl::flat_hash_map<
                  absl::string_view,
                  std::reference_wrapper<const internal::DataItem>>{});
    ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                     db->GetMutableImpl());
    RETURN_IF_ERROR(SetSchema(db_mutable_impl, uuid, {}, {},
                              /*overwrite_schemas=*/false));
    return DataSlice::Create(uuid, internal::DataItem(schema::kObject));
  }
  ASSIGN_OR_RETURN(auto aligned_values, shape::Align<DataSlice>(values));

  std::vector<std::reference_wrapper<const internal::DataItem>> schemas;
  schemas.reserve(aligned_values.size());
  for (const auto& slice : aligned_values) {
    schemas.push_back(std::cref(slice.GetSchemaImpl()));
  }

  return aligned_values.begin()->VisitImpl(
      [&]<class ImplT>(ImplT) -> absl::StatusOr<DataSlice> {
        std::vector<std::reference_wrapper<const ImplT>> aligned_values_impl;
        aligned_values_impl.reserve(aligned_values.size());
        absl::flat_hash_map<absl::string_view,
                            std::reference_wrapper<const ImplT>>
            field_map;
        for (int i = 0; i < attr_names.size(); ++i) {
          aligned_values_impl.push_back(
              std::cref(aligned_values[i].impl<ImplT>()));
          field_map.insert(
              {attr_names[i], std::cref(aligned_values[i].impl<ImplT>())});
        }

        std::optional<ImplT> impl_res;
        if constexpr (std::is_same_v<internal::DataItem, ImplT>) {
          impl_res = internal::CreateUuidFromFields(seed, field_map);
        } else {
          ASSIGN_OR_RETURN(impl_res,
                           internal::CreateUuidFromFields(seed, field_map));
        }
        ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                         db->GetMutableImpl());
        for (int i = 0; i < attr_names.size(); ++i) {
          RETURN_IF_ERROR(db_mutable_impl.SetAttr(*impl_res, attr_names[i],
                                                  aligned_values_impl[i]));
        }
        RETURN_IF_ERROR(SetSchema(db_mutable_impl, impl_res.value(),
                                  attr_names, schemas,
                                  /*overwrite_schemas=*/false));
        return DataSlice::Create(
            impl_res.value(), std::move(aligned_values.begin()->GetShapePtr()),
            internal::DataItem(schema::kObject), db);
      });
}

namespace {

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

}  // namespace

// Implementation of CreateDictLike and CreateDictShaped that handles schema
// deduction and keys/values assignment. `create_dicts_fn` must create a
// DataSlice with the provided schema.
template <typename CreateDictsFn>
absl::StatusOr<DataSlice> CreateDictImpl(
    const std::shared_ptr<DataBag>& db, const CreateDictsFn& create_dicts_fn,
    const std::optional<DataSlice>& keys,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& key_schema,
    const std::optional<DataSlice>& value_schema) {
  ASSIGN_OR_RETURN(auto deduced_key_schema, DeduceItemSchema(keys, key_schema));
  ASSIGN_OR_RETURN(auto deduced_value_schema,
                   DeduceItemSchema(values, value_schema));
  ASSIGN_OR_RETURN(auto dict_schema, CreateDictSchema(db, deduced_key_schema,
                                                      deduced_value_schema));
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

absl::StatusOr<DataSlice> CreateDictLike(
    const std::shared_ptr<DataBag>& db, const DataSlice& shape_and_mask,
    const std::optional<DataSlice>& keys,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& key_schema,
    const std::optional<DataSlice>& value_schema) {
  return CreateDictImpl(
      db,
      [&](const auto& schema) {
        return CreateLike(db, shape_and_mask, schema,
                          internal::AllocateSingleDict,
                          internal::AllocateDicts);
      },
      keys, values, key_schema, value_schema);
}

absl::StatusOr<DataSlice> CreateDictShaped(
    const std::shared_ptr<DataBag>& db, DataSlice::JaggedShapePtr shape,
    const std::optional<DataSlice>& keys,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& key_schema,
    const std::optional<DataSlice>& value_schema) {
  return CreateDictImpl(
      db,
      [&](const auto& schema) {
        if (shape->rank() == 0) {
          return DataSlice::Create(
              internal::DataItem(internal::AllocateSingleDict()),
              std::move(shape), schema, db);
        } else {
          size_t size = shape->size();
          return DataSlice::Create(
              internal::DataSliceImpl::ObjectsFromAllocation(
                  internal::AllocateDicts(size), size),
              std::move(shape), schema, db);
        }
      },
      keys, values, key_schema, value_schema);
}

absl::StatusOr<DataSlice> CreateEmptyList(
    const std::shared_ptr<DataBag>& db,
    const std::optional<DataSlice>& item_schema) {
  return CreateListShaped(db, DataSlice::JaggedShape::Empty(),
                          /*values=*/std::nullopt, item_schema);
}

absl::StatusOr<DataSlice> CreateListsFromLastDimension(
    const std::shared_ptr<DataBag>& db, const DataSlice& values,
    const std::optional<DataSlice>& item_schema) {
  size_t rank = values.GetShape().rank();
  if (rank == 0) {
    return absl::InvalidArgumentError(
        "creating a list from values requires at least one dimension");
  }
  return CreateListShaped(db, values.GetShape().RemoveDims(rank - 1), values,
                          item_schema);
}

absl::StatusOr<DataSlice> CreateNestedList(
    const std::shared_ptr<DataBag>& db, const DataSlice& values,
    const std::optional<DataSlice>& item_schema) {
  ASSIGN_OR_RETURN(auto deduced_item_schema,
                   DeduceItemSchema(values, item_schema));
  ASSIGN_OR_RETURN(DataSlice res, CreateListsFromLastDimension(
                                      db, values, deduced_item_schema));
  while (res.GetShape().rank() > 0) {
    ASSIGN_OR_RETURN(res,
                     CreateListsFromLastDimension(db, res, res.GetSchema()));
  }
  return res;
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

absl::StatusOr<DataSlice> CreateListShaped(
    const std::shared_ptr<DataBag>& db, DataSlice::JaggedShapePtr shape,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& item_schema) {
  ASSIGN_OR_RETURN(auto deduced_item_schema,
                   DeduceItemSchema(values, item_schema));
  ASSIGN_OR_RETURN(auto list_schema, CreateListSchema(db, deduced_item_schema));
  size_t size = shape->size();

  std::optional<DataSlice> result;
  if (shape->rank() == 0) {
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
    const std::shared_ptr<DataBag>& db, const DataSlice& shape_and_mask,
    const std::optional<DataSlice>& values,
    const std::optional<DataSlice>& item_schema) {
  ASSIGN_OR_RETURN(auto deduced_item_schema,
                   DeduceItemSchema(values, item_schema));
  ASSIGN_OR_RETURN(auto list_schema, CreateListSchema(db, deduced_item_schema));
  ASSIGN_OR_RETURN(auto result, CreateLike(db, shape_and_mask, list_schema,
                                           internal::AllocateSingleList,
                                           internal::AllocateLists));
  if (values.has_value()) {
    RETURN_IF_ERROR(result.AppendToList(*values));
  }
  return result;
}

absl::StatusOr<DataSlice> CreateUuidFromFields(
    absl::string_view seed,
    const std::vector<absl::string_view>& attr_names,
    const std::vector<DataSlice>& values) {
  DCHECK_EQ(attr_names.size(), values.size());
  if (values.empty()) {
    return DataSlice::Create(
        CreateUuidFromFields(
            seed, absl::flat_hash_map<
                      absl::string_view,
                      std::reference_wrapper<const internal::DataItem>>{}),
        internal::DataItem(schema::kItemId));
  }
  ASSIGN_OR_RETURN(auto aligned_values, shape::Align<DataSlice>(values));
  return aligned_values.begin()->VisitImpl([&]<class T>(const T& impl) {
    absl::flat_hash_map<absl::string_view,
                        std::reference_wrapper<const T>> field_map;
    for (int i = 0; i < aligned_values.size(); ++i) {
      field_map.insert({attr_names[i], std::cref(aligned_values[i].impl<T>())});
    }

    return DataSlice::Create(
        internal::CreateUuidFromFields(seed, field_map),
        aligned_values.begin()->GetShapePtr(),
        internal::DataItem(schema::kItemId),
        nullptr);
  });
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
  ASSIGN_OR_RETURN(auto no_follow_schema,
                   CreateNoFollowSchema(target.GetSchema()));
  return target.WithSchema(no_follow_schema);
}

}  // namespace koladata
