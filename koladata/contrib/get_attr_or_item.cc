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
#include "koladata/contrib/get_attr_or_item.h"

#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/error_repr_utils.h"
#include "koladata/internal/casting.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/collapse.h"
#include "koladata/internal/op_utils/has.h"
#include "koladata/internal/op_utils/presence_and.h"
#include "koladata/internal/op_utils/presence_or.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/operators/utils.h"
#include "koladata/schema_utils.h"

namespace koladata::contrib {
namespace {

using ::koladata::internal::DataBagImpl;
using ::koladata::internal::DataItem;
using ::koladata::internal::DataSliceImpl;
using ::koladata::internal::ObjectId;

// Items together with their schemas.
struct ItemsWithSchemas {
  DataSliceImpl items;
  DataSliceImpl schemas;
};

// Items split by kind, see `SplitByKind`.
struct ItemsByKind {
  ItemsWithSchemas entities;
  ItemsWithSchemas lists;
  ItemsWithSchemas dicts;
};

// A looked-up value together with its schema.
struct ItemWithSchema {
  DataItem value;
  DataItem schema;
};

// Returns ItemIds stored in `slice`, ignoring values of other dtypes.
arolla::DenseArray<ObjectId> GetObjectIds(const DataSliceImpl& slice) {
  arolla::DenseArray<ObjectId> res;
  slice.VisitValues([&]<typename T>(const arolla::DenseArray<T>& values) {
    if constexpr (std::is_same_v<T, ObjectId>) {
      res = values;
    }
  });
  // Only allocate if there are no ItemIds.
  if (res.empty() && slice.size() > 0) {
    res = arolla::CreateEmptyDenseArray<ObjectId>(slice.size());
  }
  return res;
}

// `got` is a DataItem or a description of the DataSlice.
template <typename T>
absl::Status UnsupportedItemsError(const T& got) {
  return absl::InvalidArgumentError(absl::StrFormat(
      "get_attr_or_item requires DataSlice to contain only entities, dicts, "
      "or lists, got %v",
      got));
}

// Splits `objs` (and their `__schema__`s) into entities, lists, and dicts.
// Returns an error if there are other items (defensive: e.g. schema ItemIds
// normally fail earlier due to missing `__schema__`).
absl::StatusOr<ItemsByKind> SplitByKind(const ItemsWithSchemas& objs) {
  const arolla::DenseArray<ObjectId> ids = GetObjectIds(objs.items);
  int64_t num_entities = 0, num_lists = 0, num_dicts = 0;
  std::optional<ObjectId> unsupported;
  ids.ForEachPresent([&](int64_t, ObjectId id) {
    if (id.IsEntity()) {
      ++num_entities;
    } else if (id.IsList()) {
      ++num_lists;
    } else if (id.IsDict()) {
      ++num_dicts;
    } else {
      unsupported = id;
    }
  });
  if (unsupported.has_value()) {
    return UnsupportedItemsError(DataItem(*unsupported));
  }
  const int64_t present_count = num_entities + num_lists + num_dicts;
  auto filter = [&](bool (ObjectId::*is_kind)() const,
                    int64_t count) -> absl::StatusOr<ItemsWithSchemas> {
    // Avoid creating masks and masking in the common case of a single kind.
    if (count == 0) {
      return ItemsWithSchemas{};
    }
    if (count == present_count) {
      return objs;
    }
    arolla::DenseArrayBuilder<arolla::Unit> mask_bldr(ids.size());
    ids.ForEachPresent([&](int64_t i, ObjectId id) {
      if ((id.*is_kind)()) {
        mask_bldr.Set(i, arolla::kUnit);
      }
    });
    auto mask = DataSliceImpl::Create(std::move(mask_bldr).Build());
    ASSIGN_OR_RETURN(auto items, internal::PresenceAndOp()(objs.items, mask));
    ASSIGN_OR_RETURN(auto schemas,
                     internal::PresenceAndOp()(objs.schemas, mask));
    return ItemsWithSchemas{std::move(items), std::move(schemas)};
  };
  ItemsByKind res;
  ASSIGN_OR_RETURN(res.entities, filter(&ObjectId::IsEntity, num_entities));
  ASSIGN_OR_RETURN(res.lists, filter(&ObjectId::IsList, num_lists));
  ASSIGN_OR_RETURN(res.dicts, filter(&ObjectId::IsDict, num_dicts));
  return res;
}

absl::StatusOr<ItemsWithSchemas> LookupInEntities(
    const ItemsWithSchemas& entities, absl::string_view attr,
    const DataBagImpl& db_impl, DataBagImpl::FallbackSpan fallbacks) {
  ASSIGN_OR_RETURN(auto attr_schemas, db_impl.GetSchemaAttrAllowMissing(
                                          entities.schemas, attr, fallbacks));
  // Entities without `attr` in their schema must be skipped even if they have
  // `attr` data.
  ASSIGN_OR_RETURN(auto has_attr, internal::HasOp()(attr_schemas));
  ASSIGN_OR_RETURN(auto objs,
                   internal::PresenceAndOp()(entities.items, has_attr));
  ASSIGN_OR_RETURN(auto values, db_impl.GetAttr(objs, attr, fallbacks));
  return ItemsWithSchemas{std::move(values), std::move(attr_schemas)};
}

absl::StatusOr<ItemsWithSchemas> LookupInLists(
    const ItemsWithSchemas& lists, int64_t index, const DataBagImpl& db_impl,
    DataBagImpl::FallbackSpan fallbacks) {
  ASSIGN_OR_RETURN(auto item_schemas,
                   db_impl.GetSchemaAttr(
                       lists.schemas, schema::kListItemsSchemaAttr, fallbacks));
  ASSIGN_OR_RETURN(auto values,
                   db_impl.GetFromLists(lists.items,
                                        arolla::CreateConstDenseArray<int64_t>(
                                            lists.items.size(), index),
                                        fallbacks));
  return ItemsWithSchemas{std::move(values), std::move(item_schemas)};
}

// Returns `key` casted to `dict_key_schema`, or a missing DataItem if `key` is
// not compatible with it. ItemId keys are returned as is.
DataItem CastDictKey(const DataItem& key, const DataItem& dict_key_schema) {
  if (dict_key_schema == schema::kObject || !key.has_value() ||
      key.holds_value<ObjectId>()) {
    return key;
  }
  // Same as `CastToNarrow`, using the schema of the primitive `key` data.
  if (auto common_schema =
          schema::CommonSchema(schema::GetDataSchema(key), dict_key_schema);
      !common_schema.ok() || *common_schema != dict_key_schema) {
    return DataItem();
  }
  auto casted = schema::CastDataTo(key, dict_key_schema);
  return casted.ok() ? *std::move(casted) : DataItem();
}

// Returns the schema shared by all present `schemas`, or a missing DataItem if
// there is no such schema.
absl::StatusOr<DataItem> GetSharedSchema(const DataSliceImpl& schemas) {
  ASSIGN_OR_RETURN(
      auto edge, arolla::DenseArrayEdge::FromUniformGroups(1, schemas.size()));
  ASSIGN_OR_RETURN(auto shared, internal::CollapseOp()(schemas, edge));
  return shared[0];
}

absl::StatusOr<ItemsWithSchemas> LookupInDicts(
    const ItemsWithSchemas& dicts, const DataItem& key,
    const DataBagImpl& db_impl, DataBagImpl::FallbackSpan fallbacks) {
  ASSIGN_OR_RETURN(auto key_schemas,
                   db_impl.GetSchemaAttr(
                       dicts.schemas, schema::kDictKeysSchemaAttr, fallbacks));
  ASSIGN_OR_RETURN(auto key_schema, GetSharedSchema(key_schemas));
  DataSliceImpl keys;
  if (key_schema.has_value()) {
    // Fast path for the common case of a single key schema.
    keys =
        DataSliceImpl::Create(dicts.items.size(), CastDictKey(key, key_schema));
  } else {
    // There are usually only a few distinct key schemas, so we cast the key
    // once per schema.
    absl::flat_hash_map<DataItem, DataItem> casted_keys;
    internal::SliceBuilder keys_bldr(dicts.items.size());
    key_schemas.AsDataItemDenseArray().ForEachPresent(
        [&](int64_t i, const DataItem& key_schema) {
          auto [it, inserted] = casted_keys.try_emplace(key_schema);
          if (inserted) {
            it->second = CastDictKey(key, key_schema);
          }
          if (it->second.has_value()) {
            keys_bldr.InsertIfNotSetAndUpdateAllocIds(i, it->second);
          }
        });
    keys = std::move(keys_bldr).Build();
  }
  ASSIGN_OR_RETURN(auto values,
                   db_impl.GetFromDict(dicts.items, keys, fallbacks));
  ASSIGN_OR_RETURN(
      auto value_schemas,
      db_impl.GetSchemaAttr(dicts.schemas, schema::kDictValuesSchemaAttr,
                            fallbacks));
  return ItemsWithSchemas{std::move(values), std::move(value_schemas)};
}

// Merges `other` into `res`. The present items are expected to come from
// disjoint `ItemsByKind` parts.
absl::Status MergeInto(ItemsWithSchemas& res, const ItemsWithSchemas& other) {
  ASSIGN_OR_RETURN(res.items, internal::PresenceOrOp</*disjoint=*/true>()(
                                  res.items, other.items));
  ASSIGN_OR_RETURN(res.schemas, internal::PresenceOrOp</*disjoint=*/true>()(
                                    res.schemas, other.schemas));
  return absl::OkStatus();
}

// Returns the schema of the looked-up `values`: NONE if all values are missing,
// the schema shared by all present values otherwise.
absl::StatusOr<DataItem> DeduceResultSchema(const ItemsWithSchemas& values) {
  if (values.items.present_count() == 0) {
    return DataItem(schema::kNone);
  }
  // Schemas may be present for missing values (e.g. missing attributes).
  ASSIGN_OR_RETURN(auto has_value, internal::HasOp()(values.items));
  ASSIGN_OR_RETURN(auto schemas,
                   internal::PresenceAndOp()(values.schemas, has_value));
  ASSIGN_OR_RETURN(auto shared_schema, GetSharedSchema(schemas));
  if (!shared_schema.has_value()) {
    return absl::UnimplementedError(
        "lookup of values with different schemas is not supported yet");
  }
  return shared_schema;
}

// Returns `key` as an attribute name, if it is a string.
std::optional<absl::string_view> GetAttrName(const DataItem& key) {
  if (key.holds_value<arolla::Text>()) {
    return key.value<arolla::Text>().view();
  }
  return std::nullopt;
}

// Returns `key` as a list index, if it is an integer.
std::optional<int64_t> GetListIndex(const DataItem& key) {
  if (key.holds_value<int32_t>()) {
    return key.value<int32_t>();
  }
  if (key.holds_value<int64_t>()) {
    return key.value<int64_t>();
  }
  return std::nullopt;
}

// Single item version of `LookupInObjectSlice` below. `obj_schema` is the
// schema of `obj`.
absl::StatusOr<ItemWithSchema> LookupInObjectItem(
    ObjectId obj, const DataItem& obj_schema, const DataItem& key,
    const DataBagImpl& db_impl, DataBagImpl::FallbackSpan fallbacks) {
  ItemWithSchema res;
  if (obj.IsEntity()) {
    if (auto attr = GetAttrName(key); attr.has_value()) {
      ASSIGN_OR_RETURN(res.schema, db_impl.GetSchemaAttrAllowMissing(
                                       obj_schema, *attr, fallbacks));
      // Entities without `attr` in their schema must be skipped even if they
      // have `attr` data.
      if (!res.schema.has_value()) {
        return ItemWithSchema{.schema = DataItem(schema::kNone)};
      }
      ASSIGN_OR_RETURN(res.value,
                       db_impl.GetAttr(DataItem(obj), *attr, fallbacks));
    }
  } else if (obj.IsList()) {
    if (auto index = GetListIndex(key); index.has_value()) {
      ASSIGN_OR_RETURN(
          res.schema, db_impl.GetSchemaAttr(
                          obj_schema, schema::kListItemsSchemaAttr, fallbacks));
      ASSIGN_OR_RETURN(res.value,
                       db_impl.GetFromList(DataItem(obj), *index, fallbacks));
    }
  } else if (obj.IsDict()) {
    ASSIGN_OR_RETURN(auto key_schema,
                     db_impl.GetSchemaAttr(
                         obj_schema, schema::kDictKeysSchemaAttr, fallbacks));
    if (auto casted_key = CastDictKey(key, key_schema);
        casted_key.has_value()) {
      ASSIGN_OR_RETURN(
          res.schema,
          db_impl.GetSchemaAttr(obj_schema, schema::kDictValuesSchemaAttr,
                                fallbacks));
      ASSIGN_OR_RETURN(
          res.value, db_impl.GetFromDict(DataItem(obj), casted_key, fallbacks));
    }
  } else {
    return UnsupportedItemsError(DataItem(obj));
  }
  if (!res.value.has_value()) {
    res.schema = DataItem(schema::kNone);
  }
  return res;
}

// Looks up a scalar `key` in each item of an OBJECT DataSlice `ds`:
//   * as an attribute name in entities (if `key` is a string),
//   * as an index in lists (if `key` is an integer),
//   * as a key in dicts (if `key` can be casted to the dict's key schema).
// Otherwise, the result is missing.
//
// The lookup is vectorized: items are split by kind and each kind is looked up
// in a single batch.
absl::StatusOr<DataSlice> LookupInObjectSlice(const DataSlice& ds,
                                              const DataItem& key) {
  if (ds.ContainsAnyPrimitives()) {
    return UnsupportedItemsError(DescribeSliceSchema(ds));
  }
  if (ds.present_count() == 0) {
    return ds.WithSchema(DataItem(schema::kNone));
  }
  DataBagPtr db = ds.GetBag();
  if (db == nullptr) {
    return absl::InvalidArgumentError("cannot lookup without a DataBag");
  }
  const DataBagImpl& db_impl = db->GetImpl();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks = fb_finder.GetFlattenFallbacks();
  auto missing_obj_schema_error = [&](absl::Status status) {
    return KodaErrorCausedByMissingObjectSchemaError(std::move(status), ds);
  };
  if (ds.is_item()) {
    ASSIGN_OR_RETURN(auto obj_schema,
                     db_impl.GetObjSchemaAttr(ds.item(), fallbacks),
                     _.With(missing_obj_schema_error));
    ASSIGN_OR_RETURN(auto res,
                     LookupInObjectItem(ds.item().value<ObjectId>(), obj_schema,
                                        key, db_impl, fallbacks));
    return DataSlice::Create(std::move(res.value), std::move(res.schema),
                             std::move(db));
  }

  const DataSliceImpl& objs = ds.slice();
  ASSIGN_OR_RETURN(auto obj_schemas, db_impl.GetObjSchemaAttr(objs, fallbacks),
                   _.With(missing_obj_schema_error));
  ASSIGN_OR_RETURN(auto by_kind, SplitByKind({objs, std::move(obj_schemas)}));

  ItemsWithSchemas res{DataSliceImpl::CreateEmptyAndUnknownType(objs.size()),
                       DataSliceImpl::CreateEmptyAndUnknownType(objs.size())};
  if (by_kind.entities.items.present_count() > 0) {
    if (auto attr = GetAttrName(key); attr.has_value()) {
      ASSIGN_OR_RETURN(
          auto entity_res,
          LookupInEntities(by_kind.entities, *attr, db_impl, fallbacks));
      RETURN_IF_ERROR(MergeInto(res, entity_res));
    }
  }
  if (by_kind.lists.items.present_count() > 0) {
    if (auto index = GetListIndex(key); index.has_value()) {
      ASSIGN_OR_RETURN(auto list_res, LookupInLists(by_kind.lists, *index,
                                                    db_impl, fallbacks));
      RETURN_IF_ERROR(MergeInto(res, list_res));
    }
  }
  if (by_kind.dicts.items.present_count() > 0) {
    ASSIGN_OR_RETURN(auto dict_res,
                     LookupInDicts(by_kind.dicts, key, db_impl, fallbacks));
    RETURN_IF_ERROR(MergeInto(res, dict_res));
  }

  ASSIGN_OR_RETURN(auto res_schema, DeduceResultSchema(res));
  return DataSlice::Create(std::move(res.items), ds.GetShape(),
                           std::move(res_schema), std::move(db));
}

// Looks up a scalar `key` in a DataSlice `ds` with an entity, dict, or list
// schema.
absl::StatusOr<DataSlice> LookupInStructSlice(const DataSlice& ds,
                                              const DataSlice& key) {
  if (ds.GetBag() == nullptr) {
    return absl::InvalidArgumentError("cannot lookup without a DataBag");
  }
  const DataSlice schema = ds.GetSchema();
  if (schema.IsDictSchema()) {
    return ds.GetFromDict(key);
  }
  if (schema.IsListSchema()) {
    return ds.GetFromList(key);
  }
  ASSIGN_OR_RETURN(absl::string_view attr, ops::GetStringArgument(key, "key"));
  return ds.GetAttr(attr);
}

}  // namespace

absl::StatusOr<DataSlice> GetAttrOrItem(const DataSlice& ds,
                                        const DataSlice& key) {
  if (!key.is_item()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("argument `key` must be an item, got a slice of "
                        "rank %d > 0",
                        key.GetShape().rank()));
  }
  const internal::DataItem& ds_schema = ds.GetSchemaImpl();
  if (ds_schema == schema::kNone) {
    return ds;
  }
  if (ds_schema == schema::kObject) {
    return LookupInObjectSlice(ds, key.item());
  }
  if (ds_schema.is_struct_schema()) {
    return LookupInStructSlice(ds, key);
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "get_attr_or_item requires DataSlice to have entity, dict, list, or "
      "OBJECT schema, got %s",
      DescribeSliceSchema(ds)));
}

}  // namespace koladata::contrib
