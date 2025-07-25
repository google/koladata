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
#include "koladata/data_slice.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/container/btree_set.h"
#include "absl/functional/overload.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/jagged_shape/qexpr/shape_operators.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/repr.h"
#include "arolla/util/status.h"
#include "arolla/util/text.h"
#include "arolla/util/view_types.h"
#include "koladata/adoption_utils.h"
#include "koladata/attr_error_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice_op.h"
#include "koladata/data_slice_repr.h"
#include "koladata/error_repr_utils.h"
#include "koladata/internal/casting.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/errors.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/coalesce_with_filtered.h"
#include "koladata/internal/op_utils/expand.h"
#include "koladata/internal/op_utils/group_by_utils.h"
#include "koladata/internal/op_utils/has.h"
#include "koladata/internal/op_utils/presence_and.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/schema_utils.h"
#include "koladata/shape_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

const DataSlice::JaggedShape& MaxRankShape(const DataSlice::JaggedShape& s1,
                                           const DataSlice::JaggedShape& s2) {
  return s1.rank() < s2.rank() ? s2 : s1;
}

absl::StatusOr<internal::DataItem> UnwrapIfNoFollowSchema(
    const internal::DataItem& schema_item) {
  if (schema_item.holds_value<internal::ObjectId>() &&
      schema_item.value<internal::ObjectId>().IsNoFollowSchema()) {
    return schema::GetNoFollowedSchemaItem(schema_item);
  }
  return schema_item;
}

absl::Status AttrAssignmentError(absl::Status status, size_t lhs_rank,
                                 size_t rhs_rank) {
  constexpr absl::string_view kAttrAssignmentError =
      "attribute values being assigned in\n\n"
      "foo.with_attrs(attr=value) or foo.attr = values or "
      "entity / object creation kd.new_shape_as(foo, ...), "
      "kd.obj_like(foo, ...), etc.\n\n"
      "must have the same or less number of dimensions as foo, got "
      "foo.get_ndim(): %d < values.get_ndim(): %d;\n"
      "consider wrapping the last %d dimensions into lists using "
      "kd.implode(values, ndim=%d)";

  if (rhs_rank > lhs_rank) {
    return absl::InvalidArgumentError(
        absl::StrFormat(kAttrAssignmentError, lhs_rank, rhs_rank,
                        rhs_rank - lhs_rank, rhs_rank - lhs_rank));
  }
  return status;
}

absl::Status ListAssignmentError(absl::Status status, size_t lhs_rank,
                                 size_t rhs_rank) {
  constexpr absl::string_view kListAssignmentError =
      "list items being assigned in\n\n"
      "kd.list_append_update(lst, items) or lst[indices] = items\n\n"
      "must have the same or less number of dimensions as lst (or indices if "
      "larger), got "
      "max(lst.get_ndim(), indices.get_ndim()): %d < items.get_ndim(): %d;\n"
      "consider wrapping the last %d dimensions into lists using "
      "kd.implode(items, ndim=%d)";

  if (rhs_rank > lhs_rank) {
    return absl::InvalidArgumentError(
        absl::StrFormat(kListAssignmentError, lhs_rank, rhs_rank,
                        rhs_rank - lhs_rank, rhs_rank - lhs_rank));
  }
  return status;
}

absl::Status DictAssignmentError(absl::Status status, size_t lhs_rank,
                                 size_t rhs_rank) {
  constexpr absl::string_view kDictAssignmentError =
      "dict values being assigned in\n\n"
      "kd.dict_update(dct, keys, values) or dct[keys] = values or "
      "kd.dict(keys, values)\n\n"
      "must have the same or less number of dimensions as dct (or keys if "
      "larger), got "
      "max(dct.get_ndim(), keys.get_ndim()): %d < values.get_ndim(): %d;\n"
      "consider wrapping the last %d dimensions into lists using "
      "kd.implode(values, ndim=%d)";

  if (rhs_rank > lhs_rank) {
    return absl::InvalidArgumentError(
        absl::StrFormat(kDictAssignmentError, lhs_rank, rhs_rank,
                        rhs_rank - lhs_rank, rhs_rank - lhs_rank));
  }
  return status;
}

// Gets embedded schema from DataItem for primitives and objects.
absl::StatusOr<internal::DataItem> GetObjSchemaImpl(
    const internal::DataItem& item, const absl_nullable DataBagPtr& db) {
  internal::DataItem res;
  RETURN_IF_ERROR(item.VisitValue([&]<class T>(const T& value) -> absl::Status {
    if constexpr (arolla::meta::contains_v<schema::supported_primitive_dtypes,
                                           T>) {
      // Primitive
      res = internal::DataItem(schema::GetDType<T>());
      return absl::OkStatus();
    } else if constexpr (std::is_same_v<T, schema::DType>) {
      // Dtype
      res = internal::DataItem(schema::kSchema);
      return absl::OkStatus();
    } else if constexpr (std::is_same_v<T, internal::ObjectId>) {
      // Object
      if (db == nullptr) {
        return absl::InvalidArgumentError(
            "DataSlice with Objects must have a DataBag attached for "
            "get_obj_schema");
      }
      const auto& db_impl = db->GetImpl();
      FlattenFallbackFinder fb_finder(*db);
      auto fallbacks = fb_finder.GetFlattenFallbacks();
      ASSIGN_OR_RETURN(res, db_impl.GetObjSchemaAttr(item, fallbacks));
      return absl::OkStatus();
    } else if constexpr (std::is_same_v<T, internal::MissingValue>) {
      // Missing value
      return absl::OkStatus();
    } else {
      return absl::InternalError("invalid variant type in GetObjSchemaImpl");
    }
  }));
  return res;
}

// Gets embedded schema from DataSliceImpl for primitives and objects.
absl::StatusOr<internal::DataSliceImpl> GetObjSchemaImpl(
    const internal::DataSliceImpl& impl, const absl_nullable DataBagPtr& db) {
  internal::SliceBuilder builder(impl.size());

  RETURN_IF_ERROR(impl.VisitValues([&]<class T>(const arolla::DenseArray<T>&
                                                    array) -> absl::Status {
    if constexpr (arolla::meta::contains_v<schema::supported_primitive_dtypes,
                                           T>) {
      // Primitives
      auto typed_builder = builder.typed<schema::DType>();
      array.ForEachPresent([&](int64_t id, arolla::view_type_t<T> value) {
        typed_builder.InsertIfNotSet(id, schema::GetDType<T>());
      });
      return absl::OkStatus();
    } else if constexpr (std::is_same_v<T, schema::DType>) {
      // Dtype
      auto typed_builder = builder.typed<schema::DType>();
      array.ForEachPresent([&](int64_t id, arolla::view_type_t<T> value) {
        typed_builder.InsertIfNotSet(id, schema::kSchema);
      });
      return absl::OkStatus();
    } else if constexpr (std::is_same_v<T, internal::ObjectId>) {
      // Objects
      if (db == nullptr) {
        return absl::InvalidArgumentError(
            "DataSlice with Objects must have a DataBag attached for "
            "get_obj_schema");
      }
      const auto& db_impl = db->GetImpl();
      FlattenFallbackFinder fb_finder(*db);
      auto fallbacks = fb_finder.GetFlattenFallbacks();
      ASSIGN_OR_RETURN(auto obj_schemas,
                       db_impl.GetObjSchemaAttr(
                           internal::DataSliceImpl::Create(array), fallbacks));
      builder.GetMutableAllocationIds().Insert(obj_schemas.allocation_ids());
      const auto& values = obj_schemas.template values<internal::ObjectId>();
      builder.InsertIfNotSet<internal::ObjectId>(
          values.bitmap, arolla::bitmap::Bitmap(), values.values);
      return absl::OkStatus();
    } else {
      return absl::InternalError("invalid variant type in GetObjSchemaImpl");
    }
  }));
  return std::move(builder).Build();
}

// Fetches all attribute names from `schema_item` and returns them ordered.
// Excludes kSchemaNameAttr, kSchemaMetadataAttr from the result.
absl::StatusOr<DataSlice::AttrNamesSet> GetAttrsFromSchemaItem(
    const internal::DataItem& schema_item, const internal::DataBagImpl& db_impl,
    internal::DataBagImpl::FallbackSpan fallbacks) {
  if (schema_item.holds_value<schema::DType>()) {
    // Primitive and special meaning schemas are ignored and empty set is
    // returned.
    return DataSlice::AttrNamesSet();
  }
  ASSIGN_OR_RETURN(auto attrs, db_impl.GetSchemaAttrs(schema_item, fallbacks));
  // Note: Empty attribute slice is empty_and_unknown.
  if (attrs.size() == 0) {
    return DataSlice::AttrNamesSet();
  }
  if (attrs.dtype() != arolla::GetQType<arolla::Text>()) {
    return absl::InternalError("dtype of attribute names must be STRING");
  }
  if (attrs.present_count() != attrs.size()) {
    return absl::InternalError("attributes must be non-empty");
  }
  DataSlice::AttrNamesSet result;
  attrs.values<arolla::Text>().ForEachPresent(
      [&](int64_t id, absl::string_view attr) {
        result.insert(std::string(attr));
      });
  result.erase(schema::kSchemaNameAttr);
  result.erase(schema::kSchemaMetadataAttr);
  return result;
}

absl::StatusOr<DataSlice::AttrNamesSet> GetAttrsFromDataItem(
    const internal::DataItem& item, const internal::DataItem& ds_schema,
    const internal::DataBagImpl& db_impl,
    internal::DataBagImpl::FallbackSpan fallbacks) {
  internal::DataItem schema_item;
  if (ds_schema == schema::kSchema) {
    schema_item = item;
  } else if (ds_schema == schema::kObject) {
    ASSIGN_OR_RETURN(schema_item, db_impl.GetObjSchemaAttr(item, fallbacks));
  } else {
    // Empty set.
    return DataSlice::AttrNamesSet();
  }
  return GetAttrsFromSchemaItem(schema_item, db_impl, fallbacks);
}

// Fetches attribute names from union or intersection of `schemas` and returns
// them ordered. All `schemas` should belong to the `schema_alloc`.
// Excludes kSchemaNameAttr, kSchemaMetadataAttr from the result.
absl::StatusOr<DataSlice::AttrNamesSet> GetAttrsFromDataSliceInSingleAllocation(
    internal::ObjectId schema_alloc, const internal::DataSliceImpl& schemas,
    const internal::DataBagImpl& db_impl,
    internal::DataBagImpl::FallbackSpan fallbacks, bool union_object_attrs) {
  if (internal::AllocationId(schema_alloc).IsSmall()) {
    return GetAttrsFromSchemaItem(internal::DataItem(schema_alloc), db_impl,
                                  fallbacks);
  }
  auto attr_names = db_impl.GetSchemaAttrsForBigAllocationAsVector(
      internal::AllocationId(schema_alloc), fallbacks);
  auto result = DataSlice::AttrNamesSet();
  for (const auto& attr_name : attr_names) {
    const auto& attr_name_view = attr_name.value<arolla::Text>();
    ASSIGN_OR_RETURN(auto attr, db_impl.GetSchemaAttrAllowMissing(
                                    schemas, attr_name_view, fallbacks));
    bool need_insert = !attr.is_empty_and_unknown();
    need_insert &=
        union_object_attrs || (attr.present_count() == schemas.present_count());
    if (need_insert) {
      result.insert(std::string(attr_name_view));
    }
  }
  // kSchemaNameAttr can only be present in a small allocation.
  result.erase(schema::kSchemaMetadataAttr);
  return result;
}

absl::StatusOr<DataSlice::AttrNamesSet> GetAttrsFromDataSlice(
    const internal::DataSliceImpl& slice, const internal::DataItem& ds_schema,
    const internal::DataBagImpl& db_impl,
    internal::DataBagImpl::FallbackSpan fallbacks, bool union_object_attrs) {
  std::optional<DataSlice::AttrNamesSet> result;
  std::optional<internal::DataSliceImpl> schemas;
  if (ds_schema == schema::kSchema) {
    schemas = slice;
  } else if (ds_schema == schema::kObject) {
    std::optional<arolla::DenseArray<internal::ObjectId>> objects_only;
    slice.VisitValues([&]<class T>(const arolla::DenseArray<T>& array) {
      if constexpr (std::is_same_v<T, internal::ObjectId>) {
        objects_only = array;
      }
    });
    if (!objects_only) {
      // Empty set.
      return DataSlice::AttrNamesSet();
    }
    ASSIGN_OR_RETURN(
        schemas,
        db_impl.GetObjSchemaAttr(internal::DataSliceImpl::Create(*objects_only),
                                 fallbacks));
  } else {
    // Empty set.
    return DataSlice::AttrNamesSet();
  }
  arolla::DenseArrayBuilder<internal::ObjectId> schema_allocs_bldr(
      schemas->size());
  bool is_single_allocation = true;
  std::optional<internal::ObjectId> first_schema_alloc = std::nullopt;
  schemas->VisitValues([&]<class T>(const arolla::DenseArray<T>& array) {
    if constexpr (std::is_same_v<T, internal::ObjectId>) {
      array.ForEachPresent([&](size_t idx, const internal::ObjectId& schema) {
        if (schema.IsNoFollowSchema()) {
          return;
        }
        internal::ObjectId schema_repr = schema;
        if (!schema.IsSmallAlloc()) {
          schema_repr = internal::AllocationId(schema).ObjectByOffset(0);
        }
        schema_allocs_bldr.Set(idx, schema_repr);
        if (first_schema_alloc.has_value()) {
          is_single_allocation &= *first_schema_alloc == schema_repr;
        } else {
          first_schema_alloc = schema_repr;
        }
      });
    }
  });
  if (!first_schema_alloc.has_value()) {
    return DataSlice::AttrNamesSet();
  }
  if (is_single_allocation) {
    return GetAttrsFromDataSliceInSingleAllocation(
        *first_schema_alloc, *schemas, db_impl, fallbacks, union_object_attrs);
  }
  auto group_by = internal::ObjectsGroupBy();
  const auto schema_allocs = std::move(schema_allocs_bldr).Build();
  ASSIGN_OR_RETURN(auto edge, group_by.EdgeFromSchemasArray(schema_allocs));
  ASSIGN_OR_RETURN(auto group_schema_allocs,
                   group_by.CollapseByEdge(edge, schema_allocs));
  ASSIGN_OR_RETURN(
      auto schemas_grouped,
      group_by.ByEdge(edge, schemas->values<internal::ObjectId>()));
  auto status = absl::OkStatus();
  group_schema_allocs.ForEachPresent(
      [&](size_t idx, internal::ObjectId schema_alloc) {
        if (!status.ok()) {
          return;
        }
        auto attrs_or = GetAttrsFromDataSliceInSingleAllocation(
            schema_alloc,
            internal::DataSliceImpl::Create(std::move(schemas_grouped[idx])),
            db_impl, fallbacks, union_object_attrs);
        if (!attrs_or.ok()) {
          status = attrs_or.status();
          return;
        }
        if (!result) {
          result = *std::move(attrs_or);
          return;
        }
        const auto& attrs = *attrs_or;
        if (union_object_attrs) {
          result->insert(attrs.begin(), attrs.end());
        } else {
          absl::erase_if(*result, [&](auto a) { return !attrs.contains(a); });
        }
      });
  RETURN_IF_ERROR(std::move(status));
  return result.value_or(DataSlice::AttrNamesSet());
}

auto KodaErrorCausedByIncompableSchemaError(const DataBagPtr& lhs_bag,
                                            const DataBagPtr& rhs_bag,
                                            const DataSlice& ds) {
  return [&](auto&& status_like) {
    RETURN_IF_ERROR(KodaErrorCausedByIncompableSchemaError(
        std::forward<decltype(status_like)>(status_like), lhs_bag, rhs_bag,
        ds));
    ABSL_UNREACHABLE();
  };
}

auto KodaErrorCausedByMissingCollectionItemSchemaError(const DataBagPtr& db) {
  return [&](auto&& status_like) {
    RETURN_IF_ERROR(KodaErrorCausedByMissingCollectionItemSchemaError(
        std::forward<decltype(status_like)>(status_like), db));
    ABSL_UNREACHABLE();
  };
}

// Helper method for fetching an attribute as if this DataSlice is a Schema
// slice (schemas are stored in a dict and not in normal attribute storage).
// * If `allow_missing` is `false_type` and schema is missing, an error is
//   returned.
// * Otherwise, empty DataSlice with `kSchema` schema is returned.
template <typename ImplT>
absl::StatusOr<ImplT> GetSchemaAttrImpl(
    const internal::DataBagImpl& db_impl, const ImplT& impl,
    absl::string_view attr_name, internal::DataBagImpl::FallbackSpan fallbacks,
    bool allow_missing) {
  if (allow_missing) {
    return db_impl.GetSchemaAttrAllowMissing(impl, attr_name, fallbacks);
  }
  return db_impl.GetSchemaAttr(impl, attr_name, fallbacks);
}

// Returns a "collapsed" schema from all schemas stored as an attribute
// `attr_name` of this->GetAttr("__schema__"). "Collapsed" means that if all
// schemas are compatible, the most common is returned (see
// schema::CommonSchema for more details).
// * In case such common schema does not exist and `allow_missing` is false, an
//   error is returned.
// * Otherwise, if "__schema__" attribute is missing for some objects (or all)
//   they are used for inferring the common schema. If all are missing, `NONE`
//   is used.
template <typename ImplT>
absl::StatusOr<internal::DataItem> GetObjCommonSchemaAttr(
    const internal::DataBagImpl& db_impl, const ImplT& impl,
    absl::string_view attr_name, internal::DataBagImpl::FallbackSpan fallbacks,
    bool allow_missing) {
  ASSIGN_OR_RETURN(auto schema_attr, db_impl.GetObjSchemaAttr(impl, fallbacks));
  ASSIGN_OR_RETURN(ImplT per_item_types,
                   GetSchemaAttrImpl(db_impl, schema_attr, attr_name, fallbacks,
                                     allow_missing));
  if (allow_missing && per_item_types.present_count() == 0) {
    return internal::DataItem();
  } else {
    ASSIGN_OR_RETURN(auto common_schema, schema::CommonSchema(per_item_types));
    if (common_schema.has_value()) {
      return common_schema;
    }
    return internal::DataItem(schema::kNone);
  }
}

// Returns a "collapsed" schema from all present schemas stored as an attribute
// `attr_name` of this->GetAttr("__schema__"). "Collapsed" means that if all
// schemas are compatible, the most common is returned (see
// schema::CommonSchema for more details).
// * In case such common schema does not exist, an error is returned.
// * Otherwise, the common schema along with a mask indicating the presence of
//   the attribute `attr_name` in each schema is returned. If all empty, OBJECT
//   is used as a common schema.
template <typename ImplT>
absl::StatusOr<std::pair<internal::DataItem, ImplT>>
GetObjCommonSchemaAttrWithPerItemMask(
    const internal::DataBagImpl& db_impl, const ImplT& impl,
    absl::string_view attr_name,
    internal::DataBagImpl::FallbackSpan fallbacks) {
  ASSIGN_OR_RETURN(auto schema_attr, db_impl.GetObjSchemaAttr(impl, fallbacks));
  ASSIGN_OR_RETURN(ImplT per_item_types,
                   GetSchemaAttrImpl(db_impl, schema_attr, attr_name, fallbacks,
                                     /*allow_missing=*/true));
  ASSIGN_OR_RETURN(auto attr_mask, internal::HasOp()(per_item_types));
  if (per_item_types.present_count() != 0) {
    ASSIGN_OR_RETURN(auto common_schema, schema::CommonSchema(per_item_types));
    return {{std::move(common_schema), std::move(attr_mask)}};
  } else {
    return {{internal::DataItem(), std::move(attr_mask)}};
  }
}

// Deduces result attribute schema for "GetAttr-like" operations.
//
// * If `schema` is NONE returns NONE.
// * If `schema` is OBJECT, returns the common value of `attr_name` attribute of
//   all the element schemas in `impl`.
// * Otherwise, returns `attr_name` attribute of `schema`.
template <typename ImplT>
absl::StatusOr<internal::DataItem> GetResultSchema(
    const internal::DataBagImpl& db_impl, const ImplT& impl,
    const internal::DataItem& schema, absl::string_view attr_name,
    internal::DataBagImpl::FallbackSpan fallbacks, bool allow_missing) {
  // TODO: Change after adding OBJECT_WITH_SCHEMA.
  if (schema == schema::kNone) {
    return internal::DataItem(schema::kNone);
  }
  // NOTE: Calling  `UnwrapIfNoFollowSchema` in 2 places below to save 1
  // creation and 1 move of DataItem.
  if (schema == schema::kObject) {
    ASSIGN_OR_RETURN(auto res_schema,
                     GetObjCommonSchemaAttr(db_impl, impl, attr_name, fallbacks,
                                            allow_missing));
    return UnwrapIfNoFollowSchema(res_schema);
  }
  ASSIGN_OR_RETURN(
      auto res_schema,
      GetSchemaAttrImpl(db_impl, schema, attr_name, fallbacks, allow_missing));
  return UnwrapIfNoFollowSchema(res_schema);
}

auto KodaErrorCausedByMissingObjectSchemaError(const DataSlice& self) {
  return [&](auto&& status_like) {
    RETURN_IF_ERROR(KodaErrorCausedByMissingObjectSchemaError(
        std::forward<decltype(status_like)>(status_like), self));
    ABSL_UNREACHABLE();
  };
}

auto KodaErrorCausedByNoCommonSchemaError(const DataBagPtr& db) {
  return [&](auto&& status_like) {
    RETURN_IF_ERROR(KodaErrorCausedByNoCommonSchemaError(
        std::forward<decltype(status_like)>(status_like), db));
    ABSL_UNREACHABLE();
  };
}

// Calls DataBagImpl::GetAttr on the specific implementation (DataSliceImpl or
// DataItem). Returns DataSliceImpl / DataItem data and fills `res_schema` with
// schema of the resulting DataSlice as side output.
// * In case `allow_missing_schema` is false, it is strict and returns an
//   error on missing attributes.
// * Otherwise, it allows missing attributes.
template <typename ImplT>
absl::StatusOr<ImplT> GetAttrImpl(const DataBagPtr& db, const ImplT& impl,
                                  const internal::DataItem& schema,
                                  absl::string_view attr_name,
                                  internal::DataItem& res_schema,
                                  bool allow_missing_schema) {
  RETURN_IF_ERROR(ValidateAttrLookupAllowed(db, impl, schema));
  const auto& db_impl = db->GetImpl();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks = fb_finder.GetFlattenFallbacks();
  if (schema == schema::kSchema) {
    if (attr_name == schema::kSchemaNameAttr) {
      res_schema = internal::DataItem(schema::kString);
    } else if (attr_name == schema::kSchemaMetadataAttr) {
      res_schema = internal::DataItem(schema::kObject);
    } else {
      res_schema = internal::DataItem(schema::kSchema);
    }
    return GetSchemaAttrImpl(db_impl, impl, attr_name, fallbacks,
                             allow_missing_schema);
  }
  std::optional<ImplT> attr_mask;
  if (schema == schema::kObject && attr_name == schema::kSchemaAttr) {
    res_schema = internal::DataItem(schema::kSchema);
  } else if (allow_missing_schema && schema == schema::kObject) {
    // If some __schema__ values do not contain the `attr_name` attribute, we
    // should avoid looking them up as data. Otherwise, we may still have data
    // but no corresponding schema which result in the data and schema being
    // incompatible.
    ASSIGN_OR_RETURN(std::tie(res_schema, attr_mask),
                     GetObjCommonSchemaAttrWithPerItemMask(
                         db_impl, impl, attr_name, fallbacks));
  } else {
    ASSIGN_OR_RETURN(
        res_schema, GetResultSchema(db_impl, impl, schema, attr_name, fallbacks,
                                    allow_missing_schema));
  }
  if (!res_schema.has_value()) {
    // Then the schema indicates that the result is empty (even though there
    // may be values at db_impl.GetAttr(...)).
    res_schema = internal::DataItem(schema::kNone);
    if constexpr (std::is_same_v<ImplT, internal::DataSliceImpl>) {
      return internal::DataSliceImpl::CreateEmptyAndUnknownType(impl.size());
    } else {
      return internal::DataItem();
    }
  }
  if (attr_mask.has_value()) {
    ASSIGN_OR_RETURN(auto impl_filtered,
                     internal::PresenceAndOp()(impl, *attr_mask));
    return db_impl.GetAttr(impl_filtered, attr_name, fallbacks);
  } else {
    return db_impl.GetAttr(impl, attr_name, fallbacks);
  }
}

template <typename ImplT>
absl::StatusOr<ImplT> HasAttrImpl(const DataBagPtr& db, const ImplT& impl,
                                  const internal::DataItem& schema,
                                  absl::string_view attr_name) {
  RETURN_IF_ERROR(ValidateAttrLookupAllowed(db, impl, schema));
  const auto& db_impl = db->GetImpl();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks = fb_finder.GetFlattenFallbacks();

  auto struct_has_attr =
      [&](const auto& attr_schema) -> absl::StatusOr<ImplT> {
    // Consider adding a low level HasAttr to avoid actually fetching the data.
    ASSIGN_OR_RETURN(auto attr, db_impl.GetAttr(impl, attr_name, fallbacks));
    ASSIGN_OR_RETURN(auto has_attr_schema, internal::HasOp()(attr_schema));
    ASSIGN_OR_RETURN(auto has_attr, internal::HasOp()(attr));
    return internal::PresenceAndOp()(has_attr, has_attr_schema);
  };

  if (schema == schema::kSchema) {
    ASSIGN_OR_RETURN(
        auto res_schema,
        db_impl.GetSchemaAttrAllowMissing(impl, attr_name, fallbacks));
    return internal::HasOp()(res_schema);
  } else if (schema == schema::kObject) {
    // `__schema__` is not an actual attribute of the underlying schema, so we
    // handle it specially.
    if (attr_name == schema::kSchemaAttr) {
      return struct_has_attr(internal::DataItem(schema::kSchema));
    }
    ASSIGN_OR_RETURN(auto schema_attr,
                     db_impl.GetObjSchemaAttr(impl, fallbacks));
    ASSIGN_OR_RETURN(auto attr_schema, db_impl.GetSchemaAttrAllowMissing(
                                           schema_attr, attr_name, fallbacks));
    return struct_has_attr(attr_schema);
  } else if (schema.is_struct_schema()) {
    ASSIGN_OR_RETURN(auto attr_schema, db_impl.GetSchemaAttrAllowMissing(
                                           schema, attr_name, fallbacks));
    return struct_has_attr(attr_schema);
  } else {
    // Then it doesn't have any attrs (e.g. schema==NONE). We check in
    // ValidateAttrLookupAllowed that looking up is allowed to begin with, so
    // we just return an empty slice here.
    if constexpr (std::is_same_v<ImplT, internal::DataSliceImpl>) {
      return internal::DataSliceImpl::CreateEmptyAndUnknownType(impl.size());
    } else {
      return internal::DataItem();
    }
  }
}

// Configures the error messages and the behavior when the attribute
// schema is missing for RhsHandler.
enum class RhsHandlerContext {
  kAttr = 0,
  kListItem = 1,
  kDict = 2,
};

// Validates that schemas of `rhs` and `lhs.attr_name` are compatible and makes
// them consistent by either casting of `rhs`, or changing `lhs.attr_name`
// schema (if implicit).
//
// If `is_readonly` is true, only explicit `lhs.attr_name` schemas are allowed
// and `lhs.attr_name` schema is never changed. In this case we also do not
// embed `rhs` schema when `lhs.attr_name` is OBJECT.
//
template <bool is_readonly = false>
class RhsHandler {
 public:
  using DataBagImplT =
      std::conditional_t<is_readonly, const internal::DataBagImpl,
                         internal::DataBagImpl>;

  RhsHandler(RhsHandlerContext context, const DataSlice& rhs,
             absl::string_view attr_name, bool overwrite_schema)
      : context_(context),
        rhs_(rhs),
        attr_name_(attr_name),
        overwrite_schema_(overwrite_schema) {}

  const DataSlice& GetValues() const {
    return casted_rhs_.has_value() ? *casted_rhs_ : rhs_;
  }

  // Verifies and processes schema of the DataSlice. It handles OBJECT schema
  // and Entity schema separately. Pass DataBagImpl to which all modifications
  // should be done:
  // * Overwriting IMPLICIT schema attribute both for `lhs` being an Object
  //   (`'__schema__'` attribute) and an Entity;
  // * Embedding `'__schema__'` attribute in case the stored schema for
  //   `attr_name_` is OBJECT and `rhs_` is an Entity.
  //
  // `db_impl` must be a DataBag of `lhs`. Never modifies `rhs` DataBag.
  absl::Status ProcessSchema(const DataSlice& lhs, DataBagImplT& db_impl,
                             internal::DataBagImpl::FallbackSpan fallbacks) {
    DCHECK(&lhs.GetBag()->GetImpl() == &db_impl);
    absl::Status status = absl::OkStatus();
    if (lhs.GetSchemaImpl() == schema::kObject) {
      status = lhs.VisitImpl([&](const auto& impl) -> absl::Status {
        ASSIGN_OR_RETURN(
            auto obj_schema, db_impl.GetObjSchemaAttr(impl, fallbacks),
            _.With(KodaErrorCausedByMissingObjectSchemaError(lhs)));
        return this->ProcessSchemaObjectAttr(obj_schema, db_impl, fallbacks);
      });
    } else if (lhs.GetSchemaImpl() != schema::kNone) {
      status = ProcessSchemaObjectAttr(lhs.GetSchemaImpl(), db_impl, fallbacks);
    }
    return status;
  }

  // Fetches the attribute `attr_name_` from `lhs_schema` from `db_impl`,
  // called, `attr_stored_schema`.
  // * For EXPLICIT `lhs_schema`: `attr_stored_schema` must be present and
  //   compatible with values' schema. Otherwise error is returned. If
  //   compatible, but different, it casts values to `attr_stored_schema`
  //   (involves `EmbedSchema` if casting to OBJECT).
  // * For IMPLICIT `lhs_schema`: `attr_stored_schema` is replaced with rhs_'
  //   schema.
  //
  // If `overwrite_schema=true`, both IMPLICIT and EXPLICIT schemas are
  // processed in the same way (as IMPLICIT schemas).
  absl::Status ProcessSchemaObjectAttr(
      const internal::DataItem& lhs_schema, DataBagImplT& db_impl,
      internal::DataBagImpl::FallbackSpan fallbacks) {
    ASSIGN_OR_RETURN(
        auto attr_stored_schema,
        db_impl.GetSchemaAttrAllowMissing(lhs_schema, attr_name_, fallbacks));
    // Error is returned in GetSchemaAttrAllowMissing if `lhs_schema` is not an
    // ObjectId (or empty).
    if (!lhs_schema.has_value()) {
      return absl::OkStatus();
    }
    auto schema_id = lhs_schema.value<internal::ObjectId>();
    if (schema_id.IsNoFollowSchema()) {
      return CannotSetAttrOnNoFollowSchemaErrorStatus();
    }
    if (schema_id.IsImplicitSchema() || overwrite_schema_ ||
        (context_ == RhsHandlerContext::kAttr &&
         !attr_stored_schema.has_value())) {
      if constexpr (is_readonly) {
        return absl::InternalError("cannot update schemas in readonly mode");
      } else {
        if (attr_stored_schema != rhs_.GetSchemaImpl()) {
          return db_impl.SetSchemaAttr(lhs_schema, attr_name_,
                                       rhs_.GetSchemaImpl());
        }
        return absl::OkStatus();
      }
    }
    if (!attr_stored_schema.has_value()) {
      // This can happen only for lists and dicts.
      return AttrSchemaMissingErrorStatus(lhs_schema);
    }
    return CastRhsTo(attr_stored_schema, db_impl);
  }

 private:
  // DataSlice version of `ProcessSchemaObjectAttr`, see the comment there.
  absl::Status ProcessSchemaObjectAttr(
      const internal::DataSliceImpl& lhs_schema, DataBagImplT& db_impl,
      internal::DataBagImpl::FallbackSpan fallbacks) {
    ASSIGN_OR_RETURN(
        auto attr_stored_schemas,
        db_impl.GetSchemaAttrAllowMissing(lhs_schema, attr_name_, fallbacks));
    // Error is returned in GetSchemaAttrAllowMissing if `lhs_schema`'s items
    // are not ObjectIds.
    if (lhs_schema.is_empty_and_unknown()) {
      return absl::OkStatus();
    }

    internal::DataItem value_schema = rhs_.GetSchemaImpl();
    bool has_implicit_schema = false;
    bool should_overwrite_schema = overwrite_schema_;
    std::optional<internal::DataItem> cast_to = std::nullopt;
    absl::Status status = absl::OkStatus();
    lhs_schema.template values<internal::ObjectId>().ForEachPresent(
        [&](int64_t id, internal::ObjectId schema_id) {
          if (status.ok() && schema_id.IsNoFollowSchema()) {
            status = CannotSetAttrOnNoFollowSchemaErrorStatus();
          }
        });
    RETURN_IF_ERROR(status);
    if (!overwrite_schema_) {
      if (attr_stored_schemas.present_count() < lhs_schema.present_count()) {
        if (context_ == RhsHandlerContext::kAttr) {
          should_overwrite_schema = true;
        } else {
          return AttrSchemaMissingErrorStatus(lhs_schema, attr_stored_schemas);
        }
      }
      RETURN_IF_ERROR(attr_stored_schemas.VisitValues(
          [&]<class T>(const arolla::DenseArray<T>& attr_stored_schemas_array)
              -> absl::Status {
            absl::Status status = absl::OkStatus();
            if constexpr (std::is_same_v<T, internal::ObjectId> ||
                          std::is_same_v<T, schema::DType>) {
              RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
                  [&](int64_t id, internal::ObjectId schema_id,
                      T attr_stored_schema) {
                    if (!status.ok()) {
                      return;
                    }
                    if (schema_id.IsImplicitSchema()) {
                      has_implicit_schema = true;
                      if (value_schema != attr_stored_schema) {
                        should_overwrite_schema = true;
                      }
                      return;
                    }
                    // lhs_schema is EXPLICIT.
                    if (cast_to.has_value() && cast_to != attr_stored_schema) {
                      // NOTE: If cast_to and attr_stored_schema are different,
                      // but compatible, we are still returning an error.
                      status = arolla::WithPayload(
                          absl::InvalidArgumentError(absl::StrFormat(
                              "assignment would require to cast values "
                              "to two different "
                              "types: %v and %v",
                              internal::DataItem(T(attr_stored_schema)),
                              *cast_to)),
                          MakeIncompatibleSchemaError(
                              internal::DataItem(T(attr_stored_schema))));
                      return;
                    }
                    if (!cast_to.has_value()) {
                      cast_to = internal::DataItem(attr_stored_schema);
                    }
                  },
                  lhs_schema.values<internal::ObjectId>(),
                  attr_stored_schemas_array));
            } else {
              DCHECK(false);
            }
            return status;
          }));
    }
    if (cast_to.has_value()) {
      RETURN_IF_ERROR(CastRhsTo(*cast_to, db_impl));
      value_schema = *cast_to;
    }
    // If we had implicit schemas and changed the type of rhs via casting, we
    // need to update the implicit schemas.
    should_overwrite_schema |= has_implicit_schema && casted_rhs_.has_value();
    if (should_overwrite_schema) {
      if constexpr (is_readonly) {
        return absl::InternalError("cannot update schemas in readonly mode");
      } else {
        // NOTE: Must happen after casting, because otherwise schema and data
        // might get out of sync (schema INT64, data INT32).
        return db_impl.SetSchemaAttr(lhs_schema, attr_name_, value_schema);
      }
    }
    return absl::OkStatus();
  }

  // NOTE: Explicit Entity -> Object casting is allowed to simplify the
  // lives for new users not familiar with the intricacies of entities vs
  // objects. All other casts are implicit-only following the rules in
  // go/koda=type-promotion.
  absl::Status CastRhsTo(const internal::DataItem& cast_to,
                         DataBagImplT& db_impl) {
    const internal::DataItem& value_schema = rhs_.GetSchemaImpl();
    if (cast_to == value_schema) {
      return absl::OkStatus();
    }
    // NOTE: primitives -> OBJECT casting is handled by generic code at a later
    // stage.
    if (cast_to == schema::kObject && value_schema.is_struct_schema()) {
      if constexpr (!is_readonly) {
        // Need to embed the schema, so we attach it to the DataBag.
        ASSIGN_OR_RETURN(auto to_object,
                         schema::ToObject::Make(value_schema, true, &db_impl));
        auto res =
            rhs_.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
              RETURN_IF_ERROR(to_object(impl));
              return DataSlice::Create(impl, rhs_.GetShape(),
                                       internal::DataItem(schema::kObject),
                                       nullptr);
            });
        if (res.ok()) {
          casted_rhs_ = *std::move(res);
          return absl::OkStatus();
        }
        return RhsCastingErrorStatus(cast_to);
      }
      return absl::OkStatus();
    }
    if (auto res = CastToNarrow(rhs_, cast_to); res.ok()) {
      casted_rhs_ = *std::move(res);
      return absl::OkStatus();
    }
    return RhsCastingErrorStatus(cast_to);
  }

  absl::Status RhsCastingErrorStatus(
      const internal::DataItem& attr_stored_schema) const {
    absl::Status status = absl::OkStatus();
    switch (context_) {
      case RhsHandlerContext::kAttr:
        status = absl::InvalidArgumentError(absl::StrFormat(
            "the schema for attribute '%s' is incompatible: expected %v, "
            "assigned %v",
            attr_name_, attr_stored_schema, rhs_.GetSchemaImpl()));
        break;
      case RhsHandlerContext::kListItem:
        status = absl::InvalidArgumentError(absl::StrFormat(
            "the schema for list items is incompatible: expected %v, assigned "
            "%v",
            attr_stored_schema, rhs_.GetSchemaImpl()));
        break;
      case RhsHandlerContext::kDict:
        absl::string_view dict_attr =
            attr_name_ == schema::kDictKeysSchemaAttr ? "keys" : "values";
        status = absl::InvalidArgumentError(absl::StrFormat(
            "the schema for dict %s is incompatible: expected %v, assigned %v",
            dict_attr, attr_stored_schema, rhs_.GetSchemaImpl()));
        break;
    }
    return arolla::WithPayload(std::move(status),
                               MakeIncompatibleSchemaError(attr_stored_schema));
  }

  absl::Status AttrSchemaMissingErrorStatus(
      const internal::DataItem& lhs_schema,
      std::optional<int64_t> item_index = std::nullopt) const {
    internal::MissingCollectionItemSchemaError error = {
        .missing_schema_item = lhs_schema,
        .collection_type =
            internal::MissingCollectionItemSchemaError::CollectionType::kList,
        .item_index = item_index};
    switch (context_) {
      case RhsHandlerContext::kAttr:
        return absl::InternalError(
            "we should have never raised for missing attr schema");
      case RhsHandlerContext::kListItem:
        return arolla::WithPayload(
            absl::InvalidArgumentError("the schema for list items is missing"),
            std::move(error));
      case RhsHandlerContext::kDict:
        absl::string_view dict_attr =
            attr_name_ == schema::kDictKeysSchemaAttr ? "keys" : "values";
        error.collection_type =
            internal::MissingCollectionItemSchemaError::CollectionType::kDict;
        return arolla::WithPayload(
            absl::InvalidArgumentError(absl::StrFormat(
                "the schema for dict %s is missing", dict_attr)),
            std::move(error));
    }
  }

  absl::Status AttrSchemaMissingErrorStatus(
      const internal::DataSliceImpl& lhs_schema,
      const internal::DataSliceImpl& attr_stored_schemas) const {
    std::optional<uint64_t> first_missing_schema_index;
    std::optional<internal::DataItem> first_missing_schema_item;
    RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
        [&first_missing_schema_index, &first_missing_schema_item](
            int64_t index, internal::DataItem schema_item,
            arolla::OptionalValue<internal::DataItem> attr_stored_schema) {
          if (!attr_stored_schema.present) {
            first_missing_schema_item = std::move(schema_item);
            first_missing_schema_index = index;
          }
        },
        lhs_schema.AsDataItemDenseArray(),
        attr_stored_schemas.AsDataItemDenseArray()));
    return AttrSchemaMissingErrorStatus(first_missing_schema_item.value(),
                                        first_missing_schema_index);
  }

  absl::Status CannotSetAttrOnNoFollowSchemaErrorStatus() const {
    return absl::InvalidArgumentError(
        "cannot set an attribute on an entity with a no-follow schema");
  }

  internal::IncompatibleSchemaError MakeIncompatibleSchemaError(
      const internal::DataItem& attr_stored_schema) const {
    return {
        .attr = std::string(attr_name_),
        .expected_schema = attr_stored_schema,
        .assigned_schema = rhs_.GetSchemaImpl(),
    };
  }

  RhsHandlerContext context_;
  const DataSlice& rhs_;
  absl::string_view attr_name_;
  bool overwrite_schema_;
  std::optional<DataSlice> casted_rhs_ = std::nullopt;
};

// Verify List Schema is valid for removal operations.
absl::Status VerifyListSchemaValid(const DataSlice& list,
                                   const internal::DataBagImpl& db_impl) {
  return list.VisitImpl([&](const auto& impl) -> absl::Status {
    // Call (and ignore the returned DataItem) to verify that the list has
    // appropriate schema (e.g. in case of OBJECT, all ListIds have __schema__
    // attribute).
    return GetResultSchema(db_impl, impl, list.GetSchemaImpl(),
                           schema::kListItemsSchemaAttr,
                           /*fallbacks=*/{},  // mutable db.
                           /*allow_missing=*/false)
        .status();
  });
}

// Deletes a schema attribute for the single item in case of IMPLICIT schemas
// and verifies the attribute exists in case of EXPLICIT schemas. Returns an
// error if the schema is missing.
absl::Status DelSchemaAttrItem(const internal::DataItem& schema_item,
                               absl::string_view attr_name,
                               internal::DataBagImpl& db_impl) {
  if (schema_item.has_value() &&
      !schema_item.holds_value<internal::ObjectId>()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "objects must have ObjectId(s) as __schema__ attribute, got %v",
        schema_item));
  }
  if (schema_item.is_implicit_schema()) {
    return db_impl.DelSchemaAttr(schema_item, attr_name);
  }
  // In case of EXPLICIT schemas, verify that it is not missing.
  return db_impl.GetSchemaAttr(schema_item, attr_name).status();
}

absl::Status AssertIsSliceSchema(const internal::DataItem& schema) {
  if (!schema.is_schema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("schema must contain either a DType or valid schema "
                        "ItemId, got %v",
                        schema));
  }
  if (schema.is_implicit_schema()) {
    return absl::InvalidArgumentError(
        "DataSlice cannot have an implicit schema as its schema");
  }
  return absl::OkStatus();
}

// Aligns `impl` with `to_schema` if `from_schema` allows it (e.g. is OBJECT).
template <class ImplT>
absl::StatusOr<ImplT> AlignDataWithSchema(ImplT impl,
                                          const internal::DataItem& from_schema,
                                          const internal::DataItem& to_schema) {
  return from_schema == schema::kObject ? schema::CastDataTo(impl, to_schema)
                                        : impl;
}

bool HasSchemaAttr(const internal::DataItem& schema_item,
                   absl::string_view attr, const internal::DataBagImpl& db_impl,
                   const FlattenFallbackFinder& fb_finder) {
  auto fallbacks = fb_finder.GetFlattenFallbacks();
  auto item_schema_or =
      db_impl.GetSchemaAttrAllowMissing(schema_item, attr, fallbacks);
  return item_schema_or.ok() && item_schema_or->has_value();
}

}  // namespace

absl::StatusOr<DataSlice> DataSlice::Create(internal::DataSliceImpl impl,
                                            JaggedShape shape,
                                            internal::DataItem schema,
                                            DataBagPtr db,
                                            Wholeness wholeness) {
  if (shape.size() != impl.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("shape size must be compatible with number of items: "
                        "shape_size=%d != items_size=%d",
                        shape.size(), impl.size()));
  }
  RETURN_IF_ERROR(VerifySchemaConsistency(schema, impl.dtype(),
                                          impl.is_empty_and_unknown()));
  if (shape.rank() == 0) {
    return DataSlice(impl[0], std::move(shape), std::move(schema),
                     std::move(db), wholeness == Wholeness::kWhole);
  }
  return DataSlice(std::move(impl), std::move(shape), std::move(schema),
                   std::move(db), wholeness == Wholeness::kWhole);
}

absl::StatusOr<DataSlice> DataSlice::Create(const internal::DataItem& item,
                                            internal::DataItem schema,
                                            DataBagPtr db,
                                            Wholeness wholeness) {
  RETURN_IF_ERROR(
      VerifySchemaConsistency(schema, item.dtype(),
                              /*empty_and_unknown=*/!item.has_value()));
  return DataSlice(item, JaggedShape::Empty(), std::move(schema), std::move(db),
                   wholeness == Wholeness::kWhole);
}

absl::StatusOr<DataSlice> DataSlice::CreateWithSchemaFromData(
    internal::DataSliceImpl impl, JaggedShape shape, DataBagPtr db,
    Wholeness wholeness) {
  if (impl.is_empty_and_unknown() || impl.is_mixed_dtype() ||
      impl.dtype() == arolla::GetQType<internal::ObjectId>()) {
    return absl::InvalidArgumentError(
        "creating a DataSlice without passing schema is supported only for "
        "primitive types where all items are the same");
  }
  internal::DataItem schema(schema::kSchema);
  if (impl.dtype() != arolla::GetQType<schema::DType>()) {
    ASSIGN_OR_RETURN(auto dtype, schema::DType::FromQType(impl.dtype()));
    schema = internal::DataItem(dtype);
  }
  return Create(std::move(impl), std::move(shape), std::move(schema),
                std::move(db), wholeness);
}

absl::StatusOr<DataSlice> DataSlice::CreateWithFlatShape(
    internal::DataSliceImpl impl, internal::DataItem schema, DataBagPtr db,
    Wholeness wholeness) {
  const auto size = impl.size();
  return Create(std::move(impl), JaggedShape::FlatFromSize(size),
                std::move(schema), std::move(db), wholeness);
}

absl::StatusOr<DataSlice> DataSlice::Create(const internal::DataItem& item,
                                            JaggedShape shape,
                                            internal::DataItem schema,
                                            DataBagPtr db,
                                            Wholeness wholeness) {
  RETURN_IF_ERROR(
      VerifySchemaConsistency(schema, item.dtype(),
                              /*empty_and_unknown=*/!item.has_value()));
  if (shape.rank() == 0) {
    return DataSlice(item, std::move(shape), std::move(schema), std::move(db),
                     wholeness == Wholeness::kWhole);
  } else {
    return DataSlice::Create(internal::DataSliceImpl::Create({item}),
                             std::move(shape), std::move(schema), std::move(db),
                             wholeness);
  }
}

absl::StatusOr<DataSlice> DataSlice::Create(
    absl::StatusOr<internal::DataSliceImpl> slice_or, JaggedShape shape,
    internal::DataItem schema, DataBagPtr db, Wholeness wholeness) {
  if (!slice_or.ok()) {
    return std::move(slice_or).status();
  }
  return DataSlice::Create(*std::move(slice_or), std::move(shape),
                           std::move(schema), std::move(db), wholeness);
}

absl::StatusOr<DataSlice> DataSlice::Create(
    absl::StatusOr<internal::DataItem> item_or, JaggedShape shape,
    internal::DataItem schema, DataBagPtr db, Wholeness wholeness) {
  if (!item_or.ok()) {
    return std::move(item_or).status();
  }
  return DataSlice::Create(*std::move(item_or), std::move(shape),
                           std::move(schema), std::move(db), wholeness);
}

absl::StatusOr<DataSlice> DataSlice::Reshape(
    DataSlice::JaggedShape shape) const {
  return VisitImpl([&](const auto& impl) {
    return DataSlice::Create(impl, std::move(shape), GetSchemaImpl(), GetBag());
  });
}

absl::StatusOr<DataSlice> DataSlice::Flatten(
    int64_t from_dim, std::optional<int64_t> to_dim) const {
  const int64_t rank = GetShape().rank();
  auto new_shape = arolla::JaggedShapeFlattenOp<JaggedShape>()(
      GetShape(), from_dim, to_dim.value_or(rank));
  return Reshape(std::move(new_shape));
}

DataSlice DataSlice::GetSchema() const {
  return *DataSlice::Create(GetSchemaImpl(),
                            internal::DataItem(schema::kSchema), GetBag());
}

absl::StatusOr<DataSlice> DataSlice::GetObjSchema() const {
  if (GetSchemaImpl() != schema::kObject) {
    return absl::InvalidArgumentError(
        "DataSlice must have OBJECT schema for get_obj_schema");
  }

  return VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto res, GetObjSchemaImpl(impl, GetBag()));
    return DataSlice(std::move(res), GetShape(),
                     internal::DataItem(schema::kSchema), GetBag());
  });
}

bool DataSlice::IsStructSchema() const {
  return GetSchemaImpl() == schema::kSchema && is_item() &&
         item().is_struct_schema();
}

bool DataSlice::IsEntitySchema() const {
  if (!IsStructSchema() || GetBag() == nullptr) {
    return false;
  }
  const auto& db_impl = GetBag()->GetImpl();
  FlattenFallbackFinder fb_finder(*GetBag());
  return !HasSchemaAttr(item(), schema::kListItemsSchemaAttr, db_impl,
                        fb_finder) &&
         !HasSchemaAttr(item(), schema::kDictKeysSchemaAttr, db_impl,
                        fb_finder) &&
         !HasSchemaAttr(item(), schema::kDictValuesSchemaAttr, db_impl,
                        fb_finder);
}

bool DataSlice::IsListSchema() const {
  if (!IsStructSchema() || GetBag() == nullptr) {
    return false;
  }
  const auto& db_impl = GetBag()->GetImpl();
  FlattenFallbackFinder fb_finder(*GetBag());
  return HasSchemaAttr(item(), schema::kListItemsSchemaAttr, db_impl,
                       fb_finder);
}

bool DataSlice::IsDictSchema() const {
  if (!IsStructSchema() || GetBag() == nullptr) {
    return false;
  }
  const auto& db_impl = GetBag()->GetImpl();
  FlattenFallbackFinder fb_finder(*GetBag());
  return HasSchemaAttr(item(), schema::kDictKeysSchemaAttr, db_impl,
                       fb_finder) &&
         HasSchemaAttr(item(), schema::kDictValuesSchemaAttr, db_impl,
                       fb_finder);
}

bool DataSlice::IsPrimitiveSchema() const {
  return (GetSchemaImpl() == schema::kSchema) && is_item() &&
         item().is_primitive_schema();
}

bool DataSlice::IsItemIdSchema() const {
  return (GetSchemaImpl() == schema::kSchema) && is_item() &&
         item().is_itemid_schema();
}

absl::StatusOr<DataSlice> DataSlice::WithSchema(const DataSlice& schema) const {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  DataBagPtr res_db = GetBag();
  if (schema.item().is_struct_schema()) {
    ASSIGN_OR_RETURN(res_db, WithAdoptedValues(res_db, schema));
  }
  return WithBag(std::move(res_db)).WithSchema(schema.item());
}

absl::StatusOr<DataSlice> DataSlice::WithSchema(
    internal::DataItem schema_item) const {
  return VisitImpl([&](const auto& impl) {
    return DataSlice::Create(impl, GetShape(), std::move(schema_item),
                             GetBag());
  });
}

absl::StatusOr<DataSlice> DataSlice::WithWholeness(Wholeness wholeness) const {
  return VisitImpl([&](const auto& impl) {
    return DataSlice::Create(impl, GetShape(), GetSchemaImpl(), GetBag(),
                             wholeness);
  });
}

absl::StatusOr<DataSlice> DataSlice::SetSchema(const DataSlice& schema) const {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  if (schema.item().is_struct_schema() && schema.GetBag() != nullptr) {
    if (GetBag() == nullptr) {
      return absl::InvalidArgumentError(
          "cannot set an Entity schema on a DataSlice without a DataBag.");
    }
    AdoptionQueue adoption_queue;
    adoption_queue.Add(schema);
    RETURN_IF_ERROR(adoption_queue.AdoptInto(*GetBag()));
  }
  return WithSchema(schema.item());
}

absl::Status DataSlice::VerifyIsSchema() const {
  if (GetSchemaImpl() != schema::kSchema) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "schema's schema must be SCHEMA, got: %v", GetSchemaImpl()));
  }
  if (GetShape().rank() != 0) {
    return absl::InvalidArgumentError(
        absl::StrFormat("schema can only be 0-rank schema slice, got: rank(%d)",
                        GetShape().rank()));
  }
  if (!item().is_schema()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "schema must contain either a DType or valid schema ItemId, got %v",
        item()));
  }
  return absl::OkStatus();
}

absl::Status DataSlice::VerifyIsPrimitiveSchema() const {
  if (GetSchemaImpl() != schema::kSchema) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "primitive schema's schema must be SCHEMA, got: %v", GetSchemaImpl()));
  }
  if (GetShape().rank() != 0) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "primitive schema can only be 0-rank schema slice, got: rank(%d)",
        GetShape().rank()));
  }
  if (!item().is_primitive_schema()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "primitive schema must contain a primitive DType, got %v", item()));
  }
  return absl::OkStatus();
}

absl::Status DataSlice::VerifyIsListSchema() const {
  if (IsListSchema()) {
    return absl::OkStatus();
  }
  RETURN_IF_ERROR(VerifyIsSchema());
  return absl::InvalidArgumentError(
      absl::StrFormat("expected List schema, got %s", SchemaToStr(*this)));
}

absl::Status DataSlice::VerifyIsDictSchema() const {
  if (IsDictSchema()) {
    return absl::OkStatus();
  }
  RETURN_IF_ERROR(VerifyIsSchema());
  return absl::InvalidArgumentError(
      absl::StrFormat("expected Dict schema, got %s", SchemaToStr(*this)));
}

absl::Status DataSlice::VerifyIsEntitySchema() const {
  if (IsEntitySchema()) {
    return absl::OkStatus();
  }
  RETURN_IF_ERROR(VerifyIsSchema());
  return absl::InvalidArgumentError(
      absl::StrFormat("expected Entity schema, got %s", SchemaToStr(*this)));
}

absl::StatusOr<DataSlice> DataSlice::GetNoFollowedSchema() const {
  RETURN_IF_ERROR(VerifyIsSchema());
  ASSIGN_OR_RETURN(auto orig_schema_item,
                   schema::GetNoFollowedSchemaItem(item()));
  return DataSlice(std::move(orig_schema_item), GetShape(), GetSchemaImpl(),
                   GetBag());
}

bool DataSlice::IsWhole() const {
  const auto& bag = GetBag();
  if (bag == nullptr) {
    return true;  // If there is no data, it is all reachable.
  }
  return internal_->is_whole_if_db_unmodified && !bag->HasMutableFallbacks() &&
         (!bag->IsMutable() || bag->GetImpl().IsPristine());
}

absl::StatusOr<DataSlice> DataSlice::ForkBag() const {
  ASSIGN_OR_RETURN(auto forked_db, GetBag()->Fork());
  return DataSlice(internal_->impl, GetShape(), GetSchemaImpl(),
                   std::move(forked_db), IsWhole());
}

DataSlice DataSlice::FreezeBag() const {
  const DataBagPtr& db = GetBag();
  if (db == nullptr) {
    return *this;
  }
  auto frozen_db = db->Freeze();
  return DataSlice(internal_->impl, GetShape(), GetSchemaImpl(),
                   std::move(frozen_db), IsWhole());
}

bool DataSlice::IsEquivalentTo(const DataSlice& other) const {
  if (this == &other || internal_ == other.internal_) {
    return true;
  }
  if (GetBag() != other.GetBag() ||
      !GetShape().IsEquivalentTo(other.GetShape()) ||
      GetSchemaImpl() != other.GetSchemaImpl() ||
      !VisitImpl([&]<class T>(const T& impl) {
        return impl.IsEquivalentTo(other.impl<T>());
      })) {
    return false;
  }
  return true;
}

absl::StatusOr<DataSlice::AttrNamesSet> DataSlice::GetAttrNames(
    bool union_object_attrs) const {
  if (GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot get available attributes without a DataBag");
  }
  const internal::DataBagImpl& db_impl = GetBag()->GetImpl();
  FlattenFallbackFinder fb_finder(*GetBag());
  auto fallbacks = fb_finder.GetFlattenFallbacks();
  if (GetSchemaImpl().holds_value<internal::ObjectId>()) {
    // For entities, just process the schema of the DataSlice.
    return GetAttrsFromSchemaItem(GetSchemaImpl(), db_impl, fallbacks);
  }
  auto result = VisitImpl(absl::Overload(
      [&](const internal::DataItem& item) {
        return GetAttrsFromDataItem(item, GetSchemaImpl(), db_impl, fallbacks);
      },
      [&](const internal::DataSliceImpl& slice) {
        return GetAttrsFromDataSlice(slice, GetSchemaImpl(), db_impl, fallbacks,
                                     union_object_attrs);
      }));
  if (!result.ok()) {
    return KodaErrorCausedByMissingObjectSchemaError(result.status(), *this);
  }
  return result;
}

absl::StatusOr<DataSlice> DataSlice::GetAttr(
    absl::string_view attr_name) const {
  ASSIGN_OR_RETURN(
      auto result,
      VisitImpl([&]<class T>(const T& impl) -> absl::StatusOr<DataSlice> {
        internal::DataItem res_schema;
        ASSIGN_OR_RETURN(
            auto res,
            GetAttrImpl(GetBag(), impl, GetSchemaImpl(), attr_name, res_schema,
                        /*allow_missing_schema=*/false),
            _.With(KodaErrorCausedByMissingObjectSchemaError(*this))
                .With(KodaErrorCausedByNoCommonSchemaError(GetBag())));
        ASSIGN_OR_RETURN(res, AlignDataWithSchema(std::move(res),
                                                  GetSchemaImpl(), res_schema));
        return DataSlice::Create(std::move(res), GetShape(),
                                 std::move(res_schema), GetBag());
      }),
      _.SetPrepend() << "failed to get attribute '" << attr_name << "': ");
  return result;
}

absl::StatusOr<DataSlice> DataSlice::GetAttrOrMissing(
    absl::string_view attr_name) const {
  ASSIGN_OR_RETURN(
      auto result,
      VisitImpl([&]<class T>(const T& impl) -> absl::StatusOr<DataSlice> {
        internal::DataItem res_schema;
        ASSIGN_OR_RETURN(
            auto res,
            GetAttrImpl(GetBag(), impl, GetSchemaImpl(), attr_name, res_schema,
                        /*allow_missing_schema=*/true),
            _.With(KodaErrorCausedByMissingObjectSchemaError(*this))
                .With(KodaErrorCausedByNoCommonSchemaError(GetBag())));
        ASSIGN_OR_RETURN(res, AlignDataWithSchema(std::move(res),
                                                  GetSchemaImpl(), res_schema));
        return DataSlice::Create(std::move(res), GetShape(),
                                 std::move(res_schema), GetBag());
      }),
      _.SetPrepend() << "failed to get attribute '" << attr_name << "': ");
  return result;
}

absl::StatusOr<DataSlice> DataSlice::GetAttrWithDefault(
    absl::string_view attr_name, const DataSlice& default_value) const {
  ASSIGN_OR_RETURN(auto expanded_default,
                   BroadcastToShape(default_value, GetShape()));
  return VisitImpl([&]<class T>(const T& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto result_or_missing, GetAttrOrMissing(attr_name));
    ASSIGN_OR_RETURN(
        auto result_schema,
        schema::CommonSchema(result_or_missing.GetSchemaImpl(),
                             default_value.GetSchemaImpl()),
        internal::KodaErrorFromCause(
            absl::StrFormat("failed to get attribute '%s' due to conflict with "
                            "the schema from the default value",
                            attr_name),
            KodaErrorCausedByNoCommonSchemaError(_, GetBag())));
    ASSIGN_OR_RETURN(auto result_db,
                     WithAdoptedValues(GetBag(), default_value));
    ASSIGN_OR_RETURN(auto res, internal::CoalesceWithFiltered(
                                   impl, result_or_missing.impl<T>(),
                                   expanded_default.impl<T>()));
    ASSIGN_OR_RETURN(res, schema::CastDataTo(res, result_schema));
    return DataSlice::Create(std::move(res), GetShape(),
                             std::move(result_schema), std::move(result_db));
  });
}

absl::StatusOr<DataSlice> DataSlice::HasAttr(
    absl::string_view attr_name) const {
  ASSIGN_OR_RETURN(
      auto result,
      VisitImpl([&]<class T>(const T& impl) -> absl::StatusOr<DataSlice> {
        ASSIGN_OR_RETURN(
            auto res, HasAttrImpl(GetBag(), impl, GetSchemaImpl(), attr_name));
        return DataSlice::Create(std::move(res), GetShape(),
                                 internal::DataItem(schema::kMask));
      }),
      _.SetPrepend() << "failed to check attribute '" << attr_name << "': ");
  return result;
}

absl::Status DataSlice::SetSchemaAttr(absl::string_view attr_name,
                                      const DataSlice& values) const {
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetBag()->GetMutableImpl());
  return VisitImpl([&]<class T>(const T& impl) {
    // NOTE: It is guaranteed that shape and values.GetShape() are equivalent
    // at this point and thus `impl` is also the same type.
    return db_mutable_impl.SetSchemaAttr(impl, attr_name, values.impl<T>());
  });
}

namespace {

absl::Status SetAttrImpl(const DataSlice& obj, absl::string_view attr_name,
                         const DataSlice& values, bool overwrite_schema) {
  DCHECK(obj.GetBag() != nullptr);  // Checked in CheckEligibleForSetAttr.
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   obj.GetBag()->GetMutableImpl());
  RhsHandler</*is_readonly=*/false> data_handler(
      RhsHandlerContext::kAttr, values, attr_name, overwrite_schema);
  if (attr_name == schema::kSchemaAttr) {
    if (values.GetSchemaImpl() != schema::kSchema) {
      return absl::InvalidArgumentError(absl::StrCat(
          "only schemas can be assigned to the '__schema__' attribute, got ",
          values.GetSchemaImpl()));
    }
  } else {
    RETURN_IF_ERROR(data_handler.ProcessSchema(obj, db_mutable_impl,
                                               /*fallbacks=*/{}));
  }
  return obj.VisitImpl([&]<class T>(const T& impl) -> absl::Status {
    return db_mutable_impl.SetAttr(impl, attr_name,
                                   data_handler.GetValues().impl<T>());
  });
}

absl::Status SetAttrForSingleItem(const internal::DataItem& obj,
                                  absl::string_view attr_name,
                                  const internal::DataItem& value,
                                  bool overwrite_schema,
                                  const internal::DataItem& obj_schema,
                                  const internal::DataItem& value_schema,
                                  const DataBagPtr& obj_db) {
  if (obj_schema == schema::kSchema) {
    ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                     obj_db->GetMutableImpl());
    return db_mutable_impl.SetSchemaAttr(obj, attr_name, value);
  }
  ASSIGN_OR_RETURN(auto obj_slice, DataSlice::Create(obj, obj_schema, obj_db));
  ASSIGN_OR_RETURN(
      auto value_slice,
      DataSlice::Create(value, DataSlice::JaggedShape::Empty(), value_schema));
  return SetAttrImpl(obj_slice, attr_name, value_slice, overwrite_schema);
}

}  // namespace

absl::Status DataSlice::SetAttr(absl::string_view attr_name,
                                const DataSlice& values,
                                bool overwrite_schema) const {
  RETURN_IF_ERROR(CheckEligibleForSetAttr(*this)).SetPrepend()
      << "failed to set attribute '" << attr_name << "': ";
  ASSIGN_OR_RETURN(auto expanded_values, BroadcastToShape(values, GetShape()),
                   _.With([&](auto status) {
                     return AttrAssignmentError(std::move(status),
                                                GetShape().rank(),
                                                values.GetShape().rank());
                   }));
  if (GetSchemaImpl() == schema::kSchema) {
    return SetSchemaAttr(attr_name, expanded_values);
  }
  return SetAttrImpl(*this, attr_name, expanded_values, overwrite_schema);
}

absl::Status DataSlice::SetAttr(const DataSlice& attr_name,
                                const DataSlice& values,
                                bool overwrite_schema) const {
  RETURN_IF_ERROR(CheckEligibleForSetAttr(*this)).SetPrepend()
      << "failed to set attribute: ";
  if (attr_name.IsEmpty()) {
    return absl::OkStatus();
  }
  RETURN_IF_ERROR(ExpectString("attr_name", attr_name));
  if (attr_name.is_item()) {
    absl::string_view attr_name_str =
        attr_name.item().value<arolla::Text>().view();
    return SetAttr(attr_name_str, values, overwrite_schema);
  }
  // NOTE: We already covered the scalar `attr_name` case above, and we are
  // using Align rather than AlignNonScalars, so it's guaranteed that
  // aligned_slices will be DataSliceImpl.
  ASSIGN_OR_RETURN(auto aligned_slices,
                   shape::Align(absl::Span<const DataSlice>{*this, attr_name}));
  const auto& aligned_obj = aligned_slices[0].impl<internal::DataSliceImpl>();
  const auto& aligned_attr_name =
      aligned_slices[1].impl<internal::DataSliceImpl>();

  ASSIGN_OR_RETURN(auto expanded_values,
                   BroadcastToShape(values, aligned_slices[0].GetShape()));
  absl::Status status = absl::OkStatus();
  RETURN_IF_ERROR(arolla::DenseArraysForEachPresent(
      [&](int64_t offset, internal::DataItem item, std::string_view attr_name,
          internal::DataItem value) {
        if (!status.ok()) {
          return;
        }
        status = SetAttrForSingleItem(item, attr_name, value, overwrite_schema,
                                      GetSchemaImpl(), values.GetSchemaImpl(),
                                      GetBag());
      },
      aligned_obj.AsDataItemDenseArray(),
      aligned_attr_name.values<arolla::Text>(),
      expanded_values.impl<internal::DataSliceImpl>().AsDataItemDenseArray()));
  return status;
}

absl::Status DataSlice::SetAttrs(absl::Span<const absl::string_view> attr_names,
                                 absl::Span<const DataSlice> values,
                                 bool overwrite_schema) const {
  DCHECK_EQ(attr_names.size(), values.size());
  for (int i = 0; i < attr_names.size(); ++i) {
    RETURN_IF_ERROR(SetAttr(attr_names[i], values[i], overwrite_schema));
  }
  return absl::OkStatus();
}

// Deletes attribute `attr_name` from "__schema__" attribute of `impl` for all
// implicit schemas.
template <typename ImplT>
absl::Status DelObjSchemaAttr(const ImplT& impl, absl::string_view attr_name,
                              internal::DataBagImpl& db_impl) {
  ASSIGN_OR_RETURN(auto schema_attr, db_impl.GetObjSchemaAttr(impl));
  if constexpr (std::is_same_v<ImplT, internal::DataItem>) {
    return DelSchemaAttrItem(schema_attr, attr_name, db_impl);
  } else {
    return schema_attr.VisitValues([&](const auto& array) -> absl::Status {
      using T = typename std::decay_t<decltype(array)>::base_type;
      if constexpr (std::is_same_v<T, internal::ObjectId>) {
        arolla::DenseArrayBuilder<internal::ObjectId> implicit_schemas_bldr(
            impl.size());
        array.ForEachPresent([&](int64_t id, internal::ObjectId schema_obj) {
          if (schema_obj.IsImplicitSchema()) {
            implicit_schemas_bldr.Set(id, schema_obj);
          }
        });
        return db_impl.DelSchemaAttr(
            internal::DataSliceImpl::CreateObjectsDataSlice(
                std::move(implicit_schemas_bldr).Build(),
                schema_attr.allocation_ids()),
            attr_name);
      }
      return absl::InternalError(
          "objects must have ObjectId(s) as __schema__ attribute");
    });
  }
}

absl::Status DataSlice::DelAttr(absl::string_view attr_name) const {
  if (GetSchemaImpl().is_primitive_schema() || ContainsAnyPrimitives()) {
    RETURN_IF_ERROR(AttrOnPrimitiveError(*this)).SetPrepend()
        << "failed to delete '" << attr_name << "' attribute; ";
  }
  if (GetSchemaImpl() == schema::kNone) {
    return absl::OkStatus();
  }
  if (GetSchemaImpl().is_itemid_schema()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "failed to delete '%s' attribute; ITEMIDs do not allow attribute "
        "access",
        attr_name));
  }
  if (GetBag() == nullptr) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "failed to delete '%s' attribute; the DataSlice is a reference without "
        "a bag",
        attr_name));
  }
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetBag()->GetMutableImpl());
  return VisitImpl([&]<class ImplT>(const ImplT& impl) -> absl::Status {
    if (GetSchemaImpl() == schema::kSchema) {
      return db_mutable_impl.DelSchemaAttr(impl, attr_name);
    }
    if (GetSchemaImpl() == schema::kObject) {
      RETURN_IF_ERROR(DelObjSchemaAttr(impl, attr_name, db_mutable_impl))
          .With(KodaErrorCausedByMissingObjectSchemaError(*this));
    } else if (GetSchemaImpl().holds_value<internal::ObjectId>()) {
      // Entity schema.
      RETURN_IF_ERROR(
          DelSchemaAttrItem(GetSchemaImpl(), attr_name, db_mutable_impl));
    } else {
      return absl::InvalidArgumentError(absl::StrFormat(
          "failed to delete '%s' attribute; cannot delete on a DataSlice with "
          "%v schema",
          attr_name, GetSchemaImpl()));
    }
    // Remove attribute data by overwriting with empty.
    if constexpr (std::is_same_v<ImplT, internal::DataSliceImpl>) {
      return db_mutable_impl.SetAttr(
          impl, attr_name,
          internal::DataSliceImpl::CreateEmptyAndUnknownType(impl.size()));
    } else {
      return db_mutable_impl.SetAttr(impl, attr_name, internal::DataItem());
    }
  });
}

absl::StatusOr<DataSlice> DataSlice::EmbedSchema(bool overwrite) const {
  if (!GetSchemaImpl().is_primitive_schema() &&
      !GetSchemaImpl().is_struct_schema() && GetSchemaImpl() != schema::kNone) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "schema embedding is only supported for a DataSlice with primitive, "
        "entity, list or dict schemas, got %v",
        GetSchemaImpl()));
  }
  return ToObject(*this, /*validate_schema=*/!overwrite);
}

bool DataSlice::ShouldApplyListOp() const {
  if (std::holds_alternative<internal::DataItem>(internal_->impl)) {
    if (item().is_list()) {
      return true;
    }
  } else {
    if (slice().dtype() == arolla::GetQType<internal::ObjectId>()) {
      for (auto opt_id : slice().values<internal::ObjectId>()) {
        if (opt_id.present && opt_id.value.IsList()) {
          return true;
        }
      }
    }
  }
  // Regardless of what the actual data is (in case it is empty_and_unknown,
  // operation will be successful, if it is non-ObjectId, error will be returned
  // from the op / method).
  return GetSchema().IsListSchema();
}

bool DataSlice::IsList() const {
  if (GetSchemaImpl() == schema::kObject || GetSchemaImpl() == schema::kNone) {
    return VisitImpl([]<typename T>(const T& impl) -> bool {
      return impl.ContainsOnlyLists();
    });
  } else {
    return GetSchema().IsListSchema();
  }
}

bool DataSlice::IsDict() const {
  if (GetSchemaImpl() == schema::kObject || GetSchemaImpl() == schema::kNone) {
    return VisitImpl([]<typename T>(const T& impl) -> bool {
      return impl.ContainsOnlyDicts();
    });
  } else {
    return GetSchema().IsDictSchema();
  }
}

bool DataSlice::IsEntity() const {
  if (GetSchemaImpl() == schema::kObject || GetSchemaImpl() == schema::kNone) {
    return VisitImpl([]<typename T>(const T& impl) -> bool {
      return impl.ContainsOnlyEntities();
    });
  }
  return GetSchema().IsEntitySchema();
}

absl::StatusOr<DataSlice> DataSlice::GetFromDict(const DataSlice& keys) const {
  if (GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot get dict values without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*GetBag());
  const JaggedShape& shape = MaxRankShape(GetShape(), keys.GetShape());
  // Note: expanding `this` has an overhead. In future we can try to optimize
  // it.
  ASSIGN_OR_RETURN(auto expanded_this, BroadcastToShape(*this, shape));
  ASSIGN_OR_RETURN(auto expanded_keys, BroadcastToShape(keys, shape));
  RhsHandler</*is_readonly=*/true> keys_handler(
      RhsHandlerContext::kDict, expanded_keys, schema::kDictKeysSchemaAttr,
      /*overwrite_schema=*/false);
  RETURN_IF_ERROR(keys_handler.ProcessSchema(*this, GetBag()->GetImpl(),
                                             fb_finder.GetFlattenFallbacks()))
      .With(KodaErrorCausedByIncompableSchemaError(GetBag(), keys.GetBag(),
                                                   *this))
      .With(KodaErrorCausedByMissingCollectionItemSchemaError(GetBag()));
  ASSIGN_OR_RETURN(auto res_schema, VisitImpl([&](const auto& impl) {
                     return GetResultSchema(GetBag()->GetImpl(), impl,
                                            GetSchemaImpl(),
                                            schema::kDictValuesSchemaAttr,
                                            fb_finder.GetFlattenFallbacks(),
                                            /*allow_missing=*/false);
                   }),
                   _.With(KodaErrorCausedByMissingObjectSchemaError(*this))
                       .With(KodaErrorCausedByNoCommonSchemaError(GetBag())));
  return expanded_this.VisitImpl(
      [&]<class T>(const T& impl) -> absl::StatusOr<DataSlice> {
        ASSIGN_OR_RETURN(auto res_impl,
                         GetBag()->GetImpl().GetFromDict(
                             impl, keys_handler.GetValues().impl<T>(),
                             fb_finder.GetFlattenFallbacks()));
        ASSIGN_OR_RETURN(
            res_impl, AlignDataWithSchema(std::move(res_impl), GetSchemaImpl(),
                                          res_schema));
        return DataSlice::Create(std::move(res_impl), shape,
                                 std::move(res_schema), GetBag());
      });
}

absl::Status DataSlice::SetInDict(const DataSlice& keys,
                                  const DataSlice& values) const {
  if (GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot set dict values without a DataBag");
  }
  const JaggedShape& shape = MaxRankShape(GetShape(), keys.GetShape());
  // Note: expanding `this` has an overhead. In future we can try to optimize
  // it.
  ASSIGN_OR_RETURN(auto expanded_this, BroadcastToShape(*this, shape));
  ASSIGN_OR_RETURN(auto expanded_keys, BroadcastToShape(keys, shape));
  ASSIGN_OR_RETURN(auto expanded_values, BroadcastToShape(values, shape),
                   _.With([&](auto status) {
                     return DictAssignmentError(std::move(status), shape.rank(),
                                                values.GetShape().rank());
                   }));

  AdoptionQueue adoption_queue;
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetBag()->GetMutableImpl());
  RhsHandler</*is_readonly=*/false> keys_handler(
      RhsHandlerContext::kDict, expanded_keys, schema::kDictKeysSchemaAttr,
      /*overwrite_schema=*/false);
  RETURN_IF_ERROR(keys_handler.ProcessSchema(*this, db_mutable_impl,
                                             /*fallbacks=*/{}))
      .With(KodaErrorCausedByIncompableSchemaError(GetBag(), keys.GetBag(),
                                                   *this))
      .With(KodaErrorCausedByMissingCollectionItemSchemaError(GetBag()));
  RhsHandler</*is_readonly=*/false> values_handler(
      RhsHandlerContext::kDict, expanded_values, schema::kDictValuesSchemaAttr,
      /*overwrite_schema=*/false);
  RETURN_IF_ERROR(values_handler.ProcessSchema(*this, db_mutable_impl,
                                               /*fallbacks=*/{}))
      .With(KodaErrorCausedByIncompableSchemaError(GetBag(), values.GetBag(),
                                                   *this))
      .With(KodaErrorCausedByMissingCollectionItemSchemaError(GetBag()));

  adoption_queue.Add(keys);
  adoption_queue.Add(values);
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*GetBag()));
  return expanded_this.VisitImpl([&]<class T>(const T& impl) -> absl::Status {
    return db_mutable_impl.SetInDict(impl, keys_handler.GetValues().impl<T>(),
                                     values_handler.GetValues().impl<T>());
  });
}

absl::StatusOr<DataSlice> DataSlice::GetDictKeys() const {
  if (GetBag() == nullptr) {
    return absl::InvalidArgumentError("cannot get dict keys without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*GetBag());
  internal::DataItem res_schema(schema::kObject);
  return VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto res_schema,
                     GetResultSchema(GetBag()->GetImpl(), impl, GetSchemaImpl(),
                                     schema::kDictKeysSchemaAttr,
                                     fb_finder.GetFlattenFallbacks(),
                                     /*allow_missing=*/false),
                     _.With(KodaErrorCausedByMissingObjectSchemaError(*this))
                         .With(KodaErrorCausedByNoCommonSchemaError(GetBag())));
    ASSIGN_OR_RETURN(
        (auto [slice, edge]),
        GetBag()->GetImpl().GetDictKeys(impl, fb_finder.GetFlattenFallbacks()));
    ASSIGN_OR_RETURN(auto shape, GetShape().AddDims({std::move(edge)}));
    ASSIGN_OR_RETURN(slice, AlignDataWithSchema(std::move(slice),
                                                GetSchemaImpl(), res_schema));
    return DataSlice::Create(std::move(slice), std::move(shape),
                             std::move(res_schema), GetBag());
  });
}

absl::StatusOr<DataSlice> DataSlice::GetDictValues() const {
  if (GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot get dict values without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*GetBag());
  return VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto res_schema,
                     GetResultSchema(GetBag()->GetImpl(), impl, GetSchemaImpl(),
                                     schema::kDictValuesSchemaAttr,
                                     fb_finder.GetFlattenFallbacks(),
                                     /*allow_missing=*/false),
                     _.With(KodaErrorCausedByMissingObjectSchemaError(*this))
                         .With(KodaErrorCausedByNoCommonSchemaError(GetBag())));
    ASSIGN_OR_RETURN((auto [slice, edge]),
                     GetBag()->GetImpl().GetDictValues(
                         impl, fb_finder.GetFlattenFallbacks()));
    ASSIGN_OR_RETURN(auto shape, GetShape().AddDims({std::move(edge)}));
    ASSIGN_OR_RETURN(slice, AlignDataWithSchema(std::move(slice),
                                                GetSchemaImpl(), res_schema));
    return DataSlice::Create(std::move(slice), std::move(shape),
                             std::move(res_schema), GetBag());
  });
}

absl::StatusOr<DataSlice> DataSlice::GetFromList(
    const DataSlice& indices) const {
  if (GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot get list items without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*GetBag());
  const JaggedShape& shape = MaxRankShape(GetShape(), indices.GetShape());
  // Note: expanding `this` has an overhead. In future we can try to optimize
  // it.
  ASSIGN_OR_RETURN(auto expanded_this, BroadcastToShape(*this, shape));
  ASSIGN_OR_RETURN(
      DataSlice indices_int64,
      CastToNarrow(indices, internal::DataItem(schema::kInt64)),
      internal::KodaErrorFromCause(
          "cannot get items from list(s): expected indices to be integers", _));
  ASSIGN_OR_RETURN(auto expanded_indices,
                   BroadcastToShape(std::move(indices_int64), shape));
  ASSIGN_OR_RETURN(auto res_schema, VisitImpl([&](const auto& impl) {
                     return GetResultSchema(GetBag()->GetImpl(), impl,
                                            GetSchemaImpl(),
                                            schema::kListItemsSchemaAttr,
                                            fb_finder.GetFlattenFallbacks(),
                                            /*allow_missing=*/false);
                   }),
                   _.With(KodaErrorCausedByMissingObjectSchemaError(*this))
                       .With(KodaErrorCausedByNoCommonSchemaError(GetBag())));
  if (expanded_indices.present_count() == 0) {
    return EmptyLike(expanded_indices.GetShape(), res_schema, GetBag());
  }
  if (std::holds_alternative<internal::DataItem>(
          expanded_this.internal_->impl)) {
    int64_t index = expanded_indices.item().value<int64_t>();
    ASSIGN_OR_RETURN(auto res_impl, GetBag()->GetImpl().GetFromList(
                                        expanded_this.item(), index,
                                        fb_finder.GetFlattenFallbacks()));
    ASSIGN_OR_RETURN(
        res_impl,
        AlignDataWithSchema(std::move(res_impl), GetSchemaImpl(), res_schema));
    return DataSlice::Create(std::move(res_impl), shape, std::move(res_schema),
                             GetBag());
  } else {
    ASSIGN_OR_RETURN(
        auto res_impl,
        GetBag()->GetImpl().GetFromLists(
            expanded_this.slice(), expanded_indices.slice().values<int64_t>(),
            fb_finder.GetFlattenFallbacks()));
    ASSIGN_OR_RETURN(
        res_impl,
        AlignDataWithSchema(std::move(res_impl), GetSchemaImpl(), res_schema));
    return DataSlice::Create(std::move(res_impl), shape, std::move(res_schema),
                             GetBag());
  }
}

absl::StatusOr<DataSlice> DataSlice::ExplodeList(
    int64_t start, std::optional<int64_t> stop) const {
  if (GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot get list items without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*GetBag());

  return this->VisitImpl([&]<class T>(
                             const T& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto schema,
                     GetResultSchema(GetBag()->GetImpl(), impl, GetSchemaImpl(),
                                     schema::kListItemsSchemaAttr,
                                     fb_finder.GetFlattenFallbacks(),
                                     /*allow_missing=*/false),
                     _.With(KodaErrorCausedByMissingObjectSchemaError(*this))
                         .With(KodaErrorCausedByNoCommonSchemaError(GetBag())));
    if constexpr (std::is_same_v<T, internal::DataItem>) {
      ASSIGN_OR_RETURN(auto values,
                       GetBag()->GetImpl().ExplodeList(
                           impl, internal::DataBagImpl::ListRange(start, stop),
                           fb_finder.GetFlattenFallbacks()));
      ASSIGN_OR_RETURN(values, AlignDataWithSchema(std::move(values),
                                                   GetSchemaImpl(), schema));
      auto shape = JaggedShape::FlatFromSize(values.size());
      return DataSlice::Create(std::move(values), std::move(shape),
                               std::move(schema), GetBag());
    } else {
      ASSIGN_OR_RETURN((auto [values, edge]),
                       GetBag()->GetImpl().ExplodeLists(
                           impl, internal::DataBagImpl::ListRange(start, stop),
                           fb_finder.GetFlattenFallbacks()));
      ASSIGN_OR_RETURN(values, AlignDataWithSchema(std::move(values),
                                                   GetSchemaImpl(), schema));
      ASSIGN_OR_RETURN(auto shape, GetShape().AddDims({edge}));
      return DataSlice::Create(std::move(values), std::move(shape),
                               std::move(schema), GetBag());
    }
  });
}

absl::StatusOr<DataSlice> DataSlice::PopFromList(
    const DataSlice& indices) const {
  if (GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot pop items from list without a DataBag");
  }
  const JaggedShape& shape = MaxRankShape(GetShape(), indices.GetShape());
  // Note: expanding `this` has an overhead. In future we can try to optimize
  // it.
  ASSIGN_OR_RETURN(auto expanded_this, BroadcastToShape(*this, shape));
  ASSIGN_OR_RETURN(DataSlice indices_int64,
                   CastToNarrow(indices, internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(auto expanded_indices,
                   BroadcastToShape(std::move(indices_int64), shape));
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetBag()->GetMutableImpl());
  ASSIGN_OR_RETURN(auto res_schema, VisitImpl([&](const auto& impl) {
                     return GetResultSchema(
                         db_mutable_impl, impl, GetSchemaImpl(),
                         schema::kListItemsSchemaAttr,
                         // No fallback finder for the mutable operation.
                         {},
                         /*allow_missing=*/false);
                   }),
                   _.With(KodaErrorCausedByMissingObjectSchemaError(*this))
                       .With(KodaErrorCausedByNoCommonSchemaError(GetBag())));
  if (expanded_indices.present_count() == 0) {
    return EmptyLike(expanded_indices.GetShape(), res_schema, GetBag());
  }
  if (std::holds_alternative<internal::DataItem>(
          expanded_this.internal_->impl)) {
    int64_t index = expanded_indices.item().value<int64_t>();
    ASSIGN_OR_RETURN(auto res_impl,
                     db_mutable_impl.PopFromList(expanded_this.item(), index));
    ASSIGN_OR_RETURN(
        res_impl,
        AlignDataWithSchema(std::move(res_impl), GetSchemaImpl(), res_schema));
    return DataSlice::Create(std::move(res_impl), shape, std::move(res_schema),
                             GetBag());
  } else {
    ASSIGN_OR_RETURN(
        auto res_impl,
        db_mutable_impl.PopFromLists(
            expanded_this.slice(), expanded_indices.slice().values<int64_t>()));
    ASSIGN_OR_RETURN(
        res_impl,
        AlignDataWithSchema(std::move(res_impl), GetSchemaImpl(), res_schema));
    return DataSlice::Create(std::move(res_impl), shape, std::move(res_schema),
                             GetBag());
  }
}

absl::StatusOr<DataSlice> DataSlice::PopFromList() const {
  ASSIGN_OR_RETURN(
      DataSlice indices,
      DataSlice::Create(internal::DataItem(int64_t{-1}),
                        internal::DataItem(schema::kInt64), nullptr));
  return PopFromList(indices);
}

absl::Status DataSlice::AppendToList(const DataSlice& values) const {
  if (GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot append items to list without a DataBag");
  }
  const JaggedShape& shape = MaxRankShape(GetShape(), values.GetShape());
  if (!GetShape().IsBroadcastableTo(shape)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "Lists DataSlice with shape=%s is not compatible with values shape=%s",
        arolla::Repr(GetShape()), arolla::Repr(shape)));
  }
  ASSIGN_OR_RETURN(auto expanded_values, BroadcastToShape(values, shape));
  AdoptionQueue adoption_queue;
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetBag()->GetMutableImpl());
  RhsHandler</*is_readonly=*/false> data_handler(RhsHandlerContext::kListItem,
                                                 expanded_values,
                                                 schema::kListItemsSchemaAttr,
                                                 /*overwrite_schema=*/false);
  RETURN_IF_ERROR(data_handler.ProcessSchema(*this, db_mutable_impl,
                                             /*fallbacks=*/{}))
      .With(KodaErrorCausedByMissingCollectionItemSchemaError(GetBag()));

  adoption_queue.Add(values);
  RETURN_IF_ERROR(adoption_queue.AdoptInto(*GetBag()));
  if (GetShape().rank() < shape.rank()) {
    return VisitImpl([&]<class T>(const T& impl) -> absl::Status {
      if constexpr (std::is_same_v<T, internal::DataItem>) {
        return db_mutable_impl.ExtendList(impl,
                                          data_handler.GetValues().slice());
      } else {
        auto edge = GetShape().GetBroadcastEdge(shape);
        return db_mutable_impl.ExtendLists(
            impl, data_handler.GetValues().slice(), edge);
      }
    });
  } else {
    return VisitImpl([&]<class T>(const T& impl) -> absl::Status {
      return db_mutable_impl.AppendToList(impl,
                                          data_handler.GetValues().impl<T>());
    });
  }
}

absl::Status DataSlice::SetInList(const DataSlice& indices,
                                  const DataSlice& values) const {
  if (GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot set list items without a DataBag");
  }
  const JaggedShape& shape = MaxRankShape(GetShape(), indices.GetShape());
  // Note: expanding `this` has an overhead. In future we can try to optimize
  // it.
  ASSIGN_OR_RETURN(auto expanded_this, BroadcastToShape(*this, shape));
  ASSIGN_OR_RETURN(
      DataSlice indices_int64,
      CastToNarrow(indices, internal::DataItem(schema::kInt64)),
      internal::KodaErrorFromCause(
          "cannot set items from list(s): expected indices to be integers", _));
  if (indices_int64.present_count() == 0) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(auto expanded_indices,
                   BroadcastToShape(std::move(indices_int64), shape));
  ASSIGN_OR_RETURN(auto expanded_values, BroadcastToShape(values, shape),
                   _.With([&](absl::Status status) {
                     return ListAssignmentError(std::move(status), shape.rank(),
                                                values.GetShape().rank());
                   }));
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetBag()->GetMutableImpl());
  RhsHandler</*is_readonly=*/false> data_handler(RhsHandlerContext::kListItem,
                                                 expanded_values,
                                                 schema::kListItemsSchemaAttr,
                                                 /*overwrite_schema=*/false);
  RETURN_IF_ERROR(data_handler.ProcessSchema(*this, db_mutable_impl,
                                             /*fallbacks=*/{}))
      .With(KodaErrorCausedByIncompableSchemaError(GetBag(), values.GetBag(),
                                                   *this))
      .With(KodaErrorCausedByNoCommonSchemaError(GetBag()));
  if (std::holds_alternative<internal::DataItem>(
          expanded_this.internal_->impl)) {
    int64_t index = expanded_indices.item().value<int64_t>();
    return db_mutable_impl.SetInList(
        expanded_this.item(), index,
        data_handler.GetValues().impl<internal::DataItem>());
  } else {
    return db_mutable_impl.SetInLists(
        expanded_this.slice(), expanded_indices.slice().values<int64_t>(),
        data_handler.GetValues().impl<internal::DataSliceImpl>());
  }
}

absl::Status DataSlice::ReplaceInList(int64_t start,
                                      std::optional<int64_t> stop,
                                      const DataSlice& values) const {
  if (GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot set items of a list without a DataBag");
  }
  auto rank = GetShape().rank();
  auto values_rank = values.GetShape().rank();
  if (values_rank == 0) {
    ASSIGN_OR_RETURN(auto exploded_list, ExplodeList(start, stop));
    ASSIGN_OR_RETURN(auto values_broadcasted,
                     BroadcastToShape(values, exploded_list.GetShape()));
    return ReplaceInList(start, stop, values_broadcasted);
  }

  if (values_rank != rank + 1) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "trying to modify a slice of lists with %d "
        "dimensions using a slice with %d "
        "dimensions, while %d dimensions are required. "
        "For example, instead of foo[1:3] = bar where bar is a list, write "
        "foo[1:3] = bar[:]",
        rank, values_rank, rank + 1));
  }
  if (!GetShape().IsBroadcastableTo(values.GetShape())) {
    return BroadcastToShape(*this, values.GetShape()).status();
  }

  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetBag()->GetMutableImpl());
  RhsHandler</*is_readonly=*/false> data_handler(
      RhsHandlerContext::kListItem, values, schema::kListItemsSchemaAttr,
      /*overwrite_schema=*/false);
  RETURN_IF_ERROR(data_handler.ProcessSchema(*this, db_mutable_impl,
                                             /*fallbacks=*/{}))
      .With(KodaErrorCausedByIncompableSchemaError(GetBag(), values.GetBag(),
                                                   *this))
      .With(KodaErrorCausedByMissingCollectionItemSchemaError(GetBag()));

  internal::DataBagImpl::ListRange list_range(start, stop);
  return VisitImpl([&]<class T>(const T& impl) -> absl::Status {
    // We have validated rank above, so it is always DataSliceImpl.
    const auto& values_impl =
        data_handler.GetValues().impl<internal::DataSliceImpl>();
    if constexpr (std::is_same_v<T, internal::DataItem>) {
      return db_mutable_impl.ReplaceInList(impl, list_range, values_impl);
    } else {
      return db_mutable_impl.ReplaceInLists(impl, list_range, values_impl,
                                            values.GetShape().edges().back());
    }
  });
}

absl::Status DataSlice::RemoveInList(const DataSlice& indices) const {
  const JaggedShape& shape = MaxRankShape(GetShape(), indices.GetShape());
  // Note: expanding `this` has an overhead. In future we can try to optimize
  // it.
  ASSIGN_OR_RETURN(auto expanded_this, BroadcastToShape(*this, shape));
  ASSIGN_OR_RETURN(
      DataSlice indices_int64,
      CastToNarrow(indices, internal::DataItem(schema::kInt64)),
      internal::KodaErrorFromCause(
          "cannot remove items from list(s): expected indices to be integers",
          _));
  if (indices_int64.present_count() == 0) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(auto expanded_indices,
                   BroadcastToShape(std::move(indices_int64), shape));
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetBag()->GetMutableImpl());
  RETURN_IF_ERROR(VerifyListSchemaValid(*this, db_mutable_impl))
      .With(KodaErrorCausedByMissingObjectSchemaError(*this))
      .With(KodaErrorCausedByNoCommonSchemaError(GetBag()));
  if (std::holds_alternative<internal::DataItem>(
          expanded_this.internal_->impl)) {
    int64_t index = expanded_indices.item().value<int64_t>();
    internal::DataBagImpl::ListRange range(index, index + 1);
    if (index == -1) {
      range = internal::DataBagImpl::ListRange(index);
    }
    return db_mutable_impl.RemoveInList(expanded_this.item(), range);
  } else {
    return db_mutable_impl.RemoveInList(
        expanded_this.slice(), expanded_indices.slice().values<int64_t>());
  }
}

absl::Status DataSlice::RemoveInList(int64_t start,
                                     std::optional<int64_t> stop) const {
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetBag()->GetMutableImpl());
  RETURN_IF_ERROR(VerifyListSchemaValid(*this, db_mutable_impl))
      .With(KodaErrorCausedByMissingObjectSchemaError(*this))
      .With(KodaErrorCausedByNoCommonSchemaError(GetBag()));
  internal::DataBagImpl::ListRange list_range(start, stop);
  return this->VisitImpl([&]<class T>(const T& impl) -> absl::Status {
    return db_mutable_impl.RemoveInList(impl, list_range);
  });
}

absl::StatusOr<DataSlice> DataSlice::GetItem(
    const DataSlice& key_or_index) const {
  return ShouldApplyListOp() ? GetFromList(key_or_index)
                             : GetFromDict(key_or_index);
}

absl::Status DataSlice::ClearDictOrList() const {
  if (GetBag() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot clear lists or dicts without a DataBag");
  }
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetBag()->GetMutableImpl());
  if (ShouldApplyListOp()) {
    return this->VisitImpl([&]<class T>(const T& impl) -> absl::Status {
      return db_mutable_impl.RemoveInList(impl,
                                          internal::DataBagImpl::ListRange());
    });
  } else {
    return this->VisitImpl([&]<class T>(const T& impl) -> absl::Status {
      return db_mutable_impl.ClearDict(impl);
    });
  }
}

// TODO: Explore whether deep schema verification through alloc_ids
// is feasible and useful. Given that Dict and List schema do not have their
// ObjectId flags, but rely on GetAttr, those checks should be closer to their
// creation.
absl::Status DataSlice::VerifySchemaConsistency(
    const internal::DataItem& schema, arolla::QTypePtr dtype,
    bool empty_and_unknown) {
  RETURN_IF_ERROR(AssertIsSliceSchema(schema));
  if (empty_and_unknown) {
    // Any schema can be assigned in this case, because there is no data in the
    // DataSlice.
    DCHECK(dtype == arolla::GetNothingQType());
    return absl::OkStatus();
  }
  if (schema.holds_value<internal::ObjectId>()) {
    if (!schema.value<internal::ObjectId>().IsSchema()) {
      return absl::InvalidArgumentError(
          "provided ItemId is an invalid Schema ItemId.");
    }
    if (dtype != arolla::GetQType<internal::ObjectId>()) {
      return absl::InvalidArgumentError(
          "DataSlice with an Entity schema must hold Entities or Objects.");
    }
    return absl::OkStatus();
  }
  // In case of OBJECT, each item has its own schema or is a primitive.
  if (schema == schema::kObject) {
    return absl::OkStatus();
  }
  if (schema == schema::kSchema) {
    if (dtype != arolla::GetQType<schema::DType>() &&
        dtype != arolla::GetQType<internal::ObjectId>() &&
        // Happens when schema slice contains both DTypes and SchemaIds.
        dtype != arolla::GetNothingQType()) {
      return absl::InvalidArgumentError(
          "a non-schema item cannot be present in a schema DataSlice.");
    }
    return absl::OkStatus();
  }
  if (schema == schema::kItemId) {
    if (dtype != arolla::GetQType<internal::ObjectId>()) {
      return absl::InvalidArgumentError(
          "ITEMID schema requires DataSlice to hold object ids.");
    }
    return absl::OkStatus();
  }
  if (schema == schema::kNone) {
    return absl::InvalidArgumentError(
        "NONE schema requires DataSlice to be empty and unknown");
  }
  // From this point, schema is definitely primitive (INT32, FLOAT32, etc.).
  DCHECK(schema.value<schema::DType>().is_primitive());
  if (auto schema_dtype = schema.value<schema::DType>();
      schema_dtype.qtype() != dtype) {
    return absl::InvalidArgumentError(
        absl::StrCat(schema_dtype,
                     " schema can only be assigned to a DataSlice that "
                     "contains only primitives of ",
                     schema_dtype));
  }
  return absl::OkStatus();
}

absl::StatusOr<DataSlice> EmptyLike(const DataSlice::JaggedShape& shape,
                                    internal::DataItem schema, DataBagPtr db) {
  return DataSlice::Create(
      internal::DataSliceImpl::CreateEmptyAndUnknownType(shape.size()), shape,
      std::move(schema), std::move(db));
}

absl::StatusOr<DataSlice> internal_broadcast::BroadcastToShapeSlow(
    const DataSlice& slice, DataSlice::JaggedShape shape) {
  auto edge = slice.GetShape().GetBroadcastEdge(shape);
  return DataSliceOp<internal::ExpandOp>()(
      slice, std::move(shape), slice.GetSchemaImpl(), slice.GetBag(), edge);
}

absl::StatusOr<DataSlice> CastOrUpdateSchema(
    const DataSlice& value, const internal::DataItem& lhs_schema,
    absl::string_view attr_name, bool overwrite_schema,
    internal::DataBagImpl& db_impl) {
  RhsHandler</*is_readonly=*/false> data_handler(RhsHandlerContext::kAttr,
                                                 /*rhs=*/value, attr_name,
                                                 overwrite_schema);
  RETURN_IF_ERROR(
      data_handler.ProcessSchemaObjectAttr(lhs_schema, db_impl, {}));
  return data_handler.GetValues();
}

absl::StatusOr<DataSlice> ListSize(const DataSlice& lists) {
  const auto& db = lists.GetBag();
  if (db == nullptr) {
    return absl::InvalidArgumentError(
        "not possible to get List size without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*db);
  internal::DataItem schema(schema::kInt64);
  return lists.VisitImpl([&]<class T>(
                              const T& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto res_impl, db->GetImpl().GetListSize(
                                        impl, fb_finder.GetFlattenFallbacks()));
    if constexpr (std::is_same_v<T, internal::DataItem>) {
      return DataSlice::Create(std::move(res_impl), lists.GetShape(),
                                std::move(schema), /*db=*/nullptr);
    } else {
      return DataSlice::Create(
          internal::DataSliceImpl::Create(std::move(res_impl)),
          lists.GetShape(), std::move(schema), /*db=*/nullptr);
    }
  });
}

}  // namespace koladata
