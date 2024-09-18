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
#include "koladata/data_slice.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "absl/base/nullability.h"
#include "absl/functional/overload.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/adoption_utils.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice_op.h"
#include "koladata/internal/casting.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/expand.h"
#include "koladata/internal/op_utils/has.h"
#include "koladata/internal/op_utils/presence_and.h"
#include "koladata/internal/op_utils/presence_or.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/repr_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/view_types.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

constexpr absl::string_view kExplicitSchemaIsMissingError =
    "the attribute '%s' is missing on the schema.";

const DataSlice::JaggedShape& MaxRankShape(const DataSlice::JaggedShape& s1,
                                           const DataSlice::JaggedShape& s2) {
  return s1.rank() < s2.rank() ? s2 : s1;
}

absl::StatusOr<DataSlice> EmptyLike(const DataSlice::JaggedShape& shape,
                                    internal::DataItem schema,
                                    std::shared_ptr<DataBag> db) {
  return DataSlice::Create(
      internal::DataSliceImpl::CreateEmptyAndUnknownType(shape.size()), shape,
      std::move(schema), std::move(db));
}

absl::StatusOr<internal::DataItem> UnwrapIfNoFollowSchema(
    const internal::DataItem& schema_item) {
  if (schema_item.holds_value<internal::ObjectId>() &&
      schema_item.value<internal::ObjectId>().IsNoFollowSchema()) {
    return schema::GetNoFollowedSchemaItem(schema_item);
  }
  return schema_item;
}

absl::Status AssignmentError(absl::Status status, size_t lhs_rank,
                             size_t rhs_rank) {
  if (rhs_rank > lhs_rank) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "trying to assign a slice with %d dimensions to a slice with only %d "
        "dimensions. To wrap the last dimension into a list, use kd.list()",
        rhs_rank, lhs_rank));
  }
  return status;
}

// Gets embedded schema from DataItem for primitives and objects.
absl::StatusOr<internal::DataItem> GetObjSchemaImpl(
    const internal::DataItem& item,
    const absl::Nullable<std::shared_ptr<DataBag>>& db) {
  internal::DataItem res;
  RETURN_IF_ERROR(item.VisitValue([&]<class T>(const T& value) -> absl::Status {
    if constexpr (arolla::meta::contains_v<schema::supported_primitive_dtypes,
                                           T>) {
      // Primitive
      res = internal::DataItem(schema::GetDType<T>());
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
      // Dtype is not supported
      return absl::InvalidArgumentError(
          "DataSlice cannot contain primitive schema items for get_obj_schema");
    }
  }));
  return res;
}

// Gets embedded schema from DataSliceImpl for primitives and objects.
absl::StatusOr<internal::DataSliceImpl> GetObjSchemaImpl(
    const internal::DataSliceImpl& impl,
    const absl::Nullable<std::shared_ptr<DataBag>>& db) {
  internal::DataSliceImpl::Builder builder(impl.size());
  arolla::DenseArrayBuilder<schema::DType> dtype_arr_builder(impl.size());

  RETURN_IF_ERROR(impl.VisitValues([&]<class T>(const arolla::DenseArray<T>&
                                                    array) -> absl::Status {
    if constexpr (arolla::meta::contains_v<schema::supported_primitive_dtypes,
                                           T>) {
      // Primitives
      // All dtypes for different primitives are put into one DenseArray,
      // thus add the DenseArray in the end. Primitive DenseArrays are
      // always disjoint.
      array.ForEachPresent([&](int64_t id, arolla::view_type_t<T> value) {
        dtype_arr_builder.Set(id, schema::GetDType<T>());
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
      builder.AddArray(obj_schemas.template values<internal::ObjectId>());
      return absl::OkStatus();
    } else {
      // DTypes are not supported
      return absl::InvalidArgumentError(
          "DataSlice cannot contain primitive schema items for get_obj_schema");
    }
  }));

  // All dtypes for different primitives are put into one DenseArray, thus add
  // the DenseArray in the end.
  builder.AddArray(std::move(dtype_arr_builder).Build());
  return std::move(builder).Build();
}

// Fetches all attribute names from `schema_item` and returns them ordered.
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
    return absl::InternalError("dtype of attribute names must be TEXT");
  }
  if (attrs.present_count() != attrs.size()) {
    return absl::InternalError("attributes must be non-empty");
  }
  DataSlice::AttrNamesSet result;
  attrs.values<arolla::Text>().ForEachPresent(
      [&](int64_t id, absl::string_view attr) {
        result.insert(std::string(attr));
      });
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

absl::StatusOr<DataSlice::AttrNamesSet> GetAttrsFromDataSlice(
    const internal::DataSliceImpl& slice, const internal::DataItem& ds_schema,
    const internal::DataBagImpl& db_impl,
    internal::DataBagImpl::FallbackSpan fallbacks) {
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
  RETURN_IF_ERROR(
      schemas->VisitValues([&]<class T>(const arolla::DenseArray<T>& array) {
        absl::Status status = absl::OkStatus();
        if constexpr (std::is_same_v<T, internal::ObjectId>) {
          array.ForEachPresent([&](int64_t id, T schema_item) {
            if (!status.ok() || (result && result->empty())) {
              return;
            }
            auto attrs_or = GetAttrsFromSchemaItem(
                internal::DataItem(schema_item), db_impl, fallbacks);
            if (!attrs_or.ok()) {
              status = attrs_or.status();
              return;
            }
            if (!result) {
              result = *std::move(attrs_or);
              return;
            }
            const auto& attrs = *attrs_or;
            absl::erase_if(*result, [&](auto a) { return !attrs.contains(a); });
          });
        }
        return status;
      }));
  return result.value_or(DataSlice::AttrNamesSet());
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
//   they are used for inferring the common schema. If all are missing, `OBJECT`
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
    return schema::CommonSchema(per_item_types);
  }
}

// Deduces result attribute schema for "GetAttr-like" operations.
//
// * If `schema` is ANY returns ANY.
// * If `schema` is OBJECT, returns the common value of `attr_name` attribute of
//   all the element schemas in `impl`.
// * Otherwise, returns `attr_name` attribute of `schema`.
template <typename ImplT>
absl::StatusOr<internal::DataItem> GetResultSchema(
    const internal::DataBagImpl& db_impl, const ImplT& impl,
    const internal::DataItem& schema, absl::string_view attr_name,
    internal::DataBagImpl::FallbackSpan fallbacks, bool allow_missing) {
  if (schema == schema::kAny) {
    return internal::DataItem(schema::kAny);
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

// Calls DataBagImpl::GetAttr on the specific implementation (DataSliceImpl or
// DataItem). Returns DataSliceImpl / DataItem data and fills `res_schema` with
// schema of the resulting DataSlice as side output.
// * In case `allow_missing_schema` is false, it is strict and returns an
//   error on missing attributes.
// * Otherwise, it allows missing. In case the schema is missing, the returned
//   DataSlice with have `ANY` schema.
template <typename ImplT>
absl::StatusOr<ImplT> GetAttrImpl(const std::shared_ptr<DataBag>& db,
                                  const ImplT& impl,
                                  const internal::DataItem& schema,
                                  absl::string_view attr_name,
                                  internal::DataItem& res_schema,
                                  bool allow_missing_schema) {
  if (db == nullptr) {
    return absl::InvalidArgumentError(
        absl::StrCat("cannot fetch attributes without a DataBag: ", attr_name));
  }
  if (schema.is_primitive_schema()) {
    return absl::InvalidArgumentError(
        "getting attributes from primitive values is not supported");
  }
  const auto& db_impl = db->GetImpl();
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks = fb_finder.GetFlattenFallbacks();
  if (schema == schema::kSchema) {
    res_schema = internal::DataItem(schema::kSchema);
    return GetSchemaAttrImpl(db_impl, impl, attr_name, fallbacks,
                             allow_missing_schema);
  }
  if (attr_name == schema::kSchemaAttr) {
    res_schema = internal::DataItem(schema::kSchema);
  } else {
    ASSIGN_OR_RETURN(
        res_schema, GetResultSchema(db_impl, impl, schema, attr_name, fallbacks,
                                    allow_missing_schema));
  }
  return db_impl.GetAttr(impl, attr_name, fallbacks);
}

// Function for `this.GetAttr(attr_name) | (default_value & has(this))`.
template <typename ImplT>
absl::StatusOr<ImplT> CoalesceWithFiltered(const ImplT& objects, const ImplT& l,
                                           const ImplT& r) {
  ASSIGN_OR_RETURN(auto objects_presence, internal::HasOp()(objects));
  ASSIGN_OR_RETURN(auto r_filtered,
                   internal::PresenceAndOp()(r, objects_presence));
  return internal::PresenceOrOp()(l, r_filtered);
}

// Configures error messages for RhsHandler.
enum class RhsHandlerErrorContext {
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

  RhsHandler(RhsHandlerErrorContext error_context, const DataSlice& rhs,
             absl::string_view attr_name)
      : error_context_(error_context), rhs_(rhs), attr_name_(attr_name) {}

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
    DCHECK(&lhs.GetDb()->GetImpl() == &db_impl);
    absl::Status status = absl::OkStatus();
    if (lhs.GetSchemaImpl() == schema::kObject) {
      status = lhs.VisitImpl([&](const auto& impl) -> absl::Status {
        ASSIGN_OR_RETURN(auto obj_schema,
                         db_impl.GetObjSchemaAttr(impl, fallbacks));
        return this->ProcessSchemaObjectAttr(obj_schema, db_impl, fallbacks);
      });
    } else if (lhs.GetSchemaImpl() != schema::kAny) {
      status = ProcessSchemaObjectAttr(lhs.GetSchemaImpl(), db_impl, fallbacks);
    }
    if (!status.ok()) {
      return AssembleErrorMessage(status,
                                  {.db = DataBag::ImmutableEmptyWithFallbacks(
                                       {rhs_.GetDb(), lhs.GetDb()}),
                                   .ds = lhs});
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
  // If `update_schema=true`, both IMPLICIT and EXPLICIT schemas are processed
  // in the same way (as IMPLICIT schemas).
  absl::Status ProcessSchemaObjectAttr(
      const internal::DataItem& lhs_schema, DataBagImplT& db_impl,
      internal::DataBagImpl::FallbackSpan fallbacks,
      bool update_schema = false) {
    ASSIGN_OR_RETURN(
        auto attr_stored_schema,
        db_impl.GetSchemaAttrAllowMissing(lhs_schema, attr_name_, fallbacks));
    // Error is returned in GetSchemaAttrAllowMissing if `lhs_schema` is not an
    // ObjectId (or empty).
    if (!lhs_schema.has_value()) {
      return absl::OkStatus();
    }
    if (lhs_schema.value<internal::ObjectId>().IsImplicitSchema() ||
        update_schema) {
      if constexpr (is_readonly) {
        return absl::InternalError(
            "cannot deal with implicit schemas on readonly databag");
      } else {
        if (attr_stored_schema != rhs_.GetSchemaImpl()) {
          return db_impl.SetSchemaAttr(lhs_schema, attr_name_,
                                       rhs_.GetSchemaImpl());
        }
        return absl::OkStatus();
      }
    }
    // lhs_schema is EXPLICIT.
    if (!attr_stored_schema.has_value()) {
      return absl::InvalidArgumentError(
          absl::StrFormat(kExplicitSchemaIsMissingError, attr_name_));
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
    bool should_update_implicit_schema = false;
    std::optional<internal::DataItem> cast_to = std::nullopt;
    absl::Status status = absl::OkStatus();
    lhs_schema.template values<internal::ObjectId>().ForEachPresent(
        [&](int64_t id, internal::ObjectId schema_id) {
          if (!status.ok()) {
            return;
          }
          auto attr_stored_schema = attr_stored_schemas[id];
          if (schema_id.IsImplicitSchema()) {
            has_implicit_schema = true;
            if (attr_stored_schema != rhs_.GetSchemaImpl()) {
              should_update_implicit_schema = true;
            }
            return;
          }
          // lhs_schema is EXPLICIT.
          if (!attr_stored_schema.has_value()) {
            status = absl::InvalidArgumentError(
                absl::StrFormat(kExplicitSchemaIsMissingError, attr_name_));
            return;
          }
          if (cast_to.has_value() && *cast_to != attr_stored_schema) {
            // NOTE: If cast_to and attr_stored_schema are different, but
            // compatible, we are still returning an error.
            status = internal::WithErrorPayload(
                absl::InvalidArgumentError(absl::StrFormat(
                    "Assignment would require to cast values to two different "
                    "types: %v and %v",
                    attr_stored_schema, *cast_to)),
                MakeIncompatibleSchemaError(attr_stored_schema));
            return;
          }
          cast_to = std::move(attr_stored_schema);
        });
    RETURN_IF_ERROR(status);
    if (cast_to.has_value()) {
      RETURN_IF_ERROR(CastRhsTo(*cast_to, db_impl));
      value_schema = *cast_to;
    }
    if (should_update_implicit_schema ||
        (has_implicit_schema && casted_rhs_.has_value())) {
      if constexpr (is_readonly) {
        return absl::InternalError(
            "cannot deal with implicit schemas on readonly databag");
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
    if (cast_to == value_schema || cast_to == schema::kAny) {
      return absl::OkStatus();
    }
    // NOTE: primitives -> OBJECT casting is handled by generic code at a later
    // stage.
    if (cast_to == schema::kObject && value_schema.is_entity_schema()) {
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
    switch (error_context_) {
      case RhsHandlerErrorContext::kAttr:
        status = absl::InvalidArgumentError(absl::StrFormat(
            "the schema for attribute '%s' is incompatible: expected %v, "
            "assigned %v",
            attr_name_, attr_stored_schema, rhs_.GetSchemaImpl()));
        break;
      case RhsHandlerErrorContext::kListItem:
        status = absl::InvalidArgumentError(absl::StrFormat(
            "the schema for list items is incompatible: expected %v, assigned "
            "%v",
            attr_stored_schema, rhs_.GetSchemaImpl()));
        break;
      case RhsHandlerErrorContext::kDict:
        absl::string_view dict_attr =
            attr_name_ == schema::kDictKeysSchemaAttr ? "keys" : "values";
        status = absl::InvalidArgumentError(absl::StrFormat(
            "the schema for dict %s is incompatible: expected %v, assigned %v",
            dict_attr, attr_stored_schema, rhs_.GetSchemaImpl()));
        break;
    }
    return WithErrorPayload(status,
                            MakeIncompatibleSchemaError(attr_stored_schema));
  }

  absl::StatusOr<internal::Error> MakeIncompatibleSchemaError(
      const internal::DataItem& attr_stored_schema) const {
    internal::Error error;
    internal::IncompatibleSchema* incompatible_schema =
        error.mutable_incompatible_schema();
    incompatible_schema->set_attr(attr_name_);
    ASSIGN_OR_RETURN(*incompatible_schema->mutable_expected_schema(),
                     internal::EncodeDataItem(attr_stored_schema));
    ASSIGN_OR_RETURN(*incompatible_schema->mutable_assigned_schema(),
                     internal::EncodeDataItem(rhs_.GetSchemaImpl()));
    return error;
  }

  RhsHandlerErrorContext error_context_;
  const DataSlice& rhs_;
  absl::string_view attr_name_;
  std::optional<DataSlice> casted_rhs_ = std::nullopt;
};

// Verify List Schema is valid for removal operations.
absl::Status VerifyListSchemaValid(const DataSlice& list,
                                   const internal::DataBagImpl& db_impl) {
  return list.VisitImpl([&](const auto& impl) -> absl::Status {
    // Call (and ignore the returned DataItem) to verify that the list has
    // appropriate schema (e.g. in case of OBJECT, all ListIds have __schema__
    // attribute).
    absl::Status status = GetResultSchema(db_impl, impl, list.GetSchemaImpl(),
                                          schema::kListItemsSchemaAttr,
                                          /*fallbacks=*/{},  // mutable db.
                                          /*allow_missing=*/false)
                              .status();
    return AssembleErrorMessage(status, {.ds = list});
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

}  // namespace

absl::StatusOr<DataSlice> DataSlice::Create(internal::DataSliceImpl impl,
                                            JaggedShape shape,
                                            internal::DataItem schema,
                                            std::shared_ptr<DataBag> db) {
  if (shape.size() != impl.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("shape size must be compatible with number of items: "
                        "shape_size=%d != items_size=%d",
                        shape.size(), impl.size()));
  }
  RETURN_IF_ERROR(AssertIsSliceSchema(schema));
  // NOTE: Checking the invariant to avoid doing non-trivial verification in
  // prod.
  DCHECK_OK(VerifySchemaConsistency(schema, impl.dtype(),
                                    impl.is_empty_and_unknown()));
  if (shape.rank() == 0) {
    return DataSlice(impl[0], std::move(shape), std::move(schema),
                     std::move(db));
  }
  return DataSlice(std::move(impl), std::move(shape), std::move(schema),
                   std::move(db));
}

absl::StatusOr<DataSlice> DataSlice::Create(const internal::DataItem& item,
                                            internal::DataItem schema,
                                            std::shared_ptr<DataBag> db) {
  RETURN_IF_ERROR(AssertIsSliceSchema(schema));
  // NOTE: Checking the invariant to avoid doing non-trivial verification in
  // prod.
  DCHECK_OK(VerifySchemaConsistency(schema, item.dtype(),
                                    /*empty_and_unknown=*/!item.has_value()));
  return DataSlice(item, JaggedShape::Empty(), std::move(schema),
                   std::move(db));
}

absl::StatusOr<DataSlice> DataSlice::CreateWithSchemaFromData(
    internal::DataSliceImpl impl, JaggedShape shape,
    std::shared_ptr<DataBag> db) {
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
                std::move(db));
}

absl::StatusOr<DataSlice> DataSlice::Create(const internal::DataItem& item,
                                            JaggedShape shape,
                                            internal::DataItem schema,
                                            std::shared_ptr<DataBag> db) {
  RETURN_IF_ERROR(AssertIsSliceSchema(schema));
  DCHECK_OK(VerifySchemaConsistency(schema, item.dtype(),
                                    /*empty_and_unknown=*/!item.has_value()));
  if (shape.rank() == 0) {
    return DataSlice(item, std::move(shape), std::move(schema), std::move(db));
  } else {
    return DataSlice::Create(internal::DataSliceImpl::Create({item}),
                             std::move(shape), std::move(schema),
                             std::move(db));
  }
}

absl::StatusOr<DataSlice> DataSlice::Create(
    absl::StatusOr<internal::DataSliceImpl> slice_or, JaggedShape shape,
    internal::DataItem schema, std::shared_ptr<DataBag> db) {
  if (!slice_or.ok()) {
    return std::move(slice_or).status();
  }
  return DataSlice::Create(*std::move(slice_or), std::move(shape),
                           std::move(schema), std::move(db));
}

absl::StatusOr<DataSlice> DataSlice::Create(
    absl::StatusOr<internal::DataItem> item_or, JaggedShape shape,
    internal::DataItem schema, std::shared_ptr<DataBag> db) {
  if (!item_or.ok()) {
    return std::move(item_or).status();
  }
  return DataSlice::Create(*std::move(item_or), std::move(shape),
                           std::move(schema), std::move(db));
}

absl::StatusOr<DataSlice> DataSlice::Reshape(
    DataSlice::JaggedShape shape) const {
  return VisitImpl([&](const auto& impl) {
    return DataSlice::Create(impl, std::move(shape), GetSchemaImpl(), GetDb());
  });
}

DataSlice DataSlice::GetSchema() const {
  return *DataSlice::Create(GetSchemaImpl(),
                            internal::DataItem(schema::kSchema), GetDb());
}

absl::StatusOr<DataSlice> DataSlice::GetObjSchema() const {
  if (GetSchemaImpl() != schema::kObject) {
    return absl::InvalidArgumentError(
        "DataSlice must have OBJECT schema for get_obj_schema");
  }

  return VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto res, GetObjSchemaImpl(impl, GetDb()));
    return DataSlice(std::move(res), GetShape(),
                     internal::DataItem(schema::kSchema), GetDb());
  });
}

bool DataSlice::IsEntitySchema() const {
  return GetSchemaImpl() == schema::kSchema && GetShape().rank() == 0 &&
         item().is_entity_schema();
}

bool DataSlice::IsListSchema() const {
  if (!IsEntitySchema() || GetDb() == nullptr) {
    return false;
  }
  const auto& db_impl = GetDb()->GetImpl();
  FlattenFallbackFinder fb_finder(*GetDb());
  auto fallbacks = fb_finder.GetFlattenFallbacks();
  auto item_schema_or = db_impl.GetSchemaAttrAllowMissing(
      item(), schema::kListItemsSchemaAttr, fallbacks);
  if (!item_schema_or.ok()) {
    return false;
  }
  return item_schema_or->has_value();
}

bool DataSlice::IsDictSchema() const {
  if (!IsEntitySchema() || GetDb() == nullptr) {
    return false;
  }
  const auto& db_impl = GetDb()->GetImpl();
  FlattenFallbackFinder fb_finder(*GetDb());
  auto fallbacks = fb_finder.GetFlattenFallbacks();
  auto key_schema_or = db_impl.GetSchemaAttrAllowMissing(
      item(), schema::kDictKeysSchemaAttr, fallbacks);
  auto value_schema_or = db_impl.GetSchemaAttrAllowMissing(
      item(), schema::kDictValuesSchemaAttr, fallbacks);
  if (!key_schema_or.ok() || !value_schema_or.ok()) {
    return false;
  }
  return key_schema_or->has_value() && value_schema_or->has_value();
}

bool DataSlice::IsPrimitiveSchema() const {
  return (GetSchemaImpl() == schema::kSchema) && (GetShape().rank() == 0) &&
         item().is_primitive_schema();
}

absl::StatusOr<DataSlice> DataSlice::WithSchema(const DataSlice& schema) const {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  if (schema.item().is_entity_schema() && schema.GetDb() != nullptr &&
      GetDb() != schema.GetDb()) {
    return absl::InvalidArgumentError(
        "with_schema does not accept schemas with different DataBag attached. "
        "Please use `set_schema`");
  }
  return WithSchema(schema.item());
}

absl::StatusOr<DataSlice> DataSlice::WithSchema(
    internal::DataItem schema_item) const {
  RETURN_IF_ERROR(AssertIsSliceSchema(schema_item));
  RETURN_IF_ERROR(
      VerifySchemaConsistency(schema_item, dtype(), impl_empty_and_unknown()));
  return DataSlice(internal_->impl, GetShape(), std::move(schema_item),
                   GetDb());
}

absl::StatusOr<DataSlice> DataSlice::SetSchema(const DataSlice& schema) const {
  RETURN_IF_ERROR(schema.VerifyIsSchema());
  if (schema.item().is_entity_schema() && schema.GetDb() != nullptr) {
    if (GetDb() == nullptr) {
      return absl::InvalidArgumentError(
          "cannot set an Entity schema on a DataSlice without a DataBag.");
    }
    AdoptionQueue adoption_queue;
    adoption_queue.Add(schema);
    RETURN_IF_ERROR(adoption_queue.AdoptInto(*GetDb()));
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
  if (IsListSchema() || item() == schema::kAny) {
    return absl::OkStatus();
  }
  RETURN_IF_ERROR(VerifyIsSchema());
  return absl::InvalidArgumentError(
      absl::StrFormat("expected List schema, got %v", item()));
}

absl::Status DataSlice::VerifyIsDictSchema() const {
  if (IsDictSchema() || item() == schema::kAny) {
    return absl::OkStatus();
  }
  RETURN_IF_ERROR(VerifyIsSchema());
  return absl::InvalidArgumentError(
      absl::StrFormat("expected Dict schema, got %v", item()));
}

absl::StatusOr<DataSlice> DataSlice::GetNoFollowedSchema() const {
  RETURN_IF_ERROR(VerifyIsSchema());
  ASSIGN_OR_RETURN(auto orig_schema_item,
                   schema::GetNoFollowedSchemaItem(item()));
  return DataSlice(std::move(orig_schema_item), GetShape(), GetSchemaImpl(),
                   GetDb());
}

absl::StatusOr<DataSlice> DataSlice::ForkDb() const {
  ASSIGN_OR_RETURN(auto forked_db, GetDb()->Fork());
  return DataSlice(internal_->impl, GetShape(), GetSchemaImpl(),
                   std::move(forked_db));
}

absl::StatusOr<DataSlice> DataSlice::Freeze() const {
  ASSIGN_OR_RETURN(auto frozen_db, GetDb()->Fork(/*immutable=*/true));
  return DataSlice(internal_->impl, GetShape(), GetSchemaImpl(),
                   std::move(frozen_db));
}

bool DataSlice::IsEquivalentTo(const DataSlice& other) const {
  if (this == &other || internal_ == other.internal_) {
    return true;
  }
  if (GetDb() != other.GetDb() ||
      !GetShape().IsEquivalentTo(other.GetShape()) ||
      GetSchemaImpl() != other.GetSchemaImpl() ||
      !VisitImpl([&]<class T>(const T& impl) {
        return impl.IsEquivalentTo(other.impl<T>());
      })) {
    return false;
  }
  return true;
}

absl::StatusOr<DataSlice::AttrNamesSet> DataSlice::GetAttrNames() const {
  if (GetDb() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot get available attributes without a DataBag");
  }
  const internal::DataBagImpl& db_impl = GetDb()->GetImpl();
  FlattenFallbackFinder fb_finder(*GetDb());
  auto fallbacks = fb_finder.GetFlattenFallbacks();
  if (GetSchemaImpl().holds_value<internal::ObjectId>()) {
    // For entities, just process `schema_` of a DataSlice.
    return GetAttrsFromSchemaItem(GetSchemaImpl(), db_impl, fallbacks);
  }
  return VisitImpl(absl::Overload(
      [&](const internal::DataItem& item) {
        return GetAttrsFromDataItem(item, GetSchemaImpl(), db_impl, fallbacks);
      },
      [&](const internal::DataSliceImpl& slice) {
        return GetAttrsFromDataSlice(slice, GetSchemaImpl(), db_impl,
                                     fallbacks);
      }));
}

absl::StatusOr<DataSlice> DataSlice::GetAttr(
    absl::string_view attr_name) const {
  return VisitImpl([&]<class T>(const T& impl) -> absl::StatusOr<DataSlice> {
    internal::DataItem res_schema;
    ASSIGN_OR_RETURN(
        auto res,
        GetAttrImpl(GetDb(), impl, GetSchemaImpl(), attr_name, res_schema,
                    /*allow_missing_schema=*/false),
        AssembleErrorMessage(_, {.ds = *this}));
    // TODO: Use DataSlice::Create instead of verifying manually.
    RETURN_IF_ERROR(AssertIsSliceSchema(res_schema));
    return DataSlice(std::move(res), GetShape(), std::move(res_schema),
                     GetDb());
  });
}

absl::StatusOr<DataSlice> DataSlice::GetAttrWithDefault(
    absl::string_view attr_name, const DataSlice& default_value) const {
  ASSIGN_OR_RETURN(auto expanded_default,
                   BroadcastToShape(default_value, GetShape()));
  return VisitImpl([&]<class T>(const T& impl) -> absl::StatusOr<DataSlice> {
    internal::DataItem res_schema;
    ASSIGN_OR_RETURN(auto res, GetAttrImpl(GetDb(), impl, GetSchemaImpl(),
                                           attr_name, res_schema,
                                           /*allow_missing_schema=*/true));
    if (!res_schema.has_value()) {
      res_schema = internal::DataItem(schema::kAny);
      if (res.present_count() == 0 && GetSchemaImpl() != schema::kAny) {
        res_schema = default_value.GetSchemaImpl();
      } else if (res.dtype() != arolla::GetNothingQType()) {
        ASSIGN_OR_RETURN(auto dtype, schema::DType::FromQType(res.dtype()));
        res_schema = internal::DataItem(dtype);
      }
    }
    ASSIGN_OR_RETURN(
        res_schema,
        schema::CommonSchema(res_schema, default_value.GetSchemaImpl()),
        AssembleErrorMessage(_, {.ds = *this}));
    auto res_db = DataBag::CommonDataBag({GetDb(), default_value.GetDb()});
    return DataSlice::Create(
        CoalesceWithFiltered(impl, res, expanded_default.impl<T>()), GetShape(),
        std::move(res_schema), std::move(res_db));
  });
}

absl::Status DataSlice::SetSchemaAttr(absl::string_view attr_name,
                                      const DataSlice& values) const {
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetDb()->GetMutableImpl());
  return VisitImpl([&]<class T>(const T& impl) {
    // NOTE: It is guaranteed that shape and values.GetShape() are equivalent
    // at this point and thus `impl` is also the same type.
    return db_mutable_impl.SetSchemaAttr(impl, attr_name, values.impl<T>());
  });
}

absl::Status DataSlice::SetAttr(absl::string_view attr_name,
                                const DataSlice& values) const {
  if (GetDb() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot set attributes without a DataBag");
  }
  ASSIGN_OR_RETURN(auto expanded_values, BroadcastToShape(values, GetShape()),
                   _.With([&](auto status) {
                     return AssignmentError(std::move(status),
                                            GetShape().rank(),
                                            values.GetShape().rank());
                   }));
  if (GetSchemaImpl() == schema::kSchema) {
    return SetSchemaAttr(attr_name, expanded_values);
  }
  if (GetSchemaImpl().holds_value<schema::DType>()) {
    if (GetSchemaImpl().value<schema::DType>().is_primitive()) {
      return absl::InvalidArgumentError(
          "setting attributes on primitive slices is not allowed");
    }
    if (GetSchemaImpl().value<schema::DType>() == schema::kItemId) {
      return absl::InvalidArgumentError(
          "setting attributes on ITEMID slices is not allowed");
    }
  }
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetDb()->GetMutableImpl());
  RhsHandler</*is_readonly=*/false> data_handler(RhsHandlerErrorContext::kAttr,
                                                 expanded_values, attr_name);
  if (attr_name == schema::kSchemaAttr) {
    if (expanded_values.GetSchemaImpl() != schema::kSchema) {
      return absl::InvalidArgumentError(absl::StrCat(
          "only schemas can be assigned to the '__schema__' attribute, got ",
          expanded_values.GetSchemaImpl()));
    }
  } else {
    RETURN_IF_ERROR(
        data_handler.ProcessSchema(*this, db_mutable_impl, /*fallbacks=*/{}));
  }
  return VisitImpl([&]<class T>(const T& impl) -> absl::Status {
    return db_mutable_impl.SetAttr(impl, attr_name,
                                   data_handler.GetValues().impl<T>());
  });
}

absl::Status DataSlice::SetAttrWithUpdateSchema(absl::string_view attr_name,
                                                const DataSlice& values) const {
  std::optional<DataSlice> schema_slice;
  if (GetSchemaImpl() == schema::kObject) {
    ASSIGN_OR_RETURN(schema_slice, GetObjSchema());
  } else {
    schema_slice = GetSchema();
  }
  RETURN_IF_ERROR(schema_slice->SetAttr(attr_name, values.GetSchema()));
  return SetAttr(attr_name, values);
}

absl::Status DataSlice::SetAttrs(absl::Span<const absl::string_view> attr_names,
                                 absl::Span<const DataSlice> values,
                                 bool update_schema) const {
  DCHECK_EQ(attr_names.size(), values.size());
  auto set_attr_fn =
      update_schema ? &DataSlice::SetAttrWithUpdateSchema : &DataSlice::SetAttr;
  for (int i = 0; i < attr_names.size(); ++i) {
    RETURN_IF_ERROR((this->*set_attr_fn)(attr_names[i], values[i]));
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
  if (GetDb() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot delete attributes without a DataBag");
  }
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetDb()->GetMutableImpl());
  return VisitImpl([&]<class ImplT>(const ImplT& impl) -> absl::Status {
    if (GetSchemaImpl() == schema::kSchema) {
      return db_mutable_impl.DelSchemaAttr(impl, attr_name);
    }
    if (GetSchemaImpl() == schema::kObject) {
      RETURN_IF_ERROR(DelObjSchemaAttr(impl, attr_name, db_mutable_impl))
          .With([&](const absl::Status& status) {
            return AssembleErrorMessage(status, {.ds = *this});
          });
    } else if (GetSchemaImpl().holds_value<internal::ObjectId>()) {
      // Entity schema.
      RETURN_IF_ERROR(
          DelSchemaAttrItem(GetSchemaImpl(), attr_name, db_mutable_impl));
    } else if (GetSchemaImpl() != schema::kAny) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "Deleting an attribute cannot be done on a DataSlice with %v schema",
          GetSchemaImpl()));
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
      !GetSchemaImpl().is_entity_schema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("schema embedding is only supported for primitive and "
                        "entity schemas, got %v",
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

bool DataSlice::ContainsOnlyLists() const {
  if (GetSchemaImpl() == schema::kObject || GetSchemaImpl() == schema::kAny ||
      GetSchemaImpl() == schema::kItemId) {
    return VisitImpl([]<typename T>(const T& impl) -> bool {
      return impl.ContainsOnlyLists();
    });
  } else {
    return GetSchema().IsListSchema();
  }
}

bool DataSlice::ContainsOnlyDicts() const {
  if (GetSchemaImpl() == schema::kObject || GetSchemaImpl() == schema::kAny ||
      GetSchemaImpl() == schema::kItemId) {
    return VisitImpl([]<typename T>(const T& impl) -> bool {
      return impl.ContainsOnlyDicts();
    });
  } else {
    return GetSchema().IsDictSchema();
  }
}

absl::StatusOr<DataSlice> DataSlice::GetFromDict(const DataSlice& keys) const {
  if (GetDb() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot get dict values without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*GetDb());
  const JaggedShape& shape = MaxRankShape(GetShape(), keys.GetShape());
  // Note: expanding `this` has an overhead. In future we can try to optimize
  // it.
  ASSIGN_OR_RETURN(auto expanded_this, BroadcastToShape(*this, shape));
  ASSIGN_OR_RETURN(auto expanded_keys, BroadcastToShape(keys, shape));
  RhsHandler</*is_readonly=*/true> keys_handler(RhsHandlerErrorContext::kDict,
                                                expanded_keys,
                                                schema::kDictKeysSchemaAttr);
  RETURN_IF_ERROR(keys_handler.ProcessSchema(*this, GetDb()->GetImpl(),
                                             fb_finder.GetFlattenFallbacks()));
  ASSIGN_OR_RETURN(auto res_schema, VisitImpl([&](const auto& impl) {
                     return GetResultSchema(GetDb()->GetImpl(), impl,
                                            GetSchemaImpl(),
                                            schema::kDictValuesSchemaAttr,
                                            fb_finder.GetFlattenFallbacks(),
                                            /*allow_missing=*/false);
                   }),
                   AssembleErrorMessage(_, {.ds = *this}));
  // TODO: Use DataSlice::Create instead of verifying manually.
  RETURN_IF_ERROR(AssertIsSliceSchema(res_schema));
  return expanded_this.VisitImpl(
      [&]<class T>(const T& impl) -> absl::StatusOr<DataSlice> {
        ASSIGN_OR_RETURN(auto res_impl,
                         GetDb()->GetImpl().GetFromDict(
                             impl, keys_handler.GetValues().impl<T>(),
                             fb_finder.GetFlattenFallbacks()));
        return DataSlice(std::move(res_impl), shape, std::move(res_schema),
                         GetDb());
      });
}

absl::Status DataSlice::SetInDict(const DataSlice& keys,
                                  const DataSlice& values) const {
  if (GetDb() == nullptr) {
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
                     return AssignmentError(std::move(status), shape.rank(),
                                            values.GetShape().rank());
                   }));

  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetDb()->GetMutableImpl());
  RhsHandler</*is_readonly=*/false> keys_handler(RhsHandlerErrorContext::kDict,
                                                 expanded_keys,
                                                 schema::kDictKeysSchemaAttr);
  RETURN_IF_ERROR(keys_handler.ProcessSchema(*this, db_mutable_impl,
                                             /*fallbacks=*/{}));
  RhsHandler</*is_readonly=*/false> values_handler(
      RhsHandlerErrorContext::kDict, expanded_values,
      schema::kDictValuesSchemaAttr);
  RETURN_IF_ERROR(values_handler.ProcessSchema(*this, db_mutable_impl,
                                               /*fallbacks=*/{}));

  return expanded_this.VisitImpl([&]<class T>(const T& impl) -> absl::Status {
    return db_mutable_impl.SetInDict(impl, keys_handler.GetValues().impl<T>(),
                                     values_handler.GetValues().impl<T>());
  });
}

absl::StatusOr<DataSlice> DataSlice::GetDictKeys() const {
  if (GetDb() == nullptr) {
    return absl::InvalidArgumentError("cannot get dict keys without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*GetDb());
  internal::DataItem res_schema(schema::kAny);
  return VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto res_schema,
                     GetResultSchema(GetDb()->GetImpl(), impl, GetSchemaImpl(),
                                     schema::kDictKeysSchemaAttr,
                                     fb_finder.GetFlattenFallbacks(),
                                     /*allow_missing=*/false),
                     AssembleErrorMessage(_, {.ds = *this}));
    ASSIGN_OR_RETURN(
        (auto [slice, edge]),
        GetDb()->GetImpl().GetDictKeys(impl, fb_finder.GetFlattenFallbacks()));
    ASSIGN_OR_RETURN(auto shape, GetShape().AddDims({std::move(edge)}));
    return DataSlice::Create(std::move(slice), std::move(shape),
                             std::move(res_schema), GetDb());
  });
}

absl::StatusOr<DataSlice> DataSlice::GetDictValues() const {
  if (GetDb() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot get dict values without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*GetDb());
  return VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto res_schema,
                     GetResultSchema(GetDb()->GetImpl(), impl, GetSchemaImpl(),
                                     schema::kDictValuesSchemaAttr,
                                     fb_finder.GetFlattenFallbacks(),
                                     /*allow_missing=*/false),
                     AssembleErrorMessage(_, {.ds = *this}));
    ASSIGN_OR_RETURN((auto [slice, edge]),
                     GetDb()->GetImpl().GetDictValues(
                         impl, fb_finder.GetFlattenFallbacks()));
    ASSIGN_OR_RETURN(auto shape, GetShape().AddDims({std::move(edge)}));
    return DataSlice::Create(std::move(slice), std::move(shape),
                             std::move(res_schema), GetDb());
  });
}

absl::StatusOr<DataSlice> DataSlice::GetFromList(
    const DataSlice& indices) const {
  if (GetDb() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot get list items without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*GetDb());
  const JaggedShape& shape = MaxRankShape(GetShape(), indices.GetShape());
  // Note: expanding `this` has an overhead. In future we can try to optimize
  // it.
  ASSIGN_OR_RETURN(auto expanded_this, BroadcastToShape(*this, shape));
  ASSIGN_OR_RETURN(DataSlice indices_int64,
                   CastToNarrow(indices, internal::DataItem(schema::kInt64)));
  ASSIGN_OR_RETURN(auto expanded_indices,
                   BroadcastToShape(std::move(indices_int64), shape));
  ASSIGN_OR_RETURN(auto res_schema, VisitImpl([&](const auto& impl) {
                     return GetResultSchema(GetDb()->GetImpl(), impl,
                                            GetSchemaImpl(),
                                            schema::kListItemsSchemaAttr,
                                            fb_finder.GetFlattenFallbacks(),
                                            /*allow_missing=*/false);
                   }),
                   AssembleErrorMessage(_, {.ds = *this}));
  // TODO: Use DataSlice::Create instead of verifying manually.
  RETURN_IF_ERROR(AssertIsSliceSchema(res_schema));
  if (expanded_indices.present_count() == 0) {
    return EmptyLike(expanded_indices.GetShape(), res_schema, GetDb());
  }
  if (std::holds_alternative<internal::DataItem>(
          expanded_this.internal_->impl)) {
    int64_t index = expanded_indices.item().value<int64_t>();
    ASSIGN_OR_RETURN(auto res_impl, GetDb()->GetImpl().GetFromList(
                                        expanded_this.item(), index,
                                        fb_finder.GetFlattenFallbacks()));
    return DataSlice(std::move(res_impl), shape, std::move(res_schema),
                     GetDb());
  } else {
    ASSIGN_OR_RETURN(
        auto res_impl,
        GetDb()->GetImpl().GetFromLists(
            expanded_this.slice(), expanded_indices.slice().values<int64_t>(),
            fb_finder.GetFlattenFallbacks()));
    return DataSlice(std::move(res_impl), shape, std::move(res_schema),
                     GetDb());
  }
}

absl::StatusOr<DataSlice> DataSlice::ExplodeList(
    int64_t start, std::optional<int64_t> stop) const {
  if (GetDb() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot get list items without a DataBag");
  }
  FlattenFallbackFinder fb_finder(*GetDb());

  return this->VisitImpl([&]<class T>(
                             const T& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto schema,
                     GetResultSchema(GetDb()->GetImpl(), impl, GetSchemaImpl(),
                                     schema::kListItemsSchemaAttr,
                                     fb_finder.GetFlattenFallbacks(),
                                     /*allow_missing=*/false),
                     AssembleErrorMessage(_, {.ds = *this}));
    if constexpr (std::is_same_v<T, internal::DataItem>) {
      ASSIGN_OR_RETURN(auto values,
                       GetDb()->GetImpl().ExplodeList(
                           impl, internal::DataBagImpl::ListRange(start, stop),
                           fb_finder.GetFlattenFallbacks()));
      auto shape = JaggedShape::FlatFromSize(values.size());
      return DataSlice::Create(std::move(values), std::move(shape),
                               std::move(schema), GetDb());
    } else {
      ASSIGN_OR_RETURN((auto [values, edge]),
                       GetDb()->GetImpl().ExplodeLists(
                           impl, internal::DataBagImpl::ListRange(start, stop),
                           fb_finder.GetFlattenFallbacks()));
      ASSIGN_OR_RETURN(auto shape, GetShape().AddDims({edge}));
      return DataSlice::Create(std::move(values), std::move(shape),
                               std::move(schema), GetDb());
    }
  });
}

absl::StatusOr<DataSlice> DataSlice::PopFromList(
    const DataSlice& indices) const {
  if (GetDb() == nullptr) {
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
                   GetDb()->GetMutableImpl());
  ASSIGN_OR_RETURN(auto res_schema, VisitImpl([&](const auto& impl) {
                     return GetResultSchema(
                         db_mutable_impl, impl, GetSchemaImpl(),
                         schema::kListItemsSchemaAttr,
                         // No fallback finder for the mutable operation.
                         {},
                         /*allow_missing=*/false);
                   }),
                   AssembleErrorMessage(_, {.ds = *this}));
  // TODO: Use DataSlice::Create instead of verifying manually.
  RETURN_IF_ERROR(AssertIsSliceSchema(res_schema));
  if (expanded_indices.present_count() == 0) {
    return EmptyLike(expanded_indices.GetShape(), res_schema, GetDb());
  }
  if (std::holds_alternative<internal::DataItem>(
          expanded_this.internal_->impl)) {
    int64_t index = expanded_indices.item().value<int64_t>();
    ASSIGN_OR_RETURN(auto res_impl,
                     db_mutable_impl.PopFromList(expanded_this.item(), index));
    return DataSlice(std::move(res_impl), shape, std::move(res_schema),
                     GetDb());
  } else {
    ASSIGN_OR_RETURN(
        auto res_impl,
        db_mutable_impl.PopFromLists(
            expanded_this.slice(), expanded_indices.slice().values<int64_t>()));
    return DataSlice(std::move(res_impl), shape, std::move(res_schema),
                     GetDb());
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
  if (GetDb() == nullptr) {
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
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetDb()->GetMutableImpl());
  RhsHandler</*is_readonly=*/false> data_handler(
      RhsHandlerErrorContext::kListItem, expanded_values,
      schema::kListItemsSchemaAttr);
  RETURN_IF_ERROR(data_handler.ProcessSchema(*this, db_mutable_impl,
                                             /*fallbacks=*/{}));

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
  if (GetDb() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot set list items without a DataBag");
  }
  const JaggedShape& shape = MaxRankShape(GetShape(), indices.GetShape());
  // Note: expanding `this` has an overhead. In future we can try to optimize
  // it.
  ASSIGN_OR_RETURN(auto expanded_this, BroadcastToShape(*this, shape));
  ASSIGN_OR_RETURN(DataSlice indices_int64,
                   CastToNarrow(indices, internal::DataItem(schema::kInt64)));
  if (indices_int64.present_count() == 0) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(auto expanded_indices,
                   BroadcastToShape(std::move(indices_int64), shape));
  ASSIGN_OR_RETURN(auto expanded_values, BroadcastToShape(values, shape),
                   _.With([&](absl::Status status) {
                     return AssignmentError(std::move(status), shape.rank(),
                                            values.GetShape().rank());
                   }));
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetDb()->GetMutableImpl());
  RhsHandler</*is_readonly=*/false> data_handler(
      RhsHandlerErrorContext::kListItem, expanded_values,
      schema::kListItemsSchemaAttr);
  RETURN_IF_ERROR(data_handler.ProcessSchema(*this, db_mutable_impl,
                                             /*fallbacks=*/{}));
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
  if (GetDb() == nullptr) {
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
                   GetDb()->GetMutableImpl());
  RhsHandler</*is_readonly=*/false> data_handler(
      RhsHandlerErrorContext::kListItem, values, schema::kListItemsSchemaAttr);
  RETURN_IF_ERROR(data_handler.ProcessSchema(*this, db_mutable_impl,
                                             /*fallbacks=*/{}));

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
  ASSIGN_OR_RETURN(DataSlice indices_int64,
                   CastToNarrow(indices, internal::DataItem(schema::kInt64)));
  if (indices_int64.present_count() == 0) {
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(auto expanded_indices,
                   BroadcastToShape(std::move(indices_int64), shape));
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetDb()->GetMutableImpl());
  RETURN_IF_ERROR(VerifyListSchemaValid(*this, db_mutable_impl));
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
                   GetDb()->GetMutableImpl());
  RETURN_IF_ERROR(VerifyListSchemaValid(*this, db_mutable_impl));
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
  if (GetDb() == nullptr) {
    return absl::InvalidArgumentError(
        "cannot clear lists or dicts without a DataBag");
  }
  ASSIGN_OR_RETURN(internal::DataBagImpl & db_mutable_impl,
                   GetDb()->GetMutableImpl());
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
  if (schema == schema::kAny || schema == schema::kObject) {
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

absl::StatusOr<DataSlice> internal_broadcast::BroadcastToShapeSlow(
    const DataSlice& slice, DataSlice::JaggedShape shape) {
  auto edge = slice.GetShape().GetBroadcastEdge(shape);
  return DataSliceOp<internal::ExpandOp>()(
      slice, std::move(shape), slice.GetSchemaImpl(), slice.GetDb(), edge);
}

absl::StatusOr<DataSlice> CastOrUpdateSchema(
    const DataSlice& value, const internal::DataItem& lhs_schema,
    absl::string_view attr_name, bool update_schema,
    internal::DataBagImpl& db_impl) {
  RhsHandler</*is_readonly=*/false> data_handler(RhsHandlerErrorContext::kAttr,
                                                 /*rhs=*/value, attr_name);
  RETURN_IF_ERROR(data_handler.ProcessSchemaObjectAttr(lhs_schema, db_impl, {},
                                                       update_schema));
  return data_handler.GetValues();
}

}  // namespace koladata
