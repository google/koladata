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
#include "koladata/casting.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/deep_diff_utils.h"
#include "koladata/error_repr_utils.h"
#include "koladata/extract_utils.h"
#include "koladata/internal/casting.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/deep_diff.h"
#include "koladata/internal/op_utils/deep_schema_compatible.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/schema_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {
using DTypeMask = uint16_t;
static_assert(schema::kNextDTypeId <= sizeof(DTypeMask) * 8);

template <typename... DTypes>
constexpr DTypeMask GetDTypeMask(DTypes... dtypes) {
  return ((DTypeMask{1} << dtypes.type_id()) | ...);
}

std::string SchemaImplToStr(const internal::DataItem& schema,
                                            absl_nullable DataBagPtr db) {
  if (!schema.is_struct_schema() || db == nullptr) {
    return absl::StrCat(schema);
  }
  auto from_schema_slice_or = DataSlice::Create(
      schema, internal::DataItem(schema::kSchema), std::move(db));
  if (!from_schema_slice_or.ok()) {
    return absl::StrCat(schema);
  }
  return absl::StrFormat("%s with id %v", SchemaToStr(*from_schema_slice_or),
                         schema);
}

absl::Status VerifyCompatibleSchema(const DataSlice& slice,
                                    DTypeMask allowed_dtypes,
                                    schema::DType dst_dtype) {
  const auto& schema_item = slice.GetSchemaImpl();
  if (schema_item.holds_value<schema::DType>() &&
      allowed_dtypes & (1 << schema_item.value<schema::DType>().type_id())) {
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "casting a DataSlice with schema %s to %s is not supported",
      SchemaToStr(slice.GetSchema()), dst_dtype.name()));
}

constexpr DTypeMask kAllowedSchemasToNumeric =
    GetDTypeMask(schema::kNone, schema::kInt32, schema::kInt64,
                 schema::kFloat32, schema::kFloat64, schema::kBool,
                 schema::kString, schema::kBytes, schema::kObject);
constexpr DTypeMask kAllowedSchemasToExpr =
    GetDTypeMask(schema::kNone, schema::kExpr, schema::kObject);
constexpr DTypeMask kAllowedSchemasToStr =
    GetDTypeMask(schema::kNone, schema::kString, schema::kBytes, schema::kMask,
                 schema::kBool, schema::kInt32, schema::kInt64,
                 schema::kFloat32, schema::kFloat64, schema::kObject);
constexpr DTypeMask kAllowedSchemasToBytes =
    GetDTypeMask(schema::kNone, schema::kBytes, schema::kObject);
constexpr DTypeMask kAllowedSchemasDecode = GetDTypeMask(
    schema::kNone, schema::kString, schema::kBytes, schema::kObject);
constexpr DTypeMask kAllowedSchemasEncode = GetDTypeMask(
    schema::kNone, schema::kString, schema::kBytes, schema::kObject);
constexpr DTypeMask kAllowedSchemasToMask =
    GetDTypeMask(schema::kNone, schema::kMask, schema::kObject);
constexpr DTypeMask kAllowedSchemasToBool = GetDTypeMask(
    schema::kNone, schema::kInt32, schema::kInt64, schema::kFloat32,
    schema::kFloat64, schema::kBool, schema::kObject);
constexpr DTypeMask kAllowedSchemasToItemId = GetDTypeMask(
    schema::kNone, schema::kItemId, schema::kObject, schema::kSchema);
constexpr DTypeMask kAllowedSchemasToSchema =
    GetDTypeMask(schema::kNone, schema::kSchema, schema::kObject);

template <typename ToNumericImpl>
absl::StatusOr<DataSlice> ToNumericLike(const DataSlice& slice,
                                        schema::DType dst_dtype) {
  RETURN_IF_ERROR(
      VerifyCompatibleSchema(slice, kAllowedSchemasToNumeric, dst_dtype));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, ToNumericImpl()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(dst_dtype), slice.GetBag());
  });
}

absl::StatusOr<DataSlice> CastTo(const DataSlice& slice,
                                 const internal::DataItem& schema,
                                 CastToParams params) {
  DCHECK(schema.is_schema());
  if (schema.holds_value<internal::ObjectId>()) {
    return ToEntity(slice, schema, params.allow_removing_attrs,
                    params.allow_new_attrs);
  }
  switch (schema.value<schema::DType>().type_id()) {
    case schema::kNone.type_id():
      return ToNone(slice);
    case schema::kInt32.type_id():
      return ToInt32(slice);
    case schema::kInt64.type_id():
      return ToInt64(slice);
    case schema::kFloat32.type_id():
      return ToFloat32(slice);
    case schema::kFloat64.type_id():
      return ToFloat64(slice);
    case schema::kBool.type_id():
      return ToBool(slice);
    case schema::kMask.type_id():
      return ToMask(slice);
    case schema::kString.type_id():
      return ToStr(slice);
    case schema::kBytes.type_id():
      return ToBytes(slice);
    case schema::kExpr.type_id():
      return ToExpr(slice);
    case schema::kItemId.type_id():
      return ToItemId(slice);
    case schema::kSchema.type_id():
      return ToSchema(slice);
    case schema::kObject.type_id():
      return ToObject(slice, params.validate_schema);
  }
  ABSL_UNREACHABLE();
}

constexpr absl::string_view kOldName = "old_schema";
constexpr absl::string_view kNewName = "new_schema";

}  // namespace

namespace casting_internal {

bool IsProbablyCastableTo(const internal::DataItem& from_schema,
                          const internal::DataItem& to_schema) {
  DCHECK(from_schema.is_schema());
  DCHECK(to_schema.is_schema());

  if (to_schema.holds_value<internal::ObjectId>()) {
    // Note: If to_schema and from_schema are both entities, they would be
    // traversed further. If from_schema is Object, compatibility can only be
    // checked based on data stored.
    return from_schema.holds_value<internal::ObjectId>() ||
           from_schema == schema::kObject || from_schema == schema::kNone;
  }
  if (from_schema.holds_value<internal::ObjectId>()) {
    return to_schema == schema::kItemId || to_schema == schema::kNone;
  }

  auto from_schema_dtype_is_allowed = [&](DTypeMask mask) {
    return (1 << from_schema.value<schema::DType>().type_id()) & mask;
  };

  switch (to_schema.value<schema::DType>().type_id()) {
    case schema::kNone.type_id():
      return true;
    case schema::kInt32.type_id():
      return from_schema_dtype_is_allowed(kAllowedSchemasToNumeric);
    case schema::kInt64.type_id():
      return from_schema_dtype_is_allowed(kAllowedSchemasToNumeric);
    case schema::kFloat32.type_id():
      return from_schema_dtype_is_allowed(kAllowedSchemasToNumeric);
    case schema::kFloat64.type_id():
      return from_schema_dtype_is_allowed(kAllowedSchemasToNumeric);
    case schema::kBool.type_id():
      return from_schema_dtype_is_allowed(kAllowedSchemasToBool);
    case schema::kMask.type_id():
      return from_schema_dtype_is_allowed(kAllowedSchemasToMask);
    case schema::kString.type_id():
      return from_schema_dtype_is_allowed(kAllowedSchemasToStr);
    case schema::kBytes.type_id():
      return from_schema_dtype_is_allowed(kAllowedSchemasToBytes);
    case schema::kExpr.type_id():
      return from_schema_dtype_is_allowed(kAllowedSchemasToExpr);
    case schema::kItemId.type_id():
      return from_schema_dtype_is_allowed(kAllowedSchemasToItemId);
    case schema::kSchema.type_id():
      return from_schema_dtype_is_allowed(kAllowedSchemasToSchema);
    case schema::kObject.type_id():
      return false;
  }
  ABSL_UNREACHABLE();
}

absl::Status AssertSchemasCompatible(
    const DataSlice& from_schema, const internal::DataItem& to_schema_impl,
    internal::DeepSchemaCompatibleOp::SchemaCompatibleParams
        schema_compatibility_params,
    internal::DeepSchemaCompatibleOp::CompatibleCallback
        schemas_compatibility_callback) {
  const auto& from_schema_impl = from_schema.impl<internal::DataItem>();
  if (from_schema_impl == to_schema_impl) {
    return absl::OkStatus();
  }
  if (!from_schema_impl.holds_value<internal::ObjectId>() ||
      !to_schema_impl.holds_value<internal::ObjectId>()) {
    if (!schemas_compatibility_callback(from_schema_impl, to_schema_impl)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "(deep) casting from %v to entity schema %v is currently not "
          "supported",
          SchemaImplToStr(from_schema_impl, from_schema.GetBag()),
          SchemaImplToStr(to_schema_impl, from_schema.GetBag())));
    }
    return absl::OkStatus();
  }
  const auto& db = from_schema.GetBag();
  if (db == nullptr) {
    return absl::InvalidArgumentError(
        absl::StrFormat("(deep) casting from entity schema %v to entity schema "
                        "%v without DataBag is currently not supported",
                        from_schema_impl, to_schema_impl));
  }
  FlattenFallbackFinder fb_finder(*db);
  auto fallbacks_span = fb_finder.GetFlattenFallbacks();
  DataBagPtr new_databag = DataBag::EmptyMutable();
  ASSIGN_OR_RETURN(auto& new_databag_impl, new_databag->GetMutableImpl());
  internal::DeepSchemaCompatibleOp deep_schema_compatible_op(
      &new_databag_impl, schema_compatibility_params,
      schemas_compatibility_callback);
  ASSIGN_OR_RETURN(
      (auto [are_compatible, diff_item]),
      deep_schema_compatible_op(from_schema_impl, db->GetImpl(), fallbacks_span,
                                to_schema_impl, db->GetImpl(), fallbacks_span));
  if (are_compatible) {
    return absl::OkStatus();
  }
  ReprOption repr_option({.depth = 2,
                          .item_limit = 5,
                          .unbounded_type_max_len = 100,
                          .show_databag_id = false});
  auto diff_paths_or =
      deep_schema_compatible_op.GetDiffPaths(diff_item, /*max_count=*/5);
  if (!diff_paths_or.ok()) {
    LOG(ERROR) << "Failed to get diff paths: "
               << diff_paths_or.status().message();
    return absl::InvalidArgumentError(
        absl::StrFormat("DataSlice with schema %v cannot be cast to entity "
                        "schema %v; failed to generate diff representation",
                        SchemaImplToStr(from_schema_impl, db),
                        SchemaImplToStr(to_schema_impl, db)));
  }
  std::vector<std::string> mismatches;
  for (const auto& diff : *diff_paths_or) {
    auto diff_item_repr_or = DiffItemRepr(
        diff, new_databag_impl, db, internal::DeepDiff::kRhsAttr, kNewName, db,
        internal::DeepDiff::kLhsAttr, kOldName, repr_option);
    if (diff_item_repr_or.ok()) {
      mismatches.push_back(std::move(*diff_item_repr_or));
    } else {
      LOG(ERROR) << "Failed to get diff item repr: "
                 << diff_item_repr_or.status().message();
    }
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "DataSlice with schema %v\n\ncannot be cast to entity schema %v;\n\n%s",
      SchemaImplToStr(from_schema_impl, db),
      SchemaImplToStr(to_schema_impl, db), absl::StrJoin(mismatches, "\n\n")));
}

absl::StatusOr<DataSlice> CastByExtracting(
    const DataSlice& x, const internal::DataItem& schema_item,
    bool allow_removing_attrs, bool allow_new_attrs) {
  if (!schema_item.is_struct_schema()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "casting by extracting is only supported for entity schemas, got %v",
        schema_item));
  }
  RETURN_IF_ERROR(
      AssertSchemasCompatible(x.GetSchema(), schema_item,
                              {.allow_removing_attrs = allow_removing_attrs,
                               .allow_new_attrs = allow_new_attrs},
                              IsProbablyCastableTo));
  auto error_suffix = [&] {
    auto to_schema_str = SchemaImplToStr(schema_item, x.GetBag());
    return absl::StrFormat("when (deep) casting slice %s to entity schema %s",
                           DataSliceRepr(x, {.show_databag_id = false}),
                           to_schema_str);
  };
  ASSIGN_OR_RETURN(auto x_with_schema, x.WithSchema(schema_item),
                   _ << error_suffix());
  auto cast_data_callback =
      static_cast<absl::StatusOr<internal::DataSliceImpl> (*)(
          const internal::DataSliceImpl&, const internal::DataItem&)>(
          schema::CastDataTo);
  ASSIGN_OR_RETURN(auto result,
                   extract_utils_internal::ExtractWithSchema(
                       x_with_schema, x_with_schema.GetSchema(),
                       /*max_depth=*/-1, cast_data_callback),
                   _ << error_suffix());
  return result;
}

}  // namespace casting_internal

absl::StatusOr<DataSlice> ToInt32(const DataSlice& slice) {
  return ToNumericLike<schema::ToInt32>(slice, schema::kInt32);
}

absl::StatusOr<DataSlice> ToInt64(const DataSlice& slice) {
  return ToNumericLike<schema::ToInt64>(slice, schema::kInt64);
}

absl::StatusOr<DataSlice> ToFloat32(const DataSlice& slice) {
  return ToNumericLike<schema::ToFloat32>(slice, schema::kFloat32);
}

absl::StatusOr<DataSlice> ToFloat64(const DataSlice& slice) {
  return ToNumericLike<schema::ToFloat64>(slice, schema::kFloat64);
}

absl::StatusOr<DataSlice> ToNone(const DataSlice& slice) {
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToNone()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kNone), slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToExpr(const DataSlice& slice) {
  RETURN_IF_ERROR(
      VerifyCompatibleSchema(slice, kAllowedSchemasToExpr, schema::kExpr));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToExpr()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kExpr), slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToStr(const DataSlice& slice) {
  RETURN_IF_ERROR(
      VerifyCompatibleSchema(slice, kAllowedSchemasToStr, schema::kString));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToStr()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kString),
                             slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToBytes(const DataSlice& slice) {
  RETURN_IF_ERROR(
      VerifyCompatibleSchema(slice, kAllowedSchemasToBytes, schema::kBytes));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToBytes()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kBytes),
                             slice.GetBag());
  });
}

absl::StatusOr<DataSlice> Decode(const DataSlice& slice,
                                 absl::string_view errors) {
  RETURN_IF_ERROR(
      VerifyCompatibleSchema(slice, kAllowedSchemasDecode, schema::kString));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::Decode()(impl, std::move(errors)));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kString),
                             slice.GetBag());
  });
}

absl::StatusOr<DataSlice> Encode(const DataSlice& slice) {
  RETURN_IF_ERROR(
      VerifyCompatibleSchema(slice, kAllowedSchemasEncode, schema::kBytes));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::Encode()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kBytes),
                             slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToMask(const DataSlice& slice) {
  RETURN_IF_ERROR(
      VerifyCompatibleSchema(slice, kAllowedSchemasToMask, schema::kMask));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToMask()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kMask), slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToBool(const DataSlice& slice) {
  if (auto status =
          VerifyCompatibleSchema(slice, kAllowedSchemasToBool, schema::kBool);
      !status.ok()) {
    if (slice.GetSchemaImpl() == schema::kMask) {
      RETURN_IF_ERROR(std::move(status))
          << "try `kd.cond(slice, True, False)` instead";
    } else {
      return status;
    }
  }
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToBool()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kBool), slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToItemId(const DataSlice& slice) {
  if (slice.GetSchemaImpl().holds_value<schema::DType>()) {
    RETURN_IF_ERROR(VerifyCompatibleSchema(slice, kAllowedSchemasToItemId,
                                           schema::kItemId));
  }
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToItemId()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kItemId),
                             slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToSchema(const DataSlice& slice) {
  if (slice.GetSchemaImpl().holds_value<schema::DType>()) {
    RETURN_IF_ERROR(VerifyCompatibleSchema(slice, kAllowedSchemasToSchema,
                                           schema::kSchema));
  }
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToSchema()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kSchema),
                             slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToEntity(const DataSlice& slice,
                                   const internal::DataItem& entity_schema,
                                   bool allow_removing_attrs,
                                   bool allow_new_attrs) {
  if (!entity_schema.is_struct_schema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected an entity schema, got: %v", entity_schema));
  }
  internal::DataItem from_schema = slice.GetSchemaImpl();
  DataSlice slice_with_schema = slice;
  if (from_schema == schema::kObject) {
    // In case of OBJECT, we validate that the existing __schema__ attributes
    // correspond to `entity_schema` and raise otherwise since we can end up
    // with a bad state.
    auto error_suffix = [&] {
      return absl::StrFormat(
          "when validating equivalence of existing __schema__ attributes with "
          "the target schema during explicit cast to %v",
          entity_schema);
    };
    ASSIGN_OR_RETURN(DataSlice obj_schema_slice, slice.GetObjSchema(),
                     _ << error_suffix());
    ASSIGN_OR_RETURN(
        from_schema,
        obj_schema_slice.VisitImpl(
            [&](const auto& impl) -> absl::StatusOr<internal::DataItem> {
              ASSIGN_OR_RETURN(internal::DataItem schema,
                               schema::CommonSchema(impl),
                               KodaErrorCausedByNoCommonSchemaError(
                                   _, obj_schema_slice.GetBag()));
              return schema.has_value() ? schema
                                        : internal::DataItem(schema::kNone);
            }),
        _ << error_suffix());
    if (from_schema.is_struct_schema()) {
      // TODO: this is a temporary tradeoff to have some checks
      // when casting Object to Entity. We should consider proper fix, where we
      // validate the object schemas recursively (not only on the top level).
      ASSIGN_OR_RETURN(slice_with_schema, slice.WithSchema(from_schema),
                       _ << error_suffix());
    }
  }
  if (from_schema == schema::kNone) {
    return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
      return DataSlice::Create(impl, slice.GetShape(), entity_schema,
                               slice.GetBag());
    });
  }
  if (from_schema == entity_schema) {
    return slice.WithSchema(entity_schema);
  }
  return casting_internal::CastByExtracting(slice_with_schema,
                                            entity_schema,
                                            allow_removing_attrs,
                                            allow_new_attrs);
}

absl::StatusOr<DataSlice> ToObject(const DataSlice& slice,
                                   bool validate_schema) {
  const auto& db = slice.GetBag();
  internal::DataBagImpl* db_impl_ptr = nullptr;
  // TODO: Consider adding support for immutable bags in the low
  // level ToObject.
  if (db != nullptr && db->IsMutable()) {
    ASSIGN_OR_RETURN(auto& db_impl, db->GetMutableImpl());
    db_impl_ptr = &db_impl;
  }
  ASSIGN_OR_RETURN(auto to_object,
                   schema::ToObject::Make(slice.GetSchemaImpl(),
                                          validate_schema, db_impl_ptr));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    RETURN_IF_ERROR(to_object(impl));
    return DataSlice::Create(impl, slice.GetShape(),
                             internal::DataItem(schema::kObject), db);
  });
}

absl::StatusOr<DataSlice> CastToImplicit(const DataSlice& slice,
                                         const internal::DataItem& schema) {
  if (slice.GetSchemaImpl() == schema) {
    return slice;
  }
  if (!schema.is_schema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a schema, got %v", schema));
  }
  ASSIGN_OR_RETURN(auto common_schema,
                   schema::CommonSchema(slice.GetSchemaImpl(), schema),
                   KodaErrorCausedByNoCommonSchemaError(_, slice.GetBag()));
  if (common_schema != schema) {
    // NOTE(b/424000273): This cannot (currently) happen if any of the schemas
    // are entity schemas, as they are then either identical or there is no
    // common schema. The existing error message is therefore enough. If the
    // type promotion rules change for entities, this should be revisited.
    return absl::InvalidArgumentError(
        absl::StrFormat("unsupported implicit cast from %v to %v",
                        slice.GetSchemaImpl(), schema));
  }
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::CastDataTo(impl, schema));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(), schema,
                             slice.GetBag());
  });
}

absl::StatusOr<DataSlice> CastToExplicit(const DataSlice& slice,
                                         const internal::DataItem& schema,
                                         CastToParams params) {
  if (slice.GetSchemaImpl() == schema) {
    return slice;
  }
  if (!schema.is_schema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a schema, got %v", schema));
  }
  return CastTo(slice, schema, params);
}

absl::StatusOr<DataSlice> CastToNarrow(const DataSlice& slice,
                                       const internal::DataItem& schema) {
  if (slice.GetSchemaImpl() == schema) {
    return slice;
  }
  if (!schema.is_schema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a schema, got %v", schema));
  }
  ASSIGN_OR_RETURN(auto common_schema,
                   schema::CommonSchema(GetNarrowedSchema(slice), schema),
                   KodaErrorCausedByNoCommonSchemaError(_, slice.GetBag()));
  if (common_schema != schema) {
    // NOTE(b/424000273): This cannot (currently) happen if any of the schemas
    // are entity schemas, as they are then either identical or there is no
    // common schema. The existing error message is therefore enough. If the
    // type promotion rules change for entities, this should be revisited.
    return absl::InvalidArgumentError(absl::StrFormat(
        "unsupported narrowing cast to %v for the given %v DataSlice", schema,
        slice.GetSchemaImpl()));
  }
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::CastDataTo(impl, schema));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(), schema,
                             slice.GetBag());
  });
}

absl::StatusOr<SchemaAlignedSlices> AlignSchemas(
    std::vector<DataSlice> slices) {
  auto get_common_schema = [&]() -> absl::StatusOr<internal::DataItem> {
    if (slices.empty()) {
      return absl::InvalidArgumentError("expected at least one slice");
    }
    schema::CommonSchemaAggregator schema_agg;
    for (const auto& slice : slices) {
      schema_agg.Add(slice.GetSchemaImpl());
    }
    return std::move(schema_agg).Get();
  };

  auto get_fallback_db = [&slices] {
    std::vector<DataBagPtr> ret(slices.size());
    absl::c_transform(slices, ret.begin(), [](const DataSlice& ds) {
      const auto& db = ds.GetBag();
      return db == nullptr ? db : db->Freeze();
    });
    return ret;
  };
  ASSIGN_OR_RETURN(
      auto common_schema, get_common_schema(),
      KodaErrorCausedByNoCommonSchemaError(
          _, *DataBag::ImmutableEmptyWithFallbacks(get_fallback_db())));
  for (auto& slice : slices) {
    // Since we cast to a common schema, we don't need to validate implicit
    // compatibility or validate schema (during casting to OBJECT) as no
    // embedding occur.
    if (slice.GetSchemaImpl() != common_schema) {
      ASSIGN_OR_RETURN(
          slice, CastTo(slice, common_schema, {.validate_schema = false}));
    }
  }
  return SchemaAlignedSlices{
      .slices = std::move(slices),
      .common_schema = std::move(common_schema),
  };
}

}  // namespace koladata
