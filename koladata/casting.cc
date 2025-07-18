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
#include <string>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/error_repr_utils.h"
#include "koladata/internal/casting.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
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

// Returns a string representation of the provided `schema`. Uses `db` for
// supplementary information in case of a struct `schema`. Unlike `SchemaToStr`,
// this includes information about the ObjectId to help discern between similar
// looking reprs.
absl::StatusOr<std::string> SchemaImplToStr(const internal::DataItem& schema,
                                            absl_nullable DataBagPtr db) {
  if (!schema.is_struct_schema() || db == nullptr) {
    return absl::StrCat(schema);
  }
  ASSIGN_OR_RETURN(
      auto from_schema_slice,
      DataSlice::Create(schema, internal::DataItem(schema::kSchema),
                        std::move(db)));
  return absl::StrFormat("%s with id %v", SchemaToStr(from_schema_slice),
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

template <typename ToNumericImpl>
absl::StatusOr<DataSlice> ToNumericLike(const DataSlice& slice,
                                        schema::DType dst_dtype) {
  constexpr DTypeMask kAllowedSchemas =
      GetDTypeMask(schema::kNone, schema::kInt32, schema::kInt64,
                   schema::kFloat32, schema::kFloat64, schema::kBool,
                   schema::kString, schema::kBytes, schema::kObject);
  RETURN_IF_ERROR(VerifyCompatibleSchema(slice, kAllowedSchemas, dst_dtype));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, ToNumericImpl()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(dst_dtype), slice.GetBag());
  });
}

absl::StatusOr<DataSlice> CastTo(const DataSlice& slice,
                                 const internal::DataItem& schema,
                                 bool validate_schema) {
  DCHECK(schema.is_schema());
  if (schema.holds_value<internal::ObjectId>()) {
    return ToEntity(slice, schema);
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
      return ToObject(slice, validate_schema);
  }
  ABSL_UNREACHABLE();
}

}  // namespace

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
  constexpr DTypeMask kAllowedSchemas =
      GetDTypeMask(schema::kNone, schema::kExpr, schema::kObject);
  RETURN_IF_ERROR(
      VerifyCompatibleSchema(slice, kAllowedSchemas, schema::kExpr));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToExpr()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kExpr), slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToStr(const DataSlice& slice) {
  constexpr DTypeMask kAllowedSchemas =
      GetDTypeMask(schema::kNone, schema::kString, schema::kBytes,
                   schema::kMask, schema::kBool, schema::kInt32, schema::kInt64,
                   schema::kFloat32, schema::kFloat64, schema::kObject);
  RETURN_IF_ERROR(
      VerifyCompatibleSchema(slice, kAllowedSchemas, schema::kString));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToStr()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kString),
                             slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToBytes(const DataSlice& slice) {
  constexpr DTypeMask kAllowedSchemas =
      GetDTypeMask(schema::kNone, schema::kBytes, schema::kObject);
  RETURN_IF_ERROR(
      VerifyCompatibleSchema(slice, kAllowedSchemas, schema::kBytes));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToBytes()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kBytes),
                             slice.GetBag());
  });
}

absl::StatusOr<DataSlice> Decode(const DataSlice& slice) {
  constexpr DTypeMask kAllowedSchemas = GetDTypeMask(
      schema::kNone, schema::kString, schema::kBytes, schema::kObject);
  RETURN_IF_ERROR(
      VerifyCompatibleSchema(slice, kAllowedSchemas, schema::kString));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::Decode()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kString),
                             slice.GetBag());
  });
}

absl::StatusOr<DataSlice> Encode(const DataSlice& slice) {
  constexpr DTypeMask kAllowedSchemas = GetDTypeMask(
      schema::kNone, schema::kString, schema::kBytes, schema::kObject);
  RETURN_IF_ERROR(
      VerifyCompatibleSchema(slice, kAllowedSchemas, schema::kBytes));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::Encode()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kBytes),
                             slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToMask(const DataSlice& slice) {
  constexpr DTypeMask kAllowedSchemas =
      GetDTypeMask(schema::kNone, schema::kMask, schema::kObject);
  RETURN_IF_ERROR(
      VerifyCompatibleSchema(slice, kAllowedSchemas, schema::kMask));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToMask()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kMask), slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToBool(const DataSlice& slice) {
  constexpr DTypeMask kAllowedSchemas = GetDTypeMask(
      schema::kNone, schema::kInt32, schema::kInt64, schema::kFloat32,
      schema::kFloat64, schema::kBool, schema::kObject);
  if (auto status =
          VerifyCompatibleSchema(slice, kAllowedSchemas, schema::kBool);
      !status.ok()) {
    if (slice.GetSchemaImpl() == schema::kMask) {
      RETURN_IF_ERROR(std::move(status))
          << "try `kd.cond(slice, True, False)` instead";
    }
    return status;
  }
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToBool()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kBool), slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToItemId(const DataSlice& slice) {
  if (slice.GetSchemaImpl().holds_value<schema::DType>()) {
    constexpr DTypeMask kAllowedSchemas = GetDTypeMask(
        schema::kNone, schema::kItemId, schema::kObject, schema::kSchema);
    RETURN_IF_ERROR(
        VerifyCompatibleSchema(slice, kAllowedSchemas, schema::kItemId));
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
    constexpr DTypeMask kAllowedSchemas =
        GetDTypeMask(schema::kNone, schema::kSchema, schema::kObject);
    RETURN_IF_ERROR(
        VerifyCompatibleSchema(slice, kAllowedSchemas, schema::kSchema));
  }
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToSchema()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kSchema),
                             slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToEntity(const DataSlice& slice,
                                   const internal::DataItem& entity_schema) {
  if (!entity_schema.is_struct_schema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected an entity schema, got: %v", entity_schema));
  }
  internal::DataItem from_schema = slice.GetSchemaImpl();
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
  }
  // TODO: Support deep casting to entity schema.
  if (!schema::IsImplicitlyCastableTo(from_schema, entity_schema)) {
    ASSIGN_OR_RETURN(std::string from_schema_str,
                     SchemaImplToStr(from_schema, slice.GetBag()));
    if (slice.GetSchemaImpl() == schema::kObject) {
      from_schema_str =
          absl::StrFormat("%v with common __schema__ %s",
                          schema::kObject.name(), from_schema_str);
    }
    // NOTE: We assume that the `entity_schema` is already part of the bag. If
    // it's not, we still end up with an OK error message so this is a best
    // effort that covers most practical scenarios.
    ASSIGN_OR_RETURN(std::string entity_schema_str,
                     SchemaImplToStr(entity_schema, slice.GetBag()));
    return absl::InvalidArgumentError(absl::StrFormat(
        "(deep) casting from %s to entity schema %s is currently not supported "
        "- please cast each attribute separately or use `kd.with_schema` if "
        "you are certain it is safe to do so",
        from_schema_str, entity_schema_str));
  }
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    return DataSlice::Create(impl, slice.GetShape(), entity_schema,
                             slice.GetBag());
  });
}

absl::StatusOr<DataSlice> ToObject(const DataSlice& slice,
                                   bool validate_schema) {
  const auto& db = slice.GetBag();
  internal::DataBagImpl* db_impl_ptr = nullptr;
  // TODO: Consider adding support for immutable bags in the low
  // level ToObject.
  if (db != nullptr && db->IsMutable()) {
    ASSIGN_OR_RETURN(auto db_impl, db->GetMutableImpl());
    db_impl_ptr = &db_impl.get();
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
                                         bool validate_schema) {
  if (slice.GetSchemaImpl() == schema) {
    return slice;
  }
  if (!schema.is_schema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a schema, got %v", schema));
  }
  return CastTo(slice, schema, validate_schema);
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
    std::transform(slices.begin(), slices.end(), ret.begin(),
                   [](const DataSlice& ds) { return ds.GetBag(); });
    return ret;
  };
  ASSIGN_OR_RETURN(
      auto common_schema, get_common_schema(),
      KodaErrorCausedByNoCommonSchemaError(
          _, DataBag::ImmutableEmptyWithFallbacks(get_fallback_db())));
  for (auto& slice : slices) {
    // Since we cast to a common schema, we don't need to validate implicit
    // compatibility or validate schema (during casting to OBJECT) as no
    // embedding occur.
    if (slice.GetSchemaImpl() != common_schema) {
      ASSIGN_OR_RETURN(slice, CastTo(slice, common_schema,
                                     /*validate_schema=*/false));
    }
  }
  return SchemaAlignedSlices{
      .slices = std::move(slices),
      .common_schema = std::move(common_schema),
  };
}

}  // namespace koladata
