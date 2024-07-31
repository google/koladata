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
#include "koladata/casting.h"

#include <cstdint>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "koladata/data_slice.h"
#include "koladata/internal/casting.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/repr_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

using DTypeMask = uint16_t;
static_assert(schema::kNextDTypeId <= sizeof(DTypeMask) * 8);

template <typename... DTypes>
constexpr DTypeMask GetDTypeMask(DTypes... dtypes) {
  return ((DTypeMask{1} << dtypes.type_id()) | ...);
}

absl::Status VerifyCompatibleSchema(const DataSlice& slice,
                                    DTypeMask allowed_dtypes) {
  const auto& schema_item = slice.GetSchemaImpl();
  if (schema_item.holds_value<schema::DType>() &&
      allowed_dtypes & (1 << schema_item.value<schema::DType>().type_id())) {
    return absl::OkStatus();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("unsupported schema: ", schema_item));
}

template <typename ToNumericImpl>
absl::StatusOr<DataSlice> ToNumericLike(const DataSlice& slice,
                                        schema::DType dst_dtype) {
  constexpr DTypeMask kAllowedSchemas = GetDTypeMask(
      schema::kNone, schema::kInt32, schema::kInt64, schema::kFloat32,
      schema::kFloat64, schema::kBool, schema::kObject, schema::kAny);
  RETURN_IF_ERROR(VerifyCompatibleSchema(slice, kAllowedSchemas));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, ToNumericImpl()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(dst_dtype), slice.GetDb());
  });
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
                             internal::DataItem(schema::kNone), slice.GetDb());
  });
}

absl::StatusOr<DataSlice> ToExpr(const DataSlice& slice) {
  constexpr DTypeMask kAllowedSchemas =
      GetDTypeMask(schema::kNone, schema::kExpr, schema::kObject, schema::kAny);
  RETURN_IF_ERROR(VerifyCompatibleSchema(slice, kAllowedSchemas));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToExpr()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kExpr), slice.GetDb());
  });
}

absl::StatusOr<DataSlice> ToText(const DataSlice& slice) {
  constexpr DTypeMask kAllowedSchemas = GetDTypeMask(
      schema::kNone, schema::kText, schema::kBytes, schema::kMask,
      schema::kBool, schema::kInt32, schema::kInt64, schema::kFloat32,
      schema::kFloat64, schema::kObject, schema::kAny);
  RETURN_IF_ERROR(VerifyCompatibleSchema(slice, kAllowedSchemas));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToText()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kText), slice.GetDb());
  });
}

absl::StatusOr<DataSlice> ToBytes(const DataSlice& slice) {
  constexpr DTypeMask kAllowedSchemas = GetDTypeMask(
      schema::kNone, schema::kBytes, schema::kObject, schema::kAny);
  RETURN_IF_ERROR(VerifyCompatibleSchema(slice, kAllowedSchemas));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToBytes()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kBytes), slice.GetDb());
  });
}

absl::StatusOr<DataSlice> Decode(const DataSlice& slice) {
  constexpr DTypeMask kAllowedSchemas =
      GetDTypeMask(schema::kNone, schema::kText, schema::kBytes,
                   schema::kObject, schema::kAny);
  RETURN_IF_ERROR(VerifyCompatibleSchema(slice, kAllowedSchemas));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::Decode()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kText), slice.GetDb());
  });
}

absl::StatusOr<DataSlice> Encode(const DataSlice& slice) {
  constexpr DTypeMask kAllowedSchemas =
      GetDTypeMask(schema::kNone, schema::kText, schema::kBytes,
                   schema::kObject, schema::kAny);
  RETURN_IF_ERROR(VerifyCompatibleSchema(slice, kAllowedSchemas));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::Encode()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kBytes), slice.GetDb());
  });
}

absl::StatusOr<DataSlice> ToMask(const DataSlice& slice) {
  constexpr DTypeMask kAllowedSchemas =
      GetDTypeMask(schema::kNone, schema::kMask, schema::kObject, schema::kAny);
  RETURN_IF_ERROR(VerifyCompatibleSchema(slice, kAllowedSchemas));
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToMask()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kMask), slice.GetDb());
  });
}

absl::StatusOr<DataSlice> ToBool(const DataSlice& slice) {
  return ToNumericLike<schema::ToBool>(slice, schema::kBool);
}

absl::StatusOr<DataSlice> ToAny(const DataSlice& slice) {
  return slice.VisitImpl([&](const auto& impl) {
    return DataSlice::Create(std::move(impl), slice.GetShape(),
                             internal::DataItem(schema::kAny), slice.GetDb());
  });
}

absl::StatusOr<DataSlice> ToItemId(const DataSlice& slice) {
  if (slice.GetSchemaImpl().holds_value<schema::DType>()) {
    constexpr DTypeMask kAllowedSchemas = GetDTypeMask(
        schema::kNone, schema::kItemId, schema::kObject, schema::kAny);
    RETURN_IF_ERROR(VerifyCompatibleSchema(slice, kAllowedSchemas));
  }
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToItemId()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kItemId),
                             slice.GetDb());
  });
}

absl::StatusOr<DataSlice> ToSchema(const DataSlice& slice) {
  if (slice.GetSchemaImpl().holds_value<schema::DType>()) {
    constexpr DTypeMask kAllowedSchemas = GetDTypeMask(
        schema::kNone, schema::kSchema, schema::kObject, schema::kAny);
    RETURN_IF_ERROR(VerifyCompatibleSchema(slice, kAllowedSchemas));
  }
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToSchema()(impl));
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             internal::DataItem(schema::kSchema),
                             slice.GetDb());
  });
}

absl::StatusOr<DataSlice> ToEntity(const DataSlice& slice,
                                   const internal::DataItem& entity_schema) {
  if (!entity_schema.is_entity_schema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected an entity schema, got: %v", entity_schema));
  }
  if (slice.GetSchemaImpl().holds_value<schema::DType>()) {
    constexpr DTypeMask kAllowedSchemas = GetDTypeMask(
        schema::kNone, schema::kObject, schema::kItemId, schema::kAny);
    RETURN_IF_ERROR(VerifyCompatibleSchema(slice, kAllowedSchemas));
  }
  return slice.VisitImpl([&](const auto& impl) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto impl_res, schema::ToItemId()(impl),
                     _ << "while casting to entity schema: " << entity_schema);
    return DataSlice::Create(std::move(impl_res), slice.GetShape(),
                             entity_schema, slice.GetDb());
  });
}

absl::StatusOr<DataSlice> ToObject(const DataSlice& slice,
                                   bool validate_schema) {
  const auto& db = slice.GetDb();
  internal::DataBagImpl* db_impl_ptr = nullptr;
  // TODO: Consider adding support for immutable bags in the low
  // level ToObject.
  if (db && db->IsMutable()) {
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

absl::StatusOr<DataSlice> CastTo(const DataSlice& slice,
                                 const internal::DataItem& schema,
                                 bool implicit_cast, bool validate_schema) {
  if (!schema.is_schema()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a schema, got %v", schema));
  }
  if (slice.GetSchemaImpl() == schema) {
    return slice;
  }
  if (implicit_cast) {
    ASSIGN_OR_RETURN(auto common_schema,
                     schema::CommonSchema(slice.GetSchemaImpl(), schema),
                     AssembleErrorMessage(_, {.db = slice.GetDb()}));
    if (common_schema != schema) {
      return absl::InvalidArgumentError(
          absl::StrFormat("unsupported implicit cast from %v to %v",
                          slice.GetSchemaImpl(), schema));
    }
  }
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
    case schema::kText.type_id():
      return ToText(slice);
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
    case schema::kAny.type_id():
      return ToAny(slice);
  }
  ABSL_UNREACHABLE();
}

absl::StatusOr<DataSlice> CastTo(const DataSlice& slice, schema::DType dtype,
                                 bool implicit_cast, bool validate_schema) {
  return CastTo(slice, internal::DataItem(dtype), implicit_cast,
                validate_schema);
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

  ASSIGN_OR_RETURN(auto common_schema, get_common_schema());
  for (auto& slice : slices) {
    // Since we cast to a common schema, we don't need to validate implicit
    // compatibility or validate schema (during casting to OBJECT) as no
    // embedding occur.
    if (slice.GetSchemaImpl() != common_schema) {
      ASSIGN_OR_RETURN(slice, CastTo(slice, common_schema,
                                     /*implicit_cast=*/false,
                                     /*validate_schema=*/false));
    }
  }
  return SchemaAlignedSlices{
      .slices = std::move(slices),
      .common_schema = std::move(common_schema),
  };
}

}  // namespace koladata
