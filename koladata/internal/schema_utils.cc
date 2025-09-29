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
#include "koladata/internal/schema_utils.h"

#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/functional/overload.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/numeric/bits.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/meta.h"
#include "arolla/util/status.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/errors.h"
#include "koladata/internal/missing_value.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::schema {

std::optional<DType> schema_internal::CommonDTypeAggregator::Get(
    absl::Status& status) const {
  if (seen_dtypes_ == 0) {
    return std::nullopt;
  }
  DTypeId res_dtype_id = absl::countr_zero(seen_dtypes_);

  for (auto mask = seen_dtypes_; true;) {
    mask &= (mask - 1);  // remove the least significant bit.
    if (mask == 0) {
      return DType::UnsafeFromId(res_dtype_id);
    }
    DTypeId i = absl::countr_zero(mask);
    DTypeId common_dtype_id = CommonDType(res_dtype_id, i);
    if (ABSL_PREDICT_FALSE(common_dtype_id == kUnknownDType)) {
      status = arolla::WithPayload(
          absl::InvalidArgumentError("no common schema"),
          internal::NoCommonSchemaError{
              .common_schema =
                  internal::DataItem(DType::UnsafeFromId(res_dtype_id)),
              .conflicting_schema =
                  internal::DataItem(DType::UnsafeFromId(i))});
      return std::nullopt;
    }
    res_dtype_id = common_dtype_id;
  }
}

void CommonSchemaAggregator::Add(const internal::DataItem& schema) {
  if (schema.holds_value<DType>()) {
    return Add(schema.value<DType>());
  }
  if (schema.holds_value<internal::ObjectId>()) {
    return Add(schema.value<internal::ObjectId>());
  }
  // Allow missing values to pass through.
  if (schema.has_value()) {
    status_ = absl::InvalidArgumentError(
        absl::StrFormat("expected Schema, got: %v", schema));
  }
}

void CommonSchemaAggregator::Add(internal::ObjectId schema_obj) {
  if (!schema_obj.IsSchema()) {
    status_ = absl::InvalidArgumentError(
        absl::StrFormat("expected a schema ObjectId, got: %v", schema_obj));
    return;
  }
  if (!res_object_id_) {
    res_object_id_ = std::move(schema_obj);
    return;
  }
  if (*res_object_id_ != schema_obj) {
    status_ = arolla::WithPayload(
        absl::InvalidArgumentError("no common schema"),
        internal::NoCommonSchemaError{
            .common_schema = internal::DataItem(*res_object_id_),
            .conflicting_schema = internal::DataItem(schema_obj)});
  }
}

absl::StatusOr<internal::DataItem> CommonSchemaAggregator::Get() && {
  std::optional<DType> res_dtype = dtype_agg_.Get(status_);
  if (!status_.ok()) {
    return std::move(status_);
  }
  if (res_dtype.has_value() && !res_object_id_) {
    return internal::DataItem(*res_dtype);
  }
  // NONE is the only dtype that casts to entity.
  if ((!res_dtype || res_dtype == kNone) && res_object_id_.has_value()) {
    return internal::DataItem(*res_object_id_);
  }
  if (!res_object_id_) {
    DCHECK(!res_dtype);
    return internal::DataItem();
  }
  return arolla::WithPayload(
      absl::InvalidArgumentError("no common schema"),
      internal::NoCommonSchemaError{
          .common_schema = internal::DataItem(*res_dtype),
          .conflicting_schema = internal::DataItem(*res_object_id_)});
}

absl::StatusOr<internal::DataItem> CommonSchema(
    const internal::DataSliceImpl& schema_ids) {
  CommonSchemaAggregator schema_agg;

  RETURN_IF_ERROR(schema_ids.VisitValues(absl::Overload(
      [&](const arolla::DenseArray<DType>& array) {
        array.ForEachPresent(
            [&](int64_t id, DType dtype) { schema_agg.Add(dtype); });
        return absl::OkStatus();
      },
      [&](const arolla::DenseArray<internal::ObjectId>& array) {
        array.ForEachPresent([&](int64_t id, internal::ObjectId schema_obj) {
          schema_agg.Add(schema_obj);
        });
        return absl::OkStatus();
      },
      [&](const auto& array) {
        using ValT = typename std::decay_t<decltype(array)>::base_type;
        return absl::InvalidArgumentError(
            absl::StrCat("expected Schema, got: ", GetDType<ValT>()));
      })));
  return std::move(schema_agg).Get();
}

bool IsImplicitlyCastableTo(const internal::DataItem& from_schema,
                            const internal::DataItem& to_schema) {
  DCHECK(from_schema.is_schema() && to_schema.is_schema());
  if (from_schema.holds_value<DType>() && to_schema.holds_value<DType>()) {
    return CommonDType(from_schema.value<DType>().type_id(),
                       to_schema.value<DType>().type_id()) ==
           to_schema.value<DType>().type_id();
  }
  if (from_schema.holds_value<internal::ObjectId>() &&
         to_schema.holds_value<internal::ObjectId>()) {
    return from_schema.value<internal::ObjectId>() ==
             to_schema.value<internal::ObjectId>();
  }
  return from_schema.holds_value<DType>() &&
         to_schema.holds_value<internal::ObjectId>() &&
         from_schema.value<DType>() == kNone;
}

absl::StatusOr<internal::DataItem> NoFollowSchemaItem(
    const internal::DataItem& schema_item) {
  if (schema_item.holds_value<DType>()) {
    if (schema_item.value<DType>() != kObject) {
      // Raises on primitives and ITEMID.
      return absl::InvalidArgumentError(absl::StrFormat(
          "calling nofollow on %v slice is not allowed", schema_item));
    }
    // NOTE: NoFollow of OBJECT schema has a reserved mask in ObjectId's
    // metadata.
    return internal::DataItem(internal::ObjectId::NoFollowObjectSchemaId());
  }
  if (!schema_item.holds_value<internal::ObjectId>()) {
    return absl::InternalError(
        "schema can be either a DType or ObjectId schema");
  }
  if (!schema_item.value<internal::ObjectId>().IsSchema()) {
    // Raises on non-schemas.
    return absl::InternalError(
        "calling nofollow on a non-schema is not allowed");
  }
  if (schema_item.value<internal::ObjectId>().IsNoFollowSchema()) {
    // Raises on already NoFollow schema.
    return absl::InvalidArgumentError(
        "calling nofollow on a nofollow slice is not allowed");
  }
  return internal::DataItem(internal::CreateNoFollowWithMainObject(
      schema_item.value<internal::ObjectId>()));
}

absl::StatusOr<internal::DataItem> GetNoFollowedSchemaItem(
    const internal::DataItem& nofollow_schema_item) {
  if (nofollow_schema_item.holds_value<DType>() ||
      !nofollow_schema_item.value<internal::ObjectId>().IsNoFollowSchema()) {
    return absl::InvalidArgumentError(
        "a nofollow schema is required in get_nofollowed_schema");
  }
  auto schema_id = nofollow_schema_item.value<internal::ObjectId>();
  if (schema_id == internal::ObjectId::NoFollowObjectSchemaId()) {
    return internal::DataItem(kObject);
  }
  return internal::DataItem(internal::GetOriginalFromNoFollow(schema_id));
}

bool VerifySchemaForItemIds(const internal::DataItem& schema_item) {
  if (!schema_item.holds_value<DType>()) {
    return false;
  }
  const DType& dtype = schema_item.value<DType>();
  return dtype == kItemId || dtype == kObject;
}

absl::Status VerifyDictKeySchema(const internal::DataItem& schema_item) {
  if (schema_item == kFloat32 || schema_item == kFloat64 ||
      schema_item == kExpr) {
    return absl::InvalidArgumentError(
        absl::StrFormat("dict keys cannot be %v", schema_item));
  }
  return absl::OkStatus();
}

internal::DataItem GetDataSchema(
    const internal::DataItem& item,
    const internal::DataBagImpl* absl_nullable db_impl,
    internal::DataBagImpl::FallbackSpan fallbacks) {
  return item.VisitValue([&]<class T>(const T& value) {
    if constexpr (arolla::meta::contains_v<supported_primitive_dtypes, T>) {
      return internal::DataItem(GetDType<T>());
    } else if constexpr (std::is_same_v<T, DType>) {
      return internal::DataItem(kSchema);
    } else if constexpr (std::is_same_v<T, internal::MissingValue>) {
      return internal::DataItem(kNone);
    } else {
      static_assert(std::is_same_v<T, internal::ObjectId>);
      if (db_impl == nullptr) {
        return internal::DataItem();
      }
      auto schema_attr = db_impl->GetAttr(item, schema::kSchemaAttr, fallbacks);
      return schema_attr.ok() ? *std::move(schema_attr) : internal::DataItem();
    }
  });
}

internal::DataItem GetDataSchema(
    const internal::DataSliceImpl& slice,
    const internal::DataBagImpl* absl_nullable db_impl,
    internal::DataBagImpl::FallbackSpan fallbacks) {
  CommonSchemaAggregator schema_agg;
  schema_agg.Add(kNone);  // All missing -> NONE.
  bool is_ambiguous = false;
  slice.VisitValues([&]<class T>(const arolla::DenseArray<T>& values) {
    if constexpr (arolla::meta::contains_v<supported_primitive_dtypes, T>) {
      schema_agg.Add(GetDType<T>());
    } else if constexpr (std::is_same_v<T, DType>) {
      schema_agg.Add(kSchema);
    } else {
      static_assert(std::is_same_v<T, internal::ObjectId>);
      if (db_impl == nullptr) {
        is_ambiguous = true;
        return;
      }
      auto schema_attrs =
          db_impl->GetAttr(slice, schema::kSchemaAttr, fallbacks);
      if (!schema_attrs.ok() ||
          slice.present_count() != schema_attrs->present_count()) {
        is_ambiguous = true;
        return;
      }
      // NOTE(b/413664265): this may construct an error that is subsequently
      // discarded. Consider changing this behavior to check if it has a common
      // schema instead with a bool.
      if (auto common_schema = CommonSchema(*schema_attrs);
          common_schema.ok()) {
        schema_agg.Add(*common_schema);
      } else {
        is_ambiguous = true;
      }
    }
  });
  if (is_ambiguous) {
    return internal::DataItem();
  }
  auto result_schema = std::move(schema_agg).Get();
  if (result_schema.ok()) {
    return *std::move(result_schema);
  } else {
    return internal::DataItem();
  }
}

}  // namespace koladata::schema
