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
#include "koladata/internal/casting.h"

#include <cstdint>
#include <string>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/equal.h"
#include "koladata/internal/schema_utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::schema {
namespace schema_internal {

std::string GetQTypeName(arolla::QTypePtr qtype) {
  // TODO: Consider renaming the OBJECT_ID QType to ITEMID.
  if (qtype == arolla::GetQType<internal::ObjectId>()) {
    return "ITEMID";
  }
  return DType::VerifyQTypeSupported(qtype)
             ? absl::StrCat(DType::FromQType(qtype)->name())
             : absl::StrCat(qtype->name());
}

}  // namespace schema_internal

namespace {

absl::Status AssertDbImpl(internal::DataBagImpl* db_impl) {
  if (!db_impl) {
    return absl::InvalidArgumentError(
        "cannot embed object schema without a mutable DataBag");
  }
  return absl::OkStatus();
}

template <typename ImplT>
absl::StatusOr<ImplT> CastDataToImpl(const ImplT& value,
                                     const internal::DataItem& schema) {
  DCHECK(schema.is_schema());
  if (schema.holds_value<internal::ObjectId>()) {
    return ToItemId()(value);
  }
  switch (schema.value<schema::DType>().type_id()) {
    case schema::kNone.type_id():
      return ToNone()(value);
    case schema::kInt32.type_id():
      return ToInt32()(value);
    case schema::kInt64.type_id():
      return ToInt64()(value);
    case schema::kFloat32.type_id():
      return ToFloat32()(value);
    case schema::kFloat64.type_id():
      return ToFloat64()(value);
    case schema::kBool.type_id():
      return ToBool()(value);
    case schema::kMask.type_id():
      return ToMask()(value);
    case schema::kString.type_id():
      return ToStr()(value);
    case schema::kBytes.type_id():
      return ToBytes()(value);
    case schema::kExpr.type_id():
      return ToExpr()(value);
    case schema::kItemId.type_id():
      return ToItemId()(value);
    case schema::kSchema.type_id():
      return ToSchema()(value);
    case schema::kObject.type_id(): {
      ASSIGN_OR_RETURN(auto to_object,
                       ToObject::Make(/*validate_schema=*/false));
      RETURN_IF_ERROR(to_object(value));
      return value;
    }
  }
  ABSL_UNREACHABLE();
}

}  // namespace

absl::StatusOr<internal::DataItem> ToNone::operator()(
    const internal::DataItem& item) const {
  if (item.has_value()) {
    return absl::InvalidArgumentError(
        "only missing values can be converted to NONE");
  }
  return item;
}

absl::StatusOr<internal::DataSliceImpl> ToNone::operator()(
    const internal::DataSliceImpl& slice) const {
  if (slice.present_count() > 0) {
    return absl::InvalidArgumentError(
        "only empty slices can be converted to NONE");
  }
  return internal::DataSliceImpl::CreateEmptyAndUnknownType(slice.size());
}

absl::StatusOr<internal::DataItem> ToSchema::operator()(
    const internal::DataItem& item) const {
  if (!item.has_value() || item.is_schema()) {
    return item;
  }
  // Unexpected type - special case ObjectId to improve the error message.
  if (item.holds_value<internal::ObjectId>()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("cannot cast %v to %v", item, schema::kSchema));
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "cannot cast %s to %v", schema_internal::GetQTypeName(item.dtype()),
      schema::kSchema));
}

absl::StatusOr<internal::DataSliceImpl> ToSchema::operator()(
    const internal::DataSliceImpl& slice) const {
  if (slice.is_empty_and_unknown() ||
      slice.dtype() == arolla::GetQType<schema::DType>()) {
    return slice;
  }
  // Validate that all values are schemas.
  RETURN_IF_ERROR(
      slice.VisitValues([&]<class T>(const arolla::DenseArray<T>& values) {
        if constexpr (std::is_same_v<T, schema::DType>) {
          return absl::OkStatus();
        } else if constexpr (std::is_same_v<T, internal::ObjectId>) {
          absl::Status status = absl::OkStatus();
          values.ForEachPresent([&](int64_t id, internal::ObjectId v) {
            if (!v.IsSchema()) {
              status = absl::InvalidArgumentError(
                  absl::StrFormat("cannot cast %v to %v", v, schema::kSchema));
            }
          });
          return status;
        } else {
          return absl::InvalidArgumentError(absl::StrFormat(
              "cannot cast %s to %v",
              schema_internal::GetQTypeName(arolla::GetQType<T>()),
              schema::kSchema));
        }
      }));
  return slice;
}

absl::StatusOr<ToObject> ToObject::Make(
    internal::DataItem schema, bool validate_schema,
    absl::Nullable<internal::DataBagImpl*> db_impl) {
  if (schema.is_struct_schema()) {
    if (schema.value<internal::ObjectId>().IsNoFollowSchema()) {
      return absl::InvalidArgumentError("schema must not be a NoFollow schema");
    }
  } else if (schema.is_schema()) {
    schema = internal::DataItem();
  } else if (schema.has_value()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected a schema, got %v", schema));
  }
  return ToObject(schema, validate_schema, std::move(db_impl));
}

absl::StatusOr<ToObject> ToObject::Make(
    bool validate_schema, absl::Nullable<internal::DataBagImpl*> db_impl) {
  return ToObject::Make(internal::DataItem(), validate_schema,
                        std::move(db_impl));
}

absl::Status ToObject::operator()(const internal::DataItem& item) const {
  if (!item.has_value()) {
    return absl::OkStatus();
  }
  if (entity_schema_.has_value()) {
    RETURN_IF_ERROR(AssertDbImpl(db_impl_));
    bool set_attr = !validate_schema_;
    if (validate_schema_) {
      ASSIGN_OR_RETURN(auto schema_attr,
                       db_impl_->GetAttr(item, schema::kSchemaAttr));
      if (schema_attr.has_value() && schema_attr != entity_schema_) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "existing schema %v differs from the provided schema %v",
            schema_attr, entity_schema_));
      }
      set_attr = !schema_attr.has_value();
    }
    if (set_attr) {
      // Also validates that item is not a primitive if !validate_schema_.
      RETURN_IF_ERROR(
          db_impl_->SetAttr(item, schema::kSchemaAttr, entity_schema_));
    }
  } else if (validate_schema_ && item.holds_value<internal::ObjectId>()) {
    RETURN_IF_ERROR(AssertDbImpl(db_impl_));
    ASSIGN_OR_RETURN(auto schema_attr,
                     db_impl_->GetAttr(item, schema::kSchemaAttr));
    if (!schema_attr.has_value()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("missing schema for %v", item));
    }
  }
  return absl::OkStatus();
}

absl::Status ToObject::operator()(const internal::DataSliceImpl& slice) const {
  if (slice.present_count() == 0) {
    return absl::OkStatus();
  }
  if (entity_schema_.has_value()) {
    auto val_schema_slice =
        internal::DataSliceImpl::Create(slice.size(), entity_schema_);
    RETURN_IF_ERROR(AssertDbImpl(db_impl_));
    bool set_attr = !validate_schema_;
    if (validate_schema_) {
      ASSIGN_OR_RETURN(auto schema_attr,
                       db_impl_->GetAttr(slice, schema::kSchemaAttr));
      ASSIGN_OR_RETURN(auto equal,
                       internal::EqualOp()(schema_attr, val_schema_slice));
      if (equal.present_count() != schema_attr.present_count()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "existing schemas %v differ from the provided schema %v",
            schema_attr, entity_schema_));
      }
      set_attr = schema_attr.present_count() < slice.present_count();
    }
    if (set_attr) {
      // Also validates that there are no primitive values if !validate_schema_.
      RETURN_IF_ERROR(
          db_impl_->SetAttr(slice, schema::kSchemaAttr, val_schema_slice));
    }
  } else if (validate_schema_ /*&& !schema_.has_value()*/) {
    RETURN_IF_ERROR(slice.VisitValues(
        [&]<class T>(const arolla::DenseArray<T>& values) -> absl::Status {
          if constexpr (!std::is_same_v<T, internal::ObjectId>) {
            return absl::OkStatus();
          }
          RETURN_IF_ERROR(AssertDbImpl(db_impl_));
          ASSIGN_OR_RETURN(
              auto schema_attr,
              // Note: create a new slice to avoid GetAttr on primitives.
              db_impl_->GetAttr(internal::DataSliceImpl::Create(values),
                                schema::kSchemaAttr));
          if (values.PresentCount() != schema_attr.present_count()) {
            return absl::InvalidArgumentError(
                "missing schema for some objects");
          }
          return absl::OkStatus();
        }));
  }
  return absl::OkStatus();
}

absl::StatusOr<internal::DataItem> CastDataTo(
    const internal::DataItem& value, const internal::DataItem& schema) {
  return CastDataToImpl(value, schema);
}

absl::StatusOr<internal::DataSliceImpl> CastDataTo(
    const internal::DataSliceImpl& value, const internal::DataItem& schema) {
  return CastDataToImpl(value, schema);
}

}  // namespace koladata::schema
