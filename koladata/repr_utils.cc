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
#include "koladata/repr_utils.h"

#include <optional>
#include <string>
#include <utility>

#include "absl/base/nullability.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/s11n/codec.pb.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

using DataItemProto = ::koladata::s11n::KodaV1Proto::DataItemProto;
using ::koladata::internal::DecodeDataItem;
using ::koladata::internal::Error;
using ::koladata::internal::GetErrorPayload;

absl::StatusOr<Error> SetNoCommonSchemaError(
    Error cause, absl::Nullable<const DataBagPtr>& db) {
  ASSIGN_OR_RETURN(internal::DataItem common_schema_item,
                   DecodeDataItem(cause.no_common_schema().common_schema()));
  ASSIGN_OR_RETURN(
      internal::DataItem conflict_schema_item,
      DecodeDataItem(cause.no_common_schema().conflicting_schema()));

  if (db) {
    ASSIGN_OR_RETURN(DataSlice common_schema,
                    DataSlice::Create(common_schema_item,
                                      internal::DataItem(schema::kSchema), db));
    ASSIGN_OR_RETURN(std::string common_schema_str,
                    DataSliceToStr(common_schema));
    ASSIGN_OR_RETURN(DataSlice conflict_schema,
                    DataSlice::Create(conflict_schema_item,
                                      internal::DataItem(schema::kSchema), db));
    ASSIGN_OR_RETURN(std::string conflict_schema_str,
                    DataSliceToStr(conflict_schema));
    cause.set_error_message(absl::StrFormat(
        "\ncannot find a common schema for provided schemas\n\n"
        " the common schema(s) %s: %s\n"
        " the first conflicting schema %s: %s",
        common_schema_item.DebugString(), common_schema_str,
        conflict_schema_item.DebugString(), conflict_schema_str));
  } else {
    cause.set_error_message(
        absl::StrFormat("\ncannot find a common schema for provided schemas\n\n"
                        " the common schema(s) %s\n"
                        " the first conflicting schema %s",
                        internal::DataItemRepr(common_schema_item),
                        internal::DataItemRepr(conflict_schema_item)));
  }

  return cause;
}

absl::StatusOr<Error> SetMissingObjectAttributeError(
    Error cause, std::optional<const DataSlice> ds) {
  if (!ds) {
    return absl::InvalidArgumentError("missing data slice");
  }
  ASSIGN_OR_RETURN(
      internal::DataItem missing_schema_item,
      DecodeDataItem(cause.missing_object_schema().missing_schema_item()));

  std::string item_str = internal::DataItemRepr(missing_schema_item);

  if (ds->GetShape().rank() == 0) {
    cause.set_error_message(absl::StrFormat(
        "object schema is missing for the DataItem whose item is: %s\n\n"
        "  DataItem with the kd.OBJECT schema usually store its schema as an "
        "attribute or implicitly hold the type information when it's a "
        "primitive type. Perhaps, the OBJECT schema is set by mistake with\n"
        "  foo.with_schema(kd.OBJECT) when 'foo' "
        "does not have stored schema attribute.\n",
        item_str));
  } else {
    ASSIGN_OR_RETURN(std::string ds_str, DataSliceToStr(*ds));
    cause.set_error_message(absl::StrFormat(
        "object schema(s) are missing for some Object(s) in the DataSlice "
        "whose items are: %s\n\n "
        "  objects in the kd.OBJECT DataSlice usually store their schemas as "
        "an attribute or implicitly hold the type information when they are "
        "primitive types. Perhaps, the OBJECT schema is set by mistake with\n"
        "  foo.with_schema(kd.OBJECT) when 'foo' "
        "does not have stored schema attributes.\n"
        "The first Object without schema: %s\n",
        ds_str, item_str));
  }
  return cause;
}

constexpr absl::string_view kExplicitSchemaIncompatibleAttrError =
    "the schema for attribute '%s' is incompatible.\n\n"
    "Expected schema for '%s': %v\n"
    "Assigned schema for '%s': %v";

constexpr const char* kExplicitSchemaIncompatibleListItemError =
    "the schema for List item is incompatible.\n\n"
    "Expected schema for List item: %v\n"
    "Assigned schema for List item: %v";
constexpr const char* kExplicitSchemaIncompatibleDictError =
    "the schema for Dict %s is incompatible.\n\n"
    "Expected schema for Dict %s: %v\n"
    "Assigned schema for Dict %s: %v";

absl::StatusOr<Error> SetIncompatibleSchemaError(
    Error cause, absl::Nullable<const DataBagPtr>& db,
    std::optional<const DataSlice> ds) {
  ASSIGN_OR_RETURN(
      internal::DataItem assigned_schema_item,
      DecodeDataItem(cause.incompatible_schema().assigned_schema()));
  ASSIGN_OR_RETURN(
      internal::DataItem expected_schema_item,
      DecodeDataItem(cause.incompatible_schema().expected_schema()));

  ASSIGN_OR_RETURN(DataSlice assigned_schema,
                   DataSlice::Create(assigned_schema_item,
                                     internal::DataItem(schema::kSchema), db));
  ASSIGN_OR_RETURN(std::string assigned_schema_str,
                   DataSliceToStr(assigned_schema));
  ASSIGN_OR_RETURN(DataSlice expected_schema,
                   DataSlice::Create(expected_schema_item,
                                     internal::DataItem(schema::kSchema), db));
  ASSIGN_OR_RETURN(std::string expected_schema_str,
                   DataSliceToStr(expected_schema));

  std::string attr_str = cause.incompatible_schema().attr();
  if (attr_str == schema::kListItemsSchemaAttr) {
    cause.set_error_message(
        absl::StrFormat(kExplicitSchemaIncompatibleListItemError,
                        expected_schema_str, assigned_schema_str));
  } else if (attr_str == schema::kDictKeysSchemaAttr) {
    cause.set_error_message(
        absl::StrFormat(kExplicitSchemaIncompatibleDictError, "key", "key",
                        expected_schema_str, "key", assigned_schema_str));
  } else if (attr_str == schema::kDictValuesSchemaAttr) {
    cause.set_error_message(
        absl::StrFormat(kExplicitSchemaIncompatibleDictError, "value", "value",
                        expected_schema_str, "value", assigned_schema_str));
  } else {
    std::string error_str = absl::StrFormat(
        kExplicitSchemaIncompatibleAttrError, attr_str, attr_str,
        expected_schema_str, attr_str, assigned_schema_str);
    if (ds && ds->GetSchemaImpl() == internal::DataItem(schema::kObject)) {
      absl::StrAppend(
          &error_str,
          absl::StrFormat(
              "\n\nTo fix this, explicitly override schema of %s in the Object "
              "schema. For example,\n"
              "foo.get_obj_schema().%s = <desired_schema>",
              attr_str, attr_str));
    } else {
      absl::StrAppend(
          &error_str,
          absl::StrFormat(
              "\n\nTo fix this, explicitly override schema of %s in the "
              "original schema. For example,\n"
              "schema.%s = <desired_schema>",
              attr_str, attr_str));
    }
    cause.set_error_message(std::move(error_str));
  }
  return cause;
}

}  // namespace

absl::Status AssembleErrorMessage(const absl::Status& status,
                                  const SupplementalData& data) {
  std::optional<Error> cause = GetErrorPayload(status);
  if (!cause) {
    return status;
  }
  if (cause->has_no_common_schema()) {
    ASSIGN_OR_RETURN(Error error,
                     SetNoCommonSchemaError(*std::move(cause), data.db));
    return WithErrorPayload(status, error);
  }
  if (cause->has_missing_object_schema()) {
    ASSIGN_OR_RETURN(Error error, SetMissingObjectAttributeError(
                                      *std::move(cause), data.ds));
    return WithErrorPayload(status, error);
  }
  if (cause->has_incompatible_schema()) {
    ASSIGN_OR_RETURN(Error error, SetIncompatibleSchemaError(*std::move(cause),
                                                             data.db, data.ds));
    return WithErrorPayload(status, error);
  }
  return status;
}

absl::Status CreateItemCreationError(const absl::Status& status,
                                     const std::optional<DataSlice>& schema) {
  internal::Error error;
  if (schema) {
    std::string schema_str;
    if (schema->GetDb() != nullptr) {
      ASSIGN_OR_RETURN(schema_str, DataSliceToStr(*schema));
    } else {
      schema_str = schema->item().DebugString();
    }
    error.set_error_message(absl::StrFormat(
        "cannot create Item(s) with the provided schema: %s", schema_str));
  } else {
    error.set_error_message(absl::StrFormat("cannot create Item(s)"));
  }
  std::optional<internal::Error> cause = internal::GetErrorPayload(status);
  if (cause) {
    *error.mutable_cause() = *std::move(cause);
  } else {
    error.mutable_cause()->set_error_message(status.message());
  }
  return internal::WithErrorPayload(status, error);
}

}  // namespace koladata
