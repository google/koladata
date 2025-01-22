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
#include "koladata/internal/object_id.h"
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

  if (db != nullptr) {
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

  if (ds->is_item()) {
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

  // If conflicting schemas have the same repr, add ItemId to the repr to better
  // distinguish them.
  if (assigned_schema_item.holds_value<internal::ObjectId>() &&
      expected_schema_item.holds_value<internal::ObjectId>() &&
      assigned_schema_str == expected_schema_str) {
    absl::StrAppend(&assigned_schema_str, " with ItemId ",
                    DataItemRepr(assigned_schema_item));
    absl::StrAppend(&expected_schema_str, " with ItemId ",
                    DataItemRepr(expected_schema_item));
  }

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
    if (ds && ds->GetSchemaImpl() == schema::kObject) {
      absl::StrAppend(
          &error_str,
          absl::StrFormat("\n\nTo fix this, explicitly override schema of '%s' "
                          "in the Object "
                          "schema. For example,\n"
                          "foo.get_obj_schema().%s = <desired_schema>",
                          attr_str, attr_str));
    } else {
      absl::StrAppend(
          &error_str,
          absl::StrFormat(
              "\n\nTo fix this, explicitly override schema of '%s' in the "
              "original schema. For example,\n"
              "schema.%s = <desired_schema>",
              attr_str, attr_str));
    }
    cause.set_error_message(std::move(error_str));
  }
  return cause;
}

constexpr const char* kDataBagMergeErrorSchemaConflict =
    "cannot merge DataBags due to an exception encountered when merging "
    "schemas.\n\n"
    "The conflicting schema in the first DataBag: %s\n"
    "The conflicting schema in the second DataBag: %s\n\n"
    "The cause is the schema for attribute %s is incompatible: %s vs %s\n";

constexpr const char* kDataBagMergeErrorDictConflict =
    "cannot merge DataBags due to an exception encountered when merging "
    "dicts.\n\n"
    "The conflicting dict in the first DataBag: %s\n"
    "The conflicting dict in the second DataBag: %s\n\n"
    "The cause is the value of the key %s is incompatible: %s vs %s\n";

absl::StatusOr<std::string> SetSchemaOrDictErrorMessage(
    const internal::DataBagMergeConflict::SchemaOrDictConflict& conflict,
    const DataBagPtr& db1, const DataBagPtr& db2) {
  ASSIGN_OR_RETURN(internal::DataItem object_item,
                   DecodeDataItem(conflict.object_id()));
  ASSIGN_OR_RETURN(internal::DataItem key_item, DecodeDataItem(conflict.key()));
  internal::DataItem schema = internal::DataItem(
      object_item.is_schema() ? schema::kSchema : schema::kAny);
  ASSIGN_OR_RETURN(
      DataSlice expected_value,
      DataSlice::Create(DecodeDataItem(conflict.expected_value()),
                        DataSlice::JaggedShape::Empty(), schema, db1));
  ASSIGN_OR_RETURN(
      DataSlice assigned_value,
      DataSlice::Create(DecodeDataItem(conflict.assigned_value()),
                        DataSlice::JaggedShape::Empty(), schema, db2));
  ASSIGN_OR_RETURN(
      DataSlice item,
      DataSlice::Create(object_item, schema, db1));
  ASSIGN_OR_RETURN(
      DataSlice conflicting_item,
      DataSlice::Create(object_item, std::move(schema), db2));

  std::string key_str =
      internal::DataItemRepr(key_item, {.strip_quotes = false});
  ASSIGN_OR_RETURN(std::string item_str, DataSliceToStr(item));
  ASSIGN_OR_RETURN(std::string conflicting_item_str,
                   DataSliceToStr(conflicting_item));
  ASSIGN_OR_RETURN(std::string expected_value_str,
                   DataSliceToStr(expected_value));
  ASSIGN_OR_RETURN(std::string assigned_value_str,
                   DataSliceToStr(assigned_value));
  return object_item.is_schema()
             ? absl::StrFormat(kDataBagMergeErrorSchemaConflict, item_str,
                               conflicting_item_str, key_str,
                               expected_value_str, assigned_value_str)
             : absl::StrFormat(kDataBagMergeErrorDictConflict, item_str,
                               conflicting_item_str, key_str,
                               expected_value_str, assigned_value_str);
}

constexpr const char* kDataBagMergeErrorListSizeConflict =
    "cannot merge DataBags due to an exception encountered when merging "
    "lists.\n\n"
    "The conflicting list in the first DataBag: %s\n"
    "The conflicting list in the second DataBag: %s\n\n"
    "The cause is the list sizes are incompatible: %d vs %d\n";

constexpr const char* kDataBagMergeErrorListItemConflict =
    "cannot merge DataBags due to an exception encountered when merging "
    "lists.\n\n"
    "The conflicting list in the first DataBag: %s\n"
    "The conflicting list in the second DataBag: %s\n\n"
    "The cause is the value at index %d is incompatible: %s vs %s\n";

absl::StatusOr<std::string> SetListErrorMessage(
    const internal::DataBagMergeConflict::ListConflict& conflict,
    const DataBagPtr& db1, const DataBagPtr& db2) {
  ASSIGN_OR_RETURN(internal::DataItem list_item,
                   DecodeDataItem(conflict.list_object_id()));
  ASSIGN_OR_RETURN(DataSlice list,
                   DataSlice::Create(list_item, DataSlice::JaggedShape::Empty(),
                                     internal::DataItem(schema::kAny), db1));
  ASSIGN_OR_RETURN(DataSlice conflicting_list,
                   DataSlice::Create(list_item, DataSlice::JaggedShape::Empty(),
                                     internal::DataItem(schema::kAny), db2));
  ASSIGN_OR_RETURN(std::string list_str, DataSliceToStr(list));
  ASSIGN_OR_RETURN(std::string conflicting_list_str,
                   DataSliceToStr(conflicting_list));
  if (conflict.has_list_item_conflict_index()) {
    ASSIGN_OR_RETURN(
        DataSlice first_conflicting_item,
        DataSlice::Create(DecodeDataItem(conflict.first_conflicting_item()),
                          DataSlice::JaggedShape::Empty(),
                          internal::DataItem(schema::kAny), db1));
    ASSIGN_OR_RETURN(
        DataSlice second_conflicting_item,
        DataSlice::Create(DecodeDataItem(conflict.second_conflicting_item()),
                          DataSlice::JaggedShape::Empty(),
                          internal::DataItem(schema::kAny), db2));
    ASSIGN_OR_RETURN(std::string first_conflicting_item_str,
                     DataSliceToStr(first_conflicting_item));
    ASSIGN_OR_RETURN(std::string second_conflicting_item_str,
                     DataSliceToStr(second_conflicting_item));
    return absl::StrFormat(
        kDataBagMergeErrorListItemConflict, list_str, conflicting_list_str,
        conflict.list_item_conflict_index(), first_conflicting_item_str,
        second_conflicting_item_str);
  }
  return absl::StrFormat(kDataBagMergeErrorListSizeConflict, list_str,
                         conflicting_list_str, conflict.first_list_size(),
                         conflict.second_list_size());
}

absl::StatusOr<Error> SetDataBagMergeError(Error cause, const DataBagPtr& db1,
                                           const DataBagPtr& db2) {
  std::string error_str;
  if (cause.data_bag_merge_conflict().has_schema_or_dict_conflict()) {
    ASSIGN_OR_RETURN(
        error_str,
        SetSchemaOrDictErrorMessage(
            cause.data_bag_merge_conflict().schema_or_dict_conflict(), db1,
            db2));
  }
  if (cause.data_bag_merge_conflict().has_list_conflict()) {
    ASSIGN_OR_RETURN(
        error_str,
        SetListErrorMessage(cause.data_bag_merge_conflict().list_conflict(),
                            db1, db2));
  }
  cause.set_error_message(std::move(error_str));
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
  if (cause->has_data_bag_merge_conflict()) {
    ASSIGN_OR_RETURN(Error error,
                     SetDataBagMergeError(*std::move(cause), data.db,
                                                data.to_be_merged_db));
    return WithErrorPayload(status, error);
  }
  return status;
}

absl::Status CreateItemCreationError(const absl::Status& status,
                                     const std::optional<DataSlice>& schema) {
  internal::Error error;
  if (schema) {
    RETURN_IF_ERROR(schema->VerifyIsSchema());
    std::string schema_str;
    if (schema->GetBag() != nullptr) {
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
