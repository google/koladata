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
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/error.pb.h"
#include "koladata/internal/error_utils.h"
#include "koladata/internal/errors.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/s11n/codec.pb.h"
#include "arolla/util/status.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

using DataItemProto = ::koladata::s11n::KodaV1Proto::DataItemProto;
using ::koladata::internal::DecodeDataItem;
using ::koladata::internal::Error;
using ::koladata::internal::GetErrorPayload;

// Returns the attr at `attr_name` for the provided `item` and `db` pair.
absl::StatusOr<internal::DataItem> GetAttr(const internal::DataItem& item,
                                           const DataBagPtr& db,
                                           absl::string_view attr_name) {
  if (db == nullptr) {
    return absl::InvalidArgumentError("missing DataBag");
  }
  FlattenFallbackFinder fb_finder(*db);
  return db->GetImpl().GetAttr(item, attr_name,
                               fb_finder.GetFlattenFallbacks());
}

absl::StatusOr<std::string> SetNoCommonSchemaError(
    const internal::NoCommonSchemaError& error,
    absl::Nullable<const DataBagPtr>& db) {
  if (db != nullptr) {
    ASSIGN_OR_RETURN(
        DataSlice common_schema,
        DataSlice::Create(error.common_schema,
                          internal::DataItem(schema::kSchema), db));
    ASSIGN_OR_RETURN(std::string common_schema_str,
                    DataSliceToStr(common_schema));
    std::string common_schema_id_repr;
    if (error.common_schema.holds_value<internal::ObjectId>()) {
      common_schema_id_repr =
          ObjectIdStr(error.common_schema.value<internal::ObjectId>(),
                      /*show_flag_prefix=*/false);
    } else {
      common_schema_id_repr = error.common_schema.DebugString();
    }
    ASSIGN_OR_RETURN(
        DataSlice conflict_schema,
        DataSlice::Create(error.conflicting_schema,
                          internal::DataItem(schema::kSchema), db));
    ASSIGN_OR_RETURN(std::string conflict_schema_str,
                    DataSliceToStr(conflict_schema));
    std::string conflict_schema_id_repr;
    if (error.conflicting_schema.holds_value<internal::ObjectId>()) {
      conflict_schema_id_repr =
          ObjectIdStr(error.conflicting_schema.value<internal::ObjectId>(),
                      /*show_flag_prefix=*/false);
    } else {
      conflict_schema_id_repr = error.conflicting_schema.DebugString();
    }
    return absl::StrFormat(
        "cannot find a common schema\n\n"
        " the common schema(s) %s: %s\n"
        " the first conflicting schema %s: %s",
        common_schema_id_repr, common_schema_str, conflict_schema_id_repr,
        conflict_schema_str);
  }
  return absl::StrFormat(
      "cannot find a common schema\n\n"
      " the common schema(s) %s\n"
      " the first conflicting schema %s",
      internal::DataItemRepr(error.common_schema),
      internal::DataItemRepr(error.conflicting_schema));
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
                          "schema by passing overwrite_schema=True.",
                          attr_str));
    } else {
      absl::StrAppend(
          &error_str,
          absl::StrFormat(
              "\n\nTo fix this, explicitly override schema of '%s' in the "
              "original schema by passing overwrite_schema=True.",
              attr_str));
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
  ASSIGN_OR_RETURN(auto expected_value,
                   DecodeDataItem(conflict.expected_value()));
  ASSIGN_OR_RETURN(auto assigned_value,
                   DecodeDataItem(conflict.assigned_value()));
  std::string key_str =
      internal::DataItemRepr(key_item, {.strip_quotes = false});
  ASSIGN_OR_RETURN(
      std::string item_str,
      DataItemToStr(object_item, /*schema=*/internal::DataItem(), db1));
  ASSIGN_OR_RETURN(
      std::string conflicting_item_str,
      DataItemToStr(object_item, /*schema=*/internal::DataItem(), db2));
  ASSIGN_OR_RETURN(
      std::string expected_value_str,
      DataItemToStr(expected_value, /*schema=*/internal::DataItem(), db1));
  ASSIGN_OR_RETURN(
      std::string assigned_value_str,
      DataItemToStr(assigned_value, /*schema=*/internal::DataItem(), db2));
  return object_item.is_schema()
             ? absl::StrFormat(kDataBagMergeErrorSchemaConflict, item_str,
                               conflicting_item_str, key_str,
                               expected_value_str, assigned_value_str)
             : absl::StrFormat(kDataBagMergeErrorDictConflict, item_str,
                               conflicting_item_str, key_str,
                               expected_value_str, assigned_value_str);
}

constexpr const char* kDataBagMergeErrorTripleConflict =
    "cannot merge DataBags due to an exception encountered when merging "
    "entities.\n\n"
    "The conflicting entities in the both DataBags: %s\n\n"
    "The cause is the values of attribute '%s' are different: %s vs %s\n";

absl::StatusOr<std::string> SetEntityOrObjectErrorMessage(
    const internal::DataBagMergeConflict::EntityObjectConflict& conflict,
    const DataBagPtr& db1, const DataBagPtr& db2) {
  ASSIGN_OR_RETURN(internal::DataItem object_item,
                   DecodeDataItem(conflict.object_id()));
  ASSIGN_OR_RETURN(internal::DataItem a,
                   GetAttr(object_item, db1, conflict.attr_name()));
  ASSIGN_OR_RETURN(internal::DataItem b,
                   GetAttr(object_item, db2, conflict.attr_name()));
  ASSIGN_OR_RETURN(
      std::string item_str,
      DataItemToStr(object_item, /*schema=*/internal::DataItem(), db1));
  ASSIGN_OR_RETURN(std::string a_str,
                   DataItemToStr(a, /*schema=*/internal::DataItem(), db1));
  ASSIGN_OR_RETURN(std::string b_str,
                   DataItemToStr(b, /*schema=*/internal::DataItem(), db2));
  return absl::StrFormat(kDataBagMergeErrorTripleConflict, item_str,
                         conflict.attr_name(), a_str, b_str);
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
  ASSIGN_OR_RETURN(
      std::string list_str,
      DataItemToStr(list_item, /*schema=*/internal::DataItem(), db1));
  ASSIGN_OR_RETURN(
      std::string conflicting_list_str,
      DataItemToStr(list_item, /*schema=*/internal::DataItem(), db2));
  if (conflict.has_list_item_conflict_index()) {
    ASSIGN_OR_RETURN(auto first_conflicting_item,
                     DecodeDataItem(conflict.first_conflicting_item()));
    ASSIGN_OR_RETURN(auto second_conflicting_item,
                     DecodeDataItem(conflict.second_conflicting_item()));
    ASSIGN_OR_RETURN(std::string first_conflicting_item_str,
                     DataItemToStr(first_conflicting_item,
                                   /*schema=*/internal::DataItem(), db1));
    ASSIGN_OR_RETURN(std::string second_conflicting_item_str,
                     DataItemToStr(second_conflicting_item,
                                   /*schema=*/internal::DataItem(), db2));
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
  if (cause.data_bag_merge_conflict().has_entity_object_conflict()) {
    ASSIGN_OR_RETURN(
        error_str, SetEntityOrObjectErrorMessage(
                       cause.data_bag_merge_conflict().entity_object_conflict(),
                       db1, db2));
  }
  cause.set_error_message(std::move(error_str));
  return cause;
}

absl::StatusOr<internal::Error> SetMissingCollectionItemSchemaError(
    internal::Error error, const DataBagPtr& db) {
  const internal::MissingCollectionItemSchemaError& missing_schema_error =
      error.missing_collection_item_schema();
  ASSIGN_OR_RETURN(internal::DataItem schema_item,
                   DecodeDataItem(missing_schema_error.missing_schema_item()));
  ASSIGN_OR_RETURN(
      DataSlice schema,
      DataSlice::Create(schema_item, DataSlice::JaggedShape::Empty(),
                        internal::DataItem(schema::kSchema), db));
  ASSIGN_OR_RETURN(std::string schema_str, DataSliceToStr(schema));

  std::string error_str;
  if (missing_schema_error.has_item_index()) {
    if (missing_schema_error.collection_type() ==
        internal::MissingCollectionItemSchemaError::LIST) {
      error_str = absl::StrCat(
          "list(s) expected, got an OBJECT DataSlice with the first non-list "
          "schema at ds.flatten().S[",
          missing_schema_error.item_index(), "] ", schema_str);
    }
    if (missing_schema_error.collection_type() ==
        internal::MissingCollectionItemSchemaError::DICT) {
      error_str = absl::StrCat(
          "dict(s) expected, got an OBJECT DataSlice with the first non-dict "
          "schema at ds.flatten().S[",
          missing_schema_error.item_index(), "] ", schema_str);
    }
  } else {
    if (missing_schema_error.collection_type() ==
        internal::MissingCollectionItemSchemaError::LIST) {
      error_str = absl::StrCat("list(s) expected, got ", schema_str);
    }
    if (missing_schema_error.collection_type() ==
        internal::MissingCollectionItemSchemaError::DICT) {
      error_str = absl::StrCat("dict(s) expected, got ", schema_str);
    }
  }
  error.set_error_message(std::move(error_str));
  return error;
}

}  // namespace

absl::Status AssembleErrorMessage(const absl::Status& status,
                                  const SupplementalData& data) {
  if (const internal::NoCommonSchemaError* no_common_schema_error =
          arolla::GetPayload<internal::NoCommonSchemaError>(status);
      no_common_schema_error != nullptr) {
    ASSIGN_OR_RETURN(std::string error_message,
                     SetNoCommonSchemaError(*no_common_schema_error, data.db));
    Error error;
    error.set_error_message(std::move(error_message));
    return WithErrorPayload(status, std::move(error));
  }

  // TODO(b/316118021) migrate away from proto based errors.
  std::optional<Error> cause = GetErrorPayload(status);
  if (!cause) {
    return status;
  }
  if (cause->has_missing_object_schema()) {
    ASSIGN_OR_RETURN(Error error, SetMissingObjectAttributeError(
                                      *std::move(cause), data.ds));
    return WithErrorPayload(status, std::move(error));
  }
  if (cause->has_incompatible_schema()) {
    ASSIGN_OR_RETURN(Error error, SetIncompatibleSchemaError(*std::move(cause),
                                                             data.db, data.ds));
    return WithErrorPayload(status, std::move(error));
  }
  if (cause->has_data_bag_merge_conflict()) {
    ASSIGN_OR_RETURN(
        Error error,
        SetDataBagMergeError(*std::move(cause), data.db, data.to_be_merged_db));
    return WithErrorPayload(status, std::move(error));
  }
  if (cause->has_missing_collection_item_schema()) {
    ASSIGN_OR_RETURN(Error error, SetMissingCollectionItemSchemaError(
                                      *std::move(cause), data.db));
    return WithErrorPayload(status, std::move(error));
  }
  return status;
}

absl::Status CreateItemCreationError(const absl::Status& status,
                                     const std::optional<DataSlice>& schema) {
  std::string error_message;
  if (schema) {
    RETURN_IF_ERROR(schema->VerifyIsSchema());
    std::string schema_str;
    if (schema->GetBag() != nullptr) {
      ASSIGN_OR_RETURN(schema_str, DataSliceToStr(*schema));
    } else {
      schema_str = schema->item().DebugString();
    }
    error_message = absl::StrFormat(
        "cannot create Item(s) with the provided schema: %s", schema_str);
  } else {
    error_message = absl::StrFormat("cannot create Item(s)");
  }
  return internal::KodaErrorFromCause(std::move(error_message), status);
}

}  // namespace koladata
