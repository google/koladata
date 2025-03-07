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
#include <variant>

#include "absl/base/nullability.h"
#include "absl/functional/overload.h"
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

absl::StatusOr<std::string> FormatMissingObjectAttributeSchemaError(
    const internal::MissingObjectSchemaError& error,
    std::optional<const DataSlice> ds) {
  if (!ds) {
    return absl::InvalidArgumentError("missing data slice");
  }
  std::string item_str = internal::DataItemRepr(error.missing_schema_item);

  if (ds->is_item()) {
    return absl::StrFormat(
        "object schema is missing for the DataItem whose item is: %s\n\n"
        "  DataItem with the kd.OBJECT schema usually store its schema as an "
        "attribute or implicitly hold the type information when it's a "
        "primitive type. Perhaps, the OBJECT schema is set by mistake with\n"
        "  foo.with_schema(kd.OBJECT) when 'foo' "
        "does not have stored schema attribute.\n",
        item_str);
  }
  ASSIGN_OR_RETURN(std::string ds_str, DataSliceToStr(*ds));
  return absl::StrFormat(
      "object schema(s) are missing for some Object(s) in the DataSlice "
      "whose items are: %s\n\n "
      "  objects in the kd.OBJECT DataSlice usually store their schemas as "
      "an attribute or implicitly hold the type information when they are "
      "primitive types. Perhaps, the OBJECT schema is set by mistake with\n"
      "  foo.with_schema(kd.OBJECT) when 'foo' "
      "does not have stored schema attributes.\n"
      "The first Object without schema: %s\n",
      ds_str, item_str);
}

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

  absl::string_view attr_str = cause.incompatible_schema().attr();

  if (attr_str == schema::kDictKeysSchemaAttr) {
    cause.set_error_message(
        absl::StrFormat("the schema for keys is incompatible.\n\n"
                        "Expected schema for keys: %v\n"
                        "Assigned schema for keys: %v",
                        expected_schema_str, assigned_schema_str));
  } else if (attr_str == schema::kDictValuesSchemaAttr) {
    cause.set_error_message(
        absl::StrFormat("the schema for values is incompatible.\n\n"
                        "Expected schema for values: %v\n"
                        "Assigned schema for values: %v",
                        expected_schema_str, assigned_schema_str));
  } else if (attr_str == schema::kListItemsSchemaAttr) {
    cause.set_error_message(
        absl::StrFormat("the schema for list items is incompatible.\n\n"
                        "Expected schema for list items: %v\n"
                        "Assigned schema for list items: %v",
                        expected_schema_str, assigned_schema_str));
  } else {
    std::string error_str = absl::StrFormat(
        "the schema for attribute '%s' is incompatible.\n\n"
        "Expected schema for '%s': %v\n"
        "Assigned schema for '%s': %v",
        attr_str, attr_str, expected_schema_str, attr_str, assigned_schema_str);
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

absl::StatusOr<std::string> FormatDictErrorMessage(
    const DataBagPtr& db1, const DataBagPtr& db2,
    const internal::DataBagMergeConflictError::DictConflict& conflict) {
  std::string key_str =
      internal::DataItemRepr(conflict.key, {.strip_quotes = false});
  ASSIGN_OR_RETURN(
      std::string item_str,
      DataItemToStr(conflict.object_id, /*schema=*/internal::DataItem(), db1));
  ASSIGN_OR_RETURN(
      std::string conflicting_item_str,
      DataItemToStr(conflict.object_id, /*schema=*/internal::DataItem(), db2));
  ASSIGN_OR_RETURN(std::string expected_value_str,
                   DataItemToStr(conflict.expected_value,
                                 /*schema=*/internal::DataItem(), db1));
  ASSIGN_OR_RETURN(std::string assigned_value_str,
                   DataItemToStr(conflict.assigned_value,
                                 /*schema=*/internal::DataItem(), db2));
  return conflict.object_id.is_schema()
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

absl::StatusOr<std::string> FormatEntityOrObjectErrorMessage(
    const DataBagPtr& db1, const DataBagPtr& db2,
    const internal::DataBagMergeConflictError::EntityObjectConflict& conflict) {
  ASSIGN_OR_RETURN(internal::DataItem a,
                   GetAttr(conflict.object_id, db1, conflict.attr_name));
  ASSIGN_OR_RETURN(internal::DataItem b,
                   GetAttr(conflict.object_id, db2, conflict.attr_name));
  ASSIGN_OR_RETURN(
      std::string item_str,
      DataItemToStr(conflict.object_id, /*schema=*/internal::DataItem(), db1));
  ASSIGN_OR_RETURN(std::string a_str,
                   DataItemToStr(a, /*schema=*/internal::DataItem(), db1));
  ASSIGN_OR_RETURN(std::string b_str,
                   DataItemToStr(b, /*schema=*/internal::DataItem(), db2));
  return absl::StrFormat(kDataBagMergeErrorTripleConflict, item_str,
                         conflict.attr_name, a_str, b_str);
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

absl::StatusOr<std::string> FormatListSizeErrorMessage(
    const DataBagPtr& db1, const DataBagPtr& db2,
    const internal::DataBagMergeConflictError::ListSizeConflict& conflict) {
  ASSIGN_OR_RETURN(std::string list_str,
                   DataItemToStr(conflict.list_object_id,
                                 /*schema=*/internal::DataItem(), db1));
  ASSIGN_OR_RETURN(std::string conflicting_list_str,
                   DataItemToStr(conflict.list_object_id,
                                 /*schema=*/internal::DataItem(), db2));
  return absl::StrFormat(kDataBagMergeErrorListSizeConflict, list_str,
                         conflicting_list_str, conflict.first_list_size,
                         conflict.second_list_size);
}

absl::StatusOr<std::string> FormatListContentErrorMessage(
    const DataBagPtr& db1, const DataBagPtr& db2,
    const internal::DataBagMergeConflictError::ListContentConflict& conflict) {
  ASSIGN_OR_RETURN(std::string list_str,
                   DataItemToStr(conflict.list_object_id,
                                 /*schema=*/internal::DataItem(), db1));
  ASSIGN_OR_RETURN(std::string conflicting_list_str,
                   DataItemToStr(conflict.list_object_id,
                                 /*schema=*/internal::DataItem(), db2));
  ASSIGN_OR_RETURN(std::string first_conflicting_item_str,
                   DataItemToStr(conflict.first_conflicting_item,
                                 /*schema=*/internal::DataItem(), db1));
  ASSIGN_OR_RETURN(std::string second_conflicting_item_str,
                   DataItemToStr(conflict.second_conflicting_item,
                                 /*schema=*/internal::DataItem(), db2));
  return absl::StrFormat(
      kDataBagMergeErrorListItemConflict, list_str, conflicting_list_str,
      conflict.list_item_conflict_index, first_conflicting_item_str,
      second_conflicting_item_str);
}

absl::StatusOr<std::string> FormatDataBagMergeError(
    const internal::DataBagMergeConflictError& conflict, const DataBagPtr& db1,
    const DataBagPtr& db2) {
  return std::visit(
      absl::Overload(
          [&](const internal::DataBagMergeConflictError::EntityObjectConflict&
                  error) {
            return FormatEntityOrObjectErrorMessage(db1, db2, error);
          },
          [&](const internal::DataBagMergeConflictError::DictConflict& error) {
            return FormatDictErrorMessage(db1, db2, error);
          },
          [&](const internal::DataBagMergeConflictError::ListSizeConflict&
                  error) {
            return FormatListSizeErrorMessage(db1, db2, error);
          },
          [&](const internal::DataBagMergeConflictError::ListContentConflict&
                  error) {
            return FormatListContentErrorMessage(db1, db2, error);
          }),
      conflict.conflict);
}

absl::StatusOr<std::string> FormatMissingCollectionItemSchemaError(
    const internal::MissingCollectionItemSchemaError& error,
    const DataBagPtr& db) {
  ASSIGN_OR_RETURN(DataSlice schema,
                   DataSlice::Create(error.missing_schema_item,
                                     DataSlice::JaggedShape::Empty(),
                                     internal::DataItem(schema::kSchema), db));
  ASSIGN_OR_RETURN(std::string schema_str, DataSliceToStr(schema));

  if (error.item_index &&
      error.collection_type ==
          internal::MissingCollectionItemSchemaError::CollectionType::kList) {
    return absl::StrCat(
        "list(s) expected, got an OBJECT DataSlice with the first non-list "
        "schema at ds.flatten().S[",
        *error.item_index, "] ", schema_str);
  }
  if (error.item_index &&
      error.collection_type ==
          internal::MissingCollectionItemSchemaError::CollectionType::kDict) {
    return absl::StrCat(
        "dict(s) expected, got an OBJECT DataSlice with the first non-dict "
        "schema at ds.flatten().S[",
        *error.item_index, "] ", schema_str);
  }
  if (error.collection_type ==
      internal::MissingCollectionItemSchemaError::CollectionType::kList) {
    return absl::StrCat("list(s) expected, got ", schema_str);
  }
  return absl::StrCat("dict(s) expected, got ", schema_str);
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

  // TODO(b/316118021) Move into different function.
  if (const internal::MissingObjectSchemaError* missing_object_schema_error =
          arolla::GetPayload<internal::MissingObjectSchemaError>(status);
      missing_object_schema_error != nullptr) {
    Error error;
    ASSIGN_OR_RETURN(std::string error_message,
                     FormatMissingObjectAttributeSchemaError(
                         *missing_object_schema_error, data.ds));
    error.set_error_message(std::move(error_message));
    return WithErrorPayload(status, std::move(error));
  }

  if (const internal::DataBagMergeConflictError* merge_conflict_error =
          arolla::GetPayload<internal::DataBagMergeConflictError>(status);
      merge_conflict_error != nullptr) {
    Error error;
    ASSIGN_OR_RETURN(std::string error_message,
                     FormatDataBagMergeError(*merge_conflict_error, data.db,
                                             data.to_be_merged_db));
    error.set_error_message(std::move(error_message));
    return WithErrorPayload(status, std::move(error));
  }

  // TODO(b/316118021) Split into different functions.
  if (const internal::MissingCollectionItemSchemaError*
          missing_collection_schema_error =
              arolla::GetPayload<internal::MissingCollectionItemSchemaError>(
                  status);
      missing_collection_schema_error != nullptr) {
    Error error;
    ASSIGN_OR_RETURN(std::string error_message,
                     FormatMissingCollectionItemSchemaError(
                         *missing_collection_schema_error, data.db));
    error.set_error_message(std::move(error_message));
    return WithErrorPayload(status, std::move(error));
  }
  // TODO(b/316118021) migrate away from proto based errors.
  std::optional<Error> cause = GetErrorPayload(status);
  if (!cause) {
    return status;
  }
  if (cause->has_incompatible_schema()) {
    ASSIGN_OR_RETURN(Error error, SetIncompatibleSchemaError(*std::move(cause),
                                                             data.db, data.ds));
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
