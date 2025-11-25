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
#include "koladata/internal/op_utils/deep_diff.h"

#include <algorithm>
#include <cstdint>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

absl::Status DeepDiff::LhsOnlyAttribute(
    const DataItem& token, const TraverseHelper::TransitionKey& key,
    const TraverseHelper::Transition& lhs) {
  if (key.type == TraverseHelper::TransitionType::kDictKey) {
    // We process dict keys when we process dict values.
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(auto diff_item, CreateLhsOnlyDiffItem(lhs));
  ASSIGN_OR_RETURN(auto diff_wrapper, CreateDiffWrapper(diff_item));
  return SaveTransition(token, key, std::move(diff_wrapper));
}

absl::Status DeepDiff::RhsOnlyAttribute(
    const DataItem& token, const TraverseHelper::TransitionKey& key,
    const TraverseHelper::Transition& rhs) {
  if (key.type == TraverseHelper::TransitionType::kDictKey) {
    // We process dict keys when we process dict values.
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(auto diff_item, CreateRhsOnlyDiffItem(rhs));
  ASSIGN_OR_RETURN(auto diff_wrapper, CreateDiffWrapper(diff_item));
  return SaveTransition(token, key, std::move(diff_wrapper));
}

absl::Status DeepDiff::LhsRhsMismatch(const DataItem& token,
                                      const TraverseHelper::TransitionKey& key,
                                      const TraverseHelper::Transition& lhs,
                                      const TraverseHelper::Transition& rhs,
                                      bool is_schema_mismatch) {
  if (key.type == TraverseHelper::TransitionType::kDictKey) {
    // We process dict keys when we process dict values.
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(auto diff_item,
                   CreateMismatchDiffItem(lhs, rhs, is_schema_mismatch));
  ASSIGN_OR_RETURN(auto diff_wrapper, CreateDiffWrapper(diff_item));
  return SaveTransition(token, key, std::move(diff_wrapper));
}

absl::StatusOr<DataItem> DeepDiff::SliceItemMismatch(
    const TraverseHelper::TransitionKey& key,
    const TraverseHelper::Transition& lhs,
    const TraverseHelper::Transition& rhs, bool is_schema_mismatch) {
  DCHECK(key.type == TraverseHelper::TransitionType::kSliceItem);
  ASSIGN_OR_RETURN(auto diff_item,
                   CreateMismatchDiffItem(lhs, rhs, is_schema_mismatch));
  return CreateDiffWrapper(diff_item);
}

absl::StatusOr<DataItem> DeepDiff::CreateTokenLike(const DataItem& item) {
  if (!item.holds_value<ObjectId>()) {
    return item;
  }
  ObjectId id;
  if (item.is_list()) {
    id = AllocateSingleList();
  } else if (item.is_dict()) {
    id = AllocateSingleDict();
  } else {
    // We create normal object tokens for schemas comparison.
    id = AllocateSingleObject();
  }
  DataItem result(id);
  ASSIGN_OR_RETURN(
      auto result_schema,
      CreateUuidWithMainObject<internal::ObjectId::kUuidImplicitSchemaFlag>(
          result, schema::kImplicitSchemaSeed));
  RETURN_IF_ERROR(
      databag_->SetAttr(result, schema::kSchemaAttr, std::move(result_schema)));
  return result;
}

absl::Status DeepDiff::SaveTransition(const DataItem& token,
                                      const TraverseHelper::TransitionKey& key,
                                      const DataItem& value) {
  if (key.type == TraverseHelper::TransitionType::kDictKey) {
    // We process dict keys when we process dict values.
    return absl::OkStatus();
  }
  ASSIGN_OR_RETURN(auto token_schema,
                   databag_->GetAttr(token, schema::kSchemaAttr));
  if (key.type == TraverseHelper::TransitionType::kAttributeName) {
    DCHECK(key.value.holds_value<arolla::Text>());
    auto attr_name = key.value.value<arolla::Text>();
    RETURN_IF_ERROR(databag_->SetSchemaAttr(token_schema, attr_name,
                                            DataItem(schema::kObject)));
    RETURN_IF_ERROR(databag_->SetAttr(token, attr_name, value));
  } else if (key.type == TraverseHelper::TransitionType::kSchemaAttributeName) {
    DCHECK(key.value.holds_value<arolla::Text>());
    // Use of special schema attributes (ex.: kListItemsSchemaAttr,
    // kDictKeysSchemaAttr, etc.) as attributes of an object leads to
    // inconsistent state of the databag, so we prepend all the attribute names
    // with kSchemaAttrPrefix.
    auto attr_name = arolla::Text(absl::StrCat(
        kSchemaAttrPrefix, key.value.value<arolla::Text>().view()));
    RETURN_IF_ERROR(databag_->SetSchemaAttr(token_schema, attr_name,
                                            DataItem(schema::kObject)));
    RETURN_IF_ERROR(databag_->SetAttr(token, attr_name, value));
  } else if (key.type == TraverseHelper::TransitionType::kListItem) {
    RETURN_IF_ERROR(databag_->SetSchemaAttr(
        token_schema, schema::kListItemsSchemaAttr, DataItem(schema::kObject)));
    ASSIGN_OR_RETURN(auto list_size, databag_->GetListSize(token));
    if (list_size == DataItem()) {
      list_size = DataItem(0);
    }
    auto extend_size =
        std::max(int64_t{0}, key.index + 1 - list_size.value<int64_t>());
    RETURN_IF_ERROR(databag_->ExtendList(
        token, DataSliceImpl::CreateEmptyAndUnknownType(extend_size)));
    RETURN_IF_ERROR(databag_->SetInList(token, key.index, value));
  } else if (key.type == TraverseHelper::TransitionType::kDictValue) {
    RETURN_IF_ERROR(databag_->SetSchemaAttr(
        token_schema, schema::kDictKeysSchemaAttr, DataItem(schema::kObject)));
    RETURN_IF_ERROR(databag_->SetSchemaAttr(token_schema,
                                            schema::kDictValuesSchemaAttr,
                                            DataItem(schema::kObject)));
    RETURN_IF_ERROR(databag_->SetInDict(token, key.value, value));
  } else {
    return absl::InternalError("unsupported transition type");
  }
  return absl::OkStatus();
}

absl::StatusOr<DataItem> DeepDiff::CreateLhsOnlyDiffItem(
    const TraverseHelper::Transition& lhs) {
  auto result = DataItem(AllocateSingleObject());
  ASSIGN_OR_RETURN(auto result_schema,
                   CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
                       result, schema::kImplicitSchemaSeed));
  RETURN_IF_ERROR(databag_->SetSchemaAttr(result_schema, kLhsAttr, lhs.schema));
  RETURN_IF_ERROR(
      databag_->SetAttr(result, schema::kSchemaAttr, std::move(result_schema)));
  RETURN_IF_ERROR(databag_->SetAttr(result, kLhsAttr, lhs.item));
  if (lhs.item.holds_value<ObjectId>() && lhs.schema == schema::kObject) {
    RETURN_IF_ERROR(databag_->SetAttr(lhs.item, schema::kSchemaAttr,
                                      DataItem(schema::kItemId)));
  }
  return result;
}

absl::StatusOr<DataItem> DeepDiff::CreateRhsOnlyDiffItem(
    const TraverseHelper::Transition& rhs) {
  auto result = DataItem(AllocateSingleObject());
  ASSIGN_OR_RETURN(auto result_schema,
                   CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
                       result, schema::kImplicitSchemaSeed));
  RETURN_IF_ERROR(databag_->SetSchemaAttr(result_schema, kRhsAttr, rhs.schema));
  RETURN_IF_ERROR(
      databag_->SetAttr(result, schema::kSchemaAttr, std::move(result_schema)));
  RETURN_IF_ERROR(databag_->SetAttr(result, kRhsAttr, rhs.item));
  if (rhs.item.holds_value<ObjectId>() && rhs.schema == schema::kObject) {
    RETURN_IF_ERROR(databag_->SetAttr(rhs.item, schema::kSchemaAttr,
                                      DataItem(schema::kItemId)));
  }
  return result;
}

absl::StatusOr<DataItem> DeepDiff::CreateMismatchDiffItem(
    const TraverseHelper::Transition& lhs,
    const TraverseHelper::Transition& rhs, bool is_schema_mismatch) {
  auto result = DataItem(AllocateSingleObject());
  ASSIGN_OR_RETURN(auto result_schema,
                   CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
                       result, schema::kImplicitSchemaSeed));
  if (is_schema_mismatch) {
    RETURN_IF_ERROR(databag_->SetSchemaAttr(result_schema, kSchemaMismatchAttr,
                                            DataItem(schema::kMask)));
    RETURN_IF_ERROR(databag_->SetAttr(result, kSchemaMismatchAttr,
                                      DataItem(arolla::kPresent)));
  }
  RETURN_IF_ERROR(databag_->SetSchemaAttr(result_schema, kLhsAttr, lhs.schema));
  RETURN_IF_ERROR(databag_->SetSchemaAttr(result_schema, kRhsAttr, rhs.schema));
  RETURN_IF_ERROR(
      databag_->SetAttr(result, schema::kSchemaAttr, std::move(result_schema)));
  RETURN_IF_ERROR(databag_->SetAttr(result, kLhsAttr, lhs.item));
  RETURN_IF_ERROR(databag_->SetAttr(result, kRhsAttr, rhs.item));
  if (lhs.item.holds_value<ObjectId>() && lhs.schema == schema::kObject) {
    RETURN_IF_ERROR(databag_->SetAttr(lhs.item, schema::kSchemaAttr,
                                      DataItem(schema::kItemId)));
  }
  if (rhs.item.holds_value<ObjectId>() && rhs.schema == schema::kObject) {
    RETURN_IF_ERROR(databag_->SetAttr(rhs.item, schema::kSchemaAttr,
                                      DataItem(schema::kItemId)));
  }
  return result;
}

absl::StatusOr<DataItem> DeepDiff::CreateDiffWrapper(
    const DataItem& diff_item) {
  auto result = DataItem(AllocateSingleObject());
  DataItem object_schema(schema::kObject);
  RETURN_IF_ERROR(databag_->SetAttr(result, kDiffItemAttr, diff_item));
  auto diff_wrapper_schema =
      CreateSchemaUuidFromFields(kDiffWrapperSeed, {}, {});
  RETURN_IF_ERROR(databag_->SetSchemaAttr(diff_wrapper_schema, kDiffItemAttr,
                                          DataItem(schema::kObject)));
  RETURN_IF_ERROR(databag_->SetAttr(result, schema::kSchemaAttr,
                                    std::move(diff_wrapper_schema)));
  return result;
}

}  // namespace koladata::internal
