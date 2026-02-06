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
#include "koladata/deep_diff_utils.h"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/op_utils/deep_diff.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/schema_attrs.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {

std::string GetPathRepr(
    absl::Span<const internal::TraverseHelper::TransitionKey> path,
    absl::string_view side) {
  if (path.empty()) {
    return std::string(side);
  }
  auto path_but_last = [&]() {
    return internal::TraverseHelper::TransitionKeySequenceToAccessPath(
        absl::MakeSpan(path).subspan(0, path.size() - 1));
  };
  if (path.back().type ==
      internal::TraverseHelper::TransitionType::kDictValue) {
    return absl::StrFormat("key %s in %s%s", DataItemRepr(path.back().value),
                           side, path_but_last());
  } else if (path.back().type ==
             internal::TraverseHelper::TransitionType::kListItem) {
    return absl::StrFormat("item at position %d in list %s%s",
                           path.back().index, side, path_but_last());
  } else {
    auto path_repr =
        internal::TraverseHelper::TransitionKeySequenceToAccessPath(
            absl::MakeSpan(path));
    return absl::StrCat(side, path_repr);
  }
}

absl::StatusOr<std::string> SchemaMismatchRepr(
    absl::Span<const internal::TraverseHelper::TransitionKey> path,
    const DataSlice& lhs, absl::string_view lhs_name, const DataSlice& rhs,
    absl::string_view rhs_name, ReprOption schema_repr_option) {
  // For the schema mismatch, we always show the item ids as schemas with
  // different item ids are not considered equivalent.
  schema_repr_option.show_item_id = true;
  auto lhs_schema_repr = DataSliceRepr(lhs.GetSchema(), schema_repr_option);
  auto rhs_schema_repr = DataSliceRepr(rhs.GetSchema(), schema_repr_option);
  if (path.size() == 1 &&
      path[0].type == internal::TraverseHelper::TransitionType::kSchema) {
    // Modified the schema of the provided DataSlice.
    return absl::StrFormat("modified schema:\n%s\n-> %s", rhs_schema_repr,
                           lhs_schema_repr);
  }
  return absl::StrFormat("modified schema:\n%s:\n%s\n-> %s:\n%s",
                         GetPathRepr(path, rhs_name), rhs_schema_repr,
                         GetPathRepr(path, lhs_name), lhs_schema_repr);
}

absl::StatusOr<std::string> DiffItemRepr(
    const internal::DeepDiff::DiffItem& diff,
    const internal::DataBagImpl& result_db_impl, const DataBagPtr& lhs_db,
    absl::string_view lhs_attr_name, absl::string_view lhs_name,
    const DataBagPtr& rhs_db, absl::string_view rhs_attr_name,
    absl::string_view rhs_name, const ReprOption& repr_option) {
  ASSIGN_OR_RETURN(
      auto diff_item,
      result_db_impl.GetAttr(diff.item, internal::DeepDiff::kDiffItemAttr));
  ASSIGN_OR_RETURN(auto diff_item_schema,
                   result_db_impl.GetAttr(diff_item, schema::kSchemaAttr));
  auto get_side_item = [&](absl::string_view side_attr_name,
                           const DataBagPtr& side_db)
      -> absl::StatusOr<std::optional<DataSlice>> {
    ASSIGN_OR_RETURN(auto side_item,
                     result_db_impl.GetAttr(diff_item, side_attr_name));
    ASSIGN_OR_RETURN(auto side_schema, result_db_impl.GetSchemaAttrAllowMissing(
                                           diff_item_schema, side_attr_name));
    if (!side_schema.has_value()) {
      return std::nullopt;
    }
    return DataSlice::Create(side_item, std::move(side_schema), side_db);
  };
  ASSIGN_OR_RETURN(auto lhs, get_side_item(lhs_attr_name, lhs_db));
  ASSIGN_OR_RETURN(auto rhs, get_side_item(rhs_attr_name, rhs_db));
  if (lhs.has_value() && !rhs.has_value()) {
    return absl::StrFormat("added:\n%s:\n%s", GetPathRepr(diff.path, lhs_name),
                           DataSliceRepr(*lhs, repr_option));
  } else if (!lhs.has_value() && rhs.has_value()) {
    return absl::StrFormat("deleted:\n%s:\n%s",
                           GetPathRepr(diff.path, rhs_name),
                           DataSliceRepr(*rhs, repr_option));
  } else if (lhs.has_value() && rhs.has_value()) {
    ASSIGN_OR_RETURN(auto is_schema_mismatch,
                     result_db_impl.GetAttr(
                         diff_item, internal::DeepDiff::kSchemaMismatchAttr));
    if (is_schema_mismatch.has_value()) {
      return SchemaMismatchRepr(diff.path, *lhs, lhs_name, *rhs, rhs_name,
                                repr_option);
    } else {
      if (diff.path.size() == 1 &&
          diff.path[0].type ==
              internal::TraverseHelper::TransitionType::kSliceItem &&
          diff.path[0].index == -1) {
        // The DataItem itself is modified.
        return absl::StrFormat("modified:\n%s:\n%s\n-> %s:\n%s", rhs_name,
                               DataSliceRepr(*rhs, repr_option), lhs_name,
                               DataSliceRepr(*lhs, repr_option));
      }
      return absl::StrFormat(
          "modified:\n%s:\n%s\n-> %s:\n%s", GetPathRepr(diff.path, rhs_name),
          DataSliceRepr(*rhs, repr_option), GetPathRepr(diff.path, lhs_name),
          DataSliceRepr(*lhs, repr_option));
    }
  }
  return absl::InvalidArgumentError(
      "failed to generate diff item repr for unexpected diff structure");
}

}  // namespace koladata
