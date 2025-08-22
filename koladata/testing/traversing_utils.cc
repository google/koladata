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
#include "koladata/testing/traversing_utils.h"

#include <cstdint>
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
#include "arolla/util/repr.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/deep_diff.h"
#include "koladata/internal/op_utils/deep_equivalent.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/schema_attrs.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::testing {

namespace {

constexpr absl::string_view kActualName = "actual";
constexpr absl::string_view kExpectedName = "expected";

std::string GetPathRepr(
    const std::vector<internal::TraverseHelper::TransitionKey>& path,
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
    const std::vector<internal::TraverseHelper::TransitionKey>& path,
    const DataSlice& lhs, const DataSlice& rhs) {
  // TODO: Use DataItem repr with included schema ObjectId.
  ASSIGN_OR_RETURN(
      auto lhs_schema_repr,
      DataItemToStr(lhs.GetSchema().item(), internal::DataItem(schema::kSchema),
                    /*db=*/nullptr));
  ASSIGN_OR_RETURN(
      auto rhs_schema_repr,
      DataItemToStr(rhs.GetSchema().item(), internal::DataItem(schema::kSchema),
                    /*db=*/nullptr));
  if (path.size() == 1 &&
      path[0].type == internal::TraverseHelper::TransitionType::kSchema) {
    // Modified the schema of the provided DataSlice.
    return absl::StrFormat("modified schema:\n%s\n-> %s", rhs_schema_repr,
                           lhs_schema_repr);
  }
  return absl::StrFormat("modified schema:\n%s:\n%s\n-> %s:\n%s",
                         GetPathRepr(path, kExpectedName), rhs_schema_repr,
                         GetPathRepr(path, kActualName), lhs_schema_repr);
}

absl::StatusOr<std::string> DiffItemRepr(
    const internal::DeepEquivalentOp::DiffItem& diff,
    const internal::DataBagImpl& result_db_impl, const DataBagPtr& lhs_db,
    const DataBagPtr& rhs_db) {
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
  ASSIGN_OR_RETURN(auto lhs,
                   get_side_item(internal::DeepDiff::kLhsAttr, lhs_db));
  ASSIGN_OR_RETURN(auto rhs,
                   get_side_item(internal::DeepDiff::kRhsAttr, rhs_db));
  ReprOption repr_option({.show_databag_id = false});
  if (lhs.has_value() && !rhs.has_value()) {
    return absl::StrFormat("added:\n%s:\n%s",
                           GetPathRepr(diff.path, kActualName),
                           DataSliceRepr(*lhs, repr_option));
  } else if (!lhs.has_value() && rhs.has_value()) {
    return absl::StrFormat("deleted:\n%s:\n%s",
                           GetPathRepr(diff.path, kExpectedName),
                           DataSliceRepr(*rhs, repr_option));
  } else if (lhs.has_value() && rhs.has_value()) {
    ASSIGN_OR_RETURN(auto is_schema_mismatch,
                     result_db_impl.GetAttr(
                         diff_item, internal::DeepDiff::kSchemaMismatchAttr));
    if (is_schema_mismatch.has_value()) {
      return SchemaMismatchRepr(diff.path, *lhs, *rhs);
    } else {
      if (diff.path.size() == 1 &&
          diff.path[0].type ==
              internal::TraverseHelper::TransitionType::kSliceItem &&
          diff.path[0].index == -1) {
        // The DataItem itself is modified.
        return absl::StrFormat("modified:\n%s:\n%s\n-> %s:\n%s",
                               kExpectedName, DataSliceRepr(*rhs, repr_option),
                               kActualName, DataSliceRepr(*lhs, repr_option));
      }
      return absl::StrFormat("modified:\n%s:\n%s\n-> %s:\n%s",
                             GetPathRepr(diff.path, kExpectedName),
                             DataSliceRepr(*rhs, repr_option),
                             GetPathRepr(diff.path, kActualName),
                             DataSliceRepr(*lhs, repr_option));
    }
  }
  LOG(FATAL) << "diff item has unexpected schema: no lhs or rhs attributes";
}

}  // namespace

absl::StatusOr<std::vector<std::string>> DeepEquivalentMismatches(
    const DataSlice& lhs, const DataSlice& rhs, int64_t max_count,
    DeepEquivalentParams comparison_params) {
  if (max_count < 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("max_count should be >= 0, got ", max_count));
  }
  if (!lhs.GetShape().IsEquivalentTo(rhs.GetShape())) {
    return std::vector<std::string>({absl::StrFormat(
        "expected both DataSlices to be of the same shape but got %s and %s",
        arolla::Repr(lhs.GetShape()), arolla::Repr(rhs.GetShape()))});
  }
  const auto empty_db = DataBag::Empty();
  const auto& lhs_db = lhs.GetBag() != nullptr ? lhs.GetBag() : empty_db;
  const auto& rhs_db = rhs.GetBag() != nullptr ? rhs.GetBag() : empty_db;
  FlattenFallbackFinder lhs_fb_finder(*lhs_db);
  auto lhs_fallbacks_span = lhs_fb_finder.GetFlattenFallbacks();
  FlattenFallbackFinder rhs_fb_finder(*rhs_db);
  auto rhs_fallbacks_span = rhs_fb_finder.GetFlattenFallbacks();
  std::vector<std::string> mismatches;
  RETURN_IF_ERROR(
      lhs.VisitImpl([&]<class T>(const T& lhs_impl) -> absl::Status {
        const T& rhs_impl = rhs.impl<T>();
        auto result_db = DataBag::EmptyMutable();
        ASSIGN_OR_RETURN(internal::DataBagImpl & result_db_impl,
                         result_db->GetMutableImpl());
        auto deep_equivalent_op =
            internal::DeepEquivalentOp(&result_db_impl, comparison_params);
        ASSIGN_OR_RETURN(auto result,
                         deep_equivalent_op(
                             lhs_impl, lhs.GetSchemaImpl(), lhs_db->GetImpl(),
                             lhs_fallbacks_span, rhs_impl, rhs.GetSchemaImpl(),
                             rhs_db->GetImpl(), rhs_fallbacks_span));
        ASSIGN_OR_RETURN(auto diff_paths,
                         deep_equivalent_op.GetDiffPaths(
                             result, internal::DataItem(schema::kObject),
                             /*max_count=*/max_count));
        for (const auto& diff : diff_paths) {
          ASSIGN_OR_RETURN(auto diff_item_repr,
                           DiffItemRepr(diff, result_db_impl, lhs_db, rhs_db));
          mismatches.push_back(std::move(diff_item_repr));
        }
        return absl::OkStatus();
      }));
  return mismatches;
}

}  // namespace koladata::testing
