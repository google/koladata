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
#include "arolla/util/repr.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/deep_diff_utils.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/deep_diff.h"
#include "koladata/internal/op_utils/deep_equivalent.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::testing {

namespace {

constexpr absl::string_view kActualName = "actual";
constexpr absl::string_view kExpectedName = "expected";

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
        ReprOption repr_option(
            {.show_databag_id = false,
             .show_item_id = comparison_params.ids_equality});
        ASSIGN_OR_RETURN(auto diff_paths,
                         deep_equivalent_op.GetDiffPaths(
                             result, internal::DataItem(schema::kObject),
                             /*max_count=*/max_count));
        if constexpr (std::is_same_v<T, internal::DataSliceImpl>) {
          // if both DataSlices are empty, for the sake of comparison we assume
          // that each DataSlice have single missing DataItem.
          if (lhs_impl.size() == 0 && result.size() != 0) {
            for (auto& diff : diff_paths) {
              if (diff.path.empty() ||
                  diff.path[0].type !=
                      internal::TraverseHelper::TransitionType::kSliceItem ||
                  diff.path[0].index != 0) {
                LOG(FATAL) << "diff path has unexpected structure";
              } else {
                diff.path[0].index = -1;
              }
            }
          }
        }
        for (const auto& diff : diff_paths) {
          ASSIGN_OR_RETURN(
              auto diff_item_repr,
              DiffItemRepr(diff, result_db_impl, lhs_db,
                           internal::DeepDiff::kLhsAttr, kActualName, rhs_db,
                           internal::DeepDiff::kRhsAttr, kExpectedName,
                           repr_option));
          mismatches.push_back(std::move(diff_item_repr));
        }
        return absl::OkStatus();
      }));
  return mismatches;
}

}  // namespace koladata::testing
