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
#include <vector>

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/util/repr.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/deep_diff.h"
#include "koladata/internal/op_utils/deep_equivalent.h"
#include "koladata/internal/schema_attrs.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::testing {

absl::StatusOr<std::vector<std::string>> DeepEquivalentMismatches(
    const DataSlice& lhs, const DataSlice& rhs, int64_t max_count) {
  if (max_count < 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("max_count should be >= 0, got ", max_count));
  }
  if (!lhs.GetShape().IsEquivalentTo(rhs.GetShape())) {
    return std::vector<std::string>({absl::StrCat(
        "Shapes are not equivalent: ", arolla::Repr(lhs.GetShape()), " vs ",
        arolla::Repr(rhs.GetShape()))});
  }
  const auto empty_db = DataBag::Empty();
  const auto& lhs_db = lhs.GetBag() != nullptr ? lhs.GetBag() : empty_db;
  const auto& rhs_db = rhs.GetBag() != nullptr ? rhs.GetBag() : empty_db;
  FlattenFallbackFinder lhs_fb_finder(*lhs_db);
  auto lhs_fallbacks_span = lhs_fb_finder.GetFlattenFallbacks();
  FlattenFallbackFinder rhs_fb_finder(*rhs_db);
  auto rhs_fallbacks_span = rhs_fb_finder.GetFlattenFallbacks();
  std::vector<std::string> mismatches;
  RETURN_IF_ERROR(lhs.VisitImpl([&]<class T>(
                                    const T& lhs_impl) -> absl::Status {
    const T& rhs_impl = rhs.impl<T>();
    auto result_db = DataBag::Empty();
    ASSIGN_OR_RETURN(auto result_db_impl, result_db->GetMutableImpl());
    auto deep_equivalent_op = internal::DeepEquivalentOp(&result_db_impl.get());
    ASSIGN_OR_RETURN(
        auto result,
        deep_equivalent_op(lhs_impl, lhs.GetSchemaImpl(), lhs_db->GetImpl(),
                           lhs_fallbacks_span, rhs_impl, rhs.GetSchemaImpl(),
                           rhs_db->GetImpl(), rhs_fallbacks_span));
    ASSIGN_OR_RETURN(auto diff_paths,
                     deep_equivalent_op.GetDiffPaths(
                         result, internal::DataItem(schema::kObject),
                         /*max_count=*/max_count));
    for (const auto& diff : diff_paths) {
      ASSIGN_OR_RETURN(auto diff_item,
                       result_db_impl.get().GetAttr(
                           diff.item, internal::DeepDiff::kDiffItemAttr));
      ASSIGN_OR_RETURN(
          auto diff_item_schema,
          result_db_impl.get().GetAttr(diff_item, schema::kSchemaAttr));

      auto get_side_item = [&](absl::string_view side_attr_name,
                               const DataBagPtr& side_db)
          -> absl::StatusOr<std::optional<DataSlice>> {
        ASSIGN_OR_RETURN(auto side_item, result_db_impl.get().GetAttr(
                                             diff_item, side_attr_name));
        ASSIGN_OR_RETURN(auto side_schema,
                         result_db_impl.get().GetSchemaAttrAllowMissing(
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
      if (lhs.has_value() && rhs.has_value()) {
        mismatches.push_back(absl::StrCat(diff.path, ": ", DataSliceRepr(*lhs),
                                          " vs ", DataSliceRepr(*rhs)));
      } else if (lhs.has_value()) {
        mismatches.push_back(
            absl::StrCat(diff.path, ": ", DataSliceRepr(*lhs), " vs missing"));
      } else if (rhs.has_value()) {
        mismatches.push_back(
            absl::StrCat(diff.path, ": missing vs ", DataSliceRepr(*rhs)));
      } else {
        LOG(FATAL)
            << "diff item has unexpected schema: no lhs or rhs attributes";
      }
    }
    return absl::OkStatus();
  }));
  return mismatches;
}

}  // namespace koladata::testing
