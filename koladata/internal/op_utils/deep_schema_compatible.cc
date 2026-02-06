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
#include "koladata/internal/op_utils/deep_schema_compatible.h"

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/deep_comparator.h"
#include "koladata/internal/op_utils/deep_diff.h"
#include "koladata/internal/op_utils/object_finder.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

namespace {

// A comparator that is used in DeepComparator to check if two schemas are
// compatible.
// The result of the comparison is constructed as DeepDiff.
class SchemaCompatibleComparator : public AbstractComparator {
 public:
  explicit SchemaCompatibleComparator(
      DataBagImplPtr result_databag,
      DeepSchemaCompatibleOp::SchemaCompatibleParams params,
      DeepSchemaCompatibleOp::CompatibleCallback schema_compatible_callback)
      : result_(std::move(result_databag)),
        params_(params),
        schema_compatible_callback_(schema_compatible_callback),
        diff_found_(false) {}

  absl::StatusOr<DataItem> CreateToken(
      const TraverseHelper::Transition& lhs,
      const TraverseHelper::Transition& rhs) override {
    if (!lhs.item.has_value()) {
      if (rhs.item.has_value()) {
        return absl::InvalidArgumentError(
            "missing and present items should not be considered for deeper "
            "comparison");
      }
      // If lhs and rhs are missing, we create a token that represents
      // a result of the schemas comparison.
      return result_.CreateTokenLike(DataItem(AllocateSingleObject()));
    }
    return result_.CreateTokenLike(lhs.item);
  }

  absl::Status LhsRhsMatch(const DataItem& parent,
                           const TraverseHelper::TransitionKey& key,
                           const DataItem& child) override {
    return result_.SaveTransition(parent, key, child);
  }
  absl::Status LhsOnlyAttribute(
      const DataItem& token, const TraverseHelper::TransitionKey& key,
      const TraverseHelper::Transition& lhs) override {
    if (params_.allow_removing_attrs) {
      return absl::OkStatus();
    }
    if (!lhs.item.is_schema()) {
      // We should ignore schema names and metadata when comparing schemas.
      return absl::OkStatus();
    }
    diff_found_ = true;
    return result_.LhsOnlyAttribute(token, key, lhs);
  }
  absl::Status RhsOnlyAttribute(
      const DataItem& token, const TraverseHelper::TransitionKey& key,
      const TraverseHelper::Transition& rhs) override {
    if (params_.allow_new_attrs) {
      return absl::OkStatus();
    }
    if (!rhs.item.is_schema()) {
      // We should ignore schema names and metadata when comparing schemas.
      return absl::OkStatus();
    }
    diff_found_ = true;
    return result_.RhsOnlyAttribute(token, key, rhs);
  }
  absl::Status LhsRhsMismatch(const DataItem& token,
                              const TraverseHelper::TransitionKey& key,
                              const TraverseHelper::Transition& lhs,
                              const TraverseHelper::Transition& rhs) override {
    if (params_.allow_removing_attrs && !rhs.item.has_value()) {
      return absl::OkStatus();
    }
    if (params_.allow_new_attrs && !lhs.item.has_value()) {
      return absl::OkStatus();
    }
    if (!lhs.item.is_schema() && !rhs.item.is_schema()) {
      // We should ignore schema names and metadata when comparing schemas.
      return absl::OkStatus();
    }
    diff_found_ = true;
    return result_.LhsRhsMismatch(token, key, lhs, rhs,
                                  /*is_schema_mismatch=*/false);
  }
  absl::StatusOr<DataItem> SliceItemMismatch(
      const TraverseHelper::TransitionKey& key,
      const TraverseHelper::Transition& lhs,
      const TraverseHelper::Transition& rhs) override {
    diff_found_ = true;
    return result_.SliceItemMismatch(key, lhs, rhs,
                                     /*is_schema_mismatch=*/false);
  }
  int CompareOrder(const TraverseHelper::TransitionKey& lhs,
                   const TraverseHelper::TransitionKey& rhs) override {
    return rhs.value.VisitValue([&]<class T>(const T& rhs_value) -> int {
      if (DataItem::Eq()(lhs.value, rhs_value)) return 0;
      return DataItem::Less()(lhs.value, rhs_value) ? -1 : 1;
    });
  }
  bool Equal(const TraverseHelper::Transition& lhs,
             const TraverseHelper::Transition& rhs) override {
    DataItem from_schema = lhs.item;
    DataItem to_schema = rhs.item;
    if (!from_schema.is_schema() || !to_schema.is_schema()) {
      // We should ignore schema names and metadata when comparing schemas. And
      // not compare metadata deeply.
      return false;
    }
    return schema_compatible_callback_(from_schema, to_schema);
  }
  bool HasDiff() const { return diff_found_; }

 private:
  DeepDiff result_;
  DeepSchemaCompatibleOp::SchemaCompatibleParams params_;
  DeepSchemaCompatibleOp::CompatibleCallback schema_compatible_callback_;
  bool diff_found_;
};

}  // namespace


absl::StatusOr<std::pair<bool, DataItem>> DeepSchemaCompatibleOp::operator()(
    const DataItem& from_schema,
    const DataBagImpl& from_databag, DataBagImpl::FallbackSpan from_fallbacks,
    const DataItem& to_schema,
    const DataBagImpl& to_databag,
    DataBagImpl::FallbackSpan to_fallbacks) const {
  // For DataItems, we adapt the DataSliceImpl interface.
  auto comparator = std::make_unique<SchemaCompatibleComparator>(
      DataBagImplPtr::NewRef(new_databag_), params_, callback_);
  auto lhs_traverse_helper = TraverseHelper(from_databag, from_fallbacks);
  auto rhs_traverse_helper = TraverseHelper(to_databag, to_fallbacks);
  auto compare_op = DeepComparator<SchemaCompatibleComparator>(
      std::move(lhs_traverse_helper), std::move(rhs_traverse_helper),
      std::move(comparator));
  ASSIGN_OR_RETURN(auto result, compare_op.CompareSlices(
      DataSliceImpl::Create(1, from_schema), DataItem(schema::kSchema),
      DataSliceImpl::Create(1, to_schema), DataItem(schema::kSchema)));
  return std::make_pair(!compare_op.Comparator().HasDiff(), result[0]);
}

absl::StatusOr<std::vector<DeepDiff::DiffItem>>
DeepSchemaCompatibleOp::GetDiffPaths(const DataItem& item,
                                     size_t max_count) const {
  auto traverse_helper = TraverseHelper(*new_databag_, {});
  std::vector<DeepDiff::DiffItem> diff_paths;
  auto diff_uuid =
      CreateSchemaUuidFromFields(DeepDiff::kDiffWrapperSeed, {}, {});
  auto lambda_visitor =
      [&](const DataItem& item, const DataItem& schema,
          absl::FunctionRef<std::vector<TraverseHelper::TransitionKey>()>
              path) {
        if (schema == diff_uuid && diff_paths.size() < max_count) {
          diff_paths.push_back(
              {.path = path(), .item = item, .schema = schema});
        }
        return absl::OkStatus();
      };
  auto diff_finder =
      ObjectFinder(*new_databag_, {}, DeepDiff::kSchemaAttrPrefix);
  RETURN_IF_ERROR(diff_finder.TraverseSlice(item, DataItem(schema::kObject),
                                            lambda_visitor));
  return diff_paths;
}

}  // namespace koladata::internal
