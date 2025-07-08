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
#include "koladata/internal/op_utils/deep_equivalent.h"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/deep_comparator.h"
#include "koladata/internal/op_utils/deep_diff.h"
#include "koladata/internal/op_utils/object_finder.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::internal {

namespace {

class EquivalentComparator : public AbstractComparator {
 public:
  explicit EquivalentComparator(DataBagImplPtr result_databag)
      : result_(std::move(result_databag)) {}

  absl::StatusOr<DataItem> CreateToken(
      TraverseHelper::Transition lhs, TraverseHelper::Transition rhs) override {
    return result_.CreateTokenLike(std::move(lhs.item));
  }
  absl::Status LhsRhsMatch(DataItem parent, TraverseHelper::TransitionKey key,
                           DataItem child) override {
    return result_.SaveTransition(std::move(parent), std::move(key),
                                  std::move(child));
  }
  absl::Status LhsOnlyAttribute(DataItem token,
                                TraverseHelper::TransitionKey key,
                                TraverseHelper::Transition lhs) override {
    return result_.LhsOnlyAttribute(std::move(token), std::move(key),
                                    std::move(lhs));
  }
  absl::Status RhsOnlyAttribute(DataItem token,
                                TraverseHelper::TransitionKey key,
                                TraverseHelper::Transition rhs) override {
    return result_.RhsOnlyAttribute(std::move(token), std::move(key),
                                    std::move(rhs));
  }
  absl::Status LhsRhsMismatch(DataItem token, TraverseHelper::TransitionKey key,
                              TraverseHelper::Transition lhs,
                              TraverseHelper::Transition rhs) override {
    return result_.LhsRhsMismatch(std::move(token), std::move(key),
                                  std::move(lhs), std::move(rhs));
  }
  absl::StatusOr<DataItem> SliceItemMismatch(
      TraverseHelper::TransitionKey key, TraverseHelper::Transition lhs,
      TraverseHelper::Transition rhs) override {
    return result_.SliceItemMismatch(std::move(key), std::move(lhs),
                                     std::move(rhs));
  }
  int CompareOrder(TraverseHelper::TransitionKey lhs,
                   TraverseHelper::TransitionKey rhs) override {
    if (lhs.type != rhs.type) {
      return lhs.type < rhs.type ? -1 : 1;
    }
    if (lhs.type == TraverseHelper::TransitionType::kListItem) {
      if (lhs.index == rhs.index) return 0;
      return lhs.index < rhs.index ? -1 : 1;
    }
    auto lhs_type_id = lhs.value.type_id();
    auto rhs_type_id = rhs.value.type_id();
    if (lhs_type_id != rhs_type_id) {
      return lhs_type_id < rhs_type_id ? -1 : 1;
    }
    return rhs.value.VisitValue([&]<class T>(const T& rhs_value) -> int {
      if (DataItem::Eq()(lhs.value, rhs_value)) return 0;
      return DataItem::Less()(lhs.value, rhs_value) ? -1 : 1;
    });
  }
  bool Equal(TraverseHelper::Transition lhs,
             TraverseHelper::Transition rhs) override {
    if (!lhs.item.holds_value<ObjectId>() ||
        !rhs.item.holds_value<ObjectId>()) {
      // If lhs and rhs are NaNs, they are considered not equal.
      return lhs.item == rhs.item;
    }
    if (lhs.item.is_list() != rhs.item.is_list()) {
      return false;
    }
    if (lhs.item.is_dict() != rhs.item.is_dict()) {
      return false;
    }
    if (lhs.item.is_schema() != rhs.item.is_schema()) {
      return false;
    }
    return true;
  }

 private:
  DeepDiff result_;
};

}  // namespace

absl::StatusOr<DataSliceImpl> DeepEquivalentOp::operator()(
    const DataSliceImpl& lhs_ds, const DataItem& lhs_schema,
    const DataBagImpl& lhs_databag, DataBagImpl::FallbackSpan lhs_fallbacks,
    const DataSliceImpl& rhs_ds, const DataItem& rhs_schema,
    const DataBagImpl& rhs_databag,
    DataBagImpl::FallbackSpan rhs_fallbacks) const {
  auto comparator = std::make_unique<EquivalentComparator>(
      DataBagImplPtr::NewRef(new_databag_));
  auto lhs_traverse_helper = TraverseHelper(lhs_databag, lhs_fallbacks);
  auto rhs_traverse_helper = TraverseHelper(rhs_databag, rhs_fallbacks);
  auto compare_op = DeepComparator<EquivalentComparator>(
      std::move(lhs_traverse_helper), std::move(rhs_traverse_helper),
      std::move(comparator));
  return compare_op.CompareSlices(lhs_ds, lhs_schema, rhs_ds, rhs_schema);
}

absl::StatusOr<DataItem> DeepEquivalentOp::operator()(
    const DataItem& lhs_item, const DataItem& lhs_schema,
    const DataBagImpl& lhs_databag, DataBagImpl::FallbackSpan lhs_fallbacks,
    const DataItem& rhs_item, const DataItem& rhs_schema,
    const DataBagImpl& rhs_databag,
    DataBagImpl::FallbackSpan rhs_fallbacks) const {
  // For DataItems, we adapt the DataSliceImpl interface.
  ASSIGN_OR_RETURN(auto result,
                   this->operator()(DataSliceImpl::Create(1, lhs_item),
                                    lhs_schema, lhs_databag, lhs_fallbacks,
                                    DataSliceImpl::Create(1, rhs_item),
                                    rhs_schema, rhs_databag, rhs_fallbacks));
  return result[0];
}

absl::StatusOr<std::vector<DeepEquivalentOp::DiffItem>>
DeepEquivalentOp::GetDiffPaths(const DataSliceImpl& ds, const DataItem& schema,
                               size_t max_count) const {
  auto traverse_helper = TraverseHelper(*new_databag_, {});
  std::vector<DiffItem> diff_paths;
  auto diff_uuid =
      CreateSchemaUuidFromFields(DeepDiff::kDiffWrapperSeed, {}, {});
  auto lambda_visitor = [&](const DataItem& item, const DataItem& schema,
                            absl::FunctionRef<std::string()> path) {
    if (schema == diff_uuid && diff_paths.size() < max_count) {
      diff_paths.push_back(
          {.path = std::string(path()), .item = item, .schema = schema});
    }
    return absl::OkStatus();
  };
  // We look for the diff items (have diff_uuid schema) in the newly
  // created databag.
  auto diff_finder = ObjectFinder(*new_databag_, {});
  RETURN_IF_ERROR(diff_finder.TraverseSlice(ds, schema, lambda_visitor));
  return diff_paths;
}

absl::StatusOr<std::vector<DeepEquivalentOp::DiffItem>>
DeepEquivalentOp::GetDiffPaths(const DataItem& item, const DataItem& schema,
                               size_t max_count) const {
  auto traverse_helper = TraverseHelper(*new_databag_, {});
  std::vector<DiffItem> diff_paths;
  auto diff_uuid =
      CreateSchemaUuidFromFields(DeepDiff::kDiffWrapperSeed, {}, {});
  auto lambda_visitor = [&](const DataItem& item, const DataItem& schema,
                            absl::FunctionRef<std::string()> path) {
    if (schema == diff_uuid && diff_paths.size() < max_count) {
      diff_paths.push_back(
          {.path = std::string(path()), .item = item, .schema = schema});
    }
    return absl::OkStatus();
  };
  auto diff_finder = ObjectFinder(*new_databag_, {});
  RETURN_IF_ERROR(diff_finder.TraverseSlice(item, schema, lambda_visitor));
  return diff_paths;
}

}  // namespace koladata::internal
