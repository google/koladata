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
#ifndef KOLADATA_INTERNAL_OP_UTILS_DEEP_DIFF_H_
#define KOLADATA_INTERNAL_OP_UTILS_DEEP_DIFF_H_

#include <string_view>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/op_utils/traverse_helper.h"

namespace koladata::internal {

// A builder class for the DataBagImpl that represents the result of
// DeepComparator operation.
// Such a result should replicate the structure of the common parts of the
// original slices, and have a special nodes when there is a difference.
//
// Example of a node for diff between DataItem(2) and DataItem(3) is:
//  kd.obj(
//    kd.new(
//      diff=kd.obj(lhs_value=2, rhs_value=3), schema=diff_wrapper_schema))
// Where diff_wrapper_schema is:
//  kd.uu_schema(seed='__diff_wrapper_schema__').with_attrs(diff=kd.OBJECT)
class DeepDiff {
 public:
  static constexpr std::string_view kLhsAttr = "lhs_value";
  static constexpr std::string_view kRhsAttr = "rhs_value";
  static constexpr std::string_view kDiffItemAttr = "diff";
  static constexpr std::string_view kDiffWrapperSeed =
      "__diff_wrapper_schema__";

  explicit DeepDiff(DataBagImplPtr databag) : databag_(std::move(databag)) {}

  // Returns a "cloned" version of the given item.
  //
  // Primitives are returned unchanged. For an ObjectId, a new ObjectId is
  // allocated with the same kListFlag/kDictFlag flags.
  absl::StatusOr<DataItem> CreateTokenLike(DataItem item);

  // Creates a transition in the result DataBag, that can be traversed from a
  // given token with the provided key, and leads to the given value.
  absl::Status SaveTransition(DataItem token, TraverseHelper::TransitionKey key,
                              DataItem value);

  // Saves a diff node, representing lhs-only attribute.
  absl::Status LhsOnlyAttribute(DataItem token,
                                TraverseHelper::TransitionKey key,
                                TraverseHelper::Transition lhs);

  // Saves a diff node, representing rhs-only attribute.
  absl::Status RhsOnlyAttribute(DataItem token,
                                TraverseHelper::TransitionKey key,
                                TraverseHelper::Transition rhs);

  // Saves a diff node, representing a mismatch between lhs and rhs.
  absl::Status LhsRhsMismatch(DataItem token, TraverseHelper::TransitionKey key,
                              TraverseHelper::Transition lhs,
                              TraverseHelper::Transition rhs);

 private:
  // Creates an Object representing an lhs-only attribute.
  absl::StatusOr<DataItem> CreateLhsOnlyDiffItem(
      TraverseHelper::Transition lhs);

  // Creates an Object representing an rhs-only attribute.
  absl::StatusOr<DataItem> CreateRhsOnlyDiffItem(
      TraverseHelper::Transition rhs);

  // Creates an Object representing a mismatch between lhs and rhs.
  absl::StatusOr<DataItem> CreateMismatchDiffItem(
      TraverseHelper::Transition lhs, TraverseHelper::Transition rhs);

  // Wraps a diff item with an Object with a diff wrapper uuid schema.
  absl::StatusOr<DataItem> CreateDiffWrapper(DataItem diff_item);

  DataBagImplPtr databag_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_DEEP_DIFF_H_
