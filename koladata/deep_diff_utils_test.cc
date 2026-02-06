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

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/deep_diff.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/testing/deep_op_utils.h"
#include "koladata/test_utils.h"

namespace koladata {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::arolla::Text;
using ::koladata::internal::DataBagImpl;
using ::koladata::internal::DataItem;
using ::koladata::internal::DeepDiff;
using ::koladata::internal::TraverseHelper;

TEST(DeepDiffUtilsTest, GetPathRepr_Empty) {
  std::vector<TraverseHelper::TransitionKey> path;
  EXPECT_EQ(GetPathRepr(path, "lhs"), "lhs");
}

TEST(DeepDiffUtilsTest, GetPathRepr_ListItem) {
  std::vector<TraverseHelper::TransitionKey> path = {
      {TraverseHelper::TransitionType::kListItem, 5}};
  EXPECT_EQ(GetPathRepr(path, "lhs"), "item at position 5 in list lhs");
}

TEST(DeepDiffUtilsTest, GetPathRepr_DictValue) {
  std::vector<TraverseHelper::TransitionKey> path = {
      {TraverseHelper::TransitionType::kDictValue, -1,
       DataItem(Text("key_name"))}};
  EXPECT_EQ(GetPathRepr(path, "lhs"), "key 'key_name' in lhs");
}

TEST(DeepDiffUtilsTest, GetPathRepr_Attribute) {
  std::vector<TraverseHelper::TransitionKey> path = {
      {TraverseHelper::TransitionType::kAttributeName, -1,
       DataItem(Text("attr"))}};
  EXPECT_EQ(GetPathRepr(path, "lhs"), "lhs.attr");
}

TEST(DeepDiffUtilsTest, GetPathRepr_MultiStep) {
  std::vector<TraverseHelper::TransitionKey> path = {
      {TraverseHelper::TransitionType::kAttributeName, -1,
       DataItem(Text("attr"))},
      {TraverseHelper::TransitionType::kListItem, 2}};
  EXPECT_EQ(GetPathRepr(path, "lhs"), "item at position 2 in list lhs.attr");
}

TEST(DeepDiffUtilsTest, GetPathRepr_MultiStep_Attribute) {
  std::vector<TraverseHelper::TransitionKey> path = {
      {TraverseHelper::TransitionType::kListItem, 2},
      {TraverseHelper::TransitionType::kAttributeName, -1,
       DataItem(Text("attr"))}};
  EXPECT_EQ(GetPathRepr(path, "lhs"), "lhs[2].attr");
}

TEST(DeepDiffUtilsTest, SchemaMismatchRepr_Root) {
  std::vector<TraverseHelper::TransitionKey> path = {
      {TraverseHelper::TransitionType::kSchema}};
  auto ds_int = test::DataItem(1);
  auto ds_float = test::DataItem(1.0f);
  EXPECT_THAT(SchemaMismatchRepr(path, ds_int, "lhs", ds_float, "rhs", {}),
              IsOkAndHolds("modified schema:\nDataItem(FLOAT32, schema: "
                           "SCHEMA)\n-> DataItem(INT32, schema: SCHEMA)"));
}

TEST(DeepDiffUtilsTest, SchemaMismatchRepr_Attribute) {
  std::vector<TraverseHelper::TransitionKey> path = {
      {TraverseHelper::TransitionType::kAttributeName, -1,
       DataItem(Text("a"))}};
  auto ds_int = test::DataItem(1);
  auto ds_float = test::DataItem(1.0f);
  EXPECT_THAT(
      SchemaMismatchRepr(path, ds_int, "lhs", ds_float, "rhs", {}),
      IsOkAndHolds("modified schema:\nrhs.a:\nDataItem(FLOAT32, schema: "
                   "SCHEMA)\n-> lhs.a:\nDataItem(INT32, schema: SCHEMA)"));
}

class DeepDiffUtilsTest : public internal::testing::deep_op_utils::DeepOpTest {
};

INSTANTIATE_TEST_SUITE_P(
    MainOrFallback, DeepDiffUtilsTest,
    ::testing::ValuesIn(internal::testing::deep_op_utils::test_param_values));

TEST_P(DeepDiffUtilsTest, DiffItemRepr_Modified) {
  auto result_db_impl = DataBagImpl::CreateEmptyDatabag();
  auto lhs_db = DataBag::EmptyMutable();
  auto rhs_db = DataBag::EmptyMutable();
  auto lhs_db_impl = lhs_db->GetMutableImpl();
  auto rhs_db_impl = rhs_db->GetMutableImpl();

  auto ds = AllocateEmptyObjects(3);
  auto token = ds[0];
  auto lhs = TraverseHelper::Transition(DataItem(1), DataItem(schema::kInt32));
  auto rhs = TraverseHelper::Transition(DataItem(2), DataItem(schema::kInt32));
  auto key = TraverseHelper::TransitionKey(
      TraverseHelper::TransitionType::kAttributeName, -1, DataItem(Text("a")));

  DeepDiff diff(result_db_impl);
  EXPECT_OK(
      diff.LhsRhsMismatch(token, key, lhs, rhs, /*is_schema_mismatch=*/false));
  ASSERT_OK_AND_ASSIGN(
      auto diff_wrapper,
      result_db_impl->GetAttr(token, key.value.value<arolla::Text>()));
  ASSERT_OK_AND_ASSIGN(
      auto diff_wrapper_schema,
      result_db_impl->GetAttr(diff_wrapper, schema::kSchemaAttr));

  DeepDiff::DiffItem diff_item = {
      .path = {{TraverseHelper::TransitionType::kAttributeName, -1,
                DataItem(Text("attr"))}},
      .item = diff_wrapper,
      .schema = diff_wrapper_schema};

  EXPECT_THAT(
      DiffItemRepr(diff_item, *result_db_impl, lhs_db, DeepDiff::kLhsAttr,
                   "lhs", rhs_db, DeepDiff::kRhsAttr, "rhs", {}),
      IsOkAndHolds(absl::StrFormat(
          "modified:\nrhs.attr:\nDataItem(2, schema: INT32, bag_id: %v)\n-> "
          "lhs.attr:\nDataItem(1, schema: INT32, bag_id: %v)",
          GetBagIdRepr(rhs_db), GetBagIdRepr(lhs_db))));

  EXPECT_THAT(
      DiffItemRepr(diff_item, *result_db_impl, rhs_db, DeepDiff::kRhsAttr,
                   "rhs", lhs_db, DeepDiff::kLhsAttr, "lhs", {}),
      IsOkAndHolds(absl::StrFormat(
          "modified:\nlhs.attr:\nDataItem(1, schema: INT32, bag_id: %v)\n-> "
          "rhs.attr:\nDataItem(2, schema: INT32, bag_id: %v)",
          GetBagIdRepr(lhs_db), GetBagIdRepr(rhs_db))));
}

TEST_P(DeepDiffUtilsTest, DiffItemRepr_Added) {
  auto result_db_impl = DataBagImpl::CreateEmptyDatabag();
  auto lhs_db = DataBag::EmptyMutable();
  auto rhs_db = DataBag::EmptyMutable();
  auto lhs_db_impl = lhs_db->GetMutableImpl();
  auto rhs_db_impl = rhs_db->GetMutableImpl();

  auto ds = AllocateEmptyObjects(3);
  auto token = ds[0];
  auto lhs = TraverseHelper::Transition(DataItem(1), DataItem(schema::kInt32));
  auto key = TraverseHelper::TransitionKey(
      TraverseHelper::TransitionType::kAttributeName, -1, DataItem(Text("a")));

  DeepDiff diff(result_db_impl);
  EXPECT_OK(
      diff.LhsOnlyAttribute(token, key, lhs));
  ASSERT_OK_AND_ASSIGN(
      auto diff_wrapper,
      result_db_impl->GetAttr(token, key.value.value<arolla::Text>()));
  ASSERT_OK_AND_ASSIGN(
      auto diff_wrapper_schema,
      result_db_impl->GetAttr(diff_wrapper, schema::kSchemaAttr));

  DeepDiff::DiffItem diff_item = {
      .path = {{TraverseHelper::TransitionType::kAttributeName, -1,
                DataItem(Text("attr"))}},
      .item = diff_wrapper,
      .schema = diff_wrapper_schema};

  EXPECT_THAT(
      DiffItemRepr(diff_item, *result_db_impl, lhs_db, DeepDiff::kLhsAttr,
                   "lhs", rhs_db, DeepDiff::kRhsAttr, "rhs", {}),
      IsOkAndHolds(absl::StrFormat(
          "added:\nlhs.attr:\nDataItem(1, schema: INT32, bag_id: %v)",
          GetBagIdRepr(lhs_db))));

  EXPECT_THAT(
      DiffItemRepr(diff_item, *result_db_impl, rhs_db, DeepDiff::kRhsAttr,
                   "rhs", lhs_db, DeepDiff::kLhsAttr, "lhs", {}),
      IsOkAndHolds(absl::StrFormat(
          "deleted:\nlhs.attr:\nDataItem(1, schema: INT32, bag_id: %v)",
          GetBagIdRepr(lhs_db))));
}


}  // namespace
}  // namespace koladata
