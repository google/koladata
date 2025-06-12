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
#include "koladata/alloc_utils.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/test_utils.h"

namespace koladata {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::testing::Eq;
using ::testing::Property;

TEST(AllocateLikeTest, ItemId_Item) {
  auto shape_and_mask_from = test::DataItem(1);
  ASSERT_OK_AND_ASSIGN(
      auto res,
      AllocateLike(shape_and_mask_from, internal::AllocateSingleObject,
                   internal::Allocate, internal::DataItem(schema::kObject)));
  EXPECT_TRUE(res.item().holds_value<internal::ObjectId>());
  EXPECT_EQ(res.GetSchemaImpl(), schema::kObject);
  EXPECT_EQ(res.GetBag(), nullptr);

  shape_and_mask_from = test::DataItem(internal::DataItem());
  EXPECT_THAT(
      AllocateLike(shape_and_mask_from, internal::AllocateSingleObject,
                   internal::Allocate, internal::DataItem(schema::kObject)),
      IsOkAndHolds(
          AllOf(Property(&DataSlice::item, Eq(shape_and_mask_from.item())),
                Property(&DataSlice::GetSchemaImpl, Eq(schema::kObject)),
                Property(&DataSlice::GetBag, Eq(nullptr)))));
}

TEST(AllocateLikeTest, ItemId_Slice) {
  auto shape_and_mask_from =
      test::DataSlice<int>({1, std::nullopt, 3, 4, 5, std::nullopt, 7, 8});
  auto res = AllocateLike(shape_and_mask_from,
                          internal::AllocateSingleObject,
                          internal::Allocate,
                          internal::DataItem(schema::kObject));
  EXPECT_THAT(res, IsOkAndHolds(AllOf(
                       Property(&DataSlice::GetSchemaImpl, Eq(schema::kObject)),
                       Property(&DataSlice::GetBag, Eq(nullptr)))));

  EXPECT_EQ(res->slice().allocation_ids().size(), 1);
  auto alloc_id = *res->slice().allocation_ids().begin();
  EXPECT_EQ(res->slice()[0], alloc_id.ObjectByOffset(0));
  EXPECT_EQ(res->slice()[1], internal::DataItem());  // missing.
  EXPECT_EQ(res->slice()[2], alloc_id.ObjectByOffset(1));
  EXPECT_EQ(res->slice()[3], alloc_id.ObjectByOffset(2));
  EXPECT_EQ(res->slice()[4], alloc_id.ObjectByOffset(3));
  EXPECT_EQ(res->slice()[5], internal::DataItem());  // missing.
  EXPECT_EQ(res->slice()[6], alloc_id.ObjectByOffset(4));
  EXPECT_EQ(res->slice()[7], alloc_id.ObjectByOffset(5));
}

TEST(AllocateLikeTest, SpecializedItemId_Item) {
  auto db = DataBag::Empty();
  auto shape_and_mask_from = test::DataItem(1);
  auto res = AllocateLike(shape_and_mask_from,
                          internal::AllocateSingleList,
                          internal::AllocateLists,
                          internal::DataItem(schema::kItemId), db);
  EXPECT_THAT(res, IsOkAndHolds(AllOf(
                       Property(&DataSlice::GetSchemaImpl, Eq(schema::kItemId)),
                       Property(&DataSlice::GetBag, Eq(db)))));
  EXPECT_TRUE(res->item().value<internal::ObjectId>().IsList());
}

TEST(AllocateLikeTest, SpecializedItemId_Slice) {
  auto db = DataBag::Empty();
  auto shape_and_mask_from = test::DataSlice<int>({1, std::nullopt, 3, 4});
  auto res = AllocateLike(shape_and_mask_from,
                          internal::AllocateSingleDict,
                          internal::AllocateDicts,
                          internal::DataItem(schema::kItemId), db);
  EXPECT_THAT(res, IsOkAndHolds(AllOf(
                       Property(&DataSlice::GetSchemaImpl, Eq(schema::kItemId)),
                       Property(&DataSlice::GetBag, Eq(db)))));

  EXPECT_EQ(res->slice().allocation_ids().size(), 1);
  auto alloc_id = *res->slice().allocation_ids().begin();
  EXPECT_EQ(res->slice()[0], alloc_id.ObjectByOffset(0));
  EXPECT_TRUE(res->slice()[0].value<internal::ObjectId>().IsDict());
  EXPECT_EQ(res->slice()[1], internal::DataItem());  // missing.
  EXPECT_EQ(res->slice()[2], alloc_id.ObjectByOffset(1));
  EXPECT_TRUE(res->slice()[2].value<internal::ObjectId>().IsDict());
  EXPECT_EQ(res->slice()[3], alloc_id.ObjectByOffset(2));
  EXPECT_TRUE(res->slice()[3].value<internal::ObjectId>().IsDict());
}

}  // namespace
}  // namespace koladata
