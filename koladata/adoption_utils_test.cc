// Copyright 2024 Google LLC
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
#include "koladata/adoption_utils.h"

#include <optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/testing/matchers.h"

namespace koladata {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::testing::ElementsAre;

TEST(AdoptionQueueTest, Empty) {
  AdoptionQueue q;
  EXPECT_TRUE(q.empty());
  EXPECT_THAT(q.GetDbOrMerge(), IsOkAndHolds(nullptr));
  auto db1 = DataBag::Empty();
  auto db2 = DataBag::Empty();
  EXPECT_OK(q.AdoptInto(*db1));
  EXPECT_THAT(db1->GetImpl(), internal::testing::DataBagEqual(db2->GetImpl()));
}

TEST(AdoptionQueueTest, Single) {
  internal::DataItem obj(internal::AllocateSingleObject());
  auto db1 = DataBag::Empty();
  auto db2 = DataBag::Empty();
  auto db3 = DataBag::Empty();
  ASSERT_OK(
      db1->GetMutableImpl()->get().SetAttr(obj, "a", internal::DataItem(1)));
  ASSERT_OK(
      db2->GetMutableImpl()->get().SetAttr(obj, "b", internal::DataItem(2)));
  ASSERT_OK(
      db2->GetMutableImpl()->get().SetAttr(obj, "c", internal::DataItem(3)));

  ASSERT_OK_AND_ASSIGN(
      DataSlice slice,
      DataSlice::Create(obj, internal::DataItem(schema::kAny), db2));

  AdoptionQueue q;
  q.Add(slice);
  EXPECT_THAT(q.GetDbOrMerge(), IsOkAndHolds(db2));
  q.Add(nullptr);
  EXPECT_THAT(q.GetDbOrMerge(), IsOkAndHolds(db2));
  q.Add(db3);
  ASSERT_OK_AND_ASSIGN(auto db_res, q.GetDbOrMerge());
  EXPECT_THAT(db_res->GetImpl().GetAttr(obj, "a"), IsOkAndHolds(std::nullopt));
  EXPECT_THAT(db_res->GetImpl().GetAttr(obj, "b"),
              IsOkAndHolds(internal::DataItem(2)));
  EXPECT_THAT(db_res->GetImpl().GetAttr(obj, "c"),
              IsOkAndHolds(internal::DataItem(3)));

  ASSERT_OK(q.AdoptInto(*db1));
  EXPECT_THAT(db1->GetImpl().GetAttr(obj, "a"),
              IsOkAndHolds(internal::DataItem(1)));
  EXPECT_THAT(db1->GetImpl().GetAttr(obj, "b"),
              IsOkAndHolds(internal::DataItem(2)));
  EXPECT_THAT(db1->GetImpl().GetAttr(obj, "c"),
              IsOkAndHolds(internal::DataItem(3)));
}

TEST(AdoptionQueueTest, GetDbOrMerge) {
  internal::DataItem obj(internal::AllocateSingleObject());
  auto db1 = DataBag::Empty();
  auto db2 = DataBag::Empty();
  ASSERT_OK(
      db1->GetMutableImpl()->get().SetAttr(obj, "a", internal::DataItem(1)));
  ASSERT_OK(
      db2->GetMutableImpl()->get().SetAttr(obj, "b", internal::DataItem(2)));

  ASSERT_OK_AND_ASSIGN(
      DataSlice slice1,
      DataSlice::Create(obj, internal::DataItem(schema::kAny), db1));
  ASSERT_OK_AND_ASSIGN(
      DataSlice slice2,
      DataSlice::Create(obj, internal::DataItem(schema::kAny), db2));

  AdoptionQueue q;
  q.Add(slice1);
  q.Add(slice2);
  q.Add(slice1);
  ASSERT_OK_AND_ASSIGN(DataBagPtr db3, q.GetDbOrMerge());

  EXPECT_THAT(db3->GetImpl().GetAttr(obj, "a"),
              IsOkAndHolds(internal::DataItem(1)));
  EXPECT_THAT(db3->GetImpl().GetAttr(obj, "b"),
              IsOkAndHolds(internal::DataItem(2)));
}

TEST(AdoptionQueueTest, WithFallbacks) {
  internal::DataItem obj(internal::AllocateSingleObject());
  auto db1 = DataBag::Empty();
  auto db2 = DataBag::Empty();
  auto db3 = DataBag::Empty();
  ASSERT_OK(
      db1->GetMutableImpl()->get().SetAttr(obj, "a", internal::DataItem(1)));
  ASSERT_OK(
      db2->GetMutableImpl()->get().SetAttr(obj, "b", internal::DataItem(2)));
  ASSERT_OK(  // conflicts with db2
      db3->GetMutableImpl()->get().SetAttr(obj, "b", internal::DataItem(4.0f)));
  ASSERT_OK(
      db3->GetMutableImpl()->get().SetAttr(obj, "c", internal::DataItem(3.0f)));

  DataBagPtr db_with_fallbacks =
      DataBag::ImmutableEmptyWithFallbacks({db2, db3});

  ASSERT_OK_AND_ASSIGN(
      DataSlice slice1,
      DataSlice::Create(obj, internal::DataItem(schema::kAny), db1));
  ASSERT_OK_AND_ASSIGN(DataSlice slice2,
                       DataSlice::Create(obj, internal::DataItem(schema::kAny),
                                         db_with_fallbacks));

  AdoptionQueue q;
  q.Add(slice1);
  q.Add(slice2);
  q.Add(slice1);

  ASSERT_OK(q.AdoptInto(*db1));

  EXPECT_THAT(db1->GetImpl().GetAttr(obj, "a"),
              IsOkAndHolds(internal::DataItem(1)));
  EXPECT_THAT(db1->GetImpl().GetAttr(obj, "b"),
              IsOkAndHolds(internal::DataItem(2)));
  EXPECT_THAT(db1->GetImpl().GetAttr(obj, "c"),
              IsOkAndHolds(internal::DataItem(3.0f)));
}

TEST(AdoptionQueueTest, TestDbVector) {
  auto db1 = DataBag::Empty();
  DataBagPtr db2;
  auto db3 = DataBag::Empty();
  AdoptionQueue q;
  q.Add(db1);
  q.Add(db2);
  q.Add(db3);

  EXPECT_THAT(q.bags(), ElementsAre(db1, db3));
}

}  // namespace
}  // namespace koladata
