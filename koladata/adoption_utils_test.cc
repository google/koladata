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
#include "koladata/object_factories.h"
#include "koladata/test_utils.h"

namespace koladata {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::testing::UnorderedElementsAre;

TEST(AdoptionQueueTest, Empty) {
  AdoptionQueue q;
  EXPECT_TRUE(q.empty());
  EXPECT_THAT(q.GetCommonOrMergedDb(), IsOkAndHolds(nullptr));
  auto db1 = DataBag::Empty();
  auto db2 = DataBag::Empty();
  EXPECT_OK(q.AdoptInto(*db1));
  EXPECT_THAT(db1->GetImpl(), internal::testing::DataBagEqual(db2->GetImpl()));
}

TEST(AdoptionQueueTest, Single) {
  ASSERT_OK_AND_ASSIGN(auto int32_schema,
                       DataSlice::Create(internal::DataItem(schema::kInt32),
                                         internal::DataItem(schema::kSchema)));
  auto schema_db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice schema,
      CreateEntitySchema(schema_db, {"a", "b", "c"},
                         {int32_schema, int32_schema, int32_schema}));

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
  ASSERT_OK_AND_ASSIGN(slice, slice.SetSchema(schema));

  AdoptionQueue q;
  q.Add(slice);
  EXPECT_THAT(q.GetCommonOrMergedDb(), IsOkAndHolds(db2));
  q.Add(nullptr);
  EXPECT_THAT(q.GetCommonOrMergedDb(), IsOkAndHolds(db2));
  q.Add(db3);
  ASSERT_OK_AND_ASSIGN(auto db_res, q.GetCommonOrMergedDb());
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

TEST(AdoptionQueueTest, GetCommonOrMergedDb) {
  auto schema_db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice schema,
                       CreateEntitySchema(schema_db, {"a", "b"},
                                          {test::Schema(schema::kInt32),
                                           test::Schema(schema::kInt32)}));

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
  ASSERT_OK_AND_ASSIGN(slice1, slice1.SetSchema(schema));
  ASSERT_OK_AND_ASSIGN(
      DataSlice slice2,
      DataSlice::Create(obj, internal::DataItem(schema::kAny), db2));
  ASSERT_OK_AND_ASSIGN(slice2, slice2.SetSchema(schema));

  AdoptionQueue q;
  q.Add(slice1);
  q.Add(slice2);
  q.Add(slice1);
  ASSERT_OK_AND_ASSIGN(DataBagPtr db3, q.GetCommonOrMergedDb());
  EXPECT_FALSE(db3->IsMutable());

  EXPECT_THAT(db3->GetImpl().GetAttr(obj, "a"),
              IsOkAndHolds(internal::DataItem(1)));
  EXPECT_THAT(db3->GetImpl().GetAttr(obj, "b"),
              IsOkAndHolds(internal::DataItem(2)));
}

TEST(AdoptionQueueTest, WithFallbacks) {
  auto schema_db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(DataSlice schema,
                       CreateEntitySchema(schema_db, {"a", "b", "c"},
                                          {test::Schema(schema::kInt32),
                                           test::Schema(schema::kInt32),
                                           test::Schema(schema::kFloat32)}));

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
      DataBag::ImmutableEmptyWithFallbacks({db2, db3, schema_db});

  ASSERT_OK_AND_ASSIGN(
      DataSlice slice1,
      DataSlice::Create(obj, internal::DataItem(schema::kAny), db1));
  ASSERT_OK_AND_ASSIGN(slice1, slice1.SetSchema(schema));
  ASSERT_OK_AND_ASSIGN(DataSlice slice2,
                       DataSlice::Create(obj, internal::DataItem(schema::kAny),
                                         db_with_fallbacks));
  ASSERT_OK_AND_ASSIGN(slice2,
                       slice2.WithSchema(schema.WithDb(db_with_fallbacks)));

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

  EXPECT_THAT(q.GetDbWithFallbacks()->GetFallbacks(),
              UnorderedElementsAre(db1, db3));
}

TEST(AdoptionQueueTest, Extraction) {
  auto db1 = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice schema1,
      CreateEntitySchema(db1, {"a"}, {test::Schema(schema::kInt32)}));
  auto& db1_impl = db1->GetMutableImpl()->get();
  internal::DataItem obj11(internal::AllocateSingleObject());
  internal::DataItem obj12(internal::AllocateSingleObject());
  internal::DataItem obj13(internal::AllocateSingleObject());
  ASSERT_OK(db1_impl.SetAttr(obj11, "a", internal::DataItem(11)));
  ASSERT_OK(db1_impl.SetAttr(obj12, "a", internal::DataItem(12)));
  ASSERT_OK(db1_impl.SetAttr(obj13, "a", internal::DataItem(13)));

  auto db2 = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      DataSlice schema2,
      CreateEntitySchema(db2, {"a"}, {test::Schema(schema::kInt32)}));
  auto& db2_impl = db2->GetMutableImpl()->get();
  internal::DataItem obj21(internal::AllocateSingleObject());
  internal::DataItem obj22(internal::AllocateSingleObject());
  ASSERT_OK(db2_impl.SetAttr(obj21, "a", internal::DataItem(21)));
  ASSERT_OK(db2_impl.SetAttr(obj22, "a", internal::DataItem(22)));

  ASSERT_OK_AND_ASSIGN(
      DataSlice slice11,
      DataSlice::Create(obj11, schema1.item(), db1));
  ASSERT_OK_AND_ASSIGN(
      DataSlice slice13,
      DataSlice::Create(obj13, schema1.item(), db1));
  ASSERT_OK_AND_ASSIGN(
      DataSlice slice21,
      DataSlice::Create(obj21, schema2.item(), db2));

  AdoptionQueue adoption_queue;
  adoption_queue.Add(slice11);
  adoption_queue.Add(slice21);
  adoption_queue.Add(slice13);

  ASSERT_OK_AND_ASSIGN(DataBagPtr db3, adoption_queue.GetCommonOrMergedDb());

  EXPECT_THAT(db3->GetImpl().GetAttr(obj11, "a"),
              IsOkAndHolds(internal::DataItem(11)));
  EXPECT_THAT(db3->GetImpl().GetAttr(obj12, "a"),
              IsOkAndHolds(internal::DataItem()));  // Not extracted.
  EXPECT_THAT(db3->GetImpl().GetAttr(obj13, "a"),
              IsOkAndHolds(internal::DataItem(13)));
  EXPECT_THAT(db3->GetImpl().GetAttr(obj21, "a"),
              IsOkAndHolds(internal::DataItem(21)));
  EXPECT_THAT(db3->GetImpl().GetAttr(obj22, "a"),
              IsOkAndHolds(internal::DataItem()));  // Not extracted.
}

}  // namespace
}  // namespace koladata
