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
#include <array>
#include <cstdint>
#include <initializer_list>
#include <numeric>
#include <optional>
#include <tuple>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/test_utils.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::DenseArrayEdge;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;

TEST(DataBagTest, EmptyAndUnknownLists) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto empty = DataSliceImpl::CreateEmptyAndUnknownType(3);
  EXPECT_THAT(
      db->GetListSize(empty),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  EXPECT_OK(db->AppendToList(empty, empty));
  EXPECT_THAT(
      db->GetFromLists(empty, arolla::CreateEmptyDenseArray<int64_t>(3)),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  EXPECT_OK(
      db->SetInLists(empty, arolla::CreateEmptyDenseArray<int64_t>(3), empty));
  EXPECT_OK(db->RemoveInList(empty, arolla::CreateEmptyDenseArray<int64_t>(3)));
  EXPECT_THAT(
      db->PopFromLists(empty, arolla::CreateEmptyDenseArray<int64_t>(3)),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  EXPECT_THAT(
      db->GetFromLists(empty, arolla::CreateEmptyDenseArray<int64_t>(3)),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  {
    ASSERT_OK_AND_ASSIGN((auto [values, edge]), db->ExplodeLists(empty));
    EXPECT_EQ(values.size(), 0);
    EXPECT_EQ(edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 0, 0, 0));
    EXPECT_OK(db->ExtendLists(empty, values, edge));
    EXPECT_OK(
        db->ReplaceInLists(empty, DataBagImpl::ListRange(), values, edge));
  }
}

TEST(DataBagTest, AppendToLists) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateLists(2);
  ObjectId list_from_other_alloc = AllocateSingleList();
  DataSliceImpl lists =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
          {alloc_id.ObjectByOffset(1), alloc_id.ObjectByOffset(0),
           list_from_other_alloc}));

  EXPECT_THAT(db->GetListSize(lists), IsOkAndHolds(ElementsAre(0, 0, 0)));
  ASSERT_OK(db->AppendToList(
      lists,
      DataSliceImpl::Create(
          arolla::CreateDenseArray<int64_t>({7, std::nullopt, std::nullopt}),
          arolla::CreateDenseArray<float>({std::nullopt, std::nullopt, 3.0}))));
  ASSERT_OK(db->AppendToList(
      DataSliceImpl::ObjectsFromAllocation(alloc_id, 1),
      DataSliceImpl::Create(arolla::CreateDenseArray<bool>({false}))));
  auto db2 = db->PartiallyPersistentFork();
  ASSERT_OK(db2->AppendToList(
      lists, DataSliceImpl::Create(
                 arolla::CreateDenseArray<int>({1, 2, std::nullopt}))));
  auto db3 = db2->PartiallyPersistentFork();

  EXPECT_THAT(db->GetListSize(lists), IsOkAndHolds(ElementsAre(1, 2, 1)));
  EXPECT_THAT(db2->GetListSize(lists), IsOkAndHolds(ElementsAre(2, 3, 2)));
  EXPECT_THAT(db3->GetListSize(lists), IsOkAndHolds(ElementsAre(2, 3, 2)));

  EXPECT_THAT(
      db2->GetFromLists(lists, arolla::CreateDenseArray<int64_t>({0, 0, 0})),
      IsOkAndHolds(ElementsAre(int64_t{7}, DataItem(), 3.0f)));
  EXPECT_THAT(
      db2->GetFromLists(lists, arolla::CreateDenseArray<int64_t>({-1, 1, 0})),
      IsOkAndHolds(ElementsAre(1, false, 3.0f)));
  EXPECT_THAT(db2->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(3.0, DataItem())));

  ASSERT_OK(
      db3->AppendToList(lists, DataSliceImpl::CreateEmptyAndUnknownType(3)));
  EXPECT_THAT(db3->GetListSize(lists), IsOkAndHolds(ElementsAre(3, 4, 3)));
}

TEST(DataBagTest, RemoveInLists) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateLists(3);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc_id, 3);

  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(0)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {1, 2, std::nullopt}))));
  ASSERT_OK(db->ExtendList(
      DataItem(alloc_id.ObjectByOffset(1)),
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({4, 6}))));
  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(2)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {std::nullopt, 8, 9}))));

  // Remove [-1:]
  ASSERT_OK(db->RemoveInList(lists, DataBagImpl::ListRange(-1)));
  EXPECT_THAT(db->ExplodeList(lists[0]), IsOkAndHolds(ElementsAre(1, 2)));
  EXPECT_THAT(db->ExplodeList(lists[1]), IsOkAndHolds(ElementsAre(4)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(std::nullopt, 8)));

  // Remove [1:0]
  ASSERT_OK(db->RemoveInList(lists, DataBagImpl::ListRange(1, 0)));
  EXPECT_THAT(db->ExplodeList(lists[0]), IsOkAndHolds(ElementsAre(1, 2)));
  EXPECT_THAT(db->ExplodeList(lists[1]), IsOkAndHolds(ElementsAre(4)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(std::nullopt, 8)));
}

TEST(DataBagTest, SetAndGetInLists) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateLists(3);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc_id, 3);

  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(0)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {1, 2, std::nullopt}))));
  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(1)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {4, std::nullopt, 6}))));
  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(2)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {std::nullopt, 8, 9}))));

  EXPECT_THAT(
      db->GetFromLists(lists, arolla::CreateDenseArray<int64_t>({0, 1, -1})),
      IsOkAndHolds(ElementsAre(1, DataItem(), 9)));

  ASSERT_OK(db->SetInLists(
      lists, arolla::CreateDenseArray<int64_t>({0, -2, 2}),
      DataSliceImpl::Create(
          arolla::CreateDenseArray<float>({10.0f, 11.0f, 12.0f}))));

  EXPECT_THAT(
      db->GetFromLists(lists, arolla::CreateDenseArray<int64_t>({2, 1, 0})),
      IsOkAndHolds(ElementsAre(DataItem(), 11.0f, DataItem())));

  ASSERT_OK(db->SetInLists(lists, arolla::CreateDenseArray<int64_t>({0, -2, 2}),
                           DataSliceImpl::CreateEmptyAndUnknownType(3)));

  EXPECT_THAT(
      db->GetFromLists(lists, arolla::CreateDenseArray<int64_t>({2, 1, 0})),
      IsOkAndHolds(ElementsAre(DataItem(), DataItem(), DataItem())));
}

TEST(DataBagTest, ExplodeLists) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateLists(3);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc_id, 3);

  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(0)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {1, 2, std::nullopt}))));
  ASSERT_OK(
      db->AppendToList(DataItem(alloc_id.ObjectByOffset(1)), DataItem(4)));
  ASSERT_OK(
      db->AppendToList(DataItem(alloc_id.ObjectByOffset(1)), DataItem(6.0f)));
  ASSERT_OK(db->ExtendList(DataItem(alloc_id.ObjectByOffset(2)),
                           DataSliceImpl::Create(arolla::CreateDenseArray<int>(
                               {std::nullopt, 8, 9}))));

  {  // Slices [1:] from each list grouped together.
    ASSERT_OK_AND_ASSIGN((auto [values, edge]),
                         db->ExplodeLists(lists, DataBagImpl::ListRange(1)));

    EXPECT_THAT(values, ElementsAre(2, std::nullopt, 6.0f, 8, 9));

    ASSERT_EQ(edge.edge_type(), arolla::DenseArrayEdge::SPLIT_POINTS);
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 2, 3, 5));
  }

  {  // Slices [2:0] from each list grouped together.
    ASSERT_OK_AND_ASSIGN((auto [values, edge]),
                         db->ExplodeLists(lists, DataBagImpl::ListRange(2, 0)));

    EXPECT_THAT(values, ElementsAre());

    ASSERT_EQ(edge.edge_type(), arolla::DenseArrayEdge::SPLIT_POINTS);
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 0, 0, 0));
  }
}

TEST(DataBagTest, ExtendAndReplaceInLists) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateLists(3);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc_id, 3);

  {  // extend
    auto values = DataSliceImpl::Create(arolla::CreateDenseArray<int>(
        {2, std::nullopt, 8, 10, 11, 12}));
    auto edge = test::EdgeFromSplitPoints({0, 2, 3, 6});
    ASSERT_OK(db->ExtendLists(lists, values, edge));
  }

  {  // replace [0:0]
    auto values = DataSliceImpl::Create(
        arolla::CreateDenseArray<int>({1, std::nullopt, 9}));
    auto edge = test::EdgeFromSplitPoints({0, 1, 2, 3});
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(0, 0), values, edge));
  }

  EXPECT_THAT(db->ExplodeList(lists[0]),
              IsOkAndHolds(ElementsAre(1, 2, std::nullopt)));
  EXPECT_THAT(db->ExplodeList(lists[1]),
              IsOkAndHolds(ElementsAre(std::nullopt, 8)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(9, 10, 11, 12)));

  {  // replace [1:3]
    auto values = DataSliceImpl::Create(
        arolla::CreateDenseArray<int>({{}, 1, {}, {}}),
        arolla::CreateDenseArray<float>({3.0f, {}, 2.0f, {}}));
    auto edge = test::EdgeFromSplitPoints({0, 1, 3, 4});
    ASSERT_OK(db->ReplaceInLists(lists, DataBagImpl::ListRange(1, 3),
                                 values, edge));
  }

  EXPECT_THAT(db->ExplodeList(lists[0]), IsOkAndHolds(ElementsAre(1, 3.0f)));
  EXPECT_THAT(db->ExplodeList(lists[1]),
              IsOkAndHolds(ElementsAre(std::nullopt, 1, 2.0f)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(9, std::nullopt, 12)));

  {  // extend, duplicated list.
    auto lists2 = DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
        {alloc_id.ObjectByOffset(2), std::nullopt,
         alloc_id.ObjectByOffset(2)}));
    auto values = DataSliceImpl::Create(arolla::CreateDenseArray<arolla::Text>(
        {arolla::Text("aaa"), arolla::Text("bbb")}));
    auto edge = test::EdgeFromSplitPoints({0, 1, 1, 2});
    ASSERT_OK(db->ExtendLists(lists2, values, edge));
  }

  EXPECT_THAT(db->ExplodeList(lists[0]), IsOkAndHolds(ElementsAre(1, 3.0f)));
  EXPECT_THAT(db->ExplodeList(lists[1]),
              IsOkAndHolds(ElementsAre(std::nullopt, 1, 2.0f)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(9, std::nullopt, 12, arolla::Text("aaa"),
                                       arolla::Text("bbb"))));

  {  // replace [-1:] (last element)
    auto values =
        DataSliceImpl::Create(arolla::CreateConstDenseArray<int>(6, 57));
    auto edge = test::EdgeFromSplitPoints({0, 2, 4, 6});
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(-1), values, edge));
  }

  EXPECT_THAT(db->ExplodeList(lists[0]), IsOkAndHolds(ElementsAre(1, 57, 57)));
  EXPECT_THAT(db->ExplodeList(lists[1]),
              IsOkAndHolds(ElementsAre(std::nullopt, 1, 57, 57)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(9, std::nullopt, 12, arolla::Text("aaa"),
                                       57, 57)));

  {  // replace [1:] with 2 missing
    auto values = DataSliceImpl::CreateEmptyAndUnknownType(6);
    auto edge = test::EdgeFromSplitPoints({0, 2, 4, 6});
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(1), values, edge));
  }

  EXPECT_THAT(db->ExplodeList(lists[0]),
              IsOkAndHolds(ElementsAre(1, std::nullopt, std::nullopt)));
  EXPECT_THAT(
      db->ExplodeList(lists[1]),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(9, std::nullopt, std::nullopt)));

  {  // replace [1:0]
    auto values = DataSliceImpl::Create(
        arolla::CreateDenseArray<int>({2, 3, std::nullopt, 10}));
    auto edge = test::EdgeFromSplitPoints({0, 1, 3, 4});
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(1, 0), values, edge));
  }

  EXPECT_THAT(db->ExplodeList(lists[0]),
              IsOkAndHolds(ElementsAre(1, 2, std::nullopt, std::nullopt)));
  EXPECT_THAT(db->ExplodeList(lists[1]),
              IsOkAndHolds(ElementsAre(std::nullopt, 3, std::nullopt,
                                       std::nullopt, std::nullopt)));
  EXPECT_THAT(db->ExplodeList(lists[2]),
              IsOkAndHolds(ElementsAre(9, 10, std::nullopt, std::nullopt)));
}

TEST(DataBagTest, ReplaceInListsEmptyAndUnknown) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateLists(3);
  DataSliceImpl lists = DataSliceImpl::ObjectsFromAllocation(alloc_id, 3);
  auto empty_values = DataSliceImpl::CreateEmptyAndUnknownType(3);
  auto values = DataSliceImpl::Create(
      arolla::CreateDenseArray<int>({1, std::nullopt, 9}));
  auto edge = test::EdgeFromSplitPoints({0, 1, 2, 3});

  {  // replace [:]
    SCOPED_TRACE("replace [:]");
    ASSERT_OK(db->ReplaceInLists(lists, DataBagImpl::ListRange(), empty_values,
                                 edge));
    for (int i = 0; i < lists.size(); ++i) {
      EXPECT_THAT(db->ExplodeList(lists[i]),
                  IsOkAndHolds(ElementsAre(std::nullopt)))
          << i;
    }

    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(), values, edge));
    ASSERT_OK(db->ReplaceInLists(lists, DataBagImpl::ListRange(), empty_values,
                                 edge));
    for (int i = 0; i < lists.size(); ++i) {
      EXPECT_THAT(db->ExplodeList(lists[i]),
                  IsOkAndHolds(ElementsAre(std::nullopt)))
          << i;
    }
  }

  {  // replace [0:1]
    SCOPED_TRACE("replace [0:1]");
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(), values, edge));
    ASSERT_OK(db->ReplaceInLists(lists, DataBagImpl::ListRange(0, 1),
                                 empty_values, edge));
    for (int i = 0; i < lists.size(); ++i) {
      EXPECT_THAT(db->ExplodeList(lists[i]),
                  IsOkAndHolds(ElementsAre(std::nullopt)))
          << i;
    }
  }

  {  // replace [0:0]
    SCOPED_TRACE("replace [0:0]");
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(), values, edge));
    ASSERT_OK(db->ReplaceInLists(lists, DataBagImpl::ListRange(0, 0),
                                 empty_values, edge));
    for (int i = 0; i < lists.size(); ++i) {
      EXPECT_THAT(db->ExplodeList(lists[i]),
                  IsOkAndHolds(ElementsAre(std::nullopt, values[i])))
          << i;
    }
  }

  {  // replace [1:1]
    SCOPED_TRACE("replace [1:1]");
    ASSERT_OK(
        db->ReplaceInLists(lists, DataBagImpl::ListRange(), values, edge));
    ASSERT_OK(db->ReplaceInLists(lists, DataBagImpl::ListRange(1, 1),
                                 empty_values, edge));
    for (int i = 0; i < lists.size(); ++i) {
      EXPECT_THAT(db->ExplodeList(lists[i]),
                  IsOkAndHolds(ElementsAre(values[i], std::nullopt)))
          << i;
    }
  }
}

TEST(DataBagTest, SingleListOps) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  DataItem list(AllocateSingleList());

  EXPECT_THAT(db->GetListSize(list), IsOkAndHolds(DataItem(int64_t{0})));

  ASSERT_OK(db->AppendToList(list, DataItem(1.0f)));
  ASSERT_OK(db->ExtendList(
      list,
      DataSliceImpl::Create(arolla::CreateDenseArray<float>({2.0f, 3.0f}))));

  EXPECT_THAT(db->GetListSize(list), IsOkAndHolds(DataItem(int64_t{3})));
  EXPECT_THAT(db->ExplodeList(list),
              IsOkAndHolds(ElementsAre(1.0f, 2.0f, 3.0f)));

  // Remove [2:1]
  ASSERT_OK(db->RemoveInList(list, DataBagImpl::ListRange(2, 1)));
  EXPECT_THAT(db->GetListSize(list), IsOkAndHolds(DataItem(int64_t{3})));
  EXPECT_THAT(db->ExplodeList(list),
              IsOkAndHolds(ElementsAre(1.0f, 2.0f, 3.0f)));

  // Remove [0:2]
  ASSERT_OK(db->RemoveInList(list, DataBagImpl::ListRange(0, 2)));
  EXPECT_THAT(db->GetListSize(list), IsOkAndHolds(DataItem(int64_t{1})));
  EXPECT_THAT(db->ExplodeList(list), IsOkAndHolds(ElementsAre(3.0f)));

  // Remove [1:]
  ASSERT_OK(db->RemoveInList(list, DataBagImpl::ListRange(1)));
  EXPECT_THAT(db->GetListSize(list), IsOkAndHolds(DataItem(int64_t{1})));
  EXPECT_THAT(db->ExplodeList(list), IsOkAndHolds(ElementsAre(3.0f)));

  ASSERT_OK(db->ExtendList(
      list, DataSliceImpl::Create(arolla::CreateDenseArray<int>({7, 8, 9}))));
  ASSERT_OK(db->ReplaceInList(
      list, DataBagImpl::ListRange(2, 3),
      DataSliceImpl::Create(arolla::CreateDenseArray<arolla::Text>(
          {arolla::Text("aaa"), arolla::Text("bbb")}))));

  EXPECT_THAT(db->GetListSize(list), IsOkAndHolds(DataItem(int64_t{5})));
  EXPECT_THAT(db->GetFromList(list, 2),
              IsOkAndHolds(DataItem(arolla::Text("aaa"))));
  EXPECT_THAT(db->GetFromList(list, -2),
              IsOkAndHolds(DataItem(arolla::Text("bbb"))));
  EXPECT_THAT(db->ExplodeList(list, DataBagImpl::ListRange(0, 2)),
              IsOkAndHolds(ElementsAre(3.0f, 7)));
  EXPECT_THAT(
      db->ExplodeList(list, DataBagImpl::ListRange(-3)),
      IsOkAndHolds(ElementsAre(arolla::Text("aaa"), arolla::Text("bbb"), 9)));

  ASSERT_OK(db->SetInList(list, -2, DataItem(arolla::Text("ccc"))));

  ASSERT_OK(db->ReplaceInList(
      list, DataBagImpl::ListRange(0, -2),
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({0}))));

  EXPECT_THAT(db->ExplodeList(list),
              IsOkAndHolds(ElementsAre(0, arolla::Text("ccc"), 9)));

  ASSERT_OK(db->ReplaceInList(
      list, DataBagImpl::ListRange(),
      DataSliceImpl::Create(
          arolla::CreateDenseArray<int>({0, std::nullopt, std::nullopt}),
          arolla::CreateDenseArray<float>(
              {std::nullopt, std::nullopt, 5.0f}))));

  EXPECT_THAT(db->ExplodeList(list),
              IsOkAndHolds(ElementsAre(0, DataItem(), 5.0f)));

  ASSERT_OK(db->ReplaceInList(
      list, DataBagImpl::ListRange(2, 1),
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({0}))));

  EXPECT_THAT(db->ExplodeList(list),
              IsOkAndHolds(ElementsAre(0, DataItem(), 0, 5.0f)));
}

TEST(DataBagTest, EmptySingleListOps) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(db->GetListSize(DataItem()), IsOkAndHolds(DataItem()));
  EXPECT_THAT(db->GetFromList(DataItem(), 0), IsOkAndHolds(DataItem()));
  EXPECT_THAT(db->PopFromList(DataItem(), 0), IsOkAndHolds(DataItem()));
  EXPECT_THAT(db->ExplodeList(DataItem()), IsOkAndHolds(ElementsAre()));

  EXPECT_OK(db->SetInList(DataItem(), 0, DataItem(42)));
  EXPECT_OK(db->AppendToList(DataItem(), DataItem()));
  EXPECT_OK(
      db->ExtendList(DataItem(), DataSliceImpl::CreateEmptyAndUnknownType(0)));
  EXPECT_OK(db->ReplaceInList(DataItem(), DataBagImpl::ListRange(),
                              DataSliceImpl::CreateEmptyAndUnknownType(0)));
  EXPECT_OK(db->RemoveInList(DataItem(), DataBagImpl::ListRange()));
}

TEST(DataBagTest, NotListError) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  EXPECT_THAT(db->GetFromList(DataItem(42), 0),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "list expected, got 42"));

  EXPECT_THAT(
      db->GetListSize(DataSliceImpl::Create(
          arolla::CreateDenseArray<int>({1, 2}))),
      StatusIs(absl::StatusCode::kFailedPrecondition, "lists expected"));
}

TEST(DataBagTest, ListWithFallback) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  DataItem list(AllocateSingleList());
  auto fb_db = DataBagImpl::CreateEmptyDatabag();

  EXPECT_THAT(db->GetListSize(list, {fb_db.get()}),
              IsOkAndHolds(DataItem(int64_t{0})));
  EXPECT_THAT(db->GetFromList(list, 0, {fb_db.get()}),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(
      db->ExplodeList(list, DataBagImpl::ListRange(0, 1), {fb_db.get()}),
      IsOkAndHolds(ElementsAre()));

  ASSERT_OK(db->AppendToList(list, DataItem(1.0f)));
  {
    EXPECT_THAT(db->GetListSize(list, {fb_db.get()}),
                IsOkAndHolds(DataItem(int64_t{1})));
    EXPECT_THAT(db->GetFromList(list, 0, {fb_db.get()}),
                IsOkAndHolds(DataItem(1.0f)));
    EXPECT_THAT(db->GetFromList(list, 1, {fb_db.get()}),
                IsOkAndHolds(DataItem()));
    EXPECT_THAT(
        db->ExplodeList(list, DataBagImpl::ListRange(0, 1), {fb_db.get()}),
        IsOkAndHolds(ElementsAre(1.0f)));
  }
  {
    EXPECT_THAT(fb_db->GetListSize(list, {db.get()}),
                IsOkAndHolds(DataItem(int64_t{1})));
    EXPECT_THAT(fb_db->GetFromList(list, 0, {db.get()}),
                IsOkAndHolds(DataItem(1.0f)));
    EXPECT_THAT(fb_db->GetFromList(list, 1, {db.get()}),
                IsOkAndHolds(DataItem()));
    EXPECT_THAT(
        fb_db->ExplodeList(list, DataBagImpl::ListRange(0, 1), {db.get()}),
        IsOkAndHolds(ElementsAre(1.0f)));
  }

  ASSERT_OK(fb_db->AppendToList(list, DataItem(2.0f)));
  ASSERT_OK(fb_db->AppendToList(list, DataItem(3.0f)));
  {
    EXPECT_THAT(db->GetListSize(list, {fb_db.get()}),
                IsOkAndHolds(DataItem(int64_t{1})));
    EXPECT_THAT(db->GetFromList(list, 0, {fb_db.get()}),
                IsOkAndHolds(DataItem(1.0f)));
    // fallback is not used in case list is not unset
    EXPECT_THAT(db->GetFromList(list, 1, {fb_db.get()}),
                IsOkAndHolds(DataItem()));
    EXPECT_THAT(
        db->ExplodeList(list, DataBagImpl::ListRange(0, 2), {fb_db.get()}),
        IsOkAndHolds(ElementsAre(1.0f)));
  }
  ASSERT_OK(db->PopFromList(list, 0));
  {
    // List is REMOVED in `db`. It is empty regardless of fallback.
    EXPECT_THAT(db->ExplodeList(list, DataBagImpl::ListRange(), {fb_db.get()}),
                IsOkAndHolds(ElementsAre()));
    EXPECT_THAT(fb_db->ExplodeList(list, DataBagImpl::ListRange(), {db.get()}),
                IsOkAndHolds(ElementsAre(2.0f, 3.0f)));
  }
  {
    EXPECT_THAT(fb_db->GetListSize(list, {db.get()}),
                IsOkAndHolds(DataItem(int64_t{2})));
    EXPECT_THAT(fb_db->GetFromList(list, 0, {db.get()}),
                IsOkAndHolds(DataItem(2.0f)));
    EXPECT_THAT(fb_db->GetFromList(list, 1, {db.get()}),
                IsOkAndHolds(DataItem(3.0f)));
    EXPECT_THAT(fb_db->GetFromList(list, 2, {db.get()}),
                IsOkAndHolds(DataItem()));
    EXPECT_THAT(
        fb_db->ExplodeList(list, DataBagImpl::ListRange(0, 2), {db.get()}),
        IsOkAndHolds(ElementsAre(2.0f, 3.0f)));
  }
}

TEST(DataBagTest, ListBatchWithFallback) {
  constexpr int64_t kSize = 7;
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto lists =
      DataSliceImpl::ObjectsFromAllocation(AllocateLists(kSize), kSize);
  auto fb_db = DataBagImpl::CreateEmptyDatabag();

  EXPECT_THAT(db->GetListSize(lists, {fb_db.get()}),
              IsOkAndHolds(ElementsAreArray(std::vector<int64_t>(kSize, 0))));
  EXPECT_THAT(
      db->GetFromLists(lists, arolla::CreateConstDenseArray<int64_t>(kSize, 0),
                       {fb_db.get()}),
      IsOkAndHolds(ElementsAreArray(std::vector<DataItem>(kSize, DataItem()))));
  {
    ASSERT_OK_AND_ASSIGN(
        (auto [values, edge]),
        db->ExplodeLists(lists, DataBagImpl::ListRange(0, kSize),
                         {fb_db.get()}));
    EXPECT_THAT(values, ElementsAre());
    EXPECT_THAT(edge.edge_values(),
                ElementsAreArray(std::vector<int64_t>(kSize + 1, 0)));
  }

  auto alloc_0 = Allocate(kSize);
  ASSERT_OK(db->AppendToList(
      lists, DataSliceImpl::ObjectsFromAllocation(alloc_0, kSize)));
  {
    EXPECT_THAT(db->GetListSize(lists, {fb_db.get()}),
                IsOkAndHolds(ElementsAreArray(std::vector<int64_t>(kSize, 1))));
    EXPECT_THAT(db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 0),
                    {fb_db.get()}),
                IsOkAndHolds(ElementsAreArray(
                    DataSliceImpl::ObjectsFromAllocation(alloc_0, kSize))));
    EXPECT_THAT(db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 1),
                    {fb_db.get()}),
                IsOkAndHolds(ElementsAreArray(
                    std::vector<DataItem>(kSize, DataItem()))));
    ASSERT_OK_AND_ASSIGN(
        (auto [values, edge]),
        db->ExplodeLists(lists, DataBagImpl::ListRange(0, kSize),
                         {fb_db.get()}));
    EXPECT_THAT(values, ElementsAreArray(DataSliceImpl::ObjectsFromAllocation(
                            alloc_0, kSize)));
    std::vector<int64_t> expected_split_points(kSize + 1, 0);
    std::iota(expected_split_points.begin(), expected_split_points.end(), 0);
    EXPECT_THAT(edge.edge_values(), ElementsAreArray(expected_split_points));
  }
  {
    EXPECT_THAT(fb_db->GetListSize(lists, {db.get()}),
                IsOkAndHolds(ElementsAreArray(std::vector<int64_t>(kSize, 1))));
    EXPECT_THAT(fb_db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 0),
                    {db.get()}),
                IsOkAndHolds(ElementsAreArray(
                    DataSliceImpl::ObjectsFromAllocation(alloc_0, kSize))));
    EXPECT_THAT(fb_db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 1),
                    {db.get()}),
                IsOkAndHolds(ElementsAreArray(
                    std::vector<DataItem>(kSize, DataItem()))));
    ASSERT_OK_AND_ASSIGN(
        (auto [values, edge]),
        fb_db->ExplodeLists(lists, DataBagImpl::ListRange(0, kSize),
                            {db.get()}));
    EXPECT_THAT(values, ElementsAreArray(DataSliceImpl::ObjectsFromAllocation(
                            alloc_0, kSize)));
    std::vector<int64_t> expected_split_points(kSize + 1, 0);
    std::iota(expected_split_points.begin(), expected_split_points.end(), 0);
    EXPECT_THAT(edge.edge_values(), ElementsAreArray(expected_split_points));
  }

  ASSERT_OK(fb_db->AppendToList(lists[0], DataItem(2.0f)));
  ASSERT_OK(fb_db->AppendToList(lists[0], DataItem(3.0f)));
  ASSERT_OK(db->AppendToList(lists[6], DataItem(57)));
  {  // fallback is not used in case list is not empty
    auto expected_sizes = std::vector<int64_t>(kSize, 1);
    expected_sizes[6] = 2;
    EXPECT_THAT(db->GetListSize(lists, {fb_db.get()}),
                IsOkAndHolds(ElementsAreArray(expected_sizes)));
    EXPECT_THAT(db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 0),
                    {fb_db.get()}),
                IsOkAndHolds(ElementsAreArray(
                    DataSliceImpl::ObjectsFromAllocation(alloc_0, kSize))));
    auto expected_items = std::vector<DataItem>(kSize);
    expected_items[6] = DataItem(57);
    EXPECT_THAT(db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 1),
                    {fb_db.get()}),
                IsOkAndHolds(ElementsAreArray(expected_items)));
    ASSERT_OK_AND_ASSIGN(
        (auto [values, edge]),
        db->ExplodeLists(lists, DataBagImpl::ListRange(0, kSize),
                         {fb_db.get()}));
    auto alloc_0_items = DataSliceImpl::ObjectsFromAllocation(alloc_0, kSize);
    expected_items =
        std::vector<DataItem>(alloc_0_items.begin(), alloc_0_items.end());
    expected_items.push_back(DataItem(57));
    EXPECT_THAT(values, ElementsAreArray(expected_items));
    std::vector<int64_t> expected_split_points(kSize + 1, 0);
    std::iota(expected_split_points.begin(), expected_split_points.end(), 0);
    expected_split_points.back() += 1;
    EXPECT_THAT(edge.edge_values(), ElementsAreArray(expected_split_points));
  }
  {
    auto expected_sizes = std::vector<int64_t>(kSize, 1);
    expected_sizes[0] = 2;
    expected_sizes[6] = 2;
    EXPECT_THAT(fb_db->GetListSize(lists, {db.get()}),
                IsOkAndHolds(ElementsAreArray(expected_sizes)));
    auto alloc_0_items = DataSliceImpl::ObjectsFromAllocation(alloc_0, kSize);
    auto expected_items =
        std::vector<DataItem>(alloc_0_items.begin(), alloc_0_items.end());
    expected_items[0] = DataItem(2.0f);
    EXPECT_THAT(fb_db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 0),
                    {db.get()}),
                IsOkAndHolds(ElementsAreArray(expected_items)));
    expected_items = std::vector<DataItem>(kSize);
    expected_items[0] = DataItem(3.0f);
    expected_items[6] = DataItem(57);
    EXPECT_THAT(fb_db->GetFromLists(
                    lists, arolla::CreateConstDenseArray<int64_t>(kSize, 1),
                    {db.get()}),
                IsOkAndHolds(ElementsAreArray(expected_items)));
    ASSERT_OK_AND_ASSIGN(
        (auto [values, edge]),
        fb_db->ExplodeLists(lists, DataBagImpl::ListRange(0, kSize),
                            {db.get()}));
    expected_items =
        std::vector<DataItem>(alloc_0_items.begin(), alloc_0_items.end());
    expected_items[0] = DataItem(2.0f);
    expected_items.insert(expected_items.begin() + 1, DataItem(3.0f));
    expected_items.push_back(DataItem(57));
    EXPECT_THAT(values, ElementsAreArray(expected_items));
    // The first and the last list have size 2.
    std::vector<int64_t> expected_split_points(kSize + 1, 0);
    expected_split_points[1] = 2;
    std::iota(expected_split_points.begin() + 2, expected_split_points.end(),
              3);
    expected_split_points.back() += 1;
    EXPECT_THAT(edge.edge_values(), ElementsAreArray(expected_split_points));
  }
}

}  // namespace
}  // namespace koladata::internal
