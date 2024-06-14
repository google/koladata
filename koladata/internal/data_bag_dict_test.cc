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
#include "koladata/internal/data_bag.h"

#include <array>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <tuple>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/testing/status_matchers_backport.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace koladata::internal {
namespace {

using ::arolla::DenseArrayEdge;
using ::koladata::testing::IsOkAndHolds;
using ::koladata::testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::Pair;
using ::testing::Property;
using ::testing::UnorderedElementsAre;

TEST(DataBagTest, Dicts) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc_id = AllocateDicts(2);
  ObjectId dict_from_other_alloc = AllocateSingleDict();
  DataSliceImpl all_dicts =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
          {alloc_id.ObjectByOffset(1), alloc_id.ObjectByOffset(0),
           dict_from_other_alloc, std::nullopt}));

  EXPECT_THAT(
      db->GetDictKeys(DataItem(dict_from_other_alloc)),
      IsOkAndHolds(Pair(ElementsAre(), Property(&DenseArrayEdge::edge_values,
                                                ElementsAre(0, 0)))));
  EXPECT_THAT(db->GetDictSize(DataItem(dict_from_other_alloc)),
              IsOkAndHolds(DataItem(int64_t{0})));

  ASSERT_OK(db->SetInDict(
      all_dicts,
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({1, 2, 3, 4})),
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({5, 6, 7, 8}))));
  EXPECT_THAT(db->SetInDict(
                  all_dicts,
                  DataSliceImpl::Create(arolla::CreateDenseArray<int>({1, 2})),
                  DataSliceImpl::Create(arolla::CreateDenseArray<int>({5, 6}))),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "dicts and keys sizes don't match: 4 vs 2"));

  auto db2 = db->PartiallyPersistentFork();

  ASSERT_OK(db2->SetInDict(
      DataSliceImpl::Create(arolla::CreateConstDenseArray<ObjectId>(
          3, alloc_id.ObjectByOffset(0))),
      DataSliceImpl::Create(arolla::CreateDenseArray<arolla::Bytes>(
          {arolla::Bytes("1.0"), arolla::Bytes("2.0"), arolla::Bytes("3.0")})),
      DataSliceImpl::Create(
          arolla::CreateDenseArray<double>({8.0, 9.0, 10.0}))));
  EXPECT_THAT(
      db2->SetInDict(
          DataSliceImpl::Create(arolla::CreateConstDenseArray<ObjectId>(
              2, alloc_id.ObjectByOffset(1))),
          DataSliceImpl::Create(arolla::CreateDenseArray<float>({1.0f, 2.0f})),
          DataSliceImpl::Create(
              arolla::CreateDenseArray<int>({1, std::nullopt}),
              arolla::CreateDenseArray<float>({std::nullopt, 2.0f}))),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "invalid key type: FLOAT32"));

  {
    ASSERT_OK_AND_ASSIGN((auto [keys, edge]), db2->GetDictKeys(all_dicts));
    EXPECT_THAT(std::vector(keys.begin(), keys.begin() + 1),
                UnorderedElementsAre(1));
    EXPECT_THAT(
        std::vector(keys.begin() + 1, keys.begin() + 5),
        UnorderedElementsAre(2, arolla::Bytes("1.0"), arolla::Bytes("2.0"),
                             arolla::Bytes("3.0")));
    EXPECT_THAT(std::vector(keys.begin() + 5, keys.end()),
                UnorderedElementsAre(3));
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 5, 6, 6));
    EXPECT_THAT(db2->GetDictSize(all_dicts),
                IsOkAndHolds(ElementsAre(1, 4, 1, std::nullopt)));
  }

  EXPECT_THAT(
      db2->GetFromDict(
          all_dicts,
          DataSliceImpl::Create(
              arolla::CreateDenseArray<int>({1, std::nullopt, std::nullopt, 4}),
              arolla::CreateDenseArray<arolla::Bytes>(
                  {std::nullopt, arolla::Bytes("2.0"), arolla::Bytes("3.0"),
                   std::nullopt}))),
      IsOkAndHolds(ElementsAre(5, 9.0, DataItem(), DataItem())));
  EXPECT_THAT(
      db2->GetFromDict(all_dicts,
                       DataSliceImpl::Create(
                           arolla::CreateDenseArray<int>({1, std::nullopt}))),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "dicts and keys sizes don't match: 4 vs 2"));

  ASSERT_OK(db2->ClearDict(all_dicts[2]));
  ASSERT_OK(db2->ClearDict(DataSliceImpl::ObjectsFromAllocation(alloc_id, 1)));
  ASSERT_OK(db2->SetInDict(DataItem(alloc_id.ObjectByOffset(1)), DataItem(1),
                           DataItem(true)));

  EXPECT_THAT(db2->GetFromDict(all_dicts[0], DataItem(1)),
              IsOkAndHolds(DataItem(true)));
  EXPECT_THAT(db2->GetFromDict(all_dicts[0], DataItem(1.0f)),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(db2->GetFromDict(all_dicts[0], DataItem(2.0f)),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(db2->GetFromDict(all_dicts[1], DataItem(2)),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(db2->GetFromDict(all_dicts[2], DataItem(3)),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(db2->GetFromDict(all_dicts[3], DataItem(4)),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "dict expected, got None"));

  ASSERT_OK(db2->SetInDict(
      all_dicts,
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({1, 1, 1, 1})),
      DataSliceImpl::CreateEmptyAndUnknownType(4)));
  EXPECT_THAT(db2->GetFromDict(all_dicts[0], DataItem(1)),
              IsOkAndHolds(DataItem()));

  EXPECT_THAT(db->GetFromDict(all_dicts[0], DataItem(1)),
              IsOkAndHolds(DataItem(5)));
  EXPECT_THAT(db->GetFromDict(all_dicts[1], DataItem(2)),
              IsOkAndHolds(DataItem(6)));
  EXPECT_THAT(db->GetFromDict(all_dicts[2], DataItem(3)),
              IsOkAndHolds(DataItem(7)));
  EXPECT_THAT(db->GetFromDict(all_dicts[3], DataItem(4)),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "dict expected, got None"));
}

TEST(DataBagTest, EmptyAndUnknownDicts) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto empty = DataSliceImpl::CreateEmptyAndUnknownType(3);
  ASSERT_OK_AND_ASSIGN((auto [keys, edge]), db->GetDictKeys(empty));
  EXPECT_EQ(keys.size(), 0);
  EXPECT_EQ(edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 0, 0, 0));
  EXPECT_OK(db->ClearDict(empty));
  EXPECT_OK(db->SetInDict(empty, empty, empty));
  EXPECT_THAT(
      db->GetDictSize(empty),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  EXPECT_THAT(db->GetFromDict(empty, empty),
              IsOkAndHolds(ElementsAre(DataItem(), DataItem(), DataItem())));
}

TEST(DataBagTest, DictFallbacks) {
  AllocationId alloc_id = AllocateDicts(2);
  ObjectId dict_from_other_alloc = AllocateSingleDict();
  DataSliceImpl all_dicts =
      DataSliceImpl::Create(arolla::CreateDenseArray<ObjectId>(
          {alloc_id.ObjectByOffset(1), alloc_id.ObjectByOffset(0),
           dict_from_other_alloc}));

  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetInDict(all_dicts[1], DataItem(2), DataItem(4.1)));
  auto fb_db = DataBagImpl::CreateEmptyDatabag();
  auto all_dict_keys =
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({1, 2, 3}));
  {
    ASSERT_OK_AND_ASSIGN((auto [keys, edge]),
                         db->GetDictKeys(all_dicts, {fb_db.get()}));
    EXPECT_THAT(keys, ElementsAre(DataItem(2)));
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 0, 1, 1));
    EXPECT_THAT(db->GetDictSize(all_dicts, {fb_db.get()}),
                IsOkAndHolds(ElementsAre(0, 1, 0)));
  }
  ASSERT_OK(fb_db->SetInDict(
      all_dicts, all_dict_keys,
      DataSliceImpl::Create(arolla::CreateDenseArray<int>({4, 5, 6}))));

  auto expected_items =
      std::vector<DataItem>({DataItem(4), DataItem(4.1), DataItem(6)});
  EXPECT_THAT(db->GetFromDict(all_dicts[0], DataItem(1), {fb_db.get()}),
              IsOkAndHolds(expected_items[0]));
  EXPECT_THAT(db->GetFromDict(all_dicts[1], DataItem(2), {fb_db.get()}),
              IsOkAndHolds(expected_items[1]));
  EXPECT_THAT(db->GetFromDict(all_dicts[2], DataItem(3), {fb_db.get()}),
              IsOkAndHolds(expected_items[2]));

  EXPECT_THAT(db->GetFromDict(all_dicts, all_dict_keys, {fb_db.get()}),
              IsOkAndHolds(ElementsAreArray(expected_items)));

  EXPECT_THAT(db->GetDictKeys(all_dicts[0], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(1)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 1)))));
  EXPECT_THAT(db->GetDictKeys(all_dicts[1], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(2)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 1)))));
  EXPECT_THAT(db->GetDictKeys(all_dicts[2], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(3)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 1)))));
  EXPECT_THAT(db->GetDictSize(all_dicts, {fb_db.get()}),
              IsOkAndHolds(ElementsAre(1, 1, 1)));
  {
    ASSERT_OK_AND_ASSIGN((auto [keys, edge]),
                         db->GetDictKeys(all_dicts, {fb_db.get()}));
    EXPECT_THAT(keys, ElementsAre(DataItem(1), DataItem(2), DataItem(3)));
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 2, 3));
  }

  ASSERT_OK(
      fb_db->SetInDict(all_dicts[1], DataItem(arolla::Text("a")), DataItem(7)));
  EXPECT_THAT(
      db->GetDictKeys(all_dicts[1], {fb_db.get()}),
      IsOkAndHolds(
          Pair(UnorderedElementsAre(DataItem(2), DataItem(arolla::Text("a"))),
               Property(&DenseArrayEdge::edge_values, ElementsAre(0, 2)))));
  {
    ASSERT_OK_AND_ASSIGN((auto [keys, edge]),
                         db->GetDictKeys(all_dicts, {fb_db.get()}));
    ASSERT_EQ(keys.size(), 4);
    EXPECT_THAT(keys[0], Eq(DataItem(1)));
    EXPECT_THAT((std::vector{keys[1], keys[2]}),
                UnorderedElementsAre(DataItem(2), DataItem(arolla::Text("a"))));
    EXPECT_THAT(keys[3], Eq(DataItem(3)));
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 3, 4));
  }

  ASSERT_OK(db->SetInDict(all_dicts[2], DataItem(7), DataItem(-1)));
  EXPECT_THAT(db->SetInDict(all_dicts[2], DataItem(), DataItem(1)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "invalid key type: NOTHING"));
  EXPECT_THAT(db->GetDictKeys(all_dicts[2], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(3), DataItem(7)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 2)))));
  {
    ASSERT_OK_AND_ASSIGN((auto [keys, edge]),
                         db->GetDictKeys(all_dicts, {fb_db.get()}));
    ASSERT_EQ(keys.size(), 5);
    EXPECT_THAT(keys[0], Eq(DataItem(1)));
    EXPECT_THAT((std::vector{keys[1], keys[2]}),
                UnorderedElementsAre(DataItem(2), DataItem(arolla::Text("a"))));
    EXPECT_THAT((std::vector{keys[3], keys[4]}),
                UnorderedElementsAre(DataItem(3), DataItem(7)));
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 3, 5));
    EXPECT_THAT(db->GetDictSize(all_dicts, {fb_db.get()}),
                IsOkAndHolds(ElementsAre(1, 2, 2)));
    EXPECT_THAT(db->GetDictSize(all_dicts[0], {fb_db.get()}),
              IsOkAndHolds(DataItem(int64_t{1})));
    EXPECT_THAT(db->GetDictSize(all_dicts[1], {fb_db.get()}),
              IsOkAndHolds(DataItem(int64_t{2})));
    EXPECT_THAT(db->GetDictSize(all_dicts[2], {fb_db.get()}),
              IsOkAndHolds(DataItem(int64_t{2})));
  }
}

}  // namespace
}  // namespace koladata::internal
