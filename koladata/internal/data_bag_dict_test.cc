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
#include <optional>
#include <tuple>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/expand.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::DenseArrayEdge;
using ::testing::_;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::Pair;
using ::testing::Property;
using ::testing::UnorderedElementsAre;

template <class Impl>
void AssertKVsAreAligned(const DataBagImpl& db, const Impl& dict,
                         DataBagImpl::FallbackSpan fallbacks = {}) {
  ASSERT_OK_AND_ASSIGN((auto [keys, key_edge]),
                       db.GetDictKeys(dict, fallbacks));
  ASSERT_OK_AND_ASSIGN((auto [values, value_edge]),
                       db.GetDictValues(dict, fallbacks));
  EXPECT_TRUE(key_edge.IsEquivalentTo(value_edge));
  ASSERT_OK_AND_ASSIGN(auto expanded_dicts, ExpandOp()(dict, key_edge));
  ASSERT_OK_AND_ASSIGN(auto expected_values,
                       db.GetFromDict(expanded_dicts, keys, fallbacks));
  EXPECT_TRUE(expected_values.IsEquivalentTo(values));
}

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
  EXPECT_THAT(
      db->GetDictValues(DataItem(dict_from_other_alloc)),
      IsOkAndHolds(Pair(ElementsAre(), Property(&DenseArrayEdge::edge_values,
                                                ElementsAre(0, 0)))));
  AssertKVsAreAligned(*db, DataItem(dict_from_other_alloc));
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
    EXPECT_THAT(keys, UnorderedElementsAre(1, 2, arolla::Bytes("1.0"),
                                           arolla::Bytes("2.0"),
                                           arolla::Bytes("3.0"), 3));
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 5, 6, 6));

    ASSERT_OK_AND_ASSIGN((auto [values, value_edge]),
                         db2->GetDictValues(all_dicts));
    EXPECT_THAT(values, UnorderedElementsAre(5, 6, 8.0, 9.0, 10.0, 7));
    EXPECT_THAT(value_edge.edge_values(), ElementsAre(0, 1, 5, 6, 6));

    AssertKVsAreAligned(*db2, all_dicts);

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
              IsOkAndHolds(DataItem()));

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
              IsOkAndHolds(DataItem()));
}

TEST(DataBagTest, EmptyAndUnknownDicts) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto empty = DataSliceImpl::CreateEmptyAndUnknownType(3);

  ASSERT_OK_AND_ASSIGN((auto [keys, edge]), db->GetDictKeys(empty));
  EXPECT_EQ(keys.size(), 0);
  EXPECT_EQ(edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 0, 0, 0));

  ASSERT_OK_AND_ASSIGN((auto [values, value_edge]), db->GetDictValues(empty));
  EXPECT_EQ(values.size(), 0);
  EXPECT_EQ(value_edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
  EXPECT_THAT(value_edge.edge_values(), ElementsAre(0, 0, 0, 0));

  AssertKVsAreAligned(*db, empty);

  EXPECT_OK(db->ClearDict(empty));
  EXPECT_OK(db->SetInDict(empty, empty, empty));
  EXPECT_THAT(
      db->GetDictSize(empty),
      IsOkAndHolds(ElementsAre(std::nullopt, std::nullopt, std::nullopt)));
  EXPECT_THAT(db->GetFromDict(empty, empty),
              IsOkAndHolds(ElementsAre(DataItem(), DataItem(), DataItem())));
}

TEST(DataBagTest, EmptySingleDict) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  ASSERT_OK_AND_ASSIGN((auto [keys, edge]), db->GetDictKeys(DataItem()));
  EXPECT_EQ(keys.size(), 0);
  EXPECT_EQ(edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
  EXPECT_THAT(edge.edge_values(), ElementsAre(0, 0));

  ASSERT_OK_AND_ASSIGN((auto [values, value_edge]),
                       db->GetDictValues(DataItem()));
  EXPECT_EQ(values.size(), 0);
  EXPECT_EQ(value_edge.edge_type(), DenseArrayEdge::SPLIT_POINTS);
  EXPECT_THAT(value_edge.edge_values(), ElementsAre(0, 0));

  AssertKVsAreAligned(*db, DataItem());

  EXPECT_OK(db->ClearDict(DataItem()));
  EXPECT_OK(db->SetInDict(DataItem(), DataItem(42), DataItem(3.14)));
  EXPECT_THAT(db->GetDictSize(DataItem()), IsOkAndHolds(DataItem()));
  EXPECT_THAT(db->GetFromDict(DataItem(), DataItem(42)),
              IsOkAndHolds(DataItem()));
}

TEST(DataBagTest, NotDictError) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  EXPECT_THAT(db->GetFromDict(DataItem(42), DataItem(42)),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "dict expected, got 42"));

  EXPECT_THAT(
      db->GetDictSize(DataSliceImpl::Create(
          arolla::CreateDenseArray<int>({1, 2}))),
      StatusIs(absl::StatusCode::kFailedPrecondition, "dicts expected"));
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
    ASSERT_OK_AND_ASSIGN((auto [values, value_edge]),
                         db->GetDictValues(all_dicts, {fb_db.get()}));
    EXPECT_THAT(values, ElementsAre(DataItem(4.1)));
    EXPECT_THAT(value_edge.edge_values(), ElementsAre(0, 0, 1, 1));
    EXPECT_THAT(db->GetDictSize(all_dicts, {fb_db.get()}),
                IsOkAndHolds(ElementsAre(0, 1, 0)));
    AssertKVsAreAligned(*db, all_dicts, {fb_db.get()});
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
  EXPECT_THAT(db->GetDictValues(all_dicts[0], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(4)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 1)))));
  AssertKVsAreAligned(*db, all_dicts[0], {fb_db.get()});
  EXPECT_THAT(db->GetDictKeys(all_dicts[1], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(2)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 1)))));
  EXPECT_THAT(db->GetDictValues(all_dicts[1], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(4.1)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 1)))));
  AssertKVsAreAligned(*db, all_dicts[1], {fb_db.get()});
  EXPECT_THAT(db->GetDictKeys(all_dicts[2], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(3)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 1)))));
  EXPECT_THAT(db->GetDictValues(all_dicts[2], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(6)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 1)))));
  AssertKVsAreAligned(*db, all_dicts[2], {fb_db.get()});
  EXPECT_THAT(db->GetDictSize(all_dicts, {fb_db.get()}),
              IsOkAndHolds(ElementsAre(1, 1, 1)));
  {
    ASSERT_OK_AND_ASSIGN((auto [keys, edge]),
                         db->GetDictKeys(all_dicts, {fb_db.get()}));
    EXPECT_THAT(keys, ElementsAre(DataItem(1), DataItem(2), DataItem(3)));
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 2, 3));
    ASSERT_OK_AND_ASSIGN((auto [values, value_edge]),
                         db->GetDictValues(all_dicts, {fb_db.get()}));
    EXPECT_THAT(values, ElementsAre(DataItem(4), DataItem(4.1), DataItem(6)));
    EXPECT_THAT(value_edge.edge_values(), ElementsAre(0, 1, 2, 3));
    AssertKVsAreAligned(*db, all_dicts, {fb_db.get()});
  }

  ASSERT_OK(
      fb_db->SetInDict(all_dicts[1], DataItem(arolla::Text("a")), DataItem(7)));
  EXPECT_THAT(
      db->GetDictKeys(all_dicts[1], {fb_db.get()}),
      IsOkAndHolds(
          Pair(UnorderedElementsAre(DataItem(2), DataItem(arolla::Text("a"))),
               Property(&DenseArrayEdge::edge_values, ElementsAre(0, 2)))));
  EXPECT_THAT(db->GetDictValues(all_dicts[1], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(4.1), DataItem(7)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 2)))));
  AssertKVsAreAligned(*db, all_dicts[1], {fb_db.get()});
  {
    ASSERT_OK_AND_ASSIGN((auto [keys, edge]),
                         db->GetDictKeys(all_dicts, {fb_db.get()}));
    ASSERT_EQ(keys.size(), 4);
    EXPECT_THAT(keys[0], Eq(DataItem(1)));
    EXPECT_THAT((std::vector{keys[1], keys[2]}),
                UnorderedElementsAre(DataItem(2), DataItem(arolla::Text("a"))));
    EXPECT_THAT(keys[3], Eq(DataItem(3)));
    EXPECT_THAT(edge.edge_values(), ElementsAre(0, 1, 3, 4));

    ASSERT_OK_AND_ASSIGN((auto [values, value_edge]),
                         db->GetDictValues(all_dicts, {fb_db.get()}));
    ASSERT_EQ(values.size(), 4);
    EXPECT_THAT(values[0], Eq(DataItem(4)));
    EXPECT_THAT((std::vector{values[1], values[2]}),
                UnorderedElementsAre(DataItem(4.1), DataItem(7)));
    EXPECT_THAT(values[3], Eq(DataItem(6)));
    EXPECT_THAT(value_edge.edge_values(), ElementsAre(0, 1, 3, 4));
    AssertKVsAreAligned(*db, all_dicts, {fb_db.get()});
  }

  ASSERT_OK(db->SetInDict(all_dicts[2], DataItem(7), DataItem(-1)));
  EXPECT_THAT(db->SetInDict(all_dicts[2], DataItem(), DataItem(1)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "invalid key type: NOTHING"));
  EXPECT_THAT(db->GetDictKeys(all_dicts[2], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(3), DataItem(7)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 2)))));
  EXPECT_THAT(db->GetDictValues(all_dicts[2], {fb_db.get()}),
              IsOkAndHolds(Pair(
                  UnorderedElementsAre(DataItem(6), DataItem(-1)),
                  Property(&DenseArrayEdge::edge_values, ElementsAre(0, 2)))));
  AssertKVsAreAligned(*db, all_dicts[2], {fb_db.get()});
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

    ASSERT_OK_AND_ASSIGN((auto [values, value_edge]),
                         db->GetDictValues(all_dicts, {fb_db.get()}));
    ASSERT_EQ(values.size(), 5);
    EXPECT_THAT(values[0], Eq(DataItem(4)));
    EXPECT_THAT((std::vector{values[1], values[2]}),
                UnorderedElementsAre(DataItem(4.1), DataItem(7)));
    EXPECT_THAT((std::vector{values[3], values[4]}),
                UnorderedElementsAre(DataItem(-1), DataItem(6)));
    EXPECT_THAT(value_edge.edge_values(), ElementsAre(0, 1, 3, 5));

    AssertKVsAreAligned(*db, all_dicts, {fb_db.get()});

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

TEST(DataBagTest, DictFallbacksSingleItemWithRemovedValues) {
  auto dict = DataItem(AllocateSingleDict());

  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetInDict(dict, DataItem(1), DataItem()));
  auto fb_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(fb_db->SetInDict(dict, DataItem(1), DataItem(7)));

  EXPECT_THAT(db->GetFromDict(dict, DataItem(1), {fb_db.get()}),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(db->GetDictKeys(dict, {fb_db.get()}),
              IsOkAndHolds(Pair(ElementsAre(DataItem(1)), _)));
  EXPECT_THAT(db->GetDictValues(dict, {fb_db.get()}),
              IsOkAndHolds(Pair(ElementsAre(DataItem()), _)));

  ASSERT_OK(db->SetInDict(dict, DataItem(2), DataItem(7)));
  auto fork_db = db->PartiallyPersistentFork();
  ASSERT_OK(fork_db->SetInDict(dict, DataItem(2), DataItem()));
  EXPECT_THAT(fork_db->GetFromDict(dict, DataItem(1), {}),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(fork_db->GetFromDict(dict, DataItem(2), {}),
              IsOkAndHolds(DataItem()));
  EXPECT_THAT(
      fork_db->GetDictKeys(dict, {fb_db.get()}),
      IsOkAndHolds(Pair(UnorderedElementsAre(DataItem(1), DataItem(2)), _)));
  EXPECT_THAT(fork_db->GetDictValues(dict, {fb_db.get()}),
              IsOkAndHolds(Pair(ElementsAre(DataItem(), DataItem()), _)));
}

TEST(DataBagTest, DictFallbacksSingleItemManyKeysWithRemovedValues) {
  auto dict = DataItem(AllocateSingleDict());

  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetInDict(DataSliceImpl::Create({dict, dict}),
                          DataSliceImpl::Create({DataItem(1), DataItem(2)}),
                          DataSliceImpl::Create({DataItem(), DataItem(7)})));
  auto fb_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(
      fb_db->SetInDict(DataSliceImpl::Create({dict, dict}),
                       DataSliceImpl::Create({DataItem(1), DataItem(2)}),
                       DataSliceImpl::Create({DataItem(9), DataItem(1)})));

  EXPECT_THAT(db->GetFromDict(DataSliceImpl::Create({dict, dict}),
                              DataSliceImpl::Create({DataItem(1), DataItem(2)}),
                              {fb_db.get()}),
              IsOkAndHolds(ElementsAre(DataItem(), DataItem(7))));
  EXPECT_THAT(
      db->GetDictKeys(dict, {fb_db.get()}),
      IsOkAndHolds(Pair(UnorderedElementsAre(DataItem(1), DataItem(2)), _)));
  EXPECT_THAT(
      db->GetDictValues(dict, {fb_db.get()}),
      IsOkAndHolds(Pair(UnorderedElementsAre(DataItem(), DataItem(7)), _)));
}

}  // namespace
}  // namespace koladata::internal
