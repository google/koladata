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
#include "koladata/internal/op_utils/extract.h"

#include <array>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/new_ids_like.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/testing/deep_op_utils.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/internal/uuid_object.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::CreateDenseArray;
using ::koladata::internal::testing::DataBagEqual;
using ::koladata::internal::testing::IsEquivalentTo;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;

using testing::deep_op_utils::DeepOpTest;
using testing::deep_op_utils::test_param_values;

struct LeafCollector {
  LeafCallback GetCallback() {
    return [this](const DataSliceImpl& slice, const DataItem& schema) {
      for (const DataItem& item : slice) {
        collected_items.push_back(item);
      }
      return absl::OkStatus();
    };
  }
  std::vector<DataItem> collected_items;
};

class ExtractTest : public DeepOpTest {};

class ShallowCloneTest : public DeepOpTest {};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, ExtractTest,
                         ::testing::ValuesIn(test_param_values));

INSTANTIATE_TEST_SUITE_P(MainOrFallback, ShallowCloneTest,
                         ::testing::ValuesIn(test_param_values));

TEST_P(ShallowCloneTest, ShallowEntitySlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"x", int_dtype}, {"y", int_dtype}}}};
  TriplesT data_triples = {{a0, {{"x", DataItem(1)}, {"y", DataItem(4)}}},
                           {a1, {{"x", DataItem(2)}, {"y", DataItem(5)}}},
                           {a2, {{"x", DataItem(3)}, {"y", DataItem(6)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyObjects(3);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      (auto [result_slice, result_schema]),
      ShallowCloneOp(result_db.get())(obj_ids, itemid, schema, *GetMainDb(db),
                                      {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_EQ(result_slice.size(), 3);
  ASSERT_EQ(result_schema, schema);
  EXPECT_NE(result_slice[0], a0);
  EXPECT_NE(result_slice[1], a1);
  EXPECT_NE(result_slice[2], a2);
  TriplesT expected_data_triples = {
      {result_slice[0], {{"x", DataItem(1)}, {"y", DataItem(4)}}},
      {result_slice[1], {{"x", DataItem(2)}, {"y", DataItem(5)}}},
      {result_slice[2], {{"x", DataItem(3)}, {"y", DataItem(6)}}}};
  TriplesT expected_schema_triples = {
      {result_schema, {{"x", int_dtype}, {"y", int_dtype}}}};
  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetDataTriples(*expected_db, expected_data_triples);
  SetSchemaTriples(*expected_db, expected_schema_triples);
  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(expected_db));
}

TEST_P(ShallowCloneTest, DeepEntitySlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto b0 = obj_ids[3];
  auto b1 = obj_ids[4];
  auto b2 = obj_ids[5];
  auto ds =
      DataSliceImpl::Create(arolla::CreateDenseArray<DataItem>({a0, a1, a2}));
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT data_triples = {{a0, {{"self", a0}, {"b", b0}}},
                           {a1, {{"self", a1}, {"b", b1}}},
                           {a2, {{"self", a2}, {"b", b2}}},
                           {b0, {{"self", b0}}},
                           {b1, {{"self", b1}}},
                           {b2, {{"self", b2}}}};
  TriplesT schema_triples = {{schema_a, {{"self", schema_a}, {"b", schema_b}}},
                             {schema_b, {{"self", schema_b}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyObjects(3);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      (auto [result_slice, result_schema]),
      ShallowCloneOp(result_db.get())(ds, itemid, schema_a, *GetMainDb(db),
                                      {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_EQ(result_slice.size(), 3);
  EXPECT_EQ(result_schema, schema_a);
  EXPECT_NE(result_slice[0], a0);
  EXPECT_NE(result_slice[1], a1);
  EXPECT_NE(result_slice[2], a2);
  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  TriplesT expected_data_triples = {
      {result_slice[0], {{"self", a0}, {"b", b0}}},
      {result_slice[1], {{"self", a1}, {"b", b1}}},
      {result_slice[2], {{"self", a2}, {"b", b2}}},
  };
  TriplesT expected_schema_triples = {
      {result_schema, {{"self", schema_a}, {"b", schema_b}}}};
  SetDataTriples(*expected_db, expected_data_triples);
  SetSchemaTriples(*expected_db, expected_schema_triples);
  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(expected_db));
}

TEST_P(ShallowCloneTest, ShallowListsSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto lists = AllocateEmptyLists(3);
  auto values =
      DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2, 3, 4, 5, 6, 7}));
  ASSERT_OK_AND_ASSIGN(auto edge, arolla::DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 3, 5, 7})));
  ASSERT_OK(db->ExtendLists(lists, values, edge));
  auto list_schema = AllocateSchema();
  TriplesT schema_triples = {
      {list_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyLists(3);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN((auto [result_slice, result_schema]),
                       ShallowCloneOp(result_db.get())(
                           lists, itemid, list_schema, *GetMainDb(db),
                           {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_EQ(result_slice.size(), 3);
  EXPECT_EQ(result_schema, list_schema);
  EXPECT_NE(result_slice[0], lists[0]);
  EXPECT_NE(result_slice[1], lists[1]);
  EXPECT_NE(result_slice[2], lists[2]);
  EXPECT_TRUE(result_slice[0].is_list());
  EXPECT_TRUE(result_slice[1].is_list());
  EXPECT_TRUE(result_slice[2].is_list());
  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  TriplesT expected_schema_triples = {
      {result_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}}};
  ASSERT_OK(expected_db->ExtendLists(result_slice, values, edge));
  SetSchemaTriples(*expected_db, expected_schema_triples);
  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ShallowCloneTest, DeepListsSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto lists = AllocateEmptyLists(3);
  auto values = AllocateEmptyObjects(7);
  ASSERT_OK_AND_ASSIGN(auto edge, arolla::DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 3, 5, 7})));
  ASSERT_OK(db->ExtendLists(lists, values, edge));
  auto item_schema = AllocateSchema();
  auto list_schema = AllocateSchema();
  TriplesT data_triples = {
      {values[0], {{"x", DataItem(1)}}}, {values[1], {{"x", DataItem(2)}}},
      {values[2], {{"x", DataItem(3)}}}, {values[3], {{"x", DataItem(4)}}},
      {values[4], {{"x", DataItem(5)}}}, {values[5], {{"x", DataItem(6)}}},
      {values[6], {{"x", DataItem(7)}}}};
  TriplesT schema_triples = {
      {list_schema, {{schema::kListItemsSchemaAttr, item_schema}}},
      {item_schema, {{"x", DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyLists(3);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN((auto [result_slice, result_schema]),
                       ShallowCloneOp(result_db.get())(
                           lists, itemid, list_schema, *GetMainDb(db),
                           {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_EQ(result_slice.size(), 3);
  EXPECT_EQ(result_schema, list_schema);
  EXPECT_NE(result_slice[0], lists[0]);
  EXPECT_NE(result_slice[1], lists[1]);
  EXPECT_NE(result_slice[2], lists[2]);
  EXPECT_TRUE(result_slice[0].is_list());
  EXPECT_TRUE(result_slice[1].is_list());
  EXPECT_TRUE(result_slice[2].is_list());
  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  TriplesT expected_schema_triples = {
      {result_schema, {{schema::kListItemsSchemaAttr, item_schema}}}};
  ASSERT_OK(expected_db->ExtendLists(result_slice, values, edge));
  SetSchemaTriples(*expected_db, expected_schema_triples);
  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ShallowCloneTest, ShallowDictsSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dicts = AllocateEmptyDicts(3);
  auto dicts_expanded = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {dicts[0], dicts[0], dicts[0], dicts[1], dicts[1], dicts[2], dicts[2]}));
  auto keys =
      DataSliceImpl::Create(CreateDenseArray<int64_t>({1, 2, 3, 1, 5, 3, 7}));
  auto values =
      DataSliceImpl::Create(CreateDenseArray<float>({1, 2, 3, 4, 5, 6, 7}));
  ASSERT_OK(db->SetInDict(dicts_expanded, keys, values));
  auto dict_schema = AllocateSchema();
  TriplesT schema_triples = {
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt32)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kFloat32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyDicts(3);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN((auto [result_slice, result_schema]),
                       ShallowCloneOp(result_db.get())(
                           dicts, itemid, dict_schema, *GetMainDb(db),
                           {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_EQ(result_slice.size(), 3);
  EXPECT_EQ(result_schema, dict_schema);
  EXPECT_NE(result_slice[0], dicts[0]);
  EXPECT_NE(result_slice[1], dicts[1]);
  EXPECT_NE(result_slice[2], dicts[2]);
  EXPECT_TRUE(result_slice[0].is_dict());
  EXPECT_TRUE(result_slice[1].is_dict());
  EXPECT_TRUE(result_slice[2].is_dict());
  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  auto result_dicts_expanded = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {result_slice[0], result_slice[0], result_slice[0], result_slice[1],
       result_slice[1], result_slice[2], result_slice[2]}));
  ASSERT_OK(expected_db->SetInDict(result_dicts_expanded, keys, values));
  TriplesT expected_schema_triples = {
      {result_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt32)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kFloat32)}}}};
  SetSchemaTriples(*expected_db, expected_schema_triples);
  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ShallowCloneTest, DeepDictsSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dicts = AllocateEmptyDicts(3);
  auto keys = AllocateEmptyObjects(4);
  auto values = AllocateEmptyObjects(7);
  auto dicts_expanded = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {dicts[0], dicts[0], dicts[0], dicts[1], dicts[1], dicts[2], dicts[2]}));
  auto keys_expanded = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {keys[0], keys[1], keys[2], keys[0], keys[3], keys[2], keys[3]}));
  ASSERT_OK(db->SetInDict(dicts_expanded, keys_expanded, values));
  auto key_schema = AllocateSchema();
  auto value_schema = AllocateSchema();
  auto dict_schema = AllocateSchema();
  TriplesT data_triples = {{keys[0], {{"name", DataItem("a")}}},
                           {keys[1], {{"name", DataItem("b")}}},
                           {keys[2], {{"name", DataItem("c")}}},
                           {keys[3], {{"name", DataItem("d")}}},
                           {values[0], {{"x", DataItem(1)}}},
                           {values[1], {{"x", DataItem(2)}}},
                           {values[2], {{"x", DataItem(3)}}},
                           {values[3], {{"x", DataItem(4)}}},
                           {values[4], {{"x", DataItem(5)}}},
                           {values[5], {{"x", DataItem(6)}}},
                           {values[6], {{"x", DataItem(7)}}}};
  TriplesT schema_triples = {
      {key_schema, {{"name", DataItem(schema::kString)}}},
      {value_schema, {{"x", DataItem(schema::kInt32)}}},
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, key_schema},
        {schema::kDictValuesSchemaAttr, value_schema}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyDicts(3);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN((auto [result_slice, result_schema]),
                       ShallowCloneOp(result_db.get())(
                           dicts, itemid, dict_schema, *GetMainDb(db),
                           {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_EQ(result_slice.size(), 3);
  EXPECT_EQ(result_schema, dict_schema);
  EXPECT_NE(result_slice[0], dicts[0]);
  EXPECT_NE(result_slice[1], dicts[1]);
  EXPECT_NE(result_slice[2], dicts[2]);
  EXPECT_TRUE(result_slice[0].is_dict());
  EXPECT_TRUE(result_slice[1].is_dict());
  EXPECT_TRUE(result_slice[2].is_dict());
  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  auto result_dicts_expanded = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {result_slice[0], result_slice[0], result_slice[0], result_slice[1],
       result_slice[1], result_slice[2], result_slice[2]}));
  ASSERT_OK(
      expected_db->SetInDict(result_dicts_expanded, keys_expanded, values));
  TriplesT expected_schema_triples = {
      {result_schema,
       {{schema::kDictKeysSchemaAttr, key_schema},
        {schema::kDictValuesSchemaAttr, value_schema}}}};
  SetSchemaTriples(*expected_db, expected_schema_triples);
  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ShallowCloneTest, ObjectsSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(10);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto a4 = obj_ids[4];
  auto a5 = obj_ids[5];
  auto dicts = AllocateEmptyDicts(2);
  auto lists = AllocateEmptyLists(2);
  ASSERT_OK(db->SetInDict(dicts[0], DataItem("a"), DataItem(1)));
  ASSERT_OK(db->SetInDict(dicts[1], a2, a3));
  ASSERT_OK(db->ExtendList(
      lists[0], DataSliceImpl::Create(CreateDenseArray<DataItem>({a4, a5}))));
  ASSERT_OK(db->ExtendList(
      lists[1], DataSliceImpl::Create(CreateDenseArray<int32_t>({0, 1, 2}))));
  auto item_schema = AllocateSchema();
  auto key_schema = AllocateSchema();
  auto dict0_schema = AllocateSchema();
  auto dict1_schema = AllocateSchema();
  auto list0_schema = AllocateSchema();
  auto list1_schema = AllocateSchema();
  TriplesT data_triples = {
      {a0, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(1)}}},
      {a1, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(2)}}},
      {a2, {{"name", DataItem("k0")}}},
      {a3, {{"x", DataItem(10)}}},
      {a4, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(3)}}},
      {a5, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(4)}}},
      {dicts[0], {{schema::kSchemaAttr, dict0_schema}}},
      {dicts[1], {{schema::kSchemaAttr, dict1_schema}}},
      {lists[0], {{schema::kSchemaAttr, list0_schema}}},
      {lists[1], {{schema::kSchemaAttr, list1_schema}}},
  };
  TriplesT schema_triples = {
      {item_schema, {{"x", DataItem(schema::kInt32)}}},
      {key_schema, {{"name", DataItem(schema::kString)}}},
      {dict0_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kString)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kInt32)}}},
      {dict1_schema,
       {{schema::kDictKeysSchemaAttr, key_schema},
        {schema::kDictValuesSchemaAttr, item_schema}}},
      {list0_schema, {{schema::kListItemsSchemaAttr, item_schema}}},
      {list1_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, GenDataTriplesForTest());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {a0, a1, DataItem(), DataItem(3), DataItem("a"), dicts[0], dicts[1],
       lists[0], lists[1]}));
  auto itemid = internal::NewIdsLike(ds);
  auto schema = AllocateSchema();
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      (auto [result_slice, result_schema]),
      ShallowCloneOp(result_db.get())(ds, itemid, DataItem(schema::kObject),
                                      *GetMainDb(db), {GetFallbackDb(db).get()},
                                      nullptr, {}));

  auto result_a0 = result_slice[0];
  auto result_a1 = result_slice[1];
  auto result_dict0 = result_slice[5];
  auto result_dict1 = result_slice[6];
  auto result_list0 = result_slice[7];
  auto result_list1 = result_slice[8];
  EXPECT_EQ(result_slice.size(), ds.size());
  EXPECT_EQ(result_schema, DataItem(schema::kObject));
  for (int64_t i = 2; i <= 4; ++i) {
    EXPECT_EQ(result_slice[i], ds[i]);
  }
  EXPECT_NE(result_a0, a0);
  EXPECT_NE(result_a1, a1);
  EXPECT_NE(result_dict0, dicts[0]);
  EXPECT_NE(result_dict1, dicts[1]);
  EXPECT_NE(result_list0, lists[0]);
  EXPECT_NE(result_list1, lists[1]);
  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->SetInDict(result_dict0, DataItem("a"), DataItem(1)));
  ASSERT_OK(expected_db->SetInDict(result_dict1, a2, a3));
  ASSERT_OK(expected_db->ExtendList(
      result_list0,
      DataSliceImpl::Create(CreateDenseArray<DataItem>({a4, a5}))));
  ASSERT_OK(expected_db->ExtendList(
      result_list1,
      DataSliceImpl::Create(CreateDenseArray<int32_t>({0, 1, 2}))));
  TriplesT expected_data_triples = {
      {result_a0, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(1)}}},
      {result_a1, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(2)}}},
      {result_dict0, {{schema::kSchemaAttr, dict0_schema}}},
      {result_dict1, {{schema::kSchemaAttr, dict1_schema}}},
      {result_list0, {{schema::kSchemaAttr, list0_schema}}},
      {result_list1, {{schema::kSchemaAttr, list1_schema}}},
  };
  TriplesT expected_schema_triples = {
      {item_schema, {{"x", DataItem(schema::kInt32)}}},
      {dict0_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kString)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kInt32)}}},
      {dict1_schema,
       {{schema::kDictKeysSchemaAttr, key_schema},
        {schema::kDictValuesSchemaAttr, item_schema}}},
      {list0_schema, {{schema::kListItemsSchemaAttr, item_schema}}},
      {list1_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}}};
  SetDataTriples(*expected_db, expected_data_triples);
  SetSchemaTriples(*expected_db, expected_schema_triples);
  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ShallowCloneTest, MixedObjectsAndSchemasSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(5);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto item_schema = AllocateSchema();
  auto schema_schema = DataItem(schema::kSchema);
  auto s0 = AllocateSchema();
  auto s1 = AllocateSchema();
  TriplesT data_triples = {
      {a0, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(1)}}},
      {a1, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(2)}}},
      {a2, {{schema::kSchemaAttr, s1}, {"self", a2}, {"name", DataItem("a2")}}},
      {s0, {{schema::kSchemaAttr, schema_schema}}},
      {s1, {{schema::kSchemaAttr, schema_schema}}},
  };
  TriplesT schema_triples = {
      {item_schema,
       {{"x", DataItem(schema::kInt32)}, {"y", DataItem(schema::kInt32)}}},
      {s0, {{"name", DataItem(schema::kString)}}},
      {s1, {{"self", s1}, {"name", DataItem(schema::kString)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, GenDataTriplesForTest());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {a0, a1, a2, DataItem(), DataItem(3), DataItem("a"), s0, s1}));
  auto itemid = internal::NewIdsLike(ds);
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(ShallowCloneOp(result_db.get())(
                  ds, itemid, DataItem(schema::kObject), *GetMainDb(db),
                  {GetFallbackDb(db).get()}, nullptr, {}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       ::testing::HasSubstr(
                           "unsupported schema found during extract/clone")));
}

TEST_P(ShallowCloneTest, ObjectsWithImplicitAndExplicitSchemas) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(5);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto a4 = obj_ids[4];
  auto schema0 = AllocateSchema();
  auto schema4 = AllocateSchema();
  auto u1 = DataItem(internal::CreateUuidWithMainObject<
                     internal::ObjectId::kUuidImplicitSchemaFlag>(
      a1.value<ObjectId>(),
      arolla::FingerprintHasher(schema::kImplicitSchemaSeed).Finish()));
  auto u2 = DataItem(internal::CreateUuidWithMainObject<
                     internal::ObjectId::kUuidImplicitSchemaFlag>(
      a2.value<ObjectId>(),
      arolla::FingerprintHasher(schema::kImplicitSchemaSeed).Finish()));
  TriplesT data_triples = {
      {a0, {{schema::kSchemaAttr, schema0}, {"x", DataItem(1)}}},
      {a1,
       {{schema::kSchemaAttr, u1},
        {"x", DataItem(2)},
        {"name", DataItem("a1")}}},
      {a2, {{schema::kSchemaAttr, u2}, {"y", DataItem(3)}, {"self", a2}}},
      {a3, {{schema::kSchemaAttr, schema0}, {"x", DataItem(4)}}},
      {a4, {{schema::kSchemaAttr, schema4}, {"name", DataItem("a4")}}},
  };
  TriplesT schema_triples = {
      {schema0, {{"x", DataItem(schema::kInt32)}}},
      {schema4, {{"name", DataItem(schema::kString)}}},
      {u1,
       {{"x", DataItem(schema::kInt32)}, {"name", DataItem(schema::kString)}}},
      {u2,
       {{"y", DataItem(schema::kInt32)}, {"self", DataItem(schema::kObject)}}},
  };
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, GenDataTriplesForTest());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());

  auto itemid = NewIdsLike(obj_ids);
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      (auto [result_slice, result_schema]),
      ShallowCloneOp(result_db.get())(obj_ids, itemid,
                                      DataItem(schema::kObject), *GetMainDb(db),
                                      {GetFallbackDb(db).get()}, nullptr, {}));
  EXPECT_EQ(result_slice.size(), obj_ids.size());
  auto result_a0 = result_slice[0];
  auto result_a1 = result_slice[1];
  auto result_a2 = result_slice[2];
  auto result_a3 = result_slice[3];
  auto result_a4 = result_slice[4];
  ASSERT_OK_AND_ASSIGN(auto result_schemas,
                       result_db->GetAttr(result_slice, schema::kSchemaAttr));
  EXPECT_EQ(result_schemas.size(), obj_ids.size());
  EXPECT_EQ(result_schema, DataItem(schema::kObject));
  auto result_u1 = result_schemas[1];
  auto result_u2 = result_schemas[2];
  EXPECT_NE(result_u1, u1);
  EXPECT_NE(result_u2, u2);
  EXPECT_TRUE(result_u1.is_implicit_schema());
  EXPECT_TRUE(result_u2.is_implicit_schema());
  TriplesT expected_data_triples = {
      {result_a0, {{schema::kSchemaAttr, schema0}, {"x", DataItem(1)}}},
      {result_a1,
       {{schema::kSchemaAttr, result_u1},
        {"x", DataItem(2)},
        {"name", DataItem("a1")}}},
      {result_a2,
       {{schema::kSchemaAttr, result_u2}, {"y", DataItem(3)}, {"self", a2}}},
      {result_a3, {{schema::kSchemaAttr, schema0}, {"x", DataItem(4)}}},
      {result_a4, {{schema::kSchemaAttr, schema4}, {"name", DataItem("a4")}}},
  };
  TriplesT expected_schema_triples = {
      {schema0, {{"x", DataItem(schema::kInt32)}}},
      {schema4, {{"name", DataItem(schema::kString)}}},
      {result_u1,
       {{"x", DataItem(schema::kInt32)}, {"name", DataItem(schema::kString)}}},
      {result_u2,
       {{"y", DataItem(schema::kInt32)}, {"self", DataItem(schema::kObject)}}}};
  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetDataTriples(*expected_db, expected_data_triples);
  SetSchemaTriples(*expected_db, expected_schema_triples);
  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ShallowCloneTest, SchemaSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s1 = AllocateSchema();
  auto s2 = AllocateSchema();
  TriplesT schema_triples = {
      {s1, {{"x", DataItem(schema::kInt32)}}},
      {s2, {{"a", DataItem(schema::kString)}}},
  };
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({s1, s2}));
  auto itemid = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({AllocateSchema(), AllocateSchema()}));
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      (auto [result_slice, result_schema]),
      ShallowCloneOp(result_db.get())(ds, itemid, DataItem(schema::kSchema),
                                      *GetMainDb(db), {GetFallbackDb(db).get()},
                                      nullptr, {}));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_NE(result_db.get(), db.get());
  EXPECT_NE(result_slice[0], s1);
  EXPECT_NE(result_slice[1], s2);
  TriplesT expected_schema_triples = {
      {result_slice[0], {{"x", DataItem(schema::kInt32)}}},
      {result_slice[1], {{"a", DataItem(schema::kString)}}},
  };
  SetSchemaTriples(*expected_db, expected_schema_triples);
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ShallowCloneTest, NamedSchemaSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s1 = AllocateSchema();
  auto s2 = AllocateSchema();
  TriplesT schema_triples = {
      {s1,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("s1"))},
        {"x", DataItem(schema::kInt32)}}},
      {s2,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("s2"))},
        {"a", DataItem(schema::kString)}}},
  };
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({s1, s2}));
  auto itemid = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({AllocateSchema(), AllocateSchema()}));
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      (auto [result_slice, result_schema]),
      ShallowCloneOp(result_db.get())(ds, itemid, DataItem(schema::kSchema),
                                      *GetMainDb(db), {GetFallbackDb(db).get()},
                                      nullptr, {}));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_NE(result_db.get(), db.get());
  EXPECT_NE(result_slice[0], s1);
  EXPECT_NE(result_slice[1], s2);
  TriplesT expected_schema_triples = {
      {result_slice[0], {{"x", DataItem(schema::kInt32)}}},
      {result_slice[1], {{"a", DataItem(schema::kString)}}},
  };
  SetSchemaTriples(*expected_db, expected_schema_triples);
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ShallowCloneTest, SchemaUuidSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s1 = DataItem(CreateUuidExplicitSchema(
      arolla::FingerprintHasher("").Combine(42).Finish()));
  auto s2 = DataItem(CreateUuidExplicitSchema(
      arolla::FingerprintHasher("").Combine(43).Finish()));
  TriplesT schema_triples = {
      {s1, {{"x", DataItem(schema::kInt32)}}},
      {s2, {{"a", DataItem(schema::kString)}}},
  };
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({s1, s2}));
  auto itemid = NewIdsLike(ds);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      (auto [result_slice, result_schema]),
      ShallowCloneOp(result_db.get())(ds, itemid, DataItem(schema::kSchema),
                                      *GetMainDb(db), {GetFallbackDb(db).get()},
                                      nullptr, {}));

  auto result_s1 = result_slice[0];
  auto result_s2 = result_slice[1];
  EXPECT_NE(result_s1, s1);
  EXPECT_NE(result_s2, s2);
  EXPECT_TRUE(result_s1.is_schema());
  EXPECT_FALSE(result_s1.is_implicit_schema());
  EXPECT_TRUE(result_s2.is_schema());
  EXPECT_FALSE(result_s2.is_implicit_schema());
  TriplesT expected_schema_triples = {
      {result_s1, {{"x", DataItem(schema::kInt32)}}},
      {result_s2, {{"a", DataItem(schema::kString)}}}};
  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, expected_schema_triples);

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ShallowCloneTest, MissingInItemid) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"x", int_dtype}, {"y", int_dtype}}}};
  TriplesT data_triples = {{a0, {{"x", DataItem(1)}, {"y", DataItem(4)}}},
                           {a1, {{"x", DataItem(2)}, {"y", DataItem(5)}}},
                           {a2, {{"x", DataItem(3)}, {"y", DataItem(6)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid_alloc = AllocateEmptyObjects(3);
  auto itemid = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {itemid_alloc[0], std::nullopt, itemid_alloc[2]}));

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ShallowCloneOp(result_db.get())(obj_ids, itemid, schema, *GetMainDb(db),
                                      {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::HasSubstr(
              "itemid must have an objectId for each item present in ds")));
}

TEST_P(ShallowCloneTest, ExtraInItemid) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"x", int_dtype}, {"y", int_dtype}}}};
  TriplesT data_triples = {{a0, {{"x", DataItem(1)}, {"y", DataItem(4)}}},
                           {a1, {{"x", DataItem(2)}, {"y", DataItem(5)}}},
                           {a2, {{"x", DataItem(3)}, {"y", DataItem(6)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyObjects(3);
  auto ds = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({a0, DataItem(AllocateSingleDict()), a2}));

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ShallowCloneOp(result_db.get())(ds, itemid, schema, *GetMainDb(db),
                                      {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr("itemid must be of the same type as "
                                    "respective ObjectId from ds")));
}

TEST_P(ShallowCloneTest, IntersectingItemidWithDsPrimitives) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"x", int_dtype}, {"y", int_dtype}}}};
  TriplesT data_triples = {
      {a0,
       {{schema::kSchemaAttr, schema}, {"x", DataItem(1)}, {"y", DataItem(4)}}},
      {a1,
       {{schema::kSchemaAttr, schema}, {"x", DataItem(2)}, {"y", DataItem(5)}}},
      {a2,
       {{schema::kSchemaAttr, schema},
        {"x", DataItem(3)},
        {"y", DataItem(6)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyObjects(3);
  auto ds =
      DataSliceImpl::Create(CreateDenseArray<DataItem>({a0, DataItem(3), a2}));

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      (auto [result, _]),
      ShallowCloneOp(result_db.get())(ds, itemid, DataItem(schema::kObject),
                                      *GetMainDb(db), {GetFallbackDb(db).get()},
                                      nullptr, {}));
  EXPECT_EQ(result[0], itemid[0]);
  EXPECT_EQ(result[1], DataItem(3));
  EXPECT_EQ(result[2], itemid[2]);
}

TEST_P(ExtractTest, DataSliceEntity) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"x", int_dtype}, {"y", int_dtype}}}};
  TriplesT data_triples = {{a0, {{"x", DataItem(1)}, {"y", DataItem(4)}}},
                           {a1, {{"x", DataItem(2)}, {"y", DataItem(5)}}},
                           {a2, {{"x", DataItem(3)}, {"y", DataItem(6)}}}};

  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(obj_ids, schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DataSliceEntityRemovedValues) {
  for (int64_t size : {1, 3, 17, 1034}) {
    auto obj_ids = AllocateEmptyObjects(size);
    auto int_dtype = DataItem(schema::kInt64);
    auto schema = AllocateSchema();

    TriplesT schema_triples = {{schema, {{"x", int_dtype}, {"y", int_dtype}}}};
    TriplesT data_triples;
    TriplesT data_triples_fallback;
    TriplesT data_triples_expected;
    for (int64_t i = 0; i < size; ++i) {
      if (i % 3 == 0) {
        data_triples.emplace_back(obj_ids[i], AttrsT{{"x", DataItem()}});
        data_triples_fallback.emplace_back(obj_ids[i],
                                           AttrsT{{"x", DataItem(i * 2 + 97)}});
        data_triples_expected.emplace_back(obj_ids[i],
                                           AttrsT{{"x", DataItem()}});
      } else if (i % 3 == 1) {
        data_triples.emplace_back(obj_ids[i], AttrsT{{"x", DataItem(i * 2)}});
        data_triples_fallback.emplace_back(obj_ids[i],
                                           AttrsT{{"x", DataItem(i * 2 + 19)}});
        data_triples_expected.emplace_back(obj_ids[i],
                                           AttrsT{{"x", DataItem(i * 2)}});
      } else {
        data_triples_fallback.emplace_back(obj_ids[i],
                                           AttrsT{{"x", DataItem(i * 2 - 13)}});
        data_triples_expected.emplace_back(obj_ids[i],
                                           AttrsT{{"x", DataItem(i * 2 - 13)}});
      }
    }

    auto db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*db, schema_triples);
    SetDataTriples(*db, data_triples);
    auto fb = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*fb, schema_triples);
    SetDataTriples(*fb, data_triples_fallback);
    SetSchemaTriples(*db, GenSchemaTriplesFoTests());
    SetDataTriples(*db, GenDataTriplesForTest());

    auto expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples);
    SetDataTriples(*expected_db, data_triples_expected);

    auto result_db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(ExtractOp(result_db.get())(obj_ids, schema, *GetMainDb(db),
                                         {GetFallbackDb(db).get()}, nullptr,
                                         {}));

    ASSERT_NE(result_db.get(), db.get());
    {
      ASSERT_OK_AND_ASSIGN(auto expected_x, expected_db->GetAttr(obj_ids, "x"));
      EXPECT_THAT(result_db->GetAttr(obj_ids, "x", {fb.get()}),
                  IsOkAndHolds(IsEquivalentTo(expected_x)));
    }
  }
}

TEST_P(ExtractTest, DataSliceEntityAllUnset) {
  for (int64_t size : {1, 3, 17, 1034}) {
    auto obj_ids = AllocateEmptyObjects(size);
    auto int_dtype = DataItem(schema::kInt32);
    auto schema = AllocateSchema();

    TriplesT schema_triples = {{schema, {{"x", int_dtype}, {"y", int_dtype}}}};
    TriplesT data_triples;
    for (int64_t i = 0; i < size; ++i) {
      data_triples.emplace_back(obj_ids[i], AttrsT{{"x", DataItem(i)}});
    }

    auto db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*db, schema_triples);
    auto fb = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*fb, schema_triples);
    SetDataTriples(*fb, data_triples);
    SetSchemaTriples(*db, GenSchemaTriplesFoTests());
    SetDataTriples(*db, GenDataTriplesForTest());

    auto expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples);
    SetDataTriples(*expected_db, data_triples);

    auto result_db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(ExtractOp(result_db.get())(obj_ids, schema, *GetMainDb(db),
                                         {GetFallbackDb(db).get()}, nullptr,
                                         {}));

    ASSERT_NE(result_db.get(), db.get());
    {
      ASSERT_OK_AND_ASSIGN(auto expected_x, expected_db->GetAttr(obj_ids, "x"));
      EXPECT_THAT(result_db->GetAttr(obj_ids, "x", {fb.get()}),
                  IsOkAndHolds(IsEquivalentTo(expected_x)));
    }
  }
}

TEST_P(ExtractTest, SchemaMetadata) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = DataItem(AllocateSchema());
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       CreateUuidWithMainObject(schema, schema::kMetadataSeed));
  ASSERT_OK_AND_ASSIGN(
      auto metadata_schema,
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          metadata, schema::kImplicitSchemaSeed));
  auto a1 = DataItem(AllocateSingleObject());
  TriplesT schema_triples = {
      {schema,
       {{"x", DataItem(schema::kInt32)},
        {schema::kSchemaMetadataAttr, metadata}}},
      {metadata_schema, {{"name", DataItem(schema::kString)}}}};
  TriplesT data_triples = {
      {a1, {{"x", DataItem(2)}}},
      {metadata,
       {{schema::kSchemaAttr, metadata_schema},
        {"name", DataItem(arolla::Text("object with metadata"))}}}};

  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(a1, schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, SchemaMetadataLoop) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = DataItem(AllocateSchema());
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       CreateUuidWithMainObject(schema, schema::kMetadataSeed));
  ASSERT_OK_AND_ASSIGN(
      auto metadata_schema,
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          metadata, schema::kImplicitSchemaSeed));
  auto a1 = DataItem(AllocateSingleObject());
  auto a2 = DataItem(AllocateSingleObject());
  TriplesT schema_triples = {
      {schema, {{"x", schema}, {schema::kSchemaMetadataAttr, metadata}}},
      {metadata_schema, {{"name", DataItem(schema::kString)}}}};
  TriplesT data_triples = {
      {a1, {{"x", a2}}},
      {a2, {{"x", a1}}},
      {metadata,
       {{schema::kSchemaAttr, metadata_schema},
        {"name", DataItem(arolla::Text("object with metadata"))}}}};

  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(a1, schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, MaxDepthSchemaMetadata) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = DataItem(AllocateSchema());
  auto item_schema = DataItem(AllocateSchema());
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       CreateUuidWithMainObject(schema, schema::kMetadataSeed));
  ASSERT_OK_AND_ASSIGN(
      auto metadata_schema,
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          metadata, schema::kImplicitSchemaSeed));
  auto a1 = DataItem(AllocateSingleObject());
  auto a2 = DataItem(AllocateSingleObject());
  auto metadata_a1 = DataItem(AllocateSingleObject());
  TriplesT schema_triples = {
      {schema, {{"x", item_schema}, {schema::kSchemaMetadataAttr, metadata}}},
      {metadata_schema,
       {{"name", DataItem(schema::kString)}, {"item", item_schema}}},
      {item_schema, {{"x", DataItem(schema::kInt32)}, {"self", item_schema}}}};
  TriplesT data_triples = {
      {a1, {{"x", a2}}},
      {metadata,
       {{schema::kSchemaAttr, metadata_schema},
        {"name", DataItem(arolla::Text("object with metadata"))},
        {"item", metadata_a1}}}};
  TriplesT data_triples_deep = {
      {a2, {{"x", DataItem(2)}, {"self", a2}}},
      {metadata_a1, {{"x", DataItem(3)}, {"self", metadata_a1}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetDataTriples(*db, data_triples_deep);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  {
    // depth = 1
    auto expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples);
    SetDataTriples(*expected_db, data_triples);

    auto result_db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(ExtractOp(result_db.get())(a1, schema, *GetMainDb(db),
                                         {GetFallbackDb(db).get()}, nullptr, {},
                                         /*max_depth=*/1));

    ASSERT_NE(result_db.get(), db.get());
    EXPECT_THAT(result_db, DataBagEqual(*expected_db));
  }
  {
    // depth = 2
    auto expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples);
    SetDataTriples(*expected_db, data_triples);
    SetDataTriples(*expected_db, data_triples_deep);

    auto result_db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(ExtractOp(result_db.get())(a1, schema, *GetMainDb(db),
                                         {GetFallbackDb(db).get()}, nullptr, {},
                                         /*max_depth=*/2));

    ASSERT_NE(result_db.get(), db.get());
    EXPECT_THAT(result_db, DataBagEqual(*expected_db));
  }
}

TEST_P(ExtractTest, MaxDepthSchemaMetadataObject) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = AllocateEmptyObjects(2);
  ASSERT_OK_AND_ASSIGN(
      auto schemas, CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
                        ds, schema::kImplicitSchemaSeed));
  auto item_schema = DataItem(AllocateSchema());
  ASSERT_OK_AND_ASSIGN(
      auto metadata, CreateUuidWithMainObject(schemas, schema::kMetadataSeed));
  ASSERT_OK_AND_ASSIGN(
      auto metadata_schema,
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          metadata, schema::kImplicitSchemaSeed));
  auto a1 = ds[0];
  auto a2 = ds[1];
  auto b1 = DataItem(AllocateSingleObject());
  auto b2 = DataItem(AllocateSingleObject());
  auto metadata_a1 = DataItem(AllocateSingleObject());
  auto metadata_a2 = DataItem(AllocateSingleObject());
  TriplesT schema_triples = {
      {schemas[0],
       {{"x", item_schema}, {schema::kSchemaMetadataAttr, metadata[0]}}},
      {schemas[1],
       {{"x", item_schema}, {schema::kSchemaMetadataAttr, metadata[1]}}},
      {metadata_schema[0],
       {{"name", DataItem(schema::kString)}, {"item", item_schema}}},
      {metadata_schema[1],
       {{"name", DataItem(schema::kString)}, {"item", item_schema}}},
      {item_schema, {{"x", DataItem(schema::kInt32)}, {"self", item_schema}}}};
  TriplesT data_triples = {
      {a1, {{schema::kSchemaAttr, schemas[0]}, {"x", b1}}},
      {a2, {{schema::kSchemaAttr, schemas[1]}, {"x", b2}}},
      {metadata[0],
       {{schema::kSchemaAttr, metadata_schema[0]},
        {"name", DataItem(arolla::Text("object with metadata"))},
        {"item", metadata_a1}}},
      {metadata[1],
       {{schema::kSchemaAttr, metadata_schema[1]},
        {"name", DataItem(arolla::Text("object with metadata"))},
        {"item", metadata_a2}}}};
  TriplesT data_triples_deep = {
      {b1, {{"x", DataItem(2)}, {"self", b1}}},
      {b2, {{"x", DataItem(2)}, {"self", b2}}},
      {metadata_a1, {{"x", DataItem(3)}, {"self", metadata_a1}}},
      {metadata_a2, {{"x", DataItem(3)}, {"self", metadata_a2}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetDataTriples(*db, data_triples_deep);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  {
    // depth = 1
    auto expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples);
    SetDataTriples(*expected_db, data_triples);

    auto result_db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(ExtractOp(result_db.get())(ds, DataItem(schema::kObject),
                                         *GetMainDb(db),
                                         {GetFallbackDb(db).get()}, nullptr, {},
                                         /*max_depth=*/1));

    ASSERT_NE(result_db.get(), db.get());
    EXPECT_THAT(result_db, DataBagEqual(*expected_db));
  }
  {
    // depth = 2
    auto expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples);
    SetDataTriples(*expected_db, data_triples);
    SetDataTriples(*expected_db, data_triples_deep);

    auto result_db = DataBagImpl::CreateEmptyDatabag();
    ASSERT_OK(ExtractOp(result_db.get())(ds, DataItem(schema::kObject),
                                         *GetMainDb(db),
                                         {GetFallbackDb(db).get()}, nullptr, {},
                                         /*max_depth=*/2));

    ASSERT_NE(result_db.get(), db.get());
    EXPECT_THAT(result_db, DataBagEqual(*expected_db));
  }
}

TEST_P(ExtractTest, MaxDepthSliceOfListsSingleAllocation) {
  const DataItem obj_dtype = DataItem(schema::kObject);
  const DataItem int_dtype = DataItem(schema::kInt32);
  const DataItem text_dtype = DataItem(schema::kString);
  DataBagImplPtr db = DataBagImpl::CreateEmptyDatabag();

  // Schema triples.
  const DataItem root_schema = obj_dtype;
  const DataItem list_schema_a = AllocateSchema();
  const DataItem list_schema_b = AllocateSchema();
  const DataItem obj_level2_schema = AllocateSchema();
  const TriplesT schema_triples_level0 = {
      {list_schema_a, {{schema::kListItemsSchemaAttr, obj_level2_schema}}},
      {list_schema_b, {{schema::kListItemsSchemaAttr, obj_level2_schema}}},
  };
  const TriplesT schema_triples_level1 = {
      {obj_level2_schema, {{"x", int_dtype}, {"y", int_dtype}}},
  };
  const TriplesT schema_triples_level2 = {};

  // Data triples.
  const AllocationId lists_alloc = AllocateLists(2);
  const DataSliceImpl root_ds =
      DataSliceImpl::ObjectsFromAllocation(lists_alloc, 2);
  const DataItem list_a = root_ds[0];
  const DataItem list_b = root_ds[1];
  const DataSliceImpl obj_ids = AllocateEmptyObjects(2);
  const DataItem obj_a = obj_ids[0];
  const DataItem obj_b = obj_ids[1];

  const TriplesT data_triples_level0 = {
      {list_a, {{schema::kSchemaAttr, list_schema_a}}},
      {list_b, {{schema::kSchemaAttr, list_schema_b}}},
  };

  const TriplesT data_triples_level1 = {};

  const TriplesT data_triples_level2 = {
      {obj_a, {{"x", DataItem(12)}, {"y", DataItem(34)}}},
      {obj_b, {{"x", DataItem(56)}, {"y", DataItem(78)}}},
  };

  // Lists values.
  const DataSliceImpl list_a_values =
      DataSliceImpl::Create(CreateDenseArray<DataItem>({obj_a}));

  const DataSliceImpl list_b_values =
      DataSliceImpl::Create(CreateDenseArray<DataItem>({obj_b}));

  SetSchemaTriples(*db, schema_triples_level0);
  SetSchemaTriples(*db, schema_triples_level1);
  SetSchemaTriples(*db, schema_triples_level2);
  SetDataTriples(*db, data_triples_level0);
  SetDataTriples(*db, data_triples_level1);
  SetDataTriples(*db, data_triples_level2);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  SetSingleListValues(db, list_a, list_a_values);
  SetSingleListValues(db, list_b, list_b_values);

  // Everything extracted.
  {
    DataBagImplPtr expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples_level0);
    SetSchemaTriples(*expected_db, schema_triples_level1);
    SetSchemaTriples(*expected_db, schema_triples_level2);
    SetDataTriples(*expected_db, data_triples_level0);
    SetDataTriples(*expected_db, data_triples_level1);
    SetDataTriples(*expected_db, data_triples_level2);
    SetSingleListValues(expected_db, list_a, list_a_values);
    SetSingleListValues(expected_db, list_b, list_b_values);
    {  // default max_depth, i.e. -1
      DataBagImplPtr result_db = DataBagImpl::CreateEmptyDatabag();
      ASSERT_OK(ExtractOp(result_db.get())(root_ds, obj_dtype, *GetMainDb(db),
                                           {GetFallbackDb(db).get()}, nullptr,
                                           {}));

      ASSERT_NE(result_db.get(), db.get());
      EXPECT_THAT(result_db, DataBagEqual(*expected_db));
    }
    {  // max_depth == maximum dataslice depth
      DataBagImplPtr result_db = DataBagImpl::CreateEmptyDatabag();
      LeafCollector leaf_collector;

      ASSERT_OK(ExtractOp(result_db.get())(
          root_ds, obj_dtype, *GetMainDb(db), {GetFallbackDb(db).get()},
          nullptr, {}, /*max_depth=*/2, leaf_collector.GetCallback()));

      ASSERT_NE(result_db.get(), db.get());
      EXPECT_THAT(result_db, DataBagEqual(*expected_db));
      EXPECT_THAT(leaf_collector.collected_items, IsEmpty());
    }
  }
  {  // max_depth = 1
    DataBagImplPtr expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples_level0);
    SetSchemaTriples(*expected_db, schema_triples_level1);
    SetDataTriples(*expected_db, data_triples_level0);
    SetDataTriples(*expected_db, data_triples_level1);
    SetSingleListValues(expected_db, list_a, list_a_values);
    SetSingleListValues(expected_db, list_b, list_b_values);

    DataBagImplPtr result_db = DataBagImpl::CreateEmptyDatabag();
    LeafCollector leaf_collector;

    ASSERT_OK(ExtractOp(result_db.get())(
        root_ds, obj_dtype, *GetMainDb(db), {GetFallbackDb(db).get()}, nullptr,
        {}, /*max_depth=*/1, leaf_collector.GetCallback()));

    ASSERT_NE(result_db.get(), db.get());
    EXPECT_THAT(result_db, DataBagEqual(*expected_db));
    EXPECT_THAT(leaf_collector.collected_items,
                UnorderedElementsAre(obj_a, obj_b));
  }
  {  // max_depth = 0
    DataBagImplPtr expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples_level0);
    SetDataTriples(*expected_db, data_triples_level0);
    SetDataTriples(*expected_db, data_triples_level1);

    DataBagImplPtr result_db = DataBagImpl::CreateEmptyDatabag();
    LeafCollector leaf_collector;

    ASSERT_OK(ExtractOp(result_db.get())(
        root_ds, obj_dtype, *GetMainDb(db), {GetFallbackDb(db).get()}, nullptr,
        {}, /*max_depth=*/0, leaf_collector.GetCallback()));

    ASSERT_NE(result_db.get(), db.get());
    EXPECT_THAT(result_db, DataBagEqual(*expected_db));
    EXPECT_THAT(leaf_collector.collected_items,
                UnorderedElementsAre(list_a, list_b));
  }
}

TEST_P(ExtractTest, MaxDepthSliceOfListsMultipleAllocation) {
  const DataItem obj_dtype = DataItem(schema::kObject);
  const DataItem int_dtype = DataItem(schema::kInt32);
  const DataItem text_dtype = DataItem(schema::kString);
  DataBagImplPtr db = DataBagImpl::CreateEmptyDatabag();

  // Schema triples.
  const DataItem root_schema = obj_dtype;
  const AllocationId list_schemas =
      AllocateExplicitSchemas(kSmallAllocMaxCapacity + 1);
  const DataItem list_schema_a = DataItem(list_schemas.ObjectByOffset(0));
  const DataItem list_schema_b = DataItem(list_schemas.ObjectByOffset(1));
  const DataItem obj_level2_schema1 = AllocateSchema();
  const DataItem obj_level2_schema2 = AllocateSchema();
  const TriplesT schema_triples_level0 = {
      {list_schema_a, {{schema::kListItemsSchemaAttr, obj_level2_schema1}}},
      {list_schema_b, {{schema::kListItemsSchemaAttr, obj_level2_schema2}}},
  };
  const TriplesT schema_triples_level1 = {
      {obj_level2_schema1, {{"x", int_dtype}, {"y", int_dtype}}},
      {obj_level2_schema2, {{"x", int_dtype}, {"y", int_dtype}}},
  };
  const TriplesT schema_triples_level2 = {};

  // Data triples.
  const AllocationId lists_alloc = AllocateLists(2);
  const DataSliceImpl root_ds =
      DataSliceImpl::ObjectsFromAllocation(lists_alloc, 2);
  const DataItem list_a = root_ds[0];
  const DataItem list_b = root_ds[1];
  const DataSliceImpl obj_ids = AllocateEmptyObjects(2);
  const DataItem obj_a = obj_ids[0];
  const DataItem obj_b = obj_ids[1];

  const TriplesT data_triples_level0 = {
      {list_a, {{schema::kSchemaAttr, list_schema_a}}},
      {list_b, {{schema::kSchemaAttr, list_schema_b}}},
  };

  const TriplesT data_triples_level1 = {};

  const TriplesT data_triples_level2 = {
      {obj_a, {{"x", DataItem(12)}, {"y", DataItem(34)}}},
      {obj_b, {{"x", DataItem(56)}, {"y", DataItem(78)}}},
  };

  // Lists values.
  const DataSliceImpl list_a_values =
      DataSliceImpl::Create(CreateDenseArray<DataItem>({obj_a}));

  const DataSliceImpl list_b_values =
      DataSliceImpl::Create(CreateDenseArray<DataItem>({obj_b}));

  SetSchemaTriples(*db, schema_triples_level0);
  SetSchemaTriples(*db, schema_triples_level1);
  SetSchemaTriples(*db, schema_triples_level2);
  SetDataTriples(*db, data_triples_level0);
  SetDataTriples(*db, data_triples_level1);
  SetDataTriples(*db, data_triples_level2);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  SetSingleListValues(db, list_a, list_a_values);
  SetSingleListValues(db, list_b, list_b_values);

  // Everything extracted.
  {
    DataBagImplPtr expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples_level0);
    SetSchemaTriples(*expected_db, schema_triples_level1);
    SetSchemaTriples(*expected_db, schema_triples_level2);
    SetDataTriples(*expected_db, data_triples_level0);
    SetDataTriples(*expected_db, data_triples_level1);
    SetDataTriples(*expected_db, data_triples_level2);
    SetSingleListValues(expected_db, list_a, list_a_values);
    SetSingleListValues(expected_db, list_b, list_b_values);
    {  // default max_depth, i.e. -1
      DataBagImplPtr result_db = DataBagImpl::CreateEmptyDatabag();
      ASSERT_OK(ExtractOp(result_db.get())(root_ds, obj_dtype, *GetMainDb(db),
                                           {GetFallbackDb(db).get()}, nullptr,
                                           {}));

      ASSERT_NE(result_db.get(), db.get());
      EXPECT_THAT(result_db, DataBagEqual(*expected_db));
    }
    {  // max_depth == maximum dataslice depth
      DataBagImplPtr result_db = DataBagImpl::CreateEmptyDatabag();
      LeafCollector leaf_collector;

      ASSERT_OK(ExtractOp(result_db.get())(
          root_ds, obj_dtype, *GetMainDb(db), {GetFallbackDb(db).get()},
          nullptr, {}, /*max_depth=*/2, leaf_collector.GetCallback()));

      ASSERT_NE(result_db.get(), db.get());
      EXPECT_THAT(result_db, DataBagEqual(*expected_db));
      EXPECT_THAT(leaf_collector.collected_items, IsEmpty());
    }
  }
  {  // max_depth = 1
    DataBagImplPtr expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples_level0);
    SetSchemaTriples(*expected_db, schema_triples_level1);
    SetDataTriples(*expected_db, data_triples_level0);
    SetDataTriples(*expected_db, data_triples_level1);
    SetSingleListValues(expected_db, list_a, list_a_values);
    SetSingleListValues(expected_db, list_b, list_b_values);

    DataBagImplPtr result_db = DataBagImpl::CreateEmptyDatabag();
    LeafCollector leaf_collector;

    ASSERT_OK(ExtractOp(result_db.get())(
        root_ds, obj_dtype, *GetMainDb(db), {GetFallbackDb(db).get()}, nullptr,
        {}, /*max_depth=*/1, leaf_collector.GetCallback()));

    ASSERT_NE(result_db.get(), db.get());
    EXPECT_THAT(result_db, DataBagEqual(*expected_db));
    EXPECT_THAT(leaf_collector.collected_items,
                UnorderedElementsAre(obj_a, obj_b));
  }
  {  // max_depth = 0
    DataBagImplPtr expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples_level0);
    SetDataTriples(*expected_db, data_triples_level0);
    SetDataTriples(*expected_db, data_triples_level1);

    DataBagImplPtr result_db = DataBagImpl::CreateEmptyDatabag();
    LeafCollector leaf_collector;

    ASSERT_OK(ExtractOp(result_db.get())(
        root_ds, obj_dtype, *GetMainDb(db), {GetFallbackDb(db).get()}, nullptr,
        {}, /*max_depth=*/0, leaf_collector.GetCallback()));

    ASSERT_NE(result_db.get(), db.get());
    EXPECT_THAT(result_db, DataBagEqual(*expected_db));
    EXPECT_THAT(leaf_collector.collected_items,
                UnorderedElementsAre(list_a, list_b));
  }
}

TEST_P(ExtractTest, MaxDepth) {
  const DataItem obj_dtype = DataItem(schema::kObject);
  const DataItem int_dtype = DataItem(schema::kInt32);
  DataBagImplPtr db = DataBagImpl::CreateEmptyDatabag();

  // Schema triples.
  const DataItem root_schema = AllocateSchema();
  const DataItem obj_level1_schema = AllocateSchema();
  const DataItem dict_level1_schema = AllocateSchema();
  const DataItem dict_level2_schema = AllocateSchema();
  const DataItem list_of_lists_level1_schema = AllocateSchema();
  const DataItem list_level2_schema = AllocateSchema();
  const TriplesT schema_triples_level0 = {
      {root_schema,
       {
           {"obj", obj_dtype},
           {"dict", dict_level1_schema},
           {"list_of_lists", list_of_lists_level1_schema},
       }},
  };
  const TriplesT schema_triples_level1 = {
      {dict_level1_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt64)},
        {schema::kDictValuesSchemaAttr, dict_level2_schema}}},
      {list_of_lists_level1_schema,
       {{schema::kListItemsSchemaAttr, list_level2_schema}}},
      {obj_level1_schema, {{"x", int_dtype}, {"y", int_dtype}}},
  };
  const TriplesT schema_triples_level2 = {
      {dict_level2_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt64)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kFloat32)}}},
      {list_level2_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}}};

  // Data triples.
  const DataSliceImpl obj_ids = AllocateEmptyObjects(2);
  const DataItem root_obj = obj_ids[0];
  const DataItem obj_level1 = obj_ids[1];
  const DataSliceImpl obj_ids_level2 = AllocateEmptyObjects(1);
  const DataItem obj_level2 = obj_ids_level2[0];
  const DataItem dict_level1 = DataItem(AllocateSingleDict());
  const DataSliceImpl dicts_level2 = AllocateEmptyDicts(1);
  const DataItem list_of_lists_level1 = DataItem(AllocateSingleList());
  const DataSliceImpl lists_level2 = AllocateEmptyLists(2);

  const TriplesT data_triples_level0 = {
      {root_obj,
       {
           {schema::kSchemaAttr, root_schema},
       }},
  };

  const TriplesT data_triples_level1 = {
      {root_obj,
       {{"obj", obj_level1},
        {"dict", dict_level1},
        {"list_of_lists", list_of_lists_level1}}},
      {obj_level1, {{schema::kSchemaAttr, obj_level1_schema}}},
  };

  const TriplesT data_triples_level2 = {
      {obj_level1, {{"x", DataItem(123)}, {"y", DataItem(456)}}},
  };

  // Dicts/lists values.
  const DataSliceImpl dicts_expanded_level1 =
      DataSliceImpl::Create(CreateDenseArray<DataItem>({dict_level1}));
  const DataSliceImpl keys_level1 =
      DataSliceImpl::Create(CreateDenseArray<int64_t>({123}));

  const DataSliceImpl keys_level2 =
      DataSliceImpl::Create(CreateDenseArray<int64_t>({456}));
  const DataSliceImpl values_level2 =
      DataSliceImpl::Create(CreateDenseArray<float>({789.0}));

  const DataSliceImpl list_values_level2 =
      DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2, 3, 4, 5, 6, 7}));

  SetSchemaTriples(*db, schema_triples_level0);
  SetSchemaTriples(*db, schema_triples_level1);
  SetSchemaTriples(*db, schema_triples_level2);
  SetDataTriples(*db, data_triples_level0);
  SetDataTriples(*db, data_triples_level1);
  SetDataTriples(*db, data_triples_level2);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  ASSERT_OK(db->SetInDict(dicts_expanded_level1, keys_level1, dicts_level2));
  ASSERT_OK(db->SetInDict(dicts_level2, keys_level2, values_level2));

  SetSingleListValues(db, list_of_lists_level1, lists_level2);
  SetListValues(db, lists_level2, list_values_level2);

  // Everything extracted.
  {
    DataBagImplPtr expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples_level0);
    SetSchemaTriples(*expected_db, schema_triples_level1);
    SetSchemaTriples(*expected_db, schema_triples_level2);
    SetDataTriples(*expected_db, data_triples_level0);
    SetDataTriples(*expected_db, data_triples_level1);
    SetDataTriples(*expected_db, data_triples_level2);
    ASSERT_OK(expected_db->SetInDict(dicts_expanded_level1, keys_level1,
                                     dicts_level2));
    ASSERT_OK(expected_db->SetInDict(dicts_level2, keys_level2, values_level2));

    SetSingleListValues(expected_db, list_of_lists_level1, lists_level2);
    SetListValues(expected_db, lists_level2, list_values_level2);
    {  // default max_depth, i.e. -1
      DataBagImplPtr result_db = DataBagImpl::CreateEmptyDatabag();

      ASSERT_OK(ExtractOp(result_db.get())(root_obj, obj_dtype, *GetMainDb(db),
                                           {GetFallbackDb(db).get()}, nullptr,
                                           {}));

      ASSERT_NE(result_db.get(), db.get());
      EXPECT_THAT(result_db, DataBagEqual(*expected_db));
    }
    {  // max_depth == maximum dataslice depth
      DataBagImplPtr result_db = DataBagImpl::CreateEmptyDatabag();
      LeafCollector leaf_collector;

      ASSERT_OK(ExtractOp(result_db.get())(
          root_obj, obj_dtype, *GetMainDb(db), {GetFallbackDb(db).get()},
          nullptr, {}, /*max_depth=*/3, leaf_collector.GetCallback()));

      ASSERT_NE(result_db.get(), db.get());
      EXPECT_THAT(result_db, DataBagEqual(*expected_db));
      EXPECT_THAT(leaf_collector.collected_items, IsEmpty());
    }
  }
  // max_depth=2
  {
    DataBagImplPtr expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples_level0);
    SetSchemaTriples(*expected_db, schema_triples_level1);
    SetSchemaTriples(*expected_db, schema_triples_level2);
    SetDataTriples(*expected_db, data_triples_level0);
    SetDataTriples(*expected_db, data_triples_level1);
    SetDataTriples(*expected_db, data_triples_level2);
    ASSERT_OK(expected_db->SetInDict(dicts_expanded_level1, keys_level1,
                                     dicts_level2));
    SetSingleListValues(expected_db, list_of_lists_level1, lists_level2);
    DataBagImplPtr result_db = DataBagImpl::CreateEmptyDatabag();
    LeafCollector leaf_collector;
    ASSERT_OK(ExtractOp(result_db.get())(root_obj, obj_dtype, *GetMainDb(db),
                                         {GetFallbackDb(db).get()}, nullptr, {},
                                         /*max_depth=*/2,
                                         leaf_collector.GetCallback()));

    ASSERT_NE(result_db.get(), db.get());
    EXPECT_THAT(result_db, DataBagEqual(*expected_db));
    EXPECT_THAT(leaf_collector.collected_items,
                UnorderedElementsAre(dicts_level2[0], lists_level2[0],
                                     lists_level2[1]));
  }
  // max_depth=1
  {
    DataBagImplPtr expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples_level0);
    SetSchemaTriples(*expected_db, schema_triples_level1);
    SetDataTriples(*expected_db, data_triples_level0);
    SetDataTriples(*expected_db, data_triples_level1);
    DataBagImplPtr result_db = DataBagImpl::CreateEmptyDatabag();
    LeafCollector leaf_collector;
    ASSERT_OK(ExtractOp(result_db.get())(root_obj, obj_dtype, *GetMainDb(db),
                                         {GetFallbackDb(db).get()}, nullptr, {},
                                         /*max_depth=*/1,
                                         leaf_collector.GetCallback()));

    ASSERT_NE(result_db.get(), db.get());
    EXPECT_THAT(result_db, DataBagEqual(*expected_db));
    EXPECT_THAT(
        leaf_collector.collected_items,
        UnorderedElementsAre(dict_level1, list_of_lists_level1, obj_level1));
  }
  // max_depth=0
  {
    DataBagImplPtr expected_db = DataBagImpl::CreateEmptyDatabag();
    SetSchemaTriples(*expected_db, schema_triples_level0);
    SetDataTriples(*expected_db, data_triples_level0);
    DataBagImplPtr result_db = DataBagImpl::CreateEmptyDatabag();
    LeafCollector leaf_collector;
    ASSERT_OK(ExtractOp(result_db.get())(root_obj, obj_dtype, *GetMainDb(db),
                                         {GetFallbackDb(db).get()}, nullptr, {},
                                         /*max_depth=*/0,
                                         leaf_collector.GetCallback()));

    ASSERT_NE(result_db.get(), db.get());
    EXPECT_THAT(result_db, DataBagEqual(*expected_db));
    EXPECT_THAT(leaf_collector.collected_items, UnorderedElementsAre(root_obj));
  }
}

TEST_P(ExtractTest, DataSliceObjectIds) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"next", schema}, {"prev", schema}}}};
  TriplesT data_triples = {{a0, {{"prev", a2}, {"next", a1}}},
                           {a1, {{"prev", a0}, {"next", a2}}},
                           {a2, {{"prev", a1}, {"next", a0}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(obj_ids, schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DataSliceObjectSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto schema0 = AllocateSchema();
  auto schema1 = AllocateSchema();
  auto schema2 = AllocateSchema();
  auto obj_dtype = DataItem(schema::kObject);

  TriplesT schema_triples = {
      {schema0, {{"next", obj_dtype}}},
      {schema1, {{"prev", obj_dtype}, {"next", obj_dtype}}},
      {schema2, {{"prev", obj_dtype}}}};
  TriplesT data_triples = {
      {a0, {{schema::kSchemaAttr, schema0}, {"next", a1}}},
      {a1, {{schema::kSchemaAttr, schema1}, {"prev", a0}, {"next", a2}}},
      {a2, {{schema::kSchemaAttr, schema2}, {"prev", a1}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(DataItem(a0), obj_dtype, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DataSliceListsPrimitives) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto lists = AllocateEmptyLists(3);
  auto values =
      DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2, 3, 4, 5, 6, 7}));
  ASSERT_OK_AND_ASSIGN(auto edge, arolla::DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 3, 5, 7})));
  ASSERT_OK(db->ExtendLists(lists, values, edge));
  auto list_schema = AllocateSchema();
  TriplesT schema_triples = {
      {list_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->ExtendLists(lists, values, edge));
  SetSchemaTriples(*expected_db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(lists, list_schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DataSliceListsObjectIds) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(7);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto a4 = obj_ids[4];
  auto a5 = obj_ids[5];
  auto a6 = obj_ids[6];
  auto lists = AllocateEmptyLists(3);
  auto values = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({a0, a1, a2, a3, a4, a5, a6}));
  ASSERT_OK_AND_ASSIGN(auto edge, arolla::DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 3, 5, 7})));
  ASSERT_OK(db->ExtendLists(lists, values, edge));
  TriplesT data_triples = {{a0, {{"x", DataItem(0)}, {"y", DataItem(0)}}},
                           {a1, {{"x", DataItem(0)}, {"y", DataItem(1)}}},
                           {a2, {{"x", DataItem(0)}, {"y", DataItem(2)}}},
                           {a3, {{"x", DataItem(1)}, {"y", DataItem(0)}}},
                           {a4, {{"x", DataItem(1)}, {"y", DataItem(1)}}},
                           {a5, {{"x", DataItem(2)}, {"y", DataItem(0)}}},
                           {a6, {{"x", DataItem(2)}, {"y", DataItem(1)}}}};
  auto list_schema = AllocateSchema();
  auto point_schema = AllocateSchema();
  TriplesT schema_triples = {
      {point_schema,
       {{"x", DataItem(schema::kInt32)}, {"y", DataItem(schema::kInt32)}}},
      {list_schema, {{schema::kListItemsSchemaAttr, point_schema}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->ExtendLists(lists, values, edge));
  SetDataTriples(*expected_db, data_triples);
  SetSchemaTriples(*expected_db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(lists, list_schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DataSliceListsObjectIdsObjectSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(7);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto a4 = obj_ids[4];
  auto a5 = obj_ids[5];
  auto a6 = obj_ids[6];
  auto lists = AllocateEmptyLists(3);
  auto values = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({a0, a1, a2, a3, a4, a5, a6}));
  ASSERT_OK_AND_ASSIGN(auto edge, arolla::DenseArrayEdge::FromSplitPoints(
                                      CreateDenseArray<int64_t>({0, 3, 5, 7})));
  ASSERT_OK(db->ExtendLists(lists, values, edge));
  auto list_schema = AllocateSchema();
  auto point_schema = AllocateSchema();
  std::pair<std::string_view, DataItem> object_schema_attr = {
      schema::kSchemaAttr, point_schema};
  TriplesT data_triples = {
      {a0, {object_schema_attr, {"x", DataItem(0)}, {"y", DataItem(0)}}},
      {a1, {object_schema_attr, {"x", DataItem(0)}, {"y", DataItem(1)}}},
      {a2, {object_schema_attr, {"x", DataItem(0)}, {"y", DataItem(2)}}},
      {a3, {object_schema_attr, {"x", DataItem(1)}, {"y", DataItem(0)}}},
      {a4, {object_schema_attr, {"x", DataItem(1)}, {"y", DataItem(1)}}},
      {a5, {object_schema_attr, {"x", DataItem(2)}, {"y", DataItem(0)}}},
      {a6, {object_schema_attr, {"x", DataItem(2)}, {"y", DataItem(1)}}}};
  TriplesT schema_triples = {
      {point_schema,
       {{"x", DataItem(schema::kInt32)}, {"y", DataItem(schema::kInt32)}}},
      {list_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kObject)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->ExtendLists(lists, values, edge));
  SetDataTriples(*expected_db, data_triples);
  SetSchemaTriples(*expected_db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(lists, list_schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DataSliceDictsPrimitives) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dicts = AllocateEmptyDicts(3);
  auto dicts_expanded = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {dicts[0], dicts[0], dicts[0], dicts[1], dicts[1], dicts[2], dicts[2]}));
  auto keys =
      DataSliceImpl::Create(CreateDenseArray<int64_t>({1, 2, 3, 1, 5, 3, 7}));
  auto values =
      DataSliceImpl::Create(CreateDenseArray<float>({1, 2, 3, 4, 5, 6, 7}));
  ASSERT_OK(db->SetInDict(dicts_expanded, keys, values));
  auto dict_schema = AllocateSchema();
  TriplesT schema_triples = {
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt64)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kFloat32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->SetInDict(dicts_expanded, keys, values));
  SetSchemaTriples(*expected_db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(dicts, dict_schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DataSliceDictsObjectIds) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dicts = AllocateEmptyDicts(3);
  auto obj_ids = AllocateEmptyObjects(7);
  auto k0 = obj_ids[0];
  auto k1 = obj_ids[1];
  auto k2 = obj_ids[2];
  auto k3 = obj_ids[3];
  auto v0 = obj_ids[4];
  auto v1 = obj_ids[5];
  auto v2 = obj_ids[6];

  auto dicts_expanded = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {dicts[0], dicts[0], dicts[0], dicts[1], dicts[1], dicts[2], dicts[2]}));
  auto keys = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({k0, k1, k2, k0, k3, k0, k2}));
  auto values = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({v0, v0, v0, v1, v2, DataItem(), DataItem()}));
  ASSERT_OK(db->SetInDict(dicts_expanded, keys, values));
  auto dict_schema = AllocateSchema();
  auto key_schema = AllocateSchema();
  auto value_schema = AllocateSchema();
  TriplesT data_triples = {{k0, {{"x", DataItem(0)}, {"y", DataItem(0)}}},
                           {k1, {{"x", DataItem(0)}, {"y", DataItem(1)}}},
                           {k2, {{"x", DataItem(0)}, {"y", DataItem(2)}}},
                           {k3, {{"x", DataItem(1)}, {"y", DataItem(0)}}},
                           {v0, {{"val", DataItem(1.5)}}},
                           {v1, {{"val", DataItem(2.0)}}},
                           {v2, {{"val", DataItem(2.5)}}}};
  TriplesT schema_triples = {
      {key_schema,
       {{"x", DataItem(schema::kInt32)}, {"y", DataItem(schema::kInt32)}}},
      {value_schema, {{"val", DataItem(schema::kFloat64)}}},
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, key_schema},
        {schema::kDictValuesSchemaAttr, value_schema}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->SetInDict(dicts_expanded, keys, values));
  SetDataTriples(*expected_db, data_triples);
  SetSchemaTriples(*expected_db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(dicts, dict_schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DataSliceDictsObjectIdsObjectSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dicts = AllocateEmptyDicts(3);
  auto obj_ids = AllocateEmptyObjects(7);
  auto k0 = obj_ids[0];
  auto k1 = obj_ids[1];
  auto k2 = obj_ids[2];
  auto k3 = obj_ids[3];
  auto v0 = obj_ids[4];
  auto v1 = obj_ids[5];
  auto v2 = obj_ids[6];

  auto dicts_expanded = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {dicts[0], dicts[0], dicts[0], dicts[1], dicts[1], dicts[2], dicts[2]}));
  auto keys = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({k0, k1, k2, k0, k3, k0, k2}));
  auto values = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({v0, v0, v0, v1, v2, DataItem(), DataItem()}));
  ASSERT_OK(db->SetInDict(dicts_expanded, keys, values));
  auto key_schema = AllocateSchema();
  auto value_schema = AllocateSchema();
  auto dict_schema = AllocateSchema();
  std::pair<std::string_view, DataItem> key_schema_attr = {schema::kSchemaAttr,
                                                           key_schema};
  std::pair<std::string_view, DataItem> value_schema_attr = {
      schema::kSchemaAttr, value_schema};
  TriplesT data_triples = {
      {k0, {key_schema_attr, {"x", DataItem(0)}, {"y", DataItem(0)}}},
      {k1, {key_schema_attr, {"x", DataItem(0)}, {"y", DataItem(1)}}},
      {k2, {key_schema_attr, {"x", DataItem(0)}, {"y", DataItem(2)}}},
      {k3, {key_schema_attr, {"x", DataItem(1)}, {"y", DataItem(0)}}},
      {v0, {value_schema_attr, {"val", DataItem(1.5)}}},
      {v1, {value_schema_attr, {"val", DataItem(2.0)}}},
      {v2, {value_schema_attr, {"val", DataItem(2.5)}}}};
  TriplesT schema_triples = {
      {key_schema,
       {{"x", DataItem(schema::kInt32)}, {"y", DataItem(schema::kInt32)}}},
      {value_schema, {{"val", DataItem(schema::kFloat64)}}},
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kObject)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kObject)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->SetInDict(dicts_expanded, keys, values));
  SetDataTriples(*expected_db, data_triples);
  SetSchemaTriples(*expected_db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(dicts, dict_schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DataSliceDicts_LoopSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dicts = AllocateEmptyDicts(3);
  auto objs = AllocateEmptyObjects(3);
  auto k0 = objs[0];
  auto k1 = objs[1];
  auto k2 = objs[2];
  auto dicts_expanded = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {dicts[0], dicts[0], dicts[0], dicts[1], dicts[1], dicts[2], dicts[2]}));
  auto keys = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({k0, k1, k2, k1, k2, k0, k2}));
  auto values = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {dicts[0], dicts[1], dicts[2], dicts[1], dicts[2], dicts[0], dicts[2]}));
  ASSERT_OK(db->SetInDict(dicts_expanded, keys, values));
  TriplesT data_triples = {
      {k0, {{"x", DataItem(0)}}},
      {k1, {{"x", DataItem(1)}}},
      {k2, {{"x", DataItem(2)}}},
  };
  auto key_schema = AllocateSchema();
  auto dict_schema = AllocateSchema();
  TriplesT schema_triples = {{key_schema, {{"x", DataItem(schema::kInt32)}}},
                             {dict_schema,
                              {{schema::kDictKeysSchemaAttr, key_schema},
                               {schema::kDictValuesSchemaAttr, dict_schema}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->SetInDict(dicts_expanded, keys, values));
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(dicts[0], dict_schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DataSliceDicts_LoopSchema_NoData) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto key_schema = AllocateSchema();
  auto dict_schema = AllocateSchema();
  TriplesT schema_triples = {{key_schema, {{"x", DataItem(schema::kInt64)}}},
                             {dict_schema,
                              {{schema::kDictKeysSchemaAttr, key_schema},
                               {schema::kDictValuesSchemaAttr, dict_schema}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(DataItem(), dict_schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DataSliceLists_LoopSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto lists = AllocateEmptyLists(3);
  auto values = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({lists[1], lists[2], lists[0]}));
  ASSERT_OK(db->AppendToList(lists, values));
  auto list_schema = AllocateSchema();
  TriplesT schema_triples = {
      {list_schema, {{schema::kListItemsSchemaAttr, list_schema}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->AppendToList(lists, values));
  SetSchemaTriples(*expected_db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(lists[0], list_schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DataSliceDicts_InvalidSchema_MissingKeys) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dict_schema = AllocateSchema();
  TriplesT schema_triples = {
      {dict_schema,
       {{schema::kDictValuesSchemaAttr, DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ExtractOp(result_db.get())(DataItem(), dict_schema, *GetMainDb(db),
                                 {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::AllOf(::testing::HasSubstr("dict schema"),
                           ::testing::HasSubstr("has unexpected attributes"))));
}

TEST_P(ExtractTest, DataSliceDicts_InvalidSchema_MissingValues) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dict_schema = AllocateSchema();
  TriplesT schema_triples = {
      {dict_schema, {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ExtractOp(result_db.get())(DataItem(), dict_schema, *GetMainDb(db),
                                 {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::AllOf(::testing::HasSubstr("dict schema"),
                           ::testing::HasSubstr("has unexpected attributes"))));
}

TEST_P(ExtractTest, DataSliceLists_InvalidSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto list_schema = AllocateSchema();
  TriplesT schema_triples = {
      {list_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)},
        {"y", DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ExtractOp(result_db.get())(DataItem(), list_schema, *GetMainDb(db),
                                 {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::AllOf(::testing::HasSubstr("list schema"),
                           ::testing::HasSubstr("has unexpected attributes"))));
}

TEST_P(ExtractTest, DataSliceDicts_InvalidSchema_UnexpectedAttr) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dict_schema = AllocateSchema();
  TriplesT schema_triples = {
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt32)},
        {"x", DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ExtractOp(result_db.get())(DataItem(), dict_schema, *GetMainDb(db),
                                 {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::AllOf(::testing::HasSubstr("dict schema"),
                           ::testing::HasSubstr("has unexpected attributes"))));

  TriplesT schema_add_triples = {
      {dict_schema,
       {{schema::kDictValuesSchemaAttr, DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_add_triples);

  EXPECT_THAT(
      ExtractOp(result_db.get())(DataItem(), dict_schema, *GetMainDb(db),
                                 {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::AllOf(::testing::HasSubstr("dict schema"),
                           ::testing::HasSubstr("has unexpected attributes"))));
}

TEST_P(ExtractTest, ExtractSchemaForEmptySlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(1);
  auto a0 = obj_ids[0];
  auto schema1 = AllocateSchema();
  auto schema2 = AllocateSchema();

  TriplesT schema_triples = {{schema1, {{"next", schema2}}},
                             {schema2, {{"prev", schema1}}}};
  TriplesT data_triples = {};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(obj_ids, schema1, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, RecursiveSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(4);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"next", schema}}}};
  TriplesT data_triples = {
      {a0, {{"next", a1}}}, {a1, {{"next", a2}}}, {a2, {{"next", a3}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(a0, schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, MixedObjectsSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto hidden_obj_ids = AllocateEmptyObjects(2);
  auto a3 = hidden_obj_ids[0];
  auto a4 = hidden_obj_ids[1];
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"next", DataItem(schema::kObject)}}}};
  TriplesT data_triples = {
      {a0, {{"next", a3}}},
      {a1, {{"next", DataItem(3)}}},
      {a2, {{"next", a4}}},
      {a3, {{schema::kSchemaAttr, schema}, {"next", DataItem(5)}}},
      {a4, {{schema::kSchemaAttr, schema}}},
  };
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(obj_ids, schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, EntityAndObjectPathes) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];

  auto schema = AllocateSchema();

  TriplesT schema_triples = {
      {schema, {{"next", DataItem(schema::kObject)}}},
  };
  TriplesT data_triples = {
      {a0, {{"next", a1}}},
      {a1, {{schema::kSchemaAttr, schema}, {"next", a2}}},
      {a2, {{schema::kSchemaAttr, schema}, {"next", DataItem(-1)}}}};
  TriplesT unreachable_data_triples = {{a0, {{schema::kSchemaAttr, schema}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetDataTriples(*db, unreachable_data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(obj_ids, schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, PartialSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(4);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();

  TriplesT schema_triples = {
      {schema, {{"next", schema}, {"x", int_dtype}, {"y", int_dtype}}}};
  TriplesT data_triples = {{a1, {{"next", a2}, {"x", DataItem(1)}}},
                           {a2, {{"next", a3}, {"y", DataItem(5)}}},
                           {a3, {{"x", DataItem(3)}, {"y", DataItem(6)}}}};
  TriplesT unreachable_data_triples = {
      {a0, {{"next", a1}, {"x", DataItem(7)}, {"z", DataItem(4)}}},
      {a1, {{"prev", a0}, {"z", DataItem(5)}}},
      {a3, {{"self", a3}}}};
  TriplesT unreachable_schema_triples = {};

  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, unreachable_data_triples);
  SetDataTriples(*db, data_triples);

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(a1, schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, PartialSchemaWithDifferentDataBag) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema_db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(4);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();
  auto unreachable_schema = AllocateSchema();

  TriplesT schema_triples = {
      {schema, {{"next", schema}, {"x", int_dtype}, {"y", int_dtype}}}};
  TriplesT unreachable_schema_triples = {
      {unreachable_schema, {{"next", unreachable_schema}}}};
  TriplesT noise_schema_triples = {
      {schema, {{"next", int_dtype}, {"z", schema}, {"y", int_dtype}}}};
  TriplesT data_triples = {{a1, {{"next", a2}, {"x", DataItem(1)}}},
                           {a2, {{"next", a3}, {"y", DataItem(5)}}},
                           {a3, {{"x", DataItem(3)}, {"y", DataItem(6)}}}};
  TriplesT unreachable_data_triples = {
      {a0, {{"next", a1}, {"x", DataItem(7)}, {"z", DataItem(4)}}},
      {a1, {{"prev", a0}, {"z", DataItem(5)}}},
      {a3, {{"self", a3}}}};

  SetSchemaTriples(*schema_db, schema_triples);
  SetSchemaTriples(*schema_db, unreachable_schema_triples);
  SetSchemaTriples(*db, unreachable_schema_triples);
  SetSchemaTriples(*db, noise_schema_triples);
  SetDataTriples(*db, unreachable_data_triples);
  SetDataTriples(*db, data_triples);

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(
      a1, schema, *GetMainDb(db), {GetFallbackDb(db).get()},
      &*GetMainDb(schema_db),
      DataBagImpl::FallbackSpan({GetFallbackDb(schema_db).get()})));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, ExtendedSchemaWithDifferentDataBag) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema_db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto oth_obj_ids = AllocateEmptyObjects(2);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto b0 = oth_obj_ids[0];
  auto b1 = oth_obj_ids[1];
  auto object_dtype = DataItem(schema::kObject);
  auto int_dtype = DataItem(schema::kInt32);
  auto schemas = AllocateExplicitSchemas(kSmallAllocMaxCapacity + 1);
  auto schema = DataItem(schemas.ObjectByOffset(0));
  auto missing_schema = DataItem(schemas.ObjectByOffset(1));

  TriplesT extended_schema_triples = {
      {schema, {{"next", schema}, {"b", object_dtype}, {"y", int_dtype}}}};
  TriplesT schema_triples = {{schema, {{"b", object_dtype}, {"y", int_dtype}}},
                             {missing_schema, {{"next", missing_schema}}}};
  TriplesT data_triples = {
      {a1, {{"next", a2}, {"b", b1}}},
      {a2, {{"y", DataItem(5)}, {"b", b0}}},
      {a3, {{"y", DataItem(6)}}},
      {b0, {{schema::kSchemaAttr, schema}, {"y", DataItem(0)}}},
      {b1, {{schema::kSchemaAttr, schema}, {"y", DataItem(1)}}},
  };

  SetSchemaTriples(*schema_db, extended_schema_triples);
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*schema_db, GenSchemaTriplesFoTests());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, extended_schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(
      obj_ids, schema, *GetMainDb(db), {GetFallbackDb(db).get()},
      &*GetMainDb(schema_db),
      DataBagImpl::FallbackSpan({GetFallbackDb(schema_db).get()})));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, NonReducingSchemaWithDifferentDataBag) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema_db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto oth_obj_ids = AllocateEmptyObjects(2);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto b0 = oth_obj_ids[0];
  auto b1 = oth_obj_ids[1];
  auto object_dtype = DataItem(schema::kObject);
  auto int_dtype = DataItem(schema::kInt32);
  auto schemas = AllocateExplicitSchemas(kSmallAllocMaxCapacity + 1);
  auto schema = DataItem(schemas.ObjectByOffset(0));
  auto schema2 = DataItem(schemas.ObjectByOffset(1));
  auto missing_schema = DataItem(schemas.ObjectByOffset(2));

  TriplesT extended_schema_triples = {
      {schema, {{"next", schema}, {"b", object_dtype}, {"y", int_dtype}}},
      {schema2, {{"next", schema2}, {"b", object_dtype}, {"y", int_dtype}}}};
  TriplesT schema_triples = {
      {schema, {{"b", object_dtype}, {"y", int_dtype}}},
      {schema2, {{"next", schema2}, {"b", object_dtype}, {"y", int_dtype}}},
      {missing_schema, {{"next", missing_schema}}}};
  TriplesT data_triples = {
      {a1, {{"next", a2}, {"b", b1}}},
      {a2, {{"y", DataItem(5)}, {"b", b0}}},
      {a3, {{"y", DataItem(6)}}},
      {b0, {{schema::kSchemaAttr, schema}, {"y", DataItem(0)}}},
      {b1, {{schema::kSchemaAttr, schema2}, {"y", DataItem(1)}}},
  };

  SetSchemaTriples(*schema_db, extended_schema_triples);
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*schema_db, GenSchemaTriplesFoTests());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, extended_schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(
      obj_ids, schema, *GetMainDb(db), {GetFallbackDb(db).get()},
      &*GetMainDb(schema_db),
      DataBagImpl::FallbackSpan({GetFallbackDb(schema_db).get()})));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DifferentSchemasInOneAllocation) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema_db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto object_dtype = DataItem(schema::kObject);
  auto int_dtype = DataItem(schema::kInt32);
  auto schemas = AllocateExplicitSchemas(kSmallAllocMaxCapacity + 1);
  auto schema0 = DataItem(schemas.ObjectByOffset(0));
  auto schema1 = DataItem(schemas.ObjectByOffset(1));

  TriplesT data_triples = {
      {a0, {{schema::kSchemaAttr, schema0}, {"c", DataItem(1)}}},
      {a1, {{schema::kSchemaAttr, schema1}, {"a", a1}}},
  };
  TriplesT schema_triples = {
      {schema0, {{"a", schema0}, {"b", object_dtype}, {"c", int_dtype}}},
      {schema1, {{"a", schema1}, {"x", object_dtype}, {"c", object_dtype}}}};

  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*schema_db, GenSchemaTriplesFoTests());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetDataTriples(*expected_db, data_triples);
  SetSchemaTriples(*expected_db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(obj_ids, object_dtype, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, MergeSchemaFromTwoDatabags) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema_db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(4);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto int_dtype = DataItem(schema::kInt32);
  auto object_dtype = DataItem(schema::kObject);
  auto schema = AllocateSchema();
  auto unreachable_schema = AllocateSchema();

  TriplesT data_triples = {
      {a0, {{"next", a1}, {"x", DataItem(1)}}},
      {a1, {{schema::kSchemaAttr, DataItem(schema)}, {"y", DataItem(4)}}},
  };
  TriplesT unreachable_data_triples = {
      {a0, {{"y", DataItem(2)}}},  // TODO: should be extracted
      {a1, {{"x", DataItem(3)}}},
  };
  TriplesT data_db_schema_triples = {
      {schema, {{"next", object_dtype}, {"y", int_dtype}}}};
  TriplesT schema_db_schema_triples = {
      {schema, {{"next", object_dtype}, {"x", int_dtype}}}};

  SetDataTriples(*db, data_triples);
  SetDataTriples(*db, unreachable_data_triples);
  SetSchemaTriples(*db, data_db_schema_triples);
  SetSchemaTriples(*schema_db, schema_db_schema_triples);

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, data_db_schema_triples);
  SetSchemaTriples(*expected_db, schema_db_schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(
      a0, schema, *GetMainDb(db), {GetFallbackDb(db).get()},
      &*GetMainDb(schema_db),
      DataBagImpl::FallbackSpan({GetFallbackDb(schema_db).get()})));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, ConflictingSchemasInTwoDatabags) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema_db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(4);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto text_dtype = DataItem(schema::kString);
  auto int_dtype = DataItem(schema::kInt32);
  auto object_dtype = DataItem(schema::kObject);
  auto schema = AllocateSchema();
  auto unreachable_schema = AllocateSchema();

  TriplesT data_triples = {
      {a0, {{"next", a1}}},
      {a1, {{schema::kSchemaAttr, DataItem(schema)}}},
  };
  TriplesT schema_triples = {
      {schema, {{"next", object_dtype}, {"x", text_dtype}}}};
  TriplesT schema_db_triples = {
      {schema, {{"next", object_dtype}, {"x", int_dtype}}}};

  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*schema_db, schema_db_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ExtractOp(result_db.get())(
          a0, schema, *GetMainDb(db), {GetFallbackDb(db).get()},
          &*GetMainDb(schema_db),
          DataBagImpl::FallbackSpan({GetFallbackDb(schema_db).get()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::AllOf(
              ::testing::HasSubstr("conflicting values for some of schemas"),
              ::testing::HasSubstr("x: [INT32] != [STRING]"))));
}

TEST_P(ExtractTest, ConflictingSchemaNamesInTwoDatabags) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema_db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(4);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto text_dtype = DataItem(schema::kString);
  auto int_dtype = DataItem(schema::kInt32);
  auto object_dtype = DataItem(schema::kObject);
  auto schema = AllocateSchema();
  auto unreachable_schema = AllocateSchema();

  TriplesT data_triples = {
      {a0, {{"next", a1}}},
      {a1, {{schema::kSchemaAttr, DataItem(schema)}}},
  };
  TriplesT schema_triples = {
      {schema,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("schema"))},
        {"next", object_dtype},
        {"x", text_dtype}}}};
  TriplesT schema_db_triples = {
      {schema,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("foo"))},
        {"next", object_dtype},
        {"x", text_dtype}}}};

  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*schema_db, schema_db_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ExtractOp(result_db.get())(
          a0, schema, *GetMainDb(db), {GetFallbackDb(db).get()},
          &*GetMainDb(schema_db),
          DataBagImpl::FallbackSpan({GetFallbackDb(schema_db).get()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ::testing::AllOf(
              ::testing::HasSubstr("conflicting values for some of schemas"),
              ::testing::HasSubstr("__schema_name__: ['foo'] != ['schema']"))));
}

TEST_P(ExtractTest, NoFollowEntitySchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(4);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema1 = AllocateSchema();
  auto schema2 = AllocateSchema();
  ASSERT_OK_AND_ASSIGN(auto nofollow_schema2,
                       schema::NoFollowSchemaItem(schema2));

  TriplesT schema_triples = {
      {schema1,
       {{"nofollow", nofollow_schema2}, {"x", int_dtype}, {"y", int_dtype}}},
  };
  TriplesT data_triples = {{a1, {{"nofollow", a2}, {"x", DataItem(1)}}}};
  TriplesT unreachable_data_triples = {
      {a0, {{"nofollow", a1}, {"x", DataItem(7)}, {"z", DataItem(4)}}},
      {a1, {{"prev", a0}, {"z", DataItem(5)}}},
      {a2, {{"nofollow", a3}, {"y", DataItem(5)}}},
      {a3, {{"self", a3}, {"x", DataItem(3)}, {"y", DataItem(6)}}}};
  TriplesT unreachable_schema_triples = {
      {nofollow_schema2,
       {{"nofollow", schema1}, {"x", int_dtype}, {"y", int_dtype}}}};

  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, unreachable_schema_triples);
  SetDataTriples(*db, unreachable_data_triples);
  SetDataTriples(*db, data_triples);

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(a1, schema1, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, NoFollowObjectSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto schema0 = AllocateSchema();
  auto schema1 = AllocateSchema();
  auto schema2 = AllocateSchema();
  ASSERT_OK_AND_ASSIGN(auto nofollow_schema1,
                       schema::NoFollowSchemaItem(schema1));
  auto obj_dtype = DataItem(schema::kObject);

  TriplesT schema_triples = {{schema0, {{"nofollow", obj_dtype}}}};
  TriplesT unreachable_schema_triples = {
      {nofollow_schema1, {{"prev", obj_dtype}, {"next", obj_dtype}}},
      {schema2, {{"prev", obj_dtype}}}};
  TriplesT data_triples = {
      {a0, {{schema::kSchemaAttr, schema0}, {"nofollow", a1}}},
      {a1, {{schema::kSchemaAttr, nofollow_schema1}}}};
  TriplesT unreachable_data_triples = {
      {a1,
       {{schema::kSchemaAttr, nofollow_schema1}, {"prev", a0}, {"next", a2}}},
      {a2, {{schema::kSchemaAttr, schema2}, {"prev", a1}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, unreachable_schema_triples);
  SetDataTriples(*db, data_triples);
  SetDataTriples(*db, unreachable_data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(DataItem(a0), obj_dtype, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, SchemaAsData) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto schema = AllocateSchema();
  auto obj_dtype = DataItem(schema::kObject);

  TriplesT schema_triples = {{schema, {{"x", schema}}}};
  TriplesT data_triples = {
      {a0, {{schema::kSchemaAttr, schema}, {"x", a1}}},
      {a1, {{schema::kSchemaAttr, schema}, {"x", a0}}},
      {schema, {{schema::kSchemaAttr, DataItem(schema::kSchema)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ExtractOp(result_db.get())(
          DataSliceImpl::Create(CreateDenseArray<DataItem>({a0, a1, schema})),
          obj_dtype, *GetMainDb(db), {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::HasSubstr(
                   "unsupported schema found during extract/clone")));
}

TEST_P(ExtractTest, SchemaSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s1 = AllocateSchema();
  auto s2 = AllocateSchema();
  TriplesT schema_triples = {
      {s1, {{"x", DataItem(schema::kInt32)}}},
      {s2, {{"a", DataItem(schema::kString)}}},
  };
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({s1, s2}));

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(ds, DataItem(schema::kSchema),
                                       *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, NamedSchemaSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s1 = AllocateSchema();
  auto s2 = AllocateSchema();
  auto s3 = AllocateSchema();
  TriplesT schema_triples = {
      {s1,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("s1"))},
        {"x", DataItem(schema::kInt32)}}},
      {s2,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("s2"))},
        {"a", DataItem(schema::kString)}}},
      {s3, {{"a", DataItem(schema::kString)}}},
  };
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);

  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({s1, s2, s3}));

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(ds, DataItem(schema::kSchema),
                                       *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, NamedSchemaWithDatabag) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema_db = DataBagImpl::CreateEmptyDatabag();
  auto s1 = AllocateSchema();
  auto s2 = AllocateSchema();
  TriplesT schema_triples = {
      {s1,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("s1"))}, {"x", s2}}},
      {s2,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("s2"))},
        {"a", DataItem(schema::kString)}}}};
  SetSchemaTriples(*schema_db, schema_triples);
  SetSchemaTriples(*schema_db, GenSchemaTriplesFoTests());
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*schema_db, GenDataTriplesForTest());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(
      DataItem(), s1, *GetMainDb(db), {GetFallbackDb(db).get()},
      &*GetMainDb(schema_db), {GetFallbackDb(schema_db).get()}));

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, NamedSchemaObjects) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s1 = AllocateSchema();
  auto s2 = AllocateSchema();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  TriplesT schema_triples = {
      {s1,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("s1"))},
        {"x", DataItem(schema::kObject)}}},
      {s2,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("s2"))},
        {"a", DataItem(schema::kString)}}}};
  TriplesT data_triples = {
      {a1, {{schema::kSchemaAttr, s1}, {"x", a2}}},
      {a2, {{schema::kSchemaAttr, s1}, {"x", a0}}},
      {a0, {{schema::kSchemaAttr, s2}, {"a", DataItem(arolla::Text("foo"))}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(a1, DataItem(schema::kObject),
                                       *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, ObjectSchemaMissing) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"x", DataItem(schema::kInt32)}}}};
  TriplesT data_triples = {
      {obj_ids[2], {{schema::kSchemaAttr, schema}, {"x", DataItem(1)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(obj_ids, DataItem(schema::kObject),
                                       *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, ObjectSchemaAllMissing) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);

  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(obj_ids, DataItem(schema::kObject),
                                       *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, InvalidSchemaType) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto schema = DataItem(1);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ExtractOp(result_db.get())(obj_ids, schema, *GetMainDb(db),
                                 {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(absl::StatusCode::kInternal, "unsupported schema type"));
}

TEST_P(ExtractTest, InvalidPrimitiveType) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(2);
  auto a0 = obj_ids[0];
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"x", DataItem(schema::kInt32)}}}};
  TriplesT data_triples = {{a0, {{"x", DataItem(arolla::Text("foo"))}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ExtractOp(result_db.get())(a0, schema, *GetMainDb(db),
                                 {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "during extract/clone, got a slice with primitive type INT32 "
               "while the actual content has type STRING"));
}

TEST_P(ExtractTest, InvalidPrimitiveTypeObject) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(2);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"x", DataItem(schema::kInt32)}}}};
  TriplesT data_triples = {{a0, {{"x", a1}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ExtractOp(result_db.get())(a0, schema, *GetMainDb(db),
                                 {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "during extract/clone, got a slice with primitive type INT32 "
               "while the actual content is mixed or not a primitive"));
}

}  // namespace
}  // namespace koladata::internal
