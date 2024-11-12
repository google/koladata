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

#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/new_ids_like.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/testing/matchers.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/fingerprint.h"

namespace koladata::internal {
namespace {

using ::absl_testing::StatusIs;

using ::arolla::CreateDenseArray;
using ::koladata::internal::testing::DataBagEqual;

using TriplesT = std::vector<
    std::pair<DataItem, std::vector<std::pair<std::string_view, DataItem>>>>;

DataItem AllocateSchema() {
  return DataItem(internal::AllocateExplicitSchema());
}

template <typename T>
DataSliceImpl CreateSlice(absl::Span<const arolla::OptionalValue<T>> values) {
  return DataSliceImpl::Create(CreateDenseArray<T>(values));
}

void SetSchemaTriples(DataBagImpl& db, const TriplesT& schema_triples) {
  for (auto [schema, attrs] : schema_triples) {
    for (auto [attr_name, attr_schema] : attrs) {
      EXPECT_OK(db.SetSchemaAttr(schema, attr_name, attr_schema));
    }
  }
}

void SetDataTriples(DataBagImpl& db, const TriplesT& data_triples) {
  for (auto [item, attrs] : data_triples) {
    for (auto [attr_name, attr_data] : attrs) {
      EXPECT_OK(db.SetAttr(item, attr_name, attr_data));
    }
  }
}

TriplesT GenNoiseDataTriples() {
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(5);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto a4 = obj_ids[4];
  TriplesT data = {{a0, {{"x", DataItem(1)}, {"next", a1}}},
                   {a1, {{"y", DataItem(3)}, {"prev", a0}, {"next", a2}}},
                   {a3, {{"x", DataItem(1)}, {"y", DataItem(2)}, {"next", a4}}},
                   {a4, {{"prev", a3}}}};
  return data;
}

TriplesT GenNoiseSchemaTriples() {
  auto schema0 = AllocateSchema();
  auto schema1 = AllocateSchema();
  auto int_dtype = DataItem(schema::kInt32);
  TriplesT schema_triples = {
      {schema0, {{"self", schema0}, {"next", schema1}, {"x", int_dtype}}},
      {schema1, {{"prev", schema0}, {"y", int_dtype}}}};
  return schema_triples;
}

enum ExtractTestParam {kMainDb, kFallbackDb};

class CopyingOpTest : public ::testing::TestWithParam<ExtractTestParam> {
 public:
  DataBagImplPtr GetMainDb(DataBagImplPtr db) {
    switch (GetParam()) {
      case kMainDb:
        return db;
      case kFallbackDb:
        return DataBagImpl::CreateEmptyDatabag();
    }
    DCHECK(false);
  }
  DataBagImplPtr GetFallbackDb(DataBagImplPtr db) {
    switch (GetParam()) {
      case kMainDb:
        return DataBagImpl::CreateEmptyDatabag();
      case kFallbackDb:
        return db;
    }
    DCHECK(false);
  }
};

class ExtractTest : public CopyingOpTest {};

class ShallowCloneTest : public CopyingOpTest {};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, ExtractTest,
                         ::testing::Values(kMainDb, kFallbackDb));

INSTANTIATE_TEST_SUITE_P(MainOrFallback, ShallowCloneTest,
                         ::testing::Values(kMainDb, kFallbackDb));

TEST_P(ShallowCloneTest, ShallowEntitySlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());
  auto itemid = DataSliceImpl::AllocateEmptyObjects(3);

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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(6);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());
  auto itemid = DataSliceImpl::AllocateEmptyObjects(3);

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
  auto lists = DataSliceImpl::ObjectsFromAllocation(AllocateLists(3), 3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());
  auto itemid = DataSliceImpl::ObjectsFromAllocation(AllocateLists(3), 3);

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
  auto lists = DataSliceImpl::ObjectsFromAllocation(AllocateLists(3), 3);
  auto values = DataSliceImpl::AllocateEmptyObjects(7);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());
  auto itemid = DataSliceImpl::ObjectsFromAllocation(AllocateLists(3), 3);

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
  auto dicts = DataSliceImpl::ObjectsFromAllocation(AllocateDicts(3), 3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());
  auto itemid = DataSliceImpl::ObjectsFromAllocation(AllocateDicts(3), 3);

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
  auto dicts = DataSliceImpl::ObjectsFromAllocation(AllocateDicts(3), 3);
  auto keys = DataSliceImpl::AllocateEmptyObjects(4);
  auto values = DataSliceImpl::AllocateEmptyObjects(7);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());
  auto itemid = DataSliceImpl::ObjectsFromAllocation(AllocateDicts(3), 3);

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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(10);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto a4 = obj_ids[4];
  auto a5 = obj_ids[5];
  auto dicts = DataSliceImpl::ObjectsFromAllocation(AllocateDicts(2), 2);
  auto lists = DataSliceImpl::ObjectsFromAllocation(AllocateLists(2), 2);
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
  SetDataTriples(*db, GenNoiseDataTriples());
  SetSchemaTriples(*db, GenNoiseSchemaTriples());

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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(5);
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
  SetDataTriples(*db, GenNoiseDataTriples());
  SetSchemaTriples(*db, GenNoiseSchemaTriples());

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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(5);
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
  SetDataTriples(*db, GenNoiseDataTriples());
  SetSchemaTriples(*db, GenNoiseSchemaTriples());

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
                {{"x", DataItem(schema::kInt32)},
                 {"name", DataItem(schema::kString)}}},
               {result_u2,
                {{"y", DataItem(schema::kInt32)},
                 {"self", DataItem(schema::kObject)}}}};
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());
  auto itemid_alloc = DataSliceImpl::AllocateEmptyObjects(3);
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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());
  auto itemid = DataSliceImpl::AllocateEmptyObjects(3);
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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());
  auto itemid = DataSliceImpl::AllocateEmptyObjects(3);
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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(obj_ids, schema, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  ASSERT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, DataSliceObjectIds) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto lists = DataSliceImpl::ObjectsFromAllocation(AllocateLists(3), 3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(7);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto a4 = obj_ids[4];
  auto a5 = obj_ids[5];
  auto a6 = obj_ids[6];
  auto lists = DataSliceImpl::ObjectsFromAllocation(AllocateLists(3), 3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(7);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto a3 = obj_ids[3];
  auto a4 = obj_ids[4];
  auto a5 = obj_ids[5];
  auto a6 = obj_ids[6];
  auto lists = DataSliceImpl::ObjectsFromAllocation(AllocateLists(3), 3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto dicts = DataSliceImpl::ObjectsFromAllocation(AllocateDicts(3), 3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto dicts = DataSliceImpl::ObjectsFromAllocation(AllocateDicts(3), 3);
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(7);
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
  auto values = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {v0, v0, v0, v1, v2, DataItem(), DataItem()}));
  ASSERT_OK(db->SetInDict(dicts_expanded, keys, values));
  auto dict_schema = AllocateSchema();
  auto key_schema = AllocateSchema();
  auto value_schema = AllocateSchema();
  TriplesT data_triples = {
      {k0, {{"x", DataItem(0)}, {"y", DataItem(0)}}},
      {k1, {{"x", DataItem(0)}, {"y", DataItem(1)}}},
      {k2, {{"x", DataItem(0)}, {"y", DataItem(2)}}},
      {k3, {{"x", DataItem(1)}, {"y", DataItem(0)}}},
      {v0, {{"val", DataItem(1.5)}}},
      {v1, {{"val", DataItem(2.0)}}},
      {v2, {{"val", DataItem(2.5)}}}};
  TriplesT schema_triples = {
      {key_schema,
       {{"x", DataItem(schema::kInt32)}, {"y", DataItem(schema::kInt32)}}},
      {value_schema, {{"val", DataItem(schema::kFloat32)}}},
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, key_schema},
        {schema::kDictValuesSchemaAttr, value_schema}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto dicts = DataSliceImpl::ObjectsFromAllocation(AllocateDicts(3), 3);
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(7);
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
  auto values = DataSliceImpl::Create(CreateDenseArray<DataItem>(
      {v0, v0, v0, v1, v2, DataItem(), DataItem()}));
  ASSERT_OK(db->SetInDict(dicts_expanded, keys, values));
  auto key_schema = AllocateSchema();
  auto value_schema = AllocateSchema();
  auto dict_schema = AllocateSchema();
  std::pair<std::string_view, DataItem> key_schema_attr = {
      schema::kSchemaAttr, key_schema};
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
      {value_schema, {{"val", DataItem(schema::kFloat32)}}},
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kObject)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kObject)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto dicts = DataSliceImpl::ObjectsFromAllocation(AllocateDicts(3), 3);
  auto objs = DataSliceImpl::AllocateEmptyObjects(3);
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
  TriplesT schema_triples = {
      {key_schema, {{"x", DataItem(schema::kInt64)}}},
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, key_schema},
        {schema::kDictValuesSchemaAttr, dict_schema}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  TriplesT schema_triples = {
      {key_schema, {{"x", DataItem(schema::kInt64)}}},
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, key_schema},
        {schema::kDictValuesSchemaAttr, dict_schema}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto lists = DataSliceImpl::ObjectsFromAllocation(AllocateLists(3), 3);
  auto values = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({lists[1], lists[2], lists[0]}));
  ASSERT_OK(db->AppendToList(lists, values));
  auto list_schema = AllocateSchema();
  TriplesT schema_triples = {
      {list_schema, {{schema::kListItemsSchemaAttr, list_schema}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());
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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(1);
  auto a0 = obj_ids[0];
  auto schema1 = AllocateSchema();
  auto schema2 = AllocateSchema();

  TriplesT schema_triples = {{schema1, {{"next", schema2}}},
                             {schema2, {{"prev", schema1}}}};
  TriplesT data_triples = {};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(4);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto hidden_obj_ids = DataSliceImpl::AllocateEmptyObjects(2);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(4);
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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(4);
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

TEST_P(ExtractTest, MergeSchemaFromTwoDatabags) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema_db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(4);
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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(4);
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
      StatusIs(absl::StatusCode::kInvalidArgument,
               ::testing::AllOf(
                   ::testing::HasSubstr("conflicting values for schema"),
                   ::testing::HasSubstr("x: INT32 != STRING"))));
}

TEST_P(ExtractTest, NoFollowEntitySchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(4);
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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto schema0 = AllocateSchema();
  auto schema1 = AllocateSchema();
  auto schema2 = AllocateSchema();
  ASSERT_OK_AND_ASSIGN(auto nofollow_schema1,
                       schema::NoFollowSchemaItem(schema1));
  auto obj_dtype = DataItem(schema::kObject);

  TriplesT schema_triples = {
      {schema0, {{"nofollow", obj_dtype}}}};
  TriplesT unreachable_schema_triples = {
      {nofollow_schema1, {{"prev", obj_dtype}, {"next", obj_dtype}}},
      {schema2, {{"prev", obj_dtype}}}};
  TriplesT data_triples = {
      {a0, {{schema::kSchemaAttr, schema0}, {"nofollow", a1}}},
      {a1, {{schema::kSchemaAttr, nofollow_schema1}}}
  };
  TriplesT unreachable_data_triples = {
      {a1,
       {{schema::kSchemaAttr, nofollow_schema1}, {"prev", a0}, {"next", a2}}},
      {a2, {{schema::kSchemaAttr, schema2}, {"prev", a1}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, unreachable_schema_triples);
  SetDataTriples(*db, data_triples);
  SetDataTriples(*db, unreachable_data_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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

TEST_P(ExtractTest, AnySchemaAsData) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto any_schema = DataItem(schema::kAny);
  auto schema_dtype = DataItem(schema::kSchema);
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"x", any_schema}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(schema, schema_dtype, *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  EXPECT_NE(result_db.get(), db.get());
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
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
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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

TEST_P(ExtractTest, ObjectSchemaMissing) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"x", DataItem(schema::kInt32)}}}};
  TriplesT data_triples = {
      {obj_ids[2], {{schema::kSchemaAttr, schema}, {"x", DataItem(1)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

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
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);

  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ExtractOp(result_db.get())(obj_ids, DataItem(schema::kObject),
                                       *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(result_db, DataBagEqual(*expected_db));
}

TEST_P(ExtractTest, InvalidSchemaType) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
  auto schema = DataItem(1);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ExtractOp(result_db.get())(obj_ids, schema, *GetMainDb(db),
                                 {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(absl::StatusCode::kInternal, "unsupported schema type"));
}

TEST_P(ExtractTest, AnySchemaType) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
  auto schema = DataItem(schema::kAny);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ExtractOp(result_db.get())(obj_ids, schema, *GetMainDb(db),
                                 {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(absl::StatusCode::kInternal,
               "clone/extract not supported for kAny schema"));
}

TEST_P(ExtractTest, AnySchemaTypeEmptySlice) {
  // TODO: expect error in test.
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = DataSliceImpl::CreateEmptyAndUnknownType(5);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_OK(ExtractOp(result_db.get())(ds, DataItem(schema::kAny),
                                       *GetMainDb(db),
                                       {GetFallbackDb(db).get()}, nullptr, {}));
}

TEST_P(ExtractTest, AnySchemaTypeInside) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = DataSliceImpl::AllocateEmptyObjects(2);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto schema1 = AllocateSchema();

  TriplesT schema_triples = {{schema1, {{"next", DataItem(schema::kAny)}}}};
  TriplesT data_triples = {{a0, {{"next", a1}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  EXPECT_THAT(
      ExtractOp(result_db.get())(a0, schema1, *GetMainDb(db),
                                 {GetFallbackDb(db).get()}, nullptr, {}),
      StatusIs(absl::StatusCode::kInternal,
               "clone/extract not supported for kAny schema"));
}

}  // namespace
}  // namespace koladata::internal
