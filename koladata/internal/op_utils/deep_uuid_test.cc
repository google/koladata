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
#include "koladata/internal/op_utils/deep_uuid.h"

#include <cstdint>
#include <functional>
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
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/text.h"

namespace koladata::internal {
namespace {

using ::absl_testing::StatusIs;
using ::arolla::CreateDenseArray;

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

enum CopyingTestParam { kMainDb, kFallbackDb };

class CopyingOpTest : public ::testing::TestWithParam<CopyingTestParam> {
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

class DeepUuidTest : public CopyingOpTest {};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, DeepUuidTest,
                         ::testing::Values(kMainDb, kFallbackDb));

TEST_P(DeepUuidTest, ShallowEntitySlice) {
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
                           {a2, {{"x", DataItem(1)}, {"y", DataItem(4)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

  auto copy_obj_ids = DataSliceImpl::AllocateEmptyObjects(3);
  auto copy_a0 = copy_obj_ids[0];
  auto copy_a1 = copy_obj_ids[1];
  auto copy_a2 = copy_obj_ids[2];
  TriplesT copy_data_triples = {
      {copy_a0, {{"x", DataItem(1)}, {"y", DataItem(4)}}},
      {copy_a1, {{"x", DataItem(2)}, {"y", DataItem(5)}}},
      {copy_a2, {{"x", DataItem(1)}, {"y", DataItem(4)}}}};
  auto copy_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*copy_db, schema_triples);
  SetDataTriples(*copy_db, copy_data_triples);

  ASSERT_OK_AND_ASSIGN(
      auto result_slice,
      DeepUuidOp()(DataItem(arolla::Text("a")), obj_ids, schema, *GetMainDb(db),
                   {GetFallbackDb(db).get()}));
  ASSERT_OK_AND_ASSIGN(auto same_result_slice,
                       DeepUuidOp()(DataItem(arolla::Text("a")), copy_obj_ids,
                                    schema, *copy_db));
  EXPECT_EQ(same_result_slice[0], result_slice[0]);
  EXPECT_EQ(same_result_slice[1], result_slice[1]);
  EXPECT_EQ(same_result_slice[2], result_slice[2]);
  EXPECT_EQ(result_slice.size(), 3);
  EXPECT_NE(result_slice[0], a0);
  EXPECT_NE(result_slice[1], a1);
  EXPECT_NE(result_slice[2], a2);
  DataItem x1 = DataItem(1);
  DataItem x4 = DataItem(4);
  EXPECT_NE(result_slice[0], result_slice[1]);
  EXPECT_EQ(result_slice[0], result_slice[2]);
  EXPECT_EQ(
      result_slice[0],
      CreateUuidFromFields("a", {"x", "y"}, {std::cref(x1), std::cref(x4)}));
}

TEST_P(DeepUuidTest, DifferentSeeds) {
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
                           {a2, {{"x", DataItem(1)}, {"y", DataItem(4)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

  ASSERT_OK_AND_ASSIGN(auto result_slice,
                       DeepUuidOp()(DataItem(arolla::Text("")), obj_ids, schema,
                                    *GetMainDb(db), {GetFallbackDb(db).get()}));
  ASSERT_OK_AND_ASSIGN(
      auto a_result_slice,
      DeepUuidOp()(DataItem(arolla::Text("a")), obj_ids, schema, *db));
  ASSERT_OK_AND_ASSIGN(
      auto b_result_slice,
      DeepUuidOp()(DataItem(arolla::Text("b")), obj_ids, schema, *db));
  for (int64_t i = 0; i < 3; ++i) {
    EXPECT_NE(a_result_slice[i], b_result_slice[i]);
    EXPECT_NE(result_slice[i], b_result_slice[i]);
    EXPECT_NE(result_slice[i], a_result_slice[i]);
  }
}

TEST_P(DeepUuidTest, DeepEntitySlice) {
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
  TriplesT data_triples = {{a0, {{"x", DataItem("a")}, {"b", b0}}},
                           {a1, {{"x", DataItem("b")}, {"b", b1}}},
                           {a2, {{"x", DataItem("c")}, {"b", b2}}},
                           {b0, {{"y", DataItem(4)}}},
                           {b1, {{"y", DataItem(5)}}},
                           {b2, {{"y", DataItem(5)}}}};
  TriplesT schema_triples = {
      {schema_a, {{"x", DataItem(schema::kBytes)}, {"b", schema_b}}},
      {schema_b, {{"y", DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

  ASSERT_OK_AND_ASSIGN(auto result_slice,
                       DeepUuidOp()(DataItem(arolla::Text("")), ds, schema_a,
                                    *GetMainDb(db), {GetFallbackDb(db).get()}));

  ASSERT_OK(db->SetAttr(a0, "x", DataItem("z")));
  ASSERT_OK(db->SetAttr(b2, "y", DataItem(11)));
  ASSERT_OK_AND_ASSIGN(auto updated_result_slice,
                       DeepUuidOp()(DataItem(arolla::Text("")), ds, schema_a,
                                    *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_EQ(result_slice.size(), 3);
  EXPECT_EQ(updated_result_slice.size(), 3);
  EXPECT_NE(result_slice[0], updated_result_slice[0]);
  EXPECT_EQ(result_slice[1], updated_result_slice[1]);
  EXPECT_NE(result_slice[2], updated_result_slice[2]);
}

TEST_P(DeepUuidTest, ListsSlice) {
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

  ASSERT_OK_AND_ASSIGN(
      auto result_slice,
      DeepUuidOp()(DataItem(arolla::Text("seed")), lists, list_schema,
                   *GetMainDb(db), {GetFallbackDb(db).get()}));
  EXPECT_EQ(
      result_slice[0],
      CreateListUuidFromItemsAndFields(
          "seed", DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2, 3})),
          {}, {}));
  EXPECT_EQ(
      result_slice[1],
      CreateListUuidFromItemsAndFields(
          "seed", DataSliceImpl::Create(CreateDenseArray<int32_t>({4, 5})), {},
          {}));
  EXPECT_EQ(
      result_slice[2],
      CreateListUuidFromItemsAndFields(
          "seed", DataSliceImpl::Create(CreateDenseArray<int32_t>({6, 7})), {},
          {}));
}

TEST_P(DeepUuidTest, DictsSlice) {
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
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt64)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kFloat32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

  ASSERT_OK_AND_ASSIGN(
      auto result_slice,
      DeepUuidOp()(DataItem(arolla::Text("seed")), dicts, dict_schema,
                   *GetMainDb(db), {GetFallbackDb(db).get()}));
  EXPECT_EQ(
      result_slice[0],
      CreateDictUuidFromKeysValuesAndFields(
          "seed", DataSliceImpl::Create(CreateDenseArray<int64_t>({1, 2, 3})),
          DataSliceImpl::Create(CreateDenseArray<float>({1, 2, 3})), {}, {}));
  EXPECT_EQ(
      result_slice[1],
      CreateDictUuidFromKeysValuesAndFields(
          "seed", DataSliceImpl::Create(CreateDenseArray<int64_t>({1, 5})),
          DataSliceImpl::Create(CreateDenseArray<float>({4, 5})), {}, {}));
  EXPECT_EQ(
      result_slice[2],
      CreateDictUuidFromKeysValuesAndFields(
          "seed", DataSliceImpl::Create(CreateDenseArray<int64_t>({3, 7})),
          DataSliceImpl::Create(CreateDenseArray<float>({6, 7})), {}, {}));
}

TEST_P(DeepUuidTest, SchemaSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dict_schema = AllocateSchema();
  auto list_schema = AllocateSchema();
  auto obj_schema = AllocateSchema();
  TriplesT schema_triples = {
      {obj_schema,
       {{"x", DataItem(schema::kInt32)}, {"y", DataItem(schema::kInt32)}}},
      {list_schema, {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}},
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kBytes)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

  auto ds = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({obj_schema, list_schema, dict_schema}));

  ASSERT_OK_AND_ASSIGN(auto result_slice,
                       DeepUuidOp()(DataItem(arolla::Text("seed")), ds,
                                    DataItem(schema::kSchema), *GetMainDb(db),
                                    {GetFallbackDb(db).get()}));

  DataItem xint = DataItem(schema::kInt32);
  DataItem xbytes = DataItem(schema::kBytes);
  EXPECT_EQ(result_slice[0],
            CreateSchemaUuidFromFields("seed", {"x", "y"},
                                       {std::cref(xint), std::cref(xint)}));
  EXPECT_EQ(result_slice[1],
            CreateSchemaUuidFromFields("seed", {schema::kListItemsSchemaAttr},
                                       {std::cref(xint)}));
  EXPECT_EQ(
      result_slice[2],
      CreateSchemaUuidFromFields(
          "seed", {schema::kDictKeysSchemaAttr, schema::kDictValuesSchemaAttr},
          {std::cref(xbytes), std::cref(xint)}));
}

TEST_P(DeepUuidTest, ObjectsSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj0 = AllocateSingleObject();
  auto obj1 = AllocateSingleObject();
  auto dict = AllocateSingleDict();
  auto list = AllocateSingleList();
  ASSERT_OK(db->SetInDict(DataItem(dict), DataItem("a"), DataItem(1)));
  ASSERT_OK(
      db->ExtendList(DataItem(list),
                     DataSliceImpl::Create(CreateDenseArray<int32_t>({4, 5}))));
  auto dict_schema = AllocateSchema();
  auto list_schema = AllocateSchema();
  auto obj_schema = AllocateSchema();
  TriplesT data_triples = {
      {DataItem(obj0),
       {{schema::kSchemaAttr, DataItem(obj_schema)},
        {"x", DataItem(1)},
        {"y", DataItem(2)}}},
      {DataItem(obj1),
       {{schema::kSchemaAttr, DataItem(obj_schema)},
        {"x", DataItem(3)},
        {"y", DataItem(4)}}},
      {DataItem(list), {{schema::kSchemaAttr, DataItem(list_schema)}}},
      {DataItem(dict), {{schema::kSchemaAttr, DataItem(dict_schema)}}}};
  TriplesT schema_triples = {
      {obj_schema,
       {{"x", DataItem(schema::kInt32)}, {"y", DataItem(schema::kInt32)}}},
      {list_schema, {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}},
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kBytes)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

  auto ds = DataSliceImpl::Create(
      CreateDenseArray<DataItem>({DataItem(obj0), DataItem(obj1), DataItem(2),
                                  DataItem(list), DataItem(dict)}));

  ASSERT_OK_AND_ASSIGN(
      auto result_slice,
      DeepUuidOp()(DataItem(arolla::Text("")), ds, DataItem(schema::kObject),
                   *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_EQ(result_slice[2], CreateUuidObject(DataItem(2).StableFingerprint()));
  DataItem x1 = DataItem(1);
  DataItem x2 = DataItem(2);
  DataItem x3 = DataItem(3);
  DataItem x4 = DataItem(4);
  DataItem xint = DataItem(schema::kInt32);
  DataItem xbytes = DataItem(schema::kBytes);
  DataItem xschema = CreateSchemaUuidFromFields(
      "", {"x", "y"}, {std::cref(xint), std::cref(xint)});
  DataItem xlistschema = CreateSchemaUuidFromFields(
      "", {schema::kListItemsSchemaAttr}, {std::cref(xint)});
  DataItem xdictschema = CreateSchemaUuidFromFields(
      "", {schema::kDictKeysSchemaAttr, schema::kDictValuesSchemaAttr},
      {std::cref(xbytes), std::cref(xint)});
  EXPECT_EQ(
      result_slice[0],
      CreateUuidFromFields("", {"x", "y"}, {std::cref(x1), std::cref(x2)}));
  EXPECT_EQ(result_slice[3],
            CreateListUuidFromItemsAndFields(
                "", DataSliceImpl::Create(CreateDenseArray<int32_t>({4, 5})),
                {}, {}));
  EXPECT_EQ(
      result_slice[4],
      CreateDictUuidFromKeysValuesAndFields(
          "",
          DataSliceImpl::Create(CreateDenseArray<DataItem>({DataItem("a")})),
          DataSliceImpl::Create(CreateDenseArray<int32_t>({1})), {}, {}));
}

TEST_P(DeepUuidTest, ItemIdSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

  DataItem xint = DataItem(schema::kInt32);
  DataItem xbytes = DataItem(schema::kBytes);
  DataItem xschema = CreateSchemaUuidFromFields(
      "", {"x", "y"}, {std::cref(xint), std::cref(xint)});
  auto ds = DataSliceImpl::Create(CreateDenseArray<DataItem>({xschema}));

  ASSERT_OK_AND_ASSIGN(
      auto result_ds,
      DeepUuidOp()(DataItem(arolla::Text("")), ds, DataItem(schema::kItemId),
                   *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_EQ(result_ds[0], xschema);
}

TEST_P(DeepUuidTest, CyclicReferences) {
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
  TriplesT data_triples = {{a0, {{"x", DataItem("a")}, {"b", b0}}},
                           {a1, {{"x", DataItem("b")}, {"b", b1}}},
                           {a2, {{"x", DataItem("c")}, {"b", b2}}},
                           {b0, {{"parent", DataItem(a0)}}},
                           {b1, {{"parent", DataItem(a1)}}},
                           {b2, {{"parent", DataItem(a2)}}}};
  TriplesT schema_triples = {
      {schema_a, {{"x", DataItem(schema::kBytes)}, {"b", schema_b}}},
      {schema_b, {{"parent", schema_a}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenNoiseSchemaTriples());
  SetDataTriples(*db, GenNoiseDataTriples());

  EXPECT_THAT(DeepUuidOp()(DataItem(arolla::Text("")), ds, schema_a,
                           *GetMainDb(db), {GetFallbackDb(db).get()}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr(
                           "cyclic attributes are not allowed in deep_uuid")));
}

}  // namespace
}  // namespace koladata::internal
