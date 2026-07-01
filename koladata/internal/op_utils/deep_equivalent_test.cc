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
#include "koladata/internal/op_utils/deep_equivalent.h"

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/deep_clone.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/testing/deep_op_utils.h"

namespace koladata::internal {
namespace {

using ::arolla::CreateDenseArray;
using testing::deep_op_utils::DeepOpTest;
using testing::deep_op_utils::test_param_values;
constexpr float NaN = std::numeric_limits<float>::quiet_NaN();

class DeepEquivalentTest : public DeepOpTest {};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, DeepEquivalentTest,
                         ::testing::ValuesIn(test_param_values));

TEST_P(DeepEquivalentTest, EmptySlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds = DataSliceImpl::AllocateEmptyObjects(3);
  auto schema = AllocateSchema();
  auto result_db = DataBagImpl::CreateEmptyDatabag();

  ASSERT_OK_AND_ASSIGN(
      auto _, DeepEquivalentOp(result_db.get(), {})(
                  ds, schema, *GetMainDb(db), {GetFallbackDb(db).get()}, ds,
                  schema, *GetMainDb(db), {GetFallbackDb(db).get()}));
}

TEST_P(DeepEquivalentTest, FloatSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds =
      DataSliceImpl::Create(CreateDenseArray<float>({1.0, NaN, std::nullopt}));
  auto schema = DataItem(schema::kObject);
  auto result_db = DataBagImpl::CreateEmptyDatabag();

  auto deep_equivalent_op = DeepEquivalentOp(result_db.get(), {});
  ASSERT_OK_AND_ASSIGN(
      auto result_ds,
      deep_equivalent_op(ds, schema, *GetMainDb(db), {GetFallbackDb(db).get()},
                         ds, schema, *GetMainDb(db),
                         {GetFallbackDb(db).get()}));
  ASSERT_OK_AND_ASSIGN(auto diffs, deep_equivalent_op.GetDiffPaths(
                                       result_ds, DataItem(schema::kObject)));
  EXPECT_THAT(diffs, ::testing::IsEmpty());
}

TEST_P(DeepEquivalentTest, FloatSliceMismatch) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto ds_a =
      DataSliceImpl::Create(CreateDenseArray<float>({1.0, NaN, std::nullopt}));
  auto ds_b =
      DataSliceImpl::Create(CreateDenseArray<float>({1.0, 2.0, std::nullopt}));
  auto schema = DataItem(schema::kObject);
  auto result_db = DataBagImpl::CreateEmptyDatabag();

  auto deep_equivalent_op = DeepEquivalentOp(result_db.get(), {});
  ASSERT_OK_AND_ASSIGN(
      auto result_ds,
      deep_equivalent_op(ds_a, schema, *GetMainDb(db),
                         {GetFallbackDb(db).get()}, ds_b, schema,
                         *GetMainDb(db), {GetFallbackDb(db).get()}));
  ASSERT_OK_AND_ASSIGN(auto diffs, deep_equivalent_op.GetDiffPaths(
                                       result_ds, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths,
              ::testing::ElementsAre(::testing::HasSubstr(".S[1]")));
}

TEST_P(DeepEquivalentTest, DeepEntitySlice) {
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
  TriplesT data_triples = {
      {a0, {{"self", a0}, {"b", b0}}}, {a1, {{"self", a1}, {"b", b1}}},
      {a2, {{"self", a2}, {"b", b2}}}, {b0, {{"x", DataItem(0)}}},
      {b1, {{"x", DataItem(1)}}},      {b2, {{"x", DataItem(2)}}}};
  TriplesT schema_triples = {{schema_a, {{"self", schema_a}, {"b", schema_b}}},
                             {schema_b, {{"x", DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyObjects(3);

  auto cloned_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      auto cloned_ds, DeepCloneOp(cloned_db.get())(ds, schema_a, *GetMainDb(db),
                                                   {GetFallbackDb(db).get()}));
  TriplesT update_triples = {{a0, {{"b", b2}}}};
  SetDataTriples(*db, update_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_equivalent_op = DeepEquivalentOp(result_db.get(), {});
  ASSERT_OK_AND_ASSIGN(auto result_ds,
                       deep_equivalent_op(ds, schema_a, *GetMainDb(db),
                                          {GetFallbackDb(db).get()}, cloned_ds,
                                          schema_a, *cloned_db, {}));

  ASSERT_OK_AND_ASSIGN(auto a0_b, result_db->GetAttr(result_ds[0], "b"));
  ASSERT_OK_AND_ASSIGN(auto a0_b_x, result_db->GetAttr(a0_b, "x"));
  ASSERT_OK_AND_ASSIGN(auto diff, result_db->GetAttr(a0_b_x, "diff"));
  ASSERT_OK_AND_ASSIGN(auto lhs, result_db->GetAttr(diff, "lhs_value"));
  ASSERT_OK_AND_ASSIGN(auto rhs, result_db->GetAttr(diff, "rhs_value"));
  EXPECT_EQ(lhs, DataItem(2));
  EXPECT_EQ(rhs, DataItem(0));

  ASSERT_OK_AND_ASSIGN(auto diffs, deep_equivalent_op.GetDiffPaths(
                                       result_ds, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths,
              ::testing::ElementsAre(::testing::HasSubstr(".S[0].b.x")));
}

TEST_P(DeepEquivalentTest, DeepEntitySlicePartial) {
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
  TriplesT data_triples = {
      {a0, {{"self", a0}, {"b", b0}}}, {a1, {{"b", b1}}},
      {a2, {{"self", a2}, {"b", b2}}}, {b0, {{"x", DataItem(0)}}},
      {b1, {{"x", DataItem(1)}}},      {b2, {{"x", DataItem(2)}}}};
  TriplesT schema_triples = {{schema_a, {{"self", schema_a}, {"b", schema_b}}},
                             {schema_b, {{"x", DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyObjects(3);

  auto cloned_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      auto cloned_ds, DeepCloneOp(cloned_db.get())(ds, schema_a, *GetMainDb(db),
                                                   {GetFallbackDb(db).get()}));
  TriplesT update_triples = {
    {a0, {{"b", b2}}},
    {a1, {{"self", a1}}},
    {a2, {{"self", DataItem()}}},
  };
  SetDataTriples(*db, update_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_equivalent_op =
      DeepEquivalentOp(result_db.get(), {.partial = true});
  ASSERT_OK_AND_ASSIGN(auto result_ds,
                       deep_equivalent_op(ds, schema_a, *GetMainDb(db),
                                          {GetFallbackDb(db).get()}, cloned_ds,
                                          schema_a, *cloned_db, {}));

  ASSERT_OK_AND_ASSIGN(auto diffs, deep_equivalent_op.GetDiffPaths(
                                       result_ds, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths,
              ::testing::UnorderedElementsAre(
                  ::testing::HasSubstr(".S[0].b.x"),
                  ::testing::HasSubstr(".S[2].self")));
}

TEST_P(DeepEquivalentTest, DeepEntityItem) {
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
  TriplesT data_triples = {
      {a0, {{"self", a0}, {"b", b0}}}, {a1, {{"self", a1}, {"b", b1}}},
      {a2, {{"self", a2}, {"b", b2}}}, {b0, {{"x", DataItem(0)}}},
      {b1, {{"x", DataItem(1)}}},      {b2, {{"x", DataItem(2)}}}};
  TriplesT schema_triples = {{schema_a, {{"self", schema_a}, {"b", schema_b}}},
                             {schema_b, {{"x", DataItem(schema::kInt32)}}}};
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto itemid = AllocateEmptyObjects(3);

  auto cloned_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      auto cloned_a0, DeepCloneOp(cloned_db.get())(a0, schema_a, *GetMainDb(db),
                                                   {GetFallbackDb(db).get()}));
  TriplesT update_triples = {{a0, {{"b", b2}}}};
  SetDataTriples(*db, update_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_equivalent_op = DeepEquivalentOp(result_db.get(), {});
  ASSERT_OK_AND_ASSIGN(auto result_item,
                       deep_equivalent_op(a0, schema_a, *GetMainDb(db),
                                          {GetFallbackDb(db).get()}, cloned_a0,
                                          schema_a, *cloned_db, {}));

  ASSERT_OK_AND_ASSIGN(auto a0_b, result_db->GetAttr(result_item, "b"));
  ASSERT_OK_AND_ASSIGN(auto a0_b_x, result_db->GetAttr(a0_b, "x"));
  ASSERT_OK_AND_ASSIGN(auto diff, result_db->GetAttr(a0_b_x, "diff"));
  ASSERT_OK_AND_ASSIGN(auto lhs, result_db->GetAttr(diff, "lhs_value"));
  ASSERT_OK_AND_ASSIGN(auto rhs, result_db->GetAttr(diff, "rhs_value"));
  EXPECT_EQ(lhs, DataItem(2));
  EXPECT_EQ(rhs, DataItem(0));

  ASSERT_OK_AND_ASSIGN(auto diffs, deep_equivalent_op.GetDiffPaths(
                                       result_item, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths, ::testing::ElementsAre(::testing::HasSubstr(".b.x")));
}

TEST_P(DeepEquivalentTest, PrimitiveSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_equivalent_op = DeepEquivalentOp(result_db.get(), {});
  ASSERT_OK_AND_ASSIGN(
      auto result_ds,
      deep_equivalent_op(
          DataSliceImpl::Create({DataItem(1), DataItem(2), DataItem(3)}),
          DataItem(schema::kObject), *GetMainDb(db), {GetFallbackDb(db).get()},
          DataSliceImpl::Create({DataItem(1), DataItem(), DataItem(4)}),
          DataItem(schema::kObject), *GetMainDb(db), {}));

  ASSERT_OK_AND_ASSIGN(auto diffs, deep_equivalent_op.GetDiffPaths(
                                       result_ds, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths,
              ::testing::UnorderedElementsAre(::testing::HasSubstr(".S[1]"),
                                              ::testing::HasSubstr(".S[2]")));
}

TEST_P(DeepEquivalentTest, AllMissing) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_equivalent_op =
      DeepEquivalentOp(result_db.get(), {.schemas_equality = false});
  auto schema = AllocateSchema();
  TriplesT schema_triples = {
      {schema, {{"self", schema}, {"b", DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());
  auto ds = DataSliceImpl::CreateEmptyAndUnknownType(3);
  ASSERT_OK_AND_ASSIGN(
      auto result_ds,
      deep_equivalent_op(ds, DataItem(schema::kInt32), *GetMainDb(db),
                         {GetFallbackDb(db).get()}, ds, schema, *GetMainDb(db),
                         {GetFallbackDb(db).get()}));

  ASSERT_OK_AND_ASSIGN(auto diffs, deep_equivalent_op.GetDiffPaths(
                                       result_ds, DataItem(schema::kObject)));
  EXPECT_THAT(diffs, ::testing::IsEmpty());
}

TEST_P(DeepEquivalentTest, MissingEntitiesSchemaMismatchInAttributes) {
  auto db_a = DataBagImpl::CreateEmptyDatabag();
  auto db_b = DataBagImpl::CreateEmptyDatabag();

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_equivalent_op =
      DeepEquivalentOp(result_db.get(), {.schemas_equality = true});
  auto schema = AllocateSchema();
  TriplesT schema_triples = {
      {schema, {{"b", DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db_a, schema_triples);
  SetSchemaTriples(*db_a, GenSchemaTriplesFoTests());
  auto ds = DataSliceImpl::CreateEmptyAndUnknownType(3);
  ASSERT_OK_AND_ASSIGN(
      auto result_ds,
      deep_equivalent_op(ds, schema, *GetMainDb(db_a),
                         {GetFallbackDb(db_a).get()}, ds, schema,
                         *GetMainDb(db_b), {GetFallbackDb(db_b).get()}));

  ASSERT_OK_AND_ASSIGN(auto diffs, deep_equivalent_op.GetDiffPaths(
                                       result_ds, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths,
              ::testing::UnorderedElementsAre(".S[0].b"));
}

TEST_P(DeepEquivalentTest, MissingListsSchemaMismatchInItems) {
  auto db_a = DataBagImpl::CreateEmptyDatabag();
  auto db_b = DataBagImpl::CreateEmptyDatabag();

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_equivalent_op =
      DeepEquivalentOp(result_db.get(), {.schemas_equality = true});
  auto schema = AllocateSchema();
  TriplesT schema_triples = {
      {schema, {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db_a, schema_triples);
  SetSchemaTriples(*db_a, GenSchemaTriplesFoTests());
  auto ds = DataSliceImpl::CreateEmptyAndUnknownType(3);
  ASSERT_OK_AND_ASSIGN(
      auto result_ds,
      deep_equivalent_op(ds, schema, *GetMainDb(db_a),
                         {GetFallbackDb(db_a).get()}, ds, schema,
                         *GetMainDb(db_b), {GetFallbackDb(db_b).get()}));

  ASSERT_OK_AND_ASSIGN(auto diffs, deep_equivalent_op.GetDiffPaths(
                                       result_ds, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths,
              ::testing::UnorderedElementsAre(".S[0].get_item_schema()"));
}

TEST_P(DeepEquivalentTest, MissingDictsSchemaMismatchInKeysAndValues) {
  auto db_a = DataBagImpl::CreateEmptyDatabag();
  auto db_b = DataBagImpl::CreateEmptyDatabag();

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_equivalent_op =
      DeepEquivalentOp(result_db.get(), {.schemas_equality = true});
  auto schema = AllocateSchema();
  auto value_schema = AllocateSchema();
  TriplesT schema_triples_a = {
      {schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt32)},
        {schema::kDictValuesSchemaAttr, value_schema}}},
      {value_schema, {{"x", DataItem(schema::kInt32)}}}};
  TriplesT schema_triples_b = {
      {schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt32)},
        {schema::kDictValuesSchemaAttr, value_schema}}},
      {value_schema, {{"y", DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db_a, schema_triples_a);
  SetSchemaTriples(*db_a, GenSchemaTriplesFoTests());
  SetSchemaTriples(*db_b, schema_triples_b);
  SetSchemaTriples(*db_b, GenSchemaTriplesFoTests());
  auto ds = DataSliceImpl::CreateEmptyAndUnknownType(3);
  ASSERT_OK_AND_ASSIGN(
      auto result_ds,
      deep_equivalent_op(ds, schema, *GetMainDb(db_a),
                         {GetFallbackDb(db_a).get()}, ds, schema,
                         *GetMainDb(db_b), {GetFallbackDb(db_b).get()}));

  ASSERT_OK_AND_ASSIGN(auto diffs, deep_equivalent_op.GetDiffPaths(
                                       result_ds, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths,
              ::testing::UnorderedElementsAre(".S[0].get_value_schema().x",
                                              ".S[0].get_value_schema().y"));
}

TEST_P(DeepEquivalentTest, SchemaSlice) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto root_schema = AllocateSchema();
  auto list_schema = AllocateSchema();
  auto item_schema = AllocateSchema();
  TriplesT schema_triples = {
      {root_schema, {{"items", list_schema}}},
      {list_schema,
       {{schema::kListItemsSchemaAttr, item_schema}}},
      {item_schema,
       {{"x", DataItem(schema::kInt32)}, {"y", DataItem(schema::kFloat32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto cloned_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK_AND_ASSIGN(
      auto cloned_root_schema,
      DeepCloneOp(cloned_db.get())(root_schema, DataItem(schema::kSchema),
                                   *GetMainDb(db), {GetFallbackDb(db).get()}));

  TriplesT update_schema_triples = {
      {item_schema, {{"x", DataItem(schema::kFloat32)}}}};
  SetSchemaTriples(*db, update_schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_equivalent_op = DeepEquivalentOp(result_db.get(), {});
  ASSERT_OK_AND_ASSIGN(
      auto result_item,
      deep_equivalent_op(root_schema, DataItem(schema::kSchema), *GetMainDb(db),
                         {GetFallbackDb(db).get()}, cloned_root_schema,
                         DataItem(schema::kSchema), *cloned_db, {}));
  ASSERT_OK_AND_ASSIGN(auto diffs, deep_equivalent_op.GetDiffPaths(
                                       result_item, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(
      diff_paths,
      ::testing::UnorderedElementsAre(::testing::HasSubstr(
          absl::StrFormat(".items.%s.x", schema::kListItemsSchemaAttr))));
}

TEST_P(DeepEquivalentTest, EntityAndObjectListsComparison) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto db2 = DataBagImpl::CreateEmptyDatabag();
  auto root_schema = AllocateSchema();
  auto list_schema = AllocateSchema();
  auto root = DataItem(AllocateSingleObject());
  auto list = DataItem(AllocateSingleList());
  TriplesT schema_triples = {
      {root_schema, {{"items", DataItem(schema::kObject)}}},
      {list_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}}};
  TriplesT data_triples = {
    {root, {{"items", list}}},
    {list, {{schema::kSchemaAttr, list_schema}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, data_triples);
  SetDataTriples(*db, GenDataTriplesForTest());
  ASSERT_OK(db->ExtendList(
      list, DataSliceImpl::Create(arolla::CreateDenseArray<int32_t>({1, 2}))));

  TriplesT other_schema_triples = {
      {root_schema, {{"items", list_schema}}},
      {list_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db2, other_schema_triples);
  SetSchemaTriples(*db2, GenSchemaTriplesFoTests());
  SetDataTriples(*db2, data_triples);
  SetDataTriples(*db2, GenDataTriplesForTest());
  ASSERT_OK(db2->ExtendList(
      list, DataSliceImpl::Create(arolla::CreateDenseArray<int32_t>({1, 3}))));

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_equivalent_op = DeepEquivalentOp(result_db.get(), {});
  ASSERT_OK_AND_ASSIGN(auto result_item,
                       deep_equivalent_op(root, root_schema, *GetMainDb(db),
                                          {GetFallbackDb(db).get()}, root,
                                          root_schema, *db2, {}));
  ASSERT_OK_AND_ASSIGN(auto diffs, deep_equivalent_op.GetDiffPaths(
                                       result_item, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths, ::testing::UnorderedElementsAre(".items[1]"));
}

TEST_P(DeepEquivalentTest, ItemIdAndListMismatch) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto db2 = DataBagImpl::CreateEmptyDatabag();
  auto root_schema = AllocateSchema();
  auto list_schema = AllocateSchema();
  auto root = DataItem(AllocateSingleObject());
  auto list = DataItem(AllocateSingleList());
  TriplesT schema_triples = {
      {root_schema, {{"items", DataItem(schema::kItemId)}}}};
  TriplesT data_triples = {{root, {{"items", list}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, data_triples);
  SetDataTriples(*db, GenDataTriplesForTest());

  TriplesT other_schema_triples = {
      {root_schema, {{"items", list_schema}}},
      {list_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db2, other_schema_triples);
  SetSchemaTriples(*db2, GenSchemaTriplesFoTests());
  SetDataTriples(*db2, data_triples);
  SetDataTriples(*db2, GenDataTriplesForTest());
  ASSERT_OK(db2->ExtendList(
      list, DataSliceImpl::Create(arolla::CreateDenseArray<int32_t>({1, 3}))));

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_equivalent_op = DeepEquivalentOp(result_db.get(), {});
  ASSERT_OK_AND_ASSIGN(auto result_item,
                       deep_equivalent_op(root, root_schema, *GetMainDb(db),
                                          {GetFallbackDb(db).get()}, root,
                                          root_schema, *db2, {}));
  ASSERT_OK_AND_ASSIGN(auto diffs, deep_equivalent_op.GetDiffPaths(
                                       result_item, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths, ::testing::UnorderedElementsAre(".items"));
}

}  // namespace
}  // namespace koladata::internal
