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
#include "koladata/internal/op_utils/traverse_helper.h"

#include <cstdint>
#include <initializer_list>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/dense_array/edge.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/testing/deep_op_utils.h"

namespace koladata::internal {
namespace {

using ::arolla::CreateDenseArray;
using ::testing::ElementsAre;
using ::testing::UnorderedElementsAre;

using testing::deep_op_utils::DeepOpTest;
using testing::deep_op_utils::test_param_values;

class TraverseHelperTest : public DeepOpTest {};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, TraverseHelperTest,
                         ::testing::ValuesIn(test_param_values));

TEST_P(TraverseHelperTest, GetTransitionsEntity) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"x", int_dtype}, {"y", int_dtype}}}};
  TriplesT data_triples = {{a0, {{"x", DataItem(1)}, {"y", DataItem(4)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  auto traverse_helper = TraverseHelper(*main_db, fallbacks);
  ASSERT_OK_AND_ASSIGN(auto transition_set,
                       traverse_helper.GetTransitions(a0, schema));
  EXPECT_TRUE(transition_set.has_attrs());
  EXPECT_THAT(transition_set.attr_names(),
              UnorderedElementsAre(arolla::Text("x"), arolla::Text("y")));
}

TEST_P(TraverseHelperTest, GetTransitionsObject) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();

  TriplesT schema_triples = {{schema, {{"x", int_dtype}, {"y", int_dtype}}}};
  TriplesT data_triples = {{a0,
                            {{schema::kSchemaAttr, schema},
                             {"x", DataItem(1)},
                             {"y", DataItem(4)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  auto traverse_helper = TraverseHelper(*main_db, fallbacks);
  ASSERT_OK_AND_ASSIGN(auto transition_set, traverse_helper.GetTransitions(
                                                a0, DataItem(schema::kObject)));
  EXPECT_TRUE(!transition_set.has_attrs());
  EXPECT_TRUE(!transition_set.is_list());
  EXPECT_TRUE(!transition_set.is_dict());

  ASSERT_OK_AND_ASSIGN(auto object_schema, traverse_helper.GetObjectSchema(a0));
  EXPECT_TRUE(object_schema.is_struct_schema());
  ASSERT_OK_AND_ASSIGN(auto entity_transition_set,
                       traverse_helper.GetTransitions(a0, object_schema));

  EXPECT_TRUE(entity_transition_set.has_attrs());
  EXPECT_THAT(entity_transition_set.attr_names(),
              UnorderedElementsAre(arolla::Text("x"), arolla::Text("y")));
}

TEST_P(TraverseHelperTest, GetTransitionsList) {
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

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  auto traverse_helper = TraverseHelper(*main_db, fallbacks);

  ASSERT_OK_AND_ASSIGN(auto transition_set_0,
    traverse_helper.GetTransitions(lists[0], list_schema));
  ASSERT_OK_AND_ASSIGN(auto transition_set_1,
                       traverse_helper.GetTransitions(lists[1], list_schema));
  EXPECT_TRUE(transition_set_0.is_list());
  EXPECT_THAT(transition_set_0.list_items(),
              ElementsAre(DataItem(1), DataItem(2), DataItem(3)));
  EXPECT_TRUE(transition_set_1.is_list());
  EXPECT_THAT(transition_set_1.list_items(),
              ElementsAre(DataItem(4), DataItem(5)));
}

TEST_P(TraverseHelperTest, GetTransitionsDict) {
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

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  auto traverse_helper = TraverseHelper(*main_db, fallbacks);

  ASSERT_OK_AND_ASSIGN(auto transition_set_0,
    traverse_helper.GetTransitions(dicts[0], dict_schema));
  ASSERT_OK_AND_ASSIGN(auto transition_set_1,
                       traverse_helper.GetTransitions(dicts[1], dict_schema));
  EXPECT_TRUE(transition_set_0.is_dict());
  EXPECT_THAT(transition_set_0.dict_keys(),
              UnorderedElementsAre(DataItem(1), DataItem(2), DataItem(3)));
  EXPECT_THAT(transition_set_0.dict_values(),
              UnorderedElementsAre(DataItem(1.), DataItem(2.), DataItem(3.)));
  EXPECT_TRUE(transition_set_1.is_dict());
  EXPECT_THAT(transition_set_1.dict_keys(),
              UnorderedElementsAre(DataItem(1), DataItem(5)));
  EXPECT_THAT(transition_set_1.dict_values(),
              UnorderedElementsAre(DataItem(4.), DataItem(5.)));
}

TEST_P(TraverseHelperTest, GetTransitionsSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();

  auto schema = AllocateSchema();
  auto int_dtype = DataItem(schema::kInt32);

  TriplesT schema_triples = {{schema, {{"x", int_dtype}, {"y", int_dtype}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  auto traverse_helper = TraverseHelper(*main_db, fallbacks);
  ASSERT_OK_AND_ASSIGN(
      auto transition_set,
      traverse_helper.GetTransitions(schema, DataItem(schema::kSchema)));
  EXPECT_TRUE(transition_set.has_attrs());
  EXPECT_THAT(transition_set.attr_names(),
              UnorderedElementsAre(arolla::Text("x"), arolla::Text("y")));
}

TEST_P(TraverseHelperTest, ForEachObject) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(4);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();

  TriplesT schema_triples = {
      {schema, {{"x", schema}, {"y", schema}, {"z", int_dtype}}}};
  TriplesT data_triples = {{a0, {{"x", a1}, {"y", a2}, {"z", DataItem(1)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  auto traverse_helper = TraverseHelper(*main_db, fallbacks);
  ASSERT_OK_AND_ASSIGN(auto transition_set,
                       traverse_helper.GetTransitions(a0, schema));

  std::vector<DataItem> result_items;
  std::vector<DataItem> result_schemas;
  EXPECT_OK(traverse_helper.ForEachObject(
      a0, schema, transition_set,
      [&](const DataItem& item, const DataItem& schema) {
        result_items.push_back(item);
        result_schemas.push_back(schema);
      }));
  EXPECT_THAT(result_items, UnorderedElementsAre(a1, a2));
  EXPECT_THAT(result_schemas, ElementsAre(schema, schema));
}

}  // namespace
}  // namespace koladata::internal
