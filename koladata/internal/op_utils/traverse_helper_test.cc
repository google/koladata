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
#include "koladata/internal/op_utils/traverse_helper.h"

#include <cstdint>
#include <initializer_list>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_format.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/testing/deep_op_utils.h"
#include "koladata/test_utils.h"

namespace koladata::internal {
namespace {

using ::arolla::CreateDenseArray;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;

using testing::deep_op_utils::DeepOpTest;
using testing::deep_op_utils::test_param_values;

using TransitionKey = TraverseHelper::TransitionKey;
using TransitionType = TraverseHelper::TransitionType;

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

  auto transition_keys = transition_set.GetTransitionKeys();
  ASSERT_EQ(transition_keys.size(), 2);
  EXPECT_EQ(transition_keys[0].type, TransitionType::kAttributeName);
  EXPECT_EQ(transition_keys[1].type, TransitionType::kAttributeName);
  std::vector<DataItem> transition_values(
      {transition_keys[0].value, transition_keys[1].value});
  EXPECT_THAT(transition_values,
              UnorderedElementsAre(DataItem(arolla::Text("x")),
                                   DataItem(arolla::Text("y"))));
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

  auto transition_keys = transition_set.GetTransitionKeys();
  EXPECT_THAT(transition_keys, IsEmpty());

  ASSERT_OK_AND_ASSIGN(auto object_schema, traverse_helper.GetObjectSchema(a0));
  EXPECT_TRUE(object_schema.is_struct_schema());
  ASSERT_OK_AND_ASSIGN(auto entity_transition_set,
                       traverse_helper.GetTransitions(a0, object_schema));

  EXPECT_TRUE(entity_transition_set.has_attrs());
  EXPECT_THAT(entity_transition_set.attr_names(),
              UnorderedElementsAre(arolla::Text("x"), arolla::Text("y")));

  auto entity_transition_keys = entity_transition_set.GetTransitionKeys();
  ASSERT_EQ(entity_transition_keys.size(), 2);
  EXPECT_EQ(entity_transition_keys[0].type, TransitionType::kAttributeName);
  EXPECT_EQ(entity_transition_keys[1].type, TransitionType::kAttributeName);
  std::vector<DataItem> transition_values(
      {entity_transition_keys[0].value, entity_transition_keys[1].value});
  EXPECT_THAT(transition_values,
              UnorderedElementsAre(DataItem(arolla::Text("x")),
                                   DataItem(arolla::Text("y"))));
}

TEST_P(TraverseHelperTest, GetTransitionsList) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto lists = AllocateEmptyLists(3);
  auto values =
      DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2, 3, 4, 5, 6, 7}));
  auto edge = test::EdgeFromSplitPoints({0, 3, 5, 7});
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

  auto transition_keys_0 = transition_set_0.GetTransitionKeys();
  ASSERT_EQ(transition_keys_0.size(), 3);
  EXPECT_EQ(transition_keys_0[0].type, TransitionType::kListItem);
  EXPECT_EQ(transition_keys_0[0].index, 0);
  EXPECT_EQ(transition_keys_0[1].type, TransitionType::kListItem);
  EXPECT_EQ(transition_keys_0[1].index, 1);
  EXPECT_EQ(transition_keys_0[2].type, TransitionType::kListItem);
  EXPECT_EQ(transition_keys_0[2].index, 2);

  auto transition_keys_1 = transition_set_1.GetTransitionKeys();
  ASSERT_EQ(transition_keys_1.size(), 2);
  EXPECT_EQ(transition_keys_1[0].type, TransitionType::kListItem);
  EXPECT_EQ(transition_keys_1[0].index, 0);
  EXPECT_EQ(transition_keys_1[1].type, TransitionType::kListItem);
  EXPECT_EQ(transition_keys_1[1].index, 1);
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

  auto transition_keys_0 = transition_set_0.GetTransitionKeys();
  ASSERT_EQ(transition_keys_0.size(), 6);
  std::vector<TransitionType> transitions_keys_types(
      {transition_keys_0[0].type, transition_keys_0[1].type,
       transition_keys_0[2].type, transition_keys_0[3].type,
       transition_keys_0[4].type, transition_keys_0[5].type});
  EXPECT_THAT(transitions_keys_types,
              UnorderedElementsAre(
                  TransitionType::kDictKey, TransitionType::kDictKey,
                  TransitionType::kDictKey, TransitionType::kDictValue,
                  TransitionType::kDictValue, TransitionType::kDictValue));
  std::vector<DataItem> transitions_keys_values(
      {transition_keys_0[0].value, transition_keys_0[1].value,
       transition_keys_0[2].value, transition_keys_0[3].value,
       transition_keys_0[4].value, transition_keys_0[5].value});
  EXPECT_THAT(transitions_keys_values,
              UnorderedElementsAre(DataItem(1), DataItem(2), DataItem(3),
                                   DataItem(1), DataItem(2), DataItem(3)));
  std::vector<int64_t> transitions_keys_indices(
      {transition_keys_0[0].index, transition_keys_0[1].index,
       transition_keys_0[2].index, transition_keys_0[3].index,
       transition_keys_0[4].index, transition_keys_0[5].index});
  EXPECT_THAT(transitions_keys_indices, UnorderedElementsAre(0, 1, 2, 0, 1, 2));
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

  auto transition_keys = transition_set.GetTransitionKeys();
  ASSERT_EQ(transition_keys.size(), 2);
  EXPECT_EQ(transition_keys[0].type, TransitionType::kAttributeName);
  EXPECT_EQ(transition_keys[1].type, TransitionType::kAttributeName);
  std::vector<DataItem> transition_values(
      {transition_keys[0].value, transition_keys[1].value});
  EXPECT_THAT(transition_values,
              UnorderedElementsAre(DataItem(arolla::Text("x")),
                                   DataItem(arolla::Text("y"))));
}

TEST_P(TraverseHelperTest, TransitionByKeyEntity) {
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

  TransitionKey transition_key({.type = TransitionType::kAttributeName,
                                .value = DataItem(arolla::Text("x"))});
  ASSERT_OK_AND_ASSIGN(auto transition,
                       traverse_helper.TransitionByKey(
                           a0, schema, transition_set, transition_key));
  EXPECT_EQ(transition.item, DataItem(1));
  EXPECT_EQ(transition.schema, DataItem(int_dtype));
}

TEST_P(TraverseHelperTest, TransitionByKeyObjectToEntity) {
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
  ASSERT_OK_AND_ASSIGN(auto transition_set,
                       traverse_helper.GetTransitions(a0, schema));

  TransitionKey transition_key({.type = TransitionType::kObjectSchema});
  ASSERT_OK_AND_ASSIGN(auto transition, traverse_helper.TransitionByKey(
                                            a0, DataItem(schema::kObject),
                                            transition_set, transition_key));
  EXPECT_EQ(transition.item, schema);
  EXPECT_EQ(transition.schema, DataItem(schema::kSchema));
}

TEST_P(TraverseHelperTest, TransitionByKeyList) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto lists = AllocateEmptyLists(3);
  auto values =
      DataSliceImpl::Create(CreateDenseArray<int32_t>({1, 2, 3, 4, 5, 6, 7}));
  auto edge = test::EdgeFromSplitPoints({0, 3, 5, 7});
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

  TransitionKey transition_key_0(
      {.type = TransitionType::kListItem, .index = 0});
  ASSERT_OK_AND_ASSIGN(
      auto transition_0,
      traverse_helper.TransitionByKey(lists[0], list_schema, transition_set_0,
                                      transition_key_0));
  EXPECT_EQ(transition_0.item, DataItem(1));
  EXPECT_EQ(transition_0.schema, DataItem(schema::kInt32));

  TransitionKey transition_key_1(
      {.type = TransitionType::kListItem, .index = 1});
  ASSERT_OK_AND_ASSIGN(
      auto transition_1,
      traverse_helper.TransitionByKey(lists[1], list_schema, transition_set_1,
                                      transition_key_1));
  EXPECT_EQ(transition_1.item, DataItem(5));
  EXPECT_EQ(transition_1.schema, DataItem(schema::kInt32));
}

TEST_P(TraverseHelperTest, TransitionByKeyDict) {
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

  TransitionKey key_0({.type = TransitionType::kDictKey,
                       .index = 0,
                       .value = transition_set_0.dict_keys()[0]});
  TransitionKey key_1({.type = TransitionType::kDictKey,
                       .index = 1,
                       .value = transition_set_0.dict_keys()[1]});
  TransitionKey key_2({.type = TransitionType::kDictKey,
                       .index = 2,
                       .value = transition_set_0.dict_keys()[2]});
  ASSERT_OK_AND_ASSIGN(auto transition_0,
                       traverse_helper.TransitionByKey(
                           dicts[0], dict_schema, transition_set_0, key_0));
  ASSERT_OK_AND_ASSIGN(auto transition_1,
                       traverse_helper.TransitionByKey(
                           dicts[0], dict_schema, transition_set_0, key_1));
  ASSERT_OK_AND_ASSIGN(auto transition_2,
                       traverse_helper.TransitionByKey(
                           dicts[0], dict_schema, transition_set_0, key_2));
  std::vector<DataItem> transition_items(
      {transition_0.item, transition_1.item, transition_2.item});
  EXPECT_THAT(transition_items,
              UnorderedElementsAre(DataItem(1), DataItem(2), DataItem(3)));
  EXPECT_EQ(transition_0.schema, DataItem(schema::kInt64));
  EXPECT_EQ(transition_1.schema, DataItem(schema::kInt64));
  EXPECT_EQ(transition_2.schema, DataItem(schema::kInt64));

  TransitionKey value_0({.type = TransitionType::kDictValue,
                         .index = 0,
                         .value = transition_0.item});
  TransitionKey value_1({.type = TransitionType::kDictValue,
                         .index = 1,
                         .value = transition_1.item});
  TransitionKey value_2({.type = TransitionType::kDictValue,
                         .index = 2,
                         .value = transition_2.item});
  ASSERT_OK_AND_ASSIGN(auto value_transition_0,
                       traverse_helper.TransitionByKey(
                           dicts[0], dict_schema, transition_set_0, value_0));
  ASSERT_OK_AND_ASSIGN(auto value_transition_1,
                       traverse_helper.TransitionByKey(
                           dicts[0], dict_schema, transition_set_0, value_1));
  ASSERT_OK_AND_ASSIGN(auto value_transition_2,
                       traverse_helper.TransitionByKey(
                           dicts[0], dict_schema, transition_set_0, value_2));
  EXPECT_EQ(value_transition_0.schema, DataItem(schema::kFloat32));
  EXPECT_EQ(value_transition_1.schema, DataItem(schema::kFloat32));
  EXPECT_EQ(value_transition_2.schema, DataItem(schema::kFloat32));
  std::vector<DataItem> transition_values({value_transition_0.item,
                                           value_transition_1.item,
                                           value_transition_2.item});
  EXPECT_THAT(transition_values,
              UnorderedElementsAre(DataItem(1.), DataItem(2.), DataItem(3.)));
}

TEST_P(TraverseHelperTest, TransitionByKeySchema) {
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

  TransitionKey transition_key({.type = TransitionType::kAttributeName,
                                .value = DataItem(arolla::Text("x"))});
  ASSERT_OK_AND_ASSIGN(auto transition, traverse_helper.TransitionByKey(
                                            schema, DataItem(schema::kSchema),
                                            transition_set, transition_key));
  EXPECT_EQ(transition.item, DataItem(int_dtype));
  EXPECT_EQ(transition.schema, DataItem(schema::kSchema));
}

TEST_P(TraverseHelperTest, TransitionKeyToAccessString) {
  EXPECT_EQ(TraverseHelper::TransitionKeyToAccessString(
                {.type = TransitionType::kListItem, .index = 2}),
            "[2]");
  EXPECT_EQ(TraverseHelper::TransitionKeyToAccessString(
                {.type = TransitionType::kDictKey, .index = 1}),
            ".get_keys().S[1]");
  EXPECT_EQ(TraverseHelper::TransitionKeyToAccessString(
                {.type = TransitionType::kDictValue, .index = 2}),
            ".get_values().S[2]");
  EXPECT_EQ(TraverseHelper::TransitionKeyToAccessString(
                {.type = TransitionType::kAttributeName,
                 .value = DataItem(arolla::Text("x"))}),
            ".x");
  EXPECT_EQ(TraverseHelper::TransitionKeyToAccessString(
                {.type = TransitionType::kAttributeName,
                 .value = DataItem(arolla::Text(schema::kSchemaNameAttr))}),
            absl::StrFormat(".get_attr('%s')", schema::kSchemaNameAttr));
  EXPECT_EQ(TraverseHelper::TransitionKeyToAccessString(
                {.type = TransitionType::kAttributeName,
                 .value = DataItem(arolla::Text(schema::kSchemaMetadataAttr))}),
            absl::StrFormat(".get_attr('%s')", schema::kSchemaMetadataAttr));
  EXPECT_EQ(TraverseHelper::TransitionKeyToAccessString(
                {.type = TransitionType::kAttributeName,
                 .value = DataItem(arolla::Text(schema::kSchemaAttr))}),
            absl::StrFormat(".get_obj_schema()"));
  EXPECT_EQ(
      TraverseHelper::TransitionKeyToAccessString(
          {.type = TransitionType::kAttributeName,
           .value = DataItem(arolla::Text(schema::kListItemsSchemaAttr))}),
      ".get_item_schema()");
  EXPECT_EQ(TraverseHelper::TransitionKeyToAccessString(
                {.type = TransitionType::kAttributeName,
                 .value = DataItem(arolla::Text(schema::kDictKeysSchemaAttr))}),
            ".get_key_schema()");
  EXPECT_EQ(
      TraverseHelper::TransitionKeyToAccessString(
          {.type = TransitionType::kAttributeName,
           .value = DataItem(arolla::Text(schema::kDictValuesSchemaAttr))}),
      ".get_value_schema()");
  EXPECT_EQ(TraverseHelper::TransitionKeyToAccessString(
                {.type = TransitionType::kAttributeName,
                 .value = DataItem(arolla::Text("foo"))}),
            ".foo");
  EXPECT_EQ(TraverseHelper::TransitionKeyToAccessString(
                {.type = TransitionType::kObjectSchema}),
            ".get_obj_schema()");
  EXPECT_EQ(TraverseHelper::TransitionKeyToAccessString(
                {.type = TransitionType::kSchema}),
            ".get_schema()");
  EXPECT_EQ(TraverseHelper::TransitionKeyToAccessString(
                {.type = TransitionType::kSliceItem, .index = 1}),
            ".S[1]");
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
