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
#include "koladata/internal/op_utils/schema_info.h"

#include <variant>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/testing/deep_op_utils.h"

namespace koladata::internal {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::testing::Field;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;
using ::testing::VariantWith;

using testing::deep_op_utils::DeepOpTest;
using testing::deep_op_utils::test_param_values;

class SchemaInfoTest : public DeepOpTest {};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, SchemaInfoTest,
                         ::testing::ValuesIn(test_param_values));

TEST_P(SchemaInfoTest, GetEntitySchemaInfo_Entity) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  auto child_schema = AllocateSchema();
  auto metadata = DataItem(AllocateSingleObject());
  TriplesT schema_triples = {
      {schema,
       {{"x", DataItem(schema::kInt32)},
        {"child", child_schema},
        {schema::kSchemaNameAttr, DataItem(arolla::Text("name"))},
        {schema::kSchemaMetadataAttr, metadata}}}};
  SetSchemaTriples(*db, schema_triples);

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  EXPECT_THAT(
      GetEntitySchemaInfo(schema, *main_db, fallbacks),
      IsOkAndHolds(VariantWith<ObjectSchemaInfo>(Field(
          &ObjectSchemaInfo::attr_names,
          UnorderedElementsAre(DataItem(arolla::Text("x")),
                               DataItem(arolla::Text("child")))))));
}

TEST_P(SchemaInfoTest, GetEntitySchemaInfo_List) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto list_schema = AllocateSchema();
  auto item_schema = AllocateSchema();
  TriplesT schema_triples = {
      {list_schema, {{schema::kListItemsSchemaAttr, item_schema}}}};
  SetSchemaTriples(*db, schema_triples);

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  EXPECT_THAT(GetEntitySchemaInfo(list_schema, *main_db, fallbacks),
              IsOkAndHolds(VariantWith<ListSchemaInfo>(
                  Field(&ListSchemaInfo::item_schema, item_schema))));
}

TEST_P(SchemaInfoTest, GetEntitySchemaInfo_Dict) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dict_schema = AllocateSchema();
  auto values_schema = AllocateSchema();
  TriplesT schema_triples = {
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kString)},
        {schema::kDictValuesSchemaAttr, values_schema}}}};
  SetSchemaTriples(*db, schema_triples);

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  ASSERT_OK_AND_ASSIGN(EntitySchemaInfo info,
                       GetEntitySchemaInfo(dict_schema, *main_db, fallbacks));
  ASSERT_TRUE(std::holds_alternative<DictSchemaInfo>(info));
  EXPECT_EQ(std::get<DictSchemaInfo>(info).keys_schema,
            DataItem(schema::kString));
  EXPECT_EQ(std::get<DictSchemaInfo>(info).values_schema, values_schema);
}

TEST_P(SchemaInfoTest, GetEntitySchemaInfo_NoFollow) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  TriplesT schema_triples = {{schema, {{"x", DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);
  auto nofollow =
      DataItem(CreateNoFollowWithMainObject(schema.value<ObjectId>()));

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  EXPECT_THAT(GetEntitySchemaInfo(nofollow, *main_db, fallbacks),
              IsOkAndHolds(VariantWith<ObjectSchemaInfo>(
                  Field(&ObjectSchemaInfo::attr_names, IsEmpty()))));
}

TEST_P(SchemaInfoTest, ListSchemaWithUnexpectedAttributes) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto list_schema = AllocateSchema();
  auto list_schema_with_name = AllocateSchema();
  TriplesT schema_triples = {
      {list_schema,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)},
        {"x", DataItem(schema::kInt32)}}},
      {list_schema_with_name,
       {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)},
        {schema::kSchemaNameAttr, DataItem(arolla::Text("list"))}}}};
  SetSchemaTriples(*db, schema_triples);

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  EXPECT_THAT(GetEntitySchemaInfo(list_schema, *main_db, fallbacks),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("list schema")));
  EXPECT_THAT(GetEntitySchemaInfo(list_schema_with_name, *main_db, fallbacks),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("list schema")));
}

TEST_P(SchemaInfoTest, DictSchemaWithUnexpectedAttributes) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dict_schema_without_values = AllocateSchema();
  auto dict_schema_with_extra_attr = AllocateSchema();
  auto dict_schema_with_name = AllocateSchema();
  TriplesT schema_triples = {
      {dict_schema_without_values,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt32)}}},
      {dict_schema_with_extra_attr,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt32)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kInt32)},
        {"x", DataItem(schema::kInt32)}}},
      {dict_schema_with_name,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt32)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kInt32)},
        {schema::kSchemaNameAttr, DataItem(arolla::Text("dict"))}}}};
  SetSchemaTriples(*db, schema_triples);

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  EXPECT_THAT(
      GetEntitySchemaInfo(dict_schema_without_values, *main_db, fallbacks),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("dict schema")));
  EXPECT_THAT(
      GetEntitySchemaInfo(dict_schema_with_extra_attr, *main_db, fallbacks),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("dict schema")));
  EXPECT_THAT(GetEntitySchemaInfo(dict_schema_with_name, *main_db, fallbacks),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("dict schema")));
}

TEST_P(SchemaInfoTest, GetSchemasAttrNames_SmallAllocation) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  TriplesT schema_triples = {
      {schema,
       {{"x", DataItem(schema::kInt32)},
        {schema::kSchemaNameAttr, DataItem(arolla::Text("name"))}}}};
  SetSchemaTriples(*db, schema_triples);

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  EXPECT_THAT(GetSchemasAttrNames(DataSliceImpl::Create(1, schema), *main_db,
                                  fallbacks),
              IsOkAndHolds(UnorderedElementsAre(
                  DataItem(arolla::Text("x")),
                  DataItem(arolla::Text(schema::kSchemaNameAttr)))));
}

TEST_P(SchemaInfoTest, GetSchemasAttrNames_BigAllocation) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  AllocationId alloc = AllocateExplicitSchemas(10);
  auto s0 = DataItem(alloc.ObjectByOffset(0));
  auto s1 = DataItem(alloc.ObjectByOffset(1));
  TriplesT schema_triples = {{s0, {{"x", DataItem(schema::kInt32)}}},
                             {s1, {{"y", DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  // All attribute names of the allocation.
  EXPECT_THAT(
      GetSchemasAttrNames(DataSliceImpl::Create(1, s0), *main_db, fallbacks),
      IsOkAndHolds(UnorderedElementsAre(DataItem(arolla::Text("x")),
                                        DataItem(arolla::Text("y")))));
}

TEST_P(SchemaInfoTest, GetSchemasAttrNames_NoFollow) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema = AllocateSchema();
  TriplesT schema_triples = {{schema, {{"x", DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);
  auto nofollow =
      DataItem(CreateNoFollowWithMainObject(schema.value<ObjectId>()));

  DataBagImplPtr main_db = GetMainDb(db);
  auto fallback_db = GetFallbackDb(db);
  auto fallbacks = std::vector<const DataBagImpl*>({fallback_db.get()});
  EXPECT_THAT(GetSchemasAttrNames(DataSliceImpl::Create(1, nofollow), *main_db,
                                  fallbacks),
              IsOkAndHolds(IsEmpty()));
}

TEST(SchemaInfoStaticTest, GetSchemaAttrSchema) {
  EXPECT_EQ(GetSchemaAttrSchema(schema::kSchemaNameAttr),
            DataItem(schema::kString));
  EXPECT_EQ(GetSchemaAttrSchema(schema::kSchemaMetadataAttr),
            DataItem(schema::kObject));
  EXPECT_EQ(GetSchemaAttrSchema(schema::kListItemsSchemaAttr),
            DataItem(schema::kSchema));
  EXPECT_EQ(GetSchemaAttrSchema("x"), DataItem(schema::kSchema));
}

}  // namespace
}  // namespace koladata::internal
