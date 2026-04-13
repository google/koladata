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
#include "koladata/internal/op_utils/apply_filter.h"

#include <initializer_list>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/testing/deep_op_utils.h"
#include "koladata/internal/testing/matchers.h"

namespace koladata::internal {
namespace {

using ::koladata::internal::testing::DataBagEqual;

using testing::deep_op_utils::DeepOpTest;
using testing::deep_op_utils::test_param_values;

using ::absl::StatusCode::kInvalidArgument;
using ::absl_testing::StatusIs;
using ::testing::HasSubstr;

class ApplyFilterTest : public DeepOpTest {};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, ApplyFilterTest,
                         ::testing::ValuesIn(test_param_values));

TEST_P(ApplyFilterTest, ShallowEntitySlice) {
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

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ApplyFilterOp(*result_db.get())(
      obj_ids, schema, *GetMainDb(db), {GetFallbackDb(db).get()}, schema,
      *GetMainDb(db), {GetFallbackDb(db).get()}));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);
  EXPECT_THAT(result_db, DataBagEqual(expected_db));
}

TEST_P(ApplyFilterTest, PrimitiveWithObjectSchema_AnyPrimitiveFilter) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto item = DataItem(1);
  auto schema = DataItem(schema::kObject);
  auto filter = schema_filters::AnyPrimitiveFilter();

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ApplyFilterOp(*result_db.get())(
      item, schema, *GetMainDb(db), {GetFallbackDb(db).get()}, filter,
      *GetMainDb(db), {GetFallbackDb(db).get()}));
}

TEST_P(ApplyFilterTest, PrimitiveWithObjectSchema_PrimitiveFilter) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto item = DataItem(1);
  auto schema = DataItem(schema::kObject);
  auto filter = DataItem(schema::kInt32);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ApplyFilterOp(*result_db.get())(
      item, schema, *GetMainDb(db), {GetFallbackDb(db).get()}, filter,
      *GetMainDb(db), {GetFallbackDb(db).get()}));
}

TEST_P(ApplyFilterTest, PrimitiveWithObjectSchema_PrimitiveFilter_Mismatch) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto item = DataItem(1);
  auto schema = DataItem(schema::kObject);
  auto filter = DataItem(schema::kFloat32);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto status = ApplyFilterOp(*result_db.get())(
      item, schema, *GetMainDb(db), {GetFallbackDb(db).get()}, filter,
      *GetMainDb(db), {GetFallbackDb(db).get()});
  EXPECT_THAT(status,
              StatusIs(kInvalidArgument, HasSubstr("does not match schema")));
}

TEST_P(ApplyFilterTest, PrimitiveWithObjectSchema_AnySchemaFilter) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto item = DataItem(1);
  auto schema = DataItem(schema::kObject);
  auto filter = schema_filters::AnySchemaFilter();

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ApplyFilterOp(*result_db.get())(
      item, schema, *GetMainDb(db), {GetFallbackDb(db).get()}, filter,
      *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_THAT(result_db, DataBagEqual(DataBagImpl::CreateEmptyDatabag()));
}

TEST_P(ApplyFilterTest, Object_AnySchemaFilter) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj = DataItem(AllocateSingleObject());
  auto filter = schema_filters::AnySchemaFilter();

  auto item_schema = AllocateSchema();
  TriplesT schema_triples = {{item_schema, {{"x", DataItem(schema::kInt32)}}}};
  TriplesT data_triples = {
      {obj, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(1)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ApplyFilterOp(*result_db.get())(
      obj, DataItem(schema::kObject), *GetMainDb(db), {GetFallbackDb(db).get()},
      filter, *GetMainDb(db), {GetFallbackDb(db).get()}));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  SetDataTriples(*expected_db, data_triples);
  EXPECT_THAT(result_db, DataBagEqual(expected_db));
}

TEST_P(ApplyFilterTest, Object_EntityFilter) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj = DataItem(AllocateSingleObject());

  auto item_schema = AllocateSchema();
  TriplesT data_triples = {{obj,
                            {{schema::kSchemaAttr, item_schema},
                             {"x", DataItem(1)},
                             {"y", DataItem(2)}}}};
  TriplesT schema_triples = {
      {item_schema,
       {{"x", DataItem(schema::kInt32)}, {"y", DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);

  auto filter_schema = AllocateSchema();
  TriplesT filter_schema_triples = {
      {filter_schema, {{"x", DataItem(schema::kInt32)}}}};
  auto filter_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*filter_db, filter_schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ApplyFilterOp(*result_db.get())(
      obj, DataItem(schema::kObject), *GetMainDb(db), {GetFallbackDb(db).get()},
      filter_schema, *GetMainDb(filter_db), {GetFallbackDb(filter_db).get()}));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  TriplesT expected_schema_triples = {
      {item_schema, {{"x", DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*expected_db, expected_schema_triples);
  TriplesT expected_data_triples = {
      {obj, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(1)}}}};
  SetDataTriples(*expected_db, expected_data_triples);
  EXPECT_THAT(result_db, DataBagEqual(expected_db));
}

TEST_P(ApplyFilterTest, Object_EntityFilter_MissingAttr) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj = DataItem(AllocateSingleObject());

  auto item_schema = AllocateSchema();
  TriplesT schema_triples = {{item_schema, {{"x", DataItem(schema::kInt32)}}}};
  TriplesT data_triples = {
      {obj, {{schema::kSchemaAttr, item_schema}, {"x", DataItem(1)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetDataTriples(*db, data_triples);

  auto filter_schema = AllocateSchema();
  TriplesT filter_schema_triples = {
      {filter_schema, {{"y", DataItem(schema::kInt32)}}}};
  auto filter_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*filter_db, filter_schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto status = ApplyFilterOp(*result_db.get())(
      obj, DataItem(schema::kObject), *GetMainDb(db), {GetFallbackDb(db).get()},
      filter_schema, *GetMainDb(filter_db), {GetFallbackDb(filter_db).get()});
  EXPECT_THAT(
      status,
      StatusIs(kInvalidArgument,
               HasSubstr("the attribute 'y' is missing on the schema")));
}

TEST_P(ApplyFilterTest, ListObject_AnySchemaFilter) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto list = DataItem(AllocateSingleList());
  auto filter = schema_filters::AnySchemaFilter();

  auto int_dtype = DataItem(schema::kInt32);
  auto item_schema = AllocateSchema();

  TriplesT schema_triples = {
      {item_schema, {{schema::kListItemsSchemaAttr, int_dtype}}}};
  SetSchemaTriples(*db, schema_triples);

  ASSERT_OK(db->SetAttr(list, schema::kSchemaAttr, item_schema));
  ASSERT_OK(db->AppendToList(list, DataItem(1)));
  ASSERT_OK(db->AppendToList(list, DataItem(2)));

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ApplyFilterOp(*result_db.get())(
      list, DataItem(schema::kObject), *GetMainDb(db),
      {GetFallbackDb(db).get()}, filter, *GetMainDb(db),
      {GetFallbackDb(db).get()}));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  ASSERT_OK(expected_db->SetAttr(list, schema::kSchemaAttr, item_schema));
  ASSERT_OK(expected_db->AppendToList(list, DataItem(1)));
  ASSERT_OK(expected_db->AppendToList(list, DataItem(2)));

  EXPECT_THAT(result_db, DataBagEqual(expected_db));
}

TEST_P(ApplyFilterTest, DictObject_AnySchemaFilter) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto dict = DataItem(AllocateSingleDict());
  auto filter = schema_filters::AnySchemaFilter();

  auto item_schema = AllocateSchema();

  TriplesT schema_triples = {
      {item_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kInt32)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);

  ASSERT_OK(db->SetAttr(dict, schema::kSchemaAttr, item_schema));
  ASSERT_OK(db->SetInDict(dict, DataItem(1), DataItem(10)));
  ASSERT_OK(db->SetInDict(dict, DataItem(2), DataItem(20)));

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ApplyFilterOp(*result_db.get())(
      dict, DataItem(schema::kObject), *GetMainDb(db),
      {GetFallbackDb(db).get()}, filter, *GetMainDb(db),
      {GetFallbackDb(db).get()}));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  ASSERT_OK(expected_db->SetAttr(dict, schema::kSchemaAttr, item_schema));
  ASSERT_OK(expected_db->SetInDict(dict, DataItem(1), DataItem(10)));
  ASSERT_OK(expected_db->SetInDict(dict, DataItem(2), DataItem(20)));

  EXPECT_THAT(result_db, DataBagEqual(expected_db));
}

TEST_P(ApplyFilterTest, Entity_AnySchemaFilter) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto entity = DataItem(AllocateSingleObject());
  auto schema = AllocateSchema();
  auto filter = schema_filters::AnySchemaFilter();

  TriplesT schema_triples = {{schema, {{"a", DataItem(schema::kInt32)}}}};
  SetSchemaTriples(*db, schema_triples);

  ASSERT_OK(db->SetAttr(entity, "a", DataItem(1)));

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(ApplyFilterOp(*result_db.get())(
      entity, schema, *GetMainDb(db), {GetFallbackDb(db).get()}, filter,
      *GetMainDb(db), {GetFallbackDb(db).get()}));

  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  SetSchemaTriples(*expected_db, schema_triples);
  ASSERT_OK(expected_db->SetAttr(entity, "a", DataItem(1)));

  EXPECT_THAT(result_db, DataBagEqual(expected_db));
}

}  // namespace
}  // namespace koladata::internal
