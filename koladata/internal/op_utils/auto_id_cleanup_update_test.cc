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
#include "koladata/internal/op_utils/auto_values_update.h"

#include <utility>
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/testing/deep_op_utils.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/internal/uuid_object.h"

namespace koladata::internal {
namespace {

using ::koladata::internal::testing::DataBagEqual;
using ::koladata::internal::testing::deep_op_utils::DeepOpTest;
using ::koladata::internal::testing::deep_op_utils::test_param_values;

class AutoIdCleanupUpdateTest : public DeepOpTest {
 protected:
  absl::StatusOr<std::pair<TriplesT, TriplesT>> AutoIdTriples(
      const DataItem& schema, absl::string_view attr_name,
      absl::string_view namespace_name) {
    ASSIGN_OR_RETURN(auto metadata,
                     CreateUuidWithMainObject(schema, schema::kMetadataSeed));
    ASSIGN_OR_RETURN(
        auto metadata_schema,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            metadata, schema::kImplicitSchemaSeed));

    auto auto_id_indicator = DataItem(arolla::Text(
        absl::StrCat(AutoIdsUpdateOp::kAutoIdPrefix, namespace_name)));
    auto auto_id_indicator_schema = CreateSchemaUuidFromFields(
        absl::StrCat("__named_schema__", auto_id_indicator), {}, {});

    TriplesT data_triples = {
        {metadata,
         {{schema::kSchemaAttr, metadata_schema},
          {attr_name, auto_id_indicator_schema}}},
    };
    TriplesT schema_triples = {
        {metadata_schema, {{attr_name, DataItem(schema::kSchema)}}},
        {schema,
         {{attr_name, DataItem(schema::kString)},
          {schema::kSchemaMetadataAttr, metadata}}},
        {auto_id_indicator_schema,
         {{schema::kSchemaNameAttr, auto_id_indicator}}},
    };
    return std::make_pair(data_triples, schema_triples);
  }

  absl::StatusOr<std::pair<TriplesT, TriplesT>> AutoIdCleanupTriples(
      const DataItem& schema, absl::string_view attr_name,
      absl::string_view namespace_name) {
    ASSIGN_OR_RETURN(auto metadata,
                     CreateUuidWithMainObject(schema, schema::kMetadataSeed));
    ASSIGN_OR_RETURN(
        auto metadata_schema,
        CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
            metadata, schema::kImplicitSchemaSeed));

    TriplesT data_triples = {
        {metadata, {{attr_name, DataItem()}}},
    };
    TriplesT schema_triples = {
        {metadata_schema, {{attr_name, DataItem()}}},
        {schema, {{attr_name, DataItem()}}},
    };
    return std::make_pair(data_triples, schema_triples);
  }
};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, AutoIdCleanupUpdateTest,
                         ::testing::ValuesIn(test_param_values));

TEST_P(AutoIdCleanupUpdateTest, AutoIdCleanupUpdate) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();

  // schema = {x: INT32, foo_id: AUTO_ID}
  ASSERT_OK(db->SetSchemaAttr(schema, "x", int_dtype));
  ASSERT_OK_AND_ASSIGN((auto [auto_id_data_triples, auto_id_schema_triples]),
                       AutoIdTriples(schema, "foo_id", "foo"));

  TriplesT data_triples = {
      {a0, {{"x", DataItem(1)}, {"foo_id", DataItem(arolla::Text("foo_1"))}}},
      {a1, {{"x", DataItem(2)}, {"foo_id", DataItem(arolla::Text("foo_2"))}}},
      {a2, {{"x", DataItem(3)}, {"foo_id", DataItem(arolla::Text("foo_3"))}}}};
  SetDataTriples(*db, data_triples);
  SetDataTriples(*db, auto_id_data_triples);
  SetSchemaTriples(*db, auto_id_schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(AutoIdCleanupUpdateOp(result_db.get())(
      obj_ids, schema, *GetMainDb(db), {GetFallbackDb(db).get()}));

  // Expected databag should only contain deletions.
  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->SetAttr(a0, "foo_id", DataItem()));
  ASSERT_OK(expected_db->SetAttr(a1, "foo_id", DataItem()));
  ASSERT_OK(expected_db->SetAttr(a2, "foo_id", DataItem()));
  ASSERT_OK_AND_ASSIGN((auto [cleanup_data_triples, cleanup_schema_triples]),
                       AutoIdCleanupTriples(schema, "foo_id", "foo"));
  SetDataTriples(*expected_db, cleanup_data_triples);
  SetSchemaTriples(*expected_db, cleanup_schema_triples);

  EXPECT_THAT(result_db, DataBagEqual(expected_db));
}

TEST_P(AutoIdCleanupUpdateTest, AutoIdCleanupUpdateInTheList) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(3);
  auto a0 = obj_ids[0];
  auto a1 = obj_ids[1];
  auto a2 = obj_ids[2];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();
  auto list = AllocateEmptyLists(1)[0];
  auto list_schema = AllocateSchema();

  // list_schema = List[{x: INT32, foo_id: AUTO_ID}]
  ASSERT_OK(db->SetSchemaAttr(schema, "x", int_dtype));
  ASSERT_OK_AND_ASSIGN((auto [auto_id_data_triples, auto_id_schema_triples]),
                       AutoIdTriples(schema, "foo_id", "foo"));

  TriplesT data_triples = {
      {a0, {{"x", DataItem(1)}, {"foo_id", DataItem(arolla::Text("foo_1"))}}},
      {a1, {{"x", DataItem(2)}, {"foo_id", DataItem(arolla::Text("foo_2"))}}},
      {a2, {{"x", DataItem(3)}, {"foo_id", DataItem(arolla::Text("foo_3"))}}}};
  TriplesT list_schema_triples = {
      {list_schema, {{schema::kListItemsSchemaAttr, schema}}},
  };
  ASSERT_OK(db->ExtendList(list, obj_ids));
  SetSchemaTriples(*db, list_schema_triples);
  SetDataTriples(*db, data_triples);
  SetDataTriples(*db, auto_id_data_triples);
  SetSchemaTriples(*db, auto_id_schema_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(AutoIdCleanupUpdateOp(result_db.get())(
      list, list_schema, *GetMainDb(db), {GetFallbackDb(db).get()}));

  // Expected databag should only contain deletions.
  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->SetAttr(a0, "foo_id", DataItem()));
  ASSERT_OK(expected_db->SetAttr(a1, "foo_id", DataItem()));
  ASSERT_OK(expected_db->SetAttr(a2, "foo_id", DataItem()));
  ASSERT_OK_AND_ASSIGN((auto [cleanup_data_triples, cleanup_schema_triples]),
                       AutoIdCleanupTriples(schema, "foo_id", "foo"));
  SetDataTriples(*expected_db, cleanup_data_triples);
  SetSchemaTriples(*expected_db, cleanup_schema_triples);

  EXPECT_THAT(result_db, DataBagEqual(expected_db));
}


TEST_P(AutoIdCleanupUpdateTest, AutoIdCleanupUpdateKeepOtherMetadata) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(1);
  auto a0 = obj_ids[0];
  auto int_dtype = DataItem(schema::kInt32);
  auto schema = AllocateSchema();

  // Setup schema: x (INT32), foo_id (AUTO_ID)
  ASSERT_OK(db->SetSchemaAttr(schema, "x", int_dtype));
  ASSERT_OK_AND_ASSIGN((auto [auto_id_data_triples, auto_id_schema_triples]),
                       AutoIdTriples(schema, "foo_id", "foo"));
  SetDataTriples(*db, auto_id_data_triples);
  SetSchemaTriples(*db, auto_id_schema_triples);

  // Add another metadata (not AUTO_ID).
  ASSERT_OK_AND_ASSIGN(auto metadata, CreateUuidWithMainObject(
                                           schema, schema::kMetadataSeed));
  auto other_meta_schema = AllocateSchema();
  ASSERT_OK(db->SetSchemaAttr(other_meta_schema, schema::kSchemaNameAttr,
                              DataItem(arolla::Text("OTHER_METADATA"))));
  ASSERT_OK(db->SetAttr(metadata, "other_attr", other_meta_schema));
  ASSERT_OK_AND_ASSIGN(auto metadata_schema, db->GetObjSchemaAttr(metadata));
  ASSERT_OK(db->SetSchemaAttr(metadata_schema, "other_attr",
                              DataItem(schema::kSchema)));

  // Setup data
  TriplesT data_triples = {
      {a0, {{"x", DataItem(1)}, {"foo_id", DataItem(arolla::Text("foo_1"))}}}};
  SetDataTriples(*db, data_triples);

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(AutoIdCleanupUpdateOp(result_db.get())(
      obj_ids, schema, *GetMainDb(db), {GetFallbackDb(db).get()}));

  // Expected databag should only contain deletions for foo_id.
  auto expected_db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(expected_db->SetAttr(a0, "foo_id", DataItem()));
  ASSERT_OK_AND_ASSIGN((auto [cleanup_data_triples, cleanup_schema_triples]),
                       AutoIdCleanupTriples(schema, "foo_id", "foo"));
  SetDataTriples(*expected_db, cleanup_data_triples);
  SetSchemaTriples(*expected_db, cleanup_schema_triples);

  EXPECT_THAT(result_db, DataBagEqual(expected_db));
}

}  // namespace
}  // namespace koladata::internal
