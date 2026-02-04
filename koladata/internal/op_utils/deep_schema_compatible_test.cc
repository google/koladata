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
#include "koladata/internal/op_utils/deep_schema_compatible.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/op_utils/traverse_helper.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/testing/deep_op_utils.h"
#include "koladata/internal/uuid_object.h"

namespace koladata::internal {
namespace {

using testing::deep_op_utils::DeepOpTest;
using testing::deep_op_utils::test_param_values;

bool ImplicitCastCompatible(const DataItem& from_schema,
                            const DataItem& to_schema) {
  if (from_schema.is_struct_schema()) {
    // Struct schemas are traversed further.
    return to_schema.is_struct_schema();
  }
  // Validate schemas compatibility.
  return schema::IsImplicitlyCastableTo(from_schema, to_schema);
}

class DeepSchemaCompatibleTest : public DeepOpTest {};

INSTANTIATE_TEST_SUITE_P(MainOrFallback, DeepSchemaCompatibleTest,
                         ::testing::ValuesIn(test_param_values));

TEST_P(DeepSchemaCompatibleTest, CompatiblePrimitives) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT schema_triples = {
      {schema_a, {{"self", schema_a}, {"x", DataItem(schema::kInt32)}}},
      {schema_b, {{"self", schema_b}, {"x", DataItem(schema::kFloat32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op =
      DeepSchemaCompatibleOp(result_db.get(), {}, ImplicitCastCompatible);
  ASSERT_OK_AND_ASSIGN(
      (auto [is_compatible, result_item]),
      deep_schema_compatible_op(schema_a, *GetMainDb(db),
                                {GetFallbackDb(db).get()}, schema_b,
                                *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_TRUE(is_compatible);
  ASSERT_OK_AND_ASSIGN(auto diffs, deep_schema_compatible_op.GetDiffPaths(
                                       result_item, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths, ::testing::IsEmpty());
}

TEST_P(DeepSchemaCompatibleTest, IncompatiblePrimitives) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT schema_triples = {
      {schema_a, {{"self", schema_a}, {"x", DataItem(schema::kInt32)}}},
      {schema_b, {{"self", schema_b}, {"x", DataItem(schema::kString)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op =
      DeepSchemaCompatibleOp(result_db.get(), {}, ImplicitCastCompatible);
  ASSERT_OK_AND_ASSIGN(
      (auto [is_compatible, result_item]),
      deep_schema_compatible_op(schema_a, *GetMainDb(db),
                                {GetFallbackDb(db).get()}, schema_b,
                                *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_FALSE(is_compatible);
  ASSERT_OK_AND_ASSIGN(auto diffs, deep_schema_compatible_op.GetDiffPaths(
                                       result_item, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths, ::testing::ElementsAre(".x"));
}

TEST_P(DeepSchemaCompatibleTest, AllowAll) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT schema_triples = {{schema_a,
                              {{"self", schema_a},
                               {"x", DataItem(schema::kInt32)},
                               {"y", DataItem(schema::kInt32)}}},
                             {schema_b,
                              {{"self", schema_b},
                               {"x", DataItem(schema::kFloat32)},
                               {"z", DataItem(schema::kString)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op = DeepSchemaCompatibleOp(
      result_db.get(), {.allow_removing_attrs = true, .allow_new_attrs = true},
      ImplicitCastCompatible);
  ASSERT_OK_AND_ASSIGN(
      (auto [is_compatible, result_item]),
      deep_schema_compatible_op(schema_a, *GetMainDb(db),
                                {GetFallbackDb(db).get()}, schema_b,
                                *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_TRUE(is_compatible);
  ASSERT_OK_AND_ASSIGN(auto diffs, deep_schema_compatible_op.GetDiffPaths(
                                       result_item, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  diffs.reserve(diffs.size());
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths, ::testing::IsEmpty());
}

TEST_P(DeepSchemaCompatibleTest, PartialFalse) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT schema_triples = {
      {schema_a,
       {{"self", schema_a},
        {"x", DataItem(schema::kInt32)},
        {"y", DataItem(schema::kInt32)}}},
      {schema_b, {{"self", schema_b}, {"x", DataItem(schema::kFloat32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op =
      DeepSchemaCompatibleOp(result_db.get(), {}, ImplicitCastCompatible);
  ASSERT_OK_AND_ASSIGN(
      (auto [is_compatible, result_item]),
      deep_schema_compatible_op(schema_a, *GetMainDb(db),
                                {GetFallbackDb(db).get()}, schema_b,
                                *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_FALSE(is_compatible);
  ASSERT_OK_AND_ASSIGN(auto diffs, deep_schema_compatible_op.GetDiffPaths(
                                       result_item, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  diffs.reserve(diffs.size());
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths, ::testing::ElementsAre(".y"));
}

TEST_P(DeepSchemaCompatibleTest, PartialFalseDeep) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto schema_a = AllocateSchema();
  auto schema_a_bar = AllocateSchema();
  auto schema_b = AllocateSchema();
  auto schema_b_bar = AllocateSchema();
  TriplesT schema_triples = {
      {schema_a, {{"bar", schema_a_bar}}},
      {schema_a_bar,
       {{"parent", schema_a},
        {"x", DataItem(schema::kInt32)},
        {"y", DataItem(schema::kInt32)}}},
      {schema_b, {{"bar", schema_b_bar}}},
      {schema_b_bar,
       {{"parent", schema_b}, {"x", DataItem(schema::kFloat32)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op =
      DeepSchemaCompatibleOp(result_db.get(), {}, ImplicitCastCompatible);
  ASSERT_OK_AND_ASSIGN(
      (auto [is_compatible, result_item]),
      deep_schema_compatible_op(schema_a, *GetMainDb(db),
                                {GetFallbackDb(db).get()}, schema_b,
                                *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_FALSE(is_compatible);
  ASSERT_OK_AND_ASSIGN(auto diffs, deep_schema_compatible_op.GetDiffPaths(
                                       result_item, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  diffs.reserve(diffs.size());
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths, ::testing::ElementsAre(".bar.y"));
}

TEST_P(DeepSchemaCompatibleTest, LhsOnly) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT schema_triples = {{schema_a,
                              {{"self", schema_a},
                               {"x", DataItem(schema::kInt32)},
                               {"y", DataItem(schema::kInt32)}}},
                             {schema_b, {{"self", schema_b}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op =
      DeepSchemaCompatibleOp(result_db.get(), {}, ImplicitCastCompatible);
  ASSERT_OK_AND_ASSIGN(
      (auto [is_compatible, result_item]),
      deep_schema_compatible_op(schema_a, *GetMainDb(db),
                                {GetFallbackDb(db).get()}, schema_b,
                                *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_FALSE(is_compatible);
  ASSERT_OK_AND_ASSIGN(auto diffs, deep_schema_compatible_op.GetDiffPaths(
                                       result_item, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  diffs.reserve(diffs.size());
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths, ::testing::UnorderedElementsAre(".x", ".y"));
}

TEST_P(DeepSchemaCompatibleTest, LhsOnlyAllowRemovingAttrs) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT schema_triples = {{schema_a,
                              {{"self", schema_a},
                               {"x", DataItem(schema::kInt32)},
                               {"y", DataItem(schema::kInt32)}}},
                             {schema_b, {{"self", schema_b}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op = DeepSchemaCompatibleOp(
      result_db.get(), {.allow_removing_attrs = true}, ImplicitCastCompatible);
  ASSERT_OK_AND_ASSIGN(
      (auto [is_compatible, result_item]),
      deep_schema_compatible_op(schema_a, *GetMainDb(db),
                                {GetFallbackDb(db).get()}, schema_b,
                                *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_TRUE(is_compatible);
}

TEST_P(DeepSchemaCompatibleTest, NamedSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s1 = AllocateSchema();
  auto s2 = AllocateSchema();
  TriplesT schema_triples = {
      {s1, {{"a", DataItem(schema::kInt32)}}},
      {s2,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("s2"))},
        {"a", DataItem(schema::kFloat32)}}},
  };
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op =
      DeepSchemaCompatibleOp(result_db.get(), {}, ImplicitCastCompatible);
  ASSERT_OK_AND_ASSIGN(
      (auto [is_compatible, result_item]),
      deep_schema_compatible_op(s1, *GetMainDb(db), {GetFallbackDb(db).get()},
                                s2, *GetMainDb(db), {GetFallbackDb(db).get()}));
  EXPECT_TRUE(is_compatible);
}

TEST_P(DeepSchemaCompatibleTest, NamedSchemaToNamedSchema) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s1 = AllocateSchema();
  auto s2 = AllocateSchema();
  TriplesT schema_triples = {
      {s1,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("s1"))},
        {"a", DataItem(schema::kInt32)}}},
      {s2,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("s2"))},
        {"a", DataItem(schema::kFloat32)}}},
  };
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op =
      DeepSchemaCompatibleOp(result_db.get(), {}, ImplicitCastCompatible);
  ASSERT_OK_AND_ASSIGN(
      (auto [is_compatible, result_item]),
      deep_schema_compatible_op(s1, *GetMainDb(db), {GetFallbackDb(db).get()},
                                s2, *GetMainDb(db), {GetFallbackDb(db).get()}));
  EXPECT_TRUE(is_compatible);
}

TEST_P(DeepSchemaCompatibleTest, ToSchemaWithMetadata) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto s1 = DataItem(AllocateSchema());
  auto schema = DataItem(AllocateSchema());
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       CreateUuidWithMainObject(schema, schema::kMetadataSeed));
  ASSERT_OK_AND_ASSIGN(
      auto metadata_schema,
      CreateUuidWithMainObject<ObjectId::kUuidImplicitSchemaFlag>(
          metadata, schema::kImplicitSchemaSeed));
  auto a1 = DataItem(AllocateSingleObject());
  TriplesT schema_triples = {
      {s1, {{"x", DataItem(schema::kInt32)}}},
      {schema,
       {{"x", DataItem(schema::kFloat32)},
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

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op =
      DeepSchemaCompatibleOp(result_db.get(), {}, ImplicitCastCompatible);
  ASSERT_OK_AND_ASSIGN((auto [is_compatible, result_item]),
                       deep_schema_compatible_op(
                           s1, *GetMainDb(db), {GetFallbackDb(db).get()},
                           schema, *GetMainDb(db), {GetFallbackDb(db).get()}));
  EXPECT_TRUE(is_compatible);
}

TEST_P(DeepSchemaCompatibleTest, RhsOnly) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT schema_triples = {{schema_a,
                              {{"self", schema_a},
                               {"x", DataItem(schema::kInt32)},
                               {"y", DataItem(schema::kInt32)}}},
                             {schema_b, {{"self", schema_b}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op =
      DeepSchemaCompatibleOp(result_db.get(), {}, ImplicitCastCompatible);
  ASSERT_OK_AND_ASSIGN(
      (auto [is_compatible, result_item]),
      deep_schema_compatible_op(schema_b, *GetMainDb(db),
                                {GetFallbackDb(db).get()}, schema_a,
                                *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_FALSE(is_compatible);
  ASSERT_OK_AND_ASSIGN(auto diffs, deep_schema_compatible_op.GetDiffPaths(
                                       result_item, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  diffs.reserve(diffs.size());
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths, ::testing::UnorderedElementsAre(".x", ".y"));
}

TEST_P(DeepSchemaCompatibleTest, RhsOnlyAllowNewAttrs) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT schema_triples = {{schema_a,
                              {{"self", schema_a},
                               {"x", DataItem(schema::kInt32)},
                               {"y", DataItem(schema::kInt32)}}},
                             {schema_b, {{"self", schema_b}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op = DeepSchemaCompatibleOp(
      result_db.get(), {.allow_new_attrs = true}, ImplicitCastCompatible);
  ASSERT_OK_AND_ASSIGN(
      (auto [is_compatible, result_item]),
      deep_schema_compatible_op(schema_b, *GetMainDb(db),
                                {GetFallbackDb(db).get()}, schema_a,
                                *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_TRUE(is_compatible);
}

TEST_P(DeepSchemaCompatibleTest, NotCastable) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto obj_ids = AllocateEmptyObjects(6);
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  TriplesT schema_triples = {
      {schema_a, {{"self", schema_a}, {"x", DataItem(schema::kInt32)}}},
      {schema_b, {{"self", schema_b}, {"x", DataItem(schema::kString)}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op =
      DeepSchemaCompatibleOp(result_db.get(), {}, ImplicitCastCompatible);
  ASSERT_OK_AND_ASSIGN(
      (auto [is_compatible, result_item]),
      deep_schema_compatible_op(schema_b, *GetMainDb(db),
                                {GetFallbackDb(db).get()}, schema_a,
                                *GetMainDb(db), {GetFallbackDb(db).get()}));

  EXPECT_FALSE(is_compatible);
  ASSERT_OK_AND_ASSIGN(auto diffs, deep_schema_compatible_op.GetDiffPaths(
                                       result_item, DataItem(schema::kObject)));
  std::vector<std::string> diff_paths;
  diffs.reserve(diffs.size());
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths, ::testing::ElementsAre(".x"));
}

}  // namespace

}  // namespace koladata::internal
