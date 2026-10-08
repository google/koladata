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
#include <utility>
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

bool ImplicitOrObjectsCastCompatible(const DataItem& from_schema,
                                     const DataItem& to_schema) {
  if (from_schema.is_struct_schema()) {
    // Struct schemas are traversed further.
    return to_schema.is_struct_schema();
  }
  if (from_schema.is_object_schema()) {
    return true;
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
  ASSERT_OK_AND_ASSIGN(auto diffs,
                       deep_schema_compatible_op.GetDiffPaths(result_item));
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
  ASSERT_OK_AND_ASSIGN(auto diffs,
                       deep_schema_compatible_op.GetDiffPaths(result_item));
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
  ASSERT_OK_AND_ASSIGN(auto diffs,
                       deep_schema_compatible_op.GetDiffPaths(result_item));
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
  ASSERT_OK_AND_ASSIGN(auto diffs,
                       deep_schema_compatible_op.GetDiffPaths(result_item));
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
  ASSERT_OK_AND_ASSIGN(auto diffs,
                       deep_schema_compatible_op.GetDiffPaths(result_item));
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
  ASSERT_OK_AND_ASSIGN(auto diffs,
                       deep_schema_compatible_op.GetDiffPaths(result_item));
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
  ASSERT_OK_AND_ASSIGN(auto diffs,
                       deep_schema_compatible_op.GetDiffPaths(result_item));
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
  ASSERT_OK_AND_ASSIGN(auto diffs,
                       deep_schema_compatible_op.GetDiffPaths(result_item));
  std::vector<std::string> diff_paths;
  diffs.reserve(diffs.size());
  for (const auto& diff : diffs) {
    diff_paths.push_back(
        TraverseHelper::TransitionKeySequenceToAccessPath(diff.path));
  }
  EXPECT_THAT(diff_paths, ::testing::ElementsAre(".x"));
}

TEST_P(DeepSchemaCompatibleTest, ObjectAttributeEarlyStop) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto schema_a = AllocateSchema();
  auto schema_b = AllocateSchema();
  auto list_schema = AllocateSchema();
  TriplesT schema_triples = {
      {schema_a, {{"self", schema_a}, {"x", DataItem(schema::kObject)}}},
      {schema_b, {{"self", schema_b}, {"x", list_schema}}},
      {list_schema, {{schema::kListItemsSchemaAttr, schema_b}}}};
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  auto result_db = DataBagImpl::CreateEmptyDatabag();
  auto deep_schema_compatible_op = DeepSchemaCompatibleOp(
      result_db.get(), {}, ImplicitOrObjectsCastCompatible);
  ASSERT_OK_AND_ASSIGN(
      (auto [is_compatible, _]),
      deep_schema_compatible_op(schema_a, *GetMainDb(db),
                                {GetFallbackDb(db).get()}, schema_b,
                                *GetMainDb(db), {GetFallbackDb(db).get()}));
  EXPECT_TRUE(is_compatible);
}

TEST_P(DeepSchemaCompatibleTest, SchemaNameAndMetadataIgnoredWithAllowFlags) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto entity_schema = AllocateSchema();
  auto named_schema = AllocateSchema();
  auto schema_with_metadata = AllocateSchema();
  TriplesT schema_triples = {
      {entity_schema, {{"x", DataItem(schema::kInt32)}}},
      {named_schema,
       {{schema::kSchemaNameAttr, DataItem(arolla::Text("named"))},
        {"x", DataItem(schema::kInt32)}}},
      {schema_with_metadata,
       {{schema::kSchemaMetadataAttr, DataItem(AllocateSingleObject())},
        {"x", DataItem(schema::kInt32)}}},
  };
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  // Schema name and metadata present on one side only are ignored regardless
  // of allow_removing_attrs and allow_new_attrs.
  for (const auto& params :
       std::vector<DeepSchemaCompatibleOp::SchemaCompatibleParams>{
           {.allow_removing_attrs = true},
           {.allow_new_attrs = true},
           {.allow_removing_attrs = true, .allow_new_attrs = true},
       }) {
    for (const auto& [from_schema, to_schema] :
         std::vector<std::pair<DataItem, DataItem>>{
             {entity_schema, named_schema},
             {named_schema, entity_schema},
             {entity_schema, schema_with_metadata},
             {schema_with_metadata, entity_schema},
         }) {
      SCOPED_TRACE(::testing::Message()
                   << from_schema.DebugString() << " -> "
                   << to_schema.DebugString()
                   << ", allow_removing_attrs=" << params.allow_removing_attrs
                   << ", allow_new_attrs=" << params.allow_new_attrs);
      auto result_db = DataBagImpl::CreateEmptyDatabag();
      auto op = DeepSchemaCompatibleOp(result_db.get(), params,
                                       ImplicitCastCompatible);
      ASSERT_OK_AND_ASSIGN(
          (auto [is_compatible, _]),
          op(from_schema, *GetMainDb(db), {GetFallbackDb(db).get()}, to_schema,
             *GetMainDb(db), {GetFallbackDb(db).get()}));
      EXPECT_TRUE(is_compatible);
    }
  }
}

TEST_P(DeepSchemaCompatibleTest, ListOrDictAttrsNotRemovableOrNew) {
  auto db = DataBagImpl::CreateEmptyDatabag();
  auto list_schema = AllocateSchema();
  auto dict_schema = AllocateSchema();
  auto entity_schema = AllocateSchema();
  TriplesT schema_triples = {
      {list_schema, {{schema::kListItemsSchemaAttr, DataItem(schema::kInt32)}}},
      {dict_schema,
       {{schema::kDictKeysSchemaAttr, DataItem(schema::kString)},
        {schema::kDictValuesSchemaAttr, DataItem(schema::kInt32)}}},
      {entity_schema, {{"x", DataItem(schema::kInt32)}}},
  };
  SetSchemaTriples(*db, schema_triples);
  SetSchemaTriples(*db, GenSchemaTriplesFoTests());
  SetDataTriples(*db, GenDataTriplesForTest());

  // Even with allow_removing_attrs and allow_new_attrs, lists, dicts and
  // entities are incompatible with each other.
  for (const auto& [from_schema, to_schema] :
       std::vector<std::pair<DataItem, DataItem>>{
           {list_schema, entity_schema},
           {dict_schema, entity_schema},
           {entity_schema, list_schema},
           {entity_schema, dict_schema},
           {list_schema, dict_schema},
       }) {
    SCOPED_TRACE(from_schema.DebugString() + " -> " + to_schema.DebugString());
    auto result_db = DataBagImpl::CreateEmptyDatabag();
    auto op = DeepSchemaCompatibleOp(
        result_db.get(),
        {.allow_removing_attrs = true, .allow_new_attrs = true},
        ImplicitCastCompatible);
    ASSERT_OK_AND_ASSIGN(
        (auto [is_compatible, _]),
        op(from_schema, *GetMainDb(db), {GetFallbackDb(db).get()}, to_schema,
           *GetMainDb(db), {GetFallbackDb(db).get()}));
    EXPECT_FALSE(is_compatible);
  }
}

}  // namespace

}  // namespace koladata::internal
