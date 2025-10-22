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
#include "koladata/metadata_utils.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "arolla/util/text.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/object_factories.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"

namespace koladata {

namespace {

using ::absl_testing::StatusIs;
using ::testing::AllOf;
using ::testing::HasSubstr;

TEST(GetMetadataForSchemaSliceTest, NotASchema) {
  auto ds = test::DataItem(42);
  EXPECT_THAT(GetMetadataForSchemaSlice(ds),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("failed to get metadata")));
}

TEST(GetMetadataForSchemaSliceTest, PrimitiveSchema) {
  auto ds = test::DataItem(42);
  EXPECT_THAT(GetMetadataForSchemaSlice(ds.GetSchema()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("primitives do not have attributes")));
}

TEST(GetMetadataForSchemaSliceTest, MissingMetadata) {
  auto db = DataBag::EmptyMutable();
  auto schema = test::EntitySchema(
      {"a", "b"},
      {test::DataItem(schema::kString), test::DataItem(schema::kString)}, db);
  EXPECT_THAT(GetMetadataForSchemaSlice(schema),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       AllOf(HasSubstr("the attribute"),
                             HasSubstr("is missing on the schema"))));
}

TEST(GetMetadataForSchemaSliceTest, GetMetadata) {
  auto db = DataBag::EmptyMutable();
  auto schema = test::EntitySchema(
      {"a", "b"},
      {test::DataItem(schema::kString), test::DataItem(schema::kString)}, db);
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       CreateMetadata(db, schema, {"a"}, {test::DataItem(1)}));
  ASSERT_OK_AND_ASSIGN(auto result, GetMetadataForSchemaSlice(schema));
  ASSERT_THAT(result,
              testing::IsDeepEquivalentTo(
                  metadata, {.schemas_equality = true, .ids_equality = true}));
}

TEST(GetOrderedAttrNamesTest, EntitySlice) {
  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto schema, CreateSchemaWithOrderedAttrs(
                                        db, {"foo", "a", "bar"},
                                        {test::DataItem(schema::kString),
                                         test::DataItem(schema::kInt32),
                                         test::DataItem(schema::kString)}));
  auto ds = test::AllocateDataSlice(3, schema.item(), db);
  ASSERT_OK_AND_ASSIGN(auto attr_names, GetOrderedAttrNames(ds));
  auto expected = test::DataSlice<arolla::Text>({"foo", "a", "bar"});
  EXPECT_THAT(attr_names, testing::IsDeepEquivalentTo(expected));
}

TEST(GetOrderedAttrNamesAsVectorTest, GetOrderedAttrNamesAsVector) {
  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto schema, CreateSchemaWithOrderedAttrs(
                                        db, {"foo", "a", "bar"},
                                        {test::DataItem(schema::kString),
                                         test::DataItem(schema::kInt32),
                                         test::DataItem(schema::kString)}));
  auto ds = test::AllocateDataSlice(3, schema.item(), db);
  ASSERT_OK_AND_ASSIGN(auto attr_names, GetOrderedOrLexicographicAttrNames(ds));
  EXPECT_EQ(attr_names, std::vector<std::string>({"foo", "a", "bar"}));
}


TEST(GetOrderedAttrNamesAsVectorTest, GetEmptyOrderedAttrNamesAsVector) {
  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto schema, CreateSchemaWithOrderedAttrs(db, {}, {}));
  auto ds = test::AllocateDataSlice(3, schema.item(), db);
  ASSERT_OK_AND_ASSIGN(auto attr_names, GetOrderedOrLexicographicAttrNames(ds));
  EXPECT_TRUE(attr_names.empty());
}

TEST(GetOrderedAttrNamesAsVectorTest, ErrorOnMissingOrder) {
  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateEntitySchema(db, {"foo", "a", "bar"},
                                          {test::DataItem(schema::kString),
                                           test::DataItem(schema::kInt32),
                                           test::DataItem(schema::kString)}));
  auto ds = test::AllocateDataSlice(3, schema.item(), db);
  EXPECT_THAT(GetOrderedOrLexicographicAttrNames(ds),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       AllOf(HasSubstr("the attribute"),
                             HasSubstr("is missing on the schema"))));
}

TEST(GetOrderedAttrNamesAsVectorTest, ErrorOnMissingOrderNoAttrs) {
  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto schema, CreateEntitySchema(db, {}, {}));
  auto ds = test::AllocateDataSlice(3, schema.item(), db);
  EXPECT_THAT(GetOrderedOrLexicographicAttrNames(ds),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       AllOf(HasSubstr("the attribute"),
                             HasSubstr("is missing on the schema"))));
}

TEST(GetOrderedAttrNamesAsVectorTest, FallbackToLexicographicOrder) {
  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateEntitySchema(db, {"foo", "a", "bar"},
                                          {test::DataItem(schema::kString),
                                           test::DataItem(schema::kInt32),
                                           test::DataItem(schema::kString)}));
  auto ds = test::AllocateDataSlice(3, schema.item(), db);
  ASSERT_OK_AND_ASSIGN(
      auto attr_names,
      GetOrderedOrLexicographicAttrNames(ds, /*assert_order_specified=*/false));
  EXPECT_EQ(attr_names, std::vector<std::string>({"a", "bar", "foo"}));
}

}  // namespace
}  // namespace koladata
