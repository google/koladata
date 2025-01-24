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
#include "koladata/schema_utils.h"

#include <cstdint>
#include <optional>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/casting.h"
#include "koladata/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

namespace koladata::schema {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::koladata::internal::ObjectId;
using ::koladata::testing::IsEquivalentTo;
using ::testing::MatchesRegex;

TEST(SchemaUtilsTest, GetNarrowedSchema_Item) {
  {
    // Primitive schema.
    EXPECT_THAT(GetNarrowedSchema(test::DataItem(1, schema::kInt32)),
                IsEquivalentTo(internal::DataItem(schema::kInt32)));
    EXPECT_THAT(GetNarrowedSchema(test::DataItem(std::nullopt, schema::kInt32)),
                IsEquivalentTo(internal::DataItem(schema::kInt32)));
    EXPECT_THAT(GetNarrowedSchema(test::DataItem(1, schema::kObject)),
                IsEquivalentTo(internal::DataItem(schema::kInt32)));
  }
  {
    // None schema.
    EXPECT_THAT(GetNarrowedSchema(test::DataItem(std::nullopt, schema::kNone)),
                IsEquivalentTo(internal::DataItem(schema::kNone)));
    EXPECT_THAT(
        GetNarrowedSchema(test::DataItem(std::nullopt, schema::kObject)),
        IsEquivalentTo(internal::DataItem(schema::kNone)));
  }
  {
    // Object ids.
    EXPECT_THAT(GetNarrowedSchema(test::DataItem(
                    internal::AllocateSingleObject(), schema::kObject)),
                IsEquivalentTo(internal::DataItem(schema::kObject)));
    EXPECT_THAT(GetNarrowedSchema(test::DataItem(
                    internal::AllocateSingleObject(), schema::kAny)),
                IsEquivalentTo(internal::DataItem(schema::kAny)));
    EXPECT_THAT(GetNarrowedSchema(test::DataItem(
                    internal::AllocateSingleObject(), schema::kItemId)),
                IsEquivalentTo(internal::DataItem(schema::kItemId)));
    internal::DataItem entity_schema(internal::AllocateExplicitSchema());
    EXPECT_THAT(GetNarrowedSchema(test::DataItem(
                    internal::AllocateSingleObject(), entity_schema)),
                IsEquivalentTo(entity_schema));
    EXPECT_THAT(GetNarrowedSchema(test::DataItem(std::nullopt, entity_schema)),
                IsEquivalentTo(entity_schema));
  }
}

TEST(SchemaUtilsTest, GetNarrowedSchema_Slice) {
  {
    // Primitive schema.
    EXPECT_THAT(GetNarrowedSchema(
                    test::DataSlice<int>({1, std::nullopt}, schema::kInt32)),
                IsEquivalentTo(internal::DataItem(schema::kInt32)));
    EXPECT_THAT(GetNarrowedSchema(test::EmptyDataSlice(3, schema::kInt32)),
                IsEquivalentTo(internal::DataItem(schema::kInt32)));
    EXPECT_THAT(GetNarrowedSchema(test::DataItem(1, schema::kObject)),
                IsEquivalentTo(internal::DataItem(schema::kInt32)));
  }
  {
    // None schema.
    EXPECT_THAT(GetNarrowedSchema(test::EmptyDataSlice(3, schema::kNone)),
                IsEquivalentTo(internal::DataItem(schema::kNone)));
    EXPECT_THAT(GetNarrowedSchema(test::EmptyDataSlice(3, schema::kObject)),
                IsEquivalentTo(internal::DataItem(schema::kNone)));
  }
  {
    // Object ids.
    EXPECT_THAT(GetNarrowedSchema(test::AllocateDataSlice(3, schema::kObject)),
                IsEquivalentTo(internal::DataItem(schema::kObject)));
    EXPECT_THAT(GetNarrowedSchema(test::AllocateDataSlice(3, schema::kAny)),
                IsEquivalentTo(internal::DataItem(schema::kAny)));
    EXPECT_THAT(GetNarrowedSchema(test::AllocateDataSlice(3, schema::kItemId)),
                IsEquivalentTo(internal::DataItem(schema::kItemId)));
    internal::DataItem entity_schema(internal::AllocateExplicitSchema());
    EXPECT_THAT(GetNarrowedSchema(test::AllocateDataSlice(3, entity_schema)),
                IsEquivalentTo(entity_schema));
    EXPECT_THAT(GetNarrowedSchema(test::EmptyDataSlice(3, entity_schema)),
                IsEquivalentTo(entity_schema));
  }
  {
    // Mixed.
    EXPECT_THAT(GetNarrowedSchema(test::MixedDataSlice<int, float>(
                    {1, std::nullopt}, {std::nullopt, 2.0f}, schema::kObject)),
                IsEquivalentTo(internal::DataItem(schema::kFloat32)));
    EXPECT_THAT(GetNarrowedSchema(test::MixedDataSlice<int, float>(
                    {1, std::nullopt}, {std::nullopt, 2.0f}, schema::kAny)),
                IsEquivalentTo(internal::DataItem(schema::kFloat32)));
    EXPECT_THAT(GetNarrowedSchema(test::MixedDataSlice<int, arolla::Text>(
                    {1, std::nullopt}, {std::nullopt, "foo"}, schema::kAny)),
                IsEquivalentTo(internal::DataItem(schema::kObject)));
    // No common schema, fallback to original schema.
    EXPECT_THAT(
        GetNarrowedSchema(test::MixedDataSlice<int, schema::DType>(
            {1, std::nullopt}, {std::nullopt, schema::kInt32}, schema::kAny)),
        IsEquivalentTo(internal::DataItem(schema::kAny)));
  }
}

TEST(SchemaUtilsTest, DescribeSliceSchema) {
  // Primitives.

  EXPECT_EQ(schema_utils_internal::DescribeSliceSchema(test::DataItem(57)),
            "INT32");
  EXPECT_EQ(schema_utils_internal::DescribeSliceSchema(
                test::DataItem(schema::kInt32)),
            "SCHEMA");
  EXPECT_EQ(schema_utils_internal::DescribeSliceSchema(
                test::DataSlice<arolla::Text>({"a", "b", std::nullopt})),
            "STRING");
  EXPECT_EQ(schema_utils_internal::DescribeSliceSchema(
                test::DataItem(57, schema::kObject)),
            "OBJECT containing INT32 values");
  EXPECT_EQ(schema_utils_internal::DescribeSliceSchema(
                test::DataItem(std::nullopt, schema::kAny)),
            "ANY containing NONE values");
  EXPECT_EQ(
      schema_utils_internal::DescribeSliceSchema(
          test::DataSlice<internal::ObjectId>({std::nullopt}, schema::kAny)),
      "ANY containing NONE values");
  EXPECT_EQ(
      schema_utils_internal::DescribeSliceSchema(test::DataSlice<arolla::Text>(
          {"a", "b", std::nullopt}, schema::kObject)),
      "OBJECT containing STRING values");

  // Entities and objects.

  auto entity_schema = internal::AllocateExplicitSchema();
  auto db = DataBag::Empty();
  auto entity = test::AllocateDataSlice(3, entity_schema, db);
  ASSERT_OK(db->GetMutableImpl()->get().SetSchemaAttr(
      internal::DataItem(entity_schema), "x",
      internal::DataItem(schema::kInt32)));
  EXPECT_EQ(schema_utils_internal::DescribeSliceSchema(entity),
            "SCHEMA(x=INT32)");
  // Without a DataBag we can only print an object id.
  EXPECT_THAT(
      schema_utils_internal::DescribeSliceSchema(entity.WithBag(nullptr)),
      MatchesRegex(R"(\$\w+)"));

  ASSERT_OK_AND_ASSIGN(auto object, ToObject(entity));
  EXPECT_EQ(schema_utils_internal::DescribeSliceSchema(object),
            "OBJECT containing non-primitive values");
  EXPECT_EQ(schema_utils_internal::DescribeSliceSchema(object.WithBag(nullptr)),
            "OBJECT containing non-primitive values");

  // Mixed slices.
  EXPECT_EQ(schema_utils_internal::DescribeSliceSchema(
                test::MixedDataSlice<arolla::Text, std::string>(
                    {"foo", std::nullopt, std::nullopt},
                    {std::nullopt, "bar", std::nullopt})),
            "OBJECT containing BYTES and STRING values");
  EXPECT_EQ(schema_utils_internal::DescribeSliceSchema(
                test::MixedDataSlice<arolla::Text, std::string, int>(
                    {"foo", std::nullopt, std::nullopt},
                    {std::nullopt, "bar", std::nullopt},
                    {std::nullopt, std::nullopt, 1})),
            "OBJECT containing BYTES, INT32 and STRING values");
  EXPECT_EQ(
      schema_utils_internal::DescribeSliceSchema(
          test::MixedDataSlice<arolla::Text, internal::ObjectId>(
              {"foo", std::nullopt, std::nullopt},
              {std::nullopt, internal::AllocateSingleObject(), std::nullopt})),
      "OBJECT containing STRING and non-primitive values");
}

TEST(SchemaUtilsTest, ExpectNumeric) {
  auto empty_and_unknown = test::DataItem(std::nullopt, schema::kObject);
  auto str = test::DataSlice<arolla::Text>({"a", "b", std::nullopt});
  auto str_obj =
      test::DataSlice<arolla::Text>({"a", "b", std::nullopt}, schema::kObject);
  auto object = test::DataItem(internal::AllocateSingleObject());
  auto entity = test::AllocateDataSlice(3, internal::AllocateExplicitSchema(),
                                        DataBag::Empty());

  EXPECT_THAT(ExpectNumeric("foo", empty_and_unknown), IsOk());
  EXPECT_THAT(ExpectNumeric("foo", test::DataSlice<int>({1, 2, std::nullopt})),
              IsOk());
  EXPECT_THAT(
      ExpectNumeric("foo", test::DataSlice<float>({1., 2., std::nullopt},
                                                  schema::kObject)),
      IsOk());
  EXPECT_THAT(ExpectNumeric("foo", test::DataSlice<int>({1, 2, std::nullopt},
                                                        schema::kAny)),
              IsOk());
  EXPECT_THAT(ExpectNumeric("foo", test::MixedDataSlice<int, float>(
                                       {1, std::nullopt, std::nullopt},
                                       {std::nullopt, 2.0f, std::nullopt})),
              IsOk());
  EXPECT_THAT(
      ExpectNumeric("foo", test::DataSlice<bool>({true, false, std::nullopt})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "argument `foo` must be a slice of numeric values, got a slice "
               "of BOOLEAN"));
  EXPECT_THAT(ExpectNumeric("foo", str),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of numeric values, got "
                       "a slice of STRING"));
  EXPECT_THAT(ExpectNumeric("foo", str_obj),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of numeric values, got "
                       "a slice of OBJECT containing STRING values"));
  EXPECT_THAT(ExpectNumeric("foo", object),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of numeric values, got "
                       "a slice of OBJECT containing non-primitive values"));
  EXPECT_THAT(ExpectNumeric("foo", entity),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of numeric values, got "
                       "a slice of SCHEMA()"));
  EXPECT_THAT(
      ExpectNumeric("foo", test::MixedDataSlice<arolla::Text, std::string>(
                               {"foo", std::nullopt, std::nullopt},
                               {std::nullopt, "bar", std::nullopt})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "argument `foo` must be a slice of numeric values, got a slice "
               "of OBJECT containing BYTES and STRING values"));
}

TEST(SchemaUtilsTest, ExpectInteger) {
  auto empty_and_unknown = test::DataItem(std::nullopt, schema::kObject);
  auto str = test::DataSlice<arolla::Text>({"a", "b", std::nullopt});
  auto str_obj =
      test::DataSlice<arolla::Text>({"a", "b", std::nullopt}, schema::kObject);
  auto object = test::DataItem(internal::AllocateSingleObject());
  auto entity = test::AllocateDataSlice(3, internal::AllocateExplicitSchema(),
                                        DataBag::Empty());

  EXPECT_THAT(ExpectInteger("foo", empty_and_unknown), IsOk());
  EXPECT_THAT(ExpectInteger("foo", test::DataSlice<int>({1, 2, std::nullopt})),
              IsOk());
  EXPECT_THAT(ExpectInteger("foo", test::DataSlice<int>({1, 2, std::nullopt},
                                                        schema::kObject)),
              IsOk());
  EXPECT_THAT(ExpectInteger("foo", test::DataSlice<int>({1, 2, std::nullopt},
                                                        schema::kAny)),
              IsOk());
  EXPECT_THAT(ExpectInteger("foo", test::MixedDataSlice<int, int64_t>(
                                       {1, std::nullopt, std::nullopt},
                                       {std::nullopt, 2, std::nullopt})),
              IsOk());
  EXPECT_THAT(
      ExpectInteger("foo", test::DataSlice<float>({1, 3, std::nullopt})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "argument `foo` must be a slice of integer values, got a slice "
               "of FLOAT32"));
  EXPECT_THAT(ExpectInteger("foo", str),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of integer values, got "
                       "a slice of STRING"));
  EXPECT_THAT(ExpectInteger("foo", str_obj),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of integer values, got "
                       "a slice of OBJECT containing STRING values"));
  EXPECT_THAT(ExpectInteger("foo", object),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of integer values, got "
                       "a slice of OBJECT containing non-primitive values"));
  EXPECT_THAT(ExpectInteger("foo", entity),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of integer values, got "
                       "a slice of SCHEMA()"));
  EXPECT_THAT(
      ExpectInteger("foo", test::MixedDataSlice<arolla::Text, std::string>(
                               {"foo", std::nullopt, std::nullopt},
                               {std::nullopt, "bar", std::nullopt})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "argument `foo` must be a slice of integer values, got a slice "
               "of OBJECT containing BYTES and STRING values"));
}

TEST(SchemaUtilsTest, ExpectCanBeAdded) {
  EXPECT_THAT(
      ExpectCanBeAdded("foo", test::DataItem(std::nullopt, schema::kObject)),
      IsOk());
  EXPECT_THAT(
      ExpectCanBeAdded("foo", test::DataSlice<int>({1, 2, std::nullopt})),
      IsOk());
  EXPECT_THAT(ExpectCanBeAdded("foo", test::DataSlice<arolla::Text>(
                                          {"a", "b", std::nullopt})),
              IsOk());
  EXPECT_THAT(ExpectCanBeAdded("foo", test::DataSlice<std::string>(
                                          {"a", "b", std::nullopt})),
              IsOk());
  EXPECT_THAT(ExpectCanBeAdded(
                  "foo", test::DataSlice<bool>({true, false, std::nullopt})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of consistent numeric, "
                       "bytes or string values, got a slice of BOOLEAN"));
  EXPECT_THAT(
      ExpectCanBeAdded("foo", test::MixedDataSlice<arolla::Text, std::string>(
                                  {"foo", std::nullopt, std::nullopt},
                                  {std::nullopt, "bar", std::nullopt})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "argument `foo` must be a slice of consistent numeric, bytes or "
               "string values, got a slice of OBJECT containing "
               "BYTES and STRING values"));
}

TEST(SchemaUtilsTest, ExpectCanBeOrdered) {
  EXPECT_THAT(
      ExpectCanBeOrdered("foo", test::DataItem(std::nullopt, schema::kObject)),
      IsOk());
  EXPECT_THAT(
      ExpectCanBeOrdered("foo", test::DataSlice<int>({1, 2, std::nullopt})),
      IsOk());
  EXPECT_THAT(ExpectCanBeOrdered("foo", test::DataSlice<arolla::Text>(
                                            {"a", "b", std::nullopt})),
              IsOk());
  EXPECT_THAT(ExpectCanBeOrdered("foo", test::DataSlice<std::string>(
                                            {"a", "b", std::nullopt})),
              IsOk());
  EXPECT_THAT(ExpectCanBeOrdered(
                  "foo", test::DataSlice<bool>({true, false, std::nullopt})),
              IsOk());
  EXPECT_THAT(ExpectCanBeOrdered("foo", test::DataSlice<arolla::Unit>(
                                            {arolla::kPresent, std::nullopt})),
              IsOk());
  EXPECT_THAT(
      ExpectCanBeOrdered("foo", test::MixedDataSlice<arolla::Text, std::string>(
                                    {"foo", std::nullopt, std::nullopt},
                                    {std::nullopt, "bar", std::nullopt})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "argument `foo` must be a slice of orderable values, got a "
               "slice of OBJECT containing BYTES and STRING values"));
}

TEST(SchemaUtilsTest, ExpectString) {
  EXPECT_THAT(
      ExpectString("foo", test::DataItem(std::nullopt, schema::kObject)),
      IsOk());
  EXPECT_THAT(ExpectString("foo", test::DataSlice<arolla::Text>(
                                      {"a", "b", std::nullopt})),
              IsOk());
  EXPECT_THAT(
      ExpectString("foo", test::DataSlice<arolla::Text>(
                              {"a", "b", std::nullopt}, schema::kObject)),
      IsOk());
  EXPECT_THAT(ExpectString("foo", test::DataSlice<std::string>(
                                      {"a", "b", std::nullopt}, schema::kAny)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of STRING, got a slice "
                       "of ANY containing BYTES values"));
}

TEST(SchemaUtilsTest, ExpectBytes) {
  EXPECT_THAT(ExpectBytes("foo", test::DataItem(std::nullopt, schema::kObject)),
              IsOk());
  EXPECT_THAT(ExpectBytes("foo", test::DataSlice<std::string>(
                                     {"a", "b", std::nullopt})),
              IsOk());
  EXPECT_THAT(
      ExpectBytes("foo", test::DataSlice<std::string>({"a", "b", std::nullopt},
                                                      schema::kObject)),
      IsOk());
  EXPECT_THAT(ExpectBytes("foo", test::DataSlice<arolla::Text>(
                                     {"a", "b", std::nullopt}, schema::kAny)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of BYTES, got a slice "
                       "of ANY containing STRING values"));
}

TEST(SchemaUtilsTest, ExpectSchema) {
  EXPECT_THAT(
      ExpectSchema("foo", test::DataItem(std::nullopt, schema::kObject)),
      IsOk());
  EXPECT_THAT(
      ExpectSchema("foo", test::DataSlice<schema::DType>(
                              {schema::kBool, schema::kInt32, std::nullopt})),
      IsOk());
  EXPECT_THAT(
      ExpectSchema("foo", test::DataSlice<schema::DType>(
                              {schema::kBool, schema::kInt32, std::nullopt},
                              schema::kObject)),
      IsOk());
  auto schema = internal::AllocateExplicitSchema();
  EXPECT_THAT(ExpectSchema("foo", test::MixedDataSlice<schema::DType, ObjectId>(
                                      {schema::kBool, std::nullopt},
                                      {std::nullopt, schema}, schema::kSchema)),
              IsOk());
  EXPECT_THAT(
      ExpectSchema("foo", test::DataSlice<arolla::Text>(
                              {"a", "b", std::nullopt}, schema::kAny)),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "argument `foo` must be a slice of SCHEMA, got a slice "
               "of ANY containing STRING values"));
}

TEST(SchemaUtilsTest, ExpectMask) {
  EXPECT_THAT(ExpectMask("foo", test::DataItem(std::nullopt, schema::kObject)),
              IsOk());
  EXPECT_THAT(ExpectMask("foo", test::DataItem(std::nullopt, schema::kMask)),
              IsOk());
  EXPECT_THAT(ExpectMask("foo", test::DataSlice<arolla::Unit>(
                                    {std::nullopt, arolla::kPresent})),
              IsOk());
  EXPECT_THAT(
      ExpectMask("foo", test::DataSlice<arolla::Unit>(
                            {std::nullopt, arolla::kPresent}, schema::kAny)),
      IsOk());
  EXPECT_THAT(ExpectMask("foo", test::DataSlice<arolla::Text>(
                                    {"a", "b", std::nullopt}, schema::kAny)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of MASK, got a slice "
                       "of ANY containing STRING values"));
}

TEST(SchemaUtilsTest, ExpectPresentScalar) {
  EXPECT_THAT(ExpectPresentScalar("foo", test::DataItem(true, schema::kBool),
                                  schema::kBool),
              IsOk());
  EXPECT_THAT(ExpectPresentScalar(
                  "foo", test::DataItem(arolla::Text("bar"), schema::kString),
                  schema::kString),
              IsOk());
  EXPECT_THAT(ExpectPresentScalar("foo", test::DataItem(true, schema::kObject),
                                  schema::kBool),
              IsOk());
  EXPECT_THAT(ExpectPresentScalar("foo", test::DataItem(true, schema::kAny),
                                  schema::kBool),
              IsOk());
  EXPECT_THAT(ExpectPresentScalar(
                  "foo", test::DataSlice<bool>({true, false, std::nullopt}),
                  schema::kBool),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be an item holding BOOLEAN, "
                       "got a slice of rank 1 > 0"));
  EXPECT_THAT(
      ExpectPresentScalar("foo", test::DataItem(std::nullopt, schema::kObject),
                          schema::kBool),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "argument `foo` must be an item holding BOOLEAN, got an "
               "item of OBJECT containing NONE values"));
  EXPECT_THAT(ExpectPresentScalar("foo", test::DataItem("true", schema::kAny),
                                  schema::kBool),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be an item holding BOOLEAN, "
                       "got an item of ANY containing STRING values"));
}

TEST(SchemaUtilsTest, ExpectConsistentStringOrBytes) {
  auto empty_and_unknown = test::DataItem(std::nullopt, schema::kObject);
  auto integer = test::DataSlice<int>({1, 2, std::nullopt});
  auto bytes = test::DataSlice<std::string>({"a", "b", std::nullopt});
  auto bytes_any =
      test::DataSlice<std::string>({"a", "b", std::nullopt}, schema::kAny);
  auto str = test::DataSlice<arolla::Text>({"a", "b", std::nullopt});
  auto str_obj =
      test::DataSlice<arolla::Text>({"a", "b", std::nullopt}, schema::kObject);
  auto object = test::DataItem(internal::AllocateSingleObject());
  auto entity = test::AllocateDataSlice(3, internal::AllocateExplicitSchema(),
                                        DataBag::Empty());

  EXPECT_THAT(ExpectConsistentStringOrBytes({"foo"}, empty_and_unknown),
              IsOk());
  EXPECT_THAT(ExpectConsistentStringOrBytes({"foo"}, bytes), IsOk());
  EXPECT_THAT(ExpectConsistentStringOrBytes({"foo"}, bytes_any), IsOk());
  EXPECT_THAT(ExpectConsistentStringOrBytes({"foo"}, str), IsOk());
  EXPECT_THAT(ExpectConsistentStringOrBytes({"foo"}, str_obj), IsOk());
  EXPECT_THAT(ExpectConsistentStringOrBytes({"foo", "bar", "baz"}, str,
                                            empty_and_unknown, str_obj),
              IsOk());
  EXPECT_THAT(ExpectConsistentStringOrBytes({"foo", "bar", "baz"}, bytes,
                                            empty_and_unknown, bytes_any),
              IsOk());

  // Unexpected type of one argument.

  EXPECT_THAT(
      ExpectConsistentStringOrBytes({"foo"}, integer),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "argument `foo` must be a slice of either STRING or BYTES, "
               "got a slice of INT32"));
  EXPECT_THAT(
      ExpectConsistentStringOrBytes({"foo"}, object),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "argument `foo` must be a slice of either STRING or BYTES, "
               "got a slice of OBJECT containing non-primitive values"));
  EXPECT_THAT(
      ExpectConsistentStringOrBytes(
          {"foo"}, test::MixedDataSlice<arolla::Text, std::string>(
                       {"foo", std::nullopt, std::nullopt},
                       {std::nullopt, "bar", std::nullopt})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "argument `foo` must be a slice of either STRING or BYTES, got a "
          "slice of OBJECT containing BYTES and STRING values"));
  EXPECT_THAT(
      ExpectConsistentStringOrBytes({"foo"}, entity),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "argument `foo` must be a slice of either STRING or BYTES, "
               "got a slice of SCHEMA()"));

  // Mixing bytes and string arguments.

  EXPECT_THAT(ExpectConsistentStringOrBytes({"foo", "bar", "baz"}, str,
                                            empty_and_unknown, bytes),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "mixing STRING and BYTES arguments is not allowed, but "
                       "`foo` contains STRING and `baz` contains BYTES"));

  // One-argument version.

  EXPECT_THAT(
      ExpectConsistentStringOrBytes(
          "foo", test::MixedDataSlice<arolla::Text, std::string>(
                     {"foo", std::nullopt, std::nullopt},
                     {std::nullopt, "bar", std::nullopt})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "argument `foo` must be a slice of either STRING or BYTES, got a "
          "slice of OBJECT containing BYTES and STRING values"));
}

TEST(SchemaUtilsTest, ExpectHaveCommonSchema) {
  auto empty_and_unknown = test::DataItem(std::nullopt, schema::kObject);
  auto integer = test::DataSlice<int>({1, 2, std::nullopt});
  auto floating = test::DataSlice<float>({1, 2, std::nullopt});
  auto bytes = test::DataSlice<std::string>({"a", "b", std::nullopt});
  auto bytes_any =
      test::DataSlice<std::string>({"a", "b", std::nullopt}, schema::kAny);
  auto schema = test::DataItem(std::nullopt, schema::kSchema);
  auto integer_object = test::DataSlice<int>({1}, schema::kObject);
  auto entity = test::AllocateDataSlice(1, internal::AllocateExplicitSchema(),
                                        DataBag::Empty());

  EXPECT_THAT(ExpectHaveCommonSchema({"foo", "bar"}, bytes, empty_and_unknown),
              IsOk());
  EXPECT_THAT(ExpectHaveCommonSchema({"foo", "bar"}, bytes, bytes_any), IsOk());
  EXPECT_THAT(ExpectHaveCommonSchema({"foo", "bar"}, integer, bytes), IsOk());
  EXPECT_THAT(ExpectHaveCommonSchema({"foo", "bar"}, integer, schema),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "arguments `foo` and `bar` must contain values castable "
                       "to a common type, got INT32 and SCHEMA"));
  EXPECT_THAT(
      ExpectHaveCommonSchema({"foo", "bar"}, entity, integer_object),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "arguments `foo` and `bar` must contain values castable to a "
               "common type, got SCHEMA() and OBJECT containing INT32 values"));
}

TEST(SchemaUtilsTest, ExpectHaveCommonPrimitiveSchema) {
  auto empty_and_unknown = test::DataItem(std::nullopt, schema::kObject);
  auto integer = test::DataSlice<int>({1, 2, std::nullopt});
  auto floating = test::DataSlice<float>({1, 2, std::nullopt});
  auto bytes = test::DataSlice<std::string>({"a", "b", std::nullopt});
  auto bytes_any =
      test::DataSlice<std::string>({"a", "b", std::nullopt}, schema::kAny);

  EXPECT_THAT(ExpectHaveCommonPrimitiveSchema({"foo", "bar"}, empty_and_unknown,
                                              empty_and_unknown),
              IsOk());
  EXPECT_THAT(
      ExpectHaveCommonPrimitiveSchema({"foo", "bar"}, bytes, empty_and_unknown),
      IsOk());
  EXPECT_THAT(ExpectHaveCommonPrimitiveSchema({"foo", "bar"}, bytes, bytes_any),
              IsOk());
  EXPECT_THAT(
      ExpectHaveCommonPrimitiveSchema({"foo", "bar"}, integer, floating),
      IsOk());

  EXPECT_THAT(ExpectHaveCommonPrimitiveSchema({"foo", "bar"}, integer, bytes),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "arguments `foo` and `bar` must contain values castable "
                       "to a common primitive type, got INT32 and BYTES"));
  EXPECT_THAT(
      ExpectHaveCommonPrimitiveSchema({"foo", "bar"}, integer, bytes_any),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "arguments `foo` and `bar` must contain values castable to a common "
          "primitive type, got INT32 and ANY containing BYTES values"));
}

}  // namespace
}  // namespace koladata::schema
