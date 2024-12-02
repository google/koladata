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
#include "arolla/util/text.h"

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
            "OBJECT with an item of type INT32");
  EXPECT_EQ(schema_utils_internal::DescribeSliceSchema(
                test::DataItem(std::nullopt, schema::kAny)),
            "ANY with an item of type NONE");
  EXPECT_EQ(
      schema_utils_internal::DescribeSliceSchema(test::DataSlice<arolla::Text>(
          {"a", "b", std::nullopt}, schema::kObject)),
      "OBJECT with items of type STRING");

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
            "OBJECT with items of type ITEMID");
  EXPECT_EQ(schema_utils_internal::DescribeSliceSchema(object.WithBag(nullptr)),
            "OBJECT with items of type ITEMID");

  // Mixed slices.
  EXPECT_EQ(schema_utils_internal::DescribeSliceSchema(
                test::MixedDataSlice<arolla::Text, std::string>(
                    {"foo", std::nullopt, std::nullopt},
                    {std::nullopt, "bar", std::nullopt})),
            "OBJECT with items of types BYTES, STRING");
  EXPECT_EQ(
      schema_utils_internal::DescribeSliceSchema(
          test::MixedDataSlice<arolla::Text, internal::ObjectId>(
              {"foo", std::nullopt, std::nullopt},
              {std::nullopt, internal::AllocateSingleObject(), std::nullopt})),
      "OBJECT with items of types ITEMID, STRING");
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
                       "a slice of OBJECT with items of type STRING"));
  EXPECT_THAT(ExpectNumeric("foo", object),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of numeric values, got "
                       "a slice of OBJECT with an item of type ITEMID"));
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
               "of OBJECT with items of types BYTES, STRING"));
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
                       "a slice of OBJECT with items of type STRING"));
  EXPECT_THAT(ExpectInteger("foo", object),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be a slice of integer values, got "
                       "a slice of OBJECT with an item of type ITEMID"));
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
               "of OBJECT with items of types BYTES, STRING"));
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
                       "of ANY with items of type BYTES"));
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
                       "of ANY with items of type STRING"));
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
               "item of OBJECT with an item of type NONE"));
  EXPECT_THAT(ExpectPresentScalar("foo", test::DataItem("true", schema::kAny),
                                  schema::kBool),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "argument `foo` must be an item holding BOOLEAN, "
                       "got an item of ANY with an item of type STRING"));
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
               "got a slice of OBJECT with an item of type ITEMID"));
  EXPECT_THAT(
      ExpectConsistentStringOrBytes(
          {"foo"}, test::MixedDataSlice<arolla::Text, std::string>(
                       {"foo", std::nullopt, std::nullopt},
                       {std::nullopt, "bar", std::nullopt})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "argument `foo` must be a slice of either STRING or BYTES, got a "
          "slice of OBJECT with items of types BYTES, STRING"));
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
          "slice of OBJECT with items of types BYTES, STRING"));
}

}  // namespace
}  // namespace koladata::schema
