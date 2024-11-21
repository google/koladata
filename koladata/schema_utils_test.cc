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

#include <optional>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
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
using ::testing::ContainsRegex;

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

TEST(Constraints, ExpectNumeric) {
  auto empty_and_unknown = test::DataItem(std::nullopt, schema::kObject);
  EXPECT_THAT(ExpectNumeric("foo", empty_and_unknown), IsOk());
  EXPECT_THAT(ExpectNumeric("foo", test::DataSlice<int>({1, 2, std::nullopt})),
              IsOk());
  EXPECT_THAT(ExpectNumeric("foo", test::DataSlice<int>({1, 2, std::nullopt},
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
               "expected a numeric value, got foo=DataSlice([True, False, "
               "None], schema: BOOLEAN, shape: JaggedShape(3))"));
}

TEST(Constraints, ExpectConsistentStringOrBytes) {
  auto empty_and_unknown = test::DataItem(std::nullopt, schema::kObject);
  auto integer = test::DataSlice<int>({1, 2, std::nullopt});
  auto bytes = test::DataSlice<std::string>({"a", "b", std::nullopt});
  auto bytes_any =
      test::DataSlice<std::string>({"a", "b", std::nullopt}, schema::kAny);
  auto str = test::DataSlice<arolla::Text>({"a", "b", std::nullopt});
  auto str_obj =
      test::DataSlice<arolla::Text>({"a", "b", std::nullopt}, schema::kObject);
  auto object = test::DataItem(internal::AllocateSingleObject());

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
  EXPECT_THAT(
      ExpectConsistentStringOrBytes({"foo"}, integer),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "expected a string or bytes, got foo=DataSlice([1, 2, None], "
               "schema: INT32, shape: JaggedShape(3))"));
  EXPECT_THAT(
      ExpectConsistentStringOrBytes(
          {"foo"}, test::MixedDataSlice<arolla::Text, std::string>(
                       {"foo", std::nullopt, std::nullopt},
                       {std::nullopt, "bar", std::nullopt})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "expected a string or bytes, got foo=DataSlice(['foo', "
               "b'bar', None], schema: OBJECT, shape: JaggedShape(3))"));
  EXPECT_THAT(
      ExpectConsistentStringOrBytes({"foo"}, object),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          ContainsRegex(
              "expected a string or bytes, got foo=DataItem.*schema: OBJECT")));
  EXPECT_THAT(ExpectConsistentStringOrBytes({"foo", "bar", "baz"}, str,
                                            empty_and_unknown, bytes),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "mixing bytes and string arguments is not allowed, got "
                       "foo=DataSlice(['a', 'b', None], schema: STRING, shape: "
                       "JaggedShape(3)) and baz=DataSlice([b'a', b'b', None], "
                       "schema: BYTES, shape: JaggedShape(3))"));
}

}  // namespace
}  // namespace koladata::schema
