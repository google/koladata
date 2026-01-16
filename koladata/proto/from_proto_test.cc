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
#include "koladata/proto/from_proto.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/types/span.h"
#include "arolla/jagged_shape/testing/matchers.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/object_factories.h"
#include "koladata/operators/masking.h"
#include "koladata/proto/testing/test_proto2.pb.h"
#include "koladata/proto/testing/test_proto3.pb.h"
#include "koladata/test_utils.h"
#include "koladata/testing/matchers.h"
#include "google/protobuf/text_format.h"

namespace koladata {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::koladata::testing::IsEquivalentTo;
using ::google::protobuf::TextFormat;
using ::testing::UnorderedElementsAre;
using ::testing::UnorderedElementsAreArray;

TEST(FromProtoTest, NullptrMessages) {
  auto db = DataBag::EmptyMutable();
  EXPECT_THAT(FromProto(db, {nullptr}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected all messages be non-null"));
  testing::ExampleMessage message;
  EXPECT_THAT(FromProto(db, {&message, nullptr}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected all messages be non-null"));
}

TEST(FromProtoTest, ZeroMessages) {
  {
    auto db = DataBag::EmptyMutable();
    ASSERT_OK_AND_ASSIGN(auto result, FromProto(db, {}));
    EXPECT_EQ(result.GetShape().rank(), 1);
    EXPECT_EQ(result.size(), 0);
    EXPECT_EQ(result.GetSchemaImpl(), schema::kObject);
    EXPECT_EQ(result.GetBag(), db);
  }
  {
    auto db = DataBag::EmptyMutable();
    auto schema = test::Schema(schema::kObject);
    ASSERT_OK_AND_ASSIGN(auto result,
                         FromProto(db, {}, {}, std::nullopt, schema));
    EXPECT_EQ(result.GetShape().rank(), 1);
    EXPECT_EQ(result.size(), 0);
    EXPECT_EQ(result.GetSchemaImpl(), schema::kObject);
    EXPECT_EQ(result.GetBag(), db);
  }
  {
    auto db = DataBag::EmptyMutable();
    ASSERT_OK_AND_ASSIGN(auto schema,
                         CreateSchema(DataBag::EmptyMutable(), {"some_field"},
                                      {test::Schema(schema::kInt32)}));
    ASSERT_OK_AND_ASSIGN(auto result,
                         FromProto(db, {}, {}, std::nullopt, schema));
    EXPECT_EQ(result.GetShape().rank(), 1);
    EXPECT_EQ(result.size(), 0);
    EXPECT_THAT(result.GetSchema(), IsEquivalentTo(schema.WithBag(db)));
    EXPECT_EQ(result.GetBag(), db);
  }
  {
    auto schema = test::EmptyDataSlice(2, schema::kObject);
    EXPECT_THAT(
        FromProto(DataBag::EmptyMutable(), {}, {}, std::nullopt, schema),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "schema's schema must be SCHEMA, got: OBJECT"));
  }
}

TEST(FromProtoTest, MismatchedTypes) {
  testing::ExampleMessage message1;
  testing::ExampleMessage2 message2;

  EXPECT_THAT(FromProto(DataBag::EmptyMutable(), {&message1, &message2}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected all messages to have the same type, got "
                       "koladata.testing.ExampleMessage and "
                       "koladata.testing.ExampleMessage2"));
}

TEST(FromProtoTest, EmptyMessage_Proto3_NoProvidedSchema) {
  testing::ExampleMessage3 message;

  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto result, FromProto(db, {&message}));
  EXPECT_EQ(result.GetShape().rank(), 1);
  EXPECT_EQ(result.size(), 1);

  EXPECT_THAT(result.GetAttrNames(),
              IsOkAndHolds(UnorderedElementsAreArray({
                  // Scalar primitive fields don't have presence, so they are
                  // always converted even if "unset".
                  "int32_field",
                  "int64_field",
                  "uint32_field",
                  "uint64_field",
                  "double_field",
                  "float_field",
                  "bool_field",
                  "enum_field",
                  "string_field",
                  "bytes_field",
              })));

  // Scalar values should have default values but have the correct schema.
  EXPECT_THAT(result.GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>({0}, db))));
  EXPECT_THAT(
      result.GetAttr("string_field"),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Text>({""}, db))));

  // Verify schema.
  EXPECT_TRUE(result.GetSchemaImpl().is_struct_schema());
  EXPECT_THAT(result.GetSchema().GetAttrNames(),
              IsOkAndHolds(UnorderedElementsAreArray({
                  "int32_field",
                  "int64_field",
                  "uint32_field",
                  "uint64_field",
                  "double_field",
                  "float_field",
                  "bool_field",
                  "enum_field",
                  "string_field",
                  "bytes_field",
              })));
}

TEST(FromProtoTest, EmptyMessage_NoProvidedSchema) {
  testing::ExampleMessage message;

  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto result, FromProto(db, {&message}));
  EXPECT_EQ(result.GetShape().rank(), 1);
  EXPECT_EQ(result.size(), 1);

  EXPECT_THAT(result.GetAttrNames(), IsOkAndHolds(UnorderedElementsAre()));
  EXPECT_TRUE(result.GetSchemaImpl().is_struct_schema());
  EXPECT_THAT(result.GetSchema().GetAttrNames(),
              IsOkAndHolds(UnorderedElementsAre()));
}

TEST(FromProtoTest, EmptyMessage_NoProvidedSchema_NoBagOverload) {
  testing::ExampleMessage message;

  ASSERT_OK_AND_ASSIGN(auto result, FromProto({&message}));
  EXPECT_EQ(result.GetShape().rank(), 1);
  EXPECT_EQ(result.size(), 1);
  EXPECT_FALSE(result.GetBag()->IsMutable());

  EXPECT_THAT(result.GetAttrNames(), IsOkAndHolds(UnorderedElementsAre()));
  EXPECT_TRUE(result.GetSchemaImpl().is_struct_schema());
  EXPECT_THAT(result.GetSchema().GetAttrNames(),
              IsOkAndHolds(UnorderedElementsAre()));
}

TEST(FromProtoTest, EmptyMessage_NoProvidedSchema_NoBagItemOverload) {
  testing::ExampleMessage message;

  ASSERT_OK_AND_ASSIGN(auto result, FromProto(message));
  EXPECT_EQ(result.GetShape().rank(), 0);
  EXPECT_EQ(result.size(), 1);
  EXPECT_FALSE(result.GetBag()->IsMutable());

  EXPECT_THAT(result.GetAttrNames(), IsOkAndHolds(UnorderedElementsAre()));
  EXPECT_TRUE(result.GetSchemaImpl().is_struct_schema());
  EXPECT_THAT(result.GetSchema().GetAttrNames(),
              IsOkAndHolds(UnorderedElementsAre()));
}

TEST(FromProtoTest, EmptyMessage_ObjectSchema) {
  testing::ExampleMessage message;

  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto result, FromProto(db, {&message}, {}, std::nullopt,
                                              test::Schema(schema::kObject)));
  EXPECT_EQ(result.GetShape().rank(), 1);
  EXPECT_EQ(result.size(), 1);

  EXPECT_THAT(result.GetAttrNames(), IsOkAndHolds(UnorderedElementsAre()));

  // Verify schema.
  EXPECT_EQ(result.GetSchemaImpl(), schema::kObject);
  EXPECT_THAT(result.GetObjSchema()->GetAttrNames(),
              IsOkAndHolds(UnorderedElementsAre()));
}

TEST(FromProtoTest, EmptyMessage_ExplicitSchema) {
  testing::ExampleMessage message;

  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(
      auto schema,
      CreateEntitySchema(
          db,
          {
              "int32_field",
              "int64_field",
              "uint32_field",
              "uint64_field",
              "double_field",
              "float_field",
              "bool_field",
              "enum_field",
              "string_field",
              "bytes_field",
              "message_field",
              "repeated_int32_field",
              "repeated_int64_field",
              "repeated_uint32_field",
              "repeated_uint64_field",
              "repeated_double_field",
              "repeated_float_field",
              "repeated_bool_field",
              "repeated_enum_field",
              "repeated_string_field",
              "repeated_bytes_field",
              "repeated_message_field",
              "map_int32_int32_field",
              "map_string_string_field",
              "map_int32_message_field",
          },
          {
              test::Schema(schema::kInt32),   test::Schema(schema::kInt64),
              test::Schema(schema::kInt64),   test::Schema(schema::kInt64),
              test::Schema(schema::kFloat64), test::Schema(schema::kFloat32),
              test::Schema(schema::kBool),    test::Schema(schema::kInt32),
              test::Schema(schema::kString),  test::Schema(schema::kBytes),
              test::Schema(schema::kObject),  test::Schema(schema::kObject),
              test::Schema(schema::kObject),  test::Schema(schema::kObject),
              test::Schema(schema::kObject),  test::Schema(schema::kObject),
              test::Schema(schema::kObject),  test::Schema(schema::kObject),
              test::Schema(schema::kObject),  test::Schema(schema::kObject),
              test::Schema(schema::kObject),  test::Schema(schema::kObject),
              test::Schema(schema::kObject),  test::Schema(schema::kObject),
              test::Schema(schema::kObject),
          }));

  ASSERT_OK_AND_ASSIGN(auto result,
                       FromProto(db, {&message}, {}, std::nullopt, schema));
  EXPECT_EQ(result.GetShape().rank(), 1);
  EXPECT_EQ(result.size(), 1);

  EXPECT_THAT(result.GetAttrNames(), IsOkAndHolds(UnorderedElementsAreArray({
                                         "int32_field",
                                         "int64_field",
                                         "uint32_field",
                                         "uint64_field",
                                         "double_field",
                                         "float_field",
                                         "bool_field",
                                         "enum_field",
                                         "string_field",
                                         "bytes_field",
                                         "message_field",
                                         "repeated_int32_field",
                                         "repeated_int64_field",
                                         "repeated_uint32_field",
                                         "repeated_uint64_field",
                                         "repeated_double_field",
                                         "repeated_float_field",
                                         "repeated_bool_field",
                                         "repeated_enum_field",
                                         "repeated_string_field",
                                         "repeated_bytes_field",
                                         "repeated_message_field",
                                         "map_int32_int32_field",
                                         "map_string_string_field",
                                         "map_int32_message_field",
                                     })));

  // Scalar values should be missing but have the correct schema.
  EXPECT_THAT(result.GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(
                  test::DataSlice<int32_t>({std::nullopt}, db))));
  EXPECT_THAT(result.GetAttr("string_field"),
              IsOkAndHolds(IsEquivalentTo(
                  test::DataSlice<arolla::Text>({std::nullopt}, db))));

  // Repeated values should be empty lists with the correct schema.
  ASSERT_OK_AND_ASSIGN(auto repeated_int32_field_values,
                       result.GetAttr("repeated_int32_field"));
  EXPECT_THAT(repeated_int32_field_values.GetSchema(),
              IsEquivalentTo(*schema.GetAttr("repeated_int32_field")));
  EXPECT_THAT(repeated_int32_field_values.ExplodeList(0, std::nullopt),
              IsOkAndHolds(IsEquivalentTo(test::EmptyDataSlice(
                  *DataSlice::JaggedShape::FromEdges({
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 0),
                  }),
                  internal::DataItem(schema::kNone), db))));

  // Verify schema.
  EXPECT_THAT(result.GetSchema(), IsEquivalentTo(schema));
}

koladata::testing::ExampleMessage GetExampleMessageAllFieldsNoExtensions() {
  koladata::testing::ExampleMessage message;
  CHECK(TextFormat::ParseFromString(
      R"pb(
        int32_field: 10
        int64_field: 20
        uint32_field: 30
        uint64_field: 40
        double_field: 50.0
        float_field: 60.0
        bool_field: true
        enum_field: EXAMPLE_ENUM_FOO
        string_field: "bar"
        bytes_field: "baz"
        message_field { int32_field: 70 }
        repeated_int32_field: [ 1, 2, 3 ]
        repeated_int64_field: [ 4, 5, 6 ]
        repeated_uint32_field: [ 7, 8, 9 ]
        repeated_uint64_field: [ 10, 11, 12 ]
        repeated_double_field: [ 13, 14, 15 ]
        repeated_float_field: [ 16, 17, 18 ]
        repeated_bool_field: [ false, true, false ]
        repeated_enum_field: [ EXAMPLE_ENUM_FOO, EXAMPLE_ENUM_BAR ]
        repeated_string_field: [ "a", "b", "c" ]
        repeated_bytes_field: [ "d", "e", "f" ]
        repeated_message_field: { string_field: "g" }
        repeated_message_field: { string_field: "h" }
        repeated_message_field: {}
        map_int32_int32_field: { key: 1 value: 2 }
        map_int32_int32_field: { key: 3 value: 4 }
        map_string_string_field: { key: "x" value: "y" }
        map_string_string_field: { key: "z" }
        map_string_string_field: { value: "w" }
        map_int32_message_field: {
          key: 5
          value: { int32_field: 6 }
        }
      )pb",
      &message));
  return message;
}

TEST(FromProtoTest, AllFieldTypes_NoProvidedSchema) {
  auto message = GetExampleMessageAllFieldsNoExtensions();
  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto result, FromProto(db, {&message}));

  // Scalar primitive fields.
  EXPECT_THAT(result.GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>({10}, db))));
  EXPECT_THAT(result.GetAttr("int64_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int64_t>({20}, db))));
  EXPECT_THAT(result.GetAttr("uint32_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int64_t>({30}, db))));
  EXPECT_THAT(result.GetAttr("uint64_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int64_t>({40}, db))));
  EXPECT_THAT(
      result.GetAttr("double_field"),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<double>({50.0}, db))));
  EXPECT_THAT(result.GetAttr("float_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<float>({60.0}, db))));
  EXPECT_THAT(result.GetAttr("bool_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<bool>({true}, db))));
  EXPECT_THAT(result.GetAttr("enum_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {koladata::testing::EXAMPLE_ENUM_FOO}, db))));
  EXPECT_THAT(
      result.GetAttr("string_field"),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Text>({"bar"}, db))));
  EXPECT_THAT(result.GetAttr("bytes_field"),
              IsOkAndHolds(
                  IsEquivalentTo(test::DataSlice<arolla::Bytes>({"baz"}, db))));

  // Scalar message field.
  EXPECT_THAT(result.GetAttr("message_field")->GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>({70}, db))));

  // Scalar repeated fields.
  EXPECT_THAT(
      result.GetAttr("repeated_int32_field")->ExplodeList(0, std::nullopt),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
          {1, 2, 3},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
          }),
          schema::kInt32, db))));
  EXPECT_THAT(
      result.GetAttr("repeated_int64_field")->ExplodeList(0, std::nullopt),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<int64_t>(
          {4, 5, 6},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
          }),
          schema::kInt64, db))));
  EXPECT_THAT(
      result.GetAttr("repeated_uint32_field")->ExplodeList(0, std::nullopt),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<int64_t>(
          {7, 8, 9},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
          }),
          schema::kInt64, db))));
  EXPECT_THAT(
      result.GetAttr("repeated_uint64_field")->ExplodeList(0, std::nullopt),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<int64_t>(
          {10, 11, 12},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
          }),
          schema::kInt64, db))));
  EXPECT_THAT(
      result.GetAttr("repeated_double_field")->ExplodeList(0, std::nullopt),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<double>(
          {13, 14, 15},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
          }),
          schema::kFloat64, db))));
  EXPECT_THAT(
      result.GetAttr("repeated_float_field")->ExplodeList(0, std::nullopt),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<float>(
          {16, 17, 18},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
          }),
          schema::kFloat32, db))));
  EXPECT_THAT(
      result.GetAttr("repeated_bool_field")->ExplodeList(0, std::nullopt),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<bool>(
          {false, true, false},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
          }),
          schema::kBool, db))));
  EXPECT_THAT(
      result.GetAttr("repeated_enum_field")->ExplodeList(0, std::nullopt),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
          {
              koladata::testing::EXAMPLE_ENUM_FOO,
              koladata::testing::EXAMPLE_ENUM_BAR,
          },
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 2),
          }),
          schema::kInt32, db))));
  EXPECT_THAT(
      result.GetAttr("repeated_string_field")->ExplodeList(0, std::nullopt),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Text>(
          {"a", "b", "c"},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
          }),
          schema::kString, db))));
  EXPECT_THAT(
      result.GetAttr("repeated_bytes_field")->ExplodeList(0, std::nullopt),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Bytes>(
          {"d", "e", "f"},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
          }),
          schema::kBytes, db))));

  // Repeated message field.
  EXPECT_THAT(result.GetAttr("repeated_message_field")
                  ->ExplodeList(0, std::nullopt)
                  ->GetAttr("string_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Text>(
                  {"g", "h", std::nullopt},
                  *DataSlice::JaggedShape::FromEdges({
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
                  }),
                  schema::kString, db))));

  // Map scalar fields.
  EXPECT_THAT(
      result.GetAttr("map_int32_int32_field")
          ->GetFromDict(test::DataSlice<int32_t>(
              {1, 2, 3},
              *DataSlice::JaggedShape::FromEdges({
                  *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                  *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
              }),
              schema::kInt32, db)),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
          {2, std::nullopt, 4},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
          }),
          schema::kInt32, db))));
  EXPECT_THAT(
      result.GetAttr("map_string_string_field")
          ->GetFromDict(test::DataSlice<arolla::Text>(
              {"", "x", "y", "z"},
              *DataSlice::JaggedShape::FromEdges({
                  *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                  *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 4),
              }),
              schema::kString, db)),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Text>(
          {"w", "y", std::nullopt, ""},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 4),
          }),
          schema::kString, db))));

  // Map message fields.
  EXPECT_THAT(
      result.GetAttr("map_int32_message_field")
          ->GetFromDict(test::DataSlice<int32_t>(
              {0, 5, 6},
              *DataSlice::JaggedShape::FromEdges({
                  *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                  *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
              }),
              schema::kInt32, db))
          ->GetAttr("int32_field"),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
          {std::nullopt, 6, std::nullopt},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
          }),
          schema::kInt32, db))));
}

TEST(FromProtoTest, ObjectSchema) {
  auto message = GetExampleMessageAllFieldsNoExtensions();
  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto result, FromProto(db, {&message}, {}, std::nullopt,
                                              test::Schema(schema::kObject)));
  EXPECT_THAT(result.GetSchema(),
              IsEquivalentTo(test::Schema(schema::kObject, db)));

  EXPECT_THAT(result.GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>({10}, db))));
  EXPECT_THAT(
      result.GetAttr("string_field"),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Text>({"bar"}, db))));
  EXPECT_THAT(result.GetAttr("message_field")->GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>({70}, db))));

  ASSERT_OK_AND_ASSIGN(auto repeated_int32_field,
                       result.GetAttr("repeated_int32_field"));
  EXPECT_EQ(repeated_int32_field.GetSchemaImpl(), schema::kObject);
  EXPECT_THAT(repeated_int32_field.GetObjSchema()->GetAttrNames(),
              IsOkAndHolds(UnorderedElementsAreArray({"__items__"})));
  EXPECT_THAT(repeated_int32_field.ExplodeList(0, std::nullopt),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {1, 2, 3},
                  *DataSlice::JaggedShape::FromEdges({
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
                  }),
                  schema::kInt32, db))));

  ASSERT_OK_AND_ASSIGN(auto map_int32_int32_field,
                       result.GetAttr("map_int32_int32_field"));
  EXPECT_EQ(map_int32_int32_field.GetSchemaImpl(), schema::kObject);
  EXPECT_THAT(
      map_int32_int32_field.GetObjSchema()->GetAttrNames(),
      IsOkAndHolds(UnorderedElementsAreArray({"__keys__", "__values__"})));
  EXPECT_THAT(map_int32_int32_field.GetFromDict(test::DataSlice<int32_t>(
                  {1, 2, 3},
                  *DataSlice::JaggedShape::FromEdges({
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
                  }),
                  schema::kInt32, db)),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {2, std::nullopt, 4},
                  *DataSlice::JaggedShape::FromEdges({
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
                  }),
                  schema::kInt32, db))));
}

TEST(FromProtoTest, ExplicitSchema) {
  testing::ExampleMessage message;
  CHECK(TextFormat::ParseFromString(R"pb(
                                      int32_field: 1
                                      int64_field: 2
                                      repeated_int32_field: [ 3, 4, 5 ]
                                      map_int32_int32_field: { key: 6 value: 7 }
                                    )pb",
                                    &message));

  auto db = DataBag::EmptyMutable();
  auto schema = test::EntitySchema(
      {
          "int32_field",
          "message_field",
          "repeated_int32_field",
          "map_int32_int32_field",
          "bytes_field",
      },
      {
          test::Schema(schema::kInt64),
          test::EntitySchema({}, {}, db),
          *CreateListSchema(db, test::Schema(schema::kInt64)),
          *CreateDictSchema(db, test::Schema(schema::kInt64),
                            test::Schema(schema::kInt64)),
          test::Schema(schema::kBytes),
      },
      db);

  ASSERT_OK_AND_ASSIGN(auto result,
                       FromProto(db, {&message}, {}, std::nullopt, schema));

  EXPECT_THAT(result.GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int64_t>({1}, db))));
  EXPECT_THAT(result.GetAttr("int64_field"),
              StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(result.GetAttr("bytes_field"),
              IsOkAndHolds(IsEquivalentTo(
                  test::DataSlice<arolla::Bytes>({std::nullopt}, db))));
  EXPECT_THAT(
      result.GetAttr("repeated_int32_field")->ExplodeList(0, std::nullopt),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<int64_t>(
          {3, 4, 5},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
          }),
          schema::kInt64, db))));
  EXPECT_THAT(result.GetAttr("map_int32_int32_field")->GetDictKeys(),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int64_t>(
                  {6},
                  *DataSlice::JaggedShape::FromEdges({
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                  }),
                  schema::kInt64, db))));
  EXPECT_THAT(result.GetAttr("map_int32_int32_field")->GetDictValues(),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int64_t>(
                  {7},
                  *DataSlice::JaggedShape::FromEdges({
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                  }),
                  schema::kInt64, db))));
}

TEST(FromProtoTest, KodaOptions) {
  testing::ExampleMessageWithKodaOptions message1;
  CHECK(TextFormat::ParseFromString(R"pb(
                                      bool_field: true
                                      repeated_bool_field: [ false, true ]
                                    )pb",
                                    &message1));
  testing::ExampleMessageWithKodaOptions message2;
  CHECK(TextFormat::ParseFromString(R"pb(
                                      bool_field: false
                                      repeated_bool_field: [ false, true ]
                                    )pb",
                                    &message2));
  testing::ExampleMessageWithKodaOptions message3;
  CHECK(TextFormat::ParseFromString(R"pb(
                                      repeated_bool_field: [ false, true ]
                                    )pb",
                                    &message3));

  {
    auto db = DataBag::EmptyMutable();
    ASSERT_OK_AND_ASSIGN(auto result,
                         FromProto(db, {&message1, &message2, &message3}));

    EXPECT_THAT(result.GetAttr("bool_field"),
                IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Unit>(
                    {arolla::kUnit, std::nullopt, std::nullopt}, db))));
    EXPECT_THAT(result.GetAttr("repeated_bool_field")
                    ->ExplodeList(0, std::nullopt)
                    ->Flatten(),
                IsEquivalentTo(test::DataSlice<arolla::Unit>(
                    {
                        arolla::OptionalValue<arolla::Unit>(),
                        arolla::OptionalValue<arolla::Unit>(arolla::kUnit),
                        arolla::OptionalValue<arolla::Unit>(),
                        arolla::OptionalValue<arolla::Unit>(arolla::kUnit),
                        arolla::OptionalValue<arolla::Unit>(),
                        arolla::OptionalValue<arolla::Unit>(arolla::kUnit),
                    },
                    db)));
  }
  {
    // Special case where bool_field is populated, but has no present values in
    // the converted output.
    auto db = DataBag::EmptyMutable();
    ASSERT_OK_AND_ASSIGN(auto result, FromProto(db, {&message2}));
    EXPECT_THAT(result.GetAttr("bool_field"),
                IsOkAndHolds(IsEquivalentTo(
                    test::DataSlice<arolla::Unit>({std::nullopt}, db))));
  }
}

TEST(FromProtoTest, Uint64Overflow) {
  testing::ExampleMessage message;
  // 18446744069414584319 = 2**64 - 2**32 - 1
  // When cast to int64, should be -2**32-1 = -4294967297
  CHECK(TextFormat::ParseFromString(R"pb(
                                      uint64_field: 18446744069414584319
                                    )pb",
                                    &message));

  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto result, FromProto(db, {&message}));

  EXPECT_THAT(result.GetAttr("uint64_field"),
              IsOkAndHolds(IsEquivalentTo(
                  test::DataSlice<int64_t>({-4294967297LL}, db))));
}

TEST(FromProtoTest, MixedMessageStructure) {
  testing::ExampleMessage message1;
  CHECK(TextFormat::ParseFromString(
      R"pb(
        int32_field: 1
        repeated_int32_field: [ 1, 2, 3 ]
        repeated_message_field {
          int32_field: 4
          repeated_int32_field: [ 100, 101 ]
        }
        map_int32_int32_field { key: 10 value: 11 }
        map_int32_int32_field { key: 12 value: 1000 }
        map_int32_message_field { key: 200 }
        map_int32_message_field { value: { int32_field: 2000 } }
      )pb",
      &message1));

  testing::ExampleMessage message2;
  CHECK(TextFormat::ParseFromString(R"pb(
                                      repeated_int32_field: [ 4, 5 ]
                                      message_field {}
                                      repeated_message_field { int32_field: 5 }
                                      repeated_message_field {
                                        int32_field: 6
                                        repeated_int32_field: [ 102 ]
                                      }
                                      repeated_message_field {}
                                      repeated_message_field {
                                        int32_field: 7
                                        repeated_int32_field: [ 103, 104 ]
                                      }
                                    )pb",
                                    &message2));

  testing::ExampleMessage message3;
  CHECK(
      TextFormat::ParseFromString(R"pb(
                                    int32_field: 2
                                    message_field {
                                      int32_field: 3
                                      repeated_int32_field: [ 20, 21, 22, 23 ]
                                    }
                                    map_int32_int32_field { key: 12 value: 13 }
                                    map_int32_int32_field { key: 14 }
                                    map_int32_int32_field { value: 15 }
                                    map_int32_message_field {
                                      key: 200
                                      value: { int32_field: 2001 }
                                    }
                                  )pb",
                                  &message3));

  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto result,
                       FromProto(db, {&message1, &message2, &message3}));

  // int32_field
  EXPECT_THAT(result.GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(
                  test::DataSlice<int32_t>({1, std::nullopt, 2}, db))));

  // repeated_int32_field presence
  EXPECT_THAT(ops::Has(*result.GetAttr("repeated_int32_field")),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Unit>(
                  {arolla::kUnit, arolla::kUnit, std::nullopt}))));

  // repeated_int32_field items
  EXPECT_THAT(
      result.GetAttr("repeated_int32_field")->ExplodeList(0, std::nullopt),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
          {1, 2, 3, 4, 5},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
              test::EdgeFromSplitPoints({0, 3, 5, 5}),
          }),
          schema::kInt32, db))));

  // map_int32_int32_field presence
  EXPECT_THAT(ops::Has(*result.GetAttr("map_int32_int32_field")),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Unit>(
                  {arolla::kUnit, std::nullopt, arolla::kUnit}))));

  // map_int32_int32_field items
  //
  // 0 -> [missing, missing, 15]
  // 10 -> [11, missing, missing]
  // 11 -> [missing, missing, missing]
  // 12 -> [1000, missing, 13]
  // 14 -> [missing, missing, 0]
  EXPECT_THAT(result.GetAttr("map_int32_int32_field")
                  ->GetFromDict(test::DataItem<int32_t>(0)),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {std::nullopt, std::nullopt, 15}, db))));
  EXPECT_THAT(result.GetAttr("map_int32_int32_field")
                  ->GetFromDict(test::DataItem<int32_t>(10)),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {11, std::nullopt, std::nullopt}, db))));
  EXPECT_THAT(result.GetAttr("map_int32_int32_field")
                  ->GetFromDict(test::DataItem<int32_t>(11)),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {std::nullopt, std::nullopt, std::nullopt}, db))));
  EXPECT_THAT(result.GetAttr("map_int32_int32_field")
                  ->GetFromDict(test::DataItem<int32_t>(12)),
              IsOkAndHolds(IsEquivalentTo(
                  test::DataSlice<int32_t>({1000, std::nullopt, 13}, db))));
  EXPECT_THAT(result.GetAttr("map_int32_int32_field")
                  ->GetFromDict(test::DataItem<int32_t>(14)),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {std::nullopt, std::nullopt, 0}, db))));

  // message_field presence
  EXPECT_THAT(ops::Has(*result.GetAttr("message_field")),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Unit>(
                  {std::nullopt, arolla::kUnit, arolla::kUnit}))));

  // message_field.int32_field
  EXPECT_THAT(result.GetAttr("message_field")->GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {std::nullopt, std::nullopt, 3}, db))));

  // message_field.repeated_int32_field presence
  EXPECT_THAT(
      ops::Has(
          *result.GetAttr("message_field")->GetAttr("repeated_int32_field")),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Unit>(
          {std::nullopt, std::nullopt, arolla::kUnit}))));

  // message_field.repeated_int32_field items
  EXPECT_THAT(result.GetAttr("message_field")
                  ->GetAttr("repeated_int32_field")
                  ->ExplodeList(0, std::nullopt),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {20, 21, 22, 23},
                  *DataSlice::JaggedShape::FromEdges({
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
                      // [0, 0, 4]
                      test::EdgeFromSplitPoints({0, 0, 0, 4}),
                  }),
                  schema::kInt32, db))));

  // repeated_message_field presence
  EXPECT_THAT(ops::Has(*result.GetAttr("repeated_message_field")),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Unit>(
                  {arolla::kUnit, arolla::kUnit, std::nullopt}))));

  // repeated_message_field.int32_field
  EXPECT_THAT(result.GetAttr("repeated_message_field")
                  ->ExplodeList(0, std::nullopt)
                  ->GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {4, 5, 6, std::nullopt, 7},
                  *DataSlice::JaggedShape::FromEdges({
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
                      // [1, 4, 0]
                      test::EdgeFromSplitPoints({0, 1, 5, 5}),
                  }),
                  schema::kInt32, db))));

  // repeated_message_field.repeated_int32_field items
  EXPECT_THAT(result.GetAttr("repeated_message_field")
                  ->ExplodeList(0, std::nullopt)
                  ->GetAttr("repeated_int32_field")
                  ->ExplodeList(0, std::nullopt),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {100, 101, 102, 103, 104},
                  *DataSlice::JaggedShape::FromEdges({
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
                      // [1, 4, 0]
                      test::EdgeFromSplitPoints({0, 1, 5, 5}),
                      // [2, 0, 1, 0, 2]
                      test::EdgeFromSplitPoints({0, 2, 2, 3, 3, 5}),
                  }),
                  schema::kInt32, db))));

  // map_int32_message_field presence
  EXPECT_THAT(ops::Has(*result.GetAttr("map_int32_message_field")),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<arolla::Unit>(
                  {arolla::kUnit, std::nullopt, arolla::kUnit}))));

  // map_int32_message_field.int32_field
  //
  // 0 -> [2000, missing, missing]
  // 200 -> [missing, missing, 2001]
  EXPECT_THAT(result.GetAttr("map_int32_message_field")
                  ->GetFromDict(test::DataItem<int32_t>(0))
                  ->GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {2000, std::nullopt, std::nullopt}, db))));
  EXPECT_THAT(result.GetAttr("map_int32_message_field")
                  ->GetFromDict(test::DataItem<int32_t>(200))
                  ->GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {std::nullopt, std::nullopt, 2001}, db))));
}

TEST(FromProtoTest, ItemId) {
  koladata::testing::ExampleMessage message;
  TextFormat::ParseFromString(R"pb(
                                message_field: { int32_field: 1 }
                                repeated_message_field { int32_field: 2 }
                                repeated_message_field { int32_field: 3 }
                                repeated_message_field {
                                  repeated_message_field { int32_field: 4 }
                                  repeated_message_field { int32_field: 5 }
                                }
                                map_int32_message_field {
                                  key: 1
                                  value { int32_field: 6 }
                                }
                                map_int32_message_field {
                                  key: 2
                                  value { int32_field: 7 }
                                }
                              )pb",
                              &message);

  ASSERT_OK_AND_ASSIGN(
      auto itemids,
      DataSlice::Create(internal::DataSliceImpl::AllocateEmptyObjects(1),
                        DataSlice::JaggedShape::FlatFromSize(1),
                        internal::DataItem(schema::kItemId)));

  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto result, FromProto(db, {&message}, {}, itemids));

  // Root itemids match the input itemids.
  EXPECT_THAT(
      result.WithSchema(test::Schema(schema::kItemId))->WithBag(nullptr),
      IsEquivalentTo(itemids));

  // Spot-check values to ensure that itemids haven't collided.
  EXPECT_THAT(result.GetAttr("message_field")->GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>({1}, db))));
  EXPECT_THAT(result.GetAttr("repeated_message_field")
                  ->ExplodeList(0, std::nullopt)
                  ->GetAttr("int32_field"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
                  {2, 3, std::nullopt},
                  *DataSlice::JaggedShape::FromEdges({
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                      *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
                  }),
                  schema::kInt32, db))));
  EXPECT_THAT(
      result.GetAttr("map_int32_message_field")
          ->GetFromDict(test::DataSlice<int32_t>(
              {1, 2, 3},
              *DataSlice::JaggedShape::FromEdges({
                  *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
                  *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
              }),
              schema::kInt32, db))
          ->GetAttr("int32_field"),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<int32_t>(
          {6, 7, std::nullopt},
          *DataSlice::JaggedShape::FromEdges({
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
              *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 3),
          }),
          schema::kInt32, db))));

  // FromProto is deterministic, including sub-messages / lists / dicts.
  auto db2 = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(auto result2, FromProto(db2, {&message}, {}, itemids));
  EXPECT_THAT(result.WithBag(nullptr),
              IsEquivalentTo(result2.WithBag(nullptr)));
  EXPECT_THAT(
      result.GetAttr("message_field")->WithBag(nullptr),
      IsEquivalentTo(result2.GetAttr("message_field")->WithBag(nullptr)));
  EXPECT_THAT(result.GetAttr("repeated_message_field")->WithBag(nullptr),
              IsEquivalentTo(
                  result2.GetAttr("repeated_message_field")->WithBag(nullptr)));
  EXPECT_THAT(result.GetAttr("repeated_message_field")
                  ->ExplodeList(0, std::nullopt)
                  ->WithBag(nullptr),
              IsEquivalentTo(result2.GetAttr("repeated_message_field")
                                 ->ExplodeList(0, std::nullopt)
                                 ->WithBag(nullptr)));
  EXPECT_THAT(result.GetAttr("repeated_message_field")
                  ->ExplodeList(0, std::nullopt)
                  ->GetAttr("repeated_message_field")
                  ->WithBag(nullptr),
              IsEquivalentTo(result2.GetAttr("repeated_message_field")
                                 ->ExplodeList(0, std::nullopt)
                                 ->GetAttr("repeated_message_field")
                                 ->WithBag(nullptr)));
  EXPECT_THAT(result.GetAttr("repeated_message_field")
                  ->ExplodeList(0, std::nullopt)
                  ->GetAttr("repeated_message_field")
                  ->ExplodeList(0, std::nullopt)
                  ->WithBag(nullptr),
              IsEquivalentTo(result2.GetAttr("repeated_message_field")
                                 ->ExplodeList(0, std::nullopt)
                                 ->GetAttr("repeated_message_field")
                                 ->ExplodeList(0, std::nullopt)
                                 ->WithBag(nullptr)));
  EXPECT_THAT(
      result.GetAttr("map_int32_message_field")->WithBag(nullptr),
      IsEquivalentTo(
          result2.GetAttr("map_int32_message_field")->WithBag(nullptr)));
}

TEST(FromProtoTest, Extension) {
  testing::ExampleMessage2 message;
  message.SetExtension(koladata::testing::m2_bool_extension_field, false);
  message.MutableExtension(koladata::testing::m2_message2_extension_field)
      ->SetExtension(koladata::testing::m2_bool_extension_field, true);

  auto db = DataBag::EmptyMutable();
  ASSERT_OK_AND_ASSIGN(
      auto result,
      FromProto(db, {&message},
                {
                    "(koladata.testing.m2_bool_extension_field)",
                    "(koladata.testing.m2_message2_extension_field)",
                    ("(koladata.testing.m2_message2_extension_field)."
                     "(koladata.testing.m2_bool_extension_field)"),
                }));

  EXPECT_THAT(result.GetAttrNames(),
              IsOkAndHolds(UnorderedElementsAreArray({
                  "(koladata.testing.m2_bool_extension_field)",
                  "(koladata.testing.m2_message2_extension_field)",
              })));
  EXPECT_THAT(result.GetAttr("(koladata.testing.m2_bool_extension_field)"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<bool>({false}, db))));

  ASSERT_OK_AND_ASSIGN(
      auto message_ext_field,
      result.GetAttr("(koladata.testing.m2_message2_extension_field)"));
  EXPECT_THAT(message_ext_field.GetAttrNames(),
              IsOkAndHolds(UnorderedElementsAreArray({
                  "(koladata.testing.m2_bool_extension_field)",
                  // Surprising behavior caused by the uu_schema being shared
                  // per-message-descriptor.
                  "(koladata.testing.m2_message2_extension_field)",
              })));
  EXPECT_THAT(
      message_ext_field.GetAttr("(koladata.testing.m2_bool_extension_field)"),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<bool>({true}, db))));
}

TEST(FromProtoTest, ExtensionViaSchema) {
  testing::ExampleMessage2 message;
  message.SetExtension(koladata::testing::m2_bool_extension_field, false);
  message.MutableExtension(koladata::testing::m2_message2_extension_field)
      ->SetExtension(koladata::testing::m2_bool_extension_field, true);

  auto db = DataBag::EmptyMutable();
  auto schema = test::EntitySchema(
      {
          "(koladata.testing.m2_bool_extension_field)",
          "(koladata.testing.m2_message2_extension_field)",
      },
      {
          test::Schema(schema::kBool),
          test::EntitySchema(
              {
                  "(koladata.testing.m2_bool_extension_field)",
              },
              {
                  test::Schema(schema::kBool),
              },
              db),
      },
      db);

  ASSERT_OK_AND_ASSIGN(auto result,
                       FromProto(db, {&message}, {}, std::nullopt, schema));

  EXPECT_THAT(result.GetAttrNames(),
              IsOkAndHolds(UnorderedElementsAreArray({
                  "(koladata.testing.m2_bool_extension_field)",
                  "(koladata.testing.m2_message2_extension_field)",
              })));
  EXPECT_THAT(result.GetAttr("(koladata.testing.m2_bool_extension_field)"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<bool>({false}, db))));

  ASSERT_OK_AND_ASSIGN(
      auto message_ext_field,
      result.GetAttr("(koladata.testing.m2_message2_extension_field)"));
  EXPECT_THAT(message_ext_field.GetAttrNames(),
              IsOkAndHolds(UnorderedElementsAreArray({
                  "(koladata.testing.m2_bool_extension_field)",
              })));
  EXPECT_THAT(
      message_ext_field.GetAttr("(koladata.testing.m2_bool_extension_field)"),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<bool>({true}, db))));
}

TEST(FromProtoTest, ExtensionViaSchemaExtensionsAndExtensionsListUnion) {
  testing::ExampleMessage2 message;
  message.SetExtension(koladata::testing::m2_bool_extension_field, false);
  message.MutableExtension(koladata::testing::m2_message2_extension_field)
      ->SetExtension(koladata::testing::m2_bool_extension_field, true);

  auto db = DataBag::EmptyMutable();
  auto schema = test::EntitySchema(
      {
          "(koladata.testing.m2_message2_extension_field)",
      },
      {
          test::EntitySchema(
              {
                  "(koladata.testing.m2_bool_extension_field)",
              },
              {
                  test::Schema(schema::kBool),
              },
              db),
      },
      db);

  ASSERT_OK_AND_ASSIGN(
      auto result, FromProto(db, {&message},
                             {
                                 "(koladata.testing.m2_bool_extension_field)",
                             },
                             std::nullopt, schema));

  EXPECT_THAT(result.GetAttrNames(),
              IsOkAndHolds(UnorderedElementsAreArray({
                  "(koladata.testing.m2_bool_extension_field)",
                  "(koladata.testing.m2_message2_extension_field)",
              })));
  EXPECT_THAT(result.GetAttr("(koladata.testing.m2_bool_extension_field)"),
              IsOkAndHolds(IsEquivalentTo(test::DataSlice<bool>({false}, db))));

  ASSERT_OK_AND_ASSIGN(
      auto message_ext_field,
      result.GetAttr("(koladata.testing.m2_message2_extension_field)"));
  EXPECT_THAT(message_ext_field.GetAttrNames(),
              IsOkAndHolds(UnorderedElementsAreArray({
                  "(koladata.testing.m2_bool_extension_field)",
              })));
  EXPECT_THAT(
      message_ext_field.GetAttr("(koladata.testing.m2_bool_extension_field)"),
      IsOkAndHolds(IsEquivalentTo(test::DataSlice<bool>({true}, db))));
}

TEST(FromProtoTest, InvalidExtensionPath) {
  testing::ExampleMessage message;

  EXPECT_THAT(
      FromProto(DataBag::EmptyMutable(), {&message}, {""}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "invalid extension path (trailing non-extension field): \"\""));

  EXPECT_THAT(
      FromProto(DataBag::EmptyMutable(), {&message}, {"a.b.c"}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "invalid extension path (trailing non-extension field): \"a.b.c\""));

  EXPECT_THAT(
      FromProto(DataBag::EmptyMutable(), {&message}, {"a.(b.c"}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "invalid extension path (missing closing parenthesis): \"a.(b.c\""));

  EXPECT_THAT(FromProto(DataBag::EmptyMutable(), {&message}, {"a.b).c"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "invalid extension path (unexpected closing "
                       "parenthesis): \"a.b).c\""));

  EXPECT_THAT(FromProto(DataBag::EmptyMutable(), {&message}, {"a.(b.(c"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "invalid extension path (unexpected opening "
                       "parenthesis): \"a.(b.(c\""));

  EXPECT_THAT(FromProto(DataBag::EmptyMutable(), {&message}, {"a.(b.c)"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "extension not found: \"b.c\""));

  EXPECT_THAT(FromProto(DataBag::EmptyMutable(), {&message},
                        {"a.(koladata.testing.bool_extension_field).(b.c)"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "extension not found: \"b.c\""));

  EXPECT_THAT(
      FromProto(DataBag::EmptyMutable(), {&message}, {}, std::nullopt,
                test::EntitySchema({"(b.c)"}, {test::Schema(schema::kObject)},
                                   DataBag::EmptyMutable())),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "extension not found: \"b.c\""));
}

TEST(FromProtoTest, TemplateFromProtoWithVectorOfMessages) {
  std::vector<testing::ExampleMessage> messages;
  messages.push_back(GetExampleMessageAllFieldsNoExtensions());
  messages.push_back(testing::ExampleMessage());

  ASSERT_OK_AND_ASSIGN(auto result, FromProto(absl::MakeConstSpan(messages)));
  EXPECT_EQ(result.size(), 2);
  EXPECT_FALSE(result.GetBag()->IsMutable());

  // Spot check a field.
  EXPECT_THAT(result.GetAttr("int32_field")->WithBag(nullptr),
              IsEquivalentTo(test::DataSlice<int32_t>({10, std::nullopt})));
}

TEST(FromProtoTest, TemplateFromProtoWithVectorOfUniquePtrs) {
  std::vector<std::unique_ptr<testing::ExampleMessage>> messages;
  messages.push_back(std::make_unique<testing::ExampleMessage>(
      GetExampleMessageAllFieldsNoExtensions()));
  messages.push_back(std::make_unique<testing::ExampleMessage>());

  ASSERT_OK_AND_ASSIGN(auto result, FromProto(absl::MakeConstSpan(messages)));
  EXPECT_EQ(result.size(), 2);
  EXPECT_FALSE(result.GetBag()->IsMutable());

  // Spot check a field.
  EXPECT_THAT(result.GetAttr("int32_field")->WithBag(nullptr),
              IsEquivalentTo(test::DataSlice<int32_t>({10, std::nullopt})));
}

TEST(FromProtoTest, SchemaFromProtoNoExtensions) {
  ASSERT_OK_AND_ASSIGN(auto schema,
                       SchemaFromProto(DataBag::EmptyMutable(),
                                       testing::ExampleMessage::descriptor()));

  EXPECT_EQ(schema.GetSchemaImpl(), internal::DataItem(schema::kSchema));
  EXPECT_THAT(*schema.GetAttrNames(),
              UnorderedElementsAre(
                  "int32_field", "int64_field", "uint32_field", "uint64_field",
                  "double_field", "float_field", "bool_field", "enum_field",
                  "string_field", "bytes_field", "message_field",
                  "repeated_int32_field", "repeated_int64_field",
                  "repeated_uint32_field", "repeated_uint64_field",
                  "repeated_double_field", "repeated_float_field",
                  "repeated_bool_field", "repeated_enum_field",
                  "repeated_string_field", "repeated_bytes_field",
                  "repeated_message_field", "map_int32_int32_field",
                  "map_string_string_field", "map_int32_message_field"));

  EXPECT_EQ(schema.GetAttr("int32_field")->item(),
            internal::DataItem(schema::kInt32));
  EXPECT_EQ(schema.GetAttr("int64_field")->item(),
            internal::DataItem(schema::kInt64));
  EXPECT_EQ(schema.GetAttr("uint32_field")->item(),
            internal::DataItem(schema::kInt64));
  EXPECT_EQ(schema.GetAttr("uint64_field")->item(),
            internal::DataItem(schema::kInt64));
  EXPECT_EQ(schema.GetAttr("double_field")->item(),
            internal::DataItem(schema::kFloat64));
  EXPECT_EQ(schema.GetAttr("float_field")->item(),
            internal::DataItem(schema::kFloat32));
  EXPECT_EQ(schema.GetAttr("bool_field")->item(),
            internal::DataItem(schema::kBool));
  EXPECT_EQ(schema.GetAttr("enum_field")->item(),
            internal::DataItem(schema::kInt32));
  EXPECT_EQ(schema.GetAttr("string_field")->item(),
            internal::DataItem(schema::kString));
  EXPECT_EQ(schema.GetAttr("bytes_field")->item(),
            internal::DataItem(schema::kBytes));
  EXPECT_EQ(schema.GetAttr("message_field")->item(), schema.item());

  const auto& get_list_schema_item =
      [&](schema::DType dtype) -> internal::DataItem {
    return CreateListSchema(
               DataBag::EmptyMutable(),
               *DataSlice::Create(internal::DataItem(dtype),
                                  internal::DataItem(schema::kSchema)))
        ->item();
  };

  EXPECT_EQ(schema.GetAttr("repeated_int32_field")->item(),
            get_list_schema_item(schema::kInt32));
  EXPECT_EQ(schema.GetAttr("repeated_int64_field")->item(),
            get_list_schema_item(schema::kInt64));
  EXPECT_EQ(schema.GetAttr("repeated_uint32_field")->item(),
            get_list_schema_item(schema::kInt64));
  EXPECT_EQ(schema.GetAttr("repeated_uint64_field")->item(),
            get_list_schema_item(schema::kInt64));
  EXPECT_EQ(schema.GetAttr("repeated_double_field")->item(),
            get_list_schema_item(schema::kFloat64));
  EXPECT_EQ(schema.GetAttr("repeated_float_field")->item(),
            get_list_schema_item(schema::kFloat32));
  EXPECT_EQ(schema.GetAttr("repeated_bool_field")->item(),
            get_list_schema_item(schema::kBool));
  EXPECT_EQ(schema.GetAttr("repeated_enum_field")->item(),
            get_list_schema_item(schema::kInt32));
  EXPECT_EQ(schema.GetAttr("repeated_string_field")->item(),
            get_list_schema_item(schema::kString));
  EXPECT_EQ(schema.GetAttr("repeated_bytes_field")->item(),
            get_list_schema_item(schema::kBytes));
  EXPECT_EQ(schema.GetAttr("repeated_message_field")->item(),
            CreateListSchema(DataBag::EmptyMutable(), schema)->item());

  EXPECT_EQ(
      schema.GetAttr("map_int32_int32_field")->item(),
      CreateDictSchema(DataBag::EmptyMutable(), test::Schema(schema::kInt32),
                       test::Schema(schema::kInt32))
          ->item());
  EXPECT_EQ(
      schema.GetAttr("map_string_string_field")->item(),
      CreateDictSchema(DataBag::EmptyMutable(), test::Schema(schema::kString),
                       test::Schema(schema::kString))
          ->item());
  EXPECT_EQ(schema.GetAttr("map_int32_message_field")->item(),
            CreateDictSchema(DataBag::EmptyMutable(),
                             test::Schema(schema::kInt32), schema)
                ->item());
}

TEST(FromProtoTest, SchemaFromProtoNoExtensions_NoBagOverload) {
  ASSERT_OK_AND_ASSIGN(auto schema,
                       SchemaFromProto(testing::ExampleMessage::descriptor()));
  EXPECT_FALSE(schema.GetBag()->IsMutable());
  ASSERT_OK_AND_ASSIGN(auto schema_with_bag,
                       SchemaFromProto(DataBag::EmptyMutable(),
                                       testing::ExampleMessage::descriptor()));
  EXPECT_EQ(schema.item(), schema_with_bag.item());
}

TEST(FromProtoTest, SchemaFromProtoWithExtensions) {
  ASSERT_OK_AND_ASSIGN(
      auto schema,
      SchemaFromProto(DataBag::EmptyMutable(),
                      testing::ExampleMessage2::descriptor(),
                      {
                          "(koladata.testing.m2_bool_extension_field)",
                          "(koladata.testing.m2_message2_extension_field)",
                      }));

  EXPECT_EQ(schema.GetSchemaImpl(), internal::DataItem(schema::kSchema));
  EXPECT_THAT(
      *schema.GetAttrNames(),
      UnorderedElementsAre("(koladata.testing.m2_bool_extension_field)",
                           "(koladata.testing.m2_message2_extension_field)"));

  EXPECT_EQ(
      schema.GetAttr("(koladata.testing.m2_bool_extension_field)")->item(),
      internal::DataItem(schema::kBool));
  EXPECT_EQ(
      schema.GetAttr("(koladata.testing.m2_message2_extension_field)")->item(),
      schema.item());
}

TEST(FromProtoTest, SchemaFromProtoWithNestedExtension) {
  ASSERT_OK_AND_ASSIGN(
      auto schema,
      SchemaFromProto(DataBag::EmptyMutable(),
                      testing::ExampleMessage::descriptor(),
                      {
                          "(koladata.testing.message_extension_field)",
                          "(koladata.testing.message_extension_field)."
                          "(koladata.testing.m2_bool_extension_field)",
                      }));

  EXPECT_EQ(schema.GetSchemaImpl(), internal::DataItem(schema::kSchema));
  EXPECT_EQ(schema.GetAttr("(koladata.testing.message_extension_field)")
                ->GetAttr("(koladata.testing.m2_bool_extension_field)")
                ->item(),
            internal::DataItem(schema::kBool));
}

TEST(FromProtoTest, SchemaFromProtoWithKodaOptions) {
  ASSERT_OK_AND_ASSIGN(
      auto schema,
      SchemaFromProto(DataBag::EmptyMutable(),
                      testing::ExampleMessageWithKodaOptions::descriptor()));
  EXPECT_EQ(schema.GetAttr("bool_field")->item(),
            internal::DataItem(schema::kMask));
  EXPECT_EQ(schema.GetAttr("repeated_bool_field")
                ->GetAttr(schema::kListItemsSchemaAttr)
                ->item(),
            internal::DataItem(schema::kMask));
  EXPECT_EQ(schema.GetAttr("int32_field_as_int64")->item(),
            internal::DataItem(schema::kInt64));
  EXPECT_EQ(schema.GetAttr("repeated_int32_field_as_int64")
                ->GetAttr(schema::kListItemsSchemaAttr)
                ->item(),
            internal::DataItem(schema::kInt64));
  EXPECT_EQ(schema.GetAttr("int64_field_as_int32")->item(),
            internal::DataItem(schema::kInt32));
  EXPECT_EQ(schema.GetAttr("repeated_int64_field_as_int32")
                ->GetAttr(schema::kListItemsSchemaAttr)
                ->item(),
            internal::DataItem(schema::kInt32));
  EXPECT_EQ(schema.GetAttr("enum_field_as_int64")->item(),
            internal::DataItem(schema::kInt64));
  EXPECT_EQ(schema.GetAttr("repeated_enum_field_as_int64")
                ->GetAttr(schema::kListItemsSchemaAttr)
                ->item(),
            internal::DataItem(schema::kInt64));
}

TEST(FromProtoTest, SchemaFromProtoWithKodaOptionsWrongTypeMask) {
  EXPECT_THAT(
      SchemaFromProto(DataBag::EmptyMutable(),
                      testing::ExampleMessageWrongTypeMask::descriptor()),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "MASK koda field schema override can only be used on bool fields"));
}

TEST(FromProtoTest, SchemaFromProtoWithKodaOptionsWrongTypeInt32) {
  EXPECT_THAT(
      SchemaFromProto(DataBag::EmptyMutable(),
                      testing::ExampleMessageWrongTypeInt32::descriptor()),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "INT32 koda field schema override can only be used on integer "
               "and enum fields"));
}

TEST(FromProtoTest, SchemaFromProtoWithKodaOptionsWrongTypeInt64) {
  EXPECT_THAT(
      SchemaFromProto(DataBag::EmptyMutable(),
                      testing::ExampleMessageWrongTypeInt64::descriptor()),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "INT64 koda field schema override can only be used on integer "
               "and enum fields"));
}

TEST(FromProtoTest, SchemaFromProtoWithKodaOptionsConflictAnnotation) {
  EXPECT_THAT(
      SchemaFromProto(DataBag::EmptyMutable(),
                      testing::ExampleMessageConflictAnnotation::descriptor()),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "koda MASK field schema override cannot be used on fields with a "
          "custom default value"));
}

}  // namespace
}  // namespace koladata
