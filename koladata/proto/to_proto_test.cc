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
#include "koladata/proto/to_proto.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/testing/equals_proto.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/schema_attrs.h"
#include "koladata/object_factories.h"
#include "koladata/operators/masking.h"
#include "koladata/proto/from_proto.h"
#include "koladata/proto/testing/test_proto2.pb.h"
#include "koladata/proto/testing/test_proto3.pb.h"
#include "koladata/test_utils.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "google/protobuf/util/message_differencer.h"
#include "arolla/util/status_macros_backport.h"

using ::absl_testing::IsOkAndHolds;
using ::absl_testing::StatusIs;
using ::arolla::testing::EqualsProto;
using ::google::protobuf::Message;
using ::google::protobuf::TextFormat;
using ::google::protobuf::util::MessageDifferencer;
using ::testing::ElementsAre;
using ::testing::MatchesRegex;
using ::testing::StartsWith;

namespace koladata {
namespace {

template <typename MessageT>
std::tuple<std::vector<Message*>, std::vector<std::unique_ptr<MessageT>>>
MakeEmptyMessages(size_t num_messages) {
  std::vector<std::unique_ptr<MessageT>> messages;
  messages.reserve(num_messages);
  std::vector<Message*> message_ptrs;
  message_ptrs.reserve(num_messages);
  for (size_t i = 0; i < num_messages; ++i) {
    messages.push_back(std::make_unique<MessageT>());
    message_ptrs.push_back(messages.back().get());
  }
  return std::make_tuple(std::move(message_ptrs), std::move(messages));
}

TEST(ToProtoTest, ZeroMessages) {
  auto slice = test::EmptyDataSlice(0, internal::DataItem(schema::kObject));
  EXPECT_OK(ToProto(slice, {}));
}

TEST(ToProtoTest, AllMissingRootInput) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema({}, {}, db);
  auto slice = test::EmptyDataSlice(2, schema.GetSchemaImpl(), db);
  auto [message_ptrs, messages] = MakeEmptyMessages<testing::ExampleMessage>(2);
  EXPECT_OK(ToProto(slice, message_ptrs));
  EXPECT_EQ(messages[0]->ByteSizeLong(), 0);
  EXPECT_EQ(messages[1]->ByteSizeLong(), 0);
}

TEST(ToProtoTest, AllFieldsAllExtensionsRoundTrip) {
  koladata::testing::ExampleMessage message1;
  // Note: 18446744073709551606 = 2**64 - 10, to test uint64 round-trip.
  CHECK(TextFormat::ParseFromString(
      R"pb(
        int32_field: -10
        int64_field: -9223372036854775808
        uint32_field: 30
        uint64_field: 18446744073709551606
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
      &message1));
  message1.SetExtension(koladata::testing::bool_extension_field, true);
  message1.MutableExtension(koladata::testing::message_extension_field);

  // To exercise batching logic. Add some minor differences.
  koladata::testing::ExampleMessage message2 = message1;
  message2.set_int32_field(11);
  message2.clear_double_field();
  koladata::testing::ExampleMessage message3 = message1;
  message3.set_int32_field(12);
  message3.clear_float_field();

  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(
      auto slice, FromProto(db, {&message1, &message2, &message3},
                            {"(koladata.testing.bool_extension_field)",
                             "(koladata.testing.message_extension_field)"}));

  koladata::testing::ExampleMessage output_message1;
  koladata::testing::ExampleMessage output_message2;
  koladata::testing::ExampleMessage output_message3;
  ASSERT_OK(
      ToProto(slice, {&output_message1, &output_message2, &output_message3}));

  EXPECT_TRUE(MessageDifferencer::Equals(output_message1, message1));
  EXPECT_TRUE(MessageDifferencer::Equals(output_message2, message2));
  EXPECT_TRUE(MessageDifferencer::Equals(output_message3, message3));
}

TEST(ToProtoTest, MissingDict) {
  auto db = DataBag::Empty();
  auto dict = *CreateDictShaped(db, DataSlice::JaggedShape::Empty(),
                                test::DataSlice<int32_t>({1, 3}),
                                test::DataSlice<int32_t>({2, 4}));
  auto slice = test::DataSlice<internal::DataItem>(
      {
          ObjectCreator::FromAttrs(db, {"map_int32_int32_field"}, {dict})
              ->item(),
          internal::DataItem(),
      },
      db);

  koladata::testing::ExampleMessage output_message1;
  koladata::testing::ExampleMessage output_message2;
  ASSERT_OK(ToProto(slice, {&output_message1, &output_message2}));

  koladata::testing::ExampleMessage expected_message1;
  koladata::testing::ExampleMessage expected_message2;
  CHECK(TextFormat::ParseFromString(R"pb(
                                      map_int32_int32_field: { key: 1 value: 2 }
                                      map_int32_int32_field: { key: 3 value: 4 }
                                    )pb",
                                    &expected_message1));

  EXPECT_TRUE(MessageDifferencer::Equals(output_message1, expected_message1));
  EXPECT_TRUE(MessageDifferencer::Equals(output_message2, expected_message2));
}

TEST(ToProtoTest, RepeatedMessageFieldMissingItem) {
  auto db = DataBag::Empty();
  auto items = *ObjectCreator::Shaped(
      db, DataSlice::JaggedShape::FlatFromSize(3), {"string_field"},
      {test::DataSlice<arolla::Text>({"a", "b", "c"})});
  auto mask = test::DataSlice<arolla::Unit>(
      {arolla::kUnit, std::nullopt, arolla::kUnit});
  items = *ops::ApplyMask(items, mask);
  auto list = *CreateListsFromLastDimension(db, items);
  auto slice = *ObjectCreator::FromAttrs(db, {"repeated_message_field"}, {list})
                    ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));

  koladata::testing::ExampleMessage output_message;
  ASSERT_OK(ToProto(slice, {&output_message}));

  koladata::testing::ExampleMessage expected_message;
  CHECK(
      TextFormat::ParseFromString(R"pb(
                                    repeated_message_field { string_field: "a" }
                                    repeated_message_field {}
                                    repeated_message_field { string_field: "c" }
                                  )pb",
                                  &expected_message));
  EXPECT_TRUE(MessageDifferencer::Equals(output_message, expected_message));
}

TEST(ToProtoTest, InvalidMapFieldNotDicts) {
  auto db = DataBag::Empty();
  auto dict = test::DataItem<int32_t>(1, db);
  auto slice = *ObjectCreator::FromAttrs(db, {"map_int32_int32_field"}, {dict})
                    ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));
  koladata::testing::ExampleMessage message;
  ASSERT_THAT(
      ToProto(slice, {&message}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          StartsWith("proto map field map_int32_int32_field expected Koda "
                     "DataSlice to contain only Dicts but got DataSlice([1], "
                     "schema: INT32, shape: JaggedShape(1), bag_id: $")));
}

TEST(ToProtoTest, RepeatedPrimitiveFieldMixedDtypes) {
  auto db = DataBag::Empty();
  auto items =
      *DataSlice::Create(internal::DataSliceImpl::Create({
                             internal::DataItem(static_cast<int32_t>(1)),
                             internal::DataItem(static_cast<int64_t>(2)),
                         }),
                         DataSlice::JaggedShape::FlatFromSize(2),
                         internal::DataItem(schema::kObject), db);
  auto list = *CreateListsFromLastDimension(db, items);
  auto slice = *ObjectCreator::FromAttrs(db, {"repeated_int32_field"}, {list})
                    ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));

  koladata::testing::ExampleMessage output_message;
  ASSERT_OK(ToProto(slice, {&output_message}));

  koladata::testing::ExampleMessage expected_message;
  CHECK(TextFormat::ParseFromString(R"pb(
                                      repeated_int32_field: 1
                                      repeated_int32_field: 2
                                    )pb",
                                    &expected_message));
  EXPECT_TRUE(MessageDifferencer::Equals(output_message, expected_message));
}

TEST(ToProtoTest, IntegerOutOfRange) {
  {
    auto db = DataBag::Empty();
    auto slice = *ObjectCreator::FromAttrs(
                      db, {"int32_field"},
                      {test::DataItem(2147483648LL,
                                      test::Schema(schema::kInt64).item())})
                      ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));
    koladata::testing::ExampleMessage message;
    ASSERT_THAT(
        ToProto(slice, {&message}),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            "value 2147483648 out of range for proto field int32_field with "
            "value type int32"));
  }
  {
    auto db = DataBag::Empty();
    auto slice = *ObjectCreator::FromAttrs(
                      db, {"int32_field"},
                      {test::DataItem(-2147483649LL,
                                      test::Schema(schema::kInt64).item())})
                      ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));
    koladata::testing::ExampleMessage message;
    ASSERT_THAT(
        ToProto(slice, {&message}),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            "value -2147483649 out of range for proto field int32_field with "
            "value type int32"));
  }
  {
    auto db = DataBag::Empty();
    auto slice =
        *ObjectCreator::FromAttrs(
             db, {"uint32_field"},
             {test::DataItem(-1LL, test::Schema(schema::kInt64).item())})
             ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));
    koladata::testing::ExampleMessage message;
    ASSERT_THAT(
        ToProto(slice, {&message}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "value -1 out of range for proto field uint32_field with "
                 "value type uint32"));
  }
  {
    auto db = DataBag::Empty();
    auto slice = *ObjectCreator::FromAttrs(
                      db, {"uint32_field"},
                      {test::DataItem(4294967297LL,
                                      test::Schema(schema::kInt64).item())})
                      ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));
    koladata::testing::ExampleMessage message;
    ASSERT_THAT(
        ToProto(slice, {&message}),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            "value 4294967297 out of range for proto field uint32_field with "
            "value type uint32"));
  }
  {
    auto db = DataBag::Empty();
    auto slice = *ObjectCreator::FromAttrs(
                      db, {"uint32_field"},
                      {test::DataItem(-1, test::Schema(schema::kInt32).item())})
                      ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));
    koladata::testing::ExampleMessage message;
    ASSERT_THAT(
        ToProto(slice, {&message}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "value -1 out of range for proto field uint32_field with "
                 "value type uint32"));
  }
  {
    auto db = DataBag::Empty();
    auto slice = *ObjectCreator::FromAttrs(
                      db, {"uint64_field"},
                      {test::DataItem(-1, test::Schema(schema::kInt32).item())})
                      ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));
    koladata::testing::ExampleMessage message;
    ASSERT_THAT(
        ToProto(slice, {&message}),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 "value -1 out of range for proto field uint64_field with "
                 "value type uint64"));
  }
}

TEST(ToProtoTest, InvalidFieldType) {
  {
    auto db = DataBag::Empty();
    auto slice =
        *ObjectCreator::FromAttrs(
             db, {"int32_field"},
             {test::DataItem("hello", test::Schema(schema::kString).item())})
             ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));
    koladata::testing::ExampleMessage message;
    EXPECT_THAT(ToProto(slice, {&message}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "invalid proto field int32_field with value type "
                         "int32 for Koda value 'hello' with dtype TEXT"));
  }
  {
    auto db = DataBag::Empty();
    auto slice =
        *ObjectCreator::FromAttrs(
             db, {"bytes_field"},
             {test::DataItem("hello", test::Schema(schema::kString).item())})
             ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));
    koladata::testing::ExampleMessage message;
    EXPECT_THAT(ToProto(slice, {&message}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "invalid proto field bytes_field with value type "
                         "bytes for Koda value 'hello' with dtype TEXT"));
  }
  {
    auto db = DataBag::Empty();
    auto slice = *ObjectCreator::FromAttrs(
                      db, {"string_field"},
                      {test::DataItem<arolla::Bytes>(
                          "hello", test::Schema(schema::kBytes).item())})
                      ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));
    koladata::testing::ExampleMessage message;
    EXPECT_THAT(ToProto(slice, {&message}),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "invalid proto field string_field with value type "
                         "string for Koda value b'hello' with dtype BYTES"));
  }
}

TEST(ToProtoTest, InvalidRepeatedFieldMissingValue) {
  auto db = DataBag::Empty();
  auto items = test::DataSlice<int32_t>({1, std::nullopt, 2}, db);
  auto list = *CreateListsFromLastDimension(db, items);
  auto slice = *ObjectCreator::FromAttrs(db, {"repeated_int32_field"}, {list})
                    ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));
  koladata::testing::ExampleMessage message;
  ASSERT_THAT(
      ToProto(slice, {&message}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               StartsWith(
                   "proto repeated field repeated_int32_field cannot represent "
                   "missing values, but got DataSlice([[1, None, 2]], schema: "
                   "INT32, shape: JaggedShape(1, 3), bag_id: $")));
}

TEST(ToProtoTest, InvalidRepeatedPrimitiveFieldNotLists) {
  auto db = DataBag::Empty();
  auto list = test::DataItem<int32_t>(1, db);
  auto slice = *ObjectCreator::FromAttrs(db, {"repeated_int32_field"}, {list})
                    ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));
  koladata::testing::ExampleMessage message;
  ASSERT_THAT(
      ToProto(slice, {&message}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          StartsWith(
              "proto repeated primitive field repeated_int32_field expected "
              "Koda DataSlice to contain only Lists but got DataSlice([1], "
              "schema: INT32, shape: JaggedShape(1), bag_id: $")));
}

TEST(ToProtoTest, InvalidRepeatedMessageFieldNotLists) {
  auto db = DataBag::Empty();
  auto list = test::DataItem<int32_t>(1, db);
  auto slice = *ObjectCreator::FromAttrs(db, {"repeated_message_field"}, {list})
                    ->Reshape(DataSlice::JaggedShape::FlatFromSize(1));
  koladata::testing::ExampleMessage message;
  ASSERT_THAT(
      ToProto(slice, {&message}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          StartsWith(
              "proto repeated message field repeated_message_field expected "
              "Koda DataSlice to contain only Lists but got DataSlice([1], "
              "schema: INT32, shape: JaggedShape(1), bag_id: $")));
}

TEST(ToProtoTest, InvalidNonItemMessage) {
  auto db = DataBag::Empty();
  auto slice = test::DataSlice<int32_t>({1}, db);
  koladata::testing::ExampleMessage message;
  ASSERT_THAT(
      ToProto(slice, {&message}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "proto message should have only entities/objects, found INT32"));
}

TEST(ToProtoTest, InvalidInputNdim) {
  {
    auto db = DataBag::Empty();
    auto slice = *ObjectCreator::FromAttrs(db, {}, {});
    auto [message_ptrs, messages] =
        MakeEmptyMessages<testing::ExampleMessage>(1);
    EXPECT_THAT(ToProto(slice, message_ptrs),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expected 1-D DataSlice, got ndim=0"));
  }
  {
    auto db = DataBag::Empty();
    auto slice = *ObjectCreator::FromAttrs(db, {}, {});
    slice = *slice.Reshape(*DataSlice::JaggedShape::FromEdges({
        *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
        *DataSlice::JaggedShape::Edge::FromUniformGroups(1, 1),
    }));
    auto [message_ptrs, messages] =
        MakeEmptyMessages<testing::ExampleMessage>(1);
    EXPECT_THAT(ToProto(slice, message_ptrs),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         "expected 1-D DataSlice, got ndim=2"));
  }
}

TEST(ToProtoTest, InvalidInputNumMessages) {
  auto slice = test::EmptyDataSlice(1, internal::DataItem(schema::kObject));
  std::vector<::google::protobuf::Message*> message_ptrs;
  EXPECT_THAT(
      ToProto(slice, message_ptrs),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "expected slice and messages to have the same size, got 1 and 0"));
}

TEST(ToProtoTest, InvalidInputMultipleDescriptors) {
  auto slice = test::EmptyDataSlice(2, internal::DataItem(schema::kObject));
  testing::ExampleMessage message1;
  testing::ExampleMessage2 message2;
  EXPECT_THAT(ToProto(slice, {&message1, &message2}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected all messages to have the same type, got "
                       "koladata.testing.ExampleMessage and "
                       "koladata.testing.ExampleMessage2"));
}

TEST(ProtoDescriptorFromSchemaTest, GeneratesPackageNameWithSchemaFingerprint) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema({}, {}, db);
  ASSERT_OK_AND_ASSIGN(auto descriptor, ProtoDescriptorFromSchema(schema));

  std::string expected_package_name =
      absl::StrCat("koladata.ephemeral.schema_",
                   schema.item().StableFingerprint().AsString());
  EXPECT_EQ(descriptor.package(), expected_package_name);
}

absl::StatusOr<google::protobuf::FileDescriptorProto> CallProtoDescriptorFromSchema(
    const DataSlice& schema, std::vector<std::string>* warnings = nullptr) {
  return ProtoDescriptorFromSchema(
      schema, /*warnings=*/warnings,
      /*file_name=*/"", /*descriptor_package_name=*/"koladata.ephemeral",
      /*root_message_name=*/"RootSchema");
}

TEST(ProtoDescriptorFromSchemaTest, EntitySchemaWithOnlyPrimitiveAttributes) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"my_string", "my_bytes", "my_int32", "my_int64", "my_float32",
       "my_float64", "my_bool", "my_mask"},
      {test::Schema(schema::kString), test::Schema(schema::kBytes),
       test::Schema(schema::kInt32), test::Schema(schema::kInt64),
       test::Schema(schema::kFloat32), test::Schema(schema::kFloat64),
       test::Schema(schema::kBool), test::Schema(schema::kMask)},
      db);
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type {
                  name: "RootSchema"
                  field {
                    name: "my_bool"
                    number: 1
                    label: LABEL_OPTIONAL
                    type: TYPE_BOOL
                  }
                  field {
                    name: "my_bytes"
                    number: 2
                    label: LABEL_OPTIONAL
                    type: TYPE_BYTES
                  }
                  field {
                    name: "my_float32"
                    number: 3
                    label: LABEL_OPTIONAL
                    type: TYPE_FLOAT
                  }
                  field {
                    name: "my_float64"
                    number: 4
                    label: LABEL_OPTIONAL
                    type: TYPE_DOUBLE
                  }
                  field {
                    name: "my_int32"
                    number: 5
                    label: LABEL_OPTIONAL
                    type: TYPE_INT32
                  }
                  field {
                    name: "my_int64"
                    number: 6
                    label: LABEL_OPTIONAL
                    type: TYPE_INT64
                  }
                  field {
                    name: "my_mask"
                    number: 7
                    label: LABEL_OPTIONAL
                    type: TYPE_BOOL
                  }
                  field {
                    name: "my_string"
                    number: 8
                    label: LABEL_OPTIONAL
                    type: TYPE_STRING
                  }
                }
              )pb")));
}

TEST(ProtoDescriptorFromSchemaTest, NestedEntities) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"value", "level_1_entity"},
      {test::Schema(schema::kInt64),
       test::EntitySchema(
           {"value", "level_2_entity"},
           {test::Schema(schema::kString),
            test::EntitySchema({"value"}, {test::Schema(schema::kInt32)}, db)},
           db)},
      db);
  EXPECT_THAT(
      CallProtoDescriptorFromSchema(schema), IsOkAndHolds(EqualsProto(R"pb(
        name: ""
        package: "koladata.ephemeral"
        message_type {
          name: "RootSchema"
          field {
            name: "level_1_entity"
            number: 1
            label: LABEL_OPTIONAL
            type: TYPE_MESSAGE
            type_name: "koladata.ephemeral.RootSchema.Level1EntitySchema"
          }
          field {
            name: "value"
            number: 2
            label: LABEL_OPTIONAL
            type: TYPE_INT64
          }
          nested_type {
            name: "Level1EntitySchema"
            field {
              name: "level_2_entity"
              number: 1
              label: LABEL_OPTIONAL
              type: TYPE_MESSAGE
              type_name: "koladata.ephemeral.RootSchema.Level1EntitySchema.Level2EntitySchema"
            }
            field {
              name: "value"
              number: 2
              label: LABEL_OPTIONAL
              type: TYPE_STRING
            }
            nested_type {
              name: "Level2EntitySchema"
              field {
                name: "value"
                number: 1
                label: LABEL_OPTIONAL
                type: TYPE_INT32
              }
            }
          }
        }
      )pb")));
}

TEST(ProtoDescriptorFromSchemaTest, RecursiveEntitySchemaLoopBackToRootSchema) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"value", "level_1_entity"},
      {test::Schema(schema::kInt64),
       test::EntitySchema(
           {"value", "level_2_entity"},
           {test::Schema(schema::kString),
            test::EntitySchema({"value"}, {test::Schema(schema::kNone)}, db)},
           db)},
      db);
  ASSERT_OK_AND_ASSIGN(auto level_1_entity, schema.GetAttr("level_1_entity"));
  ASSERT_OK_AND_ASSIGN(auto level_2_entity,
                       level_1_entity.GetAttr("level_2_entity"));
  ASSERT_OK(level_2_entity.SetAttr("value", schema));
  EXPECT_THAT(
      CallProtoDescriptorFromSchema(schema), IsOkAndHolds(EqualsProto(R"pb(
        name: ""
        package: "koladata.ephemeral"
        message_type {
          name: "RootSchema"
          field {
            name: "level_1_entity"
            number: 1
            label: LABEL_OPTIONAL
            type: TYPE_MESSAGE
            type_name: "koladata.ephemeral.RootSchema.Level1EntitySchema"
          }
          field {
            name: "value"
            number: 2
            label: LABEL_OPTIONAL
            type: TYPE_INT64
          }
          nested_type {
            name: "Level1EntitySchema"
            field {
              name: "level_2_entity"
              number: 1
              label: LABEL_OPTIONAL
              type: TYPE_MESSAGE
              type_name: "koladata.ephemeral.RootSchema.Level1EntitySchema.Level2EntitySchema"
            }
            field {
              name: "value"
              number: 2
              label: LABEL_OPTIONAL
              type: TYPE_STRING
            }
            nested_type {
              name: "Level2EntitySchema"
              field {
                name: "value"
                number: 1
                label: LABEL_OPTIONAL
                type: TYPE_MESSAGE
                type_name: "koladata.ephemeral.RootSchema"
              }
            }
          }
        }
      )pb")));
}

TEST(ProtoDescriptorFromSchemaTest, ListsOfPrimitives) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"string_list", "bytes_list", "int32_list", "int64_list", "float32_list",
       "float64_list", "bool_list", "mask_list"},
      {
          CreateListSchema(db, test::Schema(schema::kString)).value(),
          CreateListSchema(db, test::Schema(schema::kBytes)).value(),
          CreateListSchema(db, test::Schema(schema::kInt32)).value(),
          CreateListSchema(db, test::Schema(schema::kInt64)).value(),
          CreateListSchema(db, test::Schema(schema::kFloat32)).value(),
          CreateListSchema(db, test::Schema(schema::kFloat64)).value(),
          CreateListSchema(db, test::Schema(schema::kBool)).value(),
          CreateListSchema(db, test::Schema(schema::kMask)).value(),
      },
      db);
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type {
                  name: "RootSchema"
                  field {
                    name: "bool_list"
                    number: 1
                    label: LABEL_REPEATED
                    type: TYPE_BOOL
                  }
                  field {
                    name: "bytes_list"
                    number: 2
                    label: LABEL_REPEATED
                    type: TYPE_BYTES
                  }
                  field {
                    name: "float32_list"
                    number: 3
                    label: LABEL_REPEATED
                    type: TYPE_FLOAT
                  }
                  field {
                    name: "float64_list"
                    number: 4
                    label: LABEL_REPEATED
                    type: TYPE_DOUBLE
                  }
                  field {
                    name: "int32_list"
                    number: 5
                    label: LABEL_REPEATED
                    type: TYPE_INT32
                  }
                  field {
                    name: "int64_list"
                    number: 6
                    label: LABEL_REPEATED
                    type: TYPE_INT64
                  }
                  field {
                    name: "mask_list"
                    number: 7
                    label: LABEL_REPEATED
                    type: TYPE_BOOL
                  }
                  field {
                    name: "string_list"
                    number: 8
                    label: LABEL_REPEATED
                    type: TYPE_STRING
                  }
                }
              )pb")));
}

TEST(ProtoDescriptorFromSchemaTest, ListOfEntities) {
  auto db = DataBag::Empty();
  auto list_item_schema =
      test::EntitySchema({"x"}, {test::Schema(schema::kInt32)}, db);
  auto schema =
      test::EntitySchema({"my_list"},
                         {
                             CreateListSchema(db, list_item_schema).value(),
                         },
                         db);
  EXPECT_THAT(
      CallProtoDescriptorFromSchema(schema), IsOkAndHolds(EqualsProto(R"pb(
        name: ""
        package: "koladata.ephemeral"
        message_type {
          name: "RootSchema"
          field {
            name: "my_list"
            number: 1
            label: LABEL_REPEATED
            type: TYPE_MESSAGE
            type_name: "koladata.ephemeral.RootSchema.MyListSchema"
          }
          nested_type {
            name: "MyListSchema"
            field { name: "x" number: 1 label: LABEL_OPTIONAL type: TYPE_INT32 }
          }
        }
      )pb")));
}

TEST(ProtoDescriptorFromSchemaTest, ListItemSchemaIsRootSchema) {
  // A simple recursive schema.
  auto db = DataBag::Empty();
  auto schema =
      test::EntitySchema({"value"}, {test::Schema(schema::kInt64)}, db);
  ASSERT_OK(schema.SetAttr("my_list", CreateListSchema(db, schema).value()));
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type {
                  name: "RootSchema"
                  field {
                    name: "my_list"
                    number: 1
                    label: LABEL_REPEATED
                    type: TYPE_MESSAGE
                    type_name: "koladata.ephemeral.RootSchema"
                  }
                  field {
                    name: "value"
                    number: 2
                    label: LABEL_OPTIONAL
                    type: TYPE_INT64
                  }
                }
              )pb")));
}

TEST(ProtoDescriptorFromSchemaTest, DictOfPrimitives) {
  auto db = DataBag::Empty();
  auto schema =
      test::EntitySchema({"my_dict"},
                         {CreateDictSchema(db, test::Schema(schema::kString),
                                           test::Schema(schema::kInt32))
                              .value()},
                         db);
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type {
                  name: "RootSchema"
                  field {
                    name: "my_dict"
                    number: 1
                    label: LABEL_REPEATED
                    type: TYPE_MESSAGE
                    type_name: "koladata.ephemeral.RootSchema.MyDictSchema"
                  }
                  nested_type {
                    name: "MyDictSchema"
                    field {
                      name: "key"
                      number: 1
                      label: LABEL_OPTIONAL
                      type: TYPE_STRING
                    }
                    field {
                      name: "value"
                      number: 2
                      label: LABEL_OPTIONAL
                      type: TYPE_INT32
                    }
                    options { map_entry: true }
                  }
                }
              )pb")));
}

TEST(ProtoDescriptorFromSchemaTest, DictValuesAreEntities) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"my_dict"},
      {CreateDictSchema(
           db, test::Schema(schema::kString),
           test::EntitySchema({"value"}, {test::Schema(schema::kInt32)}, db))
           .value()},
      db);
  EXPECT_THAT(
      CallProtoDescriptorFromSchema(schema), IsOkAndHolds(EqualsProto(R"pb(
        name: ""
        package: "koladata.ephemeral"
        message_type {
          name: "RootSchema"
          field {
            name: "my_dict"
            number: 1
            label: LABEL_REPEATED
            type: TYPE_MESSAGE
            type_name: "koladata.ephemeral.RootSchema.MyDictSchema"
          }
          nested_type {
            name: "MyDictSchema"
            field {
              name: "key"
              number: 1
              label: LABEL_OPTIONAL
              type: TYPE_STRING
            }
            field {
              name: "value"
              number: 2
              label: LABEL_OPTIONAL
              type: TYPE_MESSAGE
              type_name: "koladata.ephemeral.RootSchema.MyDictSchema.ValueSchema"
            }
            nested_type {
              name: "ValueSchema"
              field {
                name: "value"
                number: 1
                label: LABEL_OPTIONAL
                type: TYPE_INT32
              }
            }
            options { map_entry: true }
          }
        }
      )pb")));
}

TEST(ProtoDescriptorFromSchemaTest, DictOfRecursiveEntities) {
  auto db = DataBag::Empty();
  auto schema =
      test::EntitySchema({"value"}, {test::Schema(schema::kInt64)}, db);
  ASSERT_OK(schema.SetAttr(
      "my_dict",
      CreateDictSchema(db, test::Schema(schema::kString), schema).value()));
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type {
                  name: "RootSchema"
                  field {
                    name: "my_dict"
                    number: 1
                    label: LABEL_REPEATED
                    type: TYPE_MESSAGE
                    type_name: "koladata.ephemeral.RootSchema.MyDictSchema"
                  }
                  field {
                    name: "value"
                    number: 2
                    label: LABEL_OPTIONAL
                    type: TYPE_INT64
                  }
                  nested_type {
                    name: "MyDictSchema"
                    field {
                      name: "key"
                      number: 1
                      label: LABEL_OPTIONAL
                      type: TYPE_STRING
                    }
                    field {
                      name: "value"
                      number: 2
                      label: LABEL_OPTIONAL
                      type: TYPE_MESSAGE
                      type_name: "koladata.ephemeral.RootSchema"
                    }
                    options { map_entry: true }
                  }
                }
              )pb")));
}

absl::StatusOr<DataSlice> GenerateImplicitSchema(DataBagPtr db) {
  auto item = internal::DataItem(internal::CreateUuidWithMainObject<
                                 internal::ObjectId::kUuidImplicitSchemaFlag>(
      internal::AllocateSingleObject(),
      arolla::FingerprintHasher(schema::kImplicitSchemaSeed).Finish()));
  ASSIGN_OR_RETURN(
      auto slice,
      DataSlice::Create(item, internal::DataItem(schema::kSchema), db));
  RETURN_IF_ERROR(slice.SetAttr("foo", test::Schema(schema::kString)));
  return slice;
}

TEST(ProtoDescriptorFromSchemaTest, ImplicitSchema) {
  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto implicit_schema_slice, GenerateImplicitSchema(db));
  auto schema =
      test::EntitySchema({"my_implicit"}, {implicit_schema_slice}, db);
  EXPECT_THAT(
      DataSliceRepr(schema),
      MatchesRegex("DataItem\\(SCHEMA\\(my_implicit=IMPLICIT_SCHEMA\\(foo="
                   "STRING\\)\\), schema: SCHEMA, bag_id: \\$[a-f0-9]+\\)"));
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type {
                  name: "RootSchema"
                  field {
                    name: "my_implicit"
                    number: 1
                    label: LABEL_OPTIONAL
                    type: TYPE_MESSAGE
                    type_name: "koladata.ephemeral.RootSchema.MyImplicitSchema"
                  }
                  nested_type {
                    name: "MyImplicitSchema"
                    field {
                      name: "foo"
                      number: 1
                      label: LABEL_OPTIONAL
                      type: TYPE_STRING
                    }
                  }
                }
              )pb")));
}

TEST(ProtoDescriptorFromSchemaTest, IgnoredSchemas) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"my_none", "my_object", "my_itemid"},
      {test::Schema(schema::kNone), test::Schema(schema::kObject),
       test::Schema(schema::kItemId)},
      db);
  std::vector<std::string> warnings;
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema, &warnings),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type { name: "RootSchema" }
              )pb")));
  EXPECT_THAT(
      warnings,
      ElementsAre(MatchesRegex("unsupported schema type: DataItem\\(ITEMID, "
                               "schema: SCHEMA, bag_id: \\$[a-f0-9]+\\)"),
                  MatchesRegex("unsupported schema type: DataItem\\(NONE, "
                               "schema: SCHEMA, bag_id: \\$[a-f0-9]+\\)"),
                  MatchesRegex("unsupported schema type: DataItem\\(OBJECT, "
                               "schema: SCHEMA, bag_id: \\$[a-f0-9]+\\)")));
}

TEST(ProtoDescriptorFromSchemaTest, IgnoredPrimitiveTypes) {
  // At the time of writing, EXPR is the only ignored primitive type.
  auto db = DataBag::Empty();
  auto schema =
      test::EntitySchema({"my_expr"}, {test::Schema(schema::kExpr)}, db);
  std::vector<std::string> warnings;
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema, &warnings),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type { name: "RootSchema" }
              )pb")));
  EXPECT_THAT(warnings,
              ElementsAre(MatchesRegex(
                  "ignored type: EXPR and hence not adding proto field "
                  "koladata.ephemeral.RootSchema.my_expr")));
}

TEST(ProtoDescriptorFromSchemaTest, ListOfListsIgnored) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"my_list"},
      {CreateListSchema(
           db, CreateListSchema(db, test::Schema(schema::kInt32)).value())
           .value()},
      db);
  std::vector<std::string> warnings;
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema, &warnings),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type { name: "RootSchema" }
              )pb")));
  EXPECT_THAT(warnings,
              ElementsAre(MatchesRegex(
                  "unsupported LIST schema type: "
                  "DataItem\\(LIST\\[LIST\\[INT32\\]\\], schema: "
                  "SCHEMA, bag_id: \\$[a-f0-9]+\\). Supported LIST schemas "
                  "must have items "
                  "with either a primitive schema or an entity schema")));
}

TEST(ProtoDescriptorFromSchemaTest, ListOfDictsIgnored) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"my_list"},
      {CreateListSchema(db, CreateDictSchema(db, test::Schema(schema::kInt32),
                                             test::Schema(schema::kInt64))
                                .value())
           .value()},
      db);
  std::vector<std::string> warnings;
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema, &warnings),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type { name: "RootSchema" }
              )pb")));
  EXPECT_THAT(warnings,
              ElementsAre(MatchesRegex(
                  "unsupported LIST schema type: "
                  "DataItem\\(LIST\\[DICT\\{INT32, INT64\\}\\], schema: "
                  "SCHEMA, bag_id: \\$[a-f0-9]+\\). Supported LIST schemas "
                  "must have items "
                  "with either a primitive schema or an entity schema")));
}

TEST(ProtoDescriptorFromSchemaTest, DictWithEntityKeysIgnored) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"my_dict"},
      {CreateDictSchema(
           db, test::EntitySchema({"key"}, {test::Schema(schema::kString)}, db),
           test::Schema(schema::kInt32))
           .value()},
      db);
  std::vector<std::string> warnings;
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema, &warnings),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type {
                  name: "RootSchema"
                  nested_type { name: "MyDictSchema" }
                }
              )pb")));
  EXPECT_THAT(warnings,
              ElementsAre(MatchesRegex(
                  "unsupported DICT schema type: "
                  "DataItem\\(DICT\\{SCHEMA\\(key=STRING\\), INT32\\}, schema: "
                  "SCHEMA, bag_id: \\$[a-f0-9]+\\). Supported DICT schemas "
                  "must have keys that are integral types or strings \\(floats,"
                  " bytes and non-primitive keys are not supported\\)")));
}

TEST(ProtoDescriptorFromSchemaTest, DictWithListKeysIgnored) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"my_dict"},
      {CreateDictSchema(
           db, CreateListSchema(db, test::Schema(schema::kInt32)).value(),
           test::Schema(schema::kInt64))
           .value()},
      db);
  std::vector<std::string> warnings;
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema, &warnings),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type {
                  name: "RootSchema"
                  nested_type { name: "MyDictSchema" }
                }
              )pb")));
  EXPECT_THAT(
      warnings,
      ElementsAre(MatchesRegex(
          "unsupported DICT schema type: DataItem\\(DICT\\{LIST\\[INT32\\], "
          "INT64\\}, schema: SCHEMA, bag_id: \\$[a-f0-9]+\\). Supported DICT "
          "schemas must have keys that are integral types or strings "
          "\\(floats, bytes and non-primitive keys are not supported\\)")));
}

TEST(ProtoDescriptorFromSchemaTest, DictWithListValuesIgnored) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"my_dict"},
      {CreateDictSchema(
           db, test::Schema(schema::kInt32),
           CreateListSchema(db, test::Schema(schema::kInt64)).value())
           .value()},
      db);
  std::vector<std::string> warnings;
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema, &warnings),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type {
                  name: "RootSchema"
                  nested_type { name: "MyDictSchema" }
                }
              )pb")));
  EXPECT_THAT(
      warnings,
      ElementsAre(MatchesRegex(
          "unsupported DICT schema type: DataItem\\(DICT\\{INT32, "
          "LIST\\[INT64\\]\\}, schema: SCHEMA, bag_id: \\$[a-f0-9]+\\). "
          "Supported DICT schemas must have values with either a primitive "
          "schema or an entity schema")));
}

TEST(ProtoDescriptorFromSchemaTest, DictWithDictKeysIgnored) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"my_dict"},
      {CreateDictSchema(db,
                        CreateDictSchema(db, test::Schema(schema::kInt32),
                                         test::Schema(schema::kInt64))
                            .value(),
                        test::Schema(schema::kInt64))
           .value()},
      db);
  std::vector<std::string> warnings;
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema, &warnings),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type {
                  name: "RootSchema"
                  nested_type { name: "MyDictSchema" }
                }
              )pb")));
  EXPECT_THAT(
      warnings,
      ElementsAre(MatchesRegex(
          "unsupported DICT schema type: DataItem\\(DICT\\{DICT\\{INT32, "
          "INT64\\}, INT64\\}, schema: SCHEMA, bag_id: \\$[a-f0-9]+\\). "
          "Supported DICT schemas must have keys that are integral types or "
          "strings \\(floats, bytes and non-primitive keys are not "
          "supported\\)")));
}

TEST(ProtoDescriptorFromSchemaTest, DictWithDictValuesIgnored) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"my_dict"},
      {CreateDictSchema(db, test::Schema(schema::kInt32),
                        CreateDictSchema(db, test::Schema(schema::kInt64),
                                         test::Schema(schema::kInt32))
                            .value())
           .value()},
      db);
  std::vector<std::string> warnings;
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema, &warnings),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type {
                  name: "RootSchema"
                  nested_type { name: "MyDictSchema" }
                }
              )pb")));
  EXPECT_THAT(
      warnings,
      ElementsAre(MatchesRegex(
          "unsupported DICT schema type: DataItem\\(DICT\\{INT32, "
          "DICT\\{INT64, INT32\\}\\}, schema: SCHEMA, bag_id: \\$[a-f0-9]+\\). "
          "Supported DICT schemas must have values with either a primitive "
          "schema or an entity schema")));
}

TEST(ProtoDescriptorFromSchemaTest,
     AttributesWithNamesThatAreInvalidProtoFieldNamesIgnored) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema(
      {"_foo_bar", "ignoreme?", "123_for_me"},
      {test::Schema(schema::kInt32), test::Schema(schema::kInt64),
       test::Schema(schema::kString)},
      db);
  std::vector<std::string> warnings;
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema, &warnings),
              IsOkAndHolds(EqualsProto(R"pb(
                name: ""
                package: "koladata.ephemeral"
                message_type { name: "RootSchema" }
              )pb")));
  EXPECT_THAT(
      warnings,
      ElementsAre(
          MatchesRegex(
              "ignored attribute name 123_for_me encountered in schema "
              "DataItem\\(SCHEMA\\(123_for_me=STRING, _foo_bar=INT32, "
              "ignoreme\\?=INT64\\), schema: SCHEMA, bag_id: \\$[a-f0-9]+\\) "
              "because it is not a valid proto field name"),
          MatchesRegex(
              "ignored attribute name _foo_bar encountered in schema "
              "DataItem\\(SCHEMA\\(123_for_me=STRING, _foo_bar=INT32, "
              "ignoreme\\?=INT64\\), schema: SCHEMA, bag_id: \\$[a-f0-9]+\\) "
              "because it is not a valid proto field name"),
          MatchesRegex(
              "ignored attribute name ignoreme\\? encountered in schema "
              "DataItem\\(SCHEMA\\(123_for_me=STRING, _foo_bar=INT32, "
              "ignoreme\\?=INT64\\), schema: SCHEMA, bag_id: \\$[a-f0-9]+\\) "
              "because it is not a valid proto field name")));
}

TEST(ProtoDescriptorFromSchemaTest, SchemaNotEntitySchema) {
  auto schema = test::Schema(schema::kString);
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected Entity schema, got STRING"));
}

TEST(ProtoDescriptorFromSchemaTest, MissingSchema) {
  ASSERT_OK_AND_ASSIGN(auto schema,
                       DataSlice::Create(internal::DataItem(),
                                         internal::DataItem(schema::kSchema)));
  EXPECT_THAT(CallProtoDescriptorFromSchema(schema),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "schema must contain either a DType or valid schema "
                       "ItemId, got None"));
}

TEST(ProtoDescriptorFromSchemaTest, SchemaSliceWithRank1) {
  auto schema_slice = test::DataSlice<internal::DataItem>(
      {
          internal::DataItem(schema::kString),
          internal::DataItem(schema::kInt32),
      },
      schema::kSchema);
  EXPECT_THAT(
      CallProtoDescriptorFromSchema(schema_slice),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          StartsWith("schema can only be 0-rank schema slice, got: rank(1)")));
}

}  // namespace
}  // namespace koladata
