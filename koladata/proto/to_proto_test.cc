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
#include <tuple>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/object_factories.h"
#include "koladata/operators/logical.h"
#include "koladata/proto/from_proto.h"
#include "koladata/test_utils.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "koladata/proto/testing/test_proto2.pb.h"
#include "koladata/proto/testing/test_proto3.pb.h"
#include "google/protobuf/util/message_differencer.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"

using ::google::protobuf::util::MessageDifferencer;
using ::testing::StartsWith;
using ::google::protobuf::Message;
using ::absl_testing::StatusIs;
using ::google::protobuf::TextFormat;

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
  auto slice = test::EmptyDataSlice(0, internal::DataItem(schema::kAny));
  EXPECT_OK(ToProto(slice, {}));
}

TEST(ToProtoTest, AllMissingRootInput) {
  auto db = DataBag::Empty();
  auto schema = test::EntitySchema({}, {}, db);
  auto slice = test::EmptyDataSlice(2, schema.GetSchemaImpl(), db);
  auto [message_ptrs, messages] =
      MakeEmptyMessages<testing::ExampleMessage>(2);
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
  auto items = *DataSlice::Create(
      internal::DataSliceImpl::Create({
        internal::DataItem(static_cast<int32_t>(1)),
        internal::DataItem(static_cast<int64_t>(2)),
      }),
      DataSlice::JaggedShape::FlatFromSize(2),
      internal::DataItem(schema::kObject),
      db);
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
             {test::DataItem("hello", test::Schema(schema::kText).item())})
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
             {test::DataItem("hello", test::Schema(schema::kText).item())})
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
  auto slice = test::EmptyDataSlice(1, internal::DataItem(schema::kAny));
  std::vector<::google::protobuf::Message*> message_ptrs;
  EXPECT_THAT(
      ToProto(slice, message_ptrs),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "expected slice and messages to have the same size, got 1 and 0"));
}

TEST(ToProtoTest, InvalidInputMultipleDescriptors) {
  auto slice = test::EmptyDataSlice(2, internal::DataItem(schema::kAny));
  testing::ExampleMessage message1;
  testing::ExampleMessage2 message2;
  EXPECT_THAT(ToProto(slice, {&message1, &message2}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected all messages to have the same type, got "
                       "koladata.testing.ExampleMessage and "
                       "koladata.testing.ExampleMessage2"));
}

}  // namespace
}  // namespace koladata
