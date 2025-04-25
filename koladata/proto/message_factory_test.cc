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
#include "koladata/proto/message_factory.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "koladata/proto/testing/test_proto2.pb.h"
#include "koladata/proto/testing/test_proto3.pb.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata {
namespace {

using ::absl_testing::StatusIs;

TEST(ToProtoTest, CreateMessagePrototype) {
  auto message = CreateProtoMessagePrototype("koladata.testing.ExampleMessage");
  ASSERT_NE(message, nullptr);
  EXPECT_EQ(message->GetDescriptor()->full_name(),
            "koladata.testing.ExampleMessage");
  EXPECT_EQ(message->GetDescriptor(),
            koladata::testing::ExampleMessage::descriptor());
  EXPECT_EQ(CreateProtoMessagePrototype("koladata.prod.ExampleMessage"),
            nullptr);
}

TEST(ToProtoTest, DeserializeProtoByName) {
  koladata::testing::ExampleMessage orig;
  orig.set_int32_field(123);
  ASSERT_OK_AND_ASSIGN(auto message,
                       DeserializeProtoByName("koladata.testing.ExampleMessage",
                                              orig.SerializeAsString()));
  ASSERT_NE(message, nullptr);
  EXPECT_EQ(message->GetDescriptor()->full_name(),
            "koladata.testing.ExampleMessage");
  EXPECT_EQ(message->GetDescriptor(),
            koladata::testing::ExampleMessage::descriptor());

  auto* new_proto =
      dynamic_cast<koladata::testing::ExampleMessage*>(message.get());
  ASSERT_NE(new_proto, nullptr);
  EXPECT_EQ(new_proto->int32_field(), 123);

  EXPECT_THAT(
      DeserializeProtoByName("koladata.prod.ExampleMessage",
                             orig.SerializeAsString()),
      StatusIs(absl::StatusCode::kNotFound,
               "failed to create proto message koladata.prod.ExampleMessage"));
  EXPECT_THAT(
      DeserializeProtoByName("koladata.testing.ExampleMessage",
                             "It is not a good proto!"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          "failed to parse proto message koladata.testing.ExampleMessage"));
}

}  // namespace
}  // namespace koladata
