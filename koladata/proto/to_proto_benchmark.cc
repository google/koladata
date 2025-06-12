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
#include "koladata/proto/to_proto.h"

#include <cstddef>
#include <vector>

#include "benchmark/benchmark.h"
#include "gmock/gmock.h"
#include "absl/log/check.h"
#include "koladata/data_bag.h"
#include "koladata/proto/from_proto.h"
#include "koladata/proto/testing/test_proto2.pb.h"
#include "koladata/proto/testing/test_proto3.pb.h"
#include "google/protobuf/text_format.h"

namespace koladata {
namespace {

void BM_ToProto(benchmark::State& state) {
  koladata::testing::ExampleMessage message;
  // Note: 18446744073709551606 = 2**64 - 10, to test uint64 round-trip.
  CHECK(google::protobuf::TextFormat::ParseFromString(
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
      &message));

  std::vector<koladata::testing::ExampleMessage> messages(state.range(0),
                                                          message);
  std::vector<const google::protobuf::Message*> message_ptrs(state.range(0));
  for (size_t i = 0; i < messages.size(); ++i) {
    messages[i].set_int32_field(i);
    message_ptrs[i] = &messages[i];
  }

  auto db = DataBag::Empty();
  ASSERT_OK_AND_ASSIGN(auto slice, FromProto(db, message_ptrs, {}));

  for (auto _ : state) {
    std::vector<koladata::testing::ExampleMessage> output_messages(
        state.range(0));
    std::vector<google::protobuf::Message*> output_message_ptrs(state.range(0));
    for (size_t i = 0; i < output_messages.size(); ++i) {
      output_message_ptrs[i] = &output_messages[i];
    }
    ASSERT_OK(ToProto(slice, output_message_ptrs));
  }
}

BENCHMARK(BM_ToProto)->Range(1, 5000);

}  // namespace
}  // namespace koladata
