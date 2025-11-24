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
#include <string>
#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/proto.h"
#include "koladata/proto/testing/test_proto2.pb.h"
#include "google/protobuf/text_format.h"

namespace koladata {
namespace {


std::string GetSmallProtoBytes() {
  koladata::testing::ExampleMessage message;
  CHECK(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        int32_field: -10
        float_field: 60.0
        bool_field: true
        string_field: "bar"
      )pb",
      &message));
  return message.SerializeAsString();
}


std::string GetBiggerProtoBytes() {
  koladata::testing::ExampleMessage message;
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
  return message.SerializeAsString();
}


void FromProtoBytesScalar(benchmark::State& state, std::string data) {
  DataSlice data_ds = DataSlice::CreatePrimitive<arolla::Bytes>(data);
  DataSlice proto_path_ds = DataSlice::CreatePrimitive(
      arolla::Text("koladata.testing.ExampleMessage"));

  for (auto _ : state) {
    benchmark::DoNotOptimize(data_ds);
    benchmark::DoNotOptimize(proto_path_ds);
    arolla::EvaluationContext ctx;
    auto res = ops::FromProtoBytes(&ctx, data_ds, proto_path_ds).value();
    benchmark::DoNotOptimize(res);
  }
}

void FromProtoBytesBatch(benchmark::State& state, std::string data) {
  DataSlice data_ds = DataSlice::CreateWithFlatShape(
                          internal::DataSliceImpl::Create(
                              arolla::CreateConstDenseArray<arolla::Bytes>(
                                  state.range(0), data)),
                          internal::DataItem(schema::kBytes))
                          .value();
  DataSlice proto_path_ds = DataSlice::CreatePrimitive(
      arolla::Text("koladata.testing.ExampleMessage"));

  for (auto _ : state) {
    benchmark::DoNotOptimize(data_ds);
    benchmark::DoNotOptimize(proto_path_ds);
    arolla::EvaluationContext ctx;
    auto res = ops::FromProtoBytes(&ctx, data_ds, proto_path_ds).value();
    benchmark::DoNotOptimize(res);
  }
}

void BM_FromSmallProtoBytesScalar(benchmark::State& state) {
  FromProtoBytesScalar(state, GetSmallProtoBytes());
}

void BM_FromBiggerProtoBytesScalar(benchmark::State& state) {
  FromProtoBytesScalar(state, GetBiggerProtoBytes());
}

void BM_FromSmallProtoBytesBatch(benchmark::State& state) {
  FromProtoBytesBatch(state, GetSmallProtoBytes());
}

void BM_FromBiggerProtoBytesBatch(benchmark::State& state) {
  FromProtoBytesBatch(state, GetBiggerProtoBytes());
}

BENCHMARK(BM_FromSmallProtoBytesScalar);
BENCHMARK(BM_FromBiggerProtoBytesScalar);
BENCHMARK(BM_FromSmallProtoBytesBatch)->Arg(1000);
BENCHMARK(BM_FromBiggerProtoBytesBatch)->Arg(1000);

}  // namespace
}  // namespace koladata
