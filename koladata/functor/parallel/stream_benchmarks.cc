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
#include <cstdint>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_qtype.h"
#include "koladata/functor/parallel/stream.h"
#include "koladata/test_utils.h"

namespace koladata::functor::parallel {
namespace {

template <typename T>
arolla::TypedValue MakeQValue();

template <>
arolla::TypedValue MakeQValue<int32_t>() {
  return arolla::TypedValue::FromValue(int32_t{17});
}

template <>
arolla::TypedValue MakeQValue<std::string>() {
  return arolla::TypedValue::FromValue(
      std::string("a moderately long text value, not fitting in SSO"));
}

template <>
arolla::TypedValue MakeQValue<DataSlice>() {
  return arolla::TypedValue::FromValue(test::DataItem(34));
}

// Writes `n` items into a new stream, closes it, and reads all items back.
void WriteAndReadStream(arolla::TypedRef qvalue, int n, int initial_capacity) {
  auto [stream, writer] = MakeStream(qvalue.GetType(), initial_capacity);
  for (int64_t i = 0; i < n; ++i) {
    writer->Write(qvalue);
  }
  std::move(*writer).Close();
  auto reader = stream->MakeReader();
  for (auto try_read_result = reader->TryRead();
       try_read_result.item() != nullptr; try_read_result = reader->TryRead()) {
    benchmark::DoNotOptimize(try_read_result);
  }
}

// Same as BM_Stream, with the default initial capacity.
template <typename T>
void BM_Stream(benchmark::State& state) {
  const auto n = state.range(0);
  const auto qvalue = MakeQValue<T>();
  for (auto _ : state) {
    WriteAndReadStream(qvalue.AsRef(), n, /*initial_capacity=*/0);
  }
  state.SetItemsProcessed(state.iterations() * n);
}

// Same as BM_Stream, with the specified initial capacity.
template <typename T>
void BM_Stream_ExactCapacity(benchmark::State& state) {
  const auto n = state.range(0);
  const auto qvalue = MakeQValue<T>();
  for (auto _ : state) {
    WriteAndReadStream(qvalue.AsRef(), n, /*initial_capacity=*/n);
  }
  state.SetItemsProcessed(state.iterations() * n);
}

#define STREAM_BENCHMARK_ARGS \
  Arg(0)->Arg(1)->Arg(2)->Arg(10)->Arg(100)->Arg(1000)

BENCHMARK(BM_Stream<int32_t>)->STREAM_BENCHMARK_ARGS;
BENCHMARK(BM_Stream<std::string>)->STREAM_BENCHMARK_ARGS;
BENCHMARK(BM_Stream<DataSlice>)->STREAM_BENCHMARK_ARGS;
BENCHMARK(BM_Stream_ExactCapacity<int32_t>)->STREAM_BENCHMARK_ARGS;
BENCHMARK(BM_Stream_ExactCapacity<std::string>)->STREAM_BENCHMARK_ARGS;
BENCHMARK(BM_Stream_ExactCapacity<DataSlice>)->STREAM_BENCHMARK_ARGS;

#undef STREAM_BENCHMARK_ARGS

}  // namespace
}  // namespace koladata::functor::parallel
