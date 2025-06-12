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
#include <algorithm>
#include <cstdint>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/random/random.h"
#include "arolla/dense_array/testing/util.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/quote.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization/encode.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/object_id.h"

namespace koladata {
namespace {

using arolla::TypedValue;
using internal::DataItem;
using internal::DataSliceImpl;

auto DataItemsDiverseType() {
  std::vector<DataItem> items{
      DataItem(1),
      DataItem(2.f),
      DataItem(3l),
      DataItem(3.5),
      DataItem(),
      DataItem(internal::AllocateSingleObject()),
      DataItem(arolla::kUnit),
      DataItem(arolla::Text("abc")),
      DataItem(arolla::Bytes("cba")),
      DataItem(schema::kBytes),
      DataItem(arolla::expr::ExprQuote(arolla::expr::Leaf("x")))};
  std::vector<TypedValue> typed_values;
  typed_values.reserve(items.size());
  for (const DataItem& item : items) {
    typed_values.push_back(TypedValue::FromValue(item));
  }
  return typed_values;
}

void BM_DataItemSerialization(benchmark::State& state) {
  arolla::InitArolla();
  auto typed_values = DataItemsDiverseType();
  while (state.KeepRunningBatch(typed_values.size())) {
    benchmark::DoNotOptimize(typed_values);
    auto proto = arolla::serialization::Encode(typed_values, {});
    CHECK_OK(proto);
    benchmark::DoNotOptimize(proto);
  }
}

void BM_DataItemDeserialization(benchmark::State& state) {
  arolla::InitArolla();
  auto typed_values = DataItemsDiverseType();
  auto proto = *arolla::serialization::Encode(typed_values, {});
  while (state.KeepRunningBatch(typed_values.size())) {
    benchmark::DoNotOptimize(proto);
    auto decode_result = arolla::serialization::Decode(proto);
    CHECK_OK(decode_result);
    benchmark::DoNotOptimize(decode_result);
  }
}

BENCHMARK(BM_DataItemSerialization);
BENCHMARK(BM_DataItemDeserialization);

template <typename T>
auto RandomDataSlice(int64_t size) {
  if constexpr (std::is_same_v<T, internal::ObjectId>) {
    auto slice = DataSliceImpl::AllocateEmptyObjects(size);
    std::vector<DataItem> objects(slice.begin(), slice.end());
    std::shuffle(objects.begin(), objects.end(), absl::BitGen());
    return DataSliceImpl::Create(objects);
  } else {
    absl::BitGen gen;
    return DataSliceImpl::Create(
        arolla::testing::RandomDenseArray<T>(size, /*full=*/true, 0, gen));
  }
}

template <typename T>
void BM_DataSliceSerialization(benchmark::State& state) {
  arolla::InitArolla();
  int64_t size = state.range(0);
  absl::BitGen gen;
  std::vector<TypedValue> typed_values{
      TypedValue::FromValue(RandomDataSlice<T>(size))};
  while (state.KeepRunningBatch(size)) {
    benchmark::DoNotOptimize(typed_values);
    auto proto = arolla::serialization::Encode(typed_values, {});
    CHECK_OK(proto);
    benchmark::DoNotOptimize(proto);
  }
}

template <typename T>
void BM_DataSliceDeserialization(benchmark::State& state) {
  arolla::InitArolla();
  int64_t size = state.range(0);
  absl::BitGen gen;
  std::vector<TypedValue> typed_values{
      TypedValue::FromValue(RandomDataSlice<T>(size))};
  auto proto = *arolla::serialization::Encode(typed_values, {});
  while (state.KeepRunningBatch(size)) {
    auto decode_result = arolla::serialization::Decode(proto);
    CHECK_OK(decode_result);
    benchmark::DoNotOptimize(decode_result);
  }
}

constexpr int64_t kMaxSize = 10000;

BENCHMARK(BM_DataSliceSerialization<int64_t>)->Range(1, kMaxSize);
BENCHMARK(BM_DataSliceSerialization<float>)->Range(1, kMaxSize);
BENCHMARK(BM_DataSliceSerialization<arolla::Text>)->Range(1, kMaxSize);
BENCHMARK(BM_DataSliceSerialization<internal::ObjectId>)->Range(1, kMaxSize);

BENCHMARK(BM_DataSliceDeserialization<int64_t>)->Range(1, kMaxSize);
BENCHMARK(BM_DataSliceDeserialization<float>)->Range(1, kMaxSize);
BENCHMARK(BM_DataSliceDeserialization<arolla::Text>)->Range(1, kMaxSize);
BENCHMARK(BM_DataSliceDeserialization<internal::ObjectId>)->Range(1, kMaxSize);

// BENCHMARK(BM_DataItemDeserialization);

}  // namespace
}  // namespace koladata
