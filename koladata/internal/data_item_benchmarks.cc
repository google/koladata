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

#include "benchmark/benchmark.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/text.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {
namespace {

void BM_AllocateEmptyObject(benchmark::State& state) {
  for (auto _ : state) {
    DataItem item = DataItem(AllocateSingleObject());
    benchmark::DoNotOptimize(item);
  }
}

BENCHMARK(BM_AllocateEmptyObject);

template <class T>
void BM_AllocatePrimitive(benchmark::State& state) {
  T value = T();
  for (auto _ : state) {
    benchmark::DoNotOptimize(value);
    DataItem item(value);
    benchmark::DoNotOptimize(item);
  }
}

BENCHMARK(BM_AllocatePrimitive<float>);
BENCHMARK(BM_AllocatePrimitive<int64_t>);
BENCHMARK(BM_AllocatePrimitive<arolla::Text>);

void BM_AllocateLongText(benchmark::State& state) {
  arolla::Text value("string that longer than short string optimization");
  for (auto _ : state) {
    benchmark::DoNotOptimize(value);
    DataItem item(value);
    benchmark::DoNotOptimize(item);
  }
}

BENCHMARK(BM_AllocateLongText);

}  // namespace
}  // namespace koladata::internal
