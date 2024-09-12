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
#include <cstdint>
#include <memory>

#include "benchmark/benchmark.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dict.h"

namespace koladata::internal {
namespace {

void BM_SizeNoFallbacksNoParent(benchmark::State& state) {
  int64_t key_count = state.range(0);
  Dict dict;
  for (int64_t i = 0; i < key_count; ++i) {
    dict.Set(i, DataItem(i));
  }
  for (auto _ : state) {
    benchmark::DoNotOptimize(dict);
    int64_t size = dict.GetSizeNoFallbacks();
    benchmark::DoNotOptimize(size);
  }
}

void BM_SizeNoFallbacksWithParent(benchmark::State& state) {
  int64_t key_count = state.range(0);
  std::shared_ptr<DictVector> base_dict_vector =
      std::make_shared<DictVector>(1);
  auto& base_dict = (*base_dict_vector)[0];
  for (int64_t i = 0; i < key_count; i += 2) {
    base_dict.Set(i, DataItem(i));
  }
  DictVector dict_vector(base_dict_vector);
  auto& dict = dict_vector[0];
  for (int64_t i = 1; i < key_count; i += 2) {
    dict.Set(i, DataItem(i));
  }
  for (auto _ : state) {
    benchmark::DoNotOptimize(dict);
    int64_t size = dict.GetSizeNoFallbacks();
    benchmark::DoNotOptimize(size);
  }
}

void BM_GetKeys(benchmark::State& state) {
  int64_t key_count = state.range(0);
  Dict dict;
  for (int64_t i = 0; i < key_count; ++i) {
    dict.Set(i, DataItem(i));
  }
  for (auto _ : state) {
    benchmark::DoNotOptimize(dict);
    auto keys = dict.GetKeys();
    benchmark::DoNotOptimize(keys);
  }
  state.SetItemsProcessed(state.iterations() * key_count);
}

void BM_GetKeysAfterRemoval(benchmark::State& state) {
  int64_t key_count = state.range(0);
  Dict dict;
  for (int64_t i = 0; i < key_count; ++i) {
    dict.Set(i, DataItem(i));
  }
  for (int64_t i = 0; i < key_count; i += 10) {
    dict.Set(i, DataItem());
  }
  for (auto _ : state) {
    benchmark::DoNotOptimize(dict);
    auto keys = dict.GetKeys();
    benchmark::DoNotOptimize(keys);
  }
  state.SetItemsProcessed(state.iterations() * key_count);
}

void BM_GetKeysDerived(benchmark::State& state) {
  int64_t key_count = state.range(0);
  std::shared_ptr<DictVector> base_dict_vector =
      std::make_shared<DictVector>(1);
  auto& base_dict = (*base_dict_vector)[0];
  for (int64_t i = 0; i < key_count; i += 2) {
    base_dict.Set(i, DataItem(i));
  }
  DictVector dict_vector(base_dict_vector);
  auto& dict = dict_vector[0];
  for (int64_t i = 1; i < key_count; i += 2) {
    dict.Set(i, DataItem(i));
  }

  for (auto _ : state) {
    benchmark::DoNotOptimize(dict);
    auto keys = dict.GetKeys();
    benchmark::DoNotOptimize(keys);
  }
  state.SetItemsProcessed(state.iterations() * key_count);
}

void BM_GetKeysFallback(benchmark::State& state) {
  int64_t key_count = state.range(0);
  Dict base_dict;
  for (int64_t i = 0; i < key_count; i += 2) {
    base_dict.Set(i, DataItem(i));
  }
  Dict fb_dict;
  for (int64_t i = 1; i < key_count; i += 2) {
    fb_dict.Set(i, DataItem(i));
  }

  for (auto _ : state) {
    benchmark::DoNotOptimize(base_dict);
    benchmark::DoNotOptimize(fb_dict);
    auto keys = base_dict.GetKeys({&fb_dict});
    benchmark::DoNotOptimize(keys);
  }
  state.SetItemsProcessed(state.iterations() * key_count);
}

BENCHMARK(BM_SizeNoFallbacksNoParent)->Arg(4)->Arg(10)->Arg(100);
BENCHMARK(BM_SizeNoFallbacksWithParent)->Arg(4)->Arg(10)->Arg(100);
BENCHMARK(BM_GetKeys)->Arg(1)->Arg(10)->Arg(100);
BENCHMARK(BM_GetKeysAfterRemoval)->Arg(1)->Arg(10)->Arg(100);
BENCHMARK(BM_GetKeysDerived)->Arg(1)->Arg(10)->Arg(100);
BENCHMARK(BM_GetKeysFallback)->Arg(1)->Arg(10)->Arg(100);

}  // namespace
}  // namespace koladata::internal
