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
#include "benchmark/benchmark.h"
#include "absl/numeric/int128.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/op_utils/base62.h"

void BM_Encode(benchmark::State& state) {
  auto inputs = absl::Uint128Max();
  for (auto s : state) {
    benchmark::DoNotOptimize(inputs);
    auto result = koladata::internal::EncodeBase62(inputs);
    benchmark::DoNotOptimize(result);
  }
}

void BM_Decode(benchmark::State& state) {
  absl::string_view inputs = "7n42DGM5Tflk9n8mt7Fhc7";
  for (auto s : state) {
    benchmark::DoNotOptimize(inputs);
    auto result = koladata::internal::DecodeBase62(inputs);
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK(BM_Encode);

BENCHMARK(BM_Decode);
