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
#include <array>

#include "benchmark/benchmark.h"
#include "koladata/functor/parallel/eager_executor.h"

namespace koladata::functor::parallel {
namespace {

template <int LambdaPayloadSize>
void BM_EagerExecutor_Schedule(benchmark::State& state) {
  using Payload = std::array<char, LambdaPayloadSize>;
  auto executor = GetEagerExecutor();
  for (auto _ : state) {
    executor->Schedule(
        [payload = Payload{}]() mutable { benchmark::DoNotOptimize(payload); });
  }
}

BENCHMARK(BM_EagerExecutor_Schedule<8>);
BENCHMARK(BM_EagerExecutor_Schedule<16>);
BENCHMARK(BM_EagerExecutor_Schedule<24>);
BENCHMARK(BM_EagerExecutor_Schedule<32>);
BENCHMARK(BM_EagerExecutor_Schedule<40>);

}  // namespace
}  // namespace koladata::functor::parallel
