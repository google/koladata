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
#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"
#include "absl/functional/any_invocable.h"
#include "koladata/functor/parallel/context_guard.h"
#include "koladata/functor/parallel/executor.h"

namespace koladata::functor::parallel {
namespace {

class TestEagerExecutor final : public Executor {
 public:
  TestEagerExecutor() noexcept = default;
  explicit TestEagerExecutor(absl::AnyInvocable<void(ContextGuard&) const>
                                 context_guard_initializer) noexcept
      : Executor(std::move(context_guard_initializer)) {}

  void DoSchedule(TaskFn task_fn) noexcept final { std::move(task_fn)(); }

  std::string Repr() const noexcept final { return "test_eager_executor"; }
};

template <int LambdaPayloadSize>
void BM_EagerExecutor_Schedule(benchmark::State& state) {
  using Payload = std::array<char, LambdaPayloadSize>;
  auto executor = std::make_shared<TestEagerExecutor>();
  for (auto _ : state) {
    executor->Schedule(
        [payload = Payload{}]() mutable { benchmark::DoNotOptimize(payload); });
  }
}

void BM_EagerExecutor_Schedule_WithContextGuard(benchmark::State& state) {
  ExecutorPtr some_executor = std::make_shared<TestEagerExecutor>();
  auto executor = std::make_shared<TestEagerExecutor>(
      [some_executor = std::move(some_executor)](ContextGuard& context_guard) {
        context_guard.init<int>();
      });
  for (auto _ : state) {
    executor->Schedule([]() mutable { benchmark::DoNotOptimize(true); });
  }
}

BENCHMARK(BM_EagerExecutor_Schedule<8>);
BENCHMARK(BM_EagerExecutor_Schedule<16>);
BENCHMARK(BM_EagerExecutor_Schedule<24>);
BENCHMARK(BM_EagerExecutor_Schedule<32>);
BENCHMARK(BM_EagerExecutor_Schedule<40>);

BENCHMARK(BM_EagerExecutor_Schedule_WithContextGuard);

}  // namespace
}  // namespace koladata::functor::parallel
