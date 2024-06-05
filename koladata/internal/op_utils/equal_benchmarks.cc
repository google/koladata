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
#include <utility>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/random/random.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/op_utils/equal.h"
#include "arolla/dense_array/testing/util.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/bool/comparison.h"
#include "arolla/qexpr/operators/dense_array/lifter.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/meta.h"

namespace koladata::internal {
namespace {

struct DataSliceOp {};
struct DenseArrayOp {};

using ::arolla::testing::RandomDenseArray;

constexpr auto kBenchmarkFn = [](auto* b) {
  b->Arg(1)->Arg(10)->Arg(10000)->Arg(1000000);
};

template <typename Access, typename T>
void run_benchmarks(benchmark::State& state, DataSliceImpl& ds_a,
                    DataSliceImpl& ds_b) {
  while (state.KeepRunning()) {
    if constexpr (std::is_same_v<Access, DataSliceOp>) {
      benchmark::DoNotOptimize(ds_a);
      benchmark::DoNotOptimize(ds_b);
      auto ds_ab = EqualOp()(ds_a, ds_b).value();
      benchmark::DoNotOptimize(ds_ab);
    } else if constexpr (std::is_same_v<Access, DenseArrayOp>) {
      benchmark::DoNotOptimize(ds_a);
      benchmark::DoNotOptimize(ds_b);
      arolla::EvaluationContext ctx;
      auto ab = arolla::DenseArrayLifter<
          arolla::EqualOp,
          arolla::meta::type_list<T, T>>()(
              &ctx, ds_a.values<T>(), ds_b.values<T>())
              .value();
      auto ds_ab = DataSliceImpl::Create(ab);
      benchmark::DoNotOptimize(ds_ab);
      benchmark::DoNotOptimize(ab);
    }
  }
}

template <typename Access>
void BM_float(benchmark::State& state) {
  int64_t total_size = state.range(0);
  absl::BitGen gen;
  auto values_a = RandomDenseArray<float>(total_size, /*full=*/false, 0, gen);
  auto values_b = RandomDenseArray<float>(total_size, /*full=*/false, 0, gen);

  auto ds_a = DataSliceImpl::Create(values_a);
  auto ds_b = DataSliceImpl::Create(values_b);

  run_benchmarks<Access, float>(state, ds_a, ds_b);
}

template <typename Access>
void BM_float_int32(benchmark::State& state) {
  int64_t total_size = state.range(0);
  absl::BitGen gen;
  auto values_a = RandomDenseArray<float>(total_size, /*full=*/false, 0, gen);
  auto values_b = RandomDenseArray<int>(total_size, /*full=*/false, 0, gen);

  auto ds_a = DataSliceImpl::Create(values_a);
  auto ds_b = DataSliceImpl::Create(values_b);

  run_benchmarks<Access, float>(state, ds_a, ds_b);
}

template <typename Access>
void BM_int32_int64(benchmark::State& state) {
  int64_t total_size = state.range(0);
  absl::BitGen gen;
  auto values_a = RandomDenseArray<int>(total_size, /*full=*/false, 0, gen);
  auto values_b = RandomDenseArray<int64_t>(total_size, /*full=*/false, 0, gen);

  auto ds_a = DataSliceImpl::Create(values_a);
  auto ds_b = DataSliceImpl::Create(values_b);

  run_benchmarks<Access, float>(state, ds_a, ds_b);
}

BENCHMARK(BM_float<DataSliceOp>)->Apply(kBenchmarkFn);
BENCHMARK(BM_float<DenseArrayOp>)->Apply(kBenchmarkFn);

BENCHMARK(BM_float_int32<DataSliceOp>)->Apply(kBenchmarkFn);
BENCHMARK(BM_int32_int64<DataSliceOp>)->Apply(kBenchmarkFn);

}  // namespace
}  // namespace koladata::internal
