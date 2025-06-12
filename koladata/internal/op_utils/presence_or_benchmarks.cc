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
#include <utility>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/random/random.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/logic_ops.h"
#include "arolla/qtype/base_types.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/op_utils/benchmark_util.h"
#include "koladata/internal/op_utils/presence_or.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::internal {
namespace {

struct DataSliceOp {};
struct DataItemOp {};
struct DenseArrayOp {};

constexpr auto kBenchmarkFn = [](auto* b) {
  b->Arg(1)->Arg(10)->Arg(10000)->Arg(1000000);
};

template <typename Access, typename T>
void run_benchmarks(benchmark::State& state, DataSliceImpl& ds_a,
                    DataSliceImpl& ds_b) {
  int64_t total_size = ds_a.size();
  while (state.KeepRunning()) {
    if constexpr (std::is_same_v<Access, DataSliceOp>) {
      benchmark::DoNotOptimize(ds_a);
      benchmark::DoNotOptimize(ds_b);
      auto ds_ab = PresenceOrOp</*disjoint=*/false>()(ds_a, ds_b).value();
      benchmark::DoNotOptimize(ds_ab);
    } else if constexpr (std::is_same_v<Access, DataItemOp>) {
      benchmark::DoNotOptimize(ds_a);
      benchmark::DoNotOptimize(ds_b);
      SliceBuilder builder(total_size);
      for (int64_t i = 0; i < total_size; ++i) {
        builder.InsertIfNotSetAndUpdateAllocIds(
            i, PresenceOrOp</*disjoint=*/false>()(ds_a[i], ds_b[i]).value());
      }
      benchmark::DoNotOptimize(builder);
      auto ds_ab = std::move(builder).Build();
      benchmark::DoNotOptimize(ds_ab);
    } else if constexpr (std::is_same_v<Access, DenseArrayOp>) {
      benchmark::DoNotOptimize(ds_a);
      benchmark::DoNotOptimize(ds_b);
      arolla::EvaluationContext ctx;
      auto ab = arolla::DenseArrayPresenceOrOp()(&ctx, ds_a.values<T>(),
                                                 ds_b.values<T>())
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
  auto values_a =
      RandomNonEmptyDenseArray<float>(total_size, /*full=*/false, 0, gen);
  auto values_b =
      RandomNonEmptyDenseArray<float>(total_size, /*full=*/false, 0, gen);

  auto ds_a = DataSliceImpl::Create(values_a);
  auto ds_b = DataSliceImpl::Create(values_b);

  run_benchmarks<Access, float>(state, ds_a, ds_b);
}

template <typename Access>
void BM_float_int32(benchmark::State& state) {
  int64_t total_size = state.range(0);
  absl::BitGen gen;
  auto values_a =
      RandomNonEmptyDenseArray<float>(total_size, /*full=*/false, 0, gen);
  auto values_b =
      RandomNonEmptyDenseArray<int32_t>(total_size, /*full=*/false, 0, gen);

  auto ds_a = DataSliceImpl::Create(values_a);
  auto ds_b = DataSliceImpl::Create(values_b);

  run_benchmarks<Access, float>(state, ds_a, ds_b);
}

BENCHMARK(BM_float<DataSliceOp>)->Apply(kBenchmarkFn);
BENCHMARK(BM_float<DenseArrayOp>)->Apply(kBenchmarkFn);
BENCHMARK(BM_float<DataItemOp>)->Apply(kBenchmarkFn);

BENCHMARK(BM_float_int32<DataSliceOp>)->Apply(kBenchmarkFn);
BENCHMARK(BM_float_int32<DataItemOp>)->Apply(kBenchmarkFn);

void BM_DataSliceOrDataItem(benchmark::State& state) {
  int64_t total_size = state.range(0);
  absl::BitGen gen;
  auto values_a =
      RandomNonEmptyDenseArray<float>(total_size, /*full=*/false, 0, gen);
  auto ds_a = DataSliceImpl::Create(values_a);

  auto di_b = DataItem(3.14);
  for (auto _ : state) {
    benchmark::DoNotOptimize(ds_a);
    benchmark::DoNotOptimize(di_b);
    auto res_or = PresenceOrOp</*disjoint=*/false>()(ds_a, di_b);
    benchmark::DoNotOptimize(res_or);
  }
}

BENCHMARK(BM_DataSliceOrDataItem)->Apply(kBenchmarkFn);

}  // namespace
}  // namespace koladata::internal
