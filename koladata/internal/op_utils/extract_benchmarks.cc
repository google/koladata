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
#include "benchmark/benchmark.h"
#include "absl/status/status.h"
#include "arolla/qtype/base_types.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/op_utils/deep_op_benchmarks_util.h"
#include "koladata/internal/op_utils/extract.h"

namespace koladata::internal {
namespace {

using benchmarks_utils::RunBenchmarksFn;

void RunBenchmarks(benchmark::State& state, DataSliceImpl& ds, DataItem& schema,
                   DataBagImplPtr& databag,
                   DataBagImpl::FallbackSpan fallbacks) {
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(ds);
    benchmark::DoNotOptimize(schema);
    benchmark::DoNotOptimize(databag);
    benchmark::DoNotOptimize(fallbacks);
    auto result_db = DataBagImpl::CreateEmptyDatabag();
    ExtractOp(result_db.get())(ds, schema, *databag, fallbacks, nullptr, {})
        .IgnoreError();
    benchmark::DoNotOptimize(result_db);
  }
}

void BM_DisjointChains(benchmark::State& state) {
  benchmarks_utils::BM_DisjointChains(state, RunBenchmarks);
}
BENCHMARK(BM_DisjointChains)->Apply(benchmarks_utils::kBenchmarkFn);

void BM_DisjointChainsObjects(benchmark::State& state) {
  benchmarks_utils::BM_DisjointChainsObjects(state, RunBenchmarks);
}
BENCHMARK(BM_DisjointChainsObjects)->Apply(benchmarks_utils::kBenchmarkFn);

void BM_DAG(benchmark::State& state) {
  benchmarks_utils::BM_DAG(state, RunBenchmarks);
}
BENCHMARK(BM_DAG)->Apply(benchmarks_utils::kLayersBenchmarkFn);

void BM_DAGObjects(benchmark::State& state) {
  benchmarks_utils::BM_DAGObjects(state, RunBenchmarks);
}
BENCHMARK(BM_DAGObjects)->Apply(benchmarks_utils::kLayersBenchmarkFn);

inline void BM_ScalarPrimitive(benchmark::State& state) {
  auto input_db = DataBagImpl::CreateEmptyDatabag();
  auto result_db = DataBagImpl::CreateEmptyDatabag();
  ExtractOp op(result_db.get());
  DataItem item(1);
  DataItem schema(schema::kInt32);
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(item);
    benchmark::DoNotOptimize(schema);
    benchmark::DoNotOptimize(input_db);
    auto result_db = DataBagImpl::CreateEmptyDatabag();
    op(item, schema, *input_db, {}, nullptr, {}).IgnoreError();
    benchmark::DoNotOptimize(result_db);
  }
}

BENCHMARK(BM_ScalarPrimitive);

}  // namespace
}  // namespace koladata::internal
