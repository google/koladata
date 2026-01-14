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
#include "absl/log/check.h"
#include "arolla/qtype/base_types.h"
#include "koladata/casting.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/op_utils/deep_op_benchmarks_util.h"
#include "koladata/internal/op_utils/deep_schema_compatible.h"

namespace koladata::internal {
namespace {

using benchmarks_utils::RunBenchmarksFn;

void RunCastingBenchmarks(benchmark::State& state, DataSliceImpl& ds,
                          DataItem& schema_a, DataBagImplPtr& databag_a,
                          DataBagImpl::FallbackSpan fallbacks_a,
                          DataItem& schema_b, DataBagImplPtr& databag_b,
                          DataBagImpl::FallbackSpan fallbacks_b) {
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(ds);
    benchmark::DoNotOptimize(schema_a);
    benchmark::DoNotOptimize(databag_a);
    benchmark::DoNotOptimize(fallbacks_a);
    benchmark::DoNotOptimize(schema_b);
    benchmark::DoNotOptimize(databag_b);
    benchmark::DoNotOptimize(fallbacks_b);
    auto result_db = DataBagImpl::CreateEmptyDatabag();
    DeepSchemaCompatibleOp(result_db.get(), {},
                           casting_internal::IsProbablyCastableTo)(
        schema_a, *databag_a, fallbacks_a, schema_b, *databag_b, fallbacks_b)
        .IgnoreError();
    benchmark::DoNotOptimize(result_db);
  }
}

void BM_TreeShapedIntToFloat(benchmark::State& state) {
  benchmarks_utils::BM_TreeShapedIntToFloat(state, RunCastingBenchmarks);
}
BENCHMARK(BM_TreeShapedIntToFloat)->Apply(benchmarks_utils::kTreesBenchmarkFn);

}  // namespace
}  // namespace koladata::internal
