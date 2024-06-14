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
#include <utility>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/random/random.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/op_utils/presence_and.h"
#include "arolla/dense_array/testing/util.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators/dense_array/logic_ops.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/unit.h"

namespace koladata::internal {
namespace {

struct DataSliceOp {};
struct DataItemOp {};
struct DenseArrayOp {};

using ::arolla::testing::RandomDenseArray;

constexpr auto kBenchmarkFn = [](auto* b) {
  b->Arg(1)->Arg(10)->Arg(10000)->Arg(1000000);
};

template <typename Access>
void BM_float(benchmark::State& state) {
  int64_t total_size = state.range(0);
  absl::BitGen gen;
  auto values = RandomDenseArray<float>(total_size, /*full=*/false, 0, gen);
  auto values_b = RandomDenseArray<float>(total_size, /*full=*/false, 0, gen);
  auto mask = arolla::DenseArrayHasOp()(values_b);

  auto ds_values = DataSliceImpl::Create(values);
  auto ds_mask = DataSliceImpl::Create(mask);

  while (state.KeepRunning()) {
    if constexpr (std::is_same_v<Access, DataSliceOp>) {
      benchmark::DoNotOptimize(ds_values);
      benchmark::DoNotOptimize(ds_mask);
      auto ds_filtered = PresenceAndOp()(ds_values, ds_mask).value();
      benchmark::DoNotOptimize(ds_filtered);
    } else if constexpr (std::is_same_v<Access, DataItemOp>) {
      benchmark::DoNotOptimize(ds_values);
      benchmark::DoNotOptimize(ds_mask);
      DataSliceImpl::Builder builder(total_size);
      for (int64_t i = 0; i < total_size; ++i) {
        builder.Insert(i, PresenceAndOp()(ds_values[i], ds_mask[i]).value());
      }
      benchmark::DoNotOptimize(builder);
      auto ds_filtered = std::move(builder).Build();
      benchmark::DoNotOptimize(ds_filtered);
    } else if constexpr (std::is_same_v<Access, DenseArrayOp>) {
      benchmark::DoNotOptimize(ds_values);
      benchmark::DoNotOptimize(ds_mask);
      arolla::EvaluationContext ctx;
      auto filtered =
          arolla::DenseArrayPresenceAndOp()(&ctx, ds_values.values<float>(),
                                           ds_mask.values<arolla::Unit>())
              .value();
      auto ds_filtered = DataSliceImpl::Create(filtered);
      benchmark::DoNotOptimize(ds_filtered);
      benchmark::DoNotOptimize(filtered);
    }
  }
}

BENCHMARK(BM_float<DataSliceOp>)->Apply(kBenchmarkFn);
BENCHMARK(BM_float<DenseArrayOp>)->Apply(kBenchmarkFn);
BENCHMARK(BM_float<DataItemOp>)->Apply(kBenchmarkFn);

}  // namespace
}  // namespace koladata::internal
