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
#include "arolla/dense_array/dense_array.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/unit.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/masking.h"

namespace koladata {
namespace {

void BM_HasNot_ScalarPresent(benchmark::State& state) {
  arolla::InitArolla();
  DataSlice ds = DataSlice::CreatePrimitive(std::move(arolla::kUnit));
  arolla::EvaluationContext ctx;
  (void)ops::HasNot(&ctx, ds);  // warmup caches

  for (auto _ : state) {
    benchmark::DoNotOptimize(ds);
    auto res = ops::HasNot(&ctx, ds).value();
    benchmark::DoNotOptimize(res);
  }
}

void BM_HasNot_ScalarMissing(benchmark::State& state) {
  arolla::InitArolla();
  DataSlice ds = DataSlice::UnsafeCreate(internal::DataItem(),
                                         internal::DataItem(schema::kMask));
  arolla::EvaluationContext ctx;
  (void)ops::HasNot(&ctx, ds);  // warmup caches

  for (auto _ : state) {
    benchmark::DoNotOptimize(ds);
    auto res = ops::HasNot(&ctx, ds).value();
    benchmark::DoNotOptimize(res);
  }
}

void BM_HasNot_FlatSliceMixed(benchmark::State& state) {
  arolla::InitArolla();
  int64_t size = state.range(0);
  arolla::DenseArrayBuilder<arolla::Unit> bldr(size);
  for (int64_t i = 0; i < size; ++i) {
    if (i % 2) {
      bldr.Set(i, arolla::kUnit);
    }
  }
  DataSlice ds = DataSlice::CreateWithFlatShape(
                     internal::DataSliceImpl::Create(std::move(bldr).Build()),
                     internal::DataItem(schema::kMask))
                     .value();
  arolla::EvaluationContext ctx;
  (void)ops::HasNot(&ctx, ds);  // warmup caches

  for (auto _ : state) {
    benchmark::DoNotOptimize(ds);
    auto res = ops::HasNot(&ctx, ds).value();
    benchmark::DoNotOptimize(res);
  }
}

void BM_HasNot_FlatSliceFull(benchmark::State& state) {
  arolla::InitArolla();
  int64_t size = state.range(0);
  auto da = arolla::CreateConstDenseArray<arolla::Unit>(size, arolla::kUnit);
  DataSlice ds = DataSlice::CreateWithFlatShape(
                     internal::DataSliceImpl::Create(std::move(da)),
                     internal::DataItem(schema::kMask))
                     .value();
  arolla::EvaluationContext ctx;
  (void)ops::HasNot(&ctx, ds);  // warmup caches

  for (auto _ : state) {
    benchmark::DoNotOptimize(ds);
    auto res = ops::HasNot(&ctx, ds).value();
    benchmark::DoNotOptimize(res);
  }
}

void BM_HasNot_FlatSliceEmpty(benchmark::State& state) {
  arolla::InitArolla();
  int64_t size = state.range(0);
  DataSlice ds = DataSlice::CreateWithFlatShape(
                     internal::DataSliceImpl::CreateEmptyAndUnknownType(size),
                     internal::DataItem(schema::kMask))
                     .value();
  arolla::EvaluationContext ctx;
  (void)ops::HasNot(&ctx, ds);  // warmup caches

  for (auto _ : state) {
    benchmark::DoNotOptimize(ds);
    auto res = ops::HasNot(&ctx, ds).value();
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_HasNot_ScalarPresent);
BENCHMARK(BM_HasNot_ScalarMissing);

BENCHMARK(BM_HasNot_FlatSliceMixed)->Range(1, 10000);
BENCHMARK(BM_HasNot_FlatSliceFull)->Range(1, 10000);
BENCHMARK(BM_HasNot_FlatSliceEmpty)->Range(1, 10000);

}  // namespace
}  // namespace koladata
