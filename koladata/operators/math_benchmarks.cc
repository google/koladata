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
#include "arolla/dense_array/edge.h"
#include "arolla/util/init_arolla.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/math.h"

namespace koladata::internal {
namespace {

template <typename T>
DataSlice CreateFlatSlice(int64_t size) {
  arolla::DenseArrayBuilder<T> bldr(size);
  for (int64_t i = 0; i < size; ++i) {
    if (i % 8 != 1) {
      bldr.Set(i, i);
    }
  }

  return DataSlice::CreateWithFlatShape(
             DataSliceImpl::Create(std::move(bldr).Build()),
             internal::DataItem(schema::GetDType<T>()))
      .value();
}

template <typename T>
DataSlice Create2DSlice(int64_t group_count, int64_t group_size) {
  arolla::DenseArrayBuilder<int64_t> splits_bldr(group_count + 1);
  for (int64_t i = 0; i <= group_count; ++i) {
    splits_bldr.Set(i, i * group_size);
  }
  auto shape = DataSlice::JaggedShape::FromEdges(
                   {arolla::DenseArrayEdge::FromSplitPoints(
                        arolla::CreateDenseArray<int64_t>({0, group_count}))
                        .value(),
                    arolla::DenseArrayEdge::FromSplitPoints(
                        std::move(splits_bldr).Build())
                        .value()})
                   .value();

  arolla::DenseArrayBuilder<T> bldr(group_count * group_size);
  for (int64_t i = 0; i < group_count * group_size; ++i) {
    if (i % 8 != 1) {
      bldr.Set(i, i);
    }
  }

  return DataSlice::Create(DataSliceImpl::Create(std::move(bldr).Build()),
                           std::move(shape),
                           internal::DataItem(schema::GetDType<T>()))
      .value();
}

template <class T1, class T2, bool Arg1IsScalar, bool Arg2IsScalar>
void BM_Add(benchmark::State& state) {
  arolla::InitArolla();
  DataSlice a = Arg1IsScalar ? DataSlice::CreateFromScalar(1)
                             : CreateFlatSlice<T1>(state.range(0));
  DataSlice b = Arg2IsScalar ? DataSlice::CreateFromScalar(2)
                             : CreateFlatSlice<T2>(state.range(0));

  (void)ops::Add(a, b);  // warmup caches

  for (auto _ : state) {
    benchmark::DoNotOptimize(a);
    benchmark::DoNotOptimize(b);
    auto res = ops::Add(a, b).value();
    benchmark::DoNotOptimize(res);
  }
  if constexpr (!Arg1IsScalar || !Arg2IsScalar) {
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                            state.range(0));
  } else {
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
  }
}

// Add with broadcasting from 1D to 2D slice.
template <class T1, class T2>
void BM_AddFlatTo2D(benchmark::State& state) {
  arolla::InitArolla();
  DataSlice a = CreateFlatSlice<T1>(state.range(0));
  DataSlice b = Create2DSlice<T2>(state.range(0), state.range(1));

  (void)ops::Add(a, b);  // warmup caches

  for (auto _ : state) {
    benchmark::DoNotOptimize(a);
    benchmark::DoNotOptimize(b);
    auto res = ops::Add(a, b).value();
    benchmark::DoNotOptimize(res);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          state.range(0) * state.range(1));
}

void BM_AddScalarToScalarSameType(benchmark::State& state) {
  BM_Add<int32_t, int32_t, true, true>(state);
}
void BM_AddScalarToScalarOtherType(benchmark::State& state) {
  BM_Add<int32_t, double, true, true>(state);
}

void BM_AddFlatToScalarSameType(benchmark::State& state) {
  BM_Add<int32_t, int32_t, false, true>(state);
}
void BM_AddFlatToScalarOtherType(benchmark::State& state) {
  BM_Add<int32_t, double, false, true>(state);
}

void BM_AddFlatToFlatSameType(benchmark::State& state) {
  BM_Add<int32_t, int32_t, false, false>(state);
}
void BM_AddFlatToFlatOtherType(benchmark::State& state) {
  BM_Add<int32_t, double, false, false>(state);
}

void BM_AddFlatTo2DSameType(benchmark::State& state) {
  BM_AddFlatTo2D<int32_t, int32_t>(state);
}
void BM_AddFlatTo2DOtherType(benchmark::State& state) {
  BM_AddFlatTo2D<int32_t, double>(state);
}

BENCHMARK(BM_AddScalarToScalarSameType);
BENCHMARK(BM_AddScalarToScalarOtherType);

BENCHMARK(BM_AddFlatToScalarSameType)->Range(1, 10000);
BENCHMARK(BM_AddFlatToScalarOtherType)->Range(1, 10000);

BENCHMARK(BM_AddFlatToFlatSameType)->Range(1, 10000);
BENCHMARK(BM_AddFlatToFlatOtherType)->Range(1, 10000);

BENCHMARK(BM_AddFlatTo2DSameType)
    ->ArgPair(1, 1)
    ->ArgPair(8, 8)
    ->ArgPair(32, 32)
    ->ArgPair(100, 100);
BENCHMARK(BM_AddFlatTo2DOtherType)
    ->ArgPair(1, 1)
    ->ArgPair(8, 8)
    ->ArgPair(32, 32)
    ->ArgPair(100, 100);

}  // namespace
}  // namespace koladata::internal
