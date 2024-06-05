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
#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "koladata/internal/benchmark_helpers.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/data_slice_accessors.h"
#include "koladata/internal/dense_source.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"

namespace koladata::internal {
namespace {

void BM_ObjAttributeAccessSingleSource(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  int64_t skip_size = state.range(1);
  int64_t total_size = batch_size * (skip_size + 1);
  auto ds = DataSliceImpl::AllocateEmptyObjects(total_size);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(total_size);
  std::shared_ptr<const DenseSource> source =
      DenseSource::CreateReadonly(
          AllocationId(ds.values<ObjectId>()[0].value), ds_a)
          .value();

  auto ds_filtered = FilterSingleAllocDataSlice(ds, batch_size, skip_size);

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(ds_filtered);
    benchmark::DoNotOptimize(source);

    DataSliceImpl ds_a_get =
        GetAttributeFromSources(ds_filtered, {source.get()}, {}).value();
    benchmark::DoNotOptimize(ds_a_get);
  }
}

BENCHMARK(BM_ObjAttributeAccessSingleSource)
    ->Apply(kBenchmarkObjectBatchPairsFn);

void BM_ObjAttributeAccessManySources(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  int64_t source_count = state.range(1);
  batch_size = ((batch_size - 1) / source_count + 1) * source_count;
  auto ds = DataSliceImpl::AllocateEmptyObjects(batch_size);
  auto ds_a = DataSliceImpl::AllocateEmptyObjects(batch_size);
  std::vector<std::shared_ptr<const DenseSource>> sources;
  for (int64_t i = 0; i < source_count; i++) {
    auto keep_ith = arolla::CreateDenseOp(
        [&](ObjectId obj) -> arolla::OptionalValue<ObjectId> {
          return {obj.Offset() % source_count == i, obj};
        });
    auto ds_a_filtered = DataSliceImpl::CreateWithAllocIds(
        ds_a.allocation_ids(), keep_ith(ds_a.values<ObjectId>()));
    sources.push_back(
        DenseSource::CreateReadonly(
            AllocationId(ds.values<ObjectId>()[0].value), ds_a_filtered)
            .value());
  }
  std::vector<const DenseSource*> sources_raw(sources.size());
  for (int64_t i = 0; i < sources.size(); i++) {
    sources_raw[i] = sources[i].get();
  }

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(ds);
    benchmark::DoNotOptimize(sources_raw);

    DataSliceImpl ds_a_get =
        GetAttributeFromSources(ds, sources_raw, {}).value();
    benchmark::DoNotOptimize(ds_a_get);
  }
}

BENCHMARK(BM_ObjAttributeAccessManySources)->RangePair(2, 100000, 2, 16);

void BM_Int32AttributeAccessSingleSource(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  int64_t skip_size = state.range(1);
  int64_t total_size = batch_size * (skip_size + 1);
  auto ds = DataSliceImpl::AllocateEmptyObjects(total_size);
  auto values_a =
      arolla::CreateFullDenseArray(std::vector<int32_t>(total_size, 57));
  auto alloc_id = AllocationId(ds.values<ObjectId>()[0].value);
  std::shared_ptr<const DenseSource> source =
      DenseSource::CreateReadonly(alloc_id, DataSliceImpl::Create(values_a))
          .value();

  auto ds_filtered = FilterSingleAllocDataSlice(ds, batch_size, skip_size);

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(ds_filtered);
    benchmark::DoNotOptimize(source);

    DataSliceImpl ds_a_get =
        GetAttributeFromSources(ds_filtered, {source.get()}, {}).value();
    benchmark::DoNotOptimize(ds_a_get);
  }
}

BENCHMARK(BM_Int32AttributeAccessSingleSource)
    ->Apply(kPointwiseBenchmarkObjectBatchPairsFn);

void BM_IsEquivalentTo(benchmark::State& state) {
  int64_t size = state.range(0);
  auto values = arolla::CreateFullDenseArray(std::vector<int>(size, 12));
  auto ds = DataSliceImpl::Create(values);
  for (auto _ : state) {
    auto equiv = ds.IsEquivalentTo(ds);
    benchmark::DoNotOptimize(equiv);
  }
}

BENCHMARK(BM_IsEquivalentTo)->Range(10, 100000);

}  // namespace
}  // namespace koladata::internal
