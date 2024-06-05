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
#include <type_traits>
#include <utility>

#include "benchmark/benchmark.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "koladata/internal/benchmark_helpers.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dense_source.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/util/bytes.h"
#include "arolla/util/view_types.h"

namespace koladata::internal {
namespace {

struct BatchAccess {};
struct PointwiseAccess {};

template <typename Access>
void BM_ObjAttributeAccess(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  int64_t skip_size = state.range(1);
  int64_t total_size = batch_size * (skip_size + 1);
  AllocationId alloc_obj = Allocate(total_size);

  AllocationId alloc_attr = Allocate(total_size);
  arolla::DenseArrayBuilder<ObjectId> builder_attr(total_size);
  for (int64_t i = 0; i < total_size; ++i) {
    builder_attr.Set(i, alloc_attr.ObjectByOffset(i));
  }
  arolla::DenseArray<ObjectId> attr = std::move(builder_attr).Build();

  std::shared_ptr<const DenseSource> ds =
      DenseSource::CreateReadonly(
          alloc_obj,
          DataSliceImpl::CreateWithAllocIds(AllocationIdSet(alloc_attr), attr))
          .value();

  arolla::DenseArrayBuilder<ObjectId> builder_obj(batch_size);
  for (int64_t i = 0; i < batch_size; ++i) {
    builder_obj.Set(i, alloc_obj.ObjectByOffset(i * (skip_size + 1)));
  }
  arolla::DenseArray<ObjectId> objs = std::move(builder_obj).Build();

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(objs);
    benchmark::DoNotOptimize(ds);
    if constexpr (std::is_same_v<Access, BatchAccess>) {
      auto res = ds->Get(objs, /*check_alloc_id`*/false);
      benchmark::DoNotOptimize(res);
    } else {
      for (int64_t i = 0; i != batch_size; ++i) {
        auto res = ds->Get(objs.values[i]);
        benchmark::DoNotOptimize(res);
      }
    }
  }
}

BENCHMARK(BM_ObjAttributeAccess<BatchAccess>)
    ->Apply(kBenchmarkObjectBatchPairsFn);
BENCHMARK(BM_ObjAttributeAccess<PointwiseAccess>)
    ->Apply(kPointwiseBenchmarkObjectBatchPairsFn);

template <typename T, typename Access>
void BM_AttributeAccess(benchmark::State& state) {
  int64_t batch_size = state.range(0);
  int64_t skip_size = state.range(1);
  int64_t total_size = batch_size * (skip_size + 1);
  AllocationId alloc_obj = Allocate(total_size);

  arolla::DenseArrayBuilder<T> builder_attr(total_size);
  for (int64_t i = 0; i < total_size; ++i) {
    if constexpr (std::is_same_v<absl::string_view, arolla::view_type_t<T>>) {
      builder_attr.Set(i, absl::StrCat(1000000000 + i));
    } else {
      builder_attr.Set(i, i);
    }
  }
  arolla::DenseArray<T> attr = std::move(builder_attr).Build();

  auto ds = DenseSource::CreateReadonly(alloc_obj, DataSliceImpl::Create(attr))
                .value();

  arolla::DenseArrayBuilder<ObjectId> builder_obj(batch_size);
  for (int64_t i = 0; i < batch_size; ++i) {
    builder_obj.Set(i, alloc_obj.ObjectByOffset(i * (skip_size + 1)));
  }
  arolla::DenseArray<ObjectId> objs = std::move(builder_obj).Build();

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(objs);
    benchmark::DoNotOptimize(ds);
    if constexpr (std::is_same_v<Access, BatchAccess>) {
      auto res = ds->Get(objs, /*check_alloc_id`*/false);
      benchmark::DoNotOptimize(res);
    } else {
      for (int64_t i = 0; i != batch_size; ++i) {
        auto res = ds->Get(objs.values[i]);
        benchmark::DoNotOptimize(res);
      }
    }
  }
}

BENCHMARK(BM_AttributeAccess<int32_t, BatchAccess>)
    ->Apply(kBenchmarkPrimitiveBatchPairsFn);
BENCHMARK(BM_AttributeAccess<int32_t, PointwiseAccess>)
    ->Apply(kPointwiseBenchmarkPrimitiveBatchPairsFn);

BENCHMARK(BM_AttributeAccess<arolla::Bytes, BatchAccess>)
    ->Apply(kBenchmarkPrimitiveBatchPairsFn);
BENCHMARK(BM_AttributeAccess<arolla::Bytes, PointwiseAccess>)
    ->Apply(kPointwiseBenchmarkPrimitiveBatchPairsFn);

}  // namespace
}  // namespace koladata::internal
