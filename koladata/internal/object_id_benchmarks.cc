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
#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/random/random.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/buffer.h"
#include "arolla/qtype/base_types.h"
#include "koladata/internal/benchmark_helpers.h"
#include "koladata/internal/object_id.h"

namespace koladata::internal {
namespace {

void BM_Equality(benchmark::State& state) {
  ObjectId id1 = AllocateSingleObject();
  ObjectId id2 = AllocateSingleObject();
  for (auto _ : state) {
    benchmark::DoNotOptimize(id1);
    benchmark::DoNotOptimize(id2);
    bool eq = id1 == id2;
    benchmark::DoNotOptimize(eq);
  }
}

BENCHMARK(BM_Equality);

void BM_AllocateSingleObject(benchmark::State& state) {
  for (auto _ : state) {
    ObjectId id = AllocateSingleObject();
    benchmark::DoNotOptimize(id);
  }
}

BENCHMARK(BM_AllocateSingleObject);

void BM_Allocate2PowerNObjects(benchmark::State& state) {
  int64_t n = (1ull << state.range(0));
  for (auto _ : state) {
    benchmark::DoNotOptimize(n);
    AllocationId id = Allocate(n);
    benchmark::DoNotOptimize(id);
  }
}

BENCHMARK(BM_Allocate2PowerNObjects)->Arg(0)->Arg(1)->Arg(8)->Arg(40);

void BM_AccessDenseArrayByOffset(benchmark::State& state) {
  int batch_size = state.range(0);
  int skip_size = state.range(1);
  int total_size = batch_size * (skip_size + 1);
  AllocationId alloc_obj = Allocate(total_size);

  arolla::DenseArrayBuilder<ObjectId> builder_obj(batch_size);
  for (int64_t i = 0; i < batch_size; ++i) {
    builder_obj.Set(i, alloc_obj.ObjectByOffset(i * (skip_size + 1)));
  }
  arolla::DenseArray<ObjectId> objs = std::move(builder_obj).Build();

  AllocationId alloc_attr = Allocate(total_size);
  arolla::DenseArrayBuilder<ObjectId> builder_attr(total_size);
  for (int64_t i = 0; i < total_size; ++i) {
    builder_attr.Set(i, alloc_attr.ObjectByOffset(i));
  }
  arolla::DenseArray<ObjectId> attr = std::move(builder_attr).Build();

  while (state.KeepRunningBatch(batch_size)) {
    benchmark::DoNotOptimize(objs);
    benchmark::DoNotOptimize(attr);
    benchmark::DoNotOptimize(alloc_obj);
    arolla::bitmap::AlmostFullBuilder bitmap_builder(objs.size());
    typename arolla::Buffer<ObjectId>::Builder values_builder(objs.size());
    const ObjectId* values = attr.values.span().data();
    objs.ForEach([&](int64_t id, bool present, const ObjectId& obj) {
      bool res_present = false;
      if (present && alloc_obj.Contains(obj)) {
        int64_t offset = obj.Offset();
        res_present = attr.present(offset);
        values_builder.Set(id, values[offset]);
      }
      if (!res_present) {
        bitmap_builder.AddMissed(id);
      }
    });
    arolla::DenseArray<ObjectId> get_attr{std::move(values_builder).Build(),
                                          std::move(bitmap_builder).Build()};
    benchmark::DoNotOptimize(get_attr);
  }
}

BENCHMARK(BM_AccessDenseArrayByOffset)->Apply(kBenchmarkObjectBatchPairsFn);

void BM_AllocateAndIterateInThreads(benchmark::State& state) {
  int64_t size = state.range(0);
  for (auto s : state) {
    benchmark::DoNotOptimize(size);
    AllocationId alloc_id = Allocate(size);
    for (int64_t i = 0; i < size; ++i) {
      ObjectId id = alloc_id.ObjectByOffset(i);
      benchmark::DoNotOptimize(id);
    }
    benchmark::DoNotOptimize(alloc_id);
  }
}

BENCHMARK(BM_AllocateAndIterateInThreads)
    ->Range(2, 20)
    ->Threads(1)
    ->Threads(2)
    ->Threads(32);

void BM_AllocationIdSetInsertManySame(benchmark::State& state) {
  int64_t size = state.range(0);
  AllocationId alloc_id = Allocate(size);
  for (auto s : state) {
    benchmark::DoNotOptimize(size);
    AllocationIdSet alloc_id_set;
    for (int64_t i = 0; i < size; ++i) {
      benchmark::DoNotOptimize(alloc_id);
      alloc_id_set.Insert(alloc_id);
    }
    benchmark::DoNotOptimize(alloc_id_set);
  }
}

BENCHMARK(BM_AllocationIdSetInsertManySame)->Range(1, 2048);

void BM_AllocationIdSetInsertManyDifferent(benchmark::State& state) {
  int64_t size = state.range(0);
  std::vector<AllocationId> alloc_ids;
  alloc_ids.reserve(size);
  for (int64_t i = 0; i < size; ++i) {
    alloc_ids.push_back(Allocate(57));
  }
  std::shuffle(alloc_ids.begin(), alloc_ids.end(), absl::BitGen());
  for (auto s : state) {
    benchmark::DoNotOptimize(size);
    benchmark::DoNotOptimize(alloc_ids);
    AllocationIdSet alloc_id_set;
    for (int64_t i = 0; i < size; ++i) {
      alloc_id_set.Insert(alloc_ids[i]);
    }
    benchmark::DoNotOptimize(alloc_id_set);
  }
}

BENCHMARK(BM_AllocationIdSetInsertManyDifferent)->Range(1, (1 << 13));

}  // namespace
}  // namespace koladata::internal
