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
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/hash/hash.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/uuid_object.h"
#include "koladata/internal/object_id.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"

namespace koladata::internal {
namespace {

void BM_CreateUuidWithMainObjectDataSliceSingleAlloc(benchmark::State& state) {
  int64_t size = state.range(0);
  DataSliceImpl x = DataSliceImpl::AllocateEmptyObjects(size);
  for (auto _ : state) {
    benchmark::DoNotOptimize(x);
    auto y = CreateUuidWithMainObject(x, "y");
    benchmark::DoNotOptimize(y);
  }
}

BENCHMARK(BM_CreateUuidWithMainObjectDataSliceSingleAlloc)->Range(1, 100000);

void BM_CreateUuidWithMainObjectDataSliceTwoAllocs(benchmark::State& state) {
  int64_t size = state.range(0);
  auto alloc1 = Allocate(size);
  auto alloc2 = Allocate(size);
  std::vector<arolla::OptionalValue<ObjectId>> objs;
  objs.reserve(size);
  for (int64_t i = 0; i < size; ++i) {
    if (absl::HashOf(i) % 2 == 0) {
      objs.push_back(alloc1.ObjectByOffset(i));
    } else {
      objs.push_back(alloc2.ObjectByOffset(i));
    }
  }
  auto x = DataSliceImpl::CreateWithAllocIds(
      AllocationIdSet({alloc1, alloc2}),
      arolla::CreateDenseArray<ObjectId>(std::move(objs)));
  for (auto _ : state) {
    benchmark::DoNotOptimize(x);
    auto y = CreateUuidWithMainObject(x, "y");
    benchmark::DoNotOptimize(y);
  }
}

BENCHMARK(BM_CreateUuidWithMainObjectDataSliceTwoAllocs)->Range(1, 100000);

}  // namespace
}  // namespace koladata::internal
