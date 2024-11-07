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
#include "koladata/internal/data_slice.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/internal/types.h"
#include "arolla/dense_array/bitmap.h"
#include "arolla/memory/buffer.h"

namespace koladata::internal {
namespace {

void BM_TypesBufferToBitmap(benchmark::State& state) {
  int64_t size = state.range(0);

  TypesBuffer b;
  b.types.push_back(ScalarTypeId<int>());
  b.id_to_typeidx.resize(size);
  for (int i = 0; i < size; ++i) {
    b.id_to_typeidx[i] = i % 2 ? 0 : TypesBuffer::kUnset;
  }

  for (auto _ : state) {
    benchmark::DoNotOptimize(b);
    arolla::bitmap::Bitmap res = b.ToBitmap(0);
    benchmark::DoNotOptimize(res);
  }
}

BENCHMARK(BM_TypesBufferToBitmap)->Arg(0)->Arg(1)->Arg(10)->Arg(100)->Arg(1000);

void BM_CreateEmpty(benchmark::State& state) {
  int64_t size = state.range(0);

  for (auto _ : state) {
    SliceBuilder bldr(size);
    DataSliceImpl slice = std::move(bldr).Build();
    benchmark::DoNotOptimize(slice);
  }
}

void BM_OldBuilder_CreateEmpty(benchmark::State& state) {
  int64_t size = state.range(0);

  for (auto _ : state) {
    DataSliceImpl::Builder bldr(size);
    DataSliceImpl slice = std::move(bldr).Build();
    benchmark::DoNotOptimize(slice);
  }
}

BENCHMARK(BM_CreateEmpty)->Arg(0)->Arg(1)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_OldBuilder_CreateEmpty)
    ->Arg(0)
    ->Arg(1)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000);

void BM_InsertOneType(benchmark::State& state) {
  int64_t size = state.range(0);

  for (auto _ : state) {
    SliceBuilder bldr(size);
    for (int i = 0; i < size; ++i) {
      bldr.InsertIfNotSet(i, i);
    }
    DataSliceImpl slice = std::move(bldr).Build();
    benchmark::DoNotOptimize(slice);
  }
}

void BM_OldBuilder_InsertOneType(benchmark::State& state) {
  int64_t size = state.range(0);

  for (auto _ : state) {
    DataSliceImpl::Builder bldr(size);
    for (int i = 0; i < size; ++i) {
      bldr.Insert(i, i);
    }
    DataSliceImpl slice = std::move(bldr).Build();
    benchmark::DoNotOptimize(slice);
  }
}

BENCHMARK(BM_InsertOneType)->Arg(1)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_OldBuilder_InsertOneType)->Arg(1)->Arg(10)->Arg(100)->Arg(1000);

void BM_InsertTwoTypes(benchmark::State& state) {
  int64_t size = state.range(0);

  for (auto _ : state) {
    SliceBuilder bldr(size);
    for (int i = 0; i < size - 1; i += 2) {
      bldr.InsertIfNotSet(i, i);
      bldr.InsertIfNotSet(i + 1, static_cast<float>(i));
    }
    DataSliceImpl slice = std::move(bldr).Build();
    benchmark::DoNotOptimize(slice);
  }
}

void BM_OldBuilder_InsertTwoTypes(benchmark::State& state) {
  int64_t size = state.range(0);

  for (auto _ : state) {
    DataSliceImpl::Builder bldr(size);
    for (int i = 0; i < size - 1; i += 2) {
      bldr.Insert(i, i);
      bldr.Insert(i + 1, static_cast<float>(i));
    }
    DataSliceImpl slice = std::move(bldr).Build();
    benchmark::DoNotOptimize(slice);
  }
}

BENCHMARK(BM_InsertTwoTypes)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_OldBuilder_InsertTwoTypes)->Arg(10)->Arg(100)->Arg(1000);

void BM_TypedInsert(benchmark::State& state) {
  int64_t size = state.range(0);

  for (auto _ : state) {
    SliceBuilder bldr(size);
    auto typed_bldr = bldr.typed<int>();
    for (int i = 0; i < size; ++i) {
      typed_bldr.InsertIfNotSet(i, i);
    }
    DataSliceImpl slice = std::move(bldr).Build();
    benchmark::DoNotOptimize(slice);
  }
}

void BM_OldBuilder_TypedInsert(benchmark::State& state) {
  int64_t size = state.range(0);

  for (auto _ : state) {
    DataSliceImpl::Builder bldr(size);
    auto& typed_bldr = bldr.GetArrayBuilder<int>();
    for (int i = 0; i < size; ++i) {
      typed_bldr.Set(i, i);
    }
    DataSliceImpl slice = std::move(bldr).Build();
    benchmark::DoNotOptimize(slice);
  }
}

BENCHMARK(BM_TypedInsert)->Arg(0)->Arg(1)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_OldBuilder_TypedInsert)
    ->Arg(0)
    ->Arg(1)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000);

}  // namespace
}  // namespace koladata::internal
