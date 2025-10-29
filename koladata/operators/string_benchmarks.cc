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
#include <string>
#include <utility>

#include "benchmark/benchmark.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/util/bytes.h"
#include "arolla/util/init_arolla.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/operators/strings.h"

namespace koladata {
namespace {


void BM_EncodeBase64(benchmark::State& state, bool scalar) {
  arolla::InitArolla();

  constexpr int data_size = 100;
  arolla::Bytes data;
  data.resize(data_size);
  for (char& c : data) {
    c = 'A';
  }

  DataSlice ds;
  if (scalar) {
    ds = DataSlice::CreatePrimitive(std::move(data));
  } else {
    ds = DataSlice::CreateWithFlatShape(
             internal::DataSliceImpl::Create(
                 arolla::CreateConstDenseArray<arolla::Bytes>(state.range(0),
                                                              std::move(data))),
             internal::DataItem(schema::kBytes))
             .value();
  }

  (void)ops::EncodeBase64(ds);  // warmup caches

  for (auto _ : state) {
    benchmark::DoNotOptimize(ds);
    auto res = ops::EncodeBase64(ds).value();
    benchmark::DoNotOptimize(res);
  }
  if (!scalar) {
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                            state.range(0));
  } else {
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
  }
}

void BM_EncodeBase64Scalar(benchmark::State& state) {
  BM_EncodeBase64(state, true);
}
void BM_EncodeBase64Slice(benchmark::State& state) {
  BM_EncodeBase64(state, false);
}

BENCHMARK(BM_EncodeBase64Scalar);
BENCHMARK(BM_EncodeBase64Slice)->Range(1, 100);

}  // namespace
}  // namespace koladata
