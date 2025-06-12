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
#ifndef KOLADATA_INTERNAL_BENCHMARK_HELPERS_H_
#define KOLADATA_INTERNAL_BENCHMARK_HELPERS_H_

#include <cstdint>
#include <functional>

#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

constexpr auto kBenchmarkObjectBatchPairsFn = [](auto* b) {
  b->ArgPair(1, 0)
      ->ArgPair(10, 0)
      ->ArgPair(10000, 0)
      ->ArgPair(1000000, 0)
      ->ArgPair(100, 10)
      ->ArgPair(10000, 10)
      ->ArgPair(1000000, 10);
};

constexpr auto kPointwiseBenchmarkObjectBatchPairsFn = [](auto* b) {
  b->ArgPair(1, 0)->ArgPair(10, 0)->ArgPair(10000, 10);
};

constexpr auto kBenchmarkPrimitiveBatchPairsFn = [](auto* b) {
  b->ArgPair(1, 0)
      ->ArgPair(10, 0)
      ->ArgPair(1000, 0)
      ->ArgPair(10000, 0)
      ->ArgPair(100, 10)
      ->ArgPair(10000, 10);
};

constexpr auto kPointwiseBenchmarkPrimitiveBatchPairsFn = [](auto* b) {
  b->ArgPair(1, 0)->ArgPair(10, 0)->ArgPair(10000, 10);
};

DataSliceImpl FilterSingleAllocDataSlice(const DataSliceImpl& ds,
                                         int64_t batch_size, int64_t skip_size);

// Return new DataSlice with elements replaced with None.
DataSliceImpl RemoveItemsIf(const DataSliceImpl& ds,
                            std::function<bool(const DataItem&)> remove_fn);

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_BENCHMARK_HELPERS_H_
