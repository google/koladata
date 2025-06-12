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
#ifndef KOLADATA_INTERNAL_OP_UTILS_BENCHMARK_UTIL_H_
#define KOLADATA_INTERNAL_OP_UTILS_BENCHMARK_UTIL_H_

#include <cstdint>

#include "absl/log/check.h"
#include "absl/random/random.h"
#include "arolla/dense_array/testing/util.h"

namespace koladata::internal {

template <class T>
auto RandomNonEmptyDenseArray(int64_t size, bool full, int bit_offset,
                              absl::BitGen& gen) {
  CHECK_NE(size, 0);
  auto res = arolla::testing::RandomDenseArray<T>(size, full, bit_offset, gen);
  while (res.IsAllMissing()) {
    res = arolla::testing::RandomDenseArray<T>(size, full, bit_offset, gen);
  }
  return res;
}

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_BENCHMARK_UTIL_H_
