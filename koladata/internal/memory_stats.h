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
#ifndef KOLADATA_INTERNAL_MEMORY_STATS_H_
#define KOLADATA_INTERNAL_MEMORY_STATS_H_

#include <cstddef>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"

namespace koladata::internal {

struct MemoryStatsEntry {
  std::string container_description;

  // Memory usage without taking into account allocations inside of values.
  // E.g. for array of N ExprQuote it is `N * sizeof(ExprQuote)`, which is only
  // size of shared pointers to the expressions.
  size_t shallow_size = 0;

  // Size of string data of arolla::Text and arolla::Bytes values.
  // Note that it is not included to shallow_size.
  size_t strings_size = 0;

  template <class T>
  void AppendStringsSize(const T& v) {
    absl::string_view view(v);
    if (view.data() < reinterpret_cast<const char*>(&v) ||
        view.data() >= reinterpret_cast<const char*>(&v) + sizeof(T)) {
      // String is allocated outside of sizeof(T)
      strings_size += view.size() + 1;
    }
  }
};

using MemoryStats = std::vector<MemoryStatsEntry>;

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_MEMORY_STATS_H_
