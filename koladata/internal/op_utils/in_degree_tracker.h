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
#ifndef KOLADATA_INTERNAL_OP_UTILS_IN_DEGREE_TRACKER_H_
#define KOLADATA_INTERNAL_OP_UTILS_IN_DEGREE_TRACKER_H_

#include <cstdint>

#include "absl/status/statusor.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_slice.h"

namespace koladata::internal {

// A tracker for object in-degrees used for topological ordering of a graph.
//
// Wraps a DataBag storing in-degrees of objects as an integer attribute and
// provides operations to update in-degrees during graph traversal:
// - IncrementAndGetNew: during previsit, increments in-degree and returns
//   previously unvisited objects (those whose in-degree reached 1).
// - DecrementAndGetLast: during construction of topological ordering,
// decrements
//   in-degree and returns now-free objects (those whose in-degree reached 0).
class InDegreeTracker {
 public:
  InDegreeTracker() : databag_(DataBagImpl::CreateEmptyDatabag()) {}

  // Filters `slice` to retain only ObjectIds. Missing items and primitives are
  // filtered out.
  static DataSliceImpl FilterToObjects(const DataSliceImpl& slice);

  // Increments in-degree by 1 for ObjectIds in `slice` and returns a slice of
  // unique ObjectIds that were previously not visited (those whose in-degree
  // reached 1 after increment).
  absl::StatusOr<DataSliceImpl> IncrementAndGetNew(const DataSliceImpl& slice) {
    return ProcessSlice(slice, /*delta=*/1, /*target=*/1);
  }

  // Decrements in-degree by 1 for ObjectIds in `slice` and returns a slice of
  // unique ObjectIds that are now free (those whose in-degree reached 0 after
  // decrement).
  absl::StatusOr<DataSliceImpl> DecrementAndGetLast(
      const DataSliceImpl& slice) {
    return ProcessSlice(slice, /*delta=*/-1, /*target=*/0);
  }

 private:
  absl::StatusOr<DataSliceImpl> ProcessSlice(const DataSliceImpl& slice,
                                             int64_t delta, int64_t target);

  DataBagImplPtr databag_;
};

}  // namespace koladata::internal

#endif  // KOLADATA_INTERNAL_OP_UTILS_IN_DEGREE_TRACKER_H_
