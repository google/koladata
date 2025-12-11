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
#ifndef KOLADATA_SHAPE_SHAPE_UTILS_H_
#define KOLADATA_SHAPE_SHAPE_UTILS_H_

#include <cstdint>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "koladata/data_slice.h"

namespace koladata::shape {

// Returns a shape with the highest rank. All shapes must be broadcastable to
// this resulting shape. In case they are not, the appropriate Status error is
// returned.
absl::StatusOr<DataSlice::JaggedShape> GetCommonShape(
    absl::Span<const DataSlice> slices);

// Returns the collection of broadcasted DataSlices to a common shape, i.e. a
// shape with a highest rank (among the slices) that all slices are
// broadcastable to.
absl::StatusOr<std::vector<DataSlice>> Align(std::vector<DataSlice> slices);
inline absl::StatusOr<std::vector<DataSlice>> Align(
    absl::Span<const DataSlice> slices) {
  return Align(std::vector<DataSlice>(slices.begin(), slices.end()));
}

// Returns the collection of broadcasted DataSlices to a common shape, i.e. a
// shape with a highest rank (among the slices) that all slices are
// broadcastable to. Unlike `Align`, scalars (rank-0) are left as-is and are not
// broadcasted.
absl::StatusOr<std::pair<std::vector<DataSlice>, DataSlice::JaggedShape>>
AlignNonScalars(std::vector<DataSlice> slices);

// Helper class for building a new 2D shape DataSlice::JaggedShape from number
// of groups to the number of elements in each group.
//
// Fails if:
//   - Some group sizes are negative.
//   - Number of groups added is not equal to the number of groups provided in
//     the constructor.
class Shape2DBuilder {
 public:
  explicit Shape2DBuilder(int64_t num_groups) : edge_builder_(num_groups + 1) {
    edge_builder_.Add(0, 0);
    ++i_next_;
  }

  void Add(int64_t group_size) {
    last_split_ += group_size;
    edge_builder_.Add(i_next_, last_split_);
    ++i_next_;
  }

  absl::StatusOr<DataSlice::JaggedShape> Build() &&;

 private:
  arolla::DenseArrayBuilder<int64_t> edge_builder_;
  int64_t i_next_ = 0;
  int64_t last_split_ = 0;
};

}  // namespace koladata::shape

#endif  // KOLADATA_SHAPE_SHAPE_UTILS_H_
