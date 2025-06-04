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
#include "koladata/test_utils.h"

#include <cstdint>
#include <numeric>
#include <utility>
#include <vector>

#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "koladata/data_slice.h"

namespace koladata {
namespace test {

DataSlice::JaggedShape::Edge EdgeFromSplitPoints(
    absl::Span<const int64_t> split_points) {
  auto edge = DataSlice::JaggedShape::Edge::FromSplitPoints(
      arolla::CreateFullDenseArray(
          std::vector<int64_t>(split_points.begin(), split_points.end())));
  return std::move(edge).value();
}

DataSlice::JaggedShape ShapeFromSplitPoints(
    absl::Span<const absl::Span<const int64_t>> all_edge_split_points) {
  std::vector<DataSlice::JaggedShape::Edge> edges;
  edges.reserve(all_edge_split_points.size());
  for (const auto& split_points : all_edge_split_points) {
    edges.push_back(EdgeFromSplitPoints(split_points));
  }
  auto shape = DataSlice::JaggedShape::FromEdges(std::move(edges));
  return std::move(shape).value();
}

DataSlice::JaggedShape::Edge EdgeFromSizes(absl::Span<const int64_t> sizes) {
  std::vector<int64_t> split_points(sizes.size() + 1);
  split_points[0] = 0;
  std::partial_sum(sizes.begin(), sizes.end(), split_points.begin() + 1);
  auto edge = DataSlice::JaggedShape::Edge::FromSplitPoints(
      arolla::CreateFullDenseArray(std::move(split_points)));
  return std::move(edge).value();
}

DataSlice::JaggedShape ShapeFromSizes(
    absl::Span<const absl::Span<const int64_t>> all_edge_sizes) {
  std::vector<DataSlice::JaggedShape::Edge> edges;
  edges.reserve(all_edge_sizes.size());
  for (const auto& edge_sizes : all_edge_sizes) {
    edges.push_back(EdgeFromSizes(edge_sizes));
  }
  auto shape = DataSlice::JaggedShape::FromEdges(std::move(edges));
  return std::move(shape).value();
}

}  // namespace test
}  // namespace koladata
