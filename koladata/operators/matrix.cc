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
#include "koladata/operators/matrix.h"

#include <cstdint>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/slice_builder.h"

namespace koladata::ops {

using JaggedShape = DataSlice::JaggedShape;
using Edge = JaggedShape::Edge;

namespace {

struct MatrixInfo {
  int64_t offset;  // Starting index of this matrix in the flat data array.
  int64_t m;       // Number of rows.
  int64_t n;       // Number of columns (uniform across all rows).
};

absl::StatusOr<std::vector<MatrixInfo>> ExtractMatrix2DInfos(
    const JaggedShape& shape) {
  const int rank = shape.rank();
  if (rank < 2) {
    return absl::InvalidArgumentError(
        absl::StrCat("expected at least 2D DataSlice, got ", rank, "D"));
  }
  const Edge& row_edge = shape.edges()[rank - 2];
  const Edge& col_edge = shape.edges()[rank - 1];
  DCHECK_EQ(row_edge.edge_type(), Edge::SPLIT_POINTS);
  DCHECK_EQ(col_edge.edge_type(), Edge::SPLIT_POINTS);
  absl::Span<const int64_t> row_splits = row_edge.edge_values().values.span();
  absl::Span<const int64_t> col_splits = col_edge.edge_values().values.span();
  const int64_t num_matrices = row_edge.parent_size();
  std::vector<MatrixInfo> infos(num_matrices);
  for (int64_t p = 0; p < num_matrices; ++p) {
    const int64_t m = row_splits[p + 1] - row_splits[p];
    int64_t n = 0;
    if (m > 0) {
      int64_t first_row = row_splits[p];
      n = col_splits[first_row + 1] - col_splits[first_row];
      for (int64_t r = 1; r < m; ++r) {
        const int64_t row_idx = first_row + r;
        const int64_t row_n = col_splits[row_idx + 1] - col_splits[row_idx];
        if (row_n != n) {
          return absl::InvalidArgumentError(absl::StrCat(
              "matrix ", p, " has non-uniform row sizes (", n, " vs ", row_n,
              "). Each matrix must have uniform row widths."));
        }
      }
    }
    infos[p].offset = (m > 0) ? col_splits[row_splits[p]] : 0;
    infos[p].m = m;
    infos[p].n = n;
  }
  return infos;
}

}  // namespace

absl::StatusOr<DataSlice> MatrixTranspose(const DataSlice& x) {
  const auto& shape = x.GetShape();
  const int rank = shape.rank();

  ASSIGN_OR_RETURN(auto mat_infos, ExtractMatrix2DInfos(shape));
  const int64_t num_matrices = mat_infos.size();

  // Derive new_row_splits from mat_infos.
  std::vector<int64_t> new_row_splits(num_matrices + 1);
  new_row_splits[0] = 0;
  for (int64_t p = 0; p < num_matrices; ++p) {
    // transposed: n rows
    new_row_splits[p + 1] = new_row_splits[p] + mat_infos[p].n;
  }

  // Derive new_col_splits from mat_infos.
  const int64_t total_new_rows = new_row_splits[num_matrices];
  std::vector<int64_t> new_col_splits(total_new_rows + 1);
  new_col_splits[0] = 0;
  for (int64_t p = 0; p < num_matrices; ++p) {
    for (int64_t j = 0; j < mat_infos[p].n; ++j) {
      int64_t new_row_idx = new_row_splits[p] + j;
      new_col_splits[new_row_idx + 1] =
          new_col_splits[new_row_idx] + mat_infos[p].m;  // transposed: m cols
    }
  }

  // Build the output JaggedShape: batch edges + new row/col edges.
  ASSIGN_OR_RETURN(auto result_shape, [&]() -> absl::StatusOr<JaggedShape> {
    auto new_row_edge = Edge::UnsafeFromSplitPoints(
        arolla::CreateFullDenseArray<int64_t>(new_row_splits));
    auto new_col_edge = Edge::UnsafeFromSplitPoints(
        arolla::CreateFullDenseArray<int64_t>(new_col_splits));

    std::vector<Edge> edges;
    edges.reserve(rank);
    for (int d = 0; d < rank - 2; ++d) {
      edges.push_back(shape.edges()[d]);
    }
    edges.push_back(std::move(new_row_edge));
    edges.push_back(std::move(new_col_edge));
    return JaggedShape::FromEdges(std::move(edges));
  }());

  // Permute data using SliceBuilder for efficient DenseArray access.
  auto flat = x.Flatten();
  const int64_t total = flat.GetShape().size();
  const auto& flat_impl = flat.slice();
  internal::SliceBuilder builder(total, flat_impl.allocation_ids());
  flat_impl.VisitValues(
      [&]<class T>(const arolla::DenseArray<T>& values) {
        arolla::DenseArrayBuilder<T> values_builder(total);
        for (int64_t p = 0; p < num_matrices; ++p) {
          int64_t offset = mat_infos[p].offset;
          int64_t m = mat_infos[p].m;
          int64_t n = mat_infos[p].n;
          int64_t dst = offset;
          for (int64_t j = 0; j < n; ++j) {
            for (int64_t i = 0; i < m; ++i) {
              int64_t src = offset + i * n + j;
              if (values.present(src)) {
                values_builder.Set(dst, values.values[src]);
              }
              dst++;
            }
          }
        }
        auto permuted = std::move(values_builder).Build();
        builder.InsertIfNotSet<T>(permuted.bitmap, {}, permuted.values);
      });
  return DataSlice::Create(std::move(builder).Build(),
                           std::move(result_shape), x.GetSchemaImpl(),
                           x.GetBag(),
                           x.IsWhole() ? DataSlice::Wholeness::kWhole
                                       : DataSlice::Wholeness::kNotWhole);
}
}  // namespace koladata::ops
