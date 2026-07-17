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
#include "koladata/operators/matrix_helpers.h"

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/schema_utils.h"

namespace koladata::ops::matrix_helpers {

absl::StatusOr<internal::DataItem> GetNarrowedMatrixSchema(
    const DataSlice& ds) {
  auto narrowed_schema = GetNarrowedSchema(ds);
  // Check that it is a numeric schema or NONE:
  if (!schema::IsImplicitlyCastableTo(narrowed_schema,
                                      internal::DataItem(schema::kFloat64))) {
    return absl::InvalidArgumentError(absl::StrCat(
        "unsupported narrowed schema: ", narrowed_schema.DebugString()));
  }

  // Matrix ops treat missing values as 0. If the narrowed_schema is NONE, then
  // we need to return INT32 (narrowest numeric type that can represent 0).
  if (narrowed_schema == internal::DataItem(schema::kNone)) {
    return internal::DataItem(schema::kInt32);
  }
  return narrowed_schema;
}

absl::StatusOr<BroadcastResult> BroadcastBatchDims(const JaggedShape& x_shape,
                                                   int x_batch_rank,
                                                   const JaggedShape& y_shape,
                                                   int y_batch_rank) {
  // Determine which input has more batch dims.
  const bool x_is_longer = x_batch_rank >= y_batch_rank;
  const JaggedShape& long_shape = x_is_longer ? x_shape : y_shape;
  const JaggedShape& short_shape = x_is_longer ? y_shape : x_shape;
  const int long_rank = x_is_longer ? x_batch_rank : y_batch_rank;
  const int short_rank = x_is_longer ? y_batch_rank : x_batch_rank;

  // Verify that the common prefix edges are identical.
  for (int d = 0; d < short_rank; ++d) {
    const Edge& long_edge = long_shape.edges()[d];
    const Edge& short_edge = short_shape.edges()[d];
    DCHECK_EQ(long_edge.edge_type(), Edge::SPLIT_POINTS);
    DCHECK_EQ(short_edge.edge_type(), Edge::SPLIT_POINTS);
    auto long_splits = long_edge.edge_values().values.span();
    auto short_splits = short_edge.edge_values().values.span();
    if (long_splits != short_splits) {
      return absl::InvalidArgumentError(
          absl::StrCat("batch dimensions are not compatible. "
                       "One must be a prefix of the other. "
                       "Mismatch at dimension ",
                       d));
    }
  }

  BroadcastResult result;
  // Output batch shape is the first long_rank edges of the long shape.
  result.out_batch_shape = long_shape.RemoveDims(/*from=*/long_rank);
  result.out_batch_size =
      long_rank > 0 ? long_shape.edges()[long_rank - 1].child_size() : 1;

  // The longer input maps identity.
  std::vector<int64_t> long_map(result.out_batch_size);
  std::iota(long_map.begin(), long_map.end(), 0);

  // The shorter input: expand through extra edges of the long shape.
  // Start with identity at the short batch level.
  int64_t short_size =
      short_rank > 0 ? short_shape.edges()[short_rank - 1].child_size() : 1;
  std::vector<int64_t> short_map(short_size);
  std::iota(short_map.begin(), short_map.end(), 0);

  for (int d = short_rank; d < long_rank; ++d) {
    auto edge = long_shape.edges()[d];
    DCHECK_EQ(edge.edge_type(), Edge::SPLIT_POINTS);
    auto splits = edge.edge_values().values.span();
    int64_t child_size = edge.child_size();
    std::vector<int64_t> new_map(child_size);
    for (int64_t parent = 0; parent < static_cast<int64_t>(short_map.size());
         ++parent) {
      for (int64_t child = splits[parent]; child < splits[parent + 1];
           ++child) {
        new_map[child] = short_map[parent];
      }
    }
    short_map = std::move(new_map);
  }

  if (x_is_longer) {
    result.x_batch_map = std::move(long_map);
    result.y_batch_map = std::move(short_map);
  } else {
    result.x_batch_map = std::move(short_map);
    result.y_batch_map = std::move(long_map);
  }
  return result;
}

absl::StatusOr<std::vector<MatrixInfo>> ExtractVectorInfos(
    const JaggedShape& shape) {
  const int rank = shape.rank();
  if (rank < 1) {
    return absl::InvalidArgumentError(
        absl::StrCat("expected at least 1D DataSlice, got ", rank, "D"));
  }
  const Edge& edge = shape.edges()[rank - 1];
  DCHECK_EQ(edge.edge_type(), Edge::SPLIT_POINTS);
  absl::Span<const int64_t> splits = edge.edge_values().values.span();
  const int64_t num = edge.parent_size();
  std::vector<MatrixInfo> infos(num);
  for (int64_t i = 0; i < num; ++i) {
    infos[i].offset = splits[i];
    infos[i].m = splits[i + 1] - splits[i];
    infos[i].n = 1;
  }
  return infos;
}

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

absl::StatusOr<BinaryBroadcastSetup> SetupBroadcastBinaryOp(
    const JaggedShape& x_shape, const JaggedShape& y_shape, int x_trailing_dims,
    int y_trailing_dims) {
  BinaryBroadcastSetup setup;

  if (x_trailing_dims == 1) {
    ASSIGN_OR_RETURN(setup.x_infos, ExtractVectorInfos(x_shape));
    // ExtractVectorInfos for vectors will set n=1, i.e. it will treat the
    // vector as a column vector, i.e. a matrix with 1 column. However, `x` is
    // the left operand of a binary op, and we want to treat it as a row vector,
    // i.e. a matrix with 1 row. So we transpose the metadata here:
    for (auto& info : setup.x_infos) {
      std::swap(info.m, info.n);
    }
  } else {
    ASSIGN_OR_RETURN(setup.x_infos, ExtractMatrix2DInfos(x_shape));
  }

  if (y_trailing_dims == 1) {
    ASSIGN_OR_RETURN(setup.y_infos, ExtractVectorInfos(y_shape));
  } else {
    ASSIGN_OR_RETURN(setup.y_infos, ExtractMatrix2DInfos(y_shape));
  }

  int x_batch_rank = x_shape.rank() - x_trailing_dims;
  int y_batch_rank = y_shape.rank() - y_trailing_dims;

  ASSIGN_OR_RETURN(
      setup.broadcast_result,
      BroadcastBatchDims(x_shape, x_batch_rank, y_shape, y_batch_rank));

  return setup;
}

absl::StatusOr<JaggedShape> BuildBatchedMatrixShape(
    JaggedShape batch_shape, absl::Span<const int64_t> row_counts,
    absl::Span<const int64_t> col_counts) {
  const int64_t num_matrices = row_counts.size();
  std::vector<int64_t> row_splits(num_matrices + 1);
  row_splits[0] = 0;
  for (int64_t b = 0; b < num_matrices; ++b) {
    row_splits[b + 1] = row_splits[b] + row_counts[b];
  }
  const int64_t total_rows = row_splits[num_matrices];
  auto row_da = arolla::CreateFullDenseArray<int64_t>(std::move(row_splits));
  auto row_edge = Edge::UnsafeFromSplitPoints(std::move(row_da));
  std::vector<int64_t> col_splits(total_rows + 1);
  col_splits[0] = 0;
  int64_t ri = 0;
  for (int64_t b = 0; b < num_matrices; ++b) {
    for (int64_t r = 0; r < row_counts[b]; ++r) {
      col_splits[ri + 1] = col_splits[ri] + col_counts[b];
      ++ri;
    }
  }
  auto col_da = arolla::CreateFullDenseArray<int64_t>(std::move(col_splits));
  auto col_edge = Edge::UnsafeFromSplitPoints(std::move(col_da));
  return batch_shape.AddDims({std::move(row_edge), std::move(col_edge)});
}

absl::StatusOr<JaggedShape> BuildBatchedVectorShape(
    JaggedShape batch_shape, absl::Span<const int64_t> counts) {
  int64_t num_batches = counts.size();
  std::vector<int64_t> splits(num_batches + 1);
  splits[0] = 0;
  for (int64_t b = 0; b < num_batches; ++b) {
    splits[b + 1] = splits[b] + counts[b];
  }
  auto da = arolla::CreateFullDenseArray<int64_t>(std::move(splits));
  auto edge = Edge::UnsafeFromSplitPoints(std::move(da));
  return batch_shape.AddDims({std::move(edge)});
}

}  // namespace koladata::ops::matrix_helpers
