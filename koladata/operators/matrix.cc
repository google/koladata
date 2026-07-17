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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <optional>
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
#include "arolla/qtype/qtype_traits.h"
#include "Eigen/Core"
#include "koladata/casting.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/schema_utils.h"

namespace koladata::ops {

using JaggedShape = DataSlice::JaggedShape;
using Edge = JaggedShape::Edge;

namespace {

template <typename T>
using RowMajorMatrix =
    Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>;

// Helper: extract flat double (or int64_t) data from a DataSlice that has been
// cast to FLOAT64 (or INT64). Missing values become 0.
//
// Fast path: when the data is fully present, memcpy from the underlying buffer.
template <typename T>
std::vector<T> ExtractFlat(const DataSlice& ds) {
  auto flat_ds = ds.Flatten();
  int64_t n = flat_ds.GetShape().size();
  if (flat_ds.impl_empty_and_unknown() || n == 0) {
    return std::vector<T>(n, 0);
  }

  const auto& impl = flat_ds.slice();

  DCHECK_EQ(flat_ds.dtype(), arolla::GetQType<T>());
  DCHECK_EQ(flat_ds.GetSchemaImpl(), internal::DataItem(schema::GetDType<T>()));

  const auto& arr = impl.values<T>();
  if (arr.bitmap.empty()) {
    // Fully present: memcpy from the underlying buffer.
    std::vector<T> result(n);
    std::memcpy(result.data(), arr.values.span().data(), n * sizeof(T));
    return result;
  }
  // Sparse data: fall through to per-element path.
  std::vector<T> result(n, 0);
  arr.ForEachPresent([&](int64_t id, T value) {
    result[id] = value;
  });
  return result;
}

// Helper: build a DataSlice from flat numeric data with a given shape.
template <typename T>
absl::StatusOr<DataSlice> BuildFromFlat(std::vector<T>&& data,
                                        JaggedShape shape) {
  arolla::DenseArray<T> arr = arolla::CreateFullDenseArray<T>(std::move(data));
  return DataSlice::CreatePrimitive(std::move(arr), std::move(shape));
}

// Determine the effective numeric DType of a DataSlice for matrix operations.
// For numeric schemas, the DType is the schema itself.
// For OBJECT schema, inspects the underlying data type (must be homogeneous).
// Returns std::nullopt for NONE schema or empty OBJECT (no data to inspect).
// Returns an error for non-numeric types or mixed dtypes.
absl::StatusOr<std::optional<schema::DType>> GetEffectiveDType(
    const DataSlice& ds) {
  const auto& schema = ds.GetSchemaImpl();
  if (schema == internal::DataItem(schema::kInt32)) {
    return schema::kInt32;
  }
  if (schema == internal::DataItem(schema::kInt64)) {
    return schema::kInt64;
  }
  if (schema == internal::DataItem(schema::kFloat32)) {
    return schema::kFloat32;
  }
  if (schema == internal::DataItem(schema::kFloat64)) {
    return schema::kFloat64;
  }
  if (schema == internal::DataItem(schema::kNone)) {
    return std::nullopt;
  }
  if (schema == internal::DataItem(schema::kObject)) {
    const auto& impl = ds.slice();
    if (impl.is_empty_and_unknown()) {
      return std::nullopt;
    }
    if (impl.is_mixed_dtype()) {
      return absl::InvalidArgumentError(
          "OBJECT operand with mixed data is not supported");
    }
    auto dtype = impl.dtype();
    if (dtype == arolla::GetQType<int32_t>()) return schema::kInt32;
    if (dtype == arolla::GetQType<int64_t>()) return schema::kInt64;
    if (dtype == arolla::GetQType<float>()) return schema::kFloat32;
    if (dtype == arolla::GetQType<double>()) return schema::kFloat64;
    return absl::InvalidArgumentError(
        "OBJECT operand contains non-numeric data");
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "unsupported schema: ", schema.DebugString()));
}

// Determine the common output DType for a binary numeric operation.
// Delegates to Koda's standard CommonSchema which follows the promotion
// chain: INT32 → INT64 → FLOAT32 → FLOAT64.
absl::StatusOr<schema::DType> GetCommonOutputDType(
    std::optional<schema::DType> a_dtype,
    std::optional<schema::DType> b_dtype) {
  // If either is neutral (NONE/empty OBJECT), use the other's type.
  // If both are neutral, default to INT32 (narrowest numeric type).
  if (!a_dtype && !b_dtype) {
    return schema::kInt32;
  }
  if (!a_dtype) a_dtype = b_dtype;
  if (!b_dtype) b_dtype = a_dtype;

  ASSIGN_OR_RETURN(auto common_schema,
                   schema::CommonSchema(*a_dtype, *b_dtype));
  return common_schema.value<schema::DType>();
}

// Helper: broadcast batch dimensions for a binary op on inputs x and y.
//
// Each input's shape is split into batch dims (leading) and matrix/vector dims
// (trailing). For example, a 5D input with 2 trailing matrix dims has 3 batch
// dims. The "batch elements" are the items at the innermost batch level — i.e.,
// the individual matrices or vectors that the operator will act on.
//
// Koda uses prefix broadcasting: one batch shape must be a prefix of the
// other. The edges at the common ranks must be identical. The shorter input is
// broadcast (replicated) along the extra trailing batch dims of the longer
// input.
//
// Example: x has batch shape [2] (2 batch elements) and y has batch shape
// [2, [2,3]] (5 batch elements, jagged). x's shape [2] is a prefix of y's.
// The output has 5 batch elements (one per element of y). x is broadcast:
//   output element 0 uses x[0], y[0][0]
//   output element 1 uses x[0], y[0][1]
//   output element 2 uses x[1], y[1][0]
//   output element 3 uses x[1], y[1][1]
//   output element 4 uses x[1], y[1][2]
// So x_batch_map = [0, 0, 1, 1, 1] and y_batch_map = [0, 1, 2, 3, 4].
struct BroadcastResult {
  // Total number of batch elements in the output.
  int64_t out_batch_size;

  // Maps from output batch element index to the corresponding batch element
  // index in x's MatrixInfo/VectorInfo vector. Has out_batch_size entries.
  std::vector<int64_t> x_batch_map;

  // Maps from output batch element index to the corresponding batch element
  // index in y's MatrixInfo/VectorInfo vector. Has out_batch_size entries.
  std::vector<int64_t> y_batch_map;

  // The JaggedShape representing the batch dimensions of the output (i.e., the
  // longer of the two input batch shapes). The operator appends matrix/vector
  // edges to this shape to build the full output shape.
  JaggedShape out_batch_shape;
};

absl::StatusOr<BroadcastResult> BroadcastBatchDims(
    const JaggedShape& x_shape, int x_batch_rank,
    const JaggedShape& y_shape, int y_batch_rank) {
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
      return absl::InvalidArgumentError(absl::StrCat(
          "batch dimensions are not compatible. "
          "One must be a prefix of the other. "
          "Mismatch at dimension ", d));
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
    for (int64_t parent = 0;
         parent < static_cast<int64_t>(short_map.size()); ++parent) {
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

struct MatrixInfo {
  int64_t offset;  // Starting index of this matrix in the flat data array.
  int64_t m;       // Number of rows.
  int64_t n;       // Number of columns (uniform across all rows). 1 for vectors
};

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

// =========================================================================
// Framework helpers for the "Batch + Core Op" pattern
// =========================================================================
//
// Most operators in this file follow the same high-level structure:
//
//   1. SETUP  — Validate ranks, extract per-matrix/vector metadata
//               (offset, m, n) from the JaggedShape, determine batch
//               dimensions, and broadcast the batch dims of two inputs
//               so that the shorter batch is replicated.
//
//   2. VALIDATE — Walk the broadcast batch elements and check per-element
//                 compatibility (e.g. inner-dimension match for matmul,
//                 square-matrix check for solve).
//
//   3. COMPUTE — Type-dispatch (int64 vs float64), extract flat data,
//                loop over batch elements calling the core math kernel
//                (hand-rolled or Eigen), and write into a flat result
//                buffer.
//
//   4. BUILD  — Construct the output JaggedShape (batch dims + trailing
//               matrix/vector edges) and wrap the flat result buffer
//               into a DataSlice.
//
// The helpers below factor out the code that is identical across
// operators in each phase:
//
//   SetupBroadcastBinaryOp   — phase 1 for binary ops (matmul, outer, solve)
//   DispatchNumericBinaryOp  — phase 3 for binary ops with both int & float
//   BuildBatchedMatrixShape  — phase 4 when the output has 2 trailing dims
//   BuildBatchedVectorShape  — phase 4 when the output has 1 trailing dim
//

// Setup result for a binary op with broadcasting.
struct BinaryBroadcastSetup {
  BroadcastResult broadcast_result;
  std::vector<MatrixInfo> x_infos;
  std::vector<MatrixInfo> y_infos;
};

// Extract infos and broadcast batch dims for a binary op.
// x_trailing_dims/y_trailing_dims: how many trailing dims are matrix dims
// (1 for vector, 2 for matrix).
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

  ASSIGN_OR_RETURN(setup.broadcast_result,
                   BroadcastBatchDims(x_shape, x_batch_rank,
                                     y_shape, y_batch_rank));

  return setup;
}

// Dispatch a binary numeric operation across integer and float types.
// The compute_fn is a generic lambda that takes 3 arguments:
// (const vector<T>& x_data, const vector<T>& y_data, vector<T>& result), where
// T is either int64_t or double.
// Returns the built DataSlice.
template <typename ComputeFn>
absl::StatusOr<DataSlice> DispatchNumericBinaryOp(const DataSlice& x,
                                                  const DataSlice& y,
                                                  int64_t out_total,
                                                  JaggedShape out_shape,
                                                  ComputeFn&& compute_fn) {
  bool is_object_schema =
      x.GetSchemaImpl() == internal::DataItem(schema::kObject) ||
      y.GetSchemaImpl() == internal::DataItem(schema::kObject);
  ASSIGN_OR_RETURN(auto x_dtype, GetEffectiveDType(x));
  ASSIGN_OR_RETURN(auto y_dtype, GetEffectiveDType(y));
  ASSIGN_OR_RETURN(auto output_dtype, GetCommonOutputDType(x_dtype, y_dtype));

  auto do_compute =
      [&]<typename ComputeT, typename OutputT>(
          internal::DataItem compute_schema,
          internal::DataItem output_schema) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto cast_x, CastToExplicit(x, compute_schema));
    ASSIGN_OR_RETURN(auto cast_y, CastToExplicit(y, compute_schema));
    auto x_data = ExtractFlat<ComputeT>(cast_x);
    auto y_data = ExtractFlat<ComputeT>(cast_y);
    std::vector<OutputT> result(out_total, 0);
    // Compute in ComputeT, storing as OutputT.
    std::vector<ComputeT> tmp(out_total, 0);
    compute_fn(x_data, y_data, tmp);
    for (int64_t i = 0; i < out_total; ++i) {
      result[i] = static_cast<OutputT>(tmp[i]);
    }
    ASSIGN_OR_RETURN(auto result_ds,
                     BuildFromFlat<OutputT>(std::move(result),
                                            std::move(out_shape)));
    if (is_object_schema) {
      return result_ds.WithSchema(internal::DataItem(schema::kObject));
    }
    return result_ds;
  };

  if (output_dtype == schema::kFloat32) {
    return do_compute.template operator()<double, float>(
        internal::DataItem(schema::kFloat64),
        internal::DataItem(schema::kFloat32));
  }
  if (output_dtype == schema::kFloat64) {
    return do_compute.template operator()<double, double>(
        internal::DataItem(schema::kFloat64),
        internal::DataItem(schema::kFloat64));
  }
  if (output_dtype == schema::kInt32) {
    return do_compute.template operator()<int64_t, int32_t>(
        internal::DataItem(schema::kInt64),
        internal::DataItem(schema::kInt32));
  }
  DCHECK_EQ(output_dtype, schema::kInt64);
  return do_compute.template operator()<int64_t, int64_t>(
      internal::DataItem(schema::kInt64),
      internal::DataItem(schema::kInt64));
}

// Build a JaggedShape:
// batch_shape + row_edge(row_counts) + col_edge(col_counts).
// Used for 2D output of batched matrix ops (matmul, outer, solve).
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

// Build a JaggedShape: batch_shape + vector_edge(counts).
// Used for 1D output of batched matrix ops (matmul 2D×1D, solve vector).
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

absl::StatusOr<DataSlice> MatrixMatmul(const DataSlice& a, const DataSlice& b,
                                       const DataSlice& a_ndim_ds,
                                       const DataSlice& b_ndim_ds) {
  const int a_rank = a.GetShape().rank();
  const int b_rank = b.GetShape().rank();

  if (a_rank < 1 || b_rank < 1) {
    return absl::InvalidArgumentError("inputs must have at least 1 dimension");
  }

  const auto resolve_ndim =
      [&](absl::string_view arg_name, int rank, absl::string_view ndim_arg_name,
          const DataSlice& ndim_ds) -> absl::StatusOr<int> {
    RETURN_IF_ERROR(
        ExpectPresentScalar(ndim_arg_name, ndim_ds, schema::kInt64));
    ASSIGN_OR_RETURN(const auto ndim_int64,
                     CastToNarrow(ndim_ds, internal::DataItem(schema::kInt64)));
    // -1 means auto-detect: use min(rank, 2).
    const int ndim = [&] {
      int v = ndim_int64.item().value<int64_t>();
      return v == -1 ? std::min(rank, 2) : v;
    }();
    if (ndim < 1 || ndim > 2) {
      return absl::InvalidArgumentError(absl::StrCat(
          ndim_arg_name, " must be 1, 2, or -1 (auto), got ", ndim));
    }
    if (rank < ndim) {
      return absl::InvalidArgumentError(
          absl::StrCat(arg_name, " has rank ", rank, " but ", ndim_arg_name,
                       "=", ndim, " requires at least that many"));
    }
    return ndim;
  };

  ASSIGN_OR_RETURN(const int a_ndim,
                   resolve_ndim("a", a_rank, "a_ndim", a_ndim_ds));
  ASSIGN_OR_RETURN(const int b_ndim,
                   resolve_ndim("b", b_rank, "b_ndim", b_ndim_ds));

  ASSIGN_OR_RETURN(
      (auto [broadcast_result, a_infos_vec, b_infos_vec]),
      SetupBroadcastBinaryOp(a.GetShape(), b.GetShape(), a_ndim, b_ndim));

  const bool is_object_schema =
      a.GetSchemaImpl() == internal::DataItem(schema::kObject) ||
      b.GetSchemaImpl() == internal::DataItem(schema::kObject);
  ASSIGN_OR_RETURN(auto a_dtype, GetEffectiveDType(a));
  ASSIGN_OR_RETURN(auto b_dtype, GetEffectiveDType(b));
  ASSIGN_OR_RETURN(auto output_dtype, GetCommonOutputDType(a_dtype, b_dtype));

  // Precomputed metadata for one output batch element. We collect these in a
  // single pass that also validates inner-dimension compatibility, then reuse
  // them when building the output shape and computing the matrix products.
  struct PerBatch {
    // Index into a_infos_vec for this batch element's a matrix.
    int64_t a_idx;
    // Index into b_infos_vec for this batch element's b matrix.
    int64_t b_idx;
    int64_t a_rows;  // Number of rows in a (= a_infos_vec[a_idx].m).
    int64_t k;       // Shared inner dimension (a's cols = b's rows).
    int64_t b_cols;  // Number of columns in b (= b_infos_vec[b_idx].n).
  };
  std::vector<PerBatch> pbs(broadcast_result.out_batch_size);
  int64_t out_total = 0;
  for (int64_t batch = 0; batch < broadcast_result.out_batch_size; ++batch) {
    int64_t a_b = broadcast_result.x_batch_map[batch];
    int64_t b_b = broadcast_result.y_batch_map[batch];
    int64_t a_cols = a_infos_vec[a_b].n;
    int64_t b_rows = b_infos_vec[b_b].m;
    if (a_cols != b_rows) {
      return absl::InvalidArgumentError(
          absl::StrCat("inner dimension mismatch at batch element ", batch,
                       ": ", a_cols, " vs ", b_rows));
    }
    pbs[batch] = {a_b, b_b, a_infos_vec[a_b].m, a_cols, b_infos_vec[b_b].n};
    out_total += pbs[batch].a_rows * pbs[batch].b_cols;
  }

  // Build output shape using framework helpers.
  ASSIGN_OR_RETURN(auto out_shape, [&]() -> absl::StatusOr<JaggedShape> {
    const bool a_ndim_is_1 = (a_ndim == 1);
    const bool b_ndim_is_1 = (b_ndim == 1);
    auto batch_shape = broadcast_result.out_batch_shape;
    if (!a_ndim_is_1 && !b_ndim_is_1) {
      std::vector<int64_t> row_counts(broadcast_result.out_batch_size);
      std::vector<int64_t> col_counts(broadcast_result.out_batch_size);
      for (int64_t b = 0; b < broadcast_result.out_batch_size; ++b) {
        row_counts[b] = pbs[b].a_rows;
        col_counts[b] = pbs[b].b_cols;
      }
      return BuildBatchedMatrixShape(std::move(batch_shape), row_counts,
                                     col_counts);
    } else if (!a_ndim_is_1) {
      std::vector<int64_t> counts(broadcast_result.out_batch_size);
      for (int64_t b = 0; b < broadcast_result.out_batch_size; ++b) {
        counts[b] = pbs[b].a_rows;
      }
      return BuildBatchedVectorShape(std::move(batch_shape), counts);
    } else if (!b_ndim_is_1) {
      std::vector<int64_t> counts(broadcast_result.out_batch_size);
      for (int64_t b = 0; b < broadcast_result.out_batch_size; ++b) {
        counts[b] = pbs[b].b_cols;
      }
      return BuildBatchedVectorShape(std::move(batch_shape), counts);
    } else {
      return batch_shape;
    }
  }());

  // Compute in 64-bit precision (ComputeT), then cast to the output type
  // (OutputT) to preserve schema width. E.g. FLOAT32 inputs are computed
  // in FLOAT64 but the result is stored as FLOAT32.
  auto do_matmul = [&]<typename ComputeT, typename OutputT>(
      internal::DataItem compute_schema,
      internal::DataItem output_schema) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto cast_a, CastToExplicit(a, compute_schema));
    ASSIGN_OR_RETURN(auto cast_b, CastToExplicit(b, compute_schema));
    auto a_flat = ExtractFlat<ComputeT>(cast_a);
    auto b_flat = ExtractFlat<ComputeT>(cast_b);
    std::vector<OutputT> result(out_total);
    int64_t out_off = 0;
    for (int64_t batch = 0; batch < broadcast_result.out_batch_size; ++batch) {
      int64_t a_rows = pbs[batch].a_rows;
      int64_t k = pbs[batch].k;
      int64_t b_cols = pbs[batch].b_cols;
      Eigen::Map<const RowMajorMatrix<ComputeT>> mat_a(
          a_flat.data() + a_infos_vec[pbs[batch].a_idx].offset, a_rows, k);
      Eigen::Map<const RowMajorMatrix<ComputeT>> mat_b(
          b_flat.data() + b_infos_vec[pbs[batch].b_idx].offset, k, b_cols);
      // Compute in ComputeT precision, then cast each element to OutputT.
      Eigen::Matrix<ComputeT, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>
          mat_out(a_rows, b_cols);
      mat_out.noalias() = mat_a * mat_b;
      int64_t num_elements = a_rows * b_cols;
      for (int64_t i = 0; i < num_elements; ++i) {
        result[out_off + i] = static_cast<OutputT>(mat_out.data()[i]);
      }
      out_off += a_rows * b_cols;
    }
    ASSIGN_OR_RETURN(
        auto result_ds,
        BuildFromFlat<OutputT>(std::move(result), std::move(out_shape)));
    if (is_object_schema) {
      return result_ds.WithSchema(internal::DataItem(schema::kObject));
    }
    return result_ds;
  };
  if (output_dtype == schema::kFloat32) {
    return do_matmul.operator()<double, float>(
        internal::DataItem(schema::kFloat64),
        internal::DataItem(schema::kFloat32));
  }
  if (output_dtype == schema::kFloat64) {
    return do_matmul.operator()<double, double>(
        internal::DataItem(schema::kFloat64),
        internal::DataItem(schema::kFloat64));
  }
  if (output_dtype == schema::kInt32) {
    return do_matmul.operator()<int64_t, int32_t>(
        internal::DataItem(schema::kInt64), internal::DataItem(schema::kInt32));
  }
  DCHECK_EQ(output_dtype, schema::kInt64);
  return do_matmul.operator()<int64_t, int64_t>(
      internal::DataItem(schema::kInt64), internal::DataItem(schema::kInt64));
}

absl::StatusOr<DataSlice> MatrixOuter(const DataSlice& x, const DataSlice& y) {
  if (x.GetShape().rank() < 1 || y.GetShape().rank() < 1) {
    return absl::InvalidArgumentError("inputs must have at least 1 dimension");
  }

  ASSIGN_OR_RETURN(auto setup,
                   SetupBroadcastBinaryOp(x.GetShape(), y.GetShape(), 1, 1));
  auto& broadcast_result = setup.broadcast_result;
  // SetupBroadcastBinaryOp with trailing_dims=1 stores vector infos as
  // x_infos: {offset, 1, m} and y_infos: {offset, m, 1}.
  // So x vector length = x_infos[i].n, y vector length = y_infos[i].m.
  const auto& x_infos = setup.x_infos;
  const auto& y_infos = setup.y_infos;

  // Precomputed metadata for one output batch element. We collect these in a
  // single pass and reuse them when building the products and the output shape.
  struct PerBatch {
    // Offsets in flattened x and y for this batch element.
    int64_t x_offset, y_offset;
    int64_t m, n;  // output matrix dimensions
  };
  std::vector<PerBatch> pbs(broadcast_result.out_batch_size);
  int64_t out_total = 0;
  std::vector<int64_t> out_m_vals(broadcast_result.out_batch_size);
  std::vector<int64_t> out_n_vals(broadcast_result.out_batch_size);
  for (int64_t b = 0; b < broadcast_result.out_batch_size; ++b) {
    int64_t x_idx = broadcast_result.x_batch_map[b];
    int64_t y_idx = broadcast_result.y_batch_map[b];
    auto& x_info = x_infos[x_idx];
    auto& y_info = y_infos[y_idx];
    int64_t m = x_info.n;  // x vector length
    int64_t n = y_info.m;  // y vector length
    pbs[b] = {x_info.offset, y_info.offset, m, n};
    out_m_vals[b] = m;
    out_n_vals[b] = n;
    out_total += m * n;
  }

  auto do_outer = [&](const auto& x_data, const auto& y_data, auto& result) {
    int64_t out_off = 0;
    for (int64_t b = 0; b < broadcast_result.out_batch_size; ++b) {
      const auto& pb = pbs[b];
      const int64_t m = pb.m;
      const int64_t n = pb.n;
      const int64_t x_off = pb.x_offset;
      const int64_t y_off = pb.y_offset;
      for (int64_t i = 0; i < m; ++i) {
        for (int64_t j = 0; j < n; ++j) {
          result[out_off + i * n + j] = x_data[x_off + i] * y_data[y_off + j];
        }
      }
      out_off += m * n;
    }
  };

  ASSIGN_OR_RETURN(
      auto out_shape,
      BuildBatchedMatrixShape(std::move(broadcast_result.out_batch_shape),
                              out_m_vals, out_n_vals));
  return DispatchNumericBinaryOp(x, y, out_total, std::move(out_shape),
                                 do_outer);
}

}  // namespace koladata::ops
