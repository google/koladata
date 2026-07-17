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
#ifndef KOLADATA_OPERATORS_MATRIX_HELPERS_H_
#define KOLADATA_OPERATORS_MATRIX_HELPERS_H_

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qtype/qtype_traits.h"
#include "koladata/casting.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_utils.h"

namespace koladata::ops::matrix_helpers {

using JaggedShape = DataSlice::JaggedShape;
using Edge = JaggedShape::Edge;

// Helper: extract flat data from a DataSlice that has been cast to the
// corresponding schema. For example, extract flat double data from a FLOAT64
// DataSlice, or flat float data from a FLOAT32 DataSlice, or int64_t data from
// an INT64 DataSlice.
//
// Missing values become 0.
//
// Fast path: when the data is fully present, memcpy from the underlying buffer.
//
// Idea for the future: we can return something with optional ownership, such
// as an arolla::SimpleBuffer, to avoid unnecessary copying. For example,
// arolla::SimpleBuffer(nullptr, data_span) is a buffer that does not own the
// underlying data.
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
  arr.ForEachPresent([&](int64_t id, T value) { result[id] = value; });
  return result;
}

// Helper: build a DataSlice from flat numeric data with a given shape.
template <typename T>
absl::StatusOr<DataSlice> BuildFromFlat(std::vector<T>&& data,
                                        JaggedShape shape) {
  arolla::DenseArray<T> arr = arolla::CreateFullDenseArray<T>(std::move(data));
  return DataSlice::CreatePrimitive(std::move(arr), std::move(shape));
}

// Determine the narrowest numeric schema of a DataSlice for matrix operations.
// Returns CommonSchema(GetNarrowedSchema(ds), INT32). Returns an error if
// GetNarrowedSchema(ds) is not a numeric schema.
absl::StatusOr<internal::DataItem> GetNarrowedMatrixSchema(const DataSlice& ds);

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

absl::StatusOr<BroadcastResult> BroadcastBatchDims(const JaggedShape& x_shape,
                                                   int x_batch_rank,
                                                   const JaggedShape& y_shape,
                                                   int y_batch_rank);

struct MatrixInfo {
  int64_t offset;  // Starting index of this matrix in the flat data array.
  int64_t m;       // Number of rows.
  int64_t n;       // Number of columns (uniform across all rows). 1 for vectors
};

absl::StatusOr<std::vector<MatrixInfo>> ExtractVectorInfos(
    const JaggedShape& shape);

absl::StatusOr<std::vector<MatrixInfo>> ExtractMatrix2DInfos(
    const JaggedShape& shape);

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
    int y_trailing_dims);

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
  ASSIGN_OR_RETURN(auto x_narrowed_schema, GetNarrowedMatrixSchema(x));
  ASSIGN_OR_RETURN(auto y_narrowed_schema, GetNarrowedMatrixSchema(y));
  ASSIGN_OR_RETURN(auto common_narrowed_schema,
                   schema::CommonSchema(x_narrowed_schema, y_narrowed_schema));

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
    ASSIGN_OR_RETURN(
        auto result_ds,
        BuildFromFlat<OutputT>(std::move(result), std::move(out_shape)));
    if (is_object_schema) {
      return result_ds.WithSchema(internal::DataItem(schema::kObject));
    }
    return result_ds;
  };

  if (common_narrowed_schema == internal::DataItem(schema::kFloat32)) {
    return do_compute.template operator()<double, float>(
        internal::DataItem(schema::kFloat64),
        std::move(common_narrowed_schema));
  }
  if (common_narrowed_schema == internal::DataItem(schema::kFloat64)) {
    return do_compute.template operator()<double, double>(
        internal::DataItem(schema::kFloat64),
        std::move(common_narrowed_schema));
  }
  if (common_narrowed_schema == internal::DataItem(schema::kInt32)) {
    return do_compute.template operator()<int64_t, int32_t>(
        internal::DataItem(schema::kInt64), std::move(common_narrowed_schema));
  }
  DCHECK_EQ(common_narrowed_schema, internal::DataItem(schema::kInt64));
  return do_compute.template operator()<int64_t, int64_t>(
      internal::DataItem(schema::kInt64), std::move(common_narrowed_schema));
}

// Build a JaggedShape:
// batch_shape + row_edge(row_counts) + col_edge(col_counts).
// Used for 2D output of batched matrix ops (matmul, outer, solve).
absl::StatusOr<JaggedShape> BuildBatchedMatrixShape(
    JaggedShape batch_shape, absl::Span<const int64_t> row_counts,
    absl::Span<const int64_t> col_counts);

// Build a JaggedShape: batch_shape + vector_edge(counts).
// Used for 1D output of batched matrix ops (matmul 2D×1D, solve vector).
absl::StatusOr<JaggedShape> BuildBatchedVectorShape(
    JaggedShape batch_shape, absl::Span<const int64_t> counts);

}  // namespace koladata::ops::matrix_helpers

#endif  // KOLADATA_OPERATORS_MATRIX_HELPERS_H_
