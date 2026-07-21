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
#include "Eigen/Core"
#include "koladata/casting.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/schema_utils.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/operators/matrix_helpers.h"
#include "koladata/schema_utils.h"

namespace koladata::ops {

using JaggedShape = DataSlice::JaggedShape;
using Edge = JaggedShape::Edge;

namespace {

template <typename T>
using RowMajorMatrix =
    Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>;

}  // namespace

absl::StatusOr<DataSlice> MatrixTranspose(const DataSlice& x) {
  const auto& shape = x.GetShape();
  const int rank = shape.rank();

  ASSIGN_OR_RETURN(auto mat_infos, matrix_helpers::ExtractMatrix2DInfos(shape));
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
  flat_impl.VisitValues([&]<class T>(const arolla::DenseArray<T>& values) {
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
  return DataSlice::Create(std::move(builder).Build(), std::move(result_shape),
                           x.GetSchemaImpl(), x.GetBag(),
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

  ASSIGN_OR_RETURN((auto [broadcast_result, a_infos_vec, b_infos_vec]),
                   matrix_helpers::SetupBroadcastBinaryOp(
                       a.GetShape(), b.GetShape(), a_ndim, b_ndim));

  const bool is_object_schema =
      a.GetSchemaImpl() == internal::DataItem(schema::kObject) ||
      b.GetSchemaImpl() == internal::DataItem(schema::kObject);
  ASSIGN_OR_RETURN(auto a_narrowed_schema,
                   matrix_helpers::GetNarrowedMatrixSchema(a));
  ASSIGN_OR_RETURN(auto b_narrowed_schema,
                   matrix_helpers::GetNarrowedMatrixSchema(b));
  ASSIGN_OR_RETURN(auto common_narrowed_schema,
                   schema::CommonSchema(a_narrowed_schema, b_narrowed_schema));

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
      return matrix_helpers::BuildBatchedMatrixShape(std::move(batch_shape),
                                                     row_counts, col_counts);
    } else if (!a_ndim_is_1) {
      std::vector<int64_t> counts(broadcast_result.out_batch_size);
      for (int64_t b = 0; b < broadcast_result.out_batch_size; ++b) {
        counts[b] = pbs[b].a_rows;
      }
      return matrix_helpers::BuildBatchedVectorShape(std::move(batch_shape),
                                                     counts);
    } else if (!b_ndim_is_1) {
      std::vector<int64_t> counts(broadcast_result.out_batch_size);
      for (int64_t b = 0; b < broadcast_result.out_batch_size; ++b) {
        counts[b] = pbs[b].b_cols;
      }
      return matrix_helpers::BuildBatchedVectorShape(std::move(batch_shape),
                                                     counts);
    } else {
      return batch_shape;
    }
  }());

  // Compute in 64-bit precision (ComputeT), then cast to the output type
  // (OutputT) to preserve schema width. E.g. FLOAT32 inputs are computed
  // in FLOAT64 but the result is stored as FLOAT32.
  auto do_matmul =
      [&]<typename ComputeT, typename OutputT>(
          internal::DataItem compute_schema,
          internal::DataItem output_schema) -> absl::StatusOr<DataSlice> {
    ASSIGN_OR_RETURN(auto cast_a, CastToExplicit(a, compute_schema));
    ASSIGN_OR_RETURN(auto cast_b, CastToExplicit(b, compute_schema));
    auto a_flat = matrix_helpers::ExtractFlat<ComputeT>(cast_a);
    auto b_flat = matrix_helpers::ExtractFlat<ComputeT>(cast_b);
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
    ASSIGN_OR_RETURN(auto result_ds,
                     matrix_helpers::BuildFromFlat<OutputT>(
                         std::move(result), std::move(out_shape)));
    if (is_object_schema) {
      return result_ds.WithSchema(internal::DataItem(schema::kObject));
    }
    return result_ds;
  };
  if (common_narrowed_schema == internal::DataItem(schema::kFloat32)) {
    return do_matmul.operator()<double, float>(
        internal::DataItem(schema::kFloat64),
        std::move(common_narrowed_schema));
  }
  if (common_narrowed_schema == internal::DataItem(schema::kFloat64)) {
    return do_matmul.operator()<double, double>(
        internal::DataItem(schema::kFloat64),
        std::move(common_narrowed_schema));
  }
  if (common_narrowed_schema == internal::DataItem(schema::kInt32)) {
    return do_matmul.operator()<int64_t, int32_t>(
        internal::DataItem(schema::kInt64), std::move(common_narrowed_schema));
  }
  DCHECK_EQ(common_narrowed_schema, internal::DataItem(schema::kInt64));
  return do_matmul.operator()<int64_t, int64_t>(
      internal::DataItem(schema::kInt64), std::move(common_narrowed_schema));
}

absl::StatusOr<DataSlice> MatrixOuter(const DataSlice& x, const DataSlice& y) {
  if (x.GetShape().rank() < 1 || y.GetShape().rank() < 1) {
    return absl::InvalidArgumentError("inputs must have at least 1 dimension");
  }

  ASSIGN_OR_RETURN(auto setup, matrix_helpers::SetupBroadcastBinaryOp(
                                   x.GetShape(), y.GetShape(), 1, 1));
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
      matrix_helpers::BuildBatchedMatrixShape(
          std::move(broadcast_result.out_batch_shape), out_m_vals, out_n_vals));
  return matrix_helpers::DispatchNumericBinaryOp(
      x, y, out_total, std::move(out_shape), do_outer);
}

// Note: diag_matrix is a purely structural operation (placing values on the
// diagonal of a new matrix). It uses VisitValues to dispatch once on the
// runtime element type, then operates on the concrete DenseArray<T>.
// This gives a single code path for all types with no per-element boxing.
absl::StatusOr<DataSlice> MatrixDiagMatrix(const DataSlice& x) {
  const int rank = x.GetShape().rank();
  if (rank < 1) {
    return absl::InvalidArgumentError("expected at least 1D DataSlice, got 0D");
  }

  ASSIGN_OR_RETURN(const auto vec_infos,
                   matrix_helpers::ExtractVectorInfos(x.GetShape()));
  const int64_t num_vectors = vec_infos.size();

  int64_t out_total = 0;  // The total number of elements in the flat output.
  std::vector<int64_t> dim_counts(num_vectors);
  for (int64_t v = 0; v < num_vectors; ++v) {
    int64_t m = vec_infos[v].m;
    out_total += m * m;
    dim_counts[v] = m;
  }
  const int batch_rank = rank - 1;
  auto batch_shape = x.GetShape().RemoveDims(/*from=*/batch_rank);
  ASSIGN_OR_RETURN(auto out_shape,
                   matrix_helpers::BuildBatchedMatrixShape(
                       std::move(batch_shape), dim_counts, dim_counts));

  const DataSlice flat = x.Flatten();
  const auto& flat_impl = flat.slice();

  if (flat_impl.is_empty_and_unknown()) {
    auto empty = internal::DataSliceImpl::CreateEmptyAndUnknownType(out_total);
    return DataSlice::Create(std::move(empty), std::move(out_shape),
                             x.GetSchemaImpl(), x.GetBag());
  }

  // Scatter input elements onto the diagonal positions of the output.
  // Uses SliceBuilder, which handles any element type via DenseArray<T>.
  internal::SliceBuilder builder(out_total, flat_impl.allocation_ids());
  RETURN_IF_ERROR(flat_impl.VisitValues(
      [&]<class T>(const arolla::DenseArray<T>& array) -> absl::Status {
        arolla::DenseArrayBuilder<T> arr_builder(out_total);
        bool any_present = false;
        int64_t out_off = 0;
        for (const auto& vec_info : vec_infos) {
          int64_t m = vec_info.m;
          int64_t in_off = vec_info.offset;
          for (int64_t i = 0; i < m; ++i) {
            if (array.present(in_off + i)) {
              arr_builder.Set(out_off + i * m + i, array.values[in_off + i]);
              any_present = true;
            }
          }
          out_off += m * m;
        }
        if (any_present) {
          auto result_array = std::move(arr_builder).Build();
          builder.InsertIfNotSet<T>(result_array.bitmap, {},
                                    result_array.values);
        }
        return absl::OkStatus();
      }));

  auto result_impl = std::move(builder).Build();
  return DataSlice::Create(std::move(result_impl), std::move(out_shape),
                           x.GetSchemaImpl(), x.GetBag());
}

// Note: diag_vector is a purely structural operation (extracting diagonal
// values from a matrix). It uses VisitValues to dispatch once on the runtime
// element type, then gathers from the concrete DenseArray<T>.
absl::StatusOr<DataSlice> MatrixDiagVector(const DataSlice& x) {
  const int rank = x.GetShape().rank();
  if (rank < 2) {
    return absl::InvalidArgumentError(
        absl::StrCat("expected at least 2D DataSlice, got ", rank, "D"));
  }

  ASSIGN_OR_RETURN(const auto mat_infos,
                   matrix_helpers::ExtractMatrix2DInfos(x.GetShape()));
  const int64_t num_matrices = mat_infos.size();

  int64_t out_total = 0;  // The total number of elements in the flat output.
  // Each matrix has k = min(m, n) diagonal elements, where m = rows, n = cols.
  std::vector<int64_t> k_vals(num_matrices);
  for (int64_t p = 0; p < num_matrices; ++p) {
    const auto& mat_info = mat_infos[p];
    int64_t k = std::min(mat_info.m, mat_info.n);
    k_vals[p] = k;
    out_total += k;
  }

  const int batch_rank = rank - 2;
  auto batch_shape = x.GetShape().RemoveDims(/*from=*/batch_rank);
  ASSIGN_OR_RETURN(auto out_shape, matrix_helpers::BuildBatchedVectorShape(
                                       std::move(batch_shape), k_vals));

  const DataSlice flat = x.Flatten();
  const auto& flat_impl = flat.slice();

  if (flat_impl.is_empty_and_unknown()) {
    auto empty = internal::DataSliceImpl::CreateEmptyAndUnknownType(out_total);
    return DataSlice::Create(std::move(empty), std::move(out_shape),
                             x.GetSchemaImpl(), x.GetBag());
  }

  // Gather diagonal elements from each matrix.
  // Uses SliceBuilder, which handles any element type via DenseArray<T>.
  internal::SliceBuilder builder(out_total, flat_impl.allocation_ids());
  RETURN_IF_ERROR(flat_impl.VisitValues(
      [&]<class T>(const arolla::DenseArray<T>& array) -> absl::Status {
        arolla::DenseArrayBuilder<T> arr_builder(out_total);
        bool any_present = false;
        int64_t out_off = 0;
        for (int64_t p = 0; p < num_matrices; ++p) {
          const int64_t n = mat_infos[p].n;  // Number of columns in this matrix
          const int64_t in_off = mat_infos[p].offset;
          for (int64_t i = 0; i < k_vals[p]; ++i) {
            const int64_t src_idx = in_off + i * n + i;
            if (array.present(src_idx)) {
              arr_builder.Set(out_off + i, array.values[src_idx]);
              any_present = true;
            }
          }
          out_off += k_vals[p];
        }
        if (any_present) {
          auto result_array = std::move(arr_builder).Build();
          builder.InsertIfNotSet<T>(result_array.bitmap, {},
                                    result_array.values);
        }
        return absl::OkStatus();
      }));

  auto result_impl = std::move(builder).Build();
  return DataSlice::Create(std::move(result_impl), std::move(out_shape),
                           x.GetSchemaImpl(), x.GetBag());
}

}  // namespace koladata::ops
