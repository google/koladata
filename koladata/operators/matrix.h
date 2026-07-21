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
#ifndef KOLADATA_OPERATORS_MATRIX_H_
#define KOLADATA_OPERATORS_MATRIX_H_

#include "absl/status/statusor.h"
#include "koladata/data_slice.h"

namespace koladata::ops {

// kd.matrix.transpose: Transpose a 2D+ DataSlice (swap last two dims).
// Supports batch dimensions: (..., m, n) -> (..., n, m).
absl::StatusOr<DataSlice> MatrixTranspose(const DataSlice& x);

// kd.matrix.matmul: Matrix multiplication with batch support.
// a_ndim/b_ndim control how many trailing dims are matrix dims (1 or 2). A
// value of -1 means auto-detect: min(rank, 2).
absl::StatusOr<DataSlice> MatrixMatmul(const DataSlice& a, const DataSlice& b,
                                       const DataSlice& a_ndim,
                                       const DataSlice& b_ndim);

// kd.matrix.outer: Outer product with batch + broadcast support.
// (..., m) x (..., n) -> (..., m, n). Batch dims are broadcast.
absl::StatusOr<DataSlice> MatrixOuter(const DataSlice& x, const DataSlice& y);

// kd.matrix.diag_matrix: Creates a diagonal matrix from a vector.
// (..., n) -> (..., n, n). Off-diagonal elements are None.
absl::StatusOr<DataSlice> MatrixDiagMatrix(const DataSlice& x);

// kd.matrix.diag_vector: Extracts the diagonal vector from a matrix.
// (..., m, n) -> (..., min(m,n)).
absl::StatusOr<DataSlice> MatrixDiagVector(const DataSlice& x);

}  // namespace koladata::ops

#endif  // KOLADATA_OPERATORS_MATRIX_H_
