# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Matrix operations for Koda.

The kd.matrix library provides fully vectorized support of batches of
independent matrices. Leading dimensions are interpreted as batch dimensions.
In operators that take 2 or more matrix arguments, the batch dimensions are
subject to standard Koda broadcasting rules.
"""

from arolla import arolla
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import data_slice

P = arolla.P

optools.set_namespace_docstring('kd.matrix', __doc__)


# ---- Public operators ----


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_backend_operator(
    'kd.matrix.transpose',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def transpose(x):  # pylint: disable=unused-argument
  """Transpose a matrix (swap last two dimensions).

  Supports leading batch dimensions: (..., m, n) -> (..., n, m).
  Leading batch dimensions (all except the last two) can be jagged.
  The last two dimensions must be uniform within each matrix entry (i.e.,
  every row of a given matrix must have the same number of columns), but
  different matrix entries can have different shapes.
  Preserves sparsity: None values remain None.
  Works with any schema, including numeric, TEXT, BYTES, and entities.

  Args:
    x: A DataSlice with at least 2 dimensions. The last two dimensions must be
      uniform within each matrix entry, but leading batch dimensions can be
      jagged.

  Returns:
    The transposed DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_backend_operator(
    'kd.matrix.matmul',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.a),
        qtype_utils.expect_data_slice(P.b),
        qtype_utils.expect_data_slice(P.a_ndim),
        qtype_utils.expect_data_slice(P.b_ndim),
    ],
)
# pylint: disable=unused-argument
def matmul(
    a,
    b,
    *,
    a_ndim=data_slice.DataSlice.from_vals(-1),
    b_ndim=data_slice.DataSlice.from_vals(-1),
):
  # pylint: enable=unused-argument
  """Matrix multiplication.

  Supports:
    2D x 2D -> 2D: (m,k) @ (k,n) -> (m,n)
    2D x 1D -> 1D: (m,k) @ (k,) -> (m,)
    1D x 2D -> 1D: (k,) @ (k,n) -> (n,)
    1D x 1D -> 0D: dot product
    ND x MD -> batched matmul with broadcasting leading dimensions. The batch
      dimensions (all dimensions except the last `a_ndim` or `b_ndim` dims) of
      one input must be a prefix of the batch dims of the other. The
      shorter-batch input is implicitly broadcast.

  The `a_ndim` and `b_ndim` parameters control how many trailing dimensions
  are treated as matrix dimensions for each input. Valid values are 1 or 2.
  When set to -1 (the default), defaults to 2 if the input has rank >= 2,
  or 1 if the input has rank 1.

  This is useful when both inputs have rank >= 2 but one should be treated
  as a batch of vectors (ndim=1) rather than a batch of matrices (ndim=2).

  Examples:
    matmul(shape (2, 5, 6), shape (2, 3, 6, 7)) -> shape (2, 3, 5, 7):
      a batch (2,) is prefix of b batch (2, 3), so a is broadcast.
    matmul(shape (m, k), shape (B, k, n)) -> shape (B, m, n):
      2D a has 0 batch dims, broadcast across B.
    matmul(shape (B, k), shape (B, k, n), a_ndim=1) -> shape (B, n):
      a is treated as a batch of vectors, not a matrix.

  None values are treated as 0.

  Args:
    a: A numeric DataSlice with at least 1 dimension.
    b: A numeric DataSlice with at least 1 dimension.
    a_ndim: Scalar integer. Number of trailing dimensions of `a` to use as
      matrix dimensions (1 or 2). Defaults to -1, meaning min(rank(a), 2).
    b_ndim: Scalar integer. Number of trailing dimensions of `b` to use as
      matrix dimensions (1 or 2). Defaults to -1, meaning min(rank(b), 2).

  Returns:
    The result of the matrix multiplication.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.matrix.dot',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
)
def dot(x, y):
  """Dot product along the last dimension.

  Computes sum(x * y) along the last dimension.
  Supports leading batch dimensions with Koda prefix broadcasting:
    (..., n) x (..., n) -> (...)
  The batch dimensions (all dimensions except the last) of one input must be
  a prefix of the batch dimensions of the other input. The shorter-batch
  input is implicitly broadcast.

  Examples:
    (3,) x (3,) -> ()               # no batch dims
    (2, 3) x (2, 3) -> (2,)         # matching batch dims
    (3,) x (2, 3) -> (2,)           # x batch () is prefix of y batch (2,)
    (2, 3, 4) x (2, 4) -> (2, 3)    # y batch (2,) is prefix of x batch (2, 3)

  None values are treated as 0.

  Args:
    x: A numeric DataSlice with at least 1 dimension.
    y: A numeric DataSlice with at least 1 dimension.

  Returns:
    A DataSlice with the dot product value(s).
  """
  return matmul(
      x,
      y,
      a_ndim=data_slice.DataSlice.from_vals(1),
      b_ndim=data_slice.DataSlice.from_vals(1),
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_backend_operator(
    'kd.matrix.outer',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
)
def outer(x, y):  # pylint: disable=unused-argument
  """Outer product of vectors.

  For vectors of shape (m,) and (n,), returns a matrix of shape (m, n)
  where result[i, j] = x[i] * y[j].

  Supports leading batch dimensions with Koda prefix broadcasting:
    (..., m) x (..., n) -> (..., m, n)
  The batch dimensions (all dimensions except the last) of one input must be
  a prefix of the batch dimensions of the other input. The shorter-batch
  input is implicitly broadcast.

  Examples:
    (3,) x (4,) -> (3, 4)          # no batch dims
    (2, 3) x (2, 4) -> (2, 3, 4)  # matching batch dims
    (3,) x (2, 4) -> (2, 3, 4)    # x batch () is prefix of y batch (2,)
    (2, 3, 5) x (2, 7) -> (2, 3, 5, 7)  # y batch (2,) is prefix of x

  None values are treated as 0.

  Args:
    x: A numeric DataSlice with at least 1 dimension.
    y: A numeric DataSlice with at least 1 dimension.

  Returns:
    The outer product matrix (or batch of matrices).
  """
  # One could argue that outer should propagate missing values, and not treat
  # them as 0. That would be consistent with Koda's semantics for
  # multiplication, because
  # kd.int32(1) * kd.int32(None) yields kd.int32(None).
  # All of the other matrix operators here, such as matmul, dot and trace, treat
  # missing values as 0, which is consistent with Koda's semantics for
  # summation:
  # kd.sum(kd.slice([None, None])) yields kd.int32(0).
  # So we could go either way. For speed and consistency with the other matrix
  # operators, we follow the convention here to treat missing values as 0.
  # Aside: the discussion above illustrates that Koda itself is somewhat
  # inconsistent in its treatment of missing values, since
  # kd.int32(None) + kd.int32(None) is not the same as
  # kd.sum(kd.slice([None, None], kd.INT32)).
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_backend_operator(
    'kd.matrix.diag_matrix',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.k),
    ],
)
# pylint: disable=unused-argument
def diag_matrix(x, *, k=data_slice.DataSlice.from_vals(0)):
  # pylint: enable=unused-argument
  """Create a diagonal matrix from the last dimension.

  Takes the last 1D of the input as a vector and places its elements on the
  k-th diagonal of a matrix. For input shape (..., n), returns shape
  (..., n+|k|, n+|k|) where the specified diagonal entries are set and all
  other entries are None (sparse).

  The `k` parameter controls which diagonal to fill:
    k = 0  (default): main diagonal.
    k > 0: k-th super-diagonal (above the main diagonal).
    k < 0: |k|-th sub-diagonal (below the main diagonal).

  Preserves sparsity. Works with any schema, including numeric, TEXT, BYTES,
  and entities.

  Args:
    x: A DataSlice with at least 1 dimension.
    k: Integer DataSlice. Diagonal offset. Must be broadcastable to the batch
      dimensions of `x`. 0 (default) is the main diagonal, positive values
      refer to super-diagonals, negative values refer to sub-diagonals.

  Returns:
    A DataSlice with one additional dimension, containing diagonal matrices.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_backend_operator(
    'kd.matrix.diag_vector',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.k),
    ],
)
# pylint: disable=unused-argument
def diag_vector(x, *, k=data_slice.DataSlice.from_vals(0)):
  # pylint: enable=unused-argument
  """Extract a diagonal from the last two dimensions.

  Takes the last 2D of the input as a matrix and extracts its k-th diagonal.
  For input shape (..., m, n), returns shape (..., max(0, min(m, n-k))) when
  k >= 0, or (..., max(0, min(m+k, n))) when k < 0.

  The `k` parameter controls which diagonal to extract:
    k = 0  (default): main diagonal.
    k > 0: k-th super-diagonal (above the main diagonal).
    k < 0: |k|-th sub-diagonal (below the main diagonal).

  Preserves sparsity. Works with any schema, including numeric, TEXT, BYTES,
  and entities.

  Args:
    x: A DataSlice with at least 2 dimensions.
    k: Integer DataSlice. Diagonal offset. Must be broadcastable to the batch
      dimensions of `x`. 0 (default) is the main diagonal, positive values
      refer to super-diagonals, negative values refer to sub-diagonals.

  Returns:
    A DataSlice with one fewer dimension, containing the requested diagonal
    vectors.
  """
  raise NotImplementedError('implemented in the backend')
