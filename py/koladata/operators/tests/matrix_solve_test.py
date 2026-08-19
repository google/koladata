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

import math

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants
import numpy as np

I = input_container.InputContainer('I')
kde = kde_operators.kde
kd = kde_operators.kd
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([
    # (a, b) -> result:
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    # (a, b, b_ndim) -> result:
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class MatrixSolveTest(parameterized.TestCase):

  def test_empty_zero_by_zero(self):
    # 0×0 matrix with empty RHS: should produce an empty result.
    a = kd.empty_shaped(kd.shapes.new(0, 0), schema_constants.FLOAT32)
    b = kd.empty_shaped(kd.shapes.new(0), schema_constants.FLOAT32)
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(
        result,
        kd.empty_shaped(kd.shapes.new(0), schema_constants.FLOAT32),
    )

  def test_basic(self):
    a = ds([[1.0, 2.0], [3.0, 5.0]])
    b = ds([1.0, 2.0])
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(result, ds([-1.0, 1.0], schema_constants.FLOAT32))
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

  def test_basic_float64(self):
    a = ds([[1.0, 2.0], [3.0, 5.0]], schema_constants.FLOAT64)
    b = ds([1.0, 2.0], schema_constants.FLOAT64)
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(result, ds([-1.0, 1.0], schema_constants.FLOAT64))
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

  def test_integer_int32_output_float32(self):
    # INT32 inputs → solve computes in FLOAT64, output cast to FLOAT32.
    a = ds([[1, 0], [0, 1]])  # INT32
    b = ds([3, 4])  # INT32
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(result, ds([3.0, 4.0], schema_constants.FLOAT32))
    testing.assert_allclose(
        kd.matrix.matmul(a, result),
        kd.cast_to(b, schema_constants.FLOAT32),
        atol=1e-5,
    )

  def test_int64_input(self):
    a = ds([[1, 0], [0, 1]], schema_constants.INT64)
    b = ds([3, 4], schema_constants.INT64)
    result = kd.matrix.solve(a, b)
    # INT64 inputs → output is FLOAT32. This is similar to kd.math.divide's
    # behavior.
    testing.assert_allclose(result, ds([3.0, 4.0], schema_constants.FLOAT32))
    testing.assert_allclose(
        kd.matrix.matmul(a, result),
        kd.cast_to(b, schema_constants.FLOAT32),
        atol=1e-5,
    )

  def test_mixed_schemas_int32_float32(self):
    a = ds([[1, 2], [3, 5]])  # INT32
    b = ds([1.0, 2.0])  # FLOAT32
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(result, ds([-1.0, 1.0], schema_constants.FLOAT32))
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

  def test_mixed_schemas_int64_float32(self):
    # INT64 is 64-bit, so the output is FLOAT64.
    a = ds([[1, 2], [3, 5]], schema_constants.INT64)
    b = ds([1.0, 2.0])  # FLOAT32
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(result, ds([-1.0, 1.0], schema_constants.FLOAT32))
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

  def test_mixed_schemas_float32_float64(self):
    a = ds([[1.0, 2.0], [3.0, 5.0]])  # FLOAT32
    b = ds([1.0, 2.0], schema_constants.FLOAT64)  # FLOAT64
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(result, ds([-1.0, 1.0], schema_constants.FLOAT64))
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

  def test_vs_numpy(self):
    a_np = np.array([[2.0, 1.0], [5.0, 3.0]])
    b_np = np.array([4.0, 7.0])
    expected = np.linalg.solve(a_np, b_np)
    a = ds(a_np.tolist())
    b = ds(b_np.tolist())
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-5
    )
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

  def test_matrix_rhs(self):
    a_np = np.array([[1.0, 2.0], [3.0, 4.0]])
    b_np = np.array([[1.0, 0.0], [0.0, 1.0]])
    expected = np.linalg.solve(a_np, b_np)
    a = ds(a_np.tolist())
    b = ds(b_np.tolist())
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-5
    )
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

  def test_non_square_matrix_rhs(self):
    # A is 2x2, b is 2x3 (more columns than rows).
    a = ds([[1.0, 2.0], [3.0, 5.0]])
    b = ds([[1.0, 0.0, 2.0], [0.0, 1.0, 3.0]])
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(
        result,
        ds([[-5.0, 2.0, -4.0], [3.0, -1.0, 3.0]], schema_constants.FLOAT32),
        atol=1e-5,
    )
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

  def test_batched_vector_rhs(self):
    # 2 systems: A shape (2, 2, 2), b shape (2, 2).
    a = ds([
        [[1.0, 2.0], [3.0, 5.0]],
        [[2.0, 1.0], [1.0, 3.0]],
    ])
    b = ds([
        [1.0, 2.0],
        [5.0, 5.0],
    ])
    result = kd.matrix.solve(a, b, b_ndim=1)
    testing.assert_allclose(
        result,
        ds([[-1.0, 1.0], [2.0, 1.0]], schema_constants.FLOAT32),
    )
    testing.assert_allclose(kd.matrix.matmul(a, result, b_ndim=1), b, atol=1e-5)

  def test_batched_matrix_rhs(self):
    # 2 systems: A shape (2, 2, 2), b shape (2, 2, 2).
    a_np = np.array([
        [[1.0, 2.0], [3.0, 4.0]],
        [[2.0, 1.0], [1.0, 3.0]],
    ])
    b_np = np.array([
        [[1.0, 0.0], [0.0, 1.0]],
        [[1.0, 0.0], [0.0, 1.0]],
    ])
    expected = np.linalg.solve(a_np, b_np)
    a = ds(a_np.tolist())
    b = ds(b_np.tolist())
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-5
    )
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

  def test_broadcast_a_2d_b_3d_matrix_rhs(self):
    # A is (n,n), b is (B, n, m). A is broadcast across batch B.
    a = ds([[1.0, 2.0], [3.0, 5.0]])  # (2, 2)
    b = ds([
        [[1.0, 0.0], [0.0, 1.0]],
        [[2.0, 0.0], [0.0, 2.0]],
    ])  # (2, 2, 2)
    result = kd.matrix.solve(a, b)
    a_np = np.array([[1.0, 2.0], [3.0, 5.0]])
    expected_0 = np.linalg.solve(a_np, np.array([[1.0, 0.0], [0.0, 1.0]]))
    expected_1 = np.linalg.solve(a_np, np.array([[2.0, 0.0], [0.0, 2.0]]))
    testing.assert_allclose(
        result,
        ds(
            [expected_0.tolist(), expected_1.tolist()], schema_constants.FLOAT32
        ),
        atol=1e-5,
    )
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

  def test_broadcast_a_3d_b_2d_matrix_rhs(self):
    # A is (B, n, n), b is (n, m). b is broadcast across batch B.
    a = ds([
        [[1.0, 2.0], [3.0, 5.0]],
        [[2.0, 1.0], [1.0, 3.0]],
    ])  # (2, 2, 2)
    b = ds([[1.0, 0.0], [0.0, 1.0]])  # identity (2, 2)
    result = kd.matrix.solve(a, b)
    b_np = np.array([[1.0, 0.0], [0.0, 1.0]])
    expected_0 = np.linalg.solve(
        np.array([[1.0, 2.0], [3.0, 5.0]]),
        b_np,
    )
    expected_1 = np.linalg.solve(
        np.array([[2.0, 1.0], [1.0, 3.0]]),
        b_np,
    )
    testing.assert_allclose(
        result,
        ds(
            [expected_0.tolist(), expected_1.tolist()], schema_constants.FLOAT32
        ),
        atol=1e-5,
    )
    # b was broadcast across the batch dimension, so compare against the
    # explicitly broadcast version.
    b_broadcast = ds([[[1.0, 0.0], [0.0, 1.0]], [[1.0, 0.0], [0.0, 1.0]]])
    testing.assert_allclose(kd.matrix.matmul(a, result), b_broadcast, atol=1e-5)

  def test_b_ndim_1_batched_vector_solve(self):
    # A is (B, n, n), b is (B, n). With b_ndim=1, b is batch of vectors.
    a = ds([
        [[1.0, 2.0], [3.0, 5.0]],
        [[2.0, 1.0], [1.0, 3.0]],
    ])  # (2, 2, 2)
    b = ds([
        [1.0, 2.0],
        [5.0, 5.0],
    ])  # (2, 2)
    result = kd.matrix.solve(a, b, b_ndim=1)
    testing.assert_allclose(
        result,
        ds([[-1.0, 1.0], [2.0, 1.0]], schema_constants.FLOAT32),
    )
    testing.assert_allclose(kd.matrix.matmul(a, result, b_ndim=1), b, atol=1e-5)

  def test_b_ndim_1_broadcast_vector_solve(self):
    # A is (B, n, n), b is (n,). b broadcast across batch B with b_ndim=1.
    a = ds([
        [[1.0, 2.0], [3.0, 5.0]],
        [[2.0, 1.0], [1.0, 3.0]],
    ])  # (2, 2, 2)
    b = ds([1.0, 2.0])  # (2,)
    result = kd.matrix.solve(a, b, b_ndim=1)
    b_np = np.array([1.0, 2.0])
    expected_0 = np.linalg.solve(np.array([[1.0, 2.0], [3.0, 5.0]]), b_np)
    expected_1 = np.linalg.solve(np.array([[2.0, 1.0], [1.0, 3.0]]), b_np)
    testing.assert_allclose(
        result,
        ds(
            [expected_0.tolist(), expected_1.tolist()], schema_constants.FLOAT32
        ),
        atol=1e-5,
    )
    # b was broadcast across the batch dimension.
    b_broadcast = ds([[1.0, 2.0], [1.0, 2.0]])
    testing.assert_allclose(
        kd.matrix.matmul(a, result, b_ndim=1), b_broadcast, atol=1e-5
    )

  def test_jagged_matrix_dims_vector_rhs(self):
    a = ds([[[2.0, 0.0], [0.0, 3.0]], [[5.0]]])
    b = ds([[4.0, 9.0], [10.0]])
    result = kd.matrix.solve(a, b, b_ndim=1)
    testing.assert_allclose(
        result, ds([[2.0, 3.0], [2.0]], schema_constants.FLOAT32)
    )
    testing.assert_allclose(kd.matrix.matmul(a, result, b_ndim=1), b, atol=1e-5)

  def test_jagged_matrix_dims_with_broadcast(self):
    a = ds([
        [[[2.0, 0.0], [0.0, 4.0]], [[1.0, 0.0], [0.0, 2.0]]],
        [[[5.0]], [[3.0]]],
    ])
    b = ds([[6.0, 8.0], [10.0]])
    result = kd.matrix.solve(a, b, b_ndim=1)
    testing.assert_allclose(
        result,
        ds(
            [[[3.0, 2.0], [6.0, 4.0]], [[2.0], [10.0 / 3.0]]],
            schema_constants.FLOAT32,
        ),
        atol=1e-5,
    )
    # b was broadcast across the inner batch dimension.
    b_broadcast = ds([[[6.0, 8.0], [6.0, 8.0]], [[10.0], [10.0]]])
    testing.assert_allclose(
        kd.matrix.matmul(a, result, b_ndim=1), b_broadcast, atol=1e-5
    )

  def test_sparse_data(self):
    # None values are treated as 0 in matrix operations.
    a = ds([[1.0, None], [None, 1.0]])
    b = ds([3.0, 4.0])
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(result, ds([3.0, 4.0]))
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

  def test_object_schema(self):
    # OBJECT schema inputs should produce OBJECT schema output.
    a = kd.obj(ds([[1.0, 0.0], [0.0, 1.0]]))
    b = kd.obj(ds([3.0, 4.0]))
    result = kd.matrix.solve(a, b)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        ds([schema_constants.FLOAT32, schema_constants.FLOAT32]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT32),
        ds([3.0, 4.0], schema_constants.FLOAT32),
    )
    testing.assert_allclose(
        kd.matrix.matmul(a, result).no_bag(),
        b.no_bag(),
        atol=1e-5,
    )

  def test_object_schema_mixed_with_typed(self):
    # Mixed: one OBJECT, one FLOAT32.
    a = kd.obj(ds([[1.0, 0.0], [0.0, 1.0]]))
    b = ds([3.0, 4.0])  # FLOAT32
    result = kd.matrix.solve(a, b)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        ds([schema_constants.FLOAT32, schema_constants.FLOAT32]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT32),
        ds([3.0, 4.0]),
    )
    testing.assert_allclose(
        kd.cast_to(kd.matrix.matmul(a, result), schema_constants.FLOAT32),
        b,
        atol=1e-5,
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.solve,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.solve(I.a, I.b)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify the conceptual equivalence."""

  def test_solve_vs_numpy(self):
    a_np = np.array([[3.0, 1.0], [1.0, 2.0]])
    b_np = np.array([9.0, 8.0])
    expected = np.linalg.solve(a_np, b_np)
    a = ds(a_np.tolist())
    b = ds(b_np.tolist())
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-5
    )
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

  def test_solve_matrix_rhs_vs_numpy(self):
    a_np = np.array([[3.0, 1.0], [1.0, 2.0]])
    b_np = np.array([[9.0, 4.0], [8.0, 3.0]])
    expected = np.linalg.solve(a_np, b_np)
    a = ds(a_np.tolist())
    b = ds(b_np.tolist())
    result = kd.matrix.solve(a, b)
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-5
    )
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

  def test_solve_batched_vs_numpy(self):
    rng = np.random.default_rng(123)
    batch_size = 4
    n = 3
    a_np = rng.standard_normal((batch_size, n, n)) + np.eye(n) * n
    b_np = rng.standard_normal((batch_size, n))
    a = ds(a_np.tolist())
    b = ds(b_np.tolist())
    result = kd.matrix.solve(a, b, b_ndim=1)
    expected = [
        np.linalg.solve(a_np[i], b_np[i]).tolist() for i in range(batch_size)
    ]
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )
    testing.assert_allclose(kd.matrix.matmul(a, result, b_ndim=1), b, atol=1e-5)

  def test_solve_large_vs_numpy(self):
    rng = np.random.default_rng(456)
    n = 10
    a_np = rng.standard_normal((n, n)) + np.eye(n) * n
    b_np = rng.standard_normal(n)
    expected = np.linalg.solve(a_np, b_np)
    a = ds(a_np.tolist())
    b = ds(b_np.tolist())
    result = kd.matrix.solve(a, b, b_ndim=1)
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-5
    )
    testing.assert_allclose(kd.matrix.matmul(a, result, b_ndim=1), b, atol=1e-5)


class ErrorTest(parameterized.TestCase):
  """Exhaustive tests for error messages."""

  def test_solve_a_0d_fails(self):
    a = ds(1.0)
    b = ds([1.0])
    with self.assertRaisesRegex(
        ValueError, r'solve.*A must be at least 2D.*got 0D'
    ):
      kd.matrix.solve(a, b)

  def test_solve_a_1d_fails(self):
    a = ds([1.0, 2.0])
    b = ds([1.0, 2.0])
    with self.assertRaisesRegex(
        ValueError, r'solve.*A must be at least 2D.*got 1D'
    ):
      kd.matrix.solve(a, b)

  def test_solve_b_0d_fails(self):
    a = ds([[1.0, 0.0], [0.0, 1.0]])
    b = ds(1.0)
    with self.assertRaisesRegex(
        ValueError, r'solve.*b must have at least 1 dimension'
    ):
      kd.matrix.solve(a, b)

  def test_solve_non_square_matrix_fails(self):
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])  # 2x3
    b = ds([1.0, 2.0])
    with self.assertRaisesRegex(ValueError, r'solve.*not square.*2.*3'):
      kd.matrix.solve(a, b)

  def test_solve_dimension_mismatch_vector_fails(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])  # 2x2
    b = ds([1.0, 2.0, 3.0])  # 3 elements, but A is 2x2
    with self.assertRaisesRegex(ValueError, r'solve.*dimension mismatch'):
      kd.matrix.solve(a, b)

  def test_solve_dimension_mismatch_matrix_fails(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])  # 2x2
    b = ds([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])  # 3x2 (3 != 2)
    with self.assertRaisesRegex(ValueError, r'solve.*dimension mismatch'):
      kd.matrix.solve(a, b)

  def test_solve_b_ndim_0_fails(self):
    a = ds([[1.0, 0.0], [0.0, 1.0]])
    b = ds([1.0, 2.0])
    with self.assertRaisesRegex(
        ValueError, r'solve.*b_ndim must be 1, 2, or -1 \(auto\)'
    ):
      kd.matrix.solve(a, b, b_ndim=0)

  def test_solve_b_ndim_3_fails(self):
    a = ds([[1.0, 0.0], [0.0, 1.0]])
    b = ds([1.0, 2.0])
    with self.assertRaisesRegex(
        ValueError, r'solve.*b_ndim must be 1, 2, or -1 \(auto\)'
    ):
      kd.matrix.solve(a, b, b_ndim=3)

  def test_solve_b_rank_less_than_b_ndim_fails(self):
    a = ds([[1.0, 0.0], [0.0, 1.0]])
    b = ds([1.0, 2.0])  # rank 1
    with self.assertRaisesRegex(
        ValueError, r'solve.*b has rank 1 but b_ndim=2'
    ):
      kd.matrix.solve(a, b, b_ndim=2)

  def test_solve_non_scalar_b_ndim_fails(self):
    a = ds([[1.0, 0.0], [0.0, 1.0]])
    b = ds([1.0, 2.0])
    with self.assertRaisesRegex(
        ValueError,
        r'argument `b_ndim` must be an item holding INT64, got a slice of rank'
        r' 1 > 0',
    ):
      kd.matrix.solve(a, b, b_ndim=ds([1]))

  def test_solve_batched_non_square_fails(self):
    # Batch of 2 matrices, second one is non-square.
    a = ds([
        [[1.0, 0.0], [0.0, 1.0]],
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
    ])
    b = ds([[1.0, 2.0], [1.0, 2.0, 3.0]])
    with self.assertRaisesRegex(ValueError, r'solve.*not square'):
      kd.matrix.solve(a, b, b_ndim=1)

  def test_broadcast_mismatched_batch_dims_fails(self):
    # a batch (2,) is NOT a prefix of b batch (3,).
    a = ds([
        [[1.0, 0.0], [0.0, 1.0]],
        [[1.0, 0.0], [0.0, 1.0]],
    ])  # (2, 2, 2)
    b = ds([
        [[1.0, 0.0], [0.0, 1.0]],
        [[1.0, 0.0], [0.0, 1.0]],
        [[1.0, 0.0], [0.0, 1.0]],
    ])  # (3, 2, 2)
    with self.assertRaisesRegex(
        ValueError, 'batch dimensions are not compatible'
    ):
      kd.matrix.solve(a, b)

  def test_solve_string_schema_fails(self):
    a = ds([['a', 'b'], ['c', 'd']])
    b = ds([['e', 'f'], ['g', 'h']])
    with self.assertRaisesRegex(ValueError, 'unsupported.* schema: STRING'):
      kd.matrix.solve(a, b)

  def test_solve_overflow_matrix_n_times_nrhs_fails(self):
    # The solution X has the same shape as B, so a single B whose size
    # overflows int64 could not be constructed in the first place (building
    # its JaggedShape split points would already overflow int64). We therefore
    # broadcast B across a batch of A: each batch element contributes
    # n * nrhs = 1 * 2^62 = 2^62 output elements (which fits in int64), while
    # the total output size across the 2 batch elements (2 * 2^62 = 2^63)
    # overflows int64. `a` is (2, 1, 1) and `b` is (1, 2^62) broadcast across
    # the batch, so both inputs stay representable.
    with self.assertRaisesRegex(
        ValueError, r'arguments cause integer overflow'
    ):
      kd.matrix.solve(
          kd.empty_shaped(kd.shapes.new(2, 1, 1), schema_constants.FLOAT32),
          kd.empty_shaped(kd.shapes.new(1, 2**62), schema_constants.FLOAT32),
      )

  def test_solve_overflow_accumulation_across_batches_fails(self):
    # Each of the 4 batch elements produces n * nrhs = 1 * 2^61 = 2^61 output
    # elements (which fits in int64), but the total across all batches
    # (4 * 2^61 = 2^63) overflows int64. `a` is (4, 1, 1) and `b` is (1, 2^61)
    # broadcast across the batch, so both inputs stay representable.
    with self.assertRaisesRegex(
        ValueError, r'arguments cause integer overflow'
    ):
      kd.matrix.solve(
          kd.empty_shaped(kd.shapes.new(4, 1, 1), schema_constants.FLOAT32),
          kd.empty_shaped(kd.shapes.new(1, 2**61), schema_constants.FLOAT32),
      )

  # --- solve singular matrix (implementation behavior, not API guarantees) ---
  #
  # The API only guarantees correct results for invertible matrices. The tests
  # below document the *current* behavior of partialPivLu on singular and
  # near-singular inputs. This behavior is not promised by the API and may
  # change in future implementations.

  def test_solve_singular_consistent_returns_a_solution(self):
    # Current behavior: singular A with consistent RHS silently returns a
    # valid solution (one of the infinitely many). This is not guaranteed by
    # the API.
    a = ds([[1.0, 2.0], [2.0, 4.0]])  # singular: row2 = 2*row1
    b = ds([1.0, 2.0])  # consistent: b = col1 of A
    result = kd.matrix.solve(a, b)
    # Verify the solution satisfies A @ x = b.
    testing.assert_allclose(kd.matrix.matmul(a, result), b, atol=1e-5)

    # NumPy behavior for the same system:
    a_np = np.array([[1.0, 2.0], [2.0, 4.0]])
    b_np = np.array([1.0, 2.0])
    with self.assertRaisesRegex(np.linalg.LinAlgError, 'Singular matrix'):
      np.linalg.solve(a_np, b_np)

  def test_solve_singular_inconsistent_returns_inf_or_nan(self):
    # Current behavior for exactly-singular A with inconsistent RHS: LU
    # back-substitution hits a zero pivot and produces inf/nan. This only
    # happens when the pivot is *exactly* zero in floating point — see
    # test_solve_near_singular_returns_large_finite_values for the more
    # realistic near-singular case where this breaks down.
    a = ds([[1.0, 2.0], [2.0, 4.0]])  # singular
    b = ds([1.0, 3.0])  # inconsistent: b[1] != 2*b[0]
    result = kd.matrix.solve(a, b)
    flat = result.flatten().to_py()
    self.assertTrue(
        all(math.isinf(v) or math.isnan(v) for v in flat),
        f'Expected inf or nan for inconsistent singular solve, got {flat}',
    )

    # NumPy behavior for the same system:
    a_np = np.array([[1.0, 2.0], [2.0, 4.0]])
    b_np = np.array([1.0, 3.0])
    with self.assertRaisesRegex(np.linalg.LinAlgError, 'Singular matrix'):
      np.linalg.solve(a_np, b_np)

  def test_solve_near_singular_returns_large_finite_values(self):
    # When a matrix is near-singular (rank deficient + floating-point noise),
    # partialPivLu can silently return results with extremely large values
    # instead of inf/nan. This is inherent to all partial-pivot LU solvers,
    # including NumPy's np.linalg.solve (which uses LAPACK dgesv).
    #
    # Here [[1, 2], [2, 4+eps]] is rank-2 in floating point but nearly
    # singular. With inconsistent b=[1, 3], no meaningful solution exists,
    # yet LU decomposition produces finite values of order ~1/eps.
    eps = 1e-6  # Must exceed float32 precision (~5e-7 near 4.0).
    a = ds([[1.0, 2.0], [2.0, 4.0 + eps]])  # near-singular
    b = ds([1.0, 3.0])  # inconsistent with the true singular system
    result = kd.matrix.solve(a, b, b_ndim=1)
    flat = result.flatten().to_py()
    # The result is finite (not inf/nan) but has very large magnitude (~1/eps).
    self.assertTrue(
        all(math.isfinite(v) for v in flat),
        f'Expected finite values for near-singular solve, got {flat}',
    )
    self.assertTrue(
        all(abs(v) > 1e5 for v in flat),
        f'Expected large values for near-singular solve, got {flat}',
    )

    # NumPy behavior for the same near-singular system:
    a_np = np.array([[1.0, 2.0], [2.0, 4.0 + eps]])
    b_np = np.array([1.0, 3.0])
    result_np = np.linalg.solve(a_np, b_np)
    self.assertTrue(
        all(np.isfinite(v) for v in result_np),
        f'Expected finite values for near-singular solve, got {result_np}',
    )
    self.assertTrue(
        all(abs(v) > 1e5 for v in result_np),
        f'Expected large values for near-singular solve, got {result_np}',
    )

  def test_solve_singular_numerically_unstable(self):
    # This matrix is mathematically singular (row2 = 1e7 * row1), but
    # float32 rounding means the pivot never lands exactly on zero.
    # The solver sees a tiny non-zero pivot and returns large finite garbage
    # instead of inf/nan. This demonstrates why the API does not promise
    # specific behavior for singular matrices.
    a = ds([[1.0, 1.0 + 1e-7], [1e7, 1e7 + 1.0]])  # det ≈ 0 in exact math
    b = ds([1.0, 3.0])  # inconsistent RHS
    result = kd.matrix.solve(a, b)
    flat = result.flatten().to_py()
    # The result is finite (not inf/nan) despite the matrix being
    # mathematically singular — confirming that inf/nan detection is
    # unreliable for singular matrices.
    self.assertTrue(
        all(math.isfinite(v) for v in flat),
        f'Expected finite (garbage) values, got {flat}',
    )

    # NumPy behavior for the same system:
    # 1. Using float32:
    a_np = np.array([[1.0, 1.0 + 1e-7], [1e7, 1e7 + 1.0]], dtype=np.float32)
    b_np = np.array([1.0, 3.0], dtype=np.float32)
    result_np = np.linalg.solve(a_np, b_np)
    self.assertTrue(
        all(math.isfinite(v) for v in result_np),
        f'Expected finite (garbage) values, got {result_np}',
    )
    # 2. Using float64 (the default dtype):
    a_np = np.array([[1.0, 1.0 + 1e-7], [1e7, 1e7 + 1.0]])
    b_np = np.array([1.0, 3.0])
    with self.assertRaisesRegex(np.linalg.LinAlgError, 'Singular matrix'):
      np.linalg.solve(a_np, b_np)

  def test_solve_batched_with_one_singular(self):
    # Current behavior: in a batch, exactly-singular matrices produce
    # inf/nan for their own element without affecting other elements.
    # This is not guaranteed by the API — see the near-singular tests above.
    a = ds([
        [[1.0, 0.0], [0.0, 1.0]],  # identity
        [[1.0, 2.0], [2.0, 4.0]],  # singular
    ])
    b = ds([[3.0, 4.0], [1.0, 3.0]])  # second RHS is inconsistent
    result = kd.matrix.solve(a, b, b_ndim=1)
    # First system: Ix = [3,4] => x = [3,4].
    testing.assert_allclose(
        kd.subslice(result, 0, ...),
        ds([3.0, 4.0], schema_constants.FLOAT32),
    )
    # Second system: singular+inconsistent, currently produces inf or nan.
    flat = kd.subslice(result, 1, ...).flatten().to_py()
    self.assertTrue(
        all(math.isinf(v) or math.isnan(v) for v in flat),
        f'Expected inf or nan for singular batch element, got {flat}',
    )


if __name__ == '__main__':
  absltest.main()
