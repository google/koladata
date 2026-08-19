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

"""Tests for the kd.matrix.inverse operator."""

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
    # (a,) -> result:
    (DATA_SLICE, DATA_SLICE),
])


class MatrixInverseTest(parameterized.TestCase):

  def test_empty_zero_by_zero(self):
    # 0×0 matrix: should produce an empty result.
    a = kd.empty_shaped(kd.shapes.new(0, 0), schema_constants.FLOAT32)
    result = kd.matrix.inverse(a)
    testing.assert_allclose(
        result,
        kd.empty_shaped(kd.shapes.new(0, 0), schema_constants.FLOAT32),
    )

  def test_empty_zero_by_zero_float64(self):
    a = kd.empty_shaped(kd.shapes.new(0, 0), schema_constants.FLOAT64)
    result = kd.matrix.inverse(a)
    testing.assert_allclose(
        result,
        kd.empty_shaped(kd.shapes.new(0, 0), schema_constants.FLOAT64),
    )

  def test_basic(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    result = kd.matrix.inverse(a)
    expected = ds([[-2.0, 1.0], [1.5, -0.5]])
    testing.assert_allclose(result, expected, atol=1e-5)
    testing.assert_allclose(
        kd.matrix.matmul(a, result),
        ds([[1.0, 0.0], [0.0, 1.0]]),
        atol=1e-5,
    )

  def test_basic_float64(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]], schema_constants.FLOAT64)
    result = kd.matrix.inverse(a)
    expected = ds([[-2.0, 1.0], [1.5, -0.5]], schema_constants.FLOAT64)
    testing.assert_allclose(result, expected)
    testing.assert_allclose(
        kd.matrix.matmul(a, result),
        ds([[1.0, 0.0], [0.0, 1.0]], schema_constants.FLOAT64),
        atol=1e-5,
    )

  def test_identity(self):
    a = ds([[1.0, 0.0], [0.0, 1.0]])
    result = kd.matrix.inverse(a)
    testing.assert_allclose(result, ds([[1.0, 0.0], [0.0, 1.0]]))

  def test_int32_input_produces_float32_output(self):
    # INT32 inputs → inverse computes in FLOAT64, output cast to FLOAT32.
    a = ds([[1, 0], [0, 1]])  # INT32
    result = kd.matrix.inverse(a)
    testing.assert_allclose(result, ds([[1.0, 0.0], [0.0, 1.0]]))

  def test_int64_input(self):
    # INT64 inputs → output is FLOAT32. This is similar to kd.math.divide and
    # kd.matrix.solve behavior.
    a = ds([[1, 0], [0, 1]], schema_constants.INT64)
    result = kd.matrix.inverse(a)
    testing.assert_allclose(
        result, ds([[1.0, 0.0], [0.0, 1.0]], schema_constants.FLOAT32)
    )

  def test_sparse_data(self):
    # None values are treated as 0 in matrix operations.
    a = ds([[1.0, None], [None, 2.0]])
    result = kd.matrix.inverse(a)
    testing.assert_allclose(
        result,
        ds([[1.0, 0.0], [0.0, 0.5]]),
    )
    testing.assert_allclose(
        kd.matrix.matmul(a, result),
        ds([[1.0, 0.0], [0.0, 1.0]]),
        atol=1e-5,
    )

  def test_object_schema_float(self):
    # OBJECT schema inputs should produce OBJECT schema output.
    a = kd.obj(ds([[1.0, 0.0], [0.0, 1.0]]))
    result = kd.matrix.inverse(a)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        ds([
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
        ]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT32),
        ds([[1.0, 0.0], [0.0, 1.0]]),
    )
    testing.assert_allclose(
        kd.matrix.matmul(a, result).no_bag(),
        a.no_bag(),
        atol=1e-5,
    )

  def test_object_schema_float64(self):
    a = kd.obj(ds([[1.0, 0.0], [0.0, 1.0]], schema_constants.FLOAT64))
    result = kd.matrix.inverse(a)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        ds([
            [schema_constants.FLOAT64, schema_constants.FLOAT64],
            [schema_constants.FLOAT64, schema_constants.FLOAT64],
        ]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT64),
        ds([[1.0, 0.0], [0.0, 1.0]], schema_constants.FLOAT64),
    )

  def test_object_schema_integer(self):
    a = kd.obj(ds([[1, 0], [0, 1]]))
    result = kd.matrix.inverse(a)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        ds([
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
        ]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT32),
        ds([[1.0, 0.0], [0.0, 1.0]]),
    )

  def test_object_schema_mixed_numeric_types(self):
    # Mixed numeric types wrapped in OBJECT: INT32 and INT64 -> FLOAT32.
    a = ds([[kd.int64(1), kd.obj(kd.int32(0))], [0, 1]])
    result = kd.matrix.inverse(a)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        ds([
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
        ]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT32),
        ds([[1.0, 0.0], [0.0, 1.0]]),
    )

  def test_batched_3d(self):
    # (2, 2, 2) -> (2, 2, 2). Batch of 2 inverses.
    a_np = np.array([
        [[1.0, 2.0], [3.0, 4.0]],
        [[2.0, 1.0], [1.0, 3.0]],
    ])
    expected = np.linalg.inv(a_np)
    a = ds(a_np.tolist())
    result = kd.matrix.inverse(a)
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-5
    )
    testing.assert_allclose(
        kd.matrix.matmul(a, result),
        ds([[[1.0, 0.0], [0.0, 1.0]], [[1.0, 0.0], [0.0, 1.0]]]),
        atol=1e-5,
    )

  def test_batched_4d(self):
    # (2, 2, 2, 2) -> (2, 2, 2, 2). Two levels of batch.
    a_np = np.array([
        [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 1.0], [2.0, 3.0]]],
        [[[2.0, 1.0], [1.0, 3.0]], [[4.0, 3.0], [1.0, 2.0]]],
    ])
    expected = np.linalg.inv(a_np)
    result = kd.matrix.inverse(ds(a_np.tolist()))
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-5
    )

  def test_batched_vs_numpy(self):
    # 2-batch of 3x3 matrices, cross-validated with NumPy.
    a_np = np.array([
        [[1.0, 2.0, 0.0], [0.0, 3.0, 1.0], [1.0, 0.0, 2.0]],
        [[2.0, 0.0, 1.0], [1.0, 1.0, 0.0], [0.0, 2.0, 1.0]],
    ])
    expected = np.linalg.inv(a_np)
    result = kd.matrix.inverse(ds(a_np.tolist()))
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-5
    )

  def test_jagged_matrix_dims(self):
    a1 = [[2.0, 1.0], [1.0, 3.0]]
    a2 = [[1.0]]
    a = ds([a1, a2])
    result = kd.matrix.inverse(a)
    a1_np = np.array(a1)
    a1_inv = np.linalg.inv(a1_np)
    testing.assert_allclose(
        result,
        ds([a1_inv.tolist(), [[1.0]]], schema_constants.FLOAT32),
        atol=1e-5,
    )

  def test_jagged_batch_dimensions(self):
    # 4D input where batch dim 1 is jagged. Each matrix is 2x2.
    # Shape: dim0=2, dim1=[2, 1], dim2=2, dim3=2. Total 3 matrices.
    a = ds([
        [[[1.0, 2.0], [3.0, 5.0]], [[2.0, 1.0], [1.0, 3.0]]],
        [[[1.0, 0.0], [0.0, 1.0]]],
    ])
    result = kd.matrix.inverse(a)
    testing.assert_allclose(
        kd.matrix.matmul(a, result),
        ds([
            [[[1.0, 0.0], [0.0, 1.0]], [[1.0, 0.0], [0.0, 1.0]]],
            [[[1.0, 0.0], [0.0, 1.0]]],
        ]),
        atol=1e-5,
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.inverse,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.inverse(I.a)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify the conceptual equivalence."""

  def test_inverse_vs_numpy(self):
    a_np = np.array([[2.0, 1.0], [5.0, 3.0]])
    expected = np.linalg.inv(a_np)
    a = ds(a_np.tolist())
    result = kd.matrix.inverse(a)
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-5
    )
    testing.assert_allclose(
        kd.matrix.matmul(a, result),
        ds(np.eye(2).tolist(), schema_constants.FLOAT32),
        atol=1e-5,
    )

  def test_inverse_batched_vs_numpy(self):
    rng = np.random.default_rng(789)
    batch_size = 3
    n = 4
    a_np = rng.standard_normal((batch_size, n, n)) + np.eye(n) * n
    result = kd.matrix.inverse(ds(a_np.tolist()))
    expected = [np.linalg.inv(a_np[i]).tolist() for i in range(batch_size)]
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_inverse_large_vs_numpy(self):
    rng = np.random.default_rng(456)
    n = 10
    a_np = rng.standard_normal((n, n)) + np.eye(n) * n
    expected = np.linalg.inv(a_np)
    a = ds(a_np.tolist())
    result = kd.matrix.inverse(a)
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-5
    )
    testing.assert_allclose(
        kd.matrix.matmul(a, result),
        ds(np.eye(n).tolist(), schema_constants.FLOAT32),
        atol=1e-4,
    )

  def test_inverse_identity_vs_numpy(self):
    a_np = np.eye(5)
    expected = np.linalg.inv(a_np)
    result = kd.matrix.inverse(ds(a_np.tolist()))
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-5
    )


class ErrorTest(parameterized.TestCase):
  """Exhaustive tests for error messages."""

  def test_inverse_0d_fails(self):
    a = ds(1.0)
    with self.assertRaisesRegex(
        ValueError, r'inverse.*expected at least 2D.*got 0D'
    ):
      kd.matrix.inverse(a)

  def test_inverse_1d_fails(self):
    a = ds([1.0, 2.0])
    with self.assertRaisesRegex(
        ValueError, r'inverse.*expected at least 2D.*got 1D'
    ):
      kd.matrix.inverse(a)

  def test_inverse_non_square_fails(self):
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])  # 2x3
    with self.assertRaisesRegex(ValueError, r'inverse.*not square.*2.*3'):
      kd.matrix.inverse(a)

  def test_inverse_batched_non_square_fails(self):
    # First matrix is square, second is not.
    a = ds([[[1.0, 0.0], [0.0, 1.0]], [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]])
    with self.assertRaisesRegex(ValueError, r'inverse.*not square'):
      kd.matrix.inverse(a)

  def test_inverse_non_uniform_rows_fails(self):
    # Jagged matrix: rows have different lengths.
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0]])
    with self.assertRaisesRegex(ValueError, r'non-uniform row sizes'):
      kd.matrix.inverse(a)

  def test_inverse_string_schema_fails(self):
    a = ds([['a', 'b'], ['c', 'd']])
    with self.assertRaisesRegex(
        ValueError, r'unsupported narrowed schema: STRING'
    ):
      kd.matrix.inverse(a)

  def test_inverse_strings_with_object_schema_fails(self):
    a = kd.obj(ds([['a', 'b'], ['c', 'd']]))
    with self.assertRaisesRegex(
        ValueError, r'unsupported narrowed schema: STRING'
    ):
      kd.matrix.inverse(a)

  # -- inverse singular matrix (implementation behavior, not API guarantees) --
  #
  # The API only guarantees correct results for invertible matrices. The tests
  # below document the *current* behavior of kd.matrix.inverse on singular and
  # near-singular inputs. This behavior is not promised by the API and may
  # change in future implementations.

  def test_inverse_singular_matrix_returns_inf_or_nan(self):
    # Current behavior for exactly-singular A: LU back-substitution hits a zero
    # pivot and produces inf/nan. This only happens when the pivot is *exactly*
    # zero in floating point - see
    # test_inverse_near_singular_returns_large_finite_values
    # for the more realistic near-singular case where this breaks down.
    a = ds([[1.0, 2.0], [2.0, 4.0]])  # singular: row 2 = 2× row 1
    result = kd.matrix.inverse(a)
    flat = result.flatten().to_py()
    self.assertTrue(
        all(math.isinf(v) or math.isnan(v) for v in flat),
        f'Expected inf or nan for singular matrix inverse, got {flat}',
    )

    # NumPy behavior for the same singular system:
    a_np = np.array([[1.0, 2.0], [2.0, 4.0]])
    with self.assertRaisesRegex(np.linalg.LinAlgError, 'Singular matrix'):
      np.linalg.inv(a_np)

  def test_inverse_near_singular_returns_large_finite_values(self):
    # When a matrix is near-singular (rank deficient + floating-point noise),
    # partialPivLu can silently return results with extremely large values
    # instead of inf/nan. This is inherent to all partial-pivot LU solvers,
    # including NumPy's np.linalg.inv (which uses LAPACK dgetrf/dgetri).
    eps = 1e-6  # Must exceed float32 precision (~5e-7 near 4.0).
    a = ds([[1.0, 2.0], [2.0, 4.0 + eps]])  # near-singular
    result = kd.matrix.inverse(a)
    flat = result.flatten().to_py()
    self.assertTrue(
        all(math.isfinite(v) for v in flat),
        f'Expected finite values for near-singular inverse, got {flat}',
    )
    self.assertTrue(
        all(abs(v) > 1e5 for v in flat),
        f'Expected large values for near-singular inverse, got {flat}',
    )

    # NumPy behavior for the same near-singular system:
    a_np = np.array([[1.0, 2.0], [2.0, 4.0 + eps]])
    result_np = np.linalg.inv(a_np)
    self.assertTrue(
        all(np.isfinite(v) for v in result_np.flatten()),
        f'Expected finite values for near-singular inverse, got {result_np}',
    )
    self.assertTrue(
        all(abs(v) > 1e5 for v in result_np.flatten()),
        f'Expected large values for near-singular inverse, got {result_np}',
    )

  def test_inverse_singular_numerically_unstable(self):
    # This matrix is mathematically singular (row2 = 1e7 * row1), but
    # float32 rounding means the pivot never lands exactly on zero.
    # The solver sees a tiny non-zero pivot and returns large finite garbage
    # instead of inf/nan. This demonstrates why the API does not promise
    # specific behavior for singular matrices.
    a = ds([[1.0, 1.0 + 1e-7], [1e7, 1e7 + 1.0]])  # det ≈ 0 in exact math
    result = kd.matrix.inverse(a)
    flat = result.flatten().to_py()
    self.assertTrue(
        all(math.isfinite(v) for v in flat),
        f'Expected finite (garbage) values, got {flat}',
    )

    # NumPy behavior for the same system:
    # 1. Using float32:
    a_np = np.array([[1.0, 1.0 + 1e-7], [1e7, 1e7 + 1.0]], dtype=np.float32)
    result_np = np.linalg.inv(a_np)
    self.assertTrue(
        all(math.isfinite(v) for v in result_np.flatten()),
        f'Expected finite (garbage) values, got {result_np}',
    )
    # 2. Using float64 (the default dtype):
    a_np = np.array([[1.0, 1.0 + 1e-7], [1e7, 1e7 + 1.0]])
    with self.assertRaisesRegex(np.linalg.LinAlgError, 'Singular matrix'):
      np.linalg.inv(a_np)

  def test_inverse_batched_with_one_singular(self):
    # Current behavior: in a batch, exactly-singular matrices produce
    # inf/nan for their own element without affecting other elements.
    # This is not guaranteed by the API - see the near-singular tests above.
    a = ds([
        [[1.0, 0.0], [0.0, 1.0]],  # identity (invertible)
        [[1.0, 2.0], [2.0, 4.0]],  # singular
    ])
    result = kd.matrix.inverse(a)
    # First matrix: inverse of identity is identity.
    testing.assert_allclose(
        kd.subslice(result, 0, ...),
        ds([[1.0, 0.0], [0.0, 1.0]]),
    )
    # Second matrix: should contain inf or nan.
    flat = kd.subslice(result, 1, ...).flatten().to_py()
    self.assertTrue(
        all(math.isinf(v) or math.isnan(v) for v in flat),
        f'Expected inf or nan for singular batch element, got {flat}',
    )


if __name__ == '__main__':
  absltest.main()
