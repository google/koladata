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
    # (a,) -> result:
    (DATA_SLICE, DATA_SLICE),
])


class MatrixDetTest(parameterized.TestCase):

  def test_empty_zero_by_zero(self):
    # 0×0 matrix: determinant is 1. Note that an empty matrix with schema NONE
    # produces a result with schema INT32, because INT32 is the narrowest
    # numeric type that can represent 1.
    a = kd.empty_shaped(kd.shapes.new(0, 0), schema_constants.NONE)
    result = kd.matrix.det(a)
    testing.assert_allclose(result, ds(1, schema_constants.INT32))

  def test_empty_zero_by_zero_float64(self):
    a = kd.empty_shaped(kd.shapes.new(0, 0), schema_constants.FLOAT64)
    result = kd.matrix.det(a)
    testing.assert_allclose(result, ds(1.0, schema_constants.FLOAT64))

  def test_basic(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    result = kd.matrix.det(a)
    testing.assert_allclose(result, ds(-2.0, schema_constants.FLOAT32))

  def test_basic_float64(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]], schema_constants.FLOAT64)
    result = kd.matrix.det(a)
    testing.assert_allclose(result, ds(-2.0, schema_constants.FLOAT64))

  def test_3x3(self):
    a_np = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 0.0]])
    expected = np.linalg.det(a_np)
    result = kd.matrix.det(ds(a_np.tolist()))
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_identity(self):
    a = ds([[1.0, 0.0], [0.0, 1.0]])
    result = kd.matrix.det(a)
    testing.assert_allclose(result, ds(1.0, schema_constants.FLOAT32))

  def test_int32_input_produces_int32_output(self):
    # INT32 inputs -> det computes in FLOAT64, output cast to INT32.
    a = ds([[1, 0], [0, 1]])  # INT32
    result = kd.matrix.det(a)
    testing.assert_allclose(result, ds(1, schema_constants.INT32))

  def test_int64_input(self):
    # INT64 inputs -> det computes in FLOAT64, output cast to INT64.
    a = ds([[1, 0], [0, 1]], schema_constants.INT64)
    result = kd.matrix.det(a)
    testing.assert_allclose(result, ds(1, schema_constants.INT64))

  def test_sparse_data(self):
    # None values are treated as 0 in matrix operations.
    a = ds([[1.0, None], [None, 2.0]])
    result = kd.matrix.det(a)
    testing.assert_allclose(result, ds(2.0, schema_constants.FLOAT32))

  def test_object_schema_float(self):
    # OBJECT schema inputs should produce OBJECT schema output.
    a = kd.obj(ds([[1.0, 0.0], [0.0, 1.0]]))
    result = kd.matrix.det(a)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        schema_constants.FLOAT32,
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT32),
        ds(1.0),
    )

  def test_object_schema_float64(self):
    a = kd.obj(ds([[1.0, 0.0], [0.0, 1.0]], schema_constants.FLOAT64))
    result = kd.matrix.det(a)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        schema_constants.FLOAT64,
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT64),
        ds(1.0, schema_constants.FLOAT64),
    )

  def test_object_schema_integer(self):
    a = kd.obj(ds([[1, 0], [0, 1]]))
    result = kd.matrix.det(a)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        schema_constants.INT32,
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.INT32),
        ds(1),
    )

  def test_object_schema_mixed_numeric_types(self):
    # Mixed numeric types wrapped in OBJECT: INT32 and INT64 -> INT64.
    a = ds([[kd.int64(1), kd.obj(kd.int32(0))], [0, 1]])
    result = kd.matrix.det(a)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        schema_constants.INT64,
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.INT64),
        ds(1, schema_constants.INT64),
    )

  def test_batched_3d(self):
    # (2, 2, 2) -> (2,). Two determinants.
    a_np = np.array([
        [[1.0, 2.0], [3.0, 4.0]],
        [[2.0, 1.0], [1.0, 3.0]],
    ])
    expected = np.linalg.det(a_np).tolist()
    result = kd.matrix.det(ds(a_np.tolist()))
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_batched_4d(self):
    # (2, 2, 2, 2) -> (2, 2). Two levels of batch.
    a_np = np.array([
        [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 1.0], [2.0, 3.0]]],
        [[[2.0, 1.0], [1.0, 3.0]], [[4.0, 3.0], [1.0, 2.0]]],
    ])
    expected = np.linalg.det(a_np).tolist()
    result = kd.matrix.det(ds(a_np.tolist()))
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_batched_vs_numpy(self):
    # 2-batch of 3x3 matrices.
    a_np = np.array([
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 0.0]],
        [[2.0, 0.0, 1.0], [1.0, 3.0, 0.0], [0.0, 1.0, 2.0]],
    ])
    expected = np.linalg.det(a_np).tolist()
    result = kd.matrix.det(ds(a_np.tolist()))
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_jagged_matrix_dims(self):
    a1 = [[1.0, 2.0], [3.0, 4.0]]
    a2 = [[5.0]]
    a = ds([a1, a2])
    result = kd.matrix.det(a)
    expected_0 = 1.0 * 4.0 - 2.0 * 3.0
    expected_1 = 5.0
    testing.assert_allclose(
        result,
        ds([expected_0, expected_1], schema_constants.FLOAT32),
        atol=1e-5,
    )

  def test_jagged_batch_dimensions(self):
    # 4D input where batch dim 1 is jagged. Each matrix is 2x2.
    # Shape: dim0=2, dim1=[2, 1], dim2=2, dim3=2. Total 3 matrices.
    a = ds([
        [[[1.0, 2.0], [3.0, 5.0]], [[2.0, 1.0], [1.0, 3.0]]],
        [[[1.0, 0.0], [0.0, 1.0]]],
    ])
    result = kd.matrix.det(a)
    testing.assert_allclose(
        result,
        ds(
            [
                [-1.0, 5.0],
                [1.0],
            ],
            schema_constants.FLOAT32,
        ),
        atol=1e-5,
    )

  def test_det_singular_matrix_returns_zero(self):
    # Singular matrix: determinant should be 0.
    a = ds([[1.0, 2.0], [2.0, 4.0]])
    result = kd.matrix.det(a)
    testing.assert_allclose(
        result, ds(0.0, schema_constants.FLOAT32), atol=1e-6
    )

  def test_det_batched_with_one_singular(self):
    # Batch of 2: identity has det=1, singular has det=0.
    a = ds([
        [[1.0, 0.0], [0.0, 1.0]],
        [[1.0, 2.0], [2.0, 4.0]],
    ])
    result = kd.matrix.det(a)
    testing.assert_allclose(
        result, ds([1.0, 0.0], schema_constants.FLOAT32), atol=1e-6
    )

  def test_det_near_singular_returns_small_finite_value(self):
    # When a matrix is near-singular (rank deficient + floating-point noise),
    # the determinant should be a small but finite value close to zero, rather
    # than exactly zero. This mirrors the behavior of NumPy's np.linalg.det.
    eps = 1e-6  # Must exceed float32 precision (~5e-7 near 4.0).
    a = ds([[1.0, 2.0], [2.0, 4.0 + eps]])  # near-singular
    result = kd.matrix.det(a)
    result_val = result.to_py()
    self.assertTrue(
        math.isfinite(result_val),
        'Expected finite determinant for near-singular matrix, got'
        f' {result_val}',
    )
    self.assertAlmostEqual(
        result_val,
        eps,  # det = 1*(4+eps) - 2*2 = eps
        places=5,
        msg=f'Expected det ≈ {eps} for near-singular matrix, got {result_val}',
    )

    # NumPy behavior for the same near-singular matrix:
    a_np = np.array([[1.0, 2.0], [2.0, 4.0 + eps]])
    expected_np = np.linalg.det(a_np)
    self.assertAlmostEqual(
        result_val,
        float(expected_np),
        places=5,
        msg=(
            'Near-singular det mismatch with NumPy:'
            f' kd={result_val}, np={expected_np}'
        ),
    )

  def test_int32_det_overflow_clamps_to_max(self):
    # det([[50000, 0], [0, 50000]]) = 2.5e9 > INT32_MAX (2147483647).
    # saturate_cast<int32_t>(2.5e9) clamps to INT32_MAX.
    a = ds([[50000, 0], [0, 50000]], schema_constants.INT32)
    result = kd.matrix.det(a)
    testing.assert_equal(result, ds(2147483647, schema_constants.INT32))

  def test_int32_det_overflow_clamps_to_min(self):
    # det([[-50000, 0], [0, 50000]]) = -2.5e9 < INT32_MIN (-2147483648).
    a = ds([[-50000, 0], [0, 50000]], schema_constants.INT32)
    result = kd.matrix.det(a)
    testing.assert_equal(result, ds(-2147483648, schema_constants.INT32))

  def test_int32_det_overflow_batched(self):
    # Batch of 3: first overflows to INT32_MAX, second overflows to INT32_MIN,
    # third is normal.
    a = ds(
        [
            [[50000, 0], [0, 50000]],
            [[-50000, 0], [0, 50000]],
            [[2, 1], [1, 3]],
        ],
        schema_constants.INT32,
    )
    result = kd.matrix.det(a)
    testing.assert_equal(
        result,
        ds([2147483647, -2147483648, 5], schema_constants.INT32),
    )

  def test_float32_det_overflow_to_inf(self):
    # det([[1e20, 0], [0, 1e20]]) = 1e40 > FLOAT32_MAX (~3.4e38) -> +inf.
    a = ds([[1e20, 0.0], [0.0, 1e20]], schema_constants.FLOAT32)
    result = kd.matrix.det(a)
    testing.assert_equal(result, ds(float('inf'), schema_constants.FLOAT32))

  def test_float32_det_overflow_to_neg_inf(self):
    # det([[-1e20, 0], [0, 1e20]]) = -1e40 -> -inf.
    a = ds([[-1e20, 0.0], [0.0, 1e20]], schema_constants.FLOAT32)
    result = kd.matrix.det(a)
    testing.assert_equal(result, ds(float('-inf'), schema_constants.FLOAT32))

  def test_float32_det_overflow_batched(self):
    # Batch of 3: first overflows to +inf, second overflows to -inf, third is
    # normal.
    a = ds(
        [
            [[1e20, 0.0], [0.0, 1e20]],
            [[-1e20, 0.0], [0.0, 1e20]],
            [[2.0, 1.0], [1.0, 3.0]],
        ],
        schema_constants.FLOAT32,
    )
    result = kd.matrix.det(a)
    testing.assert_equal(
        result,
        ds([float('inf'), float('-inf'), 5.0], schema_constants.FLOAT32),
    )

  def test_float32_det_intermediate_precision(self):
    # det([[100003, 100001], [100001, 100003]]) = 100003^2 - 100001^2 = 400008.
    # Both products (~1e10) exceed float32 precision (ULP ~1024 at that scale),
    # so float32 arithmetic rounds each product, yielding a wrong determinant
    # (400012). Float64 computes both products exactly (product of two 17-bit
    # integers fits in 34 bits < 53-bit double mantissa), giving 400008.
    a = ds(
        [[100003.0, 100001.0], [100001.0, 100003.0]],
        schema_constants.FLOAT32,
    )
    result = kd.matrix.det(a)
    testing.assert_equal(result, ds(400008.0, schema_constants.FLOAT32))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.det,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.det(I.a)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify the conceptual equivalence."""

  def test_det_vs_numpy(self):
    a_np = np.array([[2.0, 1.0], [5.0, 3.0]])
    expected = np.linalg.det(a_np)
    result = kd.matrix.det(ds(a_np.tolist()))
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_det_3x3_vs_numpy(self):
    a_np = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 0.0]])
    expected = np.linalg.det(a_np)
    result = kd.matrix.det(ds(a_np.tolist()))
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_det_batched_vs_numpy(self):
    rng = np.random.default_rng(101)
    a_np = rng.standard_normal((5, 3, 3))
    result = kd.matrix.det(ds(a_np.tolist()))
    expected = [np.linalg.det(a_np[i]) for i in range(5)]
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_det_large_vs_numpy(self):
    rng = np.random.default_rng(456)
    n = 10
    a_np = rng.standard_normal((n, n)) + np.eye(n) * n
    expected = np.linalg.det(a_np)
    a = ds(a_np.tolist())
    result = kd.matrix.det(a)
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_det_identity_vs_numpy(self):
    a_np = np.eye(4)
    expected = np.linalg.det(a_np)
    result = kd.matrix.det(ds(a_np.tolist()))
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-10
    )


class ErrorTest(parameterized.TestCase):
  """Exhaustive tests for error messages."""

  def test_det_0d_fails(self):
    a = ds(1.0)
    with self.assertRaisesRegex(
        ValueError, r'det.*expected at least 2D.*got 0D'
    ):
      kd.matrix.det(a)

  def test_det_1d_fails(self):
    a = ds([1.0, 2.0])
    with self.assertRaisesRegex(
        ValueError, r'det.*expected at least 2D.*got 1D'
    ):
      kd.matrix.det(a)

  def test_det_non_square_fails(self):
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])  # 2x3
    with self.assertRaisesRegex(ValueError, r'det.*not square.*2.*3'):
      kd.matrix.det(a)

  def test_det_batched_non_square_fails(self):
    a = ds([[[1.0, 0.0], [0.0, 1.0]], [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]])
    with self.assertRaisesRegex(ValueError, r'det.*not square'):
      kd.matrix.det(a)

  def test_det_non_uniform_rows_fails(self):
    # Jagged matrix: rows have different lengths.
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0]])
    with self.assertRaisesRegex(ValueError, r'non-uniform row sizes'):
      kd.matrix.det(a)

  def test_det_string_schema_fails(self):
    a = ds([['a', 'b'], ['c', 'd']])
    with self.assertRaisesRegex(ValueError, r'unsupported.*schema: STRING'):
      kd.matrix.det(a)

  def test_det_strings_with_object_schema_fails(self):
    a = kd.obj(ds([['a', 'b'], ['c', 'd']]))
    with self.assertRaisesRegex(ValueError, r'unsupported.*schema: STRING'):
      kd.matrix.det(a)


if __name__ == '__main__':
  absltest.main()
