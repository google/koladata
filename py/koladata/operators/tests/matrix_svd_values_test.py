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
    # (x,) -> result:
    (DATA_SLICE, DATA_SLICE),
])


class MatrixSvdValuesTest(parameterized.TestCase):

  def test_identity_2x2(self):
    # Singular values of identity = [1, 1].
    x = ds([[1.0, 0.0], [0.0, 1.0]])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([1.0, 1.0]), atol=1e-6)

  def test_identity_3x3(self):
    x = ds([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([1.0, 1.0, 1.0]), atol=1e-6)

  def test_diagonal_matrix(self):
    # Singular values of diag(3, 1, 2) = [3, 2, 1] (descending).
    x = ds([[3.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 2.0]])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([3.0, 2.0, 1.0]), atol=1e-6)

  def test_diagonal_with_negatives(self):
    # Singular values are absolute values: diag(-3, 1, -2) -> [3, 2, 1].
    x = ds([[-3.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, -2.0]])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([3.0, 2.0, 1.0]), atol=1e-6)

  def test_rank_deficient(self):
    # [[1, 2], [2, 4]] has rank 1, so one singular value should be ~0.
    x = ds([[1.0, 2.0], [2.0, 4.0]])
    result = kd.matrix.svd_values(x)
    self.assertEqual(result.get_size(), 2)
    # First SV = sqrt(1+4+4+16) = 5.
    testing.assert_allclose(result.S[0], ds(5.0), atol=1e-6)
    testing.assert_allclose(result.S[1], ds(0.0), atol=1e-6)

  def test_non_square_tall(self):
    # (3, 2) matrix -> 2 singular values.
    x = ds([[1.0, 0.0], [0.0, 1.0], [0.0, 0.0]])
    result = kd.matrix.svd_values(x)
    self.assertEqual(result.get_size(), 2)
    testing.assert_allclose(result, ds([1.0, 1.0]), atol=1e-6)

  def test_non_square_wide(self):
    # (2, 3) matrix -> 2 singular values.
    x = ds([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])
    result = kd.matrix.svd_values(x)
    self.assertEqual(result.get_size(), 2)
    testing.assert_allclose(result, ds([1.0, 1.0]), atol=1e-6)

  def test_1x1_matrix(self):
    x = ds([[5.0]])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([5.0]), atol=1e-6)

  def test_1x1_negative(self):
    x = ds([[-5.0]])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([5.0]), atol=1e-6)

  def test_zero_matrix(self):
    x = ds([[0.0, 0.0], [0.0, 0.0]])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([0.0, 0.0]), atol=1e-6)

  def test_empty_zero_by_zero(self):
    # 0×0 matrix: no singular values -> empty output.
    x = kd.empty_shaped(kd.shapes.new(0, 0), schema_constants.NONE)
    result = kd.matrix.svd_values(x)
    self.assertEqual(result.get_size(), 0)
    self.assertEqual(result.get_schema(), schema_constants.FLOAT32)

  def test_empty_zero_by_zero_float64(self):
    x = kd.empty_shaped(kd.shapes.new(0, 0), schema_constants.FLOAT64)
    result = kd.matrix.svd_values(x)
    self.assertEqual(result.get_size(), 0)
    self.assertEqual(result.get_schema(), schema_constants.FLOAT64)

  def test_sparse_missing_as_zero(self):
    # Missing treated as 0. Matrix is [[1, 0], [0, 4]].
    x = ds([[1.0, None], [None, 4.0]])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([4.0, 1.0]), atol=1e-6)

  def test_batched_sparse(self):
    # Batch of 2 matrices with missing values.
    x = ds([
        [[1.0, None], [None, 2.0]],  # -> diag(1, 2) -> sv = [2, 1]
        [[None, 3.0], [None, None]],  # -> [[0, 3], [0, 0]] -> sv = [3, 0]
    ])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([[2.0, 1.0], [3.0, 0.0]]), atol=1e-5)

  def test_int32_input(self):
    x = ds([[3, 0], [0, 2]])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(
        result, ds([3.0, 2.0], schema_constants.FLOAT32), atol=1e-5
    )

  def test_int64_input(self):
    x = ds([[3, 0], [0, 2]], schema_constants.INT64)
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(
        result, ds([3.0, 2.0], schema_constants.FLOAT32), atol=1e-5
    )

  def test_float64_input(self):
    x = ds([[3.0, 0.0], [0.0, 2.0]], schema_constants.FLOAT64)
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(
        result, ds([3.0, 2.0], schema_constants.FLOAT64), atol=1e-10
    )

  def test_none_schema(self):
    # All-None matrix -> all singular values are 0.
    x = ds([[None, None], [None, None]], schema_constants.NONE)
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([0.0, 0.0]), atol=1e-6)

  def test_object_schema_float(self):
    x = kd.obj(ds([[3.0, 0.0], [0.0, 2.0]]))
    result = kd.matrix.svd_values(x)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    # Output is a 1D vector, so obj_schema is per-element.
    testing.assert_equivalent(
        result.get_obj_schema(),
        ds([schema_constants.FLOAT32, schema_constants.FLOAT32]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT32),
        ds([3.0, 2.0]),
        atol=1e-5,
    )

  def test_object_schema_float64(self):
    x = kd.obj(ds([[3.0, 0.0], [0.0, 2.0]], schema_constants.FLOAT64))
    result = kd.matrix.svd_values(x)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        ds([schema_constants.FLOAT64, schema_constants.FLOAT64]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT64),
        ds([3.0, 2.0], schema_constants.FLOAT64),
        atol=1e-10,
    )

  def test_object_schema_integer(self):
    x = kd.obj(ds([[3, 0], [0, 2]]))
    result = kd.matrix.svd_values(x)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    # INT32 input -> FLOAT32 output (SVD always produces floats).
    testing.assert_equivalent(
        result.get_obj_schema(),
        ds([schema_constants.FLOAT32, schema_constants.FLOAT32]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT32),
        ds([3.0, 2.0]),
        atol=1e-5,
    )

  def test_batched_3d(self):
    # (2, 2, 2) -> (2, 2) output.
    x = ds([
        [[3.0, 0.0], [0.0, 1.0]],
        [[0.0, 5.0], [0.0, 0.0]],
    ])
    result = kd.matrix.svd_values(x)
    # First matrix: sv = [3, 1]. Second: sv = [5, 0].
    testing.assert_allclose(result, ds([[3.0, 1.0], [5.0, 0.0]]), atol=1e-5)

  def test_batched_4d(self):
    # (2, 2, 2, 2) -> (2, 2, 2). Two levels of batch.
    x = ds([
        [[[3.0, 0.0], [0.0, 1.0]], [[0.0, 5.0], [0.0, 0.0]]],
        [[[2.0, 0.0], [0.0, 4.0]], [[1.0, 0.0], [0.0, 1.0]]],
    ])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(
        result,
        ds([[[3.0, 1.0], [5.0, 0.0]], [[4.0, 2.0], [1.0, 1.0]]]),
        atol=1e-5,
    )

  def test_batched_non_square(self):
    # Batch of (2, 3) matrices -> 2 singular values each.
    x = ds([
        [[1.0, 0.0, 0.0], [0.0, 2.0, 0.0]],
        [[3.0, 0.0, 0.0], [0.0, 4.0, 0.0]],
    ])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([[2.0, 1.0], [4.0, 3.0]]), atol=1e-5)

  def test_batched_with_one_rank_deficient(self):
    # Batch: first matrix is full rank, second is rank deficient.
    x = ds([
        [[3.0, 0.0], [0.0, 2.0]],  # sv = [3, 2]
        [[1.0, 2.0], [2.0, 4.0]],  # rank 1, sv = [5, 0]
    ])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([[3.0, 2.0], [5.0, 0.0]]), atol=1e-5)

  def test_jagged_matrix_dims(self):
    # Batch of matrices with different sizes: 2x2 and 1x1.
    # SVD of each produces different-length output vectors (2 and 1).
    a1 = [[3.0, 0.0], [0.0, 2.0]]
    a2 = [[5.0]]
    x = ds([a1, a2])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([[3.0, 2.0], [5.0]]), atol=1e-5)

  def test_jagged_matrix_dims_non_square(self):
    # Batch with different non-square sizes: (2, 3) and (3, 2).
    # Each produces min(m,n)=2 singular values.
    a1 = [[1.0, 0.0, 0.0], [0.0, 2.0, 0.0]]  # (2, 3) -> 2 sv
    a2 = [[3.0, 0.0], [0.0, 4.0], [0.0, 0.0]]  # (3, 2) -> 2 sv
    x = ds([a1, a2])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(result, ds([[2.0, 1.0], [4.0, 3.0]]), atol=1e-5)

  def test_jagged_batch_dimensions(self):
    # 4D input where batch dim 1 is jagged. Each matrix is 2x2.
    # Shape: dim0=2, dim1=[2, 1], dim2=2, dim3=2. Total 3 matrices.
    x = ds([
        [[[3.0, 0.0], [0.0, 1.0]], [[2.0, 0.0], [0.0, 4.0]]],
        [[[5.0, 0.0], [0.0, 5.0]]],
    ])
    result = kd.matrix.svd_values(x)
    testing.assert_allclose(
        result,
        ds([
            [[3.0, 1.0], [4.0, 2.0]],
            [[5.0, 5.0]],
        ]),
        atol=1e-5,
    )

  def test_descending_order(self):
    # Verify that singular values are always in descending order.
    rng = np.random.default_rng(42)
    x_np = rng.standard_normal((5, 5))
    expected = np.linalg.svdvals(x_np)
    # NumPy guarantees descending order, so matching NumPy confirms order.
    result = kd.matrix.svd_values(ds(x_np.tolist()))
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-4
    )

  def test_non_negative(self):
    # Singular values are always non-negative — verified via numpy match.
    rng = np.random.default_rng(99)
    x_np = rng.standard_normal((4, 3))
    expected = np.linalg.svdvals(x_np)
    self.assertTrue(np.all(expected >= 0.0))
    result = kd.matrix.svd_values(ds(x_np.tolist()))
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-4
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.svd_values,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.svd_values(I.x)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify conceptual equivalence."""

  def test_square_vs_numpy(self):
    x_np = np.array([[1.0, 2.0], [3.0, 4.0]])
    expected = np.linalg.svdvals(x_np)
    result = kd.matrix.svd_values(ds(x_np.tolist()))
    testing.assert_allclose(
        result,
        ds(expected.tolist(), schema_constants.FLOAT32),
        atol=1e-5,
    )

  def test_non_square_vs_numpy(self):
    x_np = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    expected = np.linalg.svdvals(x_np)
    result = kd.matrix.svd_values(ds(x_np.tolist()))
    testing.assert_allclose(
        result,
        ds(expected.tolist(), schema_constants.FLOAT32),
        atol=1e-5,
    )

  def test_random_matrix_vs_numpy(self):
    rng = np.random.default_rng(42)
    x_np = rng.standard_normal((6, 4))
    expected = np.linalg.svdvals(x_np)
    result = kd.matrix.svd_values(ds(x_np.tolist()))
    testing.assert_allclose(
        result,
        ds(expected.tolist(), schema_constants.FLOAT32),
        atol=1e-4,
    )

  def test_large_matrix_vs_numpy(self):
    # Large 10×10 matrix cross-validated against NumPy.
    rng = np.random.default_rng(123)
    x_np = rng.standard_normal((10, 10))
    expected = np.linalg.svdvals(x_np)
    result = kd.matrix.svd_values(ds(x_np.tolist()))
    testing.assert_allclose(
        result,
        ds(expected.tolist(), schema_constants.FLOAT32),
        atol=1e-4,
    )

  def test_batched_vs_numpy(self):
    rng = np.random.default_rng(42)
    x_np = rng.standard_normal((5, 3, 4))
    result = kd.matrix.svd_values(ds(x_np.tolist()))
    for i in range(5):
      expected = np.linalg.svdvals(x_np[i])
      testing.assert_allclose(
          result.L[i],
          ds(expected.tolist(), schema_constants.FLOAT32),
          atol=1e-4,
      )

  def test_symmetric_vs_numpy(self):
    # Symmetric positive-definite matrix: SVD = eigenvalues.
    x_np = np.array([[4.0, 2.0], [2.0, 3.0]])
    expected = np.linalg.svdvals(x_np)
    result = kd.matrix.svd_values(ds(x_np.tolist()))
    testing.assert_allclose(
        result,
        ds(expected.tolist(), schema_constants.FLOAT32),
        atol=1e-5,
    )

  def test_large_batched_vs_numpy(self):
    # Larger batch of 8x6 matrices.
    rng = np.random.default_rng(77)
    x_np = rng.standard_normal((10, 8, 6))
    result = kd.matrix.svd_values(ds(x_np.tolist()))
    for i in range(10):
      expected = np.linalg.svdvals(x_np[i])
      testing.assert_allclose(
          result.L[i],
          ds(expected.tolist(), schema_constants.FLOAT32),
          atol=1e-3,
      )


class ErrorTest(parameterized.TestCase):
  """Tests for error messages."""

  def test_1d_input(self):
    x = ds([1.0, 2.0, 3.0])
    with self.assertRaisesRegex(ValueError, r'expected at least 2D, got 1D'):
      kd.matrix.svd_values(x)

  def test_0d_input(self):
    x = ds(3.0)
    with self.assertRaisesRegex(ValueError, r'expected at least 2D, got 0D'):
      kd.matrix.svd_values(x)

  def test_string_schema_fails(self):
    x = ds([['a', 'b'], ['c', 'd']])
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowed schema: STRING'
    ):
      kd.matrix.svd_values(x)

  def test_strings_with_object_schema_fails(self):
    x = kd.obj(ds([['a', 'b'], ['c', 'd']]))
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowed schema: STRING'
    ):
      kd.matrix.svd_values(x)

  def test_non_uniform_rows_fails(self):
    # Jagged matrix: rows have different lengths.
    x = ds([[1.0, 2.0, 3.0], [4.0, 5.0]])
    with self.assertRaisesRegex(ValueError, r'non-uniform row sizes'):
      kd.matrix.svd_values(x)


if __name__ == '__main__':
  absltest.main()
