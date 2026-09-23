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
    # (x, tol) -> result:
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class MatrixRankTest(parameterized.TestCase):

  def test_identity_2x2(self):
    x = ds([[1.0, 0.0], [0.0, 1.0]])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(2, schema_constants.INT32))

  def test_identity_3x3(self):
    x = ds([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(3, schema_constants.INT32))

  def test_zero_matrix(self):
    x = ds([[0.0, 0.0], [0.0, 0.0]])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(0, schema_constants.INT32))

  def test_diagonal_matrix(self):
    x = ds([[3.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 2.0]])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(3, schema_constants.INT32))

  def test_rank_deficient(self):
    # [[1, 2], [2, 4]] is rank 1 (second row is 2x first).
    x = ds([[1.0, 2.0], [2.0, 4.0]])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(1, schema_constants.INT32))

  def test_rank_deficient_3x3(self):
    # Third row = first + second.
    x = ds([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [1.0, 1.0, 0.0]])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(2, schema_constants.INT32))

  def test_1x1_nonzero(self):
    x = ds([[5.0]])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(1, schema_constants.INT32))

  def test_1x1_zero(self):
    x = ds([[0.0]])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(0, schema_constants.INT32))

  def test_non_square_tall(self):
    # (3, 2) matrix, rank 2.
    x = ds([[1.0, 0.0], [0.0, 1.0], [0.0, 0.0]])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(2, schema_constants.INT32))

  def test_non_square_wide(self):
    # (2, 3) matrix, rank 2.
    x = ds([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(2, schema_constants.INT32))

  def test_non_square_rank_deficient(self):
    # (3, 2) rank 1: all rows are multiples of [1, 2].
    x = ds([[1.0, 2.0], [2.0, 4.0], [3.0, 6.0]])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(1, schema_constants.INT32))

  def test_empty_zero_by_zero(self):
    x = kd.empty_shaped(kd.shapes.new(0, 0), schema_constants.FLOAT32)
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(0, schema_constants.INT32))

  def test_empty_three_by_zero(self):
    x = kd.empty_shaped(kd.shapes.new(3, 0), schema_constants.FLOAT32)
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(0, schema_constants.INT32))

  def test_empty_zero_by_three(self):
    x = kd.empty_shaped(kd.shapes.new(0, 3), schema_constants.FLOAT32)
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(0, schema_constants.INT32))

  def test_none_schema(self):
    # All-None matrix -> rank 0 (missing treated as 0).
    x = ds([[None, None], [None, None]], schema_constants.NONE)
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(0, schema_constants.INT32))

  def test_sparse_missing_as_zero(self):
    # Missing treated as 0. Matrix is [[1, 0], [0, 4]], rank 2.
    x = ds([[1.0, None], [None, 4.0]])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(2, schema_constants.INT32))

  # --- Input type tests ---

  @parameterized.parameters(
      (schema_constants.INT32,),
      (schema_constants.INT64,),
      (schema_constants.FLOAT32,),
      (schema_constants.FLOAT64,),
  )
  def test_input_schema(self, input_schema):
    x = ds([[1, 0], [0, 1]], input_schema)
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds(2, schema_constants.INT32))

  # --- OBJECT schema tests ---

  @parameterized.parameters(
      (schema_constants.FLOAT32,),
      (schema_constants.FLOAT64,),
      (schema_constants.INT32,),
      (schema_constants.INT64,),
  )
  def test_object_schema(self, inner_schema):
    x = kd.obj(ds([[1.0, 0.0], [0.0, 1.0]], inner_schema))
    result = kd.matrix.rank(x)
    self.assertEqual(result.get_schema(), schema_constants.INT32)
    testing.assert_equal(result, ds(2, schema_constants.INT32))

  def test_object_schema_mixed_numeric_types(self):
    # Rows with different numeric types -> OBJECT schema input.
    x = ds([[kd.obj(1), 2], [3, 4.0]])
    self.assertEqual(x.get_schema(), schema_constants.OBJECT)
    result = kd.matrix.rank(x)
    self.assertEqual(result.get_schema(), schema_constants.INT32)

  # --- Tolerance parameter tests ---

  def test_explicit_tol_zero(self):
    # With tol=0, only exact zeros are excluded.
    # [[1, 2], [2, 4]] has sv=[5, 0]. With tol=0, rank=1 (sv=0 not > 0).
    x = ds([[1.0, 2.0], [2.0, 4.0]])
    result = kd.matrix.rank(x, tol=0.0)
    testing.assert_equal(result, ds(1, schema_constants.INT32))

  def test_explicit_tol_large(self):
    # With a very large tolerance, all SVs are below threshold -> rank 0.
    x = ds([[1.0, 0.0], [0.0, 1.0]])
    result = kd.matrix.rank(x, tol=10.0)
    testing.assert_equal(result, ds(0, schema_constants.INT32))

  def test_explicit_tol_between_svs(self):
    # [[3, 0], [0, 1]] has sv=[3, 1]. tol=2 -> rank 1 (only sv=3 > 2).
    x = ds([[3.0, 0.0], [0.0, 1.0]])
    result = kd.matrix.rank(x, tol=2.0)
    testing.assert_equal(result, ds(1, schema_constants.INT32))

  def test_explicit_tol_integer(self):
    # tol as integer should work.
    x = ds([[3.0, 0.0], [0.0, 1.0]])
    result = kd.matrix.rank(x, tol=2)
    testing.assert_equal(result, ds(1, schema_constants.INT32))

  def test_default_tol_nearly_singular(self):
    # Near-singular matrix: second SV is very small but nonzero.
    # Default tolerance should treat it as zero.
    eps = np.finfo(np.float64).eps
    x_np = np.array([[1.0, 0.0], [0.0, eps * 0.5]])
    self.assertEqual(np.linalg.matrix_rank(x_np), 1)
    result = kd.matrix.rank(ds(x_np.tolist()))
    testing.assert_equal(result, ds(1, schema_constants.INT32))

  def test_tol_none_uses_default(self):
    # Explicit tol=None should behave the same as omitting tol.
    x = ds([[1.0, 2.0], [2.0, 4.0]])
    result_default = kd.matrix.rank(x)
    result_none = kd.matrix.rank(x, tol=None)
    testing.assert_equal(result_default, result_none)

  def test_tol_nan_gives_rank_zero(self):
    # NaN tol is not treated as default. Since sv > NaN is always false,
    # the rank is 0 for any matrix when tol=NaN.
    x = ds([[3.0, 0.0], [0.0, 1.0]])
    result = kd.matrix.rank(x, tol=float('nan'))
    testing.assert_equal(result, ds(0, schema_constants.INT32))

  def test_batched_tol_per_matrix(self):
    # [[3, 0], [0, 1]] sv=[3,1]; [[3, 0], [0, 0.1]] sv=[3,0.1].
    # Per-matrix tol: [0.5, 0.05] -> [2, 2] (both SVs above tol for each).
    x = ds([
        [[3.0, 0.0], [0.0, 1.0]],
        [[3.0, 0.0], [0.0, 0.1]],
    ])
    result = kd.matrix.rank(x, tol=ds([0.5, 0.05]))
    testing.assert_equal(result, ds([2, 2], schema_constants.INT32))
    # tol: [2.0, 0.2] -> [1, 1] (only largest SV above tol for each).
    result2 = kd.matrix.rank(x, tol=ds([2.0, 0.2]))
    testing.assert_equal(result2, ds([1, 1], schema_constants.INT32))

  def test_batched_tol_mixed_present_missing(self):
    # Missing tol uses the default adaptive tolerance.
    # [[3, 0], [0, 1]] sv=[3,1]; default tol = 2*3*eps << 1 -> rank 2.
    # [[1, 0], [0, eps/2]] sv=[1,eps/2]; default tol = 2*1*eps > eps/2 -> rank 1
    eps = np.finfo(np.float64).eps
    x = ds([
        [[3.0, 0.0], [0.0, 1.0]],
        [[1.0, 0.0], [0.0, eps * 0.5]],
    ])
    # tol=[None, None] -> both use default adaptive tolerance.
    result_default = kd.matrix.rank(x)
    testing.assert_equal(result_default, ds([2, 1], schema_constants.INT32))
    # tol=[2.0, None] -> first uses 2.0, second uses default.
    result_mixed = kd.matrix.rank(x, tol=ds([2.0, None]))
    testing.assert_equal(result_mixed, ds([1, 1], schema_constants.INT32))

  def test_batched_tol_broadcast_scalar(self):
    # Scalar tol broadcasts to all matrices.
    x = ds([
        [[3.0, 0.0], [0.0, 1.0]],
        [[3.0, 0.0], [0.0, 0.1]],
    ])
    result = kd.matrix.rank(x, tol=ds(0.5))
    testing.assert_equal(result, ds([2, 1], schema_constants.INT32))

  def test_batched_tol_broadcast_4d(self):
    # 4D input (2, 1, 2, 2): tol is 1D (2,) -> broadcast to (2, 1).
    x = ds([
        [[[3.0, 0.0], [0.0, 1.0]]],
        [[[3.0, 0.0], [0.0, 0.1]]],
    ])
    result = kd.matrix.rank(x, tol=ds([0.5, 0.2]))
    testing.assert_equal(
        result, ds([[2], [1]], schema_constants.INT32)
    )

  # --- Batched tests ---

  def test_batched_3d(self):
    x = ds([
        [[1.0, 0.0], [0.0, 1.0]],  # rank 2
        [[1.0, 2.0], [2.0, 4.0]],  # rank 1
    ])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds([2, 1], schema_constants.INT32))

  def test_batched_4d(self):
    x = ds([
        [[[1.0, 0.0], [0.0, 1.0]], [[0.0, 0.0], [0.0, 0.0]]],
        [[[1.0, 2.0], [2.0, 4.0]], [[3.0, 0.0], [0.0, 2.0]]],
    ])
    result = kd.matrix.rank(x)
    testing.assert_equal(
        result, ds([[2, 0], [1, 2]], schema_constants.INT32)
    )

  def test_batched_with_tol(self):
    # [[3, 0], [0, 1]] sv=[3,1]; [[3, 0], [0, 0.1]] sv=[3,0.1].
    # tol=0.5 -> both keep only sv > 0.5.
    x = ds([
        [[3.0, 0.0], [0.0, 1.0]],
        [[3.0, 0.0], [0.0, 0.1]],
    ])
    result = kd.matrix.rank(x, tol=0.5)
    testing.assert_equal(result, ds([2, 1], schema_constants.INT32))

  def test_batched_sparse(self):
    x = ds([
        [[1.0, None], [None, 2.0]],  # -> diag(1, 2), rank 2
        [[None, None], [None, None]],  # -> zero, rank 0
    ])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds([2, 0], schema_constants.INT32))

  def test_jagged_matrix_dims(self):
    # Batch: 2x2 and 1x1 matrices.
    a1 = [[1.0, 0.0], [0.0, 1.0]]  # rank 2
    a2 = [[5.0]]  # rank 1
    x = ds([a1, a2])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds([2, 1], schema_constants.INT32))

  def test_jagged_matrix_dims_non_square(self):
    # (2, 3) and (3, 2) matrices.
    a1 = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]  # rank 2
    a2 = [[1.0, 0.0], [0.0, 1.0], [0.0, 0.0]]  # rank 2
    x = ds([a1, a2])
    result = kd.matrix.rank(x)
    testing.assert_equal(result, ds([2, 2], schema_constants.INT32))

  def test_jagged_batch_dimensions(self):
    # 4D input: batch dim 1 is jagged. Each matrix is 2x2.
    x = ds([
        [[[1.0, 0.0], [0.0, 1.0]], [[1.0, 2.0], [2.0, 4.0]]],
        [[[0.0, 0.0], [0.0, 0.0]]],
    ])
    result = kd.matrix.rank(x)
    testing.assert_equal(
        result, ds([[2, 1], [0]], schema_constants.INT32)
    )

  def test_jagged_batch_dimensions_with_tol(self):
    # 4D input: batch dim 1 is jagged. Each matrix is 2x2.
    # [[3,0],[0,1]] sv=[3,1]; [[3,0],[0,0.1]] sv=[3,0.1]; [[3,0],[0,2]] sv=[3,2]
    x = ds([
        [[[3.0, 0.0], [0.0, 1.0]], [[3.0, 0.0], [0.0, 0.1]]],
        [[[3.0, 0.0], [0.0, 2.0]]],
    ])
    # tol is 1D (size 2), broadcasts to the jagged batch (2, [2, 1]).
    # tol[0]=0.5 applies to both matrices in the first outer batch.
    # tol[1]=2.5 applies to the single matrix in the second outer batch.
    result = kd.matrix.rank(x, tol=ds([0.5, 2.5]))
    testing.assert_equal(
        result, ds([[2, 1], [1]], schema_constants.INT32)
    )

  # --- QType and view tests ---

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.rank,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.rank(I.x)))
    self.assertTrue(view.has_koda_view(kde.matrix.rank(I.x, tol=I.tol)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify conceptual equivalence."""

  def test_identity_vs_numpy(self):
    x_np = np.eye(4)
    expected = np.linalg.matrix_rank(x_np)
    result = kd.matrix.rank(ds(x_np.tolist()))
    testing.assert_equal(result, ds(int(expected), schema_constants.INT32))

  def test_rank_deficient_vs_numpy(self):
    x_np = np.array([[1.0, 2.0], [2.0, 4.0]])
    expected = np.linalg.matrix_rank(x_np)
    result = kd.matrix.rank(ds(x_np.tolist()))
    testing.assert_equal(result, ds(int(expected), schema_constants.INT32))

  def test_non_square_vs_numpy(self):
    x_np = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    expected = np.linalg.matrix_rank(x_np)
    result = kd.matrix.rank(ds(x_np.tolist()))
    testing.assert_equal(result, ds(int(expected), schema_constants.INT32))

  def test_random_matrix_vs_numpy(self):
    rng = np.random.default_rng(42)
    x_np = rng.standard_normal((6, 4))
    expected = np.linalg.matrix_rank(x_np)
    result = kd.matrix.rank(ds(x_np.tolist()))
    testing.assert_equal(result, ds(int(expected), schema_constants.INT32))

  def test_large_matrix_vs_numpy(self):
    rng = np.random.default_rng(123)
    x_np = rng.standard_normal((10, 10))
    expected = np.linalg.matrix_rank(x_np)
    result = kd.matrix.rank(ds(x_np.tolist()))
    testing.assert_equal(result, ds(int(expected), schema_constants.INT32))

  def test_with_explicit_tol_vs_numpy(self):
    x_np = np.array([[3.0, 0.0], [0.0, 1.0]])
    for tol in [0.0, 0.5, 1.5, 5.0]:
      with self.subTest(tol=tol):
        expected = np.linalg.matrix_rank(x_np, tol=tol)
        result = kd.matrix.rank(ds(x_np.tolist()), tol=tol)
        testing.assert_equal(
            result, ds(int(expected), schema_constants.INT32)
        )

  def test_batched_vs_numpy(self):
    rng = np.random.default_rng(42)
    x_np = rng.standard_normal((5, 3, 4))
    result = kd.matrix.rank(ds(x_np.tolist()))
    for i in range(5):
      expected = np.linalg.matrix_rank(x_np[i])
      testing.assert_equal(
          result.L[i], ds(int(expected), schema_constants.INT32)
      )

  def test_nearly_singular_vs_numpy(self):
    # Matrix with one very small singular value.
    x_np = np.diag([1.0, 1e-16])
    expected = np.linalg.matrix_rank(x_np)
    result = kd.matrix.rank(ds(x_np.tolist()))
    testing.assert_equal(result, ds(int(expected), schema_constants.INT32))


class ErrorTest(parameterized.TestCase):
  """Tests for error messages."""

  def test_1d_input(self):
    x = ds([1.0, 2.0, 3.0])
    with self.assertRaisesRegex(ValueError, r'expected at least 2D, got 1D'):
      kd.matrix.rank(x)

  def test_0d_input(self):
    x = ds(3.0)
    with self.assertRaisesRegex(ValueError, r'expected at least 2D, got 0D'):
      kd.matrix.rank(x)

  def test_string_schema_fails(self):
    x = ds([['a', 'b'], ['c', 'd']])
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowed schema: STRING'
    ):
      kd.matrix.rank(x)

  def test_strings_with_object_schema_fails(self):
    x = kd.obj(ds([['a', 'b'], ['c', 'd']]))
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowed schema: STRING'
    ):
      kd.matrix.rank(x)

  def test_non_uniform_rows_fails(self):
    x = ds([[1.0, 2.0, 3.0], [4.0, 5.0]])
    with self.assertRaisesRegex(ValueError, r'non-uniform row sizes'):
      kd.matrix.rank(x)

  def test_negative_tol_fails(self):
    x = ds([[1.0, 0.0], [0.0, 1.0]])
    with self.assertRaisesRegex(ValueError, r'non-negative'):
      kd.matrix.rank(x, tol=-1.0)

  def test_batched_negative_tol_fails(self):
    x = ds([
        [[1.0, 0.0], [0.0, 1.0]],
        [[1.0, 0.0], [0.0, 1.0]],
    ])
    with self.assertRaisesRegex(ValueError, r'non-negative'):
      kd.matrix.rank(x, tol=ds([1.0, -1.0]))

  def test_tol_too_many_dims_fails(self):
    x = ds([[1.0, 0.0], [0.0, 1.0]])
    with self.assertRaisesRegex(ValueError, r'fewer dimensions'):
      kd.matrix.rank(x, tol=ds([1.0, 2.0]))

  def test_string_tol_fails(self):
    x = ds([[1.0, 0.0], [0.0, 1.0]])
    with self.assertRaisesRegex(ValueError, r'numeric'):
      kd.matrix.rank(x, tol='abc')

  def test_tol_broadcast_incompatible_fails(self):
    # 3D x has batch shape (2,), but tol has size 3 -> not broadcastable.
    x = ds([
        [[1.0, 0.0], [0.0, 1.0]],
        [[1.0, 0.0], [0.0, 1.0]],
    ])
    with self.assertRaisesRegex(ValueError, r'cannot be expanded'):
      kd.matrix.rank(x, tol=ds([1.0, 2.0, 3.0]))


if __name__ == '__main__':
  absltest.main()
