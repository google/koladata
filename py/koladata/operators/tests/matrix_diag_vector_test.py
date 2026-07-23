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

"""Tests for the kd.matrix.diag_vector operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants
import numpy as np

I = input_container.InputContainer('I')
kde = kde_operators.kde
kd = eager_op_utils.operators_container('kd')
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([
    # (x,) -> result:
    (DATA_SLICE, DATA_SLICE),
    # (x, k) -> result:
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class MatrixDiagVectorTest(parameterized.TestCase):

  def test_basic(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    result = kd.matrix.diag_vector(a)
    testing.assert_allclose(result, ds([1.0, 4.0]))

  def test_nonsquare(self):
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])  # 2x3
    result = kd.matrix.diag_vector(a)
    testing.assert_allclose(result, ds([1.0, 5.0]))

  def test_float64_schema_preserved(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]], schema_constants.FLOAT64)
    result = kd.matrix.diag_vector(a)
    testing.assert_allclose(result, ds([1.0, 4.0], schema_constants.FLOAT64))

  def test_int32_schema_preserved(self):
    a = ds([[1, 2], [3, 4]])
    result = kd.matrix.diag_vector(a)
    testing.assert_equal(result, ds([1, 4]))

  def test_int64_schema_preserved(self):
    a = ds([[1, 2], [3, 4]], schema_constants.INT64)
    result = kd.matrix.diag_vector(a)
    testing.assert_equal(result, ds([1, 4], schema_constants.INT64))

  def test_sparse_diagonal(self):
    # Diagonal elements include None.
    a = ds([[None, 2.0], [3.0, 4.0]])
    result = kd.matrix.diag_vector(a)
    testing.assert_equal(result, ds([None, 4.0]))

  def test_batched_3d_input(self):
    # (2, 2, 2) input -> (2, 2) output. Batch of 2 matrices, each 2x2.
    a = ds([
        [[1.0, 2.0], [3.0, 4.0]],
        [[5.0, 6.0], [7.0, 8.0]],
    ])
    result = kd.matrix.diag_vector(a)
    testing.assert_allclose(result, ds([[1.0, 4.0], [5.0, 8.0]]))

  def test_batched_3d_nonsquare(self):
    # (2, 2, 3) input -> (2, 2) output. min(2,3) = 2.
    a = ds([
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
        [[7.0, 8.0, 9.0], [10.0, 11.0, 12.0]],
    ])
    result = kd.matrix.diag_vector(a)
    testing.assert_allclose(result, ds([[1.0, 5.0], [7.0, 11.0]]))

  def test_batched_4d_input(self):
    # (2, 2, 2, 2) -> (2, 2, 2). Two levels of batch.
    a = ds([
        [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]],
        [[[9.0, 10.0], [11.0, 12.0]], [[13.0, 14.0], [15.0, 16.0]]],
    ])
    result = kd.matrix.diag_vector(a)
    testing.assert_allclose(
        result,
        ds([[[1.0, 4.0], [5.0, 8.0]], [[9.0, 12.0], [13.0, 16.0]]]),
    )

  def test_batched_sparse_diag(self):
    # (2, 2, 2) with sparse values. Diagonal extraction should preserve None.
    a = ds([
        [[None, 2.0], [3.0, 4.0]],
        [[5.0, None], [None, None]],
    ])
    result = kd.matrix.diag_vector(a)
    testing.assert_equal(result, ds([[None, 4.0], [5.0, None]]))

  def test_jagged_matrix_dims(self):
    x = ds([[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], [[7.0, 8.0]]])
    result = kd.matrix.diag_vector(x)
    testing.assert_allclose(result, ds([[1.0, 5.0], [7.0]]))

  def test_jagged_matrix_dims_integer(self):
    x = ds([[[1, 2], [3, 4]], [[5, 6, 7]]])
    result = kd.matrix.diag_vector(x)
    testing.assert_equal(result, ds([[1, 4], [5]]))

  def test_text(self):
    x = ds([['a', 'b'], ['c', 'd']])
    result = kd.matrix.diag_vector(x)
    testing.assert_equal(result, ds(['a', 'd']))

  def test_bytes(self):
    x = ds([[b'a', b'b'], [b'c', b'd']])
    result = kd.matrix.diag_vector(x)
    testing.assert_equal(result, ds([b'a', b'd']))

  def test_entity(self):
    doc_schema = kd.named_schema('doc')
    e1 = doc_schema.new(x=1)
    e2 = doc_schema.new(x=2)
    e3 = doc_schema.new(x=3)
    e4 = doc_schema.new(x=4)
    m = ds([[e1, e2], [e3, e4]])
    result = kd.matrix.diag_vector(m)
    testing.assert_equivalent(result, ds([e1, e4]))
    testing.assert_equal(result.get_bag(), m.get_bag())

  def test_none_schema(self):
    x = ds([[None, None], [None, None]])
    result = kd.matrix.diag_vector(x)
    testing.assert_equal(result, ds([None, None]))

  def test_mixed_int_float(self):
    x = ds([[1, kd.obj('foo')], [3.0, b'bar']])
    result = kd.matrix.diag_vector(x)
    testing.assert_equivalent(result, ds([1, b'bar']))
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equal(result.get_bag(), x.get_bag())  # Same bag.

  def test_k_positive(self):
    # Extract the k=1 super-diagonal from a 3x3 matrix.
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
    result = kd.matrix.diag_vector(a, k=1)
    testing.assert_allclose(result, ds([2.0, 6.0]))

  def test_k_negative(self):
    # Extract the k=-1 sub-diagonal from a 3x3 matrix.
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
    result = kd.matrix.diag_vector(a, k=-1)
    testing.assert_allclose(result, ds([4.0, 8.0]))

  def test_k_out_of_bounds(self):
    # k larger than matrix dimensions -> empty result.
    a = ds([[1.0, 2.0], [3.0, 4.0]])

    result = kd.matrix.diag_vector(a, k=5)
    self.assertEqual(result.get_size(), 0)
    testing.assert_equal(result, ds([], schema_constants.FLOAT32))

    result = kd.matrix.diag_vector(a, k=-5)
    self.assertEqual(result.get_size(), 0)
    testing.assert_equal(result, ds([], schema_constants.FLOAT32))

  def test_k_zero_is_default(self):
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
    result_default = kd.matrix.diag_vector(a)
    result_k0 = kd.matrix.diag_vector(a, k=0)
    testing.assert_allclose(result_default, result_k0)

  def test_k_nonsquare_matrix_with_more_columns(self):
    # 2x4 matrix, k=2 -> elements at (0,2) and (1,3).
    a = ds([[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]])
    result = kd.matrix.diag_vector(a, k=2)
    testing.assert_allclose(result, ds([3.0, 8.0]))

    result = kd.matrix.diag_vector(a, k=3)
    testing.assert_allclose(result, ds([4.0]))

    result = kd.matrix.diag_vector(a, k=-1)
    testing.assert_allclose(result, ds([5.0]))

  def test_k_nonsquare_matrix_with_more_rows(self):
    # 4x2 matrix, k=-1 -> elements at (1,0), (2,1).
    a = ds([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0], [7.0, 8.0]])
    result = kd.matrix.diag_vector(a, k=-1)
    testing.assert_allclose(result, ds([3.0, 6.0]))

    result = kd.matrix.diag_vector(a, k=-3)
    testing.assert_allclose(result, ds([7.0]))

    result = kd.matrix.diag_vector(a, k=1)
    testing.assert_allclose(result, ds([2.0]))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.diag_vector,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.diag_vector(I.x)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify the conceptual equivalence."""

  def test_diag_vector_vs_numpy(self):
    a_np = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
    expected = np.diag(a_np)
    result = kd.matrix.diag_vector(ds(a_np.tolist()))
    testing.assert_allclose(result, ds(expected.tolist()))

  def test_diag_vector_batched_vs_numpy(self):
    a_np = np.random.randn(3, 4, 4)
    result = kd.matrix.diag_vector(ds(a_np.tolist()))
    expected = [np.diag(a_np[i]).tolist() for i in range(3)]
    testing.assert_allclose(result, ds(expected), atol=1e-5)

  @parameterized.parameters(-2, -1, 0, 1, 2)
  def test_k_vs_numpy(self, k_val):
    a_np = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
    expected = np.diag(a_np, k=k_val)
    result = kd.matrix.diag_vector(ds(a_np.tolist()), k=k_val)
    testing.assert_allclose(result, ds(expected.tolist()))


class ErrorTest(parameterized.TestCase):
  """Exhaustive tests for error messages."""

  def test_diag_vector_0d_fails(self):
    x = ds(1.0)
    with self.assertRaisesRegex(ValueError, r'expected at least 2D.*got 0D'):
      kd.matrix.diag_vector(x)

  def test_diag_vector_1d_fails(self):
    x = ds([1.0, 2.0, 3.0])
    with self.assertRaisesRegex(ValueError, r'expected at least 2D.*got 1D'):
      kd.matrix.diag_vector(x)

  def test_diag_vector_non_uniform_rows_fails(self):
    x = ds([[1.0, 2.0, 3.0], [4.0, 5.0]])
    with self.assertRaisesRegex(ValueError, r'non-uniform row sizes'):
      kd.matrix.diag_vector(x)

  def test_k_float_fails(self):
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(
        ValueError, r'argument `k` must be castable to INT64'
    ):
      kd.matrix.diag_vector(x, k=ds(1.5))

  def test_k_text_fails(self):
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(
        ValueError, r'argument `k` must be castable to INT64'
    ):
      kd.matrix.diag_vector(x, k=ds('hello'))

  def test_k_not_broadcastable_wrong_size_fails(self):
    # k has 3 elements but x is a single 2x2 matrix (batch size 1).
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(ValueError, r'cannot be expanded'):
      kd.matrix.diag_vector(x, k=ds([0, 1, -1]))

  def test_k_higher_rank_than_batch_fails(self):
    # x is 2D (batch is scalar), but k is 1D — k has higher rank than batch.
    x = ds([[1.0, 2.0], [3.0, 4.0]])  # (2, 2), batch shape is scalar
    with self.assertRaisesRegex(ValueError, r'cannot be expanded'):
      kd.matrix.diag_vector(x, k=ds([0, 1]))

  def test_k_2d_for_1d_batch_fails(self):
    # x is 3D (batch is 1D with 2 elements), but k is 2D — k outranks batch.
    x = ds([
        [[1.0, 2.0], [3.0, 4.0]],
        [[5.0, 6.0], [7.0, 8.0]],
    ])  # (2, 2, 2), batch shape (2,)
    with self.assertRaisesRegex(ValueError, r'cannot be expanded'):
      kd.matrix.diag_vector(x, k=ds([[0, 1], [1, 0]]))


class BroadcastKTest(parameterized.TestCase):
  """Tests for broadcastable k parameter."""

  def test_k_per_batch_element(self):
    # 2 matrices, k=[0, 1]: extract main diagonal from first, super-diagonal
    # from second.
    a = ds([
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
        [[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]],
    ])  # (2, 3, 3)
    result = kd.matrix.diag_vector(a, k=ds([0, 1]))
    # k=0 from first: [1.0, 5.0, 9.0]
    # k=1 from second: [20.0, 60.0]
    testing.assert_allclose(result, ds([[1.0, 5.0, 9.0], [20.0, 60.0]]))

  def test_k_scalar_broadcast(self):
    # Scalar k broadcasts to all batch elements — same as before.
    a = ds([
        [[1.0, 2.0], [3.0, 4.0]],
        [[5.0, 6.0], [7.0, 8.0]],
    ])
    result_scalar = kd.matrix.diag_vector(a, k=1)
    result_broadcast = kd.matrix.diag_vector(a, k=ds([1, 1]))
    testing.assert_allclose(result_scalar, result_broadcast)

  def test_k_mixed_positive_negative(self):
    # 3 matrices, k=[1, 0, -1].
    a = ds([
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
        [[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]],
        [[100.0, 200.0], [300.0, 400.0]],
    ])
    result = kd.matrix.diag_vector(a, k=ds([1, 0, -1]))
    # k=1: [2.0, 6.0]
    # k=0: [10.0, 50.0, 90.0]
    # k=-1: [300.0]
    testing.assert_allclose(
        result, ds([[2.0, 6.0], [10.0, 50.0, 90.0], [300.0]])
    )


if __name__ == '__main__':
  absltest.main()
