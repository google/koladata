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

"""Tests for the kd.matrix.transpose operator."""

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
    (DATA_SLICE, DATA_SLICE),
])


class MatrixTransposeTest(parameterized.TestCase):

  def test_basic(self):
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])  # 2x3
    result = kd.matrix.transpose(a)
    expected = ds([[1.0, 4.0], [2.0, 5.0], [3.0, 6.0]])  # 3x2
    testing.assert_allclose(result, expected)

  def test_square(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    result = kd.matrix.transpose(a)
    testing.assert_allclose(result, ds([[1.0, 3.0], [2.0, 4.0]]))

  def test_integer_preserves_type(self):
    a = ds([[1, 2], [3, 4]])
    result = kd.matrix.transpose(a)
    testing.assert_equal(result, ds([[1, 3], [2, 4]]))

  def test_single_row(self):
    a = ds([[1.0, 2.0, 3.0]])  # 1x3
    result = kd.matrix.transpose(a)
    testing.assert_allclose(result, ds([[1.0], [2.0], [3.0]]))

  def test_sparse_preserves_none(self):
    a = ds([[1.0, 2.0], [None, 4.0]])
    result = kd.matrix.transpose(a)
    testing.assert_allclose(result, ds([[1.0, None], [2.0, 4.0]]))

  def test_batched_3d(self):
    # Batch of 2 matrices, each 2x3.
    a = ds([
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
        [[7.0, 8.0, 9.0], [10.0, 11.0, 12.0]],
    ])
    result = kd.matrix.transpose(a)
    expected = ds([
        [[1.0, 4.0], [2.0, 5.0], [3.0, 6.0]],
        [[7.0, 10.0], [8.0, 11.0], [9.0, 12.0]],
    ])
    testing.assert_allclose(result, expected)

  def test_batched_3d_sparse(self):
    # Batch of 2 matrices with None values.
    a = ds([
        [[1.0, None], [3.0, 4.0]],
        [[None, 6.0], [7.0, None]],
    ])
    result = kd.matrix.transpose(a)
    expected = ds([
        [[1.0, 3.0], [None, 4.0]],
        [[None, 7.0], [6.0, None]],
    ])
    testing.assert_allclose(result, expected)

  def test_jagged_batch_same_matrix_size(self):
    # Batch of 3 matrices each 2x3 (uniform batch, uniform matrix dims).
    # This still works because all dims are uniform.
    a = ds([
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
        [[7.0, 8.0, 9.0], [10.0, 11.0, 12.0]],
        [[13.0, 14.0, 15.0], [16.0, 17.0, 18.0]],
    ])
    result = kd.matrix.transpose(a)
    expected = ds([
        [[1.0, 4.0], [2.0, 5.0], [3.0, 6.0]],
        [[7.0, 10.0], [8.0, 11.0], [9.0, 12.0]],
        [[13.0, 16.0], [14.0, 17.0], [15.0, 18.0]],
    ])
    testing.assert_allclose(result, expected)

  def test_jagged_batch_different_matrix_sizes(self):
    # Two matrices with different shapes, created via ds with
    # jagged nested lists:
    # Matrix 0: 2x3 = [[1,2,3],[4,5,6]]
    # Matrix 1: 3x2 = [[7,8],[9,10],[11,12]]
    # Shape: dim0 = 2, dim1 = [2, 3], dim2 = [3, 3, 2, 2, 2]
    a = ds([
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
        [[7.0, 8.0], [9.0, 10.0], [11.0, 12.0]],
    ])
    result = kd.matrix.transpose(a)
    # Transposed:
    # Matrix 0 (2x3 -> 3x2): [[1,4],[2,5],[3,6]]
    # Matrix 1 (3x2 -> 2x3): [[7,9,11],[8,10,12]]
    testing.assert_allclose(
        result,
        ds([
            [[1.0, 4.0], [2.0, 5.0], [3.0, 6.0]],
            [[7.0, 9.0, 11.0], [8.0, 10.0, 12.0]],
        ]),
    )

  def test_jagged_batch_sparse(self):
    # Jagged batch with None values.
    # Matrix 0: 2x3 with sparse, Matrix 1: 3x2 with sparse.
    a = ds([
        [[1.0, None, 3.0], [4.0, 5.0, 6.0]],
        [[None, 8.0], [9.0, None], [11.0, 12.0]],
    ])
    result = kd.matrix.transpose(a)
    testing.assert_allclose(
        result,
        ds([
            [[1.0, 4.0], [None, 5.0], [3.0, 6.0]],
            [[None, 9.0, 11.0], [8.0, None, 12.0]],
        ]),
    )

  def test_jagged_batch_integer(self):
    # Same as test_jagged_batch_different_matrix_sizes but with integers.
    a = ds([
        [[1, 2, 3], [4, 5, 6]],
        [[7, 8], [9, 10], [11, 12]],
    ])
    result = kd.matrix.transpose(a)
    testing.assert_equal(
        result,
        ds([
            [[1, 4], [2, 5], [3, 6]],
            [[7, 9, 11], [8, 10, 12]],
        ]),
    )
    self.assertEqual(result.get_schema(), schema_constants.INT32)

  def test_mask(self):
    x = ds([
        [arolla.present(), arolla.present()],
        [arolla.missing(), arolla.present()],
    ])
    result = kd.matrix.transpose(x)
    testing.assert_equal(
        result,
        ds([
            [arolla.present(), arolla.missing()],
            [arolla.present(), arolla.present()],
        ]),
    )

  def test_text(self):
    x = ds([['a', 'b'], ['c', 'd']])
    result = kd.matrix.transpose(x)
    testing.assert_equal(result, ds([['a', 'c'], ['b', 'd']]))

  def test_entity(self):
    e1 = kd.obj(x=1)
    e2 = kd.obj(x=2)
    e3 = kd.obj(x=3)
    e4 = kd.obj(x=4)
    m = ds([[e1, e2], [e3, e4]])
    result = kd.matrix.transpose(m)
    testing.assert_equivalent(result.x, ds([[1, 3], [2, 4]]))

  def test_mixed_data(self):
    x = ds([[1, 'a'], [2, 'b']], schema_constants.OBJECT)
    result = kd.matrix.transpose(x)
    testing.assert_equal(
        result, ds([[1, 2], ['a', 'b']], schema_constants.OBJECT)
    )

  @parameterized.parameters(schema_constants.OBJECT, schema_constants.NONE)
  def test_empty_and_unknown(self, schema):
    x = ds([[None, None], [None, None]], schema)
    result = kd.matrix.transpose(x)
    testing.assert_equal(result, ds([[None, None], [None, None]], schema))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.transpose,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.transpose(I.x)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify the conceptual equivalence."""

  def test_transpose_vs_numpy(self):
    a_np = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    expected = a_np.T
    result = kd.matrix.transpose(ds(a_np.tolist()))
    testing.assert_allclose(result, ds(expected.tolist()))

  def test_transpose_square_vs_numpy(self):
    a_np = np.random.randn(5, 5)
    expected = a_np.T
    result = kd.matrix.transpose(ds(a_np.tolist()))
    testing.assert_allclose(result, ds(expected.tolist()), atol=1e-10)

  def test_transpose_batched_vs_numpy(self):
    a_np = np.random.randn(3, 4, 6)
    result = kd.matrix.transpose(ds(a_np.tolist()))
    expected = [a_np[i].T.tolist() for i in range(3)]
    testing.assert_allclose(result, ds(expected), atol=1e-10)


class ErrorTest(parameterized.TestCase):
  """Tests for error messages."""

  def test_transpose_0d_fails(self):
    x = ds(1.0)
    with self.assertRaisesRegex(ValueError, r'expected at least 2D.*got 0D'):
      kd.matrix.transpose(x)

  def test_transpose_1d_fails(self):
    x = ds([1.0, 2.0, 3.0])
    with self.assertRaisesRegex(ValueError, r'expected at least 2D.*got 1D'):
      kd.matrix.transpose(x)

  def test_transpose_non_uniform_rows_fails(self):
    # Matrix with rows of different lengths: [[1, 2, 3], [4, 5]].
    # This is not a valid matrix (non-uniform row widths).
    x = ds([[1.0, 2.0, 3.0], [4.0, 5.0]])
    with self.assertRaisesRegex(ValueError, r'non-uniform row sizes'):
      kd.matrix.transpose(x)


if __name__ == '__main__':
  absltest.main()
