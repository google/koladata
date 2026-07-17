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

"""Tests for the kd.matrix.dot operator."""

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
    # (x, y) -> result:
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class MatrixDotTest(parameterized.TestCase):

  def test_basic(self):
    x = ds([1.0, 2.0, 3.0])
    y = ds([4.0, 5.0, 6.0])
    result = kd.matrix.dot(x, y)
    testing.assert_allclose(
        result, ds(32.0, schema_constants.FLOAT32)
    )

  def test_int32_preserves_type(self):
    x = ds([1, 2, 3])
    y = ds([4, 5, 6])
    result = kd.matrix.dot(x, y)
    testing.assert_equal(result, ds(32, schema_constants.INT32))

  def test_int64_preserves_type(self):
    x = ds([1, 2, 3], schema_constants.INT64)
    y = ds([4, 5, 6])
    result = kd.matrix.dot(x, y)
    testing.assert_equal(result, ds(32, schema_constants.INT64))

  def test_int64_float32_yields_float32(self):
    x = ds([1, 2, 3], schema_constants.INT64)
    y = ds([4.0, 5.0, 6.0], schema_constants.FLOAT32)
    result = kd.matrix.dot(x, y)
    testing.assert_equal(result, ds(32.0, schema_constants.FLOAT32))

  def test_int32_float64_yields_float64(self):
    x = ds([1, 2, 3])
    y = ds([4.0, 5.0, 6.0], schema_constants.FLOAT64)
    result = kd.matrix.dot(x, y)
    testing.assert_equal(result, ds(32.0, schema_constants.FLOAT64))

  def test_object_schema(self):
    x = ds([1, 2, 3], schema_constants.OBJECT)
    y = ds([4.0, 5.0, 6.0], schema_constants.FLOAT32)
    result = kd.matrix.dot(x, y)
    testing.assert_equal(result, ds(32.0, schema_constants.OBJECT))
    testing.assert_equal(result.get_obj_schema(), schema_constants.FLOAT32)

    x = ds([1, 2, 3])
    y = ds([4.0, 5.0, 6.0], schema_constants.OBJECT)
    result = kd.matrix.dot(x, y)
    testing.assert_equal(result, ds(32.0, schema_constants.OBJECT))
    testing.assert_equal(result.get_obj_schema(), schema_constants.FLOAT32)

  def test_sparse(self):
    x = ds([1.0, None, 3.0])
    y = ds([4.0, 5.0, None])
    result = kd.matrix.dot(x, y)
    testing.assert_allclose(
        result, ds(4.0, schema_constants.FLOAT32)
    )

  def test_batched_2d(self):
    # (2, 3) dot (2, 3) -> (2,). Batch of 2 dot products.
    x = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    y = ds([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])
    result = kd.matrix.dot(x, y)
    # batch 0: 1*1 + 2*0 + 3*0 = 1; batch 1: 4*0 + 5*1 + 6*0 = 5
    testing.assert_allclose(
        result, ds([1.0, 5.0], schema_constants.FLOAT32)
    )

  def test_batched_3d(self):
    # (2, 2, 2) dot (2, 2, 2) -> (2, 2). Batch of 4 dot products.
    x = ds([[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]])
    y = ds([[[1.0, 0.0], [0.0, 1.0]], [[1.0, 1.0], [1.0, 1.0]]])
    result = kd.matrix.dot(x, y)
    # batch (0,0): 1; (0,1): 4; (1,0): 11; (1,1): 15
    testing.assert_allclose(
        result, ds([[1.0, 4.0], [11.0, 15.0]], schema_constants.FLOAT32)
    )

  def test_batched_sparse(self):
    # (2, 3) with sparse values. None * value = None (ignored by agg_sum).
    x = ds([[1.0, None, 3.0], [None, 5.0, 6.0]])
    y = ds([[1.0, 2.0, 3.0], [4.0, 5.0, None]])
    result = kd.matrix.dot(x, y)
    # batch 0: 1*1 + None + 3*3 = 10; batch 1: None + 5*5 + None = 25
    testing.assert_allclose(
        result, ds([10.0, 25.0], schema_constants.FLOAT32)
    )

  def test_prefix_broadcast_no_batch_vs_1_batch(self):
    # x shape (3,) with no batch dims.
    # y shape (2, 3) with 1 batch dim (2,).
    # x batch () is a prefix of y batch (2,) -> output batch is (2,).
    x = ds([1.0, 2.0, 3.0])  # (3,)
    y = ds([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])  # (2, 3)
    result = kd.matrix.dot(x, y)
    # batch 0: 1*1 + 2*0 + 3*0 = 1; batch 1: 1*0 + 2*1 + 3*0 = 2
    testing.assert_allclose(
        result, ds([1.0, 2.0], schema_constants.FLOAT32)
    )

  def test_prefix_broadcast_1_batch_vs_2_batch(self):
    # x shape (2, 3, 2) with 2 batch dims (2, 3).
    # y shape (2, 2) with 1 batch dim (2,).
    # y batch (2,) is a prefix of x batch (2, 3) -> output batch is (2, 3).
    x = ds([
        [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
        [[7.0, 8.0], [9.0, 10.0], [11.0, 12.0]],
    ])  # (2, 3, 2)
    y = ds([[1.0, 0.0], [0.0, 1.0]])  # (2, 2)
    result = kd.matrix.dot(x, y)
    # batch (0, j): dot(x[0,j], y[0]) = dot(x[0,j], [1,0]) = x[0,j][0]
    # batch (1, j): dot(x[1,j], y[1]) = dot(x[1,j], [0,1]) = x[1,j][1]
    testing.assert_allclose(
        result,
        ds([[1.0, 3.0, 5.0], [8.0, 10.0, 12.0]], schema_constants.FLOAT32),
    )

  def test_prefix_broadcast_mismatched_dims_fails(self):
    # x shape (2, 3), y shape (3, 3).
    # x batch (2,) is NOT a prefix of y batch (3,) -> error.
    x = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])  # (2, 3)
    y = ds(
        [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
    )  # (3, 3)
    with self.assertRaises(ValueError):
      kd.matrix.dot(x, y)

  def test_broadcast_integer(self):
    # 1D integer x 2D integer -> 1D integer.
    x = ds([1, 2, 3])  # (3,)
    y = ds([[1, 0, 0], [0, 1, 0]])  # (2, 3)
    result = kd.matrix.dot(x, y)
    testing.assert_equal(result, ds([1, 2], schema_constants.INT32))

  def test_jagged_vector_sizes(self):
    x = ds([[1.0, 2.0], [3.0, 4.0, 5.0]])
    y = ds([[1.0, 0.0], [1.0, 1.0, 1.0]])
    result = kd.matrix.dot(x, y)
    testing.assert_allclose(
        result, ds([1.0, 12.0], schema_constants.FLOAT32)
    )

  def test_jagged_vector_sizes_integer(self):
    x = ds([[1, 2], [3, 4, 5]])
    y = ds([[1, 0], [1, 1, 1]])
    result = kd.matrix.dot(x, y)
    testing.assert_equal(result, ds([1, 12], schema_constants.INT32))

  def test_jagged_vector_sizes_mismatch_fails(self):
    x = ds([[1.0, 2.0], [3.0, 4.0, 5.0]])
    y = ds([[1.0, 2.0, 3.0], [4.0, 5.0]])
    with self.assertRaises(ValueError):
      kd.matrix.dot(x, y)

  def test_jagged_vector_sizes_with_broadcast(self):
    x = ds(
        [[[1.0, 2.0], [3.0, 4.0]], [[10.0, 20.0, 30.0], [40.0, 50.0, 60.0]]]
    )
    y = ds([[1.0, 0.0], [1.0, 1.0, 1.0]])
    result = kd.matrix.dot(x, y)
    testing.assert_allclose(
        result,
        ds([[1.0, 3.0], [60.0, 150.0]], schema_constants.FLOAT32),
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.dot,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.dot(I.x, I.y)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify the conceptual equivalence."""

  def test_dot_vs_numpy(self):
    a_np = np.array([1.0, 2.0, 3.0, 4.0])
    b_np = np.array([5.0, 6.0, 7.0, 8.0])
    expected = np.dot(a_np, b_np)
    result = kd.matrix.dot(ds(a_np.tolist()), ds(b_np.tolist()))
    testing.assert_allclose(result, ds(expected, schema_constants.FLOAT32))

  def test_dot_batched_vs_numpy(self):
    a_np = np.random.randn(5, 8)
    b_np = np.random.randn(5, 8)
    expected = [np.dot(a_np[i], b_np[i]) for i in range(5)]
    result = kd.matrix.dot(ds(a_np.tolist()), ds(b_np.tolist()))
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )


class ErrorTest(parameterized.TestCase):
  """Exhaustive tests for error messages."""

  def test_dot_0d_input_fails(self):
    x = ds(1.0)
    y = ds([1.0, 2.0])
    with self.assertRaisesRegex(ValueError, r'at least 1 dimension'):
      kd.matrix.dot(x, y)

  def test_dot_length_mismatch_fails(self):
    x = ds([1.0, 2.0, 3.0])
    y = ds([1.0, 2.0])
    with self.assertRaisesRegex(ValueError, r'inner dimension mismatch'):
      kd.matrix.dot(x, y)


if __name__ == '__main__':
  absltest.main()
