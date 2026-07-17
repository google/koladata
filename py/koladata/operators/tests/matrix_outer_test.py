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

"""Tests for the kd.matrix.outer operator."""

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


class MatrixOuterTest(parameterized.TestCase):

  def test_basic(self):
    x = ds([1.0, 2.0, 3.0])
    y = ds([4.0, 5.0])
    result = kd.matrix.outer(x, y)
    testing.assert_allclose(
        result,
        ds([[4.0, 5.0], [8.0, 10.0], [12.0, 15.0]], schema_constants.FLOAT32),
    )

  def test_integer_preserves_type(self):
    x = ds([1, 2])
    y = ds([3, 4])
    result = kd.matrix.outer(x, y)
    testing.assert_equal(
        result, ds([[3, 4], [6, 8]], schema_constants.INT32)
    )

  def test_int64_float32_yields_float32(self):
    x = ds([1, 2], schema_constants.INT64)
    y = ds([3.0, 4.0])
    result = kd.matrix.outer(x, y)
    testing.assert_equal(
        result, ds([[3.0, 4.0], [6.0, 8.0]], schema_constants.FLOAT32)
    )

  def test_object_schema(self):
    x = ds([1, 2], schema_constants.OBJECT)
    y = ds([3.0, 4.0])
    result = kd.matrix.outer(x, y)
    testing.assert_equal(
        result,
        ds([[3.0, 4.0], [6.0, 8.0]], schema_constants.OBJECT),
    )
    testing.assert_equal(
        result.get_obj_schema(),
        ds([
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
        ]),
    )

  def test_none_schema(self):
    x = ds([None, None])
    y = ds([None, None])
    result = kd.matrix.outer(x, y)
    testing.assert_equal(
        result,
        # INT32 is the narrowest numeric schema that can store zero.
        ds([[0, 0], [0, 0]], schema_constants.INT32),
    )

  def test_batched_same_shape(self):
    # Batch of 2: each pair of vectors has outer product computed.
    x = ds([[1.0, 2.0], [3.0, 4.0]])  # (2, 2)
    y = ds([[5.0, 6.0], [7.0, 8.0]])  # (2, 2)
    result = kd.matrix.outer(x, y)
    # result shape: (2, 2, 2)
    testing.assert_allclose(
        result,
        ds([
            [[5.0, 6.0], [10.0, 12.0]],
            [[21.0, 24.0], [28.0, 32.0]],
        ], schema_constants.FLOAT32),
    )

  def test_prefix_broadcast_no_batch_vs_1_batch(self):
    # x shape (3,) with no batch dims.
    # y shape (2, 4) with 1 batch dim (2,).
    # x batch () is a prefix of y batch (2,) -> output batch is (2,).
    # x is broadcast: the same vector [1,2,3] is used for each batch element.
    x = ds([1.0, 2.0, 3.0])  # (3,)
    y = ds([[10.0, 20.0], [30.0, 40.0]])  # (2, 2)
    result = kd.matrix.outer(x, y)
    # output shape: (2, 3, 2)
    testing.assert_allclose(
        result,
        ds([
            [[10.0, 20.0], [20.0, 40.0], [30.0, 60.0]],
            [[30.0, 40.0], [60.0, 80.0], [90.0, 120.0]],
        ], schema_constants.FLOAT32),
    )

  def test_prefix_broadcast_1_batch_vs_2_batch(self):
    # x shape (2, 3, 2) with 2 batch dims (2, 3).
    # y shape (2, 3) with 1 batch dim (2,).
    # y batch (2,) is a prefix of x batch (2, 3) -> output batch is (2, 3).
    # y is broadcast: y[i] is reused for all j in x[i, j].
    x = ds([
        [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
        [[7.0, 8.0], [9.0, 10.0], [11.0, 12.0]],
    ])  # (2, 3, 2)
    y = ds([[10.0, 20.0, 30.0], [40.0, 50.0, 60.0]])  # (2, 3)
    result = kd.matrix.outer(x, y)
    # output shape: (2, 3, 2, 3)
    # batch (0, j): x[0,j] outer y[0] = x[0,j] outer [10,20,30]
    # batch (1, j): x[1,j] outer y[1] = x[1,j] outer [40,50,60]
    testing.assert_allclose(
        result,
        ds([
            [
                [[10.0, 20.0, 30.0], [20.0, 40.0, 60.0]],  # x[0,0]=[1,2]
                [[30.0, 60.0, 90.0], [40.0, 80.0, 120.0]],  # x[0,1]=[3,4]
                [[50.0, 100.0, 150.0], [60.0, 120.0, 180.0]],  # x[0,2]=[5,6]
            ],
            [
                [[280.0, 350.0, 420.0], [320.0, 400.0, 480.0]],  # x[1,0]
                [[360.0, 450.0, 540.0], [400.0, 500.0, 600.0]],  # x[1,1]
                [[440.0, 550.0, 660.0], [480.0, 600.0, 720.0]],  # x[1,2]
            ],
        ], schema_constants.FLOAT32),
    )

  def test_prefix_broadcast_mismatched_dims_fails(self):
    # x shape (2, 3), y shape (3, 2).
    # x batch (2,) is NOT a prefix of y batch (3,) -> error.
    x = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])  # (2, 3)
    y = ds([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])  # (3, 2)
    with self.assertRaises(ValueError):
      kd.matrix.outer(x, y)

  def test_batched_sparse_in_vectors(self):
    # (2, 2) with sparse entries; None treated as 0 in outer product.
    x = ds([[1.0, None], [None, 4.0]])
    y = ds([[5.0, 6.0], [7.0, 8.0]])
    result = kd.matrix.outer(x, y)
    # batch 0: [1,0] outer [5,6] = [[5,6],[0,0]]
    # batch 1: [0,4] outer [7,8] = [[0,0],[28,32]]
    testing.assert_allclose(
        result,
        ds([
            [[5.0, 6.0], [0.0, 0.0]],
            [[0.0, 0.0], [28.0, 32.0]],
        ], schema_constants.FLOAT32),
    )

  def test_jagged_vector_sizes(self):
    x = ds([[1.0, 2.0], [3.0]])
    y = ds([[1.0, 0.0, 1.0], [5.0, 6.0]])
    result = kd.matrix.outer(x, y)
    testing.assert_allclose(
        result,
        ds(
            [[[1.0, 0.0, 1.0], [2.0, 0.0, 2.0]], [[15.0, 18.0]]],
            schema_constants.FLOAT32,
        ),
    )
    x = ds([[1, 2], [3]])
    y = ds([[1, 0], [5, 6, 7]])
    result = kd.matrix.outer(x, y)
    testing.assert_equal(
        result,
        ds([[[1, 0], [2, 0]], [[15, 18, 21]]], schema_constants.INT32),
    )

  def test_jagged_vector_sizes_with_broadcast(self):
    x = ds(
        [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0, 7.0], [8.0, 9.0, 10.0]]]
    )
    y = ds([[10.0, 20.0], [100.0, 200.0, 300.0]])
    result = kd.matrix.outer(x, y)
    testing.assert_allclose(
        result,
        ds([
            [[[10.0, 20.0], [20.0, 40.0]], [[30.0, 60.0], [40.0, 80.0]]],
            [
                [
                    [500.0, 1000.0, 1500.0],
                    [600.0, 1200.0, 1800.0],
                    [700.0, 1400.0, 2100.0],
                ],
                [
                    [800.0, 1600.0, 2400.0],
                    [900.0, 1800.0, 2700.0],
                    [1000.0, 2000.0, 3000.0],
                ],
            ],
        ], schema_constants.FLOAT32),
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.outer,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.outer(I.x, I.y)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify the conceptual equivalence."""

  def test_outer_vs_numpy(self):
    a_np = np.array([1.0, 2.0, 3.0])
    b_np = np.array([4.0, 5.0, 6.0])
    expected = np.outer(a_np, b_np)
    result = kd.matrix.outer(ds(a_np.tolist()), ds(b_np.tolist()))
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32)
    )

  def test_outer_batched_vs_numpy(self):
    a_np = np.random.randn(4, 3)
    b_np = np.random.randn(4, 5)
    result = kd.matrix.outer(ds(a_np.tolist()), ds(b_np.tolist()))
    expected = [np.outer(a_np[i], b_np[i]).tolist() for i in range(4)]
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )


class ErrorTest(parameterized.TestCase):
  """Exhaustive tests for error messages."""

  def test_outer_0d_input_fails(self):
    x = ds(1.0)
    y = ds([1.0, 2.0])
    with self.assertRaisesRegex(ValueError, r'at least 1 dimension'):
      kd.matrix.outer(x, y)
    with self.assertRaisesRegex(ValueError, r'at least 1 dimension'):
      kd.matrix.outer(y, x)

  def test_outer_both_0d_fails(self):
    x = ds(1.0)
    y = ds(2.0)
    with self.assertRaisesRegex(ValueError, r'at least 1 dimension'):
      kd.matrix.outer(x, y)


if __name__ == '__main__':
  absltest.main()
