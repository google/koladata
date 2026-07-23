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

"""Tests for the kd.matrix.diag_matrix operator."""

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


class MatrixDiagMatrixTest(parameterized.TestCase):

  def test_basic(self):
    x = ds([1.0, 2.0, 3.0])
    result = kd.matrix.diag_matrix(x)
    testing.assert_equal(
        result,
        ds([[1.0, None, None], [None, 2.0, None], [None, None, 3.0]]),
    )

  def test_float64_schema_preserved(self):
    x = ds([1.0, 2.0, 3.0], schema_constants.FLOAT64)
    result = kd.matrix.diag_matrix(x)
    testing.assert_equal(
        result,
        ds(
            [[1.0, None, None], [None, 2.0, None], [None, None, 3.0]],
            schema_constants.FLOAT64,
        ),
    )

  def test_int32_schema_preserved(self):
    x = ds([1, 2, 3])
    result = kd.matrix.diag_matrix(x)
    testing.assert_equal(
        result,
        ds([[1, None, None], [None, 2, None], [None, None, 3]]),
    )

  def test_int64_schema_preserved(self):
    x = ds([1, 2, 3], schema_constants.INT64)
    result = kd.matrix.diag_matrix(x)
    testing.assert_equal(
        result,
        ds(
            [[1, None, None], [None, 2, None], [None, None, 3]],
            schema_constants.INT64,
        ),
    )

  def test_sparse_input(self):
    x = ds([1.0, None, 3.0])
    result = kd.matrix.diag_matrix(x)
    testing.assert_equal(
        result,
        ds([[1.0, None, None], [None, None, None], [None, None, 3.0]]),
    )

  def test_batched_2d_input(self):
    # 2 batches of 3-element vectors -> 2 batches of 3x3 diagonal matrices.
    x = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])  # (2, 3)
    result = kd.matrix.diag_matrix(x)
    # output shape: (2, 3, 3)
    testing.assert_equal(
        result,
        ds([
            [[1.0, None, None], [None, 2.0, None], [None, None, 3.0]],
            [[4.0, None, None], [None, 5.0, None], [None, None, 6.0]],
        ]),
    )

  def test_batched_3d_input(self):
    # (2, 2, 2) input -> (2, 2, 2, 2) output.
    x = ds([[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]])
    result = kd.matrix.diag_matrix(x)
    testing.assert_equal(
        result,
        ds([
            [
                [[1.0, None], [None, 2.0]],
                [[3.0, None], [None, 4.0]],
            ],
            [
                [[5.0, None], [None, 6.0]],
                [[7.0, None], [None, 8.0]],
            ],
        ]),
    )

  def test_batched_sparse(self):
    # (2, 3) with sparse values in the vector dimension.
    x = ds([[1.0, None, 3.0], [None, 5.0, None]])  # (2, 3)
    result = kd.matrix.diag_matrix(x)
    testing.assert_equal(
        result,
        ds([
            [[1.0, None, None], [None, None, None], [None, None, 3.0]],
            [[None, None, None], [None, 5.0, None], [None, None, None]],
        ]),
    )

  def test_roundtrip(self):
    x = ds([5.0, 10.0, 15.0])
    result = kd.matrix.diag_vector(kd.matrix.diag_matrix(x))
    testing.assert_equal(result, ds([5.0, 10.0, 15.0]))

  def test_jagged_vector_sizes(self):
    x = ds([[1.0, 2.0], [3.0, 4.0, 5.0]])
    result = kd.matrix.diag_matrix(x)
    testing.assert_equal(
        result,
        ds([
            [[1.0, None], [None, 2.0]],
            [[3.0, None, None], [None, 4.0, None], [None, None, 5.0]],
        ]),
    )

  def test_jagged_vector_sizes_integer(self):
    x = ds([[1, 2], [3]])
    result = kd.matrix.diag_matrix(x)
    testing.assert_equal(
        result,
        ds([[[1, None], [None, 2]], [[3]]]),
    )
    self.assertEqual(result.get_schema(), schema_constants.INT32)

  def test_text(self):
    x = ds(['a', 'b', 'c'])
    result = kd.matrix.diag_matrix(x)
    testing.assert_equal(
        result,
        ds([['a', None, None], [None, 'b', None], [None, None, 'c']]),
    )

  def test_bytes(self):
    x = ds([b'a', b'b', b'c'])
    result = kd.matrix.diag_matrix(x)
    testing.assert_equal(
        result,
        ds([[b'a', None, None], [None, b'b', None], [None, None, b'c']]),
    )

  def test_none(self):
    x = ds([None, None, None])
    result = kd.matrix.diag_matrix(x)
    testing.assert_equal(
        result,
        ds([[None, None, None], [None, None, None], [None, None, None]]),
    )

  def test_entity(self):
    doc_schema = kd.named_schema('doc')
    e1 = doc_schema.new(title='foo')
    e2 = doc_schema.new(title='bar')
    m = ds([e1, e2])
    result = kd.matrix.diag_matrix(m)
    testing.assert_equivalent(result, ds([[e1, None], [None, e2]]))

  def test_mixed_data_works(self):
    x = ds([1, kd.obj('foo'), 3.0])
    result = kd.matrix.diag_matrix(x)
    testing.assert_equivalent(
        result,
        ds([[1, None, None], [None, kd.obj('foo'), None], [None, None, 3.0]]),
    )
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equal(x.get_bag(), result.get_bag())  # Same bag.

  def test_k_positive(self):
    x = ds([1.0, 2.0, 3.0])
    result = kd.matrix.diag_matrix(x, k=1)
    testing.assert_equal(
        result,
        ds([
            [None, 1.0, None, None],
            [None, None, 2.0, None],
            [None, None, None, 3.0],
            [None, None, None, None],
        ]),
    )

  def test_k_negative(self):
    x = ds([1.0, 2.0, 3.0])
    result = kd.matrix.diag_matrix(x, k=-1)
    testing.assert_equal(
        result,
        ds([
            [None, None, None, None],
            [1.0, None, None, None],
            [None, 2.0, None, None],
            [None, None, 3.0, None],
        ]),
    )

  def test_k_large(self):
    x = ds([1.0, 2.0])
    result = kd.matrix.diag_matrix(x, k=2)
    testing.assert_equal(
        result,
        ds([
            [None, None, 1.0, None],
            [None, None, None, 2.0],
            [None, None, None, None],
            [None, None, None, None],
        ]),
    )

  def test_k_zero_is_default(self):
    x = ds([1.0, 2.0, 3.0])
    result_default = kd.matrix.diag_matrix(x)
    result_k0 = kd.matrix.diag_matrix(x, k=0)
    testing.assert_equal(result_default, result_k0)

  def test_roundtrip_with_k(self):
    x = ds([5.0, 10.0, 15.0])
    result = kd.matrix.diag_vector(kd.matrix.diag_matrix(x, k=1), k=1)
    testing.assert_equal(result, x)

  def test_roundtrip_with_negative_k(self):
    x = ds([5.0, 10.0, 15.0])
    result = kd.matrix.diag_vector(kd.matrix.diag_matrix(x, k=-2), k=-2)
    testing.assert_equal(result, x)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.diag_matrix,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.diag_matrix(I.x)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify the conceptual equivalence."""

  def test_diag_matrix_vs_numpy(self):
    v_np = np.array([1.0, 2.0, 3.0])
    expected = np.diag(v_np)
    # Use 0.0 to fill in the off-diagonal elements. NumPy's diag is dense,
    # while Koda's diag_matrix is sparse (None off-diagonal).
    result = kd.matrix.diag_matrix(ds(v_np.tolist())) | 0.0
    testing.assert_allclose(result, ds(expected.tolist()))

  def test_diag_matrix_batched_vs_numpy(self):
    v_np = np.random.randn(4, 5)
    # Use 0.0 to fill in the off-diagonal elements. NumPy's diag is dense,
    # while Koda's diag_matrix is sparse (None off-diagonal).
    result = kd.matrix.diag_matrix(ds(v_np.tolist())) | 0.0
    expected = [np.diag(v_np[i]).tolist() for i in range(4)]
    testing.assert_allclose(result, ds(expected), atol=1e-5)

  @parameterized.parameters(-2, -1, 0, 1, 2)
  def test_k_vs_numpy(self, k_val):
    v_np = np.array([1.0, 2.0, 3.0])
    expected = np.diag(v_np, k=k_val)
    result = kd.matrix.diag_matrix(ds(v_np.tolist()), k=k_val) | 0.0
    testing.assert_allclose(result, ds(expected.tolist()))


class ErrorTest(parameterized.TestCase):
  """Tests for error messages."""

  def test_diag_matrix_of_scalar_fails(self):
    x = ds(1.0)
    with self.assertRaisesRegex(ValueError, r'expected at least 1D.*got 0D'):
      kd.matrix.diag_matrix(x)

  def test_k_float_fails(self):
    x = ds([1.0, 2.0, 3.0])
    with self.assertRaisesRegex(
        ValueError, r'argument `k` must be castable to INT64'
    ):
      kd.matrix.diag_matrix(x, k=ds(1.5))

  def test_k_text_fails(self):
    x = ds([1.0, 2.0, 3.0])
    with self.assertRaisesRegex(
        ValueError, r'argument `k` must be castable to INT64'
    ):
      kd.matrix.diag_matrix(x, k=ds('hello'))

  def test_k_not_broadcastable_wrong_size_fails(self):
    # k has 2 elements but batch has 3 vectors — not broadcastable.
    x = ds([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])  # (3, 2)
    with self.assertRaisesRegex(ValueError, r'cannot be expanded'):
      kd.matrix.diag_matrix(x, k=ds([1, -1]))

  def test_k_higher_rank_than_batch_fails(self):
    # x is 1D (batch is scalar), but k is 1D — k has higher rank than batch.
    x = ds([1.0, 2.0, 3.0])  # (3,), batch shape is scalar
    with self.assertRaisesRegex(ValueError, r'cannot be expanded'):
      kd.matrix.diag_matrix(x, k=ds([1, 2, 3]))

  def test_k_2d_for_1d_batch_fails(self):
    # x is 2D (batch is 1D with 2 elements), but k is 2D — k outranks batch.
    x = ds([[1.0, 2.0], [3.0, 4.0]])  # (2, 2), batch shape (2,)
    with self.assertRaisesRegex(ValueError, r'cannot be expanded'):
      kd.matrix.diag_matrix(x, k=ds([[1, -1], [0, 2]]))


class BroadcastKTest(parameterized.TestCase):
  """Tests for broadcastable k parameter."""

  def test_k_per_batch_element(self):
    # 2 vectors, k=[1, -1]: first on super-diagonal, second on sub-diagonal.
    x = ds([[1.0, 2.0], [3.0, 4.0]])  # (2, 2)
    result = kd.matrix.diag_matrix(x, k=ds([1, -1]))
    # First vector k=1: 3x3 matrix with elements on super-diagonal.
    # Second vector k=-1: 3x3 matrix with elements on sub-diagonal.
    testing.assert_equal(
        result,
        ds([
            [[None, 1.0, None], [None, None, 2.0], [None, None, None]],
            [[None, None, None], [3.0, None, None], [None, 4.0, None]],
        ]),
    )

  def test_k_scalar_broadcast(self):
    # Scalar k broadcasts to all batch elements — same as before.
    x = ds([[1.0, 2.0], [3.0, 4.0]])  # (2, 2)
    result_scalar = kd.matrix.diag_matrix(x, k=1)
    result_broadcast = kd.matrix.diag_matrix(x, k=ds([1, 1]))
    testing.assert_equal(result_scalar, result_broadcast)

  def test_k_int64_accepted(self):
    x = ds([1.0, 2.0, 3.0])
    result = kd.matrix.diag_matrix(x, k=ds(1, schema_constants.INT64))
    expected = kd.matrix.diag_matrix(x, k=1)
    testing.assert_equal(result, expected)

  def test_k_mixed_positive_negative(self):
    x = ds([[10.0], [20.0], [30.0]])  # (3, 1)
    result = kd.matrix.diag_matrix(x, k=ds([0, 1, -1]))
    # k=0: 1x1 matrix [[10.0]]
    # k=1: 2x2 matrix [[None, 20.0], [None, None]]
    # k=-1: 2x2 matrix [[None, None], [30.0, None]]
    testing.assert_equal(
        result,
        ds([
            [[10.0]],
            [[None, 20.0], [None, None]],
            [[None, None], [30.0, None]],
        ]),
    )

  def test_roundtrip_broadcast_k(self):
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    k = ds([1, -1])
    result = kd.matrix.diag_vector(kd.matrix.diag_matrix(x, k=k), k=k)
    testing.assert_equal(result, x)


if __name__ == '__main__':
  absltest.main()
