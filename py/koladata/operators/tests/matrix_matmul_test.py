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

"""Tests for the kd.matrix.matmul operator."""

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
    # (a, b) -> result:
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    # (a, b, a_ndim or b_ndim) -> result:
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
    # (a, b, a_ndim, b_ndim) -> result:
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class MatrixMatmulTest(parameterized.TestCase):

  def test_2d_2d(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    b = ds([[5.0, 6.0], [7.0, 8.0]])
    result = kd.matrix.matmul(a, b)
    testing.assert_allclose(
        result, ds([[19.0, 22.0], [43.0, 50.0]], schema_constants.FLOAT32)
    )

  def test_2d_2d_integer(self):
    a = ds([[1, 2], [3, 4]])
    b = ds([[5, 6], [7, 8]])
    result = kd.matrix.matmul(a, b)
    testing.assert_equivalent(
        result, ds([[19, 22], [43, 50]], schema_constants.INT32)
    )

  def test_2d_2d_mixed_schemas(self):
    a = ds([[1, 2], [3, 4]])  # INT32
    b = ds([[5.0, 6.0], [7.0, 8.0]])  # FLOAT32
    result = kd.matrix.matmul(a, b)
    testing.assert_allclose(
        result, ds([[19.0, 22.0], [43.0, 50.0]], schema_constants.FLOAT32)
    )

    a = ds([[1.0, 2.0], [3.0, 4.0]])  # FLOAT32
    b = ds([[5, 6], [7, 8]])  # INT32
    result = kd.matrix.matmul(a, b)
    testing.assert_allclose(
        result, ds([[19.0, 22.0], [43.0, 50.0]], schema_constants.FLOAT32)
    )

  def test_2d_2d_mixed_int64_float32(self):
    # INT64 + FLOAT32 → FLOAT32 per Koda promotion: INT32→INT64→FLOAT32→FLOAT64.
    a = ds([[1, 2], [3, 4]], schema_constants.INT64)
    b = ds([[5.0, 6.0], [7.0, 8.0]])  # FLOAT32
    result = kd.matrix.matmul(a, b)
    testing.assert_allclose(
        result, ds([[19.0, 22.0], [43.0, 50.0]], schema_constants.FLOAT32)
    )

  def test_2d_2d_object_float(self):
    # OBJECT schema inputs should produce OBJECT schema output.
    a = kd.obj(ds([[1.0, 2.0], [3.0, 4.0]]))
    b = kd.obj(ds([[5.0, 6.0], [7.0, 8.0]]))
    result = kd.matrix.matmul(a, b)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equal(
        result.get_obj_schema(),
        ds([
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
        ]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT32),
        ds([[19.0, 22.0], [43.0, 50.0]], schema_constants.FLOAT32),
    )

  def test_2d_2d_object_mixed_with_typed(self):
    # Mixed: one OBJECT, one FLOAT32.
    a = kd.obj(ds([[1.0, 2.0], [3.0, 4.0]]))
    b = ds([[5.0, 6.0], [7.0, 8.0]])  # FLOAT32
    result = kd.matrix.matmul(a, b)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equal(
        result.get_obj_schema(),
        ds([
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
        ]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT32),
        ds([[19.0, 22.0], [43.0, 50.0]], schema_constants.FLOAT32),
    )

  def test_2d_2d_object_integer(self):
    # OBJECT-wrapped integers should go through the integer path.
    a = kd.obj(ds([[1, 2], [3, 4]]))
    b = kd.obj(ds([[5, 6], [7, 8]]))
    result = kd.matrix.matmul(a, b)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equal(
        result.get_obj_schema(),
        ds([
            [schema_constants.INT32, schema_constants.INT32],
            [schema_constants.INT32, schema_constants.INT32],
        ]),
    )
    testing.assert_equal(
        kd.cast_to(result, schema_constants.INT32),
        ds([[19, 22], [43, 50]], schema_constants.INT32),
    )

  def test_2d_2d_object_empty_with_int(self):
    # Empty OBJECT with INT32 → OBJECT with INT32 obj_schema.
    a = ds([[None, None], [None, None]], schema_constants.OBJECT)
    b = ds([[5, 6], [7, 8]])  # INT32
    result = kd.matrix.matmul(a, b)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equal(
        result.get_obj_schema(),
        ds([
            [schema_constants.INT32, schema_constants.INT32],
            [schema_constants.INT32, schema_constants.INT32],
        ]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.INT32),
        ds([[0, 0], [0, 0]], schema_constants.INT32),
    )

  def test_2d_2d_object_empty_with_empty(self):
    # Empty OBJECT with empty OBJECT → OBJECT with INT32 obj_schema.
    a = ds([[None, None], [None, None]], schema_constants.OBJECT)
    b = ds([[None, None], [None, None]], schema_constants.OBJECT)
    result = kd.matrix.matmul(a, b)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equal(
        result.get_obj_schema(),
        ds([
            [schema_constants.INT32, schema_constants.INT32],
            [schema_constants.INT32, schema_constants.INT32],
        ]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.INT32),
        ds([[0, 0], [0, 0]], schema_constants.INT32),
    )

  def test_2d_2d_object_empty_with_float(self):
    # Empty OBJECT with FLOAT32 → OBJECT with FLOAT32 obj_schema.
    a = ds([[None, None], [None, None]], schema_constants.OBJECT)
    b = ds([[5.0, 6.0], [7.0, 8.0]])  # FLOAT32
    result = kd.matrix.matmul(a, b)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equal(
        result.get_obj_schema(),
        ds([
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
            [schema_constants.FLOAT32, schema_constants.FLOAT32],
        ]),
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT32),
        ds([[0.0, 0.0], [0.0, 0.0]], schema_constants.FLOAT32),
    )

  def test_2d_1d(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    b = ds([5.0, 6.0])
    result = kd.matrix.matmul(a, b)
    testing.assert_allclose(result, ds([17.0, 39.0], schema_constants.FLOAT32))

  def test_1d_2d(self):
    a = ds([1.0, 2.0])
    b = ds([[3.0, 4.0], [5.0, 6.0]])
    result = kd.matrix.matmul(a, b)
    testing.assert_allclose(result, ds([13.0, 16.0], schema_constants.FLOAT32))

  def test_1d_1d(self):
    a = ds([1.0, 2.0, 3.0])
    b = ds([4.0, 5.0, 6.0])
    result = kd.matrix.matmul(a, b)
    testing.assert_allclose(result, ds(32.0, schema_constants.FLOAT32))

  def test_sparse_matmul(self):
    a = ds([[1.0, None], [None, 4.0]])
    b = ds([[5.0, 6.0], [7.0, 8.0]])
    result = kd.matrix.matmul(a, b)
    testing.assert_allclose(
        result, ds([[5.0, 6.0], [28.0, 32.0]], schema_constants.FLOAT32)
    )

    a = ds([[None, None], [None, None]])
    b = ds([[5.0, 6.0], [7.0, 8.0]])
    result = kd.matrix.matmul(a, b)
    testing.assert_allclose(
        result, ds([[0.0, 0.0], [0.0, 0.0]], schema_constants.FLOAT32)
    )

  def test_batched_3d_3d(self):
    a = ds([[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]])
    b = ds([[[1.0, 0.0], [0.0, 1.0]], [[2.0, 0.0], [0.0, 2.0]]])
    result = kd.matrix.matmul(a, b)
    expected = ds(
        [[[1.0, 2.0], [3.0, 4.0]], [[10.0, 12.0], [14.0, 16.0]]],
        schema_constants.FLOAT32,
    )
    testing.assert_allclose(result, expected)

  def test_batched_3d_sparse(self):
    # (2, 2, 2) with sparse values in the matrix entries.
    a = ds([
        [[1.0, None], [None, 4.0]],
        [[None, 2.0], [3.0, None]],
    ])
    b = ds([
        [[1.0, 0.0], [0.0, 1.0]],
        [[1.0, 0.0], [0.0, 1.0]],
    ])
    result = kd.matrix.matmul(a, b)
    # batch 0: [[1,0],[0,4]] @ I = [[1,0],[0,4]]
    # batch 1: [[0,2],[3,0]] @ I = [[0,2],[3,0]]
    testing.assert_allclose(
        result,
        ds(
            [
                [[1.0, 0.0], [0.0, 4.0]],
                [[0.0, 2.0], [3.0, 0.0]],
            ],
            schema_constants.FLOAT32,
        ),
    )

  def test_broadcast_2d_x_3d(self):
    # 2D (m,k) x 3D (B,k,n) -> 3D (B,m,n).
    # a has no batch dims, b has 1 batch dim. a is broadcast.
    a = ds([[1.0, 2.0], [3.0, 4.0]])  # (2, 2)
    b = ds([
        [[1.0, 0.0], [0.0, 1.0]],  # identity
        [[2.0, 0.0], [0.0, 2.0]],  # 2*identity
    ])  # (2, 2, 2)
    result = kd.matrix.matmul(a, b)
    # batch 0: [[1,2],[3,4]] @ I = [[1,2],[3,4]]
    # batch 1: [[1,2],[3,4]] @ 2I = [[2,4],[6,8]]
    expected = ds(
        [
            [[1.0, 2.0], [3.0, 4.0]],
            [[2.0, 4.0], [6.0, 8.0]],
        ],
        schema_constants.FLOAT32,
    )
    testing.assert_allclose(result, expected)

  def test_broadcast_3d_x_2d(self):
    # 3D (B,m,k) x 2D (k,n) -> 3D (B,m,n).
    # b has no batch dims, a has 1 batch dim. b is broadcast.
    a = ds([
        [[1.0, 2.0], [3.0, 4.0]],
        [[5.0, 6.0], [7.0, 8.0]],
    ])  # (2, 2, 2)
    b = ds([[1.0, 0.0], [0.0, 1.0]])  # identity (2, 2)
    result = kd.matrix.matmul(a, b)
    # Each batch entry times identity = itself.
    expected = ds(
        [
            [[1.0, 2.0], [3.0, 4.0]],
            [[5.0, 6.0], [7.0, 8.0]],
        ],
        schema_constants.FLOAT32,
    )
    testing.assert_allclose(result, expected)

  def test_broadcast_1d_x_3d(self):
    # 1D (k,) x 3D (B,k,n) -> 2D (B,n).
    # a is 1D with no batch dims, b has 1 batch dim.
    a = ds([1.0, 2.0])  # (2,)
    b = ds([
        [[1.0, 0.0], [0.0, 1.0]],
        [[3.0, 4.0], [5.0, 6.0]],
    ])  # (2, 2, 2)
    result = kd.matrix.matmul(a, b)
    # batch 0: [1,2] @ [[1,0],[0,1]] = [1, 2]
    # batch 1: [1,2] @ [[3,4],[5,6]] = [13, 16]
    testing.assert_allclose(
        result, ds([[1.0, 2.0], [13.0, 16.0]], schema_constants.FLOAT32)
    )

  def test_broadcast_prefix_3d_x_4d(self):
    # 3D (B1,m,k) x 4D (B1,B2,k,n) -> 4D (B1,B2,m,n).
    # a batch (B1,) is prefix of b batch (B1,B2). a is broadcast along B2.
    a = ds([
        [[1.0, 0.0], [0.0, 1.0]],  # identity for batch 0
        [[2.0, 0.0], [0.0, 2.0]],  # 2*identity for batch 1
    ])  # (2, 2, 2)
    b = ds([
        [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]],
        [[[1.0, 0.0], [0.0, 1.0]], [[2.0, 1.0], [1.0, 2.0]]],
    ])  # (2, 2, 2, 2)
    result = kd.matrix.matmul(a, b)
    # batch (0,0): I @ [[1,2],[3,4]] = [[1,2],[3,4]]
    # batch (0,1): I @ [[5,6],[7,8]] = [[5,6],[7,8]]
    # batch (1,0): 2I @ [[1,0],[0,1]] = [[2,0],[0,2]]
    # batch (1,1): 2I @ [[2,1],[1,2]] = [[4,2],[2,4]]
    expected = ds(
        [
            [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]],
            [[[2.0, 0.0], [0.0, 2.0]], [[4.0, 2.0], [2.0, 4.0]]],
        ],
        schema_constants.FLOAT32,
    )
    testing.assert_allclose(result, expected)

  def test_broadcast_mismatched_batch_dims_fails(self):
    # a batch (2,) is NOT a prefix of b batch (3,).
    a = ds([
        [[1.0, 2.0], [3.0, 4.0]],
        [[5.0, 6.0], [7.0, 8.0]],
    ])  # (2, 2, 2)
    b = ds([
        [[1.0, 0.0], [0.0, 1.0]],
        [[1.0, 0.0], [0.0, 1.0]],
        [[1.0, 0.0], [0.0, 1.0]],
    ])  # (3, 2, 2)
    with self.assertRaises(ValueError):
      kd.matrix.matmul(a, b)

  def test_broadcast_integer(self):
    # 2D integer x 3D integer -> 3D integer.
    a = ds([[1, 2], [3, 4]])  # (2, 2)
    b = ds([
        [[1, 0], [0, 1]],
        [[2, 0], [0, 2]],
    ])  # (2, 2, 2)
    result = kd.matrix.matmul(a, b)
    expected = ds(
        [
            [[1, 2], [3, 4]],
            [[2, 4], [6, 8]],
        ],
        schema_constants.INT32,
    )
    testing.assert_equal(result, expected)

  def test_a_ndim_1_batch_of_vectors_times_matrix(self):
    # a shape (2, 3), b shape (2, 3, 2).
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])  # (2, 3)
    b = ds([
        [[1.0, 0.0], [0.0, 1.0], [1.0, 0.0]],
        [[1.0, 1.0], [1.0, 1.0], [1.0, 1.0]],
    ])  # (2, 3, 2)
    # Default (a_ndim=2): a is a (2,3) matrix, b is a batch of 2 (3,2)
    # matrices. a is broadcast across the batch.
    #   batch 0: a @ b[0] -> (2, 2)
    #   batch 1: a @ b[1] -> (2, 2)
    #   result shape (2, 2, 2).
    result_default = kd.matrix.matmul(a, b)
    testing.assert_allclose(
        result_default,
        ds(
            [[[4.0, 2.0], [10.0, 5.0]], [[6.0, 6.0], [15.0, 15.0]]],
            schema_constants.FLOAT32,
        ),
    )
    self.assertEqual(result_default.get_ndim(), 3)
    # With a_ndim=1: a is a batch of 2 vectors (k=3), b is a batch of 2
    # (3,2) matrices. Batch dims are paired: a[i] @ b[i].
    #   batch 0: [1,2,3] @ b[0] = [4, 2]
    #   batch 1: [4,5,6] @ b[1] = [15, 15]
    #   result shape (2, 2).
    result_ndim1 = kd.matrix.matmul(a, b, a_ndim=1)
    testing.assert_allclose(
        result_ndim1,
        ds([[4.0, 2.0], [15.0, 15.0]], schema_constants.FLOAT32),
    )
    self.assertEqual(result_ndim1.get_ndim(), 2)

  def test_b_ndim_1_matrix_times_batch_of_vectors(self):
    # a shape (2, 3), b shape (2, 3).
    # Default (b_ndim=2): a is a 2x3 matrix, b is a 2x3 matrix.
    #   Inner dim mismatch: a has 3 cols, b has 2 rows -> error.
    # With b_ndim=1: a is a 2x3 matrix, b is a batch of 2 vectors of size 3.
    #   (m=2, k=3) x (B=2, k=3) -> (B=2, m=2) — matrix-vector products.
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])  # (2, 3)
    b = ds([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])  # (2, 3)
    # Default fails because 3 != 2.
    with self.assertRaises(ValueError):
      kd.matrix.matmul(a, b)
    # b_ndim=1: a @ b[0] = [1*1+2*0+3*0, 4*1+5*0+6*0] = [1, 4]
    #           a @ b[1] = [1*0+2*1+3*0, 4*0+5*1+6*0] = [2, 5]
    result = kd.matrix.matmul(a, b, b_ndim=1)
    testing.assert_allclose(
        result, ds([[1.0, 4.0], [2.0, 5.0]], schema_constants.FLOAT32)
    )
    self.assertEqual(result.get_ndim(), 2)

  def test_both_ndim_1_batched_dot_product(self):
    # a is (B, k), b is (B, k). Both ndim=1 -> batch of dot products.
    a = ds([[1.0, 2.0], [3.0, 4.0]])  # (2, 2)
    b = ds([[5.0, 6.0], [7.0, 8.0]])  # (2, 2)
    result = kd.matrix.matmul(a, b, a_ndim=1, b_ndim=1)
    # batch 0: [1,2]·[5,6] = 17
    # batch 1: [3,4]·[7,8] = 53
    testing.assert_allclose(result, ds([17.0, 53.0], schema_constants.FLOAT32))
    self.assertEqual(result.get_ndim(), 1)

  def test_a_ndim_1_with_broadcast(self):
    # a is (B, k), b is (B, k, n). a_ndim=1 means a is batch of vectors.
    a = ds([[1.0, 0.0], [0.0, 1.0]])  # (2, 2)
    b = ds([
        [[1.0, 2.0], [3.0, 4.0]],
        [[5.0, 6.0], [7.0, 8.0]],
    ])  # (2, 2, 2)
    result = kd.matrix.matmul(a, b, a_ndim=1)
    # batch 0: [1,0] @ [[1,2],[3,4]] = [1, 2]
    # batch 1: [0,1] @ [[5,6],[7,8]] = [7, 8]
    testing.assert_allclose(
        result, ds([[1.0, 2.0], [7.0, 8.0]], schema_constants.FLOAT32)
    )
    self.assertEqual(result.get_ndim(), 2)

  def test_explicit_ndim_2_same_as_default(self):
    # Verify that explicitly passing ndim=2 gives the same result as default.
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    b = ds([[5.0, 6.0], [7.0, 8.0]])
    result_default = kd.matrix.matmul(a, b)
    result_explicit = kd.matrix.matmul(a, b, a_ndim=2, b_ndim=2)
    testing.assert_allclose(result_explicit, result_default)

  def test_ndim_invalid_value_fails(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    b = ds([[5.0, 6.0], [7.0, 8.0]])
    with self.assertRaises(ValueError):
      kd.matrix.matmul(a, b, a_ndim=0)
    with self.assertRaises(ValueError):
      kd.matrix.matmul(a, b, a_ndim=3)

  def test_ndim_exceeds_rank_fails(self):
    a = ds([1.0, 2.0])  # rank 1
    b = ds([[1.0, 0.0], [0.0, 1.0]])  # rank 2
    with self.assertRaises(ValueError):
      kd.matrix.matmul(a, b, a_ndim=2)  # a has rank 1 but ndim=2

  def test_jagged_matrix_dims_2d_2d(self):
    a = ds([[[1.0, 2.0], [3.0, 4.0]], [[5.0]]])
    b = ds([[[1.0], [0.0]], [[2.0]]])
    result = kd.matrix.matmul(a, b)
    testing.assert_allclose(
        result, ds([[[1.0], [3.0]], [[10.0]]], schema_constants.FLOAT32)
    )

  def test_jagged_matrix_dims_2d_1d(self):
    a = ds([[[1.0, 2.0], [3.0, 4.0]], [[5.0]]])
    b = ds([[1.0, 0.0], [2.0]])
    result = kd.matrix.matmul(a, b, b_ndim=1)
    testing.assert_allclose(
        result, ds([[1.0, 3.0], [10.0]], schema_constants.FLOAT32)
    )

  def test_jagged_matrix_dims_integer(self):
    a = ds([[[1, 2], [3, 4]], [[5]]])
    b = ds([[[1], [0]], [[2]]])
    result = kd.matrix.matmul(a, b)
    testing.assert_equal(
        result, ds([[[1], [3]], [[10]]], schema_constants.INT32)
    )

  def test_jagged_matrix_dims_with_broadcast(self):
    a = ds([
        [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]],
        [[[9.0], [10.0]], [[11.0], [12.0]]],
    ])
    b = ds([[[1.0], [0.0]], [[3.0]]])
    result = kd.matrix.matmul(a, b)
    testing.assert_allclose(
        result,
        ds(
            [
                [[[1.0], [3.0]], [[5.0], [7.0]]],
                [[[27.0], [30.0]], [[33.0], [36.0]]],
            ],
            schema_constants.FLOAT32,
        ),
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.matmul,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.matmul(I.a, I.b)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify the conceptual equivalence."""

  def test_matmul_vs_numpy(self):
    a_np = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    b_np = np.array([[7.0, 8.0], [9.0, 10.0], [11.0, 12.0]])
    expected = a_np @ b_np
    result = kd.matrix.matmul(ds(a_np.tolist()), ds(b_np.tolist()))
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32)
    )

  def test_matmul_square_vs_numpy(self):
    a_np = np.random.randn(4, 4)
    b_np = np.random.randn(4, 4)
    expected = a_np @ b_np
    result = kd.matrix.matmul(ds(a_np.tolist()), ds(b_np.tolist()))
    testing.assert_allclose(
        result, ds(expected.tolist(), schema_constants.FLOAT32), atol=1e-5
    )

  def test_matmul_batched_vs_numpy(self):
    a_np = np.random.randn(3, 4, 5)
    b_np = np.random.randn(3, 5, 3)
    result = kd.matrix.matmul(ds(a_np.tolist()), ds(b_np.tolist()))
    expected = [(a_np[i] @ b_np[i]).tolist() for i in range(3)]
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )


class ErrorTest(parameterized.TestCase):
  """Tests for error messages."""

  def test_matmul_string_schema_fails(self):
    a = ds([['a', 'b'], ['c', 'd']])
    b = ds([['e', 'f'], ['g', 'h']])
    with self.assertRaisesRegex(ValueError, r'unsupported schema'):
      kd.matrix.matmul(a, b)

  def test_matmul_object_non_numeric_fails(self):
    a = kd.obj(ds([['a', 'b'], ['c', 'd']]))
    b = kd.obj(ds([['e', 'f'], ['g', 'h']]))
    with self.assertRaisesRegex(ValueError, r'non-numeric data'):
      kd.matrix.matmul(a, b)

  def test_matmul_object_mixed_dtype_fails(self):
    a = ds([[kd.int64(1), kd.obj(kd.int32(2))], [3, 4]])
    b = kd.obj(ds([[5, 6], [7, 8]]))
    with self.assertRaisesRegex(ValueError, r'mixed data is not supported'):
      kd.matrix.matmul(a, b)

  def test_matmul_0d_input_fails(self):
    a = ds(1.0)
    b = ds([1.0, 2.0])
    with self.assertRaisesRegex(ValueError, r'at least 1 dimension'):
      kd.matrix.matmul(a, b)

  def test_matmul_0d_both_inputs_fails(self):
    a = ds(1.0)
    b = ds(2.0)
    with self.assertRaisesRegex(ValueError, r'at least 1 dimension'):
      kd.matrix.matmul(a, b)

  def test_matmul_inner_dimension_mismatch_fails(self):
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])  # 2x3
    b = ds([[1.0, 2.0], [3.0, 4.0]])  # 2x2 (inner dim 2 != 3)
    with self.assertRaisesRegex(ValueError, r'inner dimension mismatch'):
      kd.matrix.matmul(a, b)

  def test_matmul_inner_dimension_mismatch_1d_2d_fails(self):
    a = ds([1.0, 2.0, 3.0])  # (3,)
    b = ds([[1.0, 2.0], [3.0, 4.0]])  # (2, 2) — inner dim 2 != 3
    with self.assertRaisesRegex(ValueError, r'inner dimension mismatch'):
      kd.matrix.matmul(a, b)

  def test_matmul_a_ndim_0_fails(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    b = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(
        ValueError, r'a_ndim must be 1, 2, or -1.*got 0'
    ):
      kd.matrix.matmul(a, b, a_ndim=0)

  def test_matmul_a_ndim_3_fails(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    b = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(
        ValueError, r'a_ndim must be 1, 2, or -1.*got 3'
    ):
      kd.matrix.matmul(a, b, a_ndim=3)

  def test_matmul_b_ndim_0_fails(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    b = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(
        ValueError, r'b_ndim must be 1, 2, or -1.*got 0'
    ):
      kd.matrix.matmul(a, b, b_ndim=0)

  def test_matmul_a_ndim_exceeds_rank_fails(self):
    a = ds([1.0, 2.0])  # rank 1
    b = ds([[1.0, 0.0], [0.0, 1.0]])  # rank 2
    with self.assertRaisesRegex(ValueError, r'a has rank 1 but a_ndim=2'):
      kd.matrix.matmul(a, b, a_ndim=2)

  def test_matmul_b_ndim_exceeds_rank_fails(self):
    a = ds([[1.0, 0.0], [0.0, 1.0]])  # rank 2
    b = ds([1.0, 2.0])  # rank 1
    with self.assertRaisesRegex(ValueError, r'b has rank 1 but b_ndim=2'):
      kd.matrix.matmul(a, b, b_ndim=2)

  def test_ndim_vector_fails(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    b = ds([[5.0, 6.0], [7.0, 8.0]])
    with self.assertRaisesRegex(
        ValueError,
        r'argument `a_ndim` must be an item holding INT64, got a slice of rank'
        r' 1 > 0',
    ):
      kd.matrix.matmul(a, b, a_ndim=ds([1, 2]))
    with self.assertRaisesRegex(
        ValueError,
        r'argument `b_ndim` must be an item holding INT64, got a slice of rank'
        r' 1 > 0',
    ):
      kd.matrix.matmul(a, b, b_ndim=ds([1, 2]))

  def test_matmul_non_uniform_rows_fails(self):
    # Jagged matrix: rows have different lengths.
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0]])
    b = ds([[1.0], [2.0]])
    with self.assertRaisesRegex(ValueError, r'non-uniform row sizes'):
      kd.matrix.matmul(a, b)

  def test_matmul_jagged_batch_dim(self):
    # Jagged batch dims are supported via Koda's prefix broadcasting.
    # 4D inputs where batch dim 1 is jagged. Each matrix is 2x2.
    # Shape: dim0=2, dim1=[2,3] (jagged!), dim2=2, dim3=2.
    # Batch dims are 0 and 1. There are 5 total batch elements.
    a = ds([
        [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]],
        [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]],
         [[9.0, 10.0], [11.0, 12.0]]],
    ])
    # b is identity matrices with the same jagged batch shape.
    b = ds([
        [[[1.0, 0.0], [0.0, 1.0]], [[1.0, 0.0], [0.0, 1.0]]],
        [
            [[1.0, 0.0], [0.0, 1.0]],
            [[1.0, 0.0], [0.0, 1.0]],
            [[1.0, 0.0], [0.0, 1.0]],
        ],
    ])
    # a @ I = a for each batch element.
    result = kd.matrix.matmul(a, b)
    testing.assert_allclose(result, kd.cast_to(a, schema_constants.FLOAT32))

    # Broadcast b to match a's batch shape also works:
    result = kd.matrix.matmul(a, ds([[1.0, 0.0], [0.0, 1.0]]))
    testing.assert_allclose(result, kd.cast_to(a, schema_constants.FLOAT32))

    # Shape: dim0=2, dim1=[1,3] (jagged!), dim2=2, dim3=2.
    # 4 total batch elements.
    a2 = ds([
        [[[1.0, 2.0], [3.0, 4.0]]],
        [
            [[1.0, 2.0], [3.0, 4.0]],
            [[5.0, 6.0], [7.0, 8.0]],
            [[9.0, 10.0], [11.0, 12.0]],
        ],
    ])
    b2 = ds([
        [[[1.0, 0.0], [0.0, 1.0]]],
        [
            [[1.0, 0.0], [0.0, 1.0]],
            [[1.0, 0.0], [0.0, 1.0]],
            [[1.0, 0.0], [0.0, 1.0]],
        ],
    ])
    result2 = kd.matrix.matmul(a2, b2)
    testing.assert_allclose(result2, kd.cast_to(a2, schema_constants.FLOAT32))

  def test_matmul_incompatible_jagged_batch_dims_fails(self):
    # 4D inputs where batch dim 1 is jagged. Each matrix is 2x2.
    # Shape: dim0=2, a's dim1=[2,3] (jagged!), b's dim1=[3,2] (jagged!) and
    # therefore not broadcast-compatible.
    a = ds([
        [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]],
        [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]],
         [[9.0, 10.0], [11.0, 12.0]]],
    ])
    b = ds([
        [
            [[1.0, 0.0], [0.0, 1.0]],
            [[1.0, 0.0], [0.0, 1.0]],
            [[1.0, 0.0], [0.0, 1.0]],
        ],
        [[[1.0, 0.0], [0.0, 1.0]], [[1.0, 0.0], [0.0, 1.0]]],
    ])
    with self.assertRaisesRegex(ValueError, r'not compatible'):
      kd.matrix.matmul(a, b)


if __name__ == '__main__':
  absltest.main()
