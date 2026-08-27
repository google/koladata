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
    # (x, offset) -> result:
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class MatrixTraceTest(parameterized.TestCase):

  def test_empty_zero_by_zero(self):
    # 0×0 matrix: trace is 0. Note that an empty matrix with schema NONE
    # produces a result with schema INT32, because INT32 is the narrowest
    # numeric type that can represent 0.
    a = kd.empty_shaped(kd.shapes.new(0, 0), schema_constants.NONE)
    result = kd.matrix.trace(a)
    testing.assert_allclose(result, ds(0, schema_constants.INT32))

  def test_basic(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    result = kd.matrix.trace(a)
    testing.assert_allclose(result, ds(5.0, schema_constants.FLOAT32))

  def test_basic_float64(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]], schema_constants.FLOAT64)
    result = kd.matrix.trace(a)
    testing.assert_allclose(result, ds(5.0, schema_constants.FLOAT64))

  def test_3x3(self):
    a = ds([[1.0, 1.0, 1.0], [2.0, 2.0, 2.0], [3.0, 3.0, 3.0]])
    result = kd.matrix.trace(a)
    testing.assert_allclose(result, ds(6.0, schema_constants.FLOAT32))

  def test_identity(self):
    a = ds([[1.0, 0.0], [0.0, 1.0]])
    result = kd.matrix.trace(a)
    testing.assert_allclose(result, ds(2.0, schema_constants.FLOAT32))

  def test_int32_input_produces_int32_output(self):
    a = ds([[1, 2], [3, 4]])  # INT32
    result = kd.matrix.trace(a)
    testing.assert_equal(result, ds(5, schema_constants.INT32))

  def test_int64_input(self):
    a = ds([[1, 2], [3, 4]], schema_constants.INT64)
    result = kd.matrix.trace(a)
    testing.assert_equal(result, ds(5, schema_constants.INT64))

  def test_nonsquare(self):
    # trace of a 2x3 matrix: sum of diagonal = a[0,0] + a[1,1].
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    result = kd.matrix.trace(a)
    testing.assert_allclose(result, ds(6.0, schema_constants.FLOAT32))

  def test_sparse_data(self):
    # None values are treated as 0 in matrix operations.
    a = ds([[1.0, None, None], [None, 2.0, None], [None, 5.0, None]])
    result = kd.matrix.trace(a)
    testing.assert_allclose(result, ds(3.0, schema_constants.FLOAT32))

  def test_batched_sparse(self):
    # (2, 2, 2) with None on diagonal. trace sums present diagonal elements.
    a = ds([
        [[None, 2.0], [3.0, 4.0]],  # diag = [None, 4] -> sum = 4
        [[5.0, None], [None, None]],  # diag = [5, None] -> sum = 5
    ])
    result = kd.matrix.trace(a)
    testing.assert_allclose(result, ds([4.0, 5.0], schema_constants.FLOAT32))

  def test_object_schema_float(self):
    # OBJECT schema inputs should produce OBJECT schema output.
    a = kd.obj(ds([[1.0, 2.0], [3.0, 4.0]]))
    result = kd.matrix.trace(a)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        schema_constants.FLOAT32,
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.FLOAT32),
        ds(5.0),
    )

  def test_object_schema_integer(self):
    a = kd.obj(ds([[1, 2], [3, 4]]))
    result = kd.matrix.trace(a)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(
        result.get_obj_schema(),
        schema_constants.INT32,
    )
    testing.assert_allclose(
        kd.cast_to(result, schema_constants.INT32),
        ds(5),
    )

  def test_batched_3d(self):
    # (2, 2, 2) -> (2,). Two traces.
    a = ds([
        [[1.0, 2.0], [3.0, 4.0]],
        [[5.0, 6.0], [7.0, 8.0]],
    ])
    result = kd.matrix.trace(a)
    testing.assert_allclose(result, ds([5.0, 13.0], schema_constants.FLOAT32))

  def test_batched_4d(self):
    # (2, 2, 2, 2) -> (2, 2). Two levels of batch.
    a = ds([
        [[[1.0, 0.0], [0.0, 2.0]], [[3.0, 0.0], [0.0, 4.0]]],
        [[[5.0, 0.0], [0.0, 6.0]], [[7.0, 0.0], [0.0, 8.0]]],
    ])
    result = kd.matrix.trace(a)
    testing.assert_allclose(
        result, ds([[3.0, 7.0], [11.0, 15.0]], schema_constants.FLOAT32)
    )

  def test_int32_trace_overflow_clamps_to_max(self):
    # Diagonal = [2000000000, 2000000000], trace = 4e9 > INT32_MAX.
    # The int64 accumulator computes the correct sum, then saturate_cast clamps
    # the result to INT32_MAX.
    a = ds(
        [[2000000000, 0], [0, 2000000000]],
        schema_constants.INT32,
    )
    result = kd.matrix.trace(a)
    testing.assert_equal(result, ds(2147483647, schema_constants.INT32))

  def test_int32_trace_overflow_clamps_to_min(self):
    # Diagonal = [-2000000000, -2000000000], trace = -4e9 < INT32_MIN.
    a = ds(
        [[-2000000000, 0], [0, -2000000000]],
        schema_constants.INT32,
    )
    result = kd.matrix.trace(a)
    testing.assert_equal(result, ds(-2147483648, schema_constants.INT32))

  def test_int32_trace_overflow_batched(self):
    # Batch of 3: first overflows to INT32_MAX, second overflows to INT32_MIN,
    # third is normal.
    a = ds(
        [
            [[2000000000, 0], [0, 2000000000]],
            [[-2000000000, 0], [0, -2000000000]],
            [[1, 0], [0, 2]],
        ],
        schema_constants.INT32,
    )
    result = kd.matrix.trace(a)
    testing.assert_equal(
        result,
        ds([2147483647, -2147483648, 3], schema_constants.INT32),
    )

  def test_float32_trace_overflow_to_inf(self):
    # Diagonal = [2e38, 2e38], trace = 4e38 > FLOAT32_MAX -> +inf.
    a = ds([[2e38, 0.0], [0.0, 2e38]], schema_constants.FLOAT32)
    result = kd.matrix.trace(a)
    testing.assert_equal(result, ds(float('inf'), schema_constants.FLOAT32))

  def test_float32_trace_overflow_to_neg_inf(self):
    # Diagonal = [-2e38, -2e38], trace = -4e38 -> -inf.
    a = ds([[-2e38, 0.0], [0.0, -2e38]], schema_constants.FLOAT32)
    result = kd.matrix.trace(a)
    testing.assert_equal(result, ds(float('-inf'), schema_constants.FLOAT32))

  def test_float32_trace_overflow_batched(self):
    # Batch of 3: first overflows to +inf, second overflows to -inf, third is
    # normal.
    a = ds(
        [
            [[2e38, 0.0], [0.0, 2e38]],
            [[-2e38, 0.0], [0.0, -2e38]],
            [[1.0, 0.0], [0.0, 2.0]],
        ],
        schema_constants.FLOAT32,
    )
    result = kd.matrix.trace(a)
    testing.assert_equal(
        result,
        ds([float('inf'), float('-inf'), 3.0], schema_constants.FLOAT32),
    )

  def test_offset_positive(self):
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])  # 3x3 matrix

    # offset=1 super-diagonal: [2.0, 6.0] -> sum = 8.0.
    result = kd.matrix.trace(a, offset=1)
    testing.assert_allclose(result, ds(8.0, schema_constants.FLOAT32))

    # offset=2 super-diagonal: [3.0] -> sum = 3.0.
    result = kd.matrix.trace(a, offset=2)
    testing.assert_allclose(result, ds(3.0, schema_constants.FLOAT32))

    # offset=3 super-diagonal: [] -> sum = 0.0.
    result = kd.matrix.trace(a, offset=3)
    testing.assert_allclose(result, ds(0.0, schema_constants.FLOAT32))

  def test_offset_negative(self):
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])  # 3x3 matrix

    # offset=-1 sub-diagonal: [4.0, 8.0] -> sum = 12.0.
    result = kd.matrix.trace(a, offset=-1)
    testing.assert_allclose(result, ds(12.0, schema_constants.FLOAT32))

    # offset=-2 sub-diagonal: [7.0] -> sum = 7.0.
    result = kd.matrix.trace(a, offset=-2)
    testing.assert_allclose(result, ds(7.0, schema_constants.FLOAT32))

    # offset=-10 sub-diagonal: [] -> sum = 0.0.
    result = kd.matrix.trace(a, offset=-10)
    testing.assert_allclose(result, ds(0.0, schema_constants.FLOAT32))

  def test_offset_zero_is_default(self):
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
    result_default = kd.matrix.trace(a)
    result_offset0 = kd.matrix.trace(a, offset=0)
    testing.assert_allclose(result_default, result_offset0)

  def test_offset_nonsquare_more_columns(self):
    # 2x4 matrix:
    # [[1, 2, 3, 4],
    #  [5, 6, 7, 8]]
    a = ds([[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]])

    # offset=2: elements at (0,2)=3.0, (1,3)=8.0 -> sum = 11.0.
    result = kd.matrix.trace(a, offset=2)
    testing.assert_allclose(result, ds(11.0, schema_constants.FLOAT32))

    # offset=3: element at (0,3)=4.0 -> sum = 4.0.
    result = kd.matrix.trace(a, offset=3)
    testing.assert_allclose(result, ds(4.0, schema_constants.FLOAT32))

    # offset=4: no elements -> sum = 0.0.
    result = kd.matrix.trace(a, offset=4)
    testing.assert_allclose(result, ds(0.0, schema_constants.FLOAT32))

    # offset=-1: element at (1,0)=5.0 -> sum = 5.0.
    result = kd.matrix.trace(a, offset=-1)
    testing.assert_allclose(result, ds(5.0, schema_constants.FLOAT32))

    # offset=-2: no elements -> sum = 0.0.
    result = kd.matrix.trace(a, offset=-2)
    testing.assert_allclose(result, ds(0.0, schema_constants.FLOAT32))

  def test_offset_nonsquare_more_rows(self):
    # 4x2 matrix:
    # [[1, 2],
    #  [3, 4],
    #  [5, 6],
    #  [7, 8]]
    a = ds([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0], [7.0, 8.0]])

    # offset=1: element at (0,1)=2.0 -> sum = 2.0.
    result = kd.matrix.trace(a, offset=1)
    testing.assert_allclose(result, ds(2.0, schema_constants.FLOAT32))

    # offset=2: no elements -> sum = 0.0.
    result = kd.matrix.trace(a, offset=2)
    testing.assert_allclose(result, ds(0.0, schema_constants.FLOAT32))

    # offset=-1: elements at (1,0)=3.0, (2,1)=6.0 -> sum = 9.0.
    result = kd.matrix.trace(a, offset=-1)
    testing.assert_allclose(result, ds(9.0, schema_constants.FLOAT32))

    # offset=-3: element at (3,0)=7.0 -> sum = 7.0.
    result = kd.matrix.trace(a, offset=-3)
    testing.assert_allclose(result, ds(7.0, schema_constants.FLOAT32))

    # offset=-4: no elements -> sum = 0.0.
    result = kd.matrix.trace(a, offset=-4)
    testing.assert_allclose(result, ds(0.0, schema_constants.FLOAT32))

  def test_offset_out_of_bounds_returns_zero(self):
    a = ds([[1.0, 2.0], [3.0, 4.0]])
    # 2x2 matrix: offset >= 2 or offset <= -2 does not exist -> returns 0.
    result = kd.matrix.trace(a, offset=2)
    testing.assert_equal(result, ds(0.0, schema_constants.FLOAT32))

    result = kd.matrix.trace(a, offset=5)
    testing.assert_equal(result, ds(0.0, schema_constants.FLOAT32))

    result = kd.matrix.trace(a, offset=-2)
    testing.assert_equal(result, ds(0.0, schema_constants.FLOAT32))

    result = kd.matrix.trace(a, offset=-5)
    testing.assert_equal(result, ds(0.0, schema_constants.FLOAT32))

  def test_offset_out_of_bounds_schema_preservation(self):
    # FLOAT64:
    a_f64 = ds([[1.0, 2.0], [3.0, 4.0]], schema_constants.FLOAT64)
    result = kd.matrix.trace(a_f64, offset=5)
    testing.assert_equal(result, ds(0.0, schema_constants.FLOAT64))

    # INT32:
    a_i32 = ds([[1, 2], [3, 4]], schema_constants.INT32)
    result = kd.matrix.trace(a_i32, offset=5)
    testing.assert_equal(result, ds(0, schema_constants.INT32))

    result = kd.matrix.trace(a_i32, offset=-5)
    testing.assert_equal(result, ds(0, schema_constants.INT32))

    # INT64:
    a_i64 = ds([[1, 2], [3, 4]], schema_constants.INT64)
    result = kd.matrix.trace(a_i64, offset=5)
    testing.assert_equal(result, ds(0, schema_constants.INT64))

    # OBJECT:
    a_obj = kd.obj(a_i32)
    result = kd.matrix.trace(a_obj, offset=5)
    self.assertEqual(result.get_schema(), schema_constants.OBJECT)
    testing.assert_equivalent(result.get_obj_schema(), schema_constants.INT32)
    testing.assert_equal(kd.cast_to(result, schema_constants.INT32), ds(0))

  def test_trace_sparse_matrix_with_offset(self):
    a = ds([[1.0, None, 3.0], [4.0, 5.0, None], [7.0, 8.0, 9.0]])
    # offset=1 diagonal is [None, None] -> sum = 0.0
    result = kd.matrix.trace(a, offset=1)
    testing.assert_allclose(result, ds(0.0, schema_constants.FLOAT32))

    # offset=-1 diagonal is [4.0, 8.0] -> sum = 12.0
    result = kd.matrix.trace(a, offset=-1)
    testing.assert_allclose(result, ds(12.0, schema_constants.FLOAT32))

  def test_batched_3d_scalar_offset_positive(self):
    a = ds([
        [[1.0, 2.0], [3.0, 4.0]],
        [[5.0, 6.0], [7.0, 8.0]],
    ])  # (2, 2, 2)
    # offset=1: super-diagonal elements are [2.0] and [6.0]
    result = kd.matrix.trace(a, offset=1)
    testing.assert_allclose(result, ds([2.0, 6.0], schema_constants.FLOAT32))

  def test_batched_3d_scalar_offset_negative(self):
    a = ds([
        [[1.0, 2.0], [3.0, 4.0]],
        [[5.0, 6.0], [7.0, 8.0]],
    ])  # (2, 2, 2)
    # offset=-1: sub-diagonal elements are [3.0] and [7.0]
    result = kd.matrix.trace(a, offset=-1)
    testing.assert_allclose(result, ds([3.0, 7.0], schema_constants.FLOAT32))

  def test_batched_4d_scalar_offset(self):
    a = ds([
        [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]],
        [[[9.0, 10.0], [11.0, 12.0]], [[13.0, 14.0], [15.0, 16.0]]],
    ])  # (2, 2, 2, 2)
    result = kd.matrix.trace(a, offset=1)
    testing.assert_allclose(
        result, ds([[2.0, 6.0], [10.0, 14.0]], schema_constants.FLOAT32)
    )
    result_neg = kd.matrix.trace(a, offset=-1)
    testing.assert_allclose(
        result_neg, ds([[3.0, 7.0], [11.0, 15.0]], schema_constants.FLOAT32)
    )

  def test_batched_offset_per_batch_element(self):
    a = ds([
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
        [[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]],
    ])  # (2, 3, 3)
    result = kd.matrix.trace(a, offset=ds([0, 1]))
    # offset=0 from first: 1.0 + 5.0 + 9.0 = 15.0
    # offset=1 from second: 20.0 + 60.0 = 80.0
    testing.assert_allclose(result, ds([15.0, 80.0], schema_constants.FLOAT32))

  def test_batched_offset_mixed_positive_negative(self):
    a = ds([
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
        [[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]],
        [[100.0, 200.0], [300.0, 400.0]],
    ])
    result = kd.matrix.trace(a, offset=ds([1, 0, -1]))
    # offset=1: 2.0 + 6.0 = 8.0
    # offset=0: 10.0 + 50.0 + 90.0 = 150.0
    # offset=-1: 300.0
    testing.assert_allclose(
        result, ds([8.0, 150.0, 300.0], schema_constants.FLOAT32)
    )

  def test_batched_offset_mixed_positive_negative_missing(self):
    a = ds([
        [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
        [[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]],
        [[100.0, 200.0], [300.0, 400.0]],
    ])
    result = kd.matrix.trace(a, offset=ds([1, None, -1]))
    # offset=1: 2.0 + 6.0 = 8.0
    # offset=None means offset=0: 10.0 + 50.0 + 90.0 = 150.0
    # offset=-1: 300.0
    testing.assert_allclose(
        result, ds([8.0, 150.0, 300.0], schema_constants.FLOAT32)
    )

  def test_batched_offset_scalar_broadcast_matches_vector(self):
    a = ds([
        [[1.0, 2.0], [3.0, 4.0]],
        [[5.0, 6.0], [7.0, 8.0]],
    ])
    result_scalar = kd.matrix.trace(a, offset=1)
    result_broadcast = kd.matrix.trace(a, offset=ds([1, 1]))
    testing.assert_allclose(result_scalar, result_broadcast)

  def test_batched_offset_nonexistent_for_some_matrices(self):
    a = ds([
        [[1.0, 2.0], [3.0, 4.0]],
        [[5.0, 6.0], [7.0, 8.0]],
    ])
    result = kd.matrix.trace(a, offset=ds([1, 5]))
    # First matrix has offset=1 -> 2.0
    # Second matrix has offset=5 -> 0.0 (does not exist)
    testing.assert_allclose(result, ds([2.0, 0.0], schema_constants.FLOAT32))

    result_neg = kd.matrix.trace(a, offset=ds([-5, -1]))
    # First matrix has offset=-5 -> 0.0 (does not exist)
    # Second matrix has offset=-1 -> 7.0
    testing.assert_allclose(
        result_neg, ds([0.0, 7.0], schema_constants.FLOAT32)
    )

  def test_batched_offset_nonexistent_for_all(self):
    a = ds([
        [[1.0, 2.0], [3.0, 4.0]],
        [[5.0, 6.0], [7.0, 8.0]],
    ])
    result = kd.matrix.trace(a, offset=10)
    testing.assert_allclose(result, ds([0.0, 0.0], schema_constants.FLOAT32))

  def test_batched_jagged_matrix_dims_with_offset(self):
    a = ds([[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], [[7.0, 8.0]]])
    # Matrix 0 is 2x3, matrix 1 is 1x2.

    # offset=2:
    # matrix 0 has element at (0, 2) = 3.0 -> sum = 3.0.
    # matrix 1 has 2 cols, so offset=2 >= n, doesn't exist -> 0.0.
    result = kd.matrix.trace(a, offset=2)
    testing.assert_allclose(result, ds([3.0, 0.0], schema_constants.FLOAT32))

    # offset=0:
    result = kd.matrix.trace(a, offset=0)
    testing.assert_allclose(result, ds([6.0, 7.0], schema_constants.FLOAT32))

    # offset=-1:
    # matrix 0 has element at (1, 0) = 4.0 -> sum = 4.0.
    # matrix 1 has 1 row, so offset=-1: -k=1 >= m, doesn't exist -> 0.0.
    result_neg = kd.matrix.trace(a, offset=-1)
    testing.assert_allclose(
        result_neg, ds([4.0, 0.0], schema_constants.FLOAT32)
    )

  def test_batched_offset_with_sparse_data(self):
    a = ds([
        [[1.0, None, 3.0], [4.0, 5.0, 6.0]],
        [[None, 2.0, None], [None, None, 7.0]],
    ])
    result = kd.matrix.trace(a, offset=1)
    # Matrix 0: offset=1 elements are (0,1)=None, (1,2)=6.0 -> 6.0
    # Matrix 1: offset=1 elements are (0,1)=2.0, (1,2)=7.0 -> 9.0
    testing.assert_allclose(result, ds([6.0, 9.0], schema_constants.FLOAT32))

  def test_jagged_matrix_dims_integer(self):
    a = ds([[[1, 2], [3, 4]], [[6, 7, 8]]])
    result = kd.matrix.trace(a)
    # Matrix 0 (2x2): diag = [1, 4] -> 5
    # Matrix 1 (1x3): diag = [6] -> 6
    testing.assert_equal(result, ds([5, 6], schema_constants.INT32))

  def test_none_schema(self):
    # All-None 2x2 matrix: trace is 0 (missing values treated as 0).
    a = ds([[None, None], [None, None]])
    result = kd.matrix.trace(a)
    testing.assert_equal(result, ds(0, schema_constants.INT32))

  def test_jagged_batch_dims(self):
    # Jagged batch: first group has 2 matrices, second has 1 matrix.
    a = ds([
        [[[1.0, 0.0], [0.0, 2.0]], [[3.0, 0.0], [0.0, 4.0]]],
        [[[5.0, 0.0], [0.0, 6.0]]],
    ])
    result = kd.matrix.trace(a)
    testing.assert_allclose(
        result, ds([[3.0, 7.0], [11.0]], schema_constants.FLOAT32)
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.matrix.trace,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.matrix.trace(I.x)))
    self.assertTrue(view.has_koda_view(kde.matrix.trace(I.x, offset=I.offset)))


class NumpyComparisonTest(parameterized.TestCase):
  """NumPy cross-validation to verify the conceptual equivalence."""

  def test_trace_vs_numpy(self):
    a_np = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
    expected = float(np.trace(a_np))
    result = kd.matrix.trace(ds(a_np.tolist()))
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_trace_nonsquare_vs_numpy(self):
    a_np = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    expected = float(np.trace(a_np))
    result = kd.matrix.trace(ds(a_np.tolist()))
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_trace_batched_vs_numpy(self):
    rng = np.random.default_rng(202)
    a_np = rng.standard_normal((4, 5, 5))
    result = kd.matrix.trace(ds(a_np.tolist()))
    expected = [float(np.trace(a_np[i])) for i in range(4)]
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  @parameterized.parameters(-3, -2, -1, 0, 1, 2, 3, 5, -5)
  def test_offset_vs_numpy(self, offset_val):
    a_np = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
    expected = float(np.trace(a_np, offset=offset_val))
    result = kd.matrix.trace(ds(a_np.tolist()), offset=offset_val)
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  @parameterized.parameters(-3, -2, -1, 0, 1, 2, 3, 5, -5)
  def test_offset_nonsquare_vs_numpy(self, offset_val):
    a_np = np.array([[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]])
    expected = float(np.trace(a_np, offset=offset_val))
    result = kd.matrix.trace(ds(a_np.tolist()), offset=offset_val)
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  @parameterized.parameters(-2, -1, 0, 1, 2, 6)
  def test_trace_batched_with_offset_vs_numpy(self, offset_val):
    rng = np.random.default_rng(202)
    a_np = rng.standard_normal((4, 5, 5))
    result = kd.matrix.trace(ds(a_np.tolist()), offset=offset_val)
    expected = [float(np.trace(a_np[i], offset=offset_val)) for i in range(4)]
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )

  def test_trace_batched_per_matrix_offset_vs_numpy(self):
    rng = np.random.default_rng(202)
    a_np = rng.standard_normal((4, 5, 5))
    offsets = [1, -2, 0, 3]
    result = kd.matrix.trace(ds(a_np.tolist()), offset=ds(offsets))
    expected = [float(np.trace(a_np[i], offset=offsets[i])) for i in range(4)]
    testing.assert_allclose(
        result, ds(expected, schema_constants.FLOAT32), atol=1e-5
    )


class ErrorTest(parameterized.TestCase):
  """Tests for error messages."""

  def test_trace_0d_fails(self):
    a = ds(1.0)
    with self.assertRaisesRegex(
        ValueError, r'trace.*expected at least 2D.*got 0D'
    ):
      kd.matrix.trace(a)

  def test_trace_1d_fails(self):
    a = ds([1.0, 2.0])
    with self.assertRaisesRegex(
        ValueError, r'trace.*expected at least 2D.*got 1D'
    ):
      kd.matrix.trace(a)

  def test_offset_float_fails(self):
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(
        ValueError, r'argument `offset` must be castable to INT64'
    ):
      kd.matrix.trace(x, offset=1.5)

  def test_offset_text_fails(self):
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(
        ValueError, r'argument `offset` must be castable to INT64'
    ):
      kd.matrix.trace(x, offset=ds('hello'))

  def test_offset_not_broadcastable_wrong_size_fails(self):
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(ValueError, r'cannot be expanded'):
      kd.matrix.trace(x, offset=ds([0, 1, -1]))

  def test_offset_higher_rank_than_batch_fails(self):
    x = ds([[1.0, 2.0], [3.0, 4.0]])  # (2, 2), batch shape is scalar
    with self.assertRaisesRegex(ValueError, r'cannot be expanded'):
      kd.matrix.trace(x, offset=ds([0, 1]))

  def test_offset_that_triggers_overflow_fails(self):
    x = ds([[1.0, 2.0], [3.0, 4.0]])
    with self.assertRaisesRegex(
        ValueError, r'absolute value of -9223372036854775808 causes overflow'
    ):
      kd.matrix.trace(x, offset=-(2**63))

  def test_non_uniform_rows_fails(self):
    # Jagged matrix: rows have different lengths.
    a = ds([[1.0, 2.0, 3.0], [4.0, 5.0]])
    with self.assertRaisesRegex(ValueError, r'non-uniform row sizes'):
      kd.matrix.trace(a)

  def test_string_schema_fails(self):
    a = ds([['a', 'b'], ['c', 'd']])
    with self.assertRaisesRegex(ValueError, r'unsupported narrowed schema'):
      kd.matrix.trace(a)

  def test_strings_with_object_schema_fail(self):
    a = kd.obj(ds([['a', 'b'], ['c', 'd']]))
    with self.assertRaisesRegex(ValueError, r'unsupported narrowed schema'):
      kd.matrix.trace(a)

  def test_offset_2d_for_1d_batch_fails(self):
    x = ds([
        [[1.0, 2.0], [3.0, 4.0]],
        [[5.0, 6.0], [7.0, 8.0]],
    ])  # (2, 2, 2), batch shape (2,)
    with self.assertRaisesRegex(ValueError, r'cannot be expanded'):
      kd.matrix.trace(x, offset=ds([[0, 1], [1, 0]]))


if __name__ == '__main__':
  absltest.main()
