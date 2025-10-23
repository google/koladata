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
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants


I = input_container.InputContainer('I')

kde = kde_operators.kde
kd = eager_op_utils.operators_container('kd')
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty_mutable

DATA_SLICE = qtypes.DATA_SLICE


QTYPES = [
    (arolla.make_tuple_qtype(DATA_SLICE), DATA_SLICE),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
    ),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
    ),
    (arolla.make_tuple_qtype(DATA_SLICE), DATA_SLICE, DATA_SLICE),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
        DATA_SLICE,
    ),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
        DATA_SLICE,
    ),
    # etc.
]


class SlicesGroupByTest(parameterized.TestCase):

  @parameterized.parameters(
      # 1D DataSlice 'x'
      (
          ds([1, 2, 3, 1, 2, 3, 1, 3]),
          ds([[1, 1, 1], [2, 2], [3, 3, 3]]),
      ),
      (
          ds([1, 3, 2, 1, 2, 3, 1, 3]),
          ds([[1, 1, 1], [3, 3, 3], [2, 2]]),
      ),
      # Missing values
      (
          ds([1, 3, 2, 1, None, 3, 1, None]),
          ds([[1, 1, 1], [3, 3], [2]]),
      ),
      # Mixed dtypes for 'x'
      (
          ds(['A', 3, b'B', 'A', b'B', 3, 'A', 3]),
          ds([['A'] * 3, [3] * 3, [b'B'] * 2]),
      ),
      # 2D DataSlice 'x'
      (
          ds([[1, 2, 1, 3, 1, 3], [1, 3, 1]]),
          ds([[[1, 1, 1], [2], [3, 3]], [[1, 1], [3]]]),
      ),
  )
  def test_eval_one_input(self, x, expected):
    result = kd.group_by(x)
    testing.assert_equal(result, expected)
    # passing the same agument many times should be equivalent to passing it
    # once.
    result_tuple = kd.group_by(x, x, x, x)
    testing.assert_equal(result_tuple, expected)

  @parameterized.parameters(
      # 1D DataSlice 'x'
      (
          ds([1, 3, 2, 1, 2, 3, 1, 3]),
          ds([[1, 1, 1], [2, 2], [3, 3, 3]]),
      ),
      # Missing values
      (
          ds([1, 3, 2, 1, None, 3, 1, None]),
          ds([[1, 1, 1], [2], [3, 3]]),
      ),
      # 2D DataSlice 'x'
      (
          ds([[1, 2, 1, 3, 1, 3], [1, 3, 1]]),
          ds([[[1, 1, 1], [2], [3, 3]], [[1, 1], [3]]]),
      ),
  )
  def test_eval_one_input_sorted(self, x, expected):
    result = kd.group_by(x, sort=True)
    testing.assert_equal(result, expected)
    # passing the same agument many times should be equivalent to passing it
    # once.
    result_tuple = kd.group_by(x, x, x, x, sort=True)
    testing.assert_equal(result_tuple, expected)

  @parameterized.parameters(
      # 1D DataSlice 'x' and 'y'
      (
          ds(list(range(1, 9))),
          ds([1, 2, 3, 1, 2, 3, 1, 3]),
          ds([9, 4, 0, 9, 4, 0, 9, 0]),
          ds([[1, 4, 7], [2, 5], [3, 6, 8]]),
      ),
      (
          ds(list(range(1, 9))),
          ds([1, 2, 3, 1, 2, 3, 1, 3]),
          ds([7, 4, 0, 9, 4, 0, 7, 0]),
          ds([[1, 7], [2, 5], [3, 6, 8], [4]]),
      ),
      # 2D DataSlice 'x' and 'y'
      (
          ds([[1, 2, 3, 4, 5, 6], [7, 8, 9]]),
          ds([[1, 2, 1, 3, 1, 3], [1, 3, 1]]),
          ds([[0, 7, 5, 5, 0, 5], [0, 0, 2]]),
          ds([[[1, 5], [2], [3], [4, 6]], [[7], [8], [9]]]),
      ),
      (
          ds([[1, 2, 3, 4, 5, 6], [7, 8, 9]]),
          ds([[1, 2, 1, 3, 1, 3], [1, 3, 1]]),
          ds([[0, 7, 5, 5, 0, 5], [None, None, None]]),
          ds([[[1, 5], [2], [3], [4, 6]], []]),
      ),
      (
          ds([[1, None, 3, 4, None, 6], [7, 8, 9]]),
          ds([[1, 2, 1, 3, 1, 3], [None, 3, None]]),
          ds([[0, 7, 5, 5, 0, 5], [1, None, None]]),
          ds([[[1, None], [None], [3], [4, 6]], []]),
      ),
      # 2D Mixed DataSlice 'x' and 'y'
      (
          ds([['A', 'B', 3, b'D', 5.0, 6], ['X', b'Y', -3]]),
          ds([[1, 'q', 1, b'3', 1, b'3'], [1, 3, 1]]),
          ds([[0, 7, b'5', b'5', 0, b'5'], [0, 0, 2]]),
          ds([[['A', 5.0], ['B'], [3], [b'D', 6]], [['X'], [b'Y'], [-3]]]),
      ),
  )
  def test_eval_two_inputs(self, x, k1, k2, expected):
    result = kd.group_by(x, k1, k2)
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      # 1D DataSlice 'x' and 'y'
      (
          ds(list(range(1, 9))),
          ds([1, 2, 3, 1, 2, 3, 1, 3]),
          ds([9, 4, 0, 9, 4, 0, 9, 0]),
          ds([[1, 4, 7], [2, 5], [3, 6, 8]]),
      ),
      (
          ds(list(range(1, 9))),
          ds([1, 2, 3, 1, 2, 3, 1, 3]),
          ds([7, 4, 0, 9, 4, 0, 7, 0]),
          ds([[1, 7], [4], [2, 5], [3, 6, 8]]),
      ),
      # 2D DataSlice 'x' and 'y'
      (
          ds([[1, 2, 3, 4, 5, 6], [7, 8, 9]]),
          ds([[1, 2, 1, 3, 1, 3], [1, 3, 1]]),
          ds([[0, 7, 5, 5, 0, 5], [0, 0, 2]]),
          ds([[[1, 5], [3], [2], [4, 6]], [[7], [9], [8]]]),
      ),
      (
          ds([[1, 2, 3, 4, 5, 6], [7, 8, 9]]),
          ds([[1, 2, 1, 3, 1, 3], [1, 3, 1]]),
          ds([[0, 7, 5, 5, 0, 5], [None, None, None]]),
          ds([[[1, 5], [3], [2], [4, 6]], []]),
      ),
      (
          ds([[1, None, 3, 4, None, 6], [7, 8, 9]]),
          ds([[1, 2, 1, 3, 1, 3], [None, 3, None]]),
          ds([[0, 7, 5, 5, 0, 5], [1, None, None]]),
          ds([[[1, None], [3], [None], [4, 6]], []]),
      ),
  )
  def test_eval_two_inputs_sorted(self, x, k1, k2, expected):
    result = kd.group_by(x, k1, k2, sort=True)
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      (ds([None] * 3), ds([], schema_constants.NONE).repeat(0)),
      (ds([]), ds([]).repeat(0)),
      (
          ds([[None] * 3, [None] * 5]),
          ds([[], []], schema_constants.NONE).repeat(0),
      ),
  )
  def test_eval_with_empty_or_unknown_single_arg(self, x, expected):
    testing.assert_equal(kd.group_by(x), expected)

  @parameterized.parameters(
      (
          ds([1, 2, 1], schema_constants.INT32),
          ds([None] * 3),
          ds([], schema_constants.INT32).repeat(0),
      ),
      (
          ds([[1, 2, 1, 2], [2, 3, 2]], schema_constants.FLOAT64),
          ds([[None] * 4, [None] * 3]),
          ds([[], []], schema_constants.FLOAT64).repeat(0),
      ),
  )
  def test_eval_with_empty_or_unknown_keys(self, x, y, expected):
    testing.assert_equal(kd.group_by(x, y), expected)

  def test_mixed_dtypes(self):
    x = ds(['A', 3, b'B', 'A', b'B', 3, 'A', 3])
    with self.assertRaisesRegex(
        ValueError,
        'sort is not supported for mixed dtype',
    ):
      kd.group_by(x, sort=True)

  def test_non_sortable_dtype(self):
    db = bag()
    x = ds([db.obj(a=1), db.obj(a=2)])
    with self.assertRaisesRegex(
        ValueError,
        'sort is not supported for ITEMID',
    ):
      kd.group_by(x, sort=True)

  @parameterized.parameters(1, ds(1))
  def test_eval_scalar_input(self, inp):
    with self.assertRaisesRegex(
        ValueError,
        'group_by arguments must be DataSlices with ndim > 0, got DataItems',
    ):
      kd.group_by(inp)

  def test_eval_wrong_type(self):
    with self.assertRaisesRegex(
        ValueError,
        'all arguments to be DATA_SLICE',
    ):
      kd.group_by(ds([1, 2]), arolla.dense_array(['a', 'b']))
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE',
    ):
      kd.group_by(arolla.dense_array(['a', 'b']), ds([1, 2]))

  def test_eval_args_non_aligned(self):
    with self.assertRaisesRegex(
        ValueError,
        'kd.slices.group_by_indices: all arguments must have the same shape',
    ):
      kd.group_by(
          ds([[0, 7, 5, 5, 0, 5], [0, 0, 2]]),
          ds([[0, 7, 5, 5, 0, 5], [0, 0, 2]]),
          ds([[[1, 2, 1], [3, 1, 3]], [[1, 3], [1, 3]]]),
      )

  def test_eval_x_non_aligned_with_args(self):
    with self.assertRaisesRegex(
        ValueError,
        'First argument `x` must have the same shape as the other arguments',
    ):
      kd.group_by(
          ds([[[1, 2, 1], [3, 1, 3]], [[1, 3], [1, 3]]]),
          ds([[0, 7, 5, 5, 0, 5], [0, 0, 2]]),
          ds([[0, 7, 5, 5, 0, 5], [0, 0, 2]]),
      )

  def test_qtype_signatures(self):
    signature = arolla.abc.get_operator_signature(kde.slices.group_by)
    self.assertLen(signature.parameters, 3)
    self.assertEqual(signature.parameters[0].name, 'x')
    self.assertEqual(signature.parameters[1].name, 'args')
    self.assertEqual(signature.parameters[2].name, 'sort')

    arolla.testing.assert_qtype_signatures(
        kde.slices.concat,
        QTYPES,
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES
        + (
            arolla.make_tuple_qtype(),
            arolla.make_tuple_qtype(DATA_SLICE),
            arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
            arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.group_by(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.group_by, kde.group_by))


if __name__ == '__main__':
  absltest.main()
