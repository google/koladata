# Copyright 2024 Google LLC
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
from koladata import kd
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class CoreGroupByIndicesSortedTest(parameterized.TestCase):

  @parameterized.parameters(
      # 1D DataSlice 'x'
      (
          ds([1, 2, 3, 1, 2, 3, 1, 3]),
          ds([[0, 3, 6], [1, 4], [2, 5, 7]], schema_constants.INT64),
      ),
      (
          ds([1, 3, 2, 1, 2, 3, 1, 3]),
          ds([[0, 3, 6], [2, 4], [1, 5, 7]], schema_constants.INT64),
      ),
      # Missing values
      (
          ds([1, 3, 2, 1, None, 3, 1, None]),
          ds([[0, 3, 6], [2], [1, 5]], schema_constants.INT64),
      ),
      # 2D DataSlice 'x'
      (
          ds([[4, 2, 4, 3, 4, 3], [1, 3, 1]]),
          ds([[[1], [3, 5], [0, 2, 4]], [[0, 2], [1]]], schema_constants.INT64),
      ),
  )
  def test_eval_one_input(self, x, expected):
    result = expr_eval.eval(kde.group_by_indices_sorted(x))
    testing.assert_equal(result, expected)
    # passing the same agument many times should be equivalent to passing it
    # once.
    result_tuple = expr_eval.eval(kde.group_by_indices_sorted(x, x, x, x))
    testing.assert_equal(result_tuple, expected)

  @parameterized.parameters(
      # 1D DataSlice 'x' and 'y'
      (
          ds([1, 2, 3, 1, 2, 3, 1, 3]),
          ds([9, 4, 0, 9, 4, 0, 9, 0]),
          ds([[0, 3, 6], [1, 4], [2, 5, 7]], schema_constants.INT64),
      ),
      (
          ds([1, 2, 3, 1, 2, 3, 1, 3]),
          ds([7, 4, 0, 9, 4, 0, 7, 0]),
          ds([[0, 6], [3], [1, 4], [2, 5, 7]], schema_constants.INT64),
      ),
      # 2D DataSlice 'x' and 'y'
      (
          ds([[1, 2, 1, 3, 1, 3, 1], [1, 3, 1]]),
          ds([[0, 7, 5, 5, 0, 5, 5], [0, 0, 2]]),
          ds(
              [[[0, 4], [2, 6], [1], [3, 5]], [[0], [2], [1]]],
              schema_constants.INT64,
          ),
      ),
      (
          ds([[1, 2, 1, 3, 1, 3, 1], [1, 3, 1]]),
          ds([[0, 7, 5, 5, 0, 5, 5], [None, None, None]]),
          ds(
              [[[0, 4], [2, 6], [1], [3, 5]], []],
              schema_constants.INT64,
          ),
      ),
  )
  def test_eval_two_inputs(self, x, y, expected):
    result = expr_eval.eval(kde.group_by_indices_sorted(x, y))
    testing.assert_equal(result, expected)

  def test_eval_with_empty_or_unknown_input_flat(self):
    x = ds([1, 2, 1])
    y = ds([None] * 3)
    expected = kd.slice([], schema_constants.INT64).add_dim(0)
    testing.assert_equal(
        expr_eval.eval(kde.group_by_indices_sorted(x, y)), expected
    )

  def test_eval_with_empty_or_unknown_input_2d(self):
    x = ds([[1, 2, 1, 2], [2, 3, 2]])
    y = ds([[None] * 4, [None] * 3])
    expected = ds([[], []], schema_constants.INT64).add_dim(0)
    testing.assert_equal(
        expr_eval.eval(kde.group_by_indices_sorted(x, y)), expected
    )

  @parameterized.parameters(1, ds(1))
  def test_eval_scalar_input(self, inp):
    with self.assertRaisesRegex(
        ValueError,
        'group_by is not supported for scalar data',
    ):
      expr_eval.eval(kde.group_by_indices_sorted(inp))

  @parameterized.parameters(
      ds([1, 'a']),
      ds([b'a', 'a']),
      ds([data_bag.DataBag.empty().obj(x=1)]),
      ds([schema_constants.FLOAT64]),
      ds([arolla.quote(arolla.literal(1))]),
  )
  def test_eval_not_supported_input_for_sort(self, inp):
    with self.assertRaisesRegex(
        ValueError,
        'sort is not supported',
    ):
      expr_eval.eval(kde.group_by_indices_sorted(inp))

  def test_eval_empty_input(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected at least one argument',
    ):
      expr_eval.eval(kde.group_by_indices_sorted())

  def test_eval_wrong_type(self):
    with self.assertRaisesRegex(
        ValueError,
        'all arguments to be DATA_SLICE',
    ):
      expr_eval.eval(
          kde.group_by_indices_sorted(
              ds([1, 2]), arolla.dense_array(['a', 'b'])
          )
      )

  def test_eval_non_aligned(self):
    with self.assertRaisesRegex(
        ValueError,
        'same shape',
    ):
      expr_eval.eval(
          kde.group_by_indices_sorted(
              ds([[[1, 2, 1], [3, 1, 3]], [[1, 3], [1, 3]]]),
              ds([[0, 7, 5, 5, 0, 5], [0, 0, 2]]),
          )
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.group_by_indices_sorted,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
            max_arity=3,
        ),
        (
            (DATA_SLICE, DATA_SLICE),
            (DATA_SLICE, DATA_SLICE, DATA_SLICE),
            (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
        ),
    )

  def test_view(self):
    self.assertTrue(
        view.has_data_slice_view(kde.core.group_by_indices_sorted(I.x))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.core.group_by_indices_sorted, kde.group_by_indices_sorted
        )
    )


if __name__ == '__main__':
  absltest.main()
