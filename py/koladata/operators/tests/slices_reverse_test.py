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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class SlicesUniqueTest(parameterized.TestCase):

  @parameterized.parameters(
      # 1D DataSlice 'x'
      (
          ds([1, 2, 3]),
          ds([3, 2, 1]),
      ),
      (
          ds([1, 3, 2, 1, 2, 3]),
          ds([3, 2, 1, 2, 3, 1]),
      ),
      # Missing values
      (
          ds([1, 3, 2, 1, None, 3, 1, None]),
          ds([None, 1, 3, None, 1, 2, 3, 1]),
      ),
      # Mixed dtypes
      (
          ds(['Z', 2, b'Q', 'A', b'B', 1, 'A', 3]),
          ds([3, 'A', 1, b'B', 'A', b'Q', 2, 'Z']),
      ),
      # 2D DataSlice
      (
          ds([[1, 3, 2, 9, 0, 5, 4], [1, 3, 9]]),
          ds([[4, 5, 0, 9, 2, 3, 1], [9, 3, 1]]),
      ),
      # 2D DataSlice Missing
      (
          ds([[None] * 5, [1, 3, 9]]),
          ds([[None] * 5, [9, 3, 1]]),
      ),
      (
          ds([[None] * 5, [1, 3, 9], [None] * 7]),
          ds([[None] * 5, [9, 3, 1], [None] * 7]),
      ),
      (
          ds([[None] * 5, [None] * 9, [1, 3, 9], [None] * 7]),
          ds([[None] * 5, [None] * 9, [9, 3, 1], [None] * 7]),
      ),
  )
  def test_eval(self, x, expected):
    result = expr_eval.eval(kde.reverse(x))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      ds([None] * 3), ds([]), ds([[None] * 3, [None] * 5])
  )
  def test_eval_with_empty_or_unknown(self, x):
    testing.assert_equal(expr_eval.eval(kde.reverse(x)), x)

  @parameterized.parameters(
      1,
      ds(1),
  )
  def test_eval_scalar_input(self, inp):
    testing.assert_equal(expr_eval.eval(kde.reverse(inp)), ds(1))

  def test_eval_wrong_type(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE',
    ):
      expr_eval.eval(kde.reverse(arolla.dense_array(['a', 'b'])))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.reverse,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
            max_arity=3,
        ),
        [
            (DATA_SLICE, DATA_SLICE),
        ],
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.reverse(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.reverse, kde.reverse))


if __name__ == '__main__':
  absltest.main()
