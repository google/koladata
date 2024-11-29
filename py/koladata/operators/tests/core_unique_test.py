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
db = data_bag.DataBag.empty()
obj1 = db.obj(x=1)
obj2 = db.obj(x=2)
obj3 = db.obj(x=3)


class CoreUniqueTest(parameterized.TestCase):

  @parameterized.parameters(
      # 1D DataSlice 'x'
      (
          ds([1, 2, 3, 1, 2, 3, 1, 3]),
          ds([1, 2, 3]),
      ),
      (
          ds([1, 3, 2, 1, 2, 3, 1, 3]),
          ds([1, 3, 2]),
      ),
      # Missing values
      (
          ds([1, 3, 2, 1, None, 3, 1, None]),
          ds([1, 3, 2]),
      ),
      # Mixed dtypes for 'x'
      (
          ds(['A', 3, b'B', 'A', b'B', 3, 'A', 3]),
          ds(['A', 3, b'B']),
      ),
      # 2D DataSlice 'x'
      (
          ds([[1, 3, 2, 1, 3, 1, 3], [1, 3, 1]]),
          ds([[1, 3, 2], [1, 3]]),
      ),
      # 1D Object DataSlice
      (
          ds([obj1, obj2, obj3, obj1, obj2, obj3, obj1, obj3]),
          ds([obj1, obj2, obj3]),
      ),
  )
  def test_eval_one_input_unsort(self, x, expected):
    result = expr_eval.eval(kde.unique(x))
    testing.assert_equal(result, expected)
    result = expr_eval.eval(kde.unique(x, sort=False))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      # 1D DataSlice 'x'
      (
          ds([1, 2, 3, 1, 2, 3, 1, 3]),
          ds([1, 2, 3]),
      ),
      (
          ds([1, 3, 2, 1, 2, 3, 1, 3]),
          ds([1, 2, 3]),
      ),
      # Missing values
      (
          ds([1, 3, 2, 1, None, 3, 1, None]),
          ds([1, 2, 3]),
      ),
      # 2D DataSlice 'x'
      (
          ds([[1, 3, 2, 1, 3, 1, 3], [1, 3, 1]]),
          ds([[1, 2, 3], [1, 3]]),
      ),
  )
  def test_eval_one_input_sort(self, x, expected):
    result = expr_eval.eval(kde.unique(x, sort=True))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      ds(['A', 3, b'B', 'A', b'B', 3, 'A', 3]),
      ds([['A', 3, b'B', 'A'], [b'B', 3, 'A', 3]]),
  )
  def test_eval_one_input_sort_mixed_dtype(self, x):
    with self.assertRaisesRegex(
        ValueError,
        'sort is not supported for mixed dtype',
    ):
      expr_eval.eval(kde.unique(x, sort=True))

  @parameterized.parameters(
      ds([obj1]),
      ds([schema_constants.FLOAT64]),
      ds([arolla.quote(arolla.literal(1))]),
  )
  def test_eval_one_input_sort_unsupported_dtype(self, x):
    with self.assertRaisesRegex(
        ValueError,
        'sort is not supported',
    ):
      expr_eval.eval(kde.unique(x, sort=True))

  @parameterized.parameters(
      (ds([None] * 3), ds([], kd.NONE)),
      (ds([]), ds([])),
      (ds([[None] * 3, [None] * 5]), ds([[], []], kd.NONE)),
  )
  def test_eval_with_empty_or_unknown_single_arg(self, x, expected):
    testing.assert_equal(expr_eval.eval(kde.unique(x)), expected)
    testing.assert_equal(expr_eval.eval(kde.unique(x, sort=False)), expected)
    testing.assert_equal(expr_eval.eval(kde.unique(x, sort=True)), expected)

  @parameterized.parameters(
      (1, True),
      (1, False),
      (ds(1), True),
      (ds(1), False),
  )
  def test_eval_scalar_input(self, inp, sort):
    testing.assert_equal(expr_eval.eval(kde.unique(inp, sort=sort)), ds(1))

  def test_eval_wrong_type(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE',
    ):
      expr_eval.eval(kde.unique(arolla.dense_array(['a', 'b'])))

  @parameterized.parameters(
      ds(1),
      ds([]),
      ds([], schema_constants.INT64),
      ds([], schema_constants.BOOLEAN),
      ds([True]),
      ds([False]),
      ds([False, True]),
  )
  def test_eval_wrong_sort(self, sort):
    with self.assertRaisesRegex(
        ValueError,
        'argument `sort` must be an item holding boolean',
    ):
      expr_eval.eval(kde.unique(ds([1, 2, 3]), sort=sort))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.unique,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
            max_arity=3,
        ),
        [
            (DATA_SLICE, DATA_SLICE),  # no sort argument
            (DATA_SLICE, DATA_SLICE, DATA_SLICE),
        ],
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.unique(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.unique, kde.unique))


if __name__ == '__main__':
  absltest.main()
