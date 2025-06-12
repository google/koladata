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

import re
from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import schema_constants


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class PyMapPyOnCondTest(parameterized.TestCase):

  # Note: This operator is assembled from the same building blocks as
  # the operator `kde.py.map_py`, and these are tested together with that
  # operator.

  def test_same_dimension_cond(self):
    yes_fn = lambda x: x + 1 if x is not None else 0
    no_fn = lambda x: x - 1 if x is not None else -1
    val = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])

    res = expr_eval.eval(kde.py.map_py_on_cond(yes_fn, no_fn, val > 2, val))
    testing.assert_equal(
        res.no_bag(), ds([[0, 1, -1, 5], [-1, -1], [8, 9, 10]])
    )

    res = expr_eval.eval(
        kde.py.map_py_on_cond(
            yes_fn, no_fn, kde.masking.has_not(val) | (val > 2), val
        )
    )
    testing.assert_equal(res.no_bag(), ds([[0, 1, 0, 5], [0, 0], [8, 9, 10]]))

  def test_smaller_dimension_cond(self):
    yes_fn = lambda x: x + 1 if x is not None else 0
    no_fn = lambda x: x - 1 if x is not None else -1
    val = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    cond = ds(
        [mask_constants.present, mask_constants.missing, mask_constants.present]
    )

    res = expr_eval.eval(kde.py.map_py_on_cond(yes_fn, no_fn, cond, val))
    testing.assert_equal(res.no_bag(), ds([[2, 3, 0, 5], [-1, -1], [8, 9, 10]]))

  def test_no_false_fn(self):
    yes_fn = lambda x: x + 1 if x is not None else 0
    val = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    cond = ds(
        [mask_constants.present, mask_constants.missing, mask_constants.present]
    )

    res = expr_eval.eval(kde.py.map_py_on_cond(yes_fn, None, cond, x=val))
    testing.assert_equal(
        res.no_bag(), ds([[2, 3, 0, 5], [None, None], [8, 9, 10]])
    )

  def test_empty_input(self):
    yes_fn = lambda x: x + 1 if x is not None else 0

    val = ds([])
    cond = ds([], schema_constants.MASK)
    res = expr_eval.eval(
        kde.py.map_py_on_cond(
            yes_fn, None, cond, x=val, schema=schema_constants.FLOAT32
        )
    )
    testing.assert_equal(res, ds([], schema_constants.FLOAT32))
    self.assertIsNone(res.get_bag())

    res = expr_eval.eval(kde.py.map_py_on_cond(yes_fn, None, cond, x=val))
    testing.assert_equal(res.no_bag(), ds([]))
    self.assertIsNone(res.get_bag())

    res = expr_eval.eval(
        kde.py.map_py_on_cond(
            yes_fn, None, cond, x=val, schema=schema_constants.OBJECT
        )
    )
    testing.assert_equal(res.no_bag(), ds([], schema_constants.OBJECT))
    self.assertIsNotNone(res.get_bag())

    val = ds([1, 2, 3])
    cond = ds(
        [mask_constants.missing, mask_constants.missing, mask_constants.missing]
    )
    res = expr_eval.eval(
        kde.py.map_py_on_cond(
            yes_fn, None, cond, x=val, schema=schema_constants.FLOAT32
        )
    )
    testing.assert_equal(res, ds([None, None, None], schema_constants.FLOAT32))
    self.assertIsNone(res.get_bag())

    res = expr_eval.eval(kde.py.map_py_on_cond(yes_fn, None, cond, x=val))
    testing.assert_equal(res, ds([None, None, None]))
    self.assertIsNone(res.get_bag())

    res = expr_eval.eval(
        kde.py.map_py_on_cond(
            yes_fn, None, cond, x=val, schema=schema_constants.OBJECT
        )
    )
    testing.assert_equal(
        res.no_bag(), ds([None, None, None], schema_constants.OBJECT)
    )
    self.assertIsNotNone(res.get_bag())

  def test_error_non_mask_cond(self):
    fn = lambda _: None
    val = ds([1])
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('expected a mask, got cond: INT32')
        ),
    ):
      expr_eval.eval(kde.py.map_py_on_cond(fn, fn, val, val))

  def test_error_higher_dimension_cond(self):
    fn = lambda _: None
    val = ds([1])
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                "'cond' must have the same or smaller dimension than `args` and"
                ' `kwargs`'
            )
        ),
    ):
      expr_eval.eval(kde.py.map_py_on_cond(fn, fn, val.repeat(1) > 2, val))

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.py.map_py_on_cond(I.fn, I.cond, I.arg))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.py.map_py_on_cond, kde.map_py_on_cond)
    )

  def test_understandable_yes_fn_error(self):
    def fn1(x):
      return x + I.x

    def fn2(x):
      return x

    expr = kde.py.map_py_on_cond(fn1, fn2, mask_constants.present, ds(1))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'unsupported type, arolla.abc.expr.Expr, for value:\n\n '
            f' {ds(1) + I.x}'
        ),
    ):
      expr_eval.eval(expr)
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_note_regex(
            re.escape(
                'Error occurred during evaluation of kd.map_py_on_cond with'
                f' true_fn={fn1} and false_fn={fn2}'
            )
        ),
    ):
      expr_eval.eval(expr)

  def test_understandable_no_fn_error(self):
    def fn1(x):
      return x + I.x

    def fn2(x):
      return x

    expr = kde.py.map_py_on_cond(fn2, fn1, mask_constants.missing, ds(1))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'unsupported type, arolla.abc.expr.Expr, for value:\n\n '
            f' {ds(1) + I.x}'
        ),
    ):
      expr_eval.eval(expr)

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_note_regex(
            re.escape(
                'Error occurred during evaluation of kd.map_py_on_cond with'
                f' true_fn={fn2} and false_fn={fn1}'
            )
        ),
    ):
      expr_eval.eval(expr)


if __name__ == '__main__':
  absltest.main()
