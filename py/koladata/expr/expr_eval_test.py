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
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import py_expr_eval_py_ext as py_expr_eval
from koladata.expr import view as _
from koladata.functions import functions as fns
from koladata.operators import kde_operators as _
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import ellipsis

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
S = I.self
ds = data_slice.DataSlice.from_vals


# Direct tests using the CPython API that cannot be tested through the normal
# Python API.
class PyExprEvalTest(absltest.TestCase):

  def test_no_inputs_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'accepts 1 positional-only argument but 0 were given'
    ):
      py_expr_eval.eval_expr()

  def test_given_kwarg(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'accepts 1 positional-only argument but 0 were given'
    ):
      py_expr_eval.eval_expr(expr=I.x)

  def test_too_many_positional_inputs_error(self):
    with self.assertRaises(TypeError):
      py_expr_eval.eval_expr(I.x, I.x)

  def test_non_expr_input(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'kd.eval() expects an expression, got expr: int'
    ):
      py_expr_eval.eval_expr(1)

  def test_non_qvalue_input(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'kd.eval() expects all inputs to be QValues, got: x=int'
    ):
      py_expr_eval.eval_expr(I.x, x=1)

  def test_missing_input(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('kd.eval() has missing inputs for: [I.x, I.z]')
    ):
      py_expr_eval.eval_expr(I.x + I.y + I.z, y=arolla.int32(1))

  def test_transformation_cache(self):
    # Sanity check that the transformation cache is working. Without, this test
    # would take a very long time.
    expr = I.x
    x = ds(1)
    for _ in range(10000):
      expr = arolla.M.annotation.qtype(expr, x.qtype)
    for _ in range(10000):
      py_expr_eval.eval_expr(expr, x=x)


class ExprEval(absltest.TestCase):

  def test_eval(self):
    x = ds([1, 2, 3])
    y = ds([0, -1, 3])
    expr = I.x + I.y
    testing.assert_equal(expr_eval.eval(expr, x=x, y=y), ds([1, 1, 6]))

  def test_eval_with_decayed_input_error(self):
    decayed_op = arolla.abc.decay_registered_operator('koda_internal.input')
    expr = decayed_op('I', 'x')
    testing.assert_equal(expr_eval.eval(expr, x=ds([1, 2, 3])), ds([1, 2, 3]))

  def test_has_placeholders_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the inputs to kd.eval() must be specified as I.x, but the provided'
            ' expression has placeholders: [x, y]'
        ),
    ):
      expr_eval.eval(arolla.P.x + arolla.P.y)

  def test_has_leaves_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the inputs to kd.eval() must be specified as I.x, but the provided'
            ' expression has leaves: [x, ...]'
        ),
    ):
      expr_eval.eval(arolla.L.x + arolla.L.y)

  def test_missing_input_error(self):
    expr = I.x + I.y
    with self.assertRaisesRegex(
        ValueError,
        re.escape('kd.eval() has missing inputs for: [I.y]'),
    ):
      expr_eval.eval(expr, x=ds([1, 2, 3]))

  def test_superfluous_inputs(self):
    testing.assert_equal(
        expr_eval.eval(I.x, x=ds([1, 2, 3]), y=ds([4, 5, 6])), ds([1, 2, 3])
    )

  def test_eval_with_py_input(self):
    testing.assert_equal(expr_eval.eval(1), ds(1))

  def test_eval_with_data_item(self):
    res = expr_eval.eval(I.x, x=3.14)
    self.assertIsInstance(res, data_item.DataItem)
    testing.assert_allclose(res, ds(3.14))

  def test_eval_with_slice(self):
    res = expr_eval.eval(I.x, x=slice(0, None, 2))
    testing.assert_equal(res, arolla.types.Slice(ds(0), None, ds(2)))

  def test_eval_with_ellipsis(self):
    res = expr_eval.eval(I.x, x=...)
    testing.assert_equal(res, ellipsis.ellipsis())

  def test_eval_with_tuple(self):
    res = expr_eval.eval(I.x, x=(0, None, 2))
    testing.assert_equal(res, arolla.tuple(ds(0), ds(None), ds(2)))

  def test_eval_with_arbitrary_qvalue(self):
    res = expr_eval.eval(I.x, x=arolla.tuple(1, 2, 3))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        res, arolla.tuple(1, 2, 3)
    )

  def test_list_input_not_allowed(self):
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list to a Koda operation is ambiguous'
    ):
      expr_eval.eval(I.x, x=[1, 2, 3])

  def test_pure_arolla_expr_not_allowed(self):
    with self.assertRaisesRegex(
        ValueError,
        'failed to construct a QValue from the provided input containing an'
        ' Expr',
    ):
      expr_eval.eval(I.x, x=I.x + I.y)

  def test_not_evaluable_py_type_error(self):
    # Should never be possible to eval.
    obj = object()
    with self.assertRaisesRegex(
        ValueError, 'object with unsupported type: object'
    ):
      expr_eval.eval(I.x, x=obj)

  def test_missing_variable_error(self):
    x = data_slice.DataSlice.from_vals([1, 2, 3])
    expr = V.x
    with self.assertRaisesRegex(
        ValueError,
        re.escape('kd.eval() has missing inputs for: [V.x]'),
    ):
      expr_eval.eval(expr, x=x)

  def test_input_inside_lambda_error(self):

    @arolla.optools.as_lambda_operator('foo.bar')
    def foo_bar():
      return I.x

    x = data_slice.DataSlice.from_vals([1, 2, 3])
    # Before lowering (which should not be done by kd.eval).
    with self.assertRaisesRegex(
        ValueError, re.escape('I.x cannot be evaluated')
    ):
      expr_eval.eval(foo_bar(), x=x)

    # After lowering (which should not be done by kd.eval).
    testing.assert_equal(
        expr_eval.eval(arolla.abc.to_lowest(foo_bar()), x=x), ds([1, 2, 3])
    )

  def test_self(self):
    testing.assert_equal(expr_eval.eval(I.self, 5), ds(5))
    testing.assert_equal(expr_eval.eval(S, 5), ds(5))
    testing.assert_equal(expr_eval.eval(S.foo, fns.new(foo=5)).no_bag(), ds(5))

  def test_self_positional_only(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'I.self must be passed as a positional argument to kd.eval()',
    ):
      _ = expr_eval.eval(I.self, self=5)

  def test_self_when_not_specified(self):
    res = expr_eval.eval(I.self)
    testing.assert_equal(res, expr_eval.UNSPECIFIED_SELF_INPUT)
    # We can improve this error message later if needed.
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                """the attribute 'foo' is missing on the schema.

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('foo')"""
            )
        ),
    ):
      expr_eval.eval(I.self.foo)

  def test_self_is_ok_when_just_forwarding(self):
    # This tests helps provide some motivation on why just "S" should work
    # even when no positional argument is passed to kd.eval(). This looks a bit
    # artificial, but will become more natural when we have kde.call (we have
    # no plans to have kde.eval).
    def simple_metric(score, label):
      return (score - label) * (score - label)

    def weighted_metric(score, label):
      return S.weight * simple_metric(score, label)

    def compute_delta(metric_func, score1, score2, label, *eval_args):
      self_input = expr_eval.eval(S, *eval_args)
      m1 = expr_eval.eval(metric_func(score1, label), self_input)
      m2 = expr_eval.eval(metric_func(score2, label), self_input)
      return m2 - m1

    self.assertEqual(compute_delta(simple_metric, 0, 1, 2), -3)
    self.assertEqual(
        compute_delta(weighted_metric, 0, 1, 2, fns.new(weight=2)), -6
    )

  def test_unspecified_default_value_str(self):
    # We can improve this later if needed, this is the best we could do without
    # custom repr code.
    self.assertEqual(
        str(expr_eval.UNSPECIFIED_SELF_INPUT),
        'Entity(self_not_specified=present)',
    )

  def test_cancellation(self):
    expr = arolla.M.core._identity_with_cancel(I.x, 'cancelled')
    x = ds([1, 2, 3])
    with self.assertRaisesRegex(ValueError, re.escape('cancelled')):
      expr_eval.eval(expr, x=x)

  def test_freeze_input_data_slice(self):
    db = data_bag.DataBag.empty()
    x = ds([1, 2, 3]).with_bag(db)
    result = expr_eval.eval(I.x, x=x)
    self.assertTrue(x.get_bag().is_mutable())
    self.assertFalse(result.get_bag().is_mutable())

    x = ds([1, 2, 3]).with_bag(db.freeze())
    result = expr_eval.eval(I.x, x=x)
    testing.assert_equal(x.get_bag(), result.get_bag())
    self.assertFalse(result.get_bag().is_mutable())

  def test_freeze_input_databag(self):
    db = data_bag.DataBag.empty()
    result = expr_eval.eval(I.x, x=db)
    self.assertTrue(db.is_mutable())
    self.assertFalse(result.is_mutable())

    frozen_db = db.freeze()
    result = expr_eval.eval(I.x, x=frozen_db)
    testing.assert_equal(frozen_db, result)
    self.assertFalse(result.is_mutable())


if __name__ == '__main__':
  absltest.main()
