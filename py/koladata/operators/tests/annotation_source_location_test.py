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

"""Tests for kd.lazy.annotation.with_name operator."""

import traceback

from absl.testing import absltest
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import py_expr_eval_py_ext
from koladata.operators import kde_operators
from koladata.operators import optools as kd_optools
from koladata.testing import testing as kd_testing
from koladata.types import data_item


I = input_container.InputContainer('I')
kd_lazy = kde_operators.kde
kd_item = data_item.DataItem.from_vals
eval_op = py_expr_eval_py_ext.eval_op


class AnnotationSourceLocationTest(absltest.TestCase):

  def test_boxing(self):
    expr = kd_lazy.annotation.source_location(
        1, 'foo', 'test.py', 123, 456, '  x + 1'
    )
    # We don't expect users to call the operator directly, so there is no point
    # in boxing the first argument specifically for Koda.
    arolla.testing.assert_expr_equal_by_fingerprint(
        expr.node_deps[0], arolla.literal(arolla.int32(1))
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        expr.node_deps[1], arolla.literal(arolla.text('foo'))
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        expr.node_deps[2], arolla.literal(arolla.text('test.py'))
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        expr.node_deps[3], arolla.literal(arolla.int32(123))
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        expr.node_deps[4], arolla.literal(arolla.int32(456))
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        expr.node_deps[5], arolla.literal(arolla.text('  x + 1'))
    )

  def test_noop_eval(self):
    expr = kd_lazy.annotation.source_location(
        I.x + 1, 'foo', 'test.py', 123, 456, '  x + 1'
    )
    kd_testing.assert_equal(expr_eval.eval(expr, x=1), kd_item(2))

  def test_stack_trace(self):

    @kd_optools.as_lambda_operator('inner_lambda')
    def inner_lambda(x, y):
      return kd_lazy.annotation.source_location(
          kd_lazy.math.floordiv(x, y),
          'inner_lambda',
          'file.py',
          57,
          2,
          'kd_lazy.math.floordiv(x, y)',
      )

    @kd_optools.as_lambda_operator('outer_lambda')
    def outer_lambda(x, y):
      inner = kd_lazy.annotation.source_location(
          inner_lambda(x, y),
          'outer_lambda',
          'file.py',
          58,
          2,
          'inner_lambda(x, y)',
      )
      return kd_lazy.annotation.source_location(
          kd_lazy.math.add(inner, 1),
          'outer_lambda',
          'file.py',
          59,
          2,
          'kd_lazy.math.add(inner, 1)',
      )

    expr = kd_lazy.annotation.source_location(
        outer_lambda(I.x, I.y),
        'main',
        'file.py',
        60,
        2,
        'outer_lambda(L.x, L.y)',
    )

    try:
      expr_eval.eval(expr, x=1, y=0)
    except ValueError as e:
      ex = e

    self.assertEqual(
        str(ex), 'outer_lambda: kd.math.floordiv: division by zero'
    )
    tb = '\n'.join(traceback.format_tb(ex.__traceback__))
    self.assertRegex(tb, 'file.py.*line 57.*inner_lambda')
    self.assertRegex(tb, 'file.py.*line 58.*outer_lambda')
    # file.py:59 annotation is an ancestor of the broken inner_lambda(x, y)
    # in the expression, but semantically does not belong to the stack trace and
    # so is not included.
    self.assertNotIn('line 59', tb)
    self.assertRegex(tb, 'file.py.*line 60.*main')

    # eval_op uses a different code path, so we test it separately.
    try:
      eval_op(outer_lambda, 1, 0)
    except ValueError as e:
      ex = e

    self.assertEqual(str(ex), 'kd.math.floordiv: division by zero')
    tb = '\n'.join(traceback.format_tb(ex.__traceback__))
    self.assertRegex(tb, 'file.py.*line 57.*inner_lambda')
    self.assertRegex(tb, 'file.py.*line 58.*outer_lambda')
    self.assertNotIn('line 59', tb)

  def test_repr(self):
    expr = kd_lazy.annotation.source_location(
        I.x + 1, 'foo', 'test.py', 123, 456, '  x + 1'
    )
    self.assertEqual(repr(expr), '(I.x + DataItem(1, schema: INT32))📍')

    expr = kd_lazy.annotation.source_location(
        I.x.attr, 'foo', 'test.py', 123, 456, '  x.attr'
    )
    self.assertEqual(repr(expr), 'I.x.attr📍')

    expr = kd_lazy.annotation.source_location(
        -I.x, 'foo', 'test.py', 123, 456, '  x.attr'
    )
    self.assertEqual(repr(expr), '(-I.x)📍')


if __name__ == '__main__':
  absltest.main()
