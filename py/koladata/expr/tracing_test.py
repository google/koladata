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

import functools
import re

from absl.testing import absltest
from arolla import arolla
from koladata import kd
from koladata.expr import tracing
from koladata.testing import testing


kde = kd.lazy
kdf = kd.functor
I = kd.I


class TracingTest(absltest.TestCase):

  def test_simple(self):
    e = tracing.trace(lambda x, y: x + y)
    testing.assert_equal(e.op, kde.annotation.source_location)
    testing.assert_equal(e.node_deps[0], I.x + I.y)

  def test_several_operators(self):
    e = tracing.trace(lambda x, y, z: x + y + z)
    testing.assert_equal(e.op, kde.annotation.source_location)
    testing.assert_equal(e.node_deps[0], e.node_deps[0].node_deps[0] + I.z)
    testing.assert_equal(
        e.node_deps[0].node_deps[0].op, kde.annotation.source_location
    )
    testing.assert_equal(e.node_deps[0].node_deps[0].node_deps[0], I.x + I.y)

  def test_explicit_lazy_operator_usage(self):
    e = tracing.trace(lambda x, y, z: kde.math.add(x + y, z))
    # No source location for explicit usage of kde.math.add.
    testing.assert_equal(e, e.node_deps[0] + I.z)
    # But the inner "+" got a source location.
    testing.assert_equal(e.node_deps[0].op, kde.annotation.source_location)
    testing.assert_equal(e.node_deps[0].node_deps[0], I.x + I.y)

  def test_ops(self):
    e = tracing.trace(lambda x: kd.sum(x))  # pylint: disable=unnecessary-lambda
    testing.assert_equal(e.op, kde.annotation.source_location)
    testing.assert_equal(e.node_deps[0], kde.sum(I.x))

  def test_ops_in_namespace(self):
    e = tracing.trace(lambda x: kd.math.abs(x))  # pylint: disable=unnecessary-lambda
    testing.assert_equal(e.op, kde.annotation.source_location)
    testing.assert_equal(e.node_deps[0], kde.math.abs(I.x))

  def test_keyword_only(self):
    e = tracing.trace(lambda x, *, y: x + y)
    testing.assert_equal(e.op, kde.annotation.source_location)
    testing.assert_equal(e.node_deps[0], I.x + I.y)

  def test_kd_constants(self):
    e = tracing.trace(lambda: kd.OBJECT)
    self.assertIsInstance(e, arolla.Expr)
    self.assertEqual(e.eval(), kd.OBJECT)

  def test_fstr(self):
    e = tracing.trace(lambda x: kd.fstr(f'{x:s}'))  # pylint: disable=unnecessary-lambda
    # TODO: b/425293814 - Why kd.fstr is not getting annotated?
    testing.assert_equal(e, kde.fstr(f'{I.x:s}'))

  def test_kd_constants_in_slice(self):
    e = tracing.trace(lambda: kd.slice([1, 2], kd.FLOAT32))
    self.assertIsInstance(e, arolla.Expr)
    self.assertEqual(e.eval().get_schema(), kd.FLOAT32)

  # TODO: Make this work.
  # def test_obj_from_dict_with_itemid(self):
  #   o = kd.new()
  #   e = tracing.trace(
  #       lambda: kd.obj(kd.dict({1: 2}, itemid=kd.get_itemid(o))))
  #   self.assertIsInstance(e, arolla.Expr)
  #   self.assertEqual(e.eval(x=o)[1], 2)
  #   self.assertEqual(e.eval(x=o).get_schema(), kd.OBJECT)
  #   self.assertEqual(e.eval(x=o), o)

  def test_defaults_ignored(self):
    e = tracing.trace(lambda x=1, *, y=2: x + y)
    testing.assert_equal(e.op, kde.annotation.source_location)
    testing.assert_equal(e.node_deps[0], I.x + I.y)

  def test_positional_only(self):
    e = tracing.trace(lambda x, /, y: x + y)
    testing.assert_equal(e.op, kde.annotation.source_location)
    testing.assert_equal(e.node_deps[0], I.x + I.y)

  def test_other_types_not_allowed(self):
    with self.assertRaisesRegex(
        ValueError, 'please start the parameter name with "unused"'
    ):
      tracing.trace(lambda x, *y: x)
    with self.assertRaisesRegex(
        ValueError, 'please start the parameter name with "unused"'
    ):
      tracing.trace(lambda x, **y: x)

  def test_eager_only_methods_not_allowed(self):
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            r'calling \.append\(\) on a DataSlice is not supported in'
            r' expr/tracing mode',
        ),
    ):
      tracing.trace(lambda x: x.append())

  def test_other_types_allowed_if_called_unused(self):
    testing.assert_equal(tracing.trace(lambda x, *unused: x), I.x)
    testing.assert_equal(tracing.trace(lambda x, *unused_y: x), I.x)
    testing.assert_equal(tracing.trace(lambda x, **unused: x), I.x)
    testing.assert_equal(tracing.trace(lambda x, **unused_y: x), I.x)

  def test_returns_non_expr(self):
    e = tracing.trace(lambda: 1)
    self.assertIsInstance(e, arolla.Expr)
    self.assertEqual(kd.eval(e), 1)

  def test_tracing_fail(self):

    # Adding a decorator, so that we can check that the error message does not
    # mention `functools._lru_cache_wrapper` instead of `my_fn`.
    @functools.lru_cache()
    def my_fn(x):
      return 1 if x else 2

    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "unhashable type: 'arolla.abc.expr.Expr'; please consider using"
        ' `arolla.quote(expr)`',
    ):
      tracing.trace(my_fn)

    with self.assertRaisesWithPredicateMatch(
        TypeError,
        arolla.testing.any_note_regex(
            'Error occurred during tracing of the'
            ' function.*TracingTest.test_tracing_fail.*my_fn.*'
        ),
    ):
      tracing.trace(my_fn)

  def test_names(self):
    fn = lambda x, y, z: kd.with_name(x + y, 'foo') + z
    self.assertEqual(fn(x=kd.item(1), y=kd.item(2), z=kd.item(3)), 6)
    e = tracing.trace(fn)
    self.assertEqual(e.eval(x=kd.item(1), y=kd.item(2), z=kd.item(3)), 6)
    self.assertIn('foo = ', str(e))
    self.assertEqual(
        kd.expr.sub_by_name(e, foo=kd.expr.as_expr(4)).eval(z=kd.item(3)), 7
    )

    fn = lambda x, y, z: (x + y).with_name('foo') + z
    self.assertEqual(fn(x=kd.item(1), y=kd.item(2), z=kd.item(3)), 6)
    e = tracing.trace(fn)
    self.assertEqual(e.eval(x=kd.item(1), y=kd.item(2), z=kd.item(3)), 6)
    self.assertIn('foo = ', str(e))
    self.assertEqual(
        kd.expr.sub_by_name(e, foo=kd.expr.as_expr(4)).eval(z=kd.item(3)), 7
    )

  def test_functor_call(self):
    fn = kdf.expr_fn(I.x + I.y)
    self.assertEqual(
        tracing.trace(lambda a, b: kd.call(fn, x=a, y=b)).eval(a=1, b=2), 3
    )
    self.assertEqual(tracing.trace(lambda a, b: fn(x=a, y=b)).eval(a=1, b=2), 3)

  def test_databag_methods(self):
    x = kd.obj(z=1)

    def get_update(a):
      upd = kd.bag()
      upd <<= kd.attrs(a, y=2)
      return upd

    self.assertEqual(x.updated(get_update(x)).y, 2)
    self.assertEqual(x.updated(tracing.trace(get_update).eval(a=x)).y, 2)

    def apply_update(a):
      upd = kd.bag()
      upd <<= kd.attrs(a, y=2)
      return a.enriched(upd)

    self.assertEqual(apply_update(x).y, 2)
    self.assertEqual(tracing.trace(apply_update).eval(a=x).y, 2)

  def test_fstr_eval(self):

    def format_greeting(x):
      return kd.fstr(f'{x.greeting:s} {kd.strings.upper(x.name):s}')

    self.assertEqual(
        format_greeting(x=kd.obj(greeting='Hello', name='World')).to_py(),
        'Hello WORLD',
    )
    self.assertEqual(
        tracing.trace(format_greeting)
        .eval(x=kd.obj(greeting='Hello', name='World'))
        .to_py(),
        'Hello WORLD',
    )

  def test_ignored_input(self):
    def fn(x, *, y):
      del y
      return x

    testing.assert_traced_exprs_equal(
        tracing.trace(fn).eval(x=kd.slice(1)), kd.slice(1)
    )

  def test_extra_input_in_expr_error(self):
    def fn(x, *, y):
      return x + y + I.z + I.a

    with self.assertRaisesWithLiteralMatch(
        ValueError,
        "unexpected inputs ['a', 'z'] found during tracing of function:"
        f' `{fn}`. Traced functions must not create new or capture external'
        ' inputs',
    ):
      tracing.trace(fn)

  def test_nested_captured_variable_error(self):
    def fn(x, y):
      def internal_fn(x):
        return x + y

      traced_internal_fn = tracing.trace(internal_fn)
      return traced_internal_fn(x)

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape("unexpected inputs ['y']")
        ),
    ):
      tracing.trace(fn)


if __name__ == '__main__':
  absltest.main()
