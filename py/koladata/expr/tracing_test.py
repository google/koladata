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
    testing.assert_equal(e, I.x + I.y)

  def test_ops(self):
    e = tracing.trace(lambda x: kd.sum(x))  # pylint: disable=unnecessary-lambda
    testing.assert_equal(e, kde.sum(I.x))

  def test_ops_in_namespace(self):
    e = tracing.trace(lambda x: kd.math.abs(x))  # pylint: disable=unnecessary-lambda
    testing.assert_equal(e, kde.math.abs(I.x))

  def test_keyword_only(self):
    e = tracing.trace(lambda x, *, y: x + y)
    testing.assert_equal(e, I.x + I.y)

  def test_kd_constants(self):
    e = tracing.trace(lambda: kd.OBJECT)
    self.assertIsInstance(e, arolla.Expr)
    self.assertEqual(e.eval(), kd.OBJECT)

  def test_fstr(self):
    e = tracing.trace(lambda x: kd.fstr(f'{x:s}'))  # pylint: disable=unnecessary-lambda
    testing.assert_equal(e, kde.fstr(f'{I.x:s}'))

  # TODO: reenable this when we have kde.slice.
  # def test_kd_constants_in_slice(self):
  #   e = tracing.trace(lambda: kd.slice([1, 2], kd.FLOAT32))
  #   self.assertIsInstance(e, arolla.Expr)
  #   self.assertEqual(e.eval().get_schema(), kd.FLOAT32)

  # TODO: reenable this when we have kde.obj.
  # def test_obj_from_dict_with_itemid(self):
  #   o = kd.new()
  #   e = tracing.trace(lambda: kd.obj({1: 2}, itemid=kd.get_itemid(o)))
  #   self.assertIsInstance(e, arolla.Expr)
  #   self.assertEqual(e.eval(x=o)[1], 2)
  #   self.assertEqual(e.eval(x=o).get_schema(), kd.OBJECT)
  #   self.assertEqual(e.eval(x=o), o)

  def test_defaults_ignored(self):
    e = tracing.trace(lambda x=1, *, y=2: x + y)
    testing.assert_equal(e, I.x + I.y)

  def test_positional_only(self):
    e = tracing.trace(lambda x, /, y: x + y)
    testing.assert_equal(e, I.x + I.y)

  def test_other_types_not_allowed(self):
    with self.assertRaisesRegex(
        ValueError, 'please start the parameter name with "unused"'
    ):
      tracing.trace(lambda x, *y: x)
    with self.assertRaisesRegex(
        ValueError, 'please start the parameter name with "unused"'
    ):
      tracing.trace(lambda x, **y: x)

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
    with self.assertRaisesRegex(ValueError, 'Failed to trace the function'):
      tracing.trace(lambda x: 1 if x else 2)

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

  # TODO: reenable this when we have kde.attrs and kde.enrich.
  # def test_databag_methods(self):
  #   x = kd.obj(z=1)

  #   def get_update(a):
  #     upd = kd.bag()
  #     upd |= kd.attrs(a, y=2)
  #     return upd

  #   self.assertEqual(x.updated(get_update(x)).y, 2)
  #   self.assertEqual(x.updated(tracing.trace(get_update).eval(a=x)).y, 2)

  #   def apply_update(a):
  #     upd = kd.bag()
  #     upd |= kd.attrs(a, y=2)
  #     return upd.enrich(a)

  #   self.assertEqual(apply_update(x).y, 2)
  #   self.assertEqual(tracing.trace(apply_update).eval(a=x).y, 2)

  # TODO: reenable this when we have kde.strings.fstr.
  # def test_fstr(self):

  #   def format_greeting(x):
  #     return kd.strings.fstr('{o.greeting} {kd.strings.upper(o.name)}', o=x)

  #   self.assertEqual(
  #       format_greeting(x=kd.obj(greeting='Hello', name='World')).to_py(),
  #       'Hello WORLD',
  #   )
  #   self.assertEqual(
  #       tracing.trace(format_greeting)
  #       .eval(x=kd.obj(greeting='Hello', name='World'))
  #       .to_py(),
  #       'Hello WORLD',
  #   )


if __name__ == '__main__':
  absltest.main()
