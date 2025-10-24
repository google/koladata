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
from arolla import arolla
from koladata import kd
from koladata.expr import introspection
from koladata.expr import tracing_mode
from koladata.functor import expr_container
from koladata.testing import testing


kde = kd.lazy
I = kd.I
V = kd.V
kdf = kd.functor


class ExprContainerTest(absltest.TestCase):

  def test_basics(self):
    c = expr_container.NamedContainer()
    c.x = I.x
    c.z = I.y
    c.sum = I.x + I.y

    self.maxDiff = None
    self.assertCountEqual(dir(c), ['x', 'z', 'sum'])
    self.assertEqual(introspection.get_name(c.x), 'x')
    arolla.testing.assert_expr_equal_by_fingerprint(c.x.node_deps[0], I.x)
    self.assertEqual(introspection.get_name(c.z), 'z')
    arolla.testing.assert_expr_equal_by_fingerprint(c.z.node_deps[0], I.y)
    self.assertEqual(introspection.get_name(c.sum), 'sum')
    arolla.testing.assert_expr_equal_by_fingerprint(
        c.sum.node_deps[0], I.x + I.y
    )

  def test_override_expr_name(self):
    c = expr_container.NamedContainer()
    c.y = I.x.with_name('x')

    self.assertCountEqual(dir(c), ['y'])
    self.assertEqual(introspection.get_name(c.y), 'y')
    arolla.testing.assert_expr_equal_by_fingerprint(
        c.y.node_deps[0], I.x.with_name('x')
    )

  def test_override_node_name(self):
    c = expr_container.NamedContainer()
    c.x = I.x
    c.x = I.y

    self.assertCountEqual(dir(c), ['x'])
    self.assertEqual(introspection.get_name(c.x), 'x')
    arolla.testing.assert_expr_equal_by_fingerprint(c.x.node_deps[0], I.y)

  def test_non_expr_value(self):
    c = expr_container.NamedContainer()
    c.x = 5
    self.assertIsInstance(c.x, int)
    self.assertEqual(c.x, 5)

  def test_non_expr_value_in_tracing_mode(self):
    c = expr_container.NamedContainer()
    with tracing_mode.enable_tracing():
      c.x = 5
    self.assertIsInstance(c.x, arolla.Expr)
    self.assertEqual(kd.eval(c.x), 5)

  def test_non_expr_value_tracing(self):
    def f(x):
      c = expr_container.NamedContainer()
      c.original = x
      c.updated = c.original + 2
      return c.updated

    fn = kdf.fn(f)
    self.assertEqual(fn(5), 7)
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(fn.get_attr('updated')), V.original + 2
    )
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(fn.original), I.x
    )

  def test_tracing_with_attrs_returns(self):
    def f(x):
      c = expr_container.NamedContainer()
      c.original = x
      c.updated = c.original + 2
      return c.updated

    fn = kdf.fn(f)
    self.assertEqual(fn.with_attrs(returns=fn.original)(x=5), 5)

  def test_lambda_tracing(self):
    def f(x):
      c = expr_container.NamedContainer()
      c.original = x
      c.update = lambda x: x + 2
      return c.update(c.original + 1)

    fn = kdf.fn(f)
    self.assertEqual(fn(5), 8)
    self.assertEqual(fn.update(5), 7)

  def test_renamed_lambda_tracing(self):
    def f(x):
      c = expr_container.NamedContainer()
      c.original = x
      c.update = lambda x: x + 2
      c.update_renamed = c.update
      return c.update_renamed(c.original + 1)

    fn = kdf.fn(f)
    self.assertEqual(fn(5), 8)
    self.assertEqual(fn.update_renamed(5), 7)

  def test_lambda_no_tracing(self):
    c = expr_container.NamedContainer()
    c.x = 5
    c.update = lambda x: x + 2
    self.assertEqual(c.update(c.x), 7)

  def test_delete_attr(self):
    c = expr_container.NamedContainer()
    c.x = 1
    c.y = 2
    c.z = 3
    del c.y
    self.assertCountEqual(dir(c), ['x', 'z'])

  def test_hasattr(self):
    c = expr_container.NamedContainer()
    c.x = 1
    c.y = 2
    self.assertTrue(hasattr(c, 'x'))
    self.assertTrue(hasattr(c, 'y'))
    self.assertFalse(hasattr(c, 'z'))

  def test_attribute_errors(self):
    c = expr_container.NamedContainer()
    with self.assertRaisesRegex(AttributeError, 'foo'):
      _ = c.foo
    with self.assertRaisesRegex(AttributeError, 'foo'):
      del c.foo

  def test_reserved_keys(self):
    c = expr_container.NamedContainer()
    with self.assertRaisesRegex(AttributeError, '_a'):
      c._a = 1
    with self.assertRaisesRegex(AttributeError, 'a_'):
      c.a_ = 1
    with self.assertRaisesRegex(AttributeError, '__setattr__'):
      c.__setattr__ = 1

  def test_vars(self):
    c = expr_container.NamedContainer()
    c.x = 1
    c.y = 2
    vars_dict = vars(c)
    self.assertSetEqual(set(vars_dict.keys()), {'x', 'y'})
    self.assertEqual(vars_dict['x'], c.x)
    self.assertEqual(vars_dict['y'], c.y)


if __name__ == '__main__':
  absltest.main()
