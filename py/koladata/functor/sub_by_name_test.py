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

import logging

from absl.testing import absltest
from koladata import kd as user_facing_kd
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.functor import boxing  # pylint: disable=unused-import
from koladata.functor import expr_container
from koladata.functor import functor_factories
from koladata.functor import sub_by_name
from koladata.functor import tracing_decorator
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

V = input_container.InputContainer('V')
I = input_container.InputContainer('I')


class CompatibleSignaturesTest(absltest.TestCase):

  def test_compatible_signatures(self):
    def f1(x):
      return x + 1

    def f2(x):
      return x + 2

    traced_f1 = functor_factories.trace_py_fn(f1)
    traced_f2 = functor_factories.trace_py_fn(f2)
    sub_by_name.assert_signatures_compatible(traced_f1, traced_f2)

  def test_kw_or_positional_to_positional_or_kw_only(self):
    @functor_factories.trace_py_fn
    def f1(x):
      return x

    @functor_factories.trace_py_fn
    def f2(x, /):
      return x

    @functor_factories.trace_py_fn
    def f3(*, x):
      return x

    # pylint: disable-next=g-error-prone-assert-raises
    with self.assertRaisesRegex(ValueError, 'Incompatible functor signatures'):
      sub_by_name.assert_signatures_compatible(f1, f2)

    # pylint: disable-next=g-error-prone-assert-raises
    with self.assertRaisesRegex(ValueError, 'Incompatible functor signatures'):
      sub_by_name.assert_signatures_compatible(f1, f3)

    # If we move to a less restrictive signature, it is ok.
    sub_by_name.assert_signatures_compatible(f2, f1)
    sub_by_name.assert_signatures_compatible(f3, f1)

  def test_mixed_arg_types(self):
    @functor_factories.trace_py_fn
    def f1(x, /, y, *, z):
      return (x + y) * z

    @functor_factories.trace_py_fn
    def f2(x, y, z):
      return (x + y) * z

    # Makes all arguments kw-or-positional, which is less restrictive, so valid.
    sub_by_name.assert_signatures_compatible(f1, f2)

    # pylint: disable-next=g-error-prone-assert-raises
    with self.assertRaisesRegex(ValueError, 'Incompatible functor signatures'):
      sub_by_name.assert_signatures_compatible(f2, f1)

  def test_adding_args(self):
    @functor_factories.trace_py_fn
    def f1(x):
      return x

    @functor_factories.trace_py_fn
    def f2(x, y):
      return (x + y) * 2

    @functor_factories.trace_py_fn
    def f3(x, y=100):
      return (x + y) * 3

    # Adding args.
    # pylint: disable-next=g-error-prone-assert-raises
    with self.assertRaisesRegex(ValueError, 'Incompatible functor signatures'):
      # Adding an argument creates an invalid call.
      sub_by_name.assert_signatures_compatible(f1, f2)

    # Adding an argument with a default value, or adding a default to an
    # existing arg is valid.
    sub_by_name.assert_signatures_compatible(f2, f3)
    sub_by_name.assert_signatures_compatible(f1, f3)

  def test_removing_args(self):
    @functor_factories.trace_py_fn
    def f1(x, y=100):
      return (x + y) * 2

    @functor_factories.trace_py_fn
    def f2(x, y):
      return (x + y) * 3

    @functor_factories.trace_py_fn
    def f3(x):
      return x

    # pylint: disable-next=g-error-prone-assert-raises
    with self.assertRaisesRegex(ValueError, 'Incompatible functor signatures'):
      sub_by_name.assert_signatures_compatible(f1, f2)
    # pylint: disable-next=g-error-prone-assert-raises
    with self.assertRaisesRegex(ValueError, 'Incompatible functor signatures'):
      sub_by_name.assert_signatures_compatible(f2, f3)
    # pylint: disable-next=g-error-prone-assert-raises
    with self.assertRaisesRegex(ValueError, 'Incompatible functor signatures'):
      sub_by_name.assert_signatures_compatible(f1, f3)


class SubByNameTest(absltest.TestCase):

  def test_sub_by_name_variables(self):
    f = functor_factories.trace_py_fn(
        lambda y: user_facing_kd.with_name(1, 'x') + y
    )
    f_new = sub_by_name.sub_by_name(f, {'x': 5})

    self.assertEqual(f(1), 2)
    self.assertEqual(f_new(1), 6)

  def test_sub_by_name_empty_variables(self):
    f = functor_factories.trace_py_fn(
        lambda y: user_facing_kd.with_name(None, 'x') + y
    )
    f_new = sub_by_name.sub_by_name(f, {'x': 5})
    self.assertEqual(f_new(1), 6)

  def test_repeated_subs(self):
    f = functor_factories.trace_py_fn(
        lambda y: user_facing_kd.with_name(1, 'x') + y
    )
    f1 = sub_by_name.sub_by_name(f, {'x': 5})
    f2 = sub_by_name.sub_by_name(f1, {'x': 10})
    f3 = sub_by_name.sub_by_name(f2, {'x': 100})

    self.assertEqual(f(1), 2)
    self.assertEqual(f1(1), 6)
    self.assertEqual(f2(1), 11)
    self.assertEqual(f3(1), 101)

  def test_disallow_sub_expr_with_another_expr(self):
    f = functor_factories.trace_py_fn(
        lambda y: user_facing_kd.with_name(1, 'x') + y
    )
    with self.assertRaises(TypeError):
      # All values passed as replacement must be eager values.
      _ = sub_by_name.sub_by_name(f, {'x': V.z})

  def test_vars_outside_returns(self):
    @functor_factories.fn
    def f(x):
      myvars = expr_container.NamedContainer()
      myvars.z = 1
      myvars.y = myvars.z
      return x + myvars.y

    logging.error(f)
    f_new = sub_by_name.sub_by_name(f, {'z': 100})
    logging.error(f_new)
    self.assertEqual(f_new(x=1), 101)

  def test_functor_structure_remains_the_same(self):
    @tracing_decorator.TraceAsFnDecorator()
    def f(x):
      return x + user_facing_kd.with_name(42, 'var_1')

    f1 = functor_factories.trace_py_fn(f)
    f2 = sub_by_name.sub_by_name(f1, dict(var_1=3.14))
    f3 = sub_by_name.sub_by_name(f2, dict(var_1=2.718))
    testing.assert_equivalent(f1.f.returns, f2.f.returns)
    testing.assert_equivalent(f2.f.returns, f3.f.returns)
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(f1.returns), V._f_result
    )
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(f2.returns), V._f_result
    )
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(f3.returns), V._f_result
    )
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(f1.f.returns), I.x + V.var_1
    )
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(f2.f.returns), I.x + V.var_1
    )
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(f3.f.returns), I.x + V.var_1
    )

  def test_vars_inside_exprs_are_not_replaced(self):
    @functor_factories.trace_py_fn
    def f():
      bar = user_facing_kd.with_name(1, 'bar')
      baz = user_facing_kd.with_name(bar, 'baz')
      foo = user_facing_kd.with_name(baz, 'foo')
      return foo + 1

    f_new = sub_by_name.sub_by_name(f, dict(bar=100))
    self.assertEqual(f(), 2)
    self.assertEqual(f_new(), 101)
    testing.assert_equivalent(f_new.bar, user_facing_kd.int32(100))
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(f_new.baz), V.bar
    )
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(f_new.foo), V.baz
    )
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(f_new.returns), V.foo + 1
    )

  def test_functor_structure_remains_the_same_no_trace_as_fn(self):
    @functor_factories.fn
    def f(x):
      return x + user_facing_kd.with_name(None, 'var_1')

    f1 = sub_by_name.sub_by_name(f, dict(var_1=42))
    f2 = sub_by_name.sub_by_name(f1, dict(var_1=3.14))
    f3 = sub_by_name.sub_by_name(f2, dict(var_1=2.718))
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(f1.returns), I.x + V.var_1
    )
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(f2.returns), I.x + V.var_1
    )
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(f3.returns), I.x + V.var_1
    )
    testing.assert_allclose(
        data_slice.from_vals([f1(1), f2(1), f3(1)]),
        data_slice.from_vals([43, 4.14, 3.718]),
    )

  def test_comment(self):
    with self.subTest('comment #1'):

      @functor_factories.trace_py_fn
      def f(y):
        x = user_facing_kd.with_name(1, 'x')
        return x + y

      print(f)
      f_new = sub_by_name.sub_by_name(f, {'x': 5})
      self.assertEqual(f_new(1), 6)

    with self.subTest('comment #2'):

      @functor_factories.trace_py_fn
      @tracing_decorator.TraceAsFnDecorator()
      def double(x):
        return x * 2

      @tracing_decorator.TraceAsFnDecorator()
      def halve(x):
        return x / 2

      f = functor_factories.trace_py_fn(lambda x: double(x) + 1)
      f_new = sub_by_name.sub_by_name(
          f, {'double': functor_factories.trace_py_fn(halve)}
      )
      self.assertEqual(f(10), 21)
      f_new_result = f_new(10)
      self.assertEqual(f_new_result, 6.0)
      self.assertEqual(f_new_result.get_schema(), schema_constants.FLOAT32)

  def test_sub_var_by_name_in_nested_functor(self):
    @tracing_decorator.TraceAsFnDecorator()
    def f(x):
      return x + user_facing_kd.with_name(10000, 'y')

    f_fn = functor_factories.trace_py_fn(f)
    f_sub = sub_by_name.sub_by_name(f_fn, {'y': 1})
    self.assertEqual(f_sub(x=1), 2)

    h1 = functor_factories.trace_py_fn(lambda z: f_sub(x=z) * 2)
    self.assertEqual(h1(1), 4)

    h2 = sub_by_name.sub_by_name(h1, {'y': 5})
    self.assertEqual(h2(z=1), 12)

  def test_sub_var_by_name_with_named_container(self):
    @tracing_decorator.TraceAsFnDecorator()
    def f(x):
      myvars = expr_container.NamedContainer()
      myvars.y = 1
      return x + myvars.y

    self.assertEqual(f(x=1), 2)

    h1 = functor_factories.trace_py_fn(lambda z: f(x=z) * 2)
    h2 = sub_by_name.sub_by_name(h1, {'y': 5})
    self.assertEqual(h1(1), 4)
    self.assertEqual(h2(z=1), 12)

  def test_sub_by_name_subfunctors(self):
    @tracing_decorator.TraceAsFnDecorator()
    def g(y):
      return y + 1

    @tracing_decorator.TraceAsFnDecorator()
    def h(y):
      return y * 2

    f_named = functor_factories.trace_py_fn(lambda x: g(x) + 2)
    f_new = sub_by_name.sub_by_name(
        f_named, {'g': functor_factories.trace_py_fn(h)}
    )

    self.assertEqual(f_named(x=3), 6)
    self.assertEqual(f_new(x=3), 8)

  def test_fails_on_sub_subfunctors_with_non_functor(self):
    @tracing_decorator.TraceAsFnDecorator()
    def g(y):
      return y + 1

    @functor_factories.trace_py_fn
    def f_named(x):
      return g(x) + 2

    with self.assertRaisesRegex(
        ValueError, 'replacement for subfunctor must be a functor'
    ):
      sub_by_name.sub_by_name(f_named, {'g': 1})

  def test_sub_by_name_accepts_signature_different_names(self):
    @tracing_decorator.TraceAsFnDecorator()
    def g(y, /):
      return y + 1

    @tracing_decorator.TraceAsFnDecorator()
    def h(z, /):
      return z * 2

    # pylint: disable-next=unnecessary-lambda
    f = functor_factories.trace_py_fn(lambda x: g(x))
    f_new = sub_by_name.sub_by_name(f, {'g': functor_factories.trace_py_fn(h)})
    self.assertEqual(f_new(x=3), 6)

  def test_sub_by_name_positional_only_with_kw_or_positional_arg(self):
    @tracing_decorator.TraceAsFnDecorator()
    def positional_only(y, /):
      return y + 1

    @tracing_decorator.TraceAsFnDecorator()
    def kw_or_positional(y):
      return y * 2

    with self.subTest('ok, positional_only to kw_or_positional'):
      # Ok if we move from a more restrictive to a less restrictive signature.
      # pylint: disable-next=unnecessary-lambda
      f1 = functor_factories.trace_py_fn(lambda x: positional_only(x))
      _ = sub_by_name.sub_by_name(
          f1,
          {'positional_only': functor_factories.trace_py_fn(kw_or_positional)},
      )

    with self.subTest('kw_or_positional to positional_only + positional arg'):
      # pylint: disable-next=unnecessary-lambda
      f1_pos = functor_factories.trace_py_fn(lambda x: kw_or_positional(x))
      with self.assertRaisesRegex(
          ValueError, 'Incompatible functor signatures'
      ):
        # pylint: disable-next=unnecessary-lambda
        sub_by_name.sub_by_name(
            f1_pos,
            {
                'kw_or_positional': functor_factories.trace_py_fn(
                    positional_only
                )
            },
        )

    with self.subTest('kw_or_positional to positional_only + kw arg'):
      # pylint: disable-next=unnecessary-lambda
      f1_kw = functor_factories.trace_py_fn(lambda x: kw_or_positional(y=x))
      with self.assertRaisesRegex(
          ValueError, 'Incompatible functor signatures'
      ):
        sub_by_name.sub_by_name(
            f1_kw,
            {
                'kw_or_positional': functor_factories.trace_py_fn(
                    positional_only
                )
            },
        )

  def test_sub_by_name_signature_arity_mismatch(self):
    @tracing_decorator.TraceAsFnDecorator()
    def g(y):
      return y + 1

    @tracing_decorator.TraceAsFnDecorator()
    def h(x, y):
      return x + y

    @functor_factories.trace_py_fn
    def f(x):
      return g(y=x)

    with self.assertRaisesRegex(ValueError, 'Incompatible functor signatures'):
      sub_by_name.sub_by_name(f, {'g': functor_factories.trace_py_fn(h)})

  def test_sub_by_name_signature_kind_mismatch(self):
    @tracing_decorator.TraceAsFnDecorator()
    def g(y):
      return y * 2

    @tracing_decorator.TraceAsFnDecorator()
    def h(y, /):
      return y * 3

    @functor_factories.trace_py_fn
    def f(x):
      return g(y=x)

    with self.assertRaisesRegex(ValueError, 'Incompatible functor signatures'):
      sub_by_name.sub_by_name(f, {'g': functor_factories.trace_py_fn(h)})

  def test_sub_by_name_signature_default_mismatch(self):

    @tracing_decorator.TraceAsFnDecorator()
    def g1(y):
      return y * 3

    @tracing_decorator.TraceAsFnDecorator()
    def g2(y):
      return y * 5

    @tracing_decorator.TraceAsFnDecorator()
    def h1(y=1):
      return y * 7

    @tracing_decorator.TraceAsFnDecorator()
    def h2(y=100):
      return y * 11

    @functor_factories.trace_py_fn
    def calls_g1(x):
      return g1(x)

    @functor_factories.trace_py_fn
    def calls_h1(x):
      return h1(x)

    self.assertEqual(calls_g1(x=2), 6)
    self.assertEqual(calls_h1(x=2), 14)

    with self.subTest('ok, no default to arg with default'):
      # Valid substitution, from a signature without defaults to another
      # signature with defaults, so less restrictive.
      new = sub_by_name.sub_by_name(
          calls_g1, {'g1': functor_factories.trace_py_fn(h1)}
      )
      self.assertEqual(new(x=2), 14)

    with self.subTest('ok, both have no defaults'):
      # pylint: disable-next=unnecessary-lambda
      f_new = sub_by_name.sub_by_name(
          calls_g1, {'g1': functor_factories.trace_py_fn(g2)}
      )
      self.assertEqual(f_new(x=2), 10)

    with self.subTest('ok, both have defaults'):
      f_new = sub_by_name.sub_by_name(
          calls_h1, {'h1': functor_factories.trace_py_fn(h2)}
      )
      self.assertEqual(f_new(x=2), 22)

    with self.subTest('fail, removing default parameter'):
      with self.assertRaisesRegex(
          ValueError, 'Incompatible functor signatures'
      ):
        sub_by_name.sub_by_name(
            calls_h1, {'h1': functor_factories.trace_py_fn(g1)}
        )

  def test_sub_by_name_signature_variadic_works(self):
    @functor_factories.fn
    @tracing_decorator.TraceAsFnDecorator()
    def g1(y, **unused_kwargs):
      return y + 1

    @functor_factories.fn
    @tracing_decorator.TraceAsFnDecorator()
    def g2(y, **unused_kwargs):
      return y * 2

    @functor_factories.fn
    @tracing_decorator.TraceAsFnDecorator()
    def g3(y, *unused_args):
      return y + 1

    @functor_factories.fn
    @tracing_decorator.TraceAsFnDecorator()
    def g4(y, *unused_args):
      return y * 2

    @functor_factories.fn
    @tracing_decorator.TraceAsFnDecorator()
    def g5(y, /, *unused_args, **unused_kwargs):
      return y + 1

    @functor_factories.fn
    @tracing_decorator.TraceAsFnDecorator()
    def g6(y, /, *unused_args, **unused_kwargs):
      return y * 2

    with self.subTest('functor with kwargs'):
      f = functor_factories.trace_py_fn(lambda x: g1(y=x))
      f_new = sub_by_name.sub_by_name(f, {'g1': g2})
      self.assertEqual(f_new(x=3), 6)

    with self.subTest('functor with args'):
      f = functor_factories.trace_py_fn(lambda x: g3(y=x))
      f_new = sub_by_name.sub_by_name(f, {'g3': g4})
      self.assertEqual(f_new(x=3), 6)

    with self.subTest('functor with args and kwargs'):
      f = functor_factories.trace_py_fn(lambda x: g5(x, 10, 20, z=1))
      f_new = sub_by_name.sub_by_name(f, {'g5': g6})
      self.assertEqual(f_new(x=3), 6)

  def test_sub_by_name_signature_ignore_checks(self):
    # Changing the name of a keyword-only argument.
    @tracing_decorator.TraceAsFnDecorator()
    def g(*, x):
      return x + 1

    @tracing_decorator.TraceAsFnDecorator()
    def h(*, y):
      return y * 2

    # Root functor
    f = functor_factories.trace_py_fn(lambda z: g(x=z))
    self.assertEqual(f(z=3), 4)

    with self.assertRaisesRegex(ValueError, 'Incompatible functor signatures'):
      # By default, doesn't allow signature mismatches
      sub_by_name.sub_by_name(f, {'g': functor_factories.trace_py_fn(h)})

    # Turning off the checks it should work.
    f_new = sub_by_name.sub_by_name(
        f,
        {'g': functor_factories.trace_py_fn(h)},
        ignore_signature_checks=True,
    )
    with self.assertRaises(ValueError):
      # We end up with an invalid functor, as expected.
      f_new(z=3)

  def test_sub_by_name_subfunctor_collision(self):
    @tracing_decorator.TraceAsFnDecorator()
    def original(x):
      return x + 1

    @tracing_decorator.TraceAsFnDecorator(name='original')
    def replacement(x):
      return x * 3

    @functor_factories.trace_py_fn
    def branch_a(x):
      return original(x)

    @functor_factories.trace_py_fn
    def branch_b(x):
      return original(x)

    @functor_factories.trace_py_fn
    def branch_c(x):
      return replacement(x)

    with self.subTest('ok, same functor on both branches'):
      @functor_factories.trace_py_fn
      def f1(x):
        return branch_a(x) + branch_b(x)

      f_new = sub_by_name.sub_by_name(
          f1,
          {'original': functor_factories.trace_py_fn(replacement)},
      )
      self.assertEqual(f_new(1), (1 * 3) + (1 * 3))

    with self.subTest('raise error, different functor with same name'):
      @functor_factories.trace_py_fn
      def f2(x):
        return branch_a(x) + branch_c(x)

      with self.assertRaisesRegex(
          ValueError,
          'different variables share the same name: original',
      ):
        sub_by_name.sub_by_name(
            f2,
            {'original': functor_factories.trace_py_fn(replacement)},
        )

  def test_sub_by_name_variable_name_collision(self):
    power = user_facing_kd.int32(2)
    another_power = user_facing_kd.int32(7)

    @functor_factories.trace_py_fn
    def branch_a(x):
      return x ** user_facing_kd.with_name(power, 'power')

    @functor_factories.trace_py_fn
    def branch_b(x):
      return x ** user_facing_kd.with_name(power, 'power')

    @functor_factories.trace_py_fn
    def branch_c(x):
      return x ** user_facing_kd.with_name(another_power, 'power')

    with self.subTest('ok, same variable on both branches'):
      @functor_factories.trace_py_fn
      def f1(x):
        return branch_a(x) + branch_b(x)

      f_new = sub_by_name.sub_by_name(f1, {'power': another_power})
      self.assertEqual(f_new(2), (2 ** 7) + (2 ** 7))

    with self.subTest('raise error, different variable on branches'):
      @functor_factories.trace_py_fn
      def f2(x):
        return branch_a(x) + branch_c(x)

      with self.assertRaisesRegex(
          ValueError,
          'different variables share the same name: power',
      ):
        sub_by_name.sub_by_name(f2, {'power': another_power})


if __name__ == '__main__':
  absltest.main()
