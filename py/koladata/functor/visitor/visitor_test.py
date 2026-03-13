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
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.functions import functions as fns
from koladata.functions import predicates
from koladata.functor import functor_factories
from koladata.functor import tracing_decorator
from koladata.functor.visitor import visitor
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import schema_constants

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')

_kd = eager_op_utils.operators_container('kd')


class VisitVariablesTest(absltest.TestCase):

  def test_simple(self):
    fn = functor_factories.fn(lambda x: x)

    vars_ = []
    self.assertEqual(
        visitor.visit_variables(fn, lambda v, _: vars_.append(v)), fn
    )
    self.assertLen(vars_, 1)
    testing.assert_equal(fn, vars_[0])

  def test_with_variables(self):
    fn = functor_factories.fn(
        lambda x: V.a * x + V.b
    ).with_attrs(a=42, b='xyz')

    vars_ = []
    self.assertEqual(
        visitor.visit_variables(fn, lambda v, _: vars_.append(v)), fn
    )
    self.assertLen(vars_, 3)
    testing.assert_equal(fn.a, vars_[0])
    testing.assert_equal(fn.b, vars_[1])
    testing.assert_equal(fn, vars_[2])

  def test_with_manual_functors_and_nested_variables(self):
    fn = functor_factories.fn(
        lambda x, y: V.g(x) + V.h(y)
    ).with_attrs(
        g=functor_factories.fn(lambda x: V.a * x).with_attrs(
            a=42
        ),
        h=functor_factories.fn(lambda x: V.b * x).with_attrs(
            b=37
        ),
    )
    vars_ = []
    self.assertEqual(
        visitor.visit_variables(fn, lambda v, _: vars_.append(v)), fn
    )
    self.assertLen(vars_, 5)
    testing.assert_equal(fn.g.a, vars_[0])
    testing.assert_equal(fn.g, vars_[1])
    testing.assert_equal(fn.h.b, vars_[2])
    testing.assert_equal(fn.h, vars_[3])
    testing.assert_equal(fn, vars_[4])

  def test_sub_functor_with_auto_and_manual_variables(self):

    def h(x):
      return x

    traced_h = tracing_decorator.TraceAsFnDecorator()(h)

    def g(x):
      return traced_h(x / 2.)

    traced_g = tracing_decorator.TraceAsFnDecorator()(g)

    def f(x):
      return (
          V.h(x + 1) * 4
          + V.c
          + V.d
      )

    def top(x):
      return V.f(x) + traced_g(x)

    top_fn = functor_factories.fn(top)
    f_fn = functor_factories.fn(f).with_attrs(c=42)
    fn = top_fn.with_attrs(f=f_fn.with_attrs(h=top_fn.g.h))

    vars_ = []
    sub_vars = {}
    def callback(v, sub_v):
      sub_vars[v.fingerprint] = sub_v
      vars_.append(v)

    self.assertEqual(visitor.visit_variables(fn, callback), fn)
    self.assertLen(vars_, 9)
    # `vars_` are postordered.
    self.assertEqual(fn.f.h, fn.g.h)
    testing.assert_equal(vars_[0], fn.f.h)
    self.assertEmpty(sub_vars[vars_[0].fingerprint].keys())
    testing.assert_equal(vars_[1], fn.f.c)
    self.assertEmpty(sub_vars[vars_[1].fingerprint].keys())
    testing.assert_equal(vars_[2], fn.f)
    self.assertCountEqual(sub_vars[vars_[2].fingerprint].keys(), ['h', 'c'])
    testing.assert_equal(vars_[3], fn.maybe('_aux_0'))
    self.assertEmpty(sub_vars[vars_[3].fingerprint].keys())
    testing.assert_equal(vars_[4], fn.g.get_attr('_h_result'))
    self.assertCountEqual(sub_vars[vars_[4].fingerprint].keys(), ['h'])
    testing.assert_equal(vars_[5], fn.g)
    self.assertCountEqual(
        sub_vars[vars_[5].fingerprint].keys(), ['h', '_h_result']
    )
    testing.assert_equal(vars_[6], fn.maybe('_aux_0'))
    self.assertEmpty(sub_vars[vars_[6].fingerprint].keys())
    testing.assert_equal(vars_[7], fn.maybe('_g_result'))
    self.assertCountEqual(
        sub_vars[vars_[7].fingerprint].keys(), ['g', '_aux_0']
    )
    testing.assert_equal(vars_[8], fn)
    self.assertCountEqual(
        sub_vars[vars_[8].fingerprint].keys(), ['g', '_g_result', 'f', '_aux_0']
    )

  def test_simple_transform(self):
    g = functor_factories.fn(
        lambda x: V.a * x + V.b
    ).with_attrs(b=3.14)
    h = functor_factories.fn(lambda x: V.b * x)
    fn = functor_factories.fn(
        lambda x, y: V.g(x) + V.h(y)
    ).with_attrs(
        g=g,
        h=h,
    )

    def transform(var, sub_vars):
      if var == g:
        return var.with_attrs(a=42)
      if var == h:
        return var.with_attrs(b=37)
      if sub_vars:
        return var.with_attrs(**sub_vars)
      return var

    new_fn = visitor.visit_variables(fn, transform)
    testing.assert_equivalent(
        new_fn,
        fn.updated(_kd.attrs(fn.g, a=42), _kd.attrs(fn.h, b=37)),
        schemas_equality=False,
    )

  def test_on_stack_works_correctly_when_fn_is_already_visited(self):

    @tracing_decorator.TraceAsFnDecorator()
    def h(x):
      return x

    @tracing_decorator.TraceAsFnDecorator()
    def f0(x):
      return h(x)

    @tracing_decorator.TraceAsFnDecorator()
    def f1(x):
      return h(x) + 1

    @tracing_decorator.TraceAsFnDecorator()
    def f2(x):
      return h(x) / 2.

    fn = functor_factories.fn(lambda x: f0(x) + f1(x) + f2(x))
    # Does not raise false recursion, because it correctly removes functor
    # itemids from the stack.
    _ = visitor.visit_variables(fn, lambda _1, _2: None)

  def test_recursion_not_supported(self):
    fn = functor_factories.expr_fn(
        kde_operators.kde.cond(
            I.n == 0,
            V.stop,
            V.go,
        )(n=I.n),
        go=functor_factories.expr_fn(
            I.n
            * V.rec(n=I.n - 1)
        ),
        stop=functor_factories.expr_fn(1),
    )
    fn = fn.updated(_kd.attrs(fn.go, rec=fn))

    with self.assertRaisesRegex(ValueError, 'is recursive'):
      _ = visitor.visit_variables(fn, lambda _1, _2: None)

  def test_expr_recursion_not_supported(self):
    fn = functor_factories.fn(lambda x: V.f).with_attrs(
        f=introspection.pack_expr(V.f)
    )

    with self.assertRaisesRegex(ValueError, 'has dependency cycle'):
      _ = visitor.visit_variables(fn, lambda _1, _2: None)

  def test_visit_item_fallback(self):
    fn = functor_factories.fn(
        lambda x: x + V.a + V.lst[0]
    ).with_attrs(a=42, lst=fns.list([1, 2, 3]))

    vars_ = []
    self.assertEqual(
        visitor.visit_variables(fn, lambda v, _: vars_.append(v)), fn
    )
    self.assertLen(vars_, 3)
    testing.assert_equal(fn.a, vars_[0])
    testing.assert_equal(fn.lst, vars_[1])
    testing.assert_equal(fn, vars_[2])

  def test_callback_fn_returns_non_dataitem_for_functor(self):
    fn = functor_factories.fn(lambda x: x + V.a).with_attrs(
        a=functor_factories.fn(lambda x: x)
    )

    def callback(v, _):
      if functor_factories.is_fn(v) and v.fingerprint == fn.a.fingerprint:
        return 'not a dataitem'
      return v

    with self.assertRaisesRegex(
        ValueError, '`callback_fn` should return either None or DataItem'
    ):
      _ = visitor.visit_variables(fn, callback)

  def test_callback_fn_returns_non_dataitem_for_expr(self):
    fn = functor_factories.fn(lambda x: x + V.a).with_attrs(
        a=introspection.pack_expr(V.b)
    )

    def callback(v, _):
      if (
          v.get_schema() == schema_constants.EXPR
          and introspection.unpack_expr(v).fingerprint
          == V.b.fingerprint
      ):
        return 'not a dataitem'
      return v

    with self.assertRaisesRegex(
        ValueError, '`callback_fn` should return either None or DataItem'
    ):
      _ = visitor.visit_variables(fn, callback)

  def test_callback_fn_returns_non_dataitem_for_item(self):
    fn = functor_factories.fn(lambda x: x + V.a).with_attrs(
        a=42
    )

    def callback(v, _):
      if predicates.is_item(v) and v.fingerprint == fn.a.fingerprint:
        return 'not a dataitem'
      return v

    with self.assertRaisesRegex(
        ValueError, '`callback_fn` should return either None or DataItem'
    ):
      _ = visitor.visit_variables(fn, callback)

  def test_visiting_non_functor_raises_error(self):
    with self.assertRaisesRegex(ValueError, '.* is not a functor'):
      _ = visitor.visit_variables(fns.item(42), lambda _1, _2: None)


class VisitSubfunctorsTest(absltest.TestCase):

  def test_non_nested(self):
    fn = functor_factories.fn(lambda x: x)

    sub_fns = []
    visitor.visit_subfunctors(fn, sub_fns.append)

    self.assertLen(sub_fns, 1)
    testing.assert_equal(fn, sub_fns[0])

  def test_sub_functor_as_auto_variable(self):

    def f(x):
      return x

    traced_f = tracing_decorator.TraceAsFnDecorator()(f)

    fn = functor_factories.fn(traced_f)

    sub_fns = []
    visitor.visit_subfunctors(fn, sub_fns.append)

    self.assertLen(sub_fns, 2)
    # `sub_fns` are postordered.
    testing.assert_equivalent(sub_fns[0], functor_factories.fn(f))
    testing.assert_equivalent(sub_fns[1], fn)

  def test_sub_functor_as_manual_variable(self):

    def f(x):
      return V.g(x + 1) * 5 + V.a

    def g(x):
      return V.h(x / 2.0)

    g_fn = functor_factories.fn(g).with_attrs(
        h=functor_factories.fn(lambda x: x)
    )
    fn = functor_factories.fn(f).with_attrs(g=g_fn, a=42)

    sub_fns = []
    visitor.visit_subfunctors(fn, sub_fns.append)

    self.assertLen(sub_fns, 3)
    # `sub_fns` are postordered.
    testing.assert_equivalent(sub_fns[0], functor_factories.fn(lambda x: x))
    testing.assert_equivalent(sub_fns[1], g_fn)
    testing.assert_equivalent(sub_fns[2], fn)


if __name__ == '__main__':
  absltest.main()
