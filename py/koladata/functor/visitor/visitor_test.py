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
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.functions import functions as fns
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


class VisitFunctorsTest(absltest.TestCase):

  def test_simple(self):
    fn = functor_factories.fn(lambda x: x)

    visited = []

    def callback(f, subfunctors):
      visited.append(f)
      self.assertEmpty(subfunctors)
      return f

    result = visitor.visit_functors(fn, callback)
    self.assertLen(visited, 1)
    testing.assert_equal(fn, visited[0])
    testing.assert_equal(result, fn)

  def test_with_manual_nested_subfunctors(self):
    g = functor_factories.fn(lambda x: x + 1)
    h = functor_factories.fn(lambda x: x * 2)
    k = functor_factories.fn(V.g(I.x) * 0.5, g=g)
    fn = functor_factories.fn(V.k(I.x) + V.h(I.y), k=k, h=h)

    visited = []
    subfunctor_info = {}

    def callback(f, subfunctors):
      visited.append(f)
      subfunctor_info[f.fingerprint] = subfunctors
      return f

    result = visitor.visit_functors(fn, callback)
    kd.testing.assert_equal(result, fn)
    self.assertLen(visited, 4)
    # Postorder: leaves first.
    testing.assert_equal(visited[0], fn.h)
    self.assertEmpty(subfunctor_info[visited[0].fingerprint])
    testing.assert_equal(visited[1], fn.k.g)
    self.assertEmpty(subfunctor_info[visited[1].fingerprint])
    testing.assert_equal(visited[2], fn.k)
    self.assertCountEqual(subfunctor_info[visited[2].fingerprint], ['g'])
    self.assertCountEqual(
        subfunctor_info[visited[3].fingerprint].keys(), ['k', 'h']
    )

  def test_transform_subfunctors(self):
    g = functor_factories.fn(V.a * I.x + V.b, a=None, b=3.14)
    h = functor_factories.fn(V.b * I.x, b=None)
    fn = functor_factories.fn(V.g(I.x) + V.h(I.y)).with_attrs(g=g, h=h)

    def transform(f, subfunctors):
      if f == g:
        return f.with_attrs(a=42)
      if f == h:
        return f.with_attrs(b=37)
      if subfunctors:
        return f.with_attrs(**subfunctors)
      return f

    new_fn = visitor.visit_functors(fn, transform)
    testing.assert_equivalent(
        new_fn,
        fn.updated(_kd.attrs(fn.g, a=42), _kd.attrs(fn.h, b=37)),
        schemas_equality=False,
    )

  def test_shared_subfunctor_visited_once(self):

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

    visited = []
    def callback(f, _):
      visited.append(f)
      return f

    fn = functor_factories.fn(lambda x: f0(x) + f1(x) + f2(x))
    # Does not raise false recursion and h is visited only once.
    _ = visitor.visit_functors(fn, callback)
    self.assertLen(visited, 5)

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
      _ = visitor.visit_functors(fn, lambda f, _: f)

  def test_expr_recursion_not_supported(self):
    fn = functor_factories.fn(V.g(I.x), g=introspection.pack_expr(V.g))

    with self.assertRaisesRegex(ValueError, 'has a dependency cycle'):
      _ = visitor.visit_variables(fn, lambda _1, _2: None)

  def test_visiting_non_functor_raises_error(self):
    with self.assertRaisesRegex(ValueError, '.* is not a functor'):
      _ = visitor.visit_functors(fns.item(42), lambda _1, _2: None)

  def test_functor_returns_non_expr(self):
    fn = functor_factories.fn(lambda: 42).with_attrs(returns=fns.item(42))

    def callback(f, _):
      self.assertEqual(f.returns, fns.item(42))
      return f

    _ = visitor.visit_functors(fn, callback)

  def test_callback_fn_returns_non_dataitem(self):
    g = functor_factories.fn(lambda x: x + 1)
    h = functor_factories.fn(lambda x: x * 2)
    k = functor_factories.fn(V.g(I.x) * 0.5, g=g)
    fn = functor_factories.fn(V.k(I.x) + V.h(I.y), k=k, h=h)

    def callback(_, sub_fns):
      deps = [
          f'{name}: {value}' for name, value in sub_fns.items()
      ]
      return f'deps[{", ".join(deps)}]'

    self.assertEqual(
        visitor.visit_functors(fn, callback),
        'deps[h: deps[], k: deps[g: deps[]]]',
    )


class VisitVariablesTest(absltest.TestCase):
  """Tests for the non-recursive visit_variables (single functor)."""

  def test_simple_no_variables(self):
    fn = functor_factories.fn(lambda x: x)

    result = visitor.visit_variables(fn, lambda _n, v, _vs: v)
    # Result is the returns DataItem (an EXPR).
    self.assertEqual(result.get_schema(), schema_constants.EXPR)

  def test_identity_callback(self):
    fn = functor_factories.fn(V.a * I.x + V.b, a=42, b=fns.item('xyz'))

    names = []
    def callback(var_name, var, visited_vars):
      del visited_vars  # Unused.
      names.append(var_name)
      return var

    visitor.visit_variables(fn, callback)
    # Variables visited in topological order, including 'returns'.
    self.assertEqual(names, ['a', 'b', 'returns'])

  def test_inline_expr_variables(self):
    """Callback can return Expr to inline variables into returns."""
    fn = functor_factories.fn(V.a * I.x, a=42)

    def callback(_, var, visited_vars):
      if var.get_schema() == schema_constants.EXPR:
        subs = {V[n].fingerprint: val for n, val in visited_vars.items()}
        return arolla.abc.sub_by_fingerprint(
            introspection.unpack_expr(var), subs
        )
      return kd.expr.literal(var.no_bag())

    result = visitor.visit_variables(fn, callback)
    # returns should have V.a inlined as literal(42).
    expected = kd.lazy.math.multiply(kd.expr.literal(fns.item(42)), I.x)
    testing.assert_equal(result, expected)

  def test_visited_vars_accumulates(self):
    """visited_vars contains all previously visited callback results."""
    fn = functor_factories.fn(V.a * I.x + V.b, a=42, b=37)

    visited_at = {}

    def callback(var_name, var, visited_vars):
      visited_at[var_name] = list(visited_vars.keys())
      return var

    visitor.visit_variables(fn, callback)
    self.assertEmpty(visited_at['a'])  # first - no previous vars.
    self.assertEqual(visited_at['b'], ['a'])
    self.assertEqual(visited_at['returns'], ['a', 'b'])

  def test_subfunctor_not_recursed(self):
    """visit_variables should not recurse into subfunctors."""
    g = functor_factories.fn(V.a * I.x, a=42)
    fn = functor_factories.fn(V.g(I.x) + V.b, g=g, b=37)

    names = []
    visitor.visit_variables(fn, lambda n, v, _vs: names.append(n) or v)
    self.assertEqual(names, ['b', 'g', 'returns'])

  def test_visiting_non_functor_raises_error(self):
    with self.assertRaisesRegex(ValueError, '.* is not a functor'):
      _ = visitor.visit_variables(fns.item(42), lambda _1, _2, _3: None)


if __name__ == '__main__':
  absltest.main()
