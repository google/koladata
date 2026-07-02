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

import ast
import inspect
import textwrap

from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd

from koladata.ext.functor import fn_to_py

_MODULE_DOCSTRING = (
    '"""Auto-generated Python code representing a Koda Functor'
    ' converted to evaluation-equivalent Python.'
    ' Do not edit manually."""'
)


class FnToPyFuncRecTest(parameterized.TestCase):

  def test_trace_single_python_function(self):
    @kd.trace_as_fn()
    def f(x):
      return x * 2

    fn = kd.fn(f)
    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')
    self.assertIn(fn.f.get_itemid().no_bag(), result)

    func_rec = result[fn.f.get_itemid().no_bag()]
    self.assertEqual(func_rec.name, 'f')
    kd.testing.assert_equal(
        kd.expr.unpack_expr(func_rec.code),
        kd.I.x * 2,
    )
    self.assertEqual(func_rec.signature, inspect.signature(lambda x: None))

  @parameterized.parameters(
      (kd.S + 1, inspect.signature(lambda self, /: None)),
      (kd.I.x + kd.I.y, inspect.signature(lambda *, x, y: None)),
      (kd.I.x + kd.S, inspect.signature(lambda self, /, *, x: None)),
  )
  def test_koda_expr(self, expr, expected_signature):
    fn = kd.fn(expr)
    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')
    self.assertLen(result, 1)
    func_rec = result[fn.get_itemid().no_bag()]
    kd.testing.assert_equal(kd.expr.unpack_expr(func_rec.code), expr)
    self.assertEqual(func_rec.signature, expected_signature)

  def test_koda_fn_with_self(self):
    fn = kd.fn(lambda self=kd.item(42), /: self * 2)
    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')
    self.assertLen(result, 1)
    func_rec = result[fn.get_itemid().no_bag()]
    kd.testing.assert_equal(kd.expr.unpack_expr(func_rec.code), kd.S * 2)
    self.assertEqual(
        func_rec.signature,
        inspect.signature(lambda self=kd.item(42), /: None)
    )

  def test_trace_after_koda_expr(self):
    fn = kd.fn(lambda self, /: self.x * 2)
    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')
    self.assertLen(result, 1)
    func_rec = result[fn.get_itemid().no_bag()]
    kd.testing.assert_equal(kd.expr.unpack_expr(func_rec.code), kd.S.x * 2)
    self.assertEqual(
        func_rec.signature, inspect.signature(lambda self, /: None)
    )

  def test_complex_nested_definitions(self):
    @kd.trace_as_fn()
    def h(x):
      return x

    @kd.trace_as_fn()
    def g(x):
      return h(x / 2.0)

    @kd.trace_as_fn()
    def f(x):
      return h(x + 1) * 4

    def top(x):
      return f(x) + g(x)

    fn = kd.fn(top)
    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')

    self.assertLen(result, 4)  # top, f, g, h
    h_rec = result[fn.g.h.get_itemid().no_bag()]
    g_rec = result[fn.g.get_itemid().no_bag()]
    f_rec = result[fn.f.get_itemid().no_bag()]
    top_rec = result[fn.get_itemid().no_bag()]

    self.assertEqual(h_rec.name, 'h')
    self.assertEqual(g_rec.name, 'g')
    self.assertEqual(f_rec.name, 'f')
    self.assertEqual(top_rec.name, 'top')

    self.assertEqual(h_rec.signature, inspect.signature(lambda x: None))
    self.assertEqual(g_rec.signature, inspect.signature(lambda x: None))
    self.assertEqual(f_rec.signature, inspect.signature(lambda x: None))
    self.assertEqual(top_rec.signature, inspect.signature(lambda x: None))

    # Check that code packing worked and is present
    kd.testing.assert_equal(kd.expr.unpack_expr(h_rec.code), kd.I.x)
    kd.testing.assert_non_deterministic_exprs_equal(
        kd.expr.unpack_expr(g_rec.code),
        kd.V.h(kd.I.x / 2.0),
    )
    kd.testing.assert_non_deterministic_exprs_equal(
        kd.expr.unpack_expr(f_rec.code),
        kd.V.h(kd.I.x + 1) * 4,
    )
    kd.testing.assert_non_deterministic_exprs_equal(
        kd.expr.unpack_expr(top_rec.code),
        kd.V.f(kd.I.x) + kd.V.g(kd.I.x),
    )

  def test_embed_non_expr_variable_as_literal(self):
    fn = kd.fn(lambda x: x + kd.with_name(42, 'a'))  # pyrefly: ignore[missing-attribute]
    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')
    self.assertLen(result, 1)
    func_rec = result[fn.get_itemid().no_bag()]
    kd.testing.assert_equal(
        kd.expr.unpack_expr(func_rec.code),
        kd.I.x + kd.item(42).with_bag(fn.get_bag()),  # pyrefly: ignore[missing-attribute]
    )
    self.assertEqual(func_rec.signature, inspect.signature(lambda x: None))

  def test_topological_order(self):
    @kd.trace_as_fn()
    def a(x):
      return x * 2

    @kd.trace_as_fn()
    def b(x):
      return a(x) - 1

    @kd.trace_as_fn()
    def c(x):
      return a(x) + 1

    def d(x):
      return c(x) * b(x)

    fn = kd.fn(d)

    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')
    self.assertLen(result, 4)

    # Get ItemIds
    d_id = fn.get_itemid().no_bag()
    c_id = fn.c.get_itemid().no_bag()
    b_id = fn.b.get_itemid().no_bag()
    self.assertEqual(fn.c.a, fn.b.a)
    a_id = fn.b.a.get_itemid().no_bag()

    # Default order returned by `visit_variables` is bottom up (leaves first)
    keys = list(result.keys())

    self.assertLess(keys.index(a_id), keys.index(b_id))
    self.assertLess(keys.index(a_id), keys.index(c_id))
    self.assertLess(keys.index(b_id), keys.index(d_id))
    self.assertLess(keys.index(c_id), keys.index(d_id))
    self.assertEqual(keys[-1], d_id)

  def test_non_functor_raises_error(self):
    item = kd.item(42)  # pyrefly: ignore[missing-attribute]
    with self.assertRaisesRegex(ValueError, '.* is not a functor'):
      fn_to_py.fn_to_py_fn_rec(item, root_name='top')

  def test_different_functors_same_name_are_renamed(self):
    g1 = kd.fn(lambda x: x + 1)
    g2 = kd.fn(lambda x: x + 2)
    f1 = kd.fn(kd.V.g(kd.S), g=g1)
    f2 = kd.fn(kd.V.g(kd.S), g=g2)
    fn = kd.fn(kd.V.f1(kd.V.f2(kd.S)), f1=f1, f2=f2)

    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')

    self.assertEqual(result[g1.get_itemid().no_bag()].name, 'g')
    self.assertEqual(result[g2.get_itemid().no_bag()].name, 'g_2')
    self.assertEqual(result[f1.get_itemid().no_bag()].name, 'f1')
    self.assertEqual(result[f2.get_itemid().no_bag()].name, 'f2')

  def test_identical_functors_same_name_not_renamed(self):
    @kd.trace_as_fn()
    def a(x):
      return x * 2

    @kd.trace_as_fn()
    def b(x):
      return a(x) - 1

    def c(x):
      return a(x) + b(x)

    fn = kd.fn(c)
    # Verify a is shared between b and c.
    self.assertEqual(fn.a, fn.b.a)

    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')

    self.assertEqual(result[fn.a.get_itemid().no_bag()].name, 'a')
    self.assertEqual(result[fn.b.get_itemid().no_bag()].name, 'b')

  def test_multiple_conflict_groups(self):
    g1 = kd.fn(lambda x: x + 1)
    g2 = kd.fn(lambda x: x + 2)
    g3 = kd.fn(lambda x: x + 3)
    h1 = kd.fn(lambda x: x * 3)
    h2 = kd.fn(lambda x: x * 4)
    f1 = kd.fn(kd.V.g(kd.V.h(kd.S)), g=g1, h=h1)
    f2 = kd.fn(kd.V.g(kd.V.h(kd.S)), g=g2, h=h2)
    f3 = kd.fn(kd.V.g(kd.V.h(kd.S)), g=g3, h=h1)
    fn = kd.fn(kd.V.f1(kd.V.f2(kd.V.f3(kd.S))), f1=f1, f2=f2, f3=f3)

    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')

    self.assertEqual(result[g1.get_itemid().no_bag()].name, 'g')
    self.assertEqual(result[g2.get_itemid().no_bag()].name, 'g_2')
    self.assertEqual(result[g3.get_itemid().no_bag()].name, 'g_3')
    self.assertEqual(result[h1.get_itemid().no_bag()].name, 'h')
    self.assertEqual(result[h2.get_itemid().no_bag()].name, 'h_2')

  def test_collision_with_existing_name(self):
    g1 = kd.fn(lambda x: x + 1)
    g2 = kd.fn(lambda x: x + 2)
    g_2_sub = kd.fn(lambda x: x * 10)

    f1 = kd.fn(kd.V.g(kd.S), g=g1)
    f2 = kd.fn(kd.V.g(kd.V.g_2(kd.S)), g=g2, g_2=g_2_sub)
    fn = kd.fn(kd.V.f1(kd.S) ** kd.V.f2(kd.S), f1=f1, f2=f2)

    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')

    self.assertEqual(result[g1.get_itemid().no_bag()].name, 'g')
    self.assertEqual(result[g2.get_itemid().no_bag()].name, 'g_2')
    self.assertEqual(result[g_2_sub.get_itemid().no_bag()].name, 'g_2_2')

  def test_collision_with_variable_in_sibling_functor(self):
    g1 = kd.fn(lambda x: x + 1)
    g2 = kd.fn(lambda x: x + 2)
    g_2_sub = kd.fn(lambda x: x * 10)

    # f1 has subfunctor g1 named 'g', and another subfunctor named 'g_2'.
    f1 = kd.fn(kd.V.g(kd.V.g_2(kd.S)), g=g1, g_2=g_2_sub)
    # f2 also has a different subfunctor named 'g' (g2).
    f2 = kd.fn(kd.V.g(kd.S), g=g2)
    fn = kd.fn(kd.V.f1(kd.V.f2(kd.S)), f1=f1, f2=f2)

    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')

    # g_2_sub already occupies 'g_2', so g2 must be renamed to 'g_3'.
    self.assertEqual(result[g1.get_itemid().no_bag()].name, 'g')
    self.assertEqual(result[g2.get_itemid().no_bag()].name, 'g_3')
    self.assertEqual(result[g_2_sub.get_itemid().no_bag()].name, 'g_2')

  def test_intermediate_expr_variable_rename(self):
    g1 = kd.fn(lambda x: x + 1)
    g2 = kd.fn(lambda x: x + 2)
    intermediate_expr = kd.expr.pack_expr(kd.V.g(kd.I.x))
    f1 = kd.fn(kd.V.intermediate, intermediate=intermediate_expr, g=g1)
    f2 = kd.fn(kd.V.intermediate, intermediate=intermediate_expr, g=g2)
    fn = kd.fn(kd.V.f1(kd.V.f2(kd.I.x)), f1=f1, f2=f2)

    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')

    self.assertEqual(result[g1.get_itemid().no_bag()].name, 'g')
    self.assertEqual(result[g2.get_itemid().no_bag()].name, 'g_2')
    # f1's inlined code references 'g' (original name kept).
    kd.testing.assert_non_deterministic_exprs_equal(
        kd.expr.unpack_expr(result[f1.get_itemid().no_bag()].code),
        kd.V.g(kd.I.x),
    )
    # f2's inlined code references 'g_2' (renamed via inlining).
    kd.testing.assert_non_deterministic_exprs_equal(
        kd.expr.unpack_expr(result[f2.get_itemid().no_bag()].code),
        kd.V.g_2(kd.I.x),
    )

  def test_single_functor_multiple_names(self):
    shared_sub = kd.fn(lambda x: x + 1)
    f1 = kd.functor.expr_fn(kd.V.f(kd.S), f=shared_sub)  # pyrefly: ignore[missing-attribute]
    f2 = kd.fn(kd.V.g(kd.S), g=shared_sub)
    fn = kd.fn(kd.V.f1(kd.V.f2(kd.S)), f1=f1, f2=f2)

    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')

    # Deduplicated to a single name (the first one encountered, 'f').
    self.assertEqual(result[shared_sub.get_itemid().no_bag()].name, 'f')
    # f2's code should reference 'f' (the deduplicated name), not 'g'.
    kd.testing.assert_non_deterministic_exprs_equal(
        kd.expr.unpack_expr(result[f2.get_itemid().no_bag()].code),
        kd.V.f(kd.S),
    )

  def test_same_subfunctor_different_names_same_parent(self):
    shared = kd.fn(lambda x: x + 1)
    fn = kd.functor.expr_fn(kd.V.f(kd.V.g(kd.S)), f=shared, g=shared)  # pyrefly: ignore[missing-attribute]

    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='top')

    # Deduplicated to a single name (the first one encountered, 'f').
    self.assertEqual(result[shared.get_itemid().no_bag()].name, 'f')
    # The root expression should have 'g' rewritten to 'f'.
    kd.testing.assert_non_deterministic_exprs_equal(
        kd.expr.unpack_expr(result[fn.get_itemid().no_bag()].code),
        kd.V.f(kd.V.f(kd.S)),
    )

  def test_root_name_collision(self):
    g1 = kd.fn(lambda x: x + 1)
    g2 = kd.fn(lambda x: x + 2)
    f1 = kd.fn(kd.V.g(kd.I.x), g=g1)
    f2 = kd.fn(kd.V.g(kd.I.x), g=g2)
    fn = kd.fn(kd.V.f1(kd.V.f2(kd.I.x)), f1=f1, f2=f2)

    # Passing root_name='g_2' means 'g_2' is occupied by the root.
    # g2 (the second 'g') should be renamed to 'g_3'.
    result = fn_to_py.fn_to_py_fn_rec(fn, root_name='g_2')

    self.assertEqual(result[g1.get_itemid().no_bag()].name, 'g')
    self.assertEqual(result[g2.get_itemid().no_bag()].name, 'g_3')
    self.assertEqual(result[f1.get_itemid().no_bag()].name, 'f1')
    self.assertEqual(result[f2.get_itemid().no_bag()].name, 'f2')


class FnToPyTest(absltest.TestCase):

  def test_fn_to_py(self):
    @kd.trace_as_fn()
    def helper(x):
      return x * 2

    def compute(x, y):
      return helper(x + y)

    fn = kd.fn(compute)
    result = fn_to_py.fn_to_py(fn)

    self.assertIsInstance(result, ast.Module)
    self.assertLen(result.body, 5)

    expected_code = _MODULE_DOCSTRING + textwrap.dedent("""
        from arolla import arolla
        from koladata import kd

        @kd.trace_as_fn()
        def helper(x):
            return x * 2

        def top(x, y):
            return helper(x + y)""")
    code = ast.unparse(result)
    self.assertEqual(code, expected_code)

  def test_fn_to_py_compiles(self):
    @kd.trace_as_fn()
    def helper(x):
      return x * 2

    def compute(x, y):
      return helper(x + y)

    fn = kd.fn(compute)
    module = fn_to_py.fn_to_py(fn)
    # Succeeds if the code can be run.
    _ = compile(module, filename='<ast>', mode='exec')

  def test_fn_to_py_with_name_conflicts(self):
    g1 = kd.fn(lambda x: x + 1)
    g2 = kd.fn(lambda x: x + 2)
    f1 = kd.fn(kd.V.g(kd.I.x), g=g1)
    f2 = kd.fn(kd.V.g(kd.I.x), g=g2)
    fn = kd.fn(kd.V.f1(kd.I.x) + kd.V.f2(kd.I.x), f1=f1, f2=f2)
    result = fn_to_py.fn_to_py(fn)

    expected_code = _MODULE_DOCSTRING + textwrap.dedent("""
        from arolla import arolla
        from koladata import kd

        @kd.trace_as_fn()
        def g(x):
            return x + 1

        @kd.trace_as_fn()
        def f1(*, x):
            return g(x)

        @kd.trace_as_fn()
        def g_2(x):
            return x + 2

        @kd.trace_as_fn()
        def f2(*, x):
            return g_2(x)

        def top(*, x):
            return f1(x) + f2(x)""")
    code = ast.unparse(result)
    self.assertEqual(code, expected_code)

  def test_fn_to_py_single_functor_multiple_names(self):
    shared_sub = kd.fn(lambda x: x + 1)
    f1 = kd.functor.expr_fn(kd.V.f(kd.I.x), f=shared_sub)  # pyrefly: ignore[missing-attribute]
    f2 = kd.fn(kd.V.g(kd.I.x), g=shared_sub)
    fn = kd.fn(kd.V.f1(kd.I.x) + kd.V.f2(kd.I.x), f1=f1, f2=f2)

    result = fn_to_py.fn_to_py(fn)
    expected_code = _MODULE_DOCSTRING + textwrap.dedent("""
        from arolla import arolla
        from koladata import kd

        @kd.trace_as_fn()
        def f(x):
            return x + 1

        @kd.trace_as_fn()
        def f1(*, x):
            return f(x)

        @kd.trace_as_fn()
        def f2(*, x):
            return f(x)

        def top(*, x):
            return f1(x) + f2(x)""")
    code = ast.unparse(result)
    self.assertEqual(code, expected_code)

  def test_invalid_root_name_error(self):
    fn = kd.fn(lambda x: x + 1)
    with self.assertRaisesRegex(
        ValueError, 'is not a valid Python identifier'
    ):
      fn_to_py.fn_to_py(fn, name='not-valid')

  def test_undefined_subfunctor_error(self):
    fn = kd.fn(kd.V.undefined_fn(kd.I.x), undefined_fn=None)
    with self.assertRaisesRegex(
        ValueError, r'\[top\] only functor variables can be called'
    ):
      fn_to_py.fn_to_py(fn)

  def test_non_functor_error(self):
    with self.assertRaisesRegex(ValueError, 'is not a functor'):
      fn_to_py.fn_to_py(kd.item(42))  # pyrefly: ignore[missing-attribute]

  def test_fn_to_py_with_docstrings(self):
    @kd.trace_as_fn()
    def helper(x):
      """Doubles the input."""
      return x * 2

    def compute(x, y):
      """Computes the result."""
      return helper(x + y)

    fn = kd.fn(compute)
    result = fn_to_py.fn_to_py(fn)

    expected_code = _MODULE_DOCSTRING + textwrap.dedent("""
        from arolla import arolla
        from koladata import kd

        @kd.trace_as_fn()
        def helper(x):
            \"\"\"Doubles the input.\"\"\"
            return x * 2

        def top(x, y):
            \"\"\"Computes the result.\"\"\"
            return helper(x + y)""")
    code = ast.unparse(result)
    self.assertEqual(code, expected_code)

  def test_fn_to_py_with_functor_literals_with_name(self):
    def f1_py(x):
      return x + 1

    def f2_py(x):
      return x ** 2

    f1 = kd.fn(f1_py)
    f2 = kd.fn(f2_py)
    fn = kd.fn(
        lambda x: kd.with_name(f1, 'my_f1')(x) + kd.with_name(f2, 'my_f2')(x)  # pyrefly: ignore[missing-attribute]
    )
    result = fn_to_py.fn_to_py(fn)

    expected_code = _MODULE_DOCSTRING + textwrap.dedent("""
        from arolla import arolla
        from koladata import kd

        @kd.trace_as_fn()
        def my_f1(x):
            return x + 1

        @kd.trace_as_fn()
        def my_f2(x):
            return x ** 2

        def top(x):
            return my_f1(x) + my_f2(x)""")
    code = ast.unparse(result)
    self.assertEqual(code, expected_code)


if __name__ == '__main__':
  absltest.main()
