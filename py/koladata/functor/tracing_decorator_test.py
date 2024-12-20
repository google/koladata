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
from absl.testing import parameterized
from koladata import kd as user_facing_kd
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.functor import tracing_decorator
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class TracingDecoratorTest(parameterized.TestCase):

  def test_default_behavior(self):
    @tracing_decorator.TraceAsFnDecorator()
    def f(x):
      return x + 1

    # This does not get wrapped into a DataSlice since in eager mode the
    # tracing decorator does nothing.
    self.assertEqual(f(x=1), 2)
    outer_fn = lambda x: f(x=x + 2)
    self.assertEqual(outer_fn(x=1), 4)

    fn = functor_factories.trace_py_fn(outer_fn)
    testing.assert_equal(fn(x=1), ds(4))
    # The decorator assigned a name which was then auto-extracted into a
    # variable, so we can use that to access the sub-function.
    testing.assert_equal(fn.f(x=1), ds(2))
    # Make sure tracing actually happened for the contents of f.
    testing.assert_equal(introspection.unpack_expr(fn.f.returns), I.x + 1)

  def test_two_lambdas(self):
    f1 = tracing_decorator.TraceAsFnDecorator()(lambda x: x + 1)
    f2 = tracing_decorator.TraceAsFnDecorator()(lambda x: x * 2)
    fn = functor_factories.trace_py_fn(lambda x: f2(f1(x)))
    testing.assert_equal(fn(x=1), ds(4))
    self.assertCountEqual(
        fn.get_attr_names(intersection=True),
        [
            'returns',
            '<lambda>',
            '<lambda>_0',
            '<lambda>_result',
            '<lambda>_result_0',
            '__signature__',
        ],
    )

  def test_class_method_does_not_work(self):
    class Helper:

      @tracing_decorator.TraceAsFnDecorator()
      def f(self, x):
        return x + 1

    def my_fn(x):
      return Helper().f(x)

    self.assertEqual(my_fn(1), 2)
    # This fails because it tries to auto-box the Helper class instance into
    # an expr, which is not supported.
    with self.assertRaisesRegex(ValueError, 'Failed to trace the function'):
      _ = functor_factories.trace_py_fn(my_fn)

  def test_name_override(self):
    @tracing_decorator.TraceAsFnDecorator(name='foo')
    def f(x):
      return x + 1

    fn = functor_factories.trace_py_fn(lambda x: f(x=x + 2))
    testing.assert_equal(fn(x=1), ds(4))
    testing.assert_equal(fn.foo(x=1), ds(2))
    self.assertNotIn('f', fn.get_attr_names(intersection=True))

  def test_py_fn_mode(self):
    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def f(x):
      if x == 0:
        return 1
      return x * f(x - 1)

    fn = functor_factories.trace_py_fn(lambda x: f(x=x + 2))
    testing.assert_equal(fn(x=1), ds(6))
    testing.assert_equal(fn.f(x=1), ds(1))

  def test_two_calls(self):
    @tracing_decorator.TraceAsFnDecorator()
    def f(x):
      return x + 1

    fn = functor_factories.trace_py_fn(lambda x: f(x=x + 2) * f(x=x + 3))
    testing.assert_equal(fn(x=1), ds(20))
    testing.assert_equal(fn.f(x=1), ds(2))
    # Make sure we have only one copy of 'f' but two versions of 'f_result'.
    self.assertCountEqual(
        fn.get_attr_names(intersection=True),
        ['returns', 'f', 'f_result', 'f_result_0', '__signature__'],
    )

  def test_return_type_as(self):
    @tracing_decorator.TraceAsFnDecorator(return_type_as=data_bag.DataBag)
    def f(x):
      return user_facing_kd.uu(seed='test').with_attrs(x=x).get_bag()

    fn = functor_factories.trace_py_fn(
        lambda: user_facing_kd.uu(seed='test').with_bag(f(5)).x
    )
    testing.assert_equal(fn().no_bag(), ds(5))

  def test_return_type_as_py_fn(self):
    @tracing_decorator.TraceAsFnDecorator(
        return_type_as=data_bag.DataBag, py_fn=True
    )
    def f(x):
      return user_facing_kd.uu(seed='test').with_attrs(x=x).get_bag()

    fn = functor_factories.trace_py_fn(
        lambda: user_facing_kd.uu(seed='test').with_bag(f(5)).x
    )
    testing.assert_equal(fn().no_bag(), ds(5))

  def test_return_type_as_errors(self):
    @tracing_decorator.TraceAsFnDecorator()
    def f(x):
      return user_facing_kd.uu(seed='test').with_attrs(x=x).get_bag()

    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'The function [f] annotated with @kd.trace_as_fn() was expected to'
        ' return `DATA_SLICE` as the output type, but the computation resulted'
        ' in type `DATA_BAG` instead. Consider adding or updating'
        ' return_type_as= argument to @kd.trace_as_fn().',
    ):
      f(5)

    fn = functor_factories.trace_py_fn(f)
    with self.assertRaisesRegex(
        ValueError,
        'the functor was called with `DATA_SLICE` as the output type, but the'
        ' computation resulted in type `DATA_BAG` instead',
    ):
      fn(5)

  def test_result_boxing(self):
    @tracing_decorator.TraceAsFnDecorator()
    def f(x):
      return x + 1

    res = f(5)
    self.assertIsInstance(res, data_item.DataItem)
    testing.assert_equal(res, ds(6))

  def test_output_structure_when_used_with_kd_updated(self):
    empty_bag = data_bag.DataBag.empty()

    @tracing_decorator.TraceAsFnDecorator(return_type_as=empty_bag)
    def f(x):
      return user_facing_kd.attrs(x, foo=1)

    x = fns.new(bar=2)

    fn = functor_factories.trace_py_fn(lambda x: x.updated(f(x)))
    self.assertEqual(fn(x).to_pytree(), {'foo': 1, 'bar': 2})
    testing.assert_non_deterministic_exprs_equal(
        introspection.unpack_expr(fn.returns),
        I.x.updated(V.f_result),
    )
    testing.assert_non_deterministic_exprs_equal(
        introspection.unpack_expr(fn.f_result),
        V.f(I.x, return_type_as=empty_bag),
    )
    testing.assert_equal(
        introspection.unpack_expr(fn.f.returns),
        kde.attrs(I.x, foo=1),
    )

  @parameterized.parameters(True, False)
  def test_wrapper(self, py_fn):
    @tracing_decorator.TraceAsFnDecorator(
        py_fn=py_fn, wrapper=lambda func: lambda x: func(x + 5)
    )
    def f(x):
      return x + 1

    fn = functor_factories.trace_py_fn(f)

    testing.assert_equal(f(ds([1, 2])), ds([2, 3]))
    testing.assert_equal(fn(ds([1, 2])), ds([7, 8]))


if __name__ == '__main__':
  absltest.main()
