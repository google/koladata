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

"""Tests for tracing_decorator."""

from absl.testing import absltest
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.functor import functor_factories
from koladata.functor import tracing_decorator
from koladata.testing import testing
from koladata.types import data_slice

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals


class TracingDecoratorTest(absltest.TestCase):

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
        dir(fn),
        ['returns', '<lambda>', '<lambda>_0', '__signature__'],
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
    self.assertNotIn('f', dir(fn))

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
    # Make sure we have only one copy of 'f'.
    self.assertCountEqual(dir(fn), ['returns', 'f', '__signature__'])

  # TODO: Reenable this when we have kd.attrs.
  # def test_output_structure_when_used_with_kd_updated(self):
  #   @tracing_decorator.TraceAsFnDecorator()
  #   def f(x):
  #     return user_facing_kd.attrs(x, foo=1)

  #   x = fns.new(bar=2)

  #   fn = functor_factories.trace_py_fn(lambda x: x.updated(f))
  #   self.assertEqual(fn(x).to_pytree(), {'foo': 1, 'bar': 2})
  #   testing.assert_equal(
  #       introspection.unpack_expr(fn.returns),
  #       I.x.updated(V.f(I.x)),
  #   )
  #   testing.assert_equal(
  #       introspection.unpack_expr(fn.f.returns),
  #       kde.attrs(I.x, foo=1),
  #   )


if __name__ == '__main__':
  absltest.main()
