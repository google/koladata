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

import dataclasses
import re
from typing import Any, ClassVar, Self

from absl.testing import absltest
from absl.testing import parameterized
from IPython.core import ultratb
from koladata import kd as user_facing_kd
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.functions import functions as fns
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.functor import tracing_decorator
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice


I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
ds = data_slice.DataSlice.from_vals
kd_eager = eager_op_utils.operators_container('kd')
kd_lazy = kde_operators.kde


class TuplePairTracingConfig(tracing_decorator.TypeTracingConfig):
  """A type tracing config for Pair using tuples."""

  def return_type_as(self, annotation: type['PairWithTupleTracing']) -> Any:
    # This is never called in tracing mode, but using user_facing_kd here
    # to emulate what will happen in real user code.
    # This is auto-boxed to a Koda tuple.
    return data_slice.DataSlice, data_slice.DataSlice

  def to_kd(
      self,
      annotation: type['PairWithTupleTracing'],
      value: 'PairWithTupleTracing',
  ) -> Any:
    # This is auto-boxed to a Koda tuple.
    return value.x, value.y

  def from_kd(
      self, annotation: type['PairWithTupleTracing'], value: Any
  ) -> 'PairWithTupleTracing':
    return annotation(x=value[0], y=value[1])


# We demonstrate two main ways to define custom tracing behavior for composite
# types: using tuples and using entities.
# The benefit of using tuples is avoiding any additional conversions.
# The benefit of using entities is that those can be stored in a DataBag,
# for example as default values for a functor.
@dataclasses.dataclass(frozen=True)
class PairWithTupleTracing:
  x: data_slice.DataSlice | user_facing_kd.types.Expr
  y: data_slice.DataSlice | user_facing_kd.types.Expr

  def __add__(self, other: Any) -> Self:
    return PairWithTupleTracing(x=self.x + other, y=self.y + other)

  _koladata_type_tracing_config_: ClassVar[type[TuplePairTracingConfig]] = (
      TuplePairTracingConfig
  )


class EntityPairTracingConfig(tracing_decorator.TypeTracingConfig):
  """A type tracing config for Pair using entities."""

  def return_type_as(self, annotation: type['PairWithEntityTracing']) -> Any:
    return data_slice.DataSlice

  def to_kd(
      self,
      annotation: type['PairWithEntityTracing'],
      value: 'PairWithEntityTracing',
  ) -> Any:
    return user_facing_kd.new(
        x=user_facing_kd.implode(value.x, ndim=value.x.get_ndim()),
        x_ndim=value.x.get_ndim(),
        y=user_facing_kd.implode(value.y, ndim=value.y.get_ndim()),
        y_ndim=value.y.get_ndim(),
        schema='_testing_pair',
    )

  def from_kd(
      self, annotation: type['PairWithEntityTracing'], value: Any
  ) -> 'PairWithEntityTracing':
    return annotation(
        x=user_facing_kd.explode(value.x, ndim=value.x_ndim),
        y=user_facing_kd.explode(value.y, ndim=value.y_ndim),
    )


@dataclasses.dataclass(frozen=True)
class PairWithEntityTracing:
  x: data_slice.DataSlice | user_facing_kd.types.Expr
  y: data_slice.DataSlice | user_facing_kd.types.Expr

  _koladata_type_tracing_config_: ClassVar[type[EntityPairTracingConfig]] = (
      EntityPairTracingConfig
  )


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
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(fn.f.returns), I.x + 1
    )

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
            '_<lambda>_result',
            '_<lambda>_result_0',
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

    # This fails because it tries to auto-box the Helper class instance into
    # a Koda value, which is not supported.
    with self.assertRaisesRegex(
        ValueError, 'object with unsupported type: Helper'
    ):
      my_fn(1)

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
        [
            'returns',
            'f',
            '_f_result',
            '_f_result_0',
            '__signature__',
        ],
    )

  def test_return_type_as(self):
    @tracing_decorator.TraceAsFnDecorator(return_type_as=data_bag.DataBag)
    def f(x):
      return user_facing_kd.uu(seed='test').with_attrs(x=x).get_bag()

    fn = functor_factories.trace_py_fn(
        lambda: user_facing_kd.uu(seed='test').with_bag(f(5)).x
    )
    testing.assert_equal(fn().no_bag(), ds(5))

  def test_return_type_as_from_type_annotation(self):
    @tracing_decorator.TraceAsFnDecorator()
    def f(x) -> data_bag.DataBag:
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
        re.escape(
            'The functor was called with `DATA_SLICE` as the output type, but'
            ' the computation resulted in type `DATA_BAG` instead. You can'
            ' specify the expected output type via the `return_type_as=`'
            ' parameter to the functor call.'
        ),
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
    empty_bag = data_bag.DataBag.empty().freeze()

    @tracing_decorator.TraceAsFnDecorator(return_type_as=empty_bag)
    def f(x):
      return user_facing_kd.attrs(x, foo=1)

    x = fns.new(bar=2)

    fn = functor_factories.trace_py_fn(lambda x: x.updated(f(x)))
    self.assertEqual(fn(x).to_pytree(), {'foo': 1, 'bar': 2})
    testing.assert_traced_non_deterministic_exprs_equal(
        introspection.unpack_expr(fn.returns),
        I.x.updated(V._f_result),
    )
    testing.assert_traced_non_deterministic_exprs_equal(
        introspection.unpack_expr(fn.get_attr('_f_result')),
        V.f(I.x, return_type_as=empty_bag),
    )
    testing.assert_traced_non_deterministic_exprs_equal(
        introspection.unpack_expr(fn.f.returns),
        kd_lazy.attrs(I.x, foo=1),
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

  def test_custom_tracing_config_tuple_tracing(self):

    @tracing_decorator.TraceAsFnDecorator()
    def swap(a: PairWithTupleTracing) -> PairWithTupleTracing:
      return PairWithTupleTracing(x=a.y, y=a.x)

    def f(o):
      p = PairWithTupleTracing(x=o.x, y=o.y[:])
      p = swap(p) + 3
      return user_facing_kd.obj(x=user_facing_kd.implode(p.x), y=p.y)

    res = f(fns.obj(x=ds([1, 2, 3]), y=fns.implode(ds([[4, 5], [], [6]]))))
    testing.assert_equal(res.x[:].no_bag(), ds([[7, 8], [], [9]]))
    testing.assert_equal(res.y.no_bag(), ds([4, 5, 6]))

    fn = functor_factories.trace_py_fn(f)
    res = fn(fns.obj(x=ds([1, 2, 3]), y=fns.implode(ds([[4, 5], [], [6]]))))
    testing.assert_equal(res.x[:].no_bag(), ds([[7, 8], [], [9]]))
    testing.assert_equal(res.y.no_bag(), ds([4, 5, 6]))

  def test_custom_tracing_config_entity_tracing(self):

    @tracing_decorator.TraceAsFnDecorator()
    def swap(a: PairWithEntityTracing) -> PairWithEntityTracing:
      return PairWithEntityTracing(x=a.y, y=a.x)

    def f(o):
      p = PairWithEntityTracing(x=o.x, y=o.y[:])
      p = swap(p)
      return user_facing_kd.obj(x=user_facing_kd.implode(p.x), y=p.y)

    fn = functor_factories.trace_py_fn(f)
    res = fn(fns.obj(x=ds([1, 2, 3]), y=fns.implode(ds([[4, 5], [], [6]]))))
    testing.assert_equal(res.x[:].no_bag(), ds([[4, 5], [], [6]]))
    testing.assert_equal(res.y.no_bag(), ds([1, 2, 3]))

  def test_custom_tracing_config_with_py_fn(self):

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def swap(a: PairWithTupleTracing) -> PairWithTupleTracing:
      return PairWithTupleTracing(x=a.y, y=a.x)

    def f(o):
      p = PairWithTupleTracing(x=o.x, y=o.y)
      p = swap(p)
      return user_facing_kd.obj(x=p.x, y=p.y)

    fn = functor_factories.trace_py_fn(f)
    res = fn(fns.obj(x=ds(1), y=ds(2)))
    testing.assert_equal(res.x.no_bag(), ds(2))
    testing.assert_equal(res.y.no_bag(), ds(1))

  def test_custom_tracing_config_default_values(self):

    @tracing_decorator.TraceAsFnDecorator()
    def pair_add(
        a: PairWithEntityTracing,
        b: PairWithEntityTracing = PairWithEntityTracing(x=ds(5), y=ds(7)),
    ) -> PairWithEntityTracing:
      return PairWithEntityTracing(x=a.x + b.x, y=a.y + b.y)

    def f(o):
      p = PairWithEntityTracing(x=o.x, y=o.y)
      p = pair_add(p)
      p = pair_add(p, p)
      return user_facing_kd.obj(x=p.x, y=p.y)

    res = f(fns.obj(x=ds(1), y=ds(2)))
    testing.assert_equal(res.x.no_bag(), ds((1 + 5) * 2))
    testing.assert_equal(res.y.no_bag(), ds((2 + 7) * 2))

    fn = functor_factories.trace_py_fn(f)
    res = fn(fns.obj(x=ds(1), y=ds(2)))
    testing.assert_equal(res.x.no_bag(), ds((1 + 5) * 2))
    testing.assert_equal(res.y.no_bag(), ds((2 + 7) * 2))

    testing.assert_equal(
        functor_factories.get_signature(fn.pair_add)
        .parameters[1]
        .name.no_bag(),
        ds('b'),
    )

    # Check that the default values from the Koda signature are respected.
    fn = fn.updated(
        kd_eager.attrs(
            functor_factories.get_signature(fn.pair_add).parameters[1],
            default_value=EntityPairTracingConfig().to_kd(
                PairWithEntityTracing, PairWithEntityTracing(x=ds(3), y=ds(4))
            ),
        ),
    )
    res = fn(fns.obj(x=ds(1), y=ds(2)))
    testing.assert_equal(res.x.no_bag(), ds((1 + 3) * 2))
    testing.assert_equal(res.y.no_bag(), ds((2 + 4) * 2))

  def test_custom_tracing_config_default_values_does_not_work_with_tuples(self):

    with self.assertRaisesRegex(
        ValueError,
        'only DataItems can be used as default values',
    ):

      @tracing_decorator.TraceAsFnDecorator()
      def pair_add(
          a: PairWithTupleTracing,
          b: PairWithTupleTracing = PairWithTupleTracing(x=ds(5), y=ds(7)),
      ) -> PairWithTupleTracing:
        return PairWithTupleTracing(x=a.x + b.x, y=a.y + b.y)

      del pair_add  # Unused.

  def test_can_pass_unwrapped_lambdas(self):

    @tracing_decorator.TraceAsFnDecorator()
    def f(x, fltr):
      return user_facing_kd.select(x, fltr)

    testing.assert_equal(f(ds([1, 2, 3]), lambda x: x % 2 == 0), ds([2]))

  def test_cannot_return_expr_in_eager_mode(self):

    @tracing_decorator.TraceAsFnDecorator()
    def f(x):  # pylint: disable=unused-argument
      return I.x + 1

    with self.assertRaisesRegex(
        ValueError, 'computation returned an Expr instead'
    ):
      _ = f(5)

  def test_functor_wrong_arg_count_traceback(self):
    @tracing_decorator.TraceAsFnDecorator()
    def f(x, y):
      return x // y

    f = functor_factories.fn(f)

    try:
      f(0)
    except ValueError as e:
      ex = e

    formatted_message = '\n'.join(
        ultratb.VerboseTB(
            color_scheme='NoColor', include_vars=False
        ).structured_traceback(type(ex), ex, ex.__traceback__)
    )
    self.assertNotIn('/tracing_decorator.py', formatted_message)
    self.assertIn('/tracing_decorator_test.py', formatted_message)
    self.assertIn('f(0)', formatted_message)

  def test_functor_call_traceback(self):
    @tracing_decorator.TraceAsFnDecorator()
    def foo(x):
      return 1 // x

    @tracing_decorator.TraceAsFnDecorator()
    def bar(x):
      return 1 // foo(x)

    @tracing_decorator.TraceAsFnDecorator()
    def baz(x):
      return 1 // bar(x)

    baz = functor_factories.fn(baz)

    try:
      baz(0)
    except ValueError as e:
      ex = e

    self.assertEqual(str(ex), 'kd.math.floordiv: division by zero')

    tb = '\n'.join(
        ultratb.VerboseTB(
            color_scheme='NoColor', include_vars=False
        ).structured_traceback(type(ex), ex, ex.__traceback__)
    )

    self.assertNotIn('/tracing.py', tb)
    self.assertNotIn('/functor_factories.py', tb)
    self.assertNotIn('/tracing_decorator.py', tb)
    self.assertNotIn('/stack_trace.py', tb)
    self.assertIn('baz(0)', tb)
    self.assertRegex(
        tb, 'tracing_decorator_test.py.*test_functor_call_traceback'
    )
    self.assertIn('baz = functor_factories.fn(baz)', tb)
    self.assertRegex(tb, 'tracing_decorator_test.py.*baz')
    self.assertIn('return 1 // bar(x)', tb)
    self.assertRegex(tb, 'tracing_decorator_test.py.*bar')
    self.assertIn('return 1 // foo(x)', tb)
    self.assertRegex(tb, 'tracing_decorator_test.py.*foo')
    self.assertIn('return 1 // x', tb)

  def test_autoboxing_functor_call_traceback(self):
    @tracing_decorator.TraceAsFnDecorator()
    def foo(x):
      return user_facing_kd.if_(
          x > 5,
          lambda a: a + 1,
          lambda a: 1 // a,
          x,
      )

    try:
      foo(0)
    except ValueError as e:
      ex = e

    self.assertEqual(str(ex), 'kd.math.floordiv: division by zero')

    tb = '\n'.join(
        ultratb.VerboseTB(
            color_scheme='NoColor', include_vars=False
        ).structured_traceback(type(ex), ex, ex.__traceback__)
    )

    self.assertNotIn('/tracing.py', tb)
    self.assertNotIn('/functor_factories.py', tb)
    self.assertNotIn('/tracing_decorator.py', tb)
    self.assertNotIn('/py_boxing.py', tb)
    self.assertNotIn('/stack_trace.py', tb)
    self.assertRegex(
        tb, 'tracing_decorator_test.py.*test_autoboxing_functor_call_traceback'
    )
    self.assertIn('return user_facing_kd.if_(', tb)
    self.assertIn('lambda a: 1 // a,', tb)


if __name__ == '__main__':
  absltest.main()
