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

import threading
import traceback

from absl.testing import absltest
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.functor.parallel import clib as _
from koladata.operators import iterables
from koladata.operators import kde_operators
from koladata.operators import koda_internal_parallel
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import iterable_qvalue

from koladata.functor.parallel import execution_config_pb2


I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
S = I.self
ds = data_slice.DataSlice.from_vals
kd_lazy = kde_operators.kde


_REPLACEMENTS = fns.new(
    operator_replacements=[
        fns.new(
            from_op='kd.functor.call',
            to_op='koda_internal.parallel.parallel_call',
            argument_transformation=fns.new(
                arguments=[
                    execution_config_pb2.ExecutionConfig.ArgumentTransformation.EXECUTOR,
                    execution_config_pb2.ExecutionConfig.ArgumentTransformation.EXECUTION_CONTEXT,
                    execution_config_pb2.ExecutionConfig.ArgumentTransformation.ORIGINAL_ARGUMENTS,
                ],
            ),
        ),
    ],
)


class ParallelCallTest(absltest.TestCase):

  def test_simple(self):
    executor = koda_internal_parallel.get_eager_executor()
    context = koda_internal_parallel.create_execution_context(_REPLACEMENTS)
    fn = functor_factories.expr_fn(I.x + I.y)
    call_expr = koda_internal_parallel.parallel_call(
        executor,
        context,
        koda_internal_parallel.as_future(fn),
        x=koda_internal_parallel.as_future(I.foo),
        y=koda_internal_parallel.as_future(I.bar),
    )
    testing.assert_equal(
        koda_internal_parallel.get_future_value_for_testing(call_expr).eval(
            foo=1, bar=2
        ),
        ds(3),
    )

  def test_simple_no_replacements(self):
    executor = koda_internal_parallel.get_eager_executor()
    context = koda_internal_parallel.create_execution_context(None)
    fn = functor_factories.expr_fn(I.x + I.y)
    call_expr = koda_internal_parallel.parallel_call(
        executor,
        context,
        koda_internal_parallel.as_future(fn),
        x=koda_internal_parallel.as_future(I.foo),
        y=koda_internal_parallel.as_future(I.bar),
    )
    testing.assert_equal(
        koda_internal_parallel.get_future_value_for_testing(call_expr).eval(
            foo=1, bar=2
        ),
        ds(3),
    )

  def test_parallel_execution_of_variables(self):
    barrier = threading.Barrier(2)

    @optools.as_py_function_operator('aux')
    def my_op(x):
      barrier.wait()
      return x

    executor = expr_eval.eval(
        koda_internal_parallel.make_executor(thread_limit=2)
    )
    context = koda_internal_parallel.create_execution_context(None)
    fn = functor_factories.expr_fn(V.x + V.y, x=my_op(I.x), y=my_op(I.y))
    call_expr = koda_internal_parallel.parallel_call(
        executor,
        context,
        koda_internal_parallel.as_future(fn),
        x=koda_internal_parallel.as_future(I.foo),
        y=koda_internal_parallel.as_future(I.bar),
    )
    testing.assert_equal(
        koda_internal_parallel.stream_from_future(call_expr)
        .eval(foo=1, bar=2)
        .read_all(timeout=5.0)[0],
        ds(3),
    )

  def test_nested(self):
    executor = koda_internal_parallel.get_eager_executor()
    context = koda_internal_parallel.create_execution_context(_REPLACEMENTS)
    inner_fn = functor_factories.expr_fn(I.self + I.other)
    fn = functor_factories.expr_fn(
        V.inner(I.x, other=I.y) + V.inner(I.y, other=I.x), inner=inner_fn
    )
    call_expr = koda_internal_parallel.parallel_call(
        executor,
        context,
        koda_internal_parallel.as_future(fn),
        x=koda_internal_parallel.as_future(I.foo),
        y=koda_internal_parallel.as_future(I.bar),
    )
    testing.assert_equal(
        koda_internal_parallel.get_future_value_for_testing(call_expr).eval(
            foo=1, bar=2
        ),
        ds(6),
    )

  def test_parallel_execution_of_nested_variables(self):
    barrier = threading.Barrier(3)

    @optools.as_py_function_operator('aux')
    def my_op(x):
      barrier.wait()
      return x

    executor = expr_eval.eval(
        koda_internal_parallel.make_executor(thread_limit=3)
    )
    context = koda_internal_parallel.create_execution_context(_REPLACEMENTS)
    inner_fn = functor_factories.expr_fn(V.x + V.y, x=my_op(I.x), y=my_op(I.y))
    fn = functor_factories.expr_fn(
        V.x + V.inner(x=I.y, y=I.y), x=my_op(I.x), inner=inner_fn
    )
    call_expr = koda_internal_parallel.parallel_call(
        executor,
        context,
        koda_internal_parallel.as_future(fn),
        x=koda_internal_parallel.as_future(I.foo),
        y=koda_internal_parallel.as_future(I.bar),
    )
    testing.assert_equal(
        koda_internal_parallel.stream_from_future(call_expr)
        .eval(foo=1, bar=2)
        .read_all(timeout=5.0)[0],
        ds(5),
    )

  def test_positional_arg(self):
    executor = koda_internal_parallel.get_eager_executor()
    context = koda_internal_parallel.create_execution_context(_REPLACEMENTS)
    fn = functor_factories.expr_fn(S.x + S.y)
    call_expr = koda_internal_parallel.parallel_call(
        executor,
        context,
        koda_internal_parallel.as_future(fn),
        koda_internal_parallel.as_future(I.foo),
    )
    testing.assert_equal(
        koda_internal_parallel.get_future_value_for_testing(call_expr).eval(
            foo=fns.new(x=1, y=2)
        ),
        ds(3),
    )

  def test_source_location(self):
    executor = koda_internal_parallel.get_eager_executor()
    context = koda_internal_parallel.create_execution_context(_REPLACEMENTS)
    fn = functor_factories.expr_fn(
        kd_lazy.annotation.source_location(
            S.x, 'test_function', 'test_file.py', 57, 0, '  return S.x'
        ),
    )
    call_expr = koda_internal_parallel.parallel_call(
        executor,
        context,
        koda_internal_parallel.as_future(fn),
        koda_internal_parallel.as_future(I.foo),
    )
    try:
      _ = koda_internal_parallel.get_future_value_for_testing(call_expr).eval(
          foo=fns.new(y=2)
      )
      self.fail('Expected ValueError')
    except ValueError as e:
      ex = e

    self.assertIn("failed to get attribute 'x'", str(ex))
    tb = '\n'.join(traceback.format_tb(ex.__traceback__))
    # TODO: Make this work.
    self.assertNotIn('File "test_file.py", line 57, in test_function', tb)

  def test_return_type_as(self):
    executor = koda_internal_parallel.get_eager_executor()
    context = koda_internal_parallel.create_execution_context(None)
    fn = functor_factories.expr_fn(S)
    call_expr = koda_internal_parallel.parallel_call(
        executor,
        context,
        koda_internal_parallel.as_future(fn),
        koda_internal_parallel.as_future(I.foo),
        return_type_as=koda_internal_parallel.as_future(data_bag.DataBag),
    )
    db = data_bag.DataBag.empty().freeze()
    testing.assert_equal(
        koda_internal_parallel.get_future_value_for_testing(call_expr).eval(
            foo=db
        ),
        db,
    )

  def test_nested_return_type_as(self):
    executor = koda_internal_parallel.get_eager_executor()
    context = koda_internal_parallel.create_execution_context(_REPLACEMENTS)
    inner_fn = functor_factories.expr_fn(S)
    fn = functor_factories.expr_fn(
        V.inner(S, return_type_as=data_bag.DataBag), inner=inner_fn
    )
    call_expr = koda_internal_parallel.parallel_call(
        executor,
        context,
        koda_internal_parallel.as_future(fn),
        koda_internal_parallel.as_future(I.foo),
        return_type_as=koda_internal_parallel.as_future(data_bag.DataBag),
    )
    db = data_bag.DataBag.empty().freeze()
    testing.assert_equal(
        koda_internal_parallel.get_future_value_for_testing(call_expr).eval(
            foo=db
        ),
        db,
    )

  def test_custom_op_consuming_iterable_raises(self):
    executor = koda_internal_parallel.get_eager_executor()
    context = koda_internal_parallel.create_execution_context(_REPLACEMENTS)

    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.custom_op_consuming_iterable',
    )
    def op(x):
      return iterables.reduce_concat(x, initial_value=ds([]))

    fn = functor_factories.expr_fn(op(I.x))
    testing.assert_equal(
        fn(x=iterable_qvalue.Iterable(ds([1]), ds([2]))), ds([1, 2])
    )
    call_expr = koda_internal_parallel.parallel_call(
        executor,
        context,
        koda_internal_parallel.as_future(fn),
        x=koda_internal_parallel.stream_make(1, 2),
    )
    with self.assertRaisesRegex(
        ValueError,
        'future_from_parallel can only be applied to a parallel non-stream'
        ' type',
    ):
      _ = koda_internal_parallel.get_future_value_for_testing(call_expr).eval()

  def test_custom_op_returning_iterable_works(self):
    executor = koda_internal_parallel.get_eager_executor()
    context = koda_internal_parallel.create_execution_context(_REPLACEMENTS)

    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.custom_op_returning_iterable',
    )
    def op(x):
      return iterables.make(x, x + 1)

    fn = functor_factories.expr_fn(op(I.x))
    testing.assert_equal(
        fn(x=1, return_type_as=iterable_qvalue.Iterable()),
        iterable_qvalue.Iterable(1, 2),
    )
    call_expr = koda_internal_parallel.parallel_call(
        executor,
        context,
        koda_internal_parallel.as_future(fn),
        x=koda_internal_parallel.as_future(1),
        return_type_as=koda_internal_parallel.stream_make(),
    )
    res = call_expr.eval()
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=5.0)), arolla.tuple(ds(1), ds(2))
    )

  def test_wrong_input_types(self):
    executor = koda_internal_parallel.get_eager_executor()
    context = koda_internal_parallel.create_execution_context(_REPLACEMENTS)
    fn = functor_factories.expr_fn(S)

    with self.assertRaisesRegex(
        ValueError, 'expected a future to a data slice'
    ):
      _ = koda_internal_parallel.parallel_call(executor, context, fn)

    with self.assertRaisesRegex(ValueError, 'args must have parallel types'):
      _ = koda_internal_parallel.parallel_call(
          executor,
          context,
          koda_internal_parallel.as_future(fn),
          57,
      )

    with self.assertRaisesRegex(ValueError, 'kwargs must have parallel types'):
      _ = koda_internal_parallel.parallel_call(
          executor,
          context,
          koda_internal_parallel.as_future(fn),
          x=57,
      )

    with self.assertRaisesRegex(
        ValueError, 'expected a future to a data slice'
    ):
      _ = koda_internal_parallel.parallel_call(
          executor,
          context,
          fn,
      )

    with self.assertRaisesRegex(ValueError, 'expected an execution context'):
      _ = koda_internal_parallel.parallel_call(
          executor,
          None,
          koda_internal_parallel.as_future(fn),
      )

    with self.assertRaisesRegex(ValueError, 'expected an executor'):
      _ = koda_internal_parallel.parallel_call(
          None,
          context,
          koda_internal_parallel.as_future(fn),
      )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.parallel_call(I.executor, I.context, I.fn)
        )
    )


if __name__ == '__main__':
  absltest.main()
