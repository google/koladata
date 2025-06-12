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
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.functor import stack_trace
from koladata.functor.parallel import clib as _
from koladata.operators import koda_internal_parallel
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice

from koladata.functor.parallel import execution_config_pb2

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
S = I.self
ds = data_slice.DataSlice.from_vals


_REPLACEMENTS = fns.new(
    operator_replacements=[
        fns.new(
            from_op='kd.functor.call',
            to_op='koda_internal.parallel.parallel_call',
            argument_transformation=fns.new(
                arguments=[
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
    context = koda_internal_parallel.create_execution_context(
        executor, _REPLACEMENTS
    )
    fn = functor_factories.expr_fn(I.x + I.y)
    call_expr = koda_internal_parallel.parallel_call(
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
    context = koda_internal_parallel.create_execution_context(executor, None)
    fn = functor_factories.expr_fn(I.x + I.y)
    call_expr = koda_internal_parallel.parallel_call(
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

    executor = koda_internal_parallel.make_executor(thread_limit=2)
    context = koda_internal_parallel.create_execution_context(executor, None)
    fn = functor_factories.expr_fn(V.x + V.y, x=my_op(I.x), y=my_op(I.y))
    call_expr = koda_internal_parallel.parallel_call(
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
    context = koda_internal_parallel.create_execution_context(
        executor, _REPLACEMENTS
    )
    inner_fn = functor_factories.expr_fn(I.self + I.other)
    fn = functor_factories.expr_fn(
        V.inner(I.x, other=I.y) + V.inner(I.y, other=I.x), inner=inner_fn
    )
    call_expr = koda_internal_parallel.parallel_call(
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

    executor = koda_internal_parallel.make_executor(thread_limit=3)
    context = koda_internal_parallel.create_execution_context(
        executor, _REPLACEMENTS
    )
    inner_fn = functor_factories.expr_fn(V.x + V.y, x=my_op(I.x), y=my_op(I.y))
    fn = functor_factories.expr_fn(
        V.x + V.inner(x=I.y, y=I.y), x=my_op(I.x), inner=inner_fn
    )
    call_expr = koda_internal_parallel.parallel_call(
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
    context = koda_internal_parallel.create_execution_context(
        executor, _REPLACEMENTS
    )
    fn = functor_factories.expr_fn(S.x + S.y)
    call_expr = koda_internal_parallel.parallel_call(
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

  def test_stack_trace_frame(self):
    executor = koda_internal_parallel.get_eager_executor()
    context = koda_internal_parallel.create_execution_context(
        executor, _REPLACEMENTS
    )
    fn = functor_factories.expr_fn(S.x)
    stack_frame = stack_trace.create_stack_trace_frame(
        function_name='test_function',
        file_name='test_file.py',
        line_number=57,
        line_text='x = y // 0',
    )
    call_expr = koda_internal_parallel.parallel_call(
        context,
        koda_internal_parallel.as_future(fn),
        koda_internal_parallel.as_future(I.foo),
        stack_trace_frame=koda_internal_parallel.as_future(stack_frame),
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
    context = koda_internal_parallel.create_execution_context(executor, None)
    fn = functor_factories.expr_fn(S)
    call_expr = koda_internal_parallel.parallel_call(
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
    context = koda_internal_parallel.create_execution_context(
        executor, _REPLACEMENTS
    )
    inner_fn = functor_factories.expr_fn(S)
    fn = functor_factories.expr_fn(
        V.inner(S, return_type_as=data_bag.DataBag), inner=inner_fn
    )
    call_expr = koda_internal_parallel.parallel_call(
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

  def test_wrong_input_types(self):
    executor = koda_internal_parallel.get_eager_executor()
    context = koda_internal_parallel.create_execution_context(
        executor, _REPLACEMENTS
    )
    fn = functor_factories.expr_fn(S)

    with self.assertRaisesRegex(
        ValueError, 'expected a future to a data slice'
    ):
      _ = koda_internal_parallel.parallel_call(context, fn)

    with self.assertRaisesRegex(ValueError, 'args must have parallel types'):
      _ = koda_internal_parallel.parallel_call(
          context,
          koda_internal_parallel.as_future(fn),
          57,
      )

    with self.assertRaisesRegex(ValueError, 'kwargs must have parallel types'):
      _ = koda_internal_parallel.parallel_call(
          context,
          koda_internal_parallel.as_future(fn),
          x=57,
      )

    with self.assertRaisesRegex(
        ValueError, 'expected a future to a data slice'
    ):
      _ = koda_internal_parallel.parallel_call(
          context,
          koda_internal_parallel.as_future(fn),
          stack_trace_frame=None,
      )

    with self.assertRaisesRegex(ValueError, 'expected an execution context'):
      _ = koda_internal_parallel.parallel_call(
          None,
          koda_internal_parallel.as_future(fn),
      )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.parallel_call(I.context, I.fn)
        )
    )


if __name__ == '__main__':
  absltest.main()
