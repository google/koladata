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

import threading

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.expr import expr_eval
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.functor import tracing_decorator
from koladata.operators import bootstrap
from koladata.operators import koda_internal_parallel
from koladata.operators import optools
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_slice

ds = data_slice.DataSlice.from_vals


class KodaInternalParallelGetDefaultExecutionContextTest(
    parameterized.TestCase
):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.get_default_execution_context,
        [
            (arolla.eval(bootstrap.get_execution_context_qtype()),),
        ],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.get_default_execution_context()
        )
    )

  def test_basic(self):
    expr = koda_internal_parallel.get_default_execution_context()
    res = expr.eval()
    testing.assert_equal(
        res.qtype, arolla.eval(bootstrap.get_execution_context_qtype())
    )
    testing.assert_equal(
        koda_internal_parallel.get_executor_from_context(res).eval(),
        arolla.eval(koda_internal_parallel.get_default_executor()),
    )

  @parameterized.named_parameters(
      (
          'arolla_tuple',
          arolla.M.core.make_tuple,
          lambda x: arolla.M.core.get_nth(x, 0),
          lambda x: arolla.M.core.get_nth(x, 1),
      ),
      (
          'koda_tuple',
          lambda x, y: user_facing_kd.make_tuple(x, y),  # pylint: disable=unnecessary-lambda
          lambda x: user_facing_kd.tuple.get_nth(x, 0),
          lambda x: user_facing_kd.tuple.get_nth(x, 1),
      ),
      ('python_tuple', lambda x, y: (x, y), lambda x: x[0], lambda x: x[1]),
      (
          'arolla_namedtuple',
          lambda x, y: arolla.M.namedtuple.make(a=x, b=y),
          lambda x: arolla.M.namedtuple.get_field(x, 'a'),
          lambda x: arolla.M.namedtuple.get_field(x, 'b'),
      ),
      (
          'koda_namedtuple',
          lambda x, y: user_facing_kd.make_namedtuple(a=x, b=y),
          lambda x: user_facing_kd.tuple.get_namedtuple_field(x, 'a'),
          lambda x: user_facing_kd.tuple.get_namedtuple_field(x, 'b'),
      ),
      (
          'koda_namedtuple_with_python_access',
          lambda x, y: user_facing_kd.make_namedtuple(a=x, b=y),
          lambda x: x['a'],
          lambda x: x['b'],
      ),
  )
  def test_call_and_tuple_and_namedtuple(
      self, make_tuple_fn, get_first_fn, get_second_fn
  ):
    e1 = threading.Event()
    e2 = threading.Event()

    @optools.as_py_function_operator(
        name='aux',
    )
    def wait_and_return_1():
      self.assertTrue(e1.wait(timeout=5.0))
      return ds(1)

    @optools.as_py_function_operator(
        name='aux',
    )
    def wait_and_return_2():
      self.assertTrue(e2.wait(timeout=5.0))
      return ds(2)

    @tracing_decorator.TraceAsFnDecorator(
        return_type_as=expr_eval.eval(make_tuple_fn(ds(0), ds(0))),
    )
    def f(x, y):
      return make_tuple_fn(x, y)

    def g(x, y):
      res = f(x, y)
      return make_tuple_fn(get_first_fn(res), get_second_fn(res))

    fn = functor_factories.trace_py_fn(g)
    context = koda_internal_parallel.get_default_execution_context().eval()
    executor = koda_internal_parallel.get_executor_from_context(context).eval()
    transformed_fn = koda_internal_parallel.transform(context, fn)
    future_1 = expr_eval.eval(
        koda_internal_parallel.async_eval(executor, wait_and_return_1)
    )
    future_2 = expr_eval.eval(
        koda_internal_parallel.async_eval(executor, wait_and_return_2)
    )
    future_none = expr_eval.eval(koda_internal_parallel.as_future(None))
    res = transformed_fn(
        x=future_1,
        y=future_2,
        return_type_as=make_tuple_fn(future_none, future_none),
    ).eval()

    # Make sure the tuple elements can be evaluated independently, with
    # no barrier to sync.
    first_as_stream = koda_internal_parallel.stream_from_future(
        # TODO: Invoke get_first_fn directly.
        functor_factories.trace_py_fn(get_first_fn)(
            res, return_type_as=future_none
        )
    ).eval()
    second_as_stream = koda_internal_parallel.stream_from_future(
        # TODO: Invoke get_second_fn directly.
        functor_factories.trace_py_fn(get_second_fn)(
            res, return_type_as=future_none
        )
    ).eval()
    # unlock get_second_fn while get_first_fn is still waiting.
    e2.set()
    testing.assert_equal(second_as_stream.read_all(timeout=5.0)[0], ds(2))
    e1.set()
    testing.assert_equal(first_as_stream.read_all(timeout=5.0)[0], ds(1))

  def test_non_deterministic_token_handling(self):
    fn = functor_factories.expr_fn(optools.unified_non_deterministic_arg())
    context = koda_internal_parallel.get_default_execution_context()
    transformed_fn = koda_internal_parallel.transform(context, fn)
    res = transformed_fn(
        return_type_as=optools.unified_non_deterministic_arg()
    ).eval()
    self.assertEqual(res.qtype, qtypes.NON_DETERMINISTIC_TOKEN)
    self.assertNotIn('async_eval', str(res))
    # To make sure the assert above is meaningful.
    res_no_replacements = koda_internal_parallel.transform(
        koda_internal_parallel.create_execution_context(
            koda_internal_parallel.get_default_executor(), None
        ),
        fn,
    ).eval()
    self.assertIn('async_eval', str(res_no_replacements))

  def test_get_item_on_dicts(self):

    def f(x, y):
      return x[y]

    x = fns.dict({1: 2, 3: 4})
    y = ds([1, 2, 3])

    fn = functor_factories.trace_py_fn(f)
    context = koda_internal_parallel.get_default_execution_context().eval()
    transformed_fn = koda_internal_parallel.transform(context, fn)
    res = transformed_fn(
        x=koda_internal_parallel.as_future(x),
        y=koda_internal_parallel.as_future(y),
        return_type_as=koda_internal_parallel.as_future(None),
    ).eval()

    res_as_stream = koda_internal_parallel.stream_from_future(res).eval()
    testing.assert_equal(
        res_as_stream.read_all(timeout=5.0)[0],
        ds([2, None, 4]).with_bag(x.get_bag()),
    )

  def test_get_item_on_dicts_with_literal_key(self):

    def f(x):
      return x[3]

    x = fns.dict({1: 2, 3: 4})

    fn = functor_factories.trace_py_fn(f)
    context = koda_internal_parallel.get_default_execution_context().eval()
    transformed_fn = koda_internal_parallel.transform(context, fn)
    res = transformed_fn(
        x=koda_internal_parallel.as_future(x),
        return_type_as=koda_internal_parallel.as_future(None),
    ).eval()

    res_as_stream = koda_internal_parallel.stream_from_future(res).eval()
    testing.assert_equal(
        res_as_stream.read_all(timeout=5.0)[0],
        ds(4).with_bag(x.get_bag()),
    )


if __name__ == '__main__':
  absltest.main()
