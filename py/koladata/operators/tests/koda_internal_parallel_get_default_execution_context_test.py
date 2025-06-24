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

import re
import threading
import time

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.functor import tracing_decorator
from koladata.functor.parallel import clib
from koladata.operators import bootstrap
from koladata.operators import eager_op_utils
from koladata.operators import iterables
from koladata.operators import koda_internal_parallel
from koladata.operators import optools
from koladata.operators import tuple as tuple_ops
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants

ds = data_slice.DataSlice.from_vals
I = input_container.InputContainer('I')
kd = eager_op_utils.operators_container('kd')


def _parallel_eval(func, *args, return_type_as=data_slice.DataSlice, **kwargs):
  f = functor_factories.trace_py_fn(func)

  transformed_fn = koda_internal_parallel.transform(
      koda_internal_parallel.get_default_execution_context(), f
  )
  res = koda_internal_parallel.stream_from_future(
      transformed_fn(
          *[koda_internal_parallel.as_future(arg) for arg in args],
          return_type_as=koda_internal_parallel.as_future(return_type_as),
          **{k: koda_internal_parallel.as_future(v) for k, v in kwargs.items()},
      )
  ).eval()
  return res.read_all(timeout=5.0)[0]


class KodaInternalParallelGetDefaultExecutionContextTest(
    parameterized.TestCase
):

  def _wait_until_n_items(self, stream, n):
    """Waits until at least n items, end-of-stream is counted as 1 item."""
    executor = expr_eval.eval(koda_internal_parallel.get_eager_executor())
    reader = stream.make_reader()
    found = 0
    while True:
      got = reader.read_available()
      if got is None:
        found += 1
        if found >= n:
          return
        self.fail(f'waiting for {n} items, but stream over after {found}')
      found += len(got)
      if found >= n:
        return
      e = threading.Event()
      reader.subscribe_once(executor, e.set)
      self.assertTrue(e.wait(timeout=5.0))

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
          lambda x, y: user_facing_kd.tuple(x, y),  # pylint: disable=unnecessary-lambda
          lambda x: user_facing_kd.tuples.get_nth(x, 0),
          lambda x: user_facing_kd.tuples.get_nth(x, 1),
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
          lambda x, y: user_facing_kd.namedtuple(a=x, b=y),
          lambda x: user_facing_kd.tuples.get_namedtuple_field(x, 'a'),
          lambda x: user_facing_kd.tuples.get_namedtuple_field(x, 'b'),
      ),
      (
          'koda_namedtuple_with_python_access',
          lambda x, y: user_facing_kd.namedtuple(a=x, b=y),
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

  @parameterized.parameters(mask_constants.missing, mask_constants.present)
  def test_if(self, branch_to_use):
    barrier = threading.Barrier(2)

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_x(x):
      barrier.wait()
      return x

    @tracing_decorator.TraceAsFnDecorator()
    def do_two_things(x):
      return wait_and_return_x(x + 1) + wait_and_return_x(x + 2)

    @tracing_decorator.TraceAsFnDecorator()
    def do_other_two_things(x):
      return wait_and_return_x(x + 3) + wait_and_return_x(x + 4)

    fn = functor_factories.trace_py_fn(
        lambda x: user_facing_kd.if_(
            branch_to_use, do_two_things, do_other_two_things, x
        )
    )
    context = koda_internal_parallel.get_default_execution_context()
    transformed_fn = koda_internal_parallel.transform(context, fn)
    res = koda_internal_parallel.stream_from_future(
        transformed_fn(
            koda_internal_parallel.as_future(ds(1)),
            return_type_as=koda_internal_parallel.as_future(None),
        )
    ).eval()
    if branch_to_use:
      testing.assert_equal(res.read_all(timeout=5.0)[0], ds(5))
    else:
      testing.assert_equal(res.read_all(timeout=5.0)[0], ds(9))

  def test_if_on_bags(self):
    fn = functor_factories.trace_py_fn(
        lambda x, y: user_facing_kd.if_(
            x,
            lambda z: user_facing_kd.attrs(z, foo=1),
            lambda z: user_facing_kd.attrs(z, foo=2),
            y,
            return_type_as=data_bag.DataBag,
        )
    )
    context = koda_internal_parallel.get_default_execution_context()
    transformed_fn = koda_internal_parallel.transform(context, fn)
    y = fns.new()
    res = koda_internal_parallel.stream_from_future(
        transformed_fn(
            koda_internal_parallel.as_future(mask_constants.present),
            koda_internal_parallel.as_future(y),
            return_type_as=koda_internal_parallel.as_future(data_bag.DataBag),
        )
    ).eval()
    new_y = y.updated(res.read_all(timeout=5.0)[0])
    testing.assert_equal(new_y.foo.no_bag(), ds(1))

    res = koda_internal_parallel.stream_from_future(
        transformed_fn(
            koda_internal_parallel.as_future(mask_constants.missing),
            koda_internal_parallel.as_future(y),
            return_type_as=koda_internal_parallel.as_future(data_bag.DataBag),
        )
    ).eval()
    new_y = y.updated(res.read_all(timeout=5.0)[0])
    testing.assert_equal(new_y.foo.no_bag(), ds(2))

  def test_iterables_make(self):
    e1 = threading.Event()
    e2 = threading.Event()
    e3 = threading.Event()

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_1():
      self.assertTrue(e1.wait(timeout=5.0))
      return 1

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_2():
      self.assertTrue(e2.wait(timeout=5.0))
      return 2

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_3():
      self.assertTrue(e3.wait(timeout=5.0))
      return 3

    def f():
      return iterables.make(
          wait_and_return_1(), wait_and_return_2(), wait_and_return_3()
      )

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), f
    )
    res = transformed_fn(
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()
    reader = res.make_reader()
    time.sleep(0.01)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())
    e2.set()
    time.sleep(0.01)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())
    e1.set()
    self._wait_until_n_items(res, 2)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(1), ds(2))
    )
    e3.set()
    self._wait_until_n_items(res, 4)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(3))
    )
    self.assertIsNone(reader.read_available())

  def test_iterables_make_unordered(self):
    e1 = threading.Event()
    e2 = threading.Event()
    e3 = threading.Event()

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_1():
      self.assertTrue(e1.wait(timeout=5.0))
      return 1

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_2():
      self.assertTrue(e2.wait(timeout=5.0))
      return 2

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_3():
      self.assertTrue(e3.wait(timeout=5.0))
      return 3

    def f():
      return iterables.make_unordered(
          wait_and_return_1(), wait_and_return_2(), wait_and_return_3()
      )

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), f
    )
    res = transformed_fn(
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()
    reader = res.make_reader()
    time.sleep(0.01)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())
    e2.set()
    self._wait_until_n_items(res, 1)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(2))
    )
    e1.set()
    self._wait_until_n_items(res, 2)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(1))
    )
    e3.set()
    self._wait_until_n_items(res, 4)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(3))
    )
    self.assertIsNone(reader.read_available())

  @parameterized.parameters(
      iterables.make,
      iterables.make_unordered,
      iterables.chain,
      iterables.interleave,
  )
  def test_iterables_make_empty(self, op):

    def f():
      return op()

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), f
    )
    res = transformed_fn(
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    self.assertEqual(res.read_all(timeout=5.0), [])

  @parameterized.parameters(
      iterables.make,
      iterables.make_unordered,
      iterables.chain,
      iterables.interleave,
  )
  def test_iterables_make_empty_bags(self, op):

    def f():
      return op(value_type_as=data_bag.DataBag)

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), f
    )
    res = transformed_fn(
        return_type_as=koda_internal_parallel.stream_make(
            value_type_as=data_bag.DataBag
        ),
    ).eval()
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_BAG)
    self.assertEqual(res.read_all(timeout=5.0), [])

  @parameterized.parameters(iterables.make, iterables.make_unordered)
  def test_iterables_make_bags(self, op):

    def f(x):
      return op(x, value_type_as=data_bag.DataBag)

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), f
    )
    db = data_bag.DataBag.empty().freeze()
    res = transformed_fn(
        x=koda_internal_parallel.as_future(db),
        return_type_as=koda_internal_parallel.stream_make(
            value_type_as=data_bag.DataBag
        ),
    ).eval()
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=5.0)), arolla.tuple(db)
    )

  @parameterized.parameters(iterables.make, iterables.make_unordered)
  def test_iterables_make_tuples(self, op):

    def f(x, y):
      return op(tuple_ops.tuple_(x, y))

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), f
    )
    res = transformed_fn(
        x=koda_internal_parallel.as_future(1),
        y=koda_internal_parallel.as_future(2),
        return_type_as=koda_internal_parallel.stream_make(
            value_type_as=tuple_ops.tuple_(None, None)
        ),
    ).eval()
    self.assertEqual(
        res.qtype.value_qtype,
        arolla.make_tuple_qtype(qtypes.DATA_SLICE, qtypes.DATA_SLICE),
    )
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=5.0)),
        arolla.tuple(arolla.tuple(ds(1), ds(2))),
    )

  def test_iterables_chain(self):
    e1 = threading.Event()
    e2 = threading.Event()
    e3 = threading.Event()
    e4 = threading.Event()

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_1():
      self.assertTrue(e1.wait(timeout=5.0))
      return 1

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_2():
      self.assertTrue(e2.wait(timeout=5.0))
      return 2

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_3():
      self.assertTrue(e3.wait(timeout=5.0))
      return 3

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_4():
      self.assertTrue(e4.wait(timeout=5.0))
      return 4

    def f():
      return iterables.chain(
          iterables.make(wait_and_return_1(), wait_and_return_2()),
          iterables.make(wait_and_return_3()),
          iterables.make(wait_and_return_4()),
      )

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), f
    )
    res = transformed_fn(
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()
    reader = res.make_reader()
    time.sleep(0.01)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())
    e2.set()
    e3.set()
    time.sleep(0.01)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())
    e1.set()
    self._wait_until_n_items(res, 3)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()),
        arolla.tuple(ds(1), ds(2), ds(3)),
    )
    e4.set()
    self._wait_until_n_items(res, 5)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(4))
    )
    self.assertIsNone(reader.read_available())

  def test_iterables_interleave(self):
    e1 = threading.Event()
    e2 = threading.Event()
    e3 = threading.Event()
    e4 = threading.Event()

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_1():
      self.assertTrue(e1.wait(timeout=5.0))
      return 1

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_2():
      self.assertTrue(e2.wait(timeout=5.0))
      return 2

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_3():
      self.assertTrue(e3.wait(timeout=5.0))
      return 3

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_4():
      self.assertTrue(e4.wait(timeout=5.0))
      return 4

    def f():
      return iterables.interleave(
          iterables.make(wait_and_return_1(), wait_and_return_2()),
          iterables.make(wait_and_return_3()),
          iterables.make(wait_and_return_4()),
      )

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), f
    )
    res = transformed_fn(
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()
    reader = res.make_reader()
    time.sleep(0.01)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())
    e2.set()
    time.sleep(0.01)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())
    e3.set()
    self._wait_until_n_items(res, 1)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()),
        arolla.tuple(ds(3)),
    )
    e1.set()
    self._wait_until_n_items(res, 3)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()),
        arolla.tuple(ds(1), ds(2)),
    )
    e4.set()
    self._wait_until_n_items(res, 5)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(4))
    )
    self.assertIsNone(reader.read_available())

  @parameterized.parameters(iterables.chain, iterables.interleave)
  def test_iterables_concat_bags(self, op):

    def f(x):
      return op(iterables.make(x), value_type_as=data_bag.DataBag)

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), f
    )
    db = data_bag.DataBag.empty().freeze()
    res = transformed_fn(
        x=koda_internal_parallel.as_future(db),
        return_type_as=koda_internal_parallel.stream_make(
            value_type_as=data_bag.DataBag
        ),
    ).eval()
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=5.0)), arolla.tuple(db)
    )

  def test_iterables_flat_map_chain(self):
    e1 = threading.Event()
    e2 = threading.Event()
    e3 = threading.Event()

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_1():
      self.assertTrue(e1.wait(timeout=5.0))
      return 1

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_2():
      self.assertTrue(e2.wait(timeout=5.0))
      return 2

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_3():
      self.assertTrue(e3.wait(timeout=5.0))
      return 3

    def f(x):
      return user_facing_kd.if_(
          x == 1,
          lambda: user_facing_kd.iterables.make(wait_and_return_1()),
          lambda: user_facing_kd.iterables.make(
              wait_and_return_2(), wait_and_return_3()
          ),
          return_type_as=user_facing_kd.iterables.make(),
      )

    def g(x):
      return user_facing_kd.functor.flat_map_chain(x, f)

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), g
    )
    stream, writer = clib.make_stream(qtypes.DATA_SLICE)
    res = transformed_fn(
        x=stream,
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()

    reader = res.make_reader()
    writer.write(ds(1))
    writer.write(ds(2))
    e2.set()
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    e1.set()
    self._wait_until_n_items(res, 2)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(1), ds(2))
    )
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    e3.set()
    self._wait_until_n_items(res, 3)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(3))
    )
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    writer.close()
    self._wait_until_n_items(res, 4)
    self.assertIsNone(reader.read_available())

  def test_iterables_flat_map_interleave(self):
    e1 = threading.Event()
    e2 = threading.Event()
    e3 = threading.Event()

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_1():
      self.assertTrue(e1.wait(timeout=5.0))
      return 1

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_2():
      self.assertTrue(e2.wait(timeout=5.0))
      return 2

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_3():
      self.assertTrue(e3.wait(timeout=5.0))
      return 3

    def f(x):
      return user_facing_kd.if_(
          x == 1,
          lambda: user_facing_kd.iterables.make(wait_and_return_1()),
          lambda: user_facing_kd.iterables.make(
              wait_and_return_2(), wait_and_return_3()
          ),
          return_type_as=user_facing_kd.iterables.make(),
      )

    def g(x):
      return user_facing_kd.functor.flat_map_interleaved(x, f)

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), g
    )
    stream, writer = clib.make_stream(qtypes.DATA_SLICE)
    res = transformed_fn(
        x=stream,
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()

    reader = res.make_reader()
    writer.write(ds(1))
    writer.write(ds(2))
    e2.set()
    self._wait_until_n_items(res, 1)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(2))
    )
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    e1.set()
    self._wait_until_n_items(res, 2)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(1))
    )
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    e3.set()
    self._wait_until_n_items(res, 3)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(3))
    )
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    writer.close()
    self._wait_until_n_items(res, 4)
    self.assertIsNone(reader.read_available())

  def test_reduce(self):
    def f(x, y):
      return x * 10 + y

    def g(x, initial):
      return user_facing_kd.functor.reduce(f, x, initial)

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), g
    )
    res = koda_internal_parallel.stream_from_future(
        transformed_fn(
            x=koda_internal_parallel.stream_make(1, 2, 3, 4),
            initial=koda_internal_parallel.as_future(5),
            return_type_as=koda_internal_parallel.as_future(None),
        )
    ).eval()

    testing.assert_equal(res.read_all(timeout=5.0)[0], ds(51234))

  def test_reduce_tuples(self):
    def f(x, y):
      return (x[1], x[0] * 10 + y)

    def g(x, initial):
      return user_facing_kd.functor.reduce(f, x, initial)

    context = koda_internal_parallel.get_default_execution_context()
    transformed_fn = koda_internal_parallel.transform(context, g)
    res = koda_internal_parallel.stream_from_future(
        koda_internal_parallel.future_from_parallel(
            koda_internal_parallel.get_executor_from_context(context),
            transformed_fn(
                x=koda_internal_parallel.stream_make(1, 2, 3, 4),
                initial=koda_internal_parallel.as_parallel((5, 6)),
                return_type_as=koda_internal_parallel.as_parallel((None, None)),
            ),
        )
    ).eval()

    testing.assert_equal(
        res.read_all(timeout=5.0)[0], arolla.tuple(ds(513), ds(624))
    )

  def test_reduce_concat(self):
    def g(x, initial):
      return user_facing_kd.iterables.reduce_concat(x, initial)

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), g
    )
    res = koda_internal_parallel.stream_from_future(
        transformed_fn(
            x=koda_internal_parallel.stream_make(ds([1]), ds([2])),
            initial=koda_internal_parallel.as_future(ds([5])),
            return_type_as=koda_internal_parallel.as_future(None),
        )
    ).eval()

    testing.assert_equal(res.read_all(timeout=5.0)[0], ds([5, 1, 2]))

  def test_reduce_concat_ndim(self):
    def g(x, initial, ndim):
      return user_facing_kd.iterables.reduce_concat(x, initial, ndim=ndim)

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), g
    )
    res = koda_internal_parallel.stream_from_future(
        transformed_fn(
            x=koda_internal_parallel.stream_make(ds([[1]]), ds([[2]])),
            initial=koda_internal_parallel.as_future(ds([[5]])),
            ndim=koda_internal_parallel.as_future(ds(2)),
            return_type_as=koda_internal_parallel.as_future(None),
        )
    ).eval()

    testing.assert_equal(res.read_all(timeout=5.0)[0], ds([[5], [1], [2]]))

  def test_reduce_updated_bag(self):
    def g(x, initial):
      return user_facing_kd.iterables.reduce_updated_bag(x, initial)

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), g
    )
    o = fns.new()
    b1 = kd.attrs(o, a=1, b=5)
    b2 = kd.attrs(o, b=2)
    b3 = kd.attrs(o, c=3, a=4)
    res = koda_internal_parallel.stream_from_future(
        transformed_fn(
            x=koda_internal_parallel.stream_make(b1, b2),
            initial=koda_internal_parallel.as_future(b3),
            return_type_as=koda_internal_parallel.as_future(b1),
        )
    ).eval()

    o_res = o.with_bag(res.read_all(timeout=5.0)[0])
    testing.assert_equal(o_res.a.no_bag(), ds(1))
    testing.assert_equal(o_res.b.no_bag(), ds(2))
    testing.assert_equal(o_res.c.no_bag(), ds(3))

  def test_while_returns(self):
    factorial = functor_factories.trace_py_fn(
        lambda n: user_facing_kd.functor.while_(
            lambda n, returns: n > 0,
            lambda n, returns: user_facing_kd.namedtuple(
                returns=returns * n,
                n=n - 1,
            ),
            n=n,
            returns=1,
        )
    )
    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), factorial
    )
    res = koda_internal_parallel.stream_from_future(
        transformed_fn(
            n=koda_internal_parallel.as_future(5),
            return_type_as=koda_internal_parallel.as_future(None),
        )
    ).eval()
    testing.assert_equal(res.read_all(timeout=5.0)[0], ds(120))

  def test_while_yields(self):
    factorial = functor_factories.trace_py_fn(
        lambda n: user_facing_kd.functor.while_(
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.namedtuple(
                yields=user_facing_kd.iterables.make(res * n),
                n=n - 1,
                res=res * n,
            ),
            n=n,
            res=1,
            yields=user_facing_kd.iterables.make(),
        )
    )
    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), factorial
    )
    res = transformed_fn(
        n=koda_internal_parallel.as_future(5),
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=5.0)),
        arolla.tuple(ds(5), ds(20), ds(60), ds(120), ds(120)),
    )

  def test_while_yields_interleaved(self):
    factorial = functor_factories.trace_py_fn(
        lambda n: user_facing_kd.functor.while_(
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.namedtuple(
                yields_interleaved=user_facing_kd.iterables.make(res * n),
                n=n - 1,
                res=res * n,
            ),
            n=n,
            res=1,
            yields_interleaved=user_facing_kd.iterables.make(),
        )
    )
    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), factorial
    )
    res = transformed_fn(
        n=koda_internal_parallel.as_future(5),
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()
    self.assertCountEqual(
        [x.to_py() for x in res.read_all(timeout=5.0)],
        [5, 20, 60, 120, 120],
    )

  def test_while_yields_ordering(self):
    e1 = threading.Event()
    e2 = threading.Event()
    e3 = threading.Event()

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_1():
      self.assertTrue(e1.wait(timeout=5.0))
      return 1

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_2():
      self.assertTrue(e2.wait(timeout=5.0))
      return 2

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_3():
      self.assertTrue(e3.wait(timeout=5.0))
      return 3

    def body(i):
      return user_facing_kd.namedtuple(
          yields=user_facing_kd.if_(
              i == 1,
              lambda: user_facing_kd.iterables.make(wait_and_return_1()),
              lambda: user_facing_kd.iterables.make(
                  wait_and_return_2(), wait_and_return_3()
              ),
              return_type_as=user_facing_kd.iterables.make(),
          ),
          i=i + 1,
      )

    def g():
      return user_facing_kd.functor.while_(
          lambda i: i <= 2,
          body,
          i=1,
          yields=user_facing_kd.iterables.make(),
      )

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), g
    )
    res = transformed_fn(
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()

    reader = res.make_reader()
    e2.set()
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    e1.set()
    self._wait_until_n_items(res, 2)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(1), ds(2))
    )
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    e3.set()
    self._wait_until_n_items(res, 4)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(3))
    )
    self.assertIsNone(reader.read_available())

  def test_while_yields_interleaved_ordering(self):
    e1 = threading.Event()
    e2 = threading.Event()
    e3 = threading.Event()

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_1():
      self.assertTrue(e1.wait(timeout=5.0))
      return 1

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_2():
      self.assertTrue(e2.wait(timeout=5.0))
      return 2

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_3():
      self.assertTrue(e3.wait(timeout=5.0))
      return 3

    def body(i):
      return user_facing_kd.namedtuple(
          yields_interleaved=user_facing_kd.if_(
              i == 1,
              lambda: user_facing_kd.iterables.make(wait_and_return_1()),
              lambda: user_facing_kd.iterables.make(
                  wait_and_return_2(), wait_and_return_3()
              ),
              return_type_as=user_facing_kd.iterables.make(),
          ),
          i=i + 1,
      )

    def g():
      return user_facing_kd.functor.while_(
          lambda i: i <= 2,
          body,
          i=1,
          yields_interleaved=user_facing_kd.iterables.make(),
      )

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), g
    )
    res = transformed_fn(
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()

    reader = res.make_reader()
    e2.set()
    self._wait_until_n_items(res, 1)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(2))
    )
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    e1.set()
    self._wait_until_n_items(res, 2)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(1))
    )
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    e3.set()
    self._wait_until_n_items(res, 4)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(3))
    )
    self.assertIsNone(reader.read_available())

  def test_for_returns(self):
    factorial = functor_factories.trace_py_fn(
        lambda vals: user_facing_kd.functor.for_(
            vals,
            lambda n, returns: user_facing_kd.namedtuple(
                returns=returns * n,
            ),
            returns=1,
        )
    )
    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), factorial
    )
    res = koda_internal_parallel.stream_from_future(
        transformed_fn(
            vals=koda_internal_parallel.stream_make(*range(1, 6)),
            return_type_as=koda_internal_parallel.as_future(None),
        )
    ).eval()
    testing.assert_equal(res.read_all(timeout=5.0)[0], ds(120))

  def test_for_yields(self):
    factorial = functor_factories.trace_py_fn(
        lambda vals: user_facing_kd.functor.for_(
            vals,
            lambda n, res: user_facing_kd.namedtuple(
                yields=user_facing_kd.iterables.make(res * n),
                res=res * n,
            ),
            res=1,
            yields=user_facing_kd.iterables.make(),
        )
    )
    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), factorial
    )
    res = transformed_fn(
        vals=koda_internal_parallel.stream_make(*range(1, 6)),
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=5.0)),
        arolla.tuple(ds(1), ds(2), ds(6), ds(24), ds(120)),
    )

  def test_for_yields_interleaved(self):
    factorial = functor_factories.trace_py_fn(
        lambda vals: user_facing_kd.functor.for_(
            vals,
            lambda n, res: user_facing_kd.namedtuple(
                yields_interleaved=user_facing_kd.iterables.make(res * n),
                res=res * n,
            ),
            res=1,
            yields_interleaved=user_facing_kd.iterables.make(),
        )
    )
    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), factorial
    )
    res = transformed_fn(
        vals=koda_internal_parallel.stream_make(*range(1, 6)),
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()
    self.assertCountEqual(
        [x.to_py() for x in res.read_all(timeout=5.0)],
        [1, 2, 6, 24, 120],
    )

  def test_for_yields_ordering(self):
    e1 = threading.Event()
    e2 = threading.Event()
    e3 = threading.Event()

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_1():
      self.assertTrue(e1.wait(timeout=5.0))
      return 1

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_2():
      self.assertTrue(e2.wait(timeout=5.0))
      return 2

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_3():
      self.assertTrue(e3.wait(timeout=5.0))
      return 3

    def body(i):
      return user_facing_kd.namedtuple(
          yields=user_facing_kd.if_(
              i == 1,
              lambda: user_facing_kd.iterables.make(wait_and_return_1()),
              lambda: user_facing_kd.iterables.make(
                  wait_and_return_2(), wait_and_return_3()
              ),
              return_type_as=user_facing_kd.iterables.make(),
          ),
      )

    def g():
      return user_facing_kd.functor.for_(
          user_facing_kd.iterables.make(1, 2),
          body,
          yields=user_facing_kd.iterables.make(),
      )

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), g
    )
    res = transformed_fn(
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()

    reader = res.make_reader()
    e2.set()
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    e1.set()
    self._wait_until_n_items(res, 2)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(1), ds(2))
    )
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    e3.set()
    self._wait_until_n_items(res, 4)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(3))
    )
    self.assertIsNone(reader.read_available())

  def test_for_yields_interleaved_ordering(self):
    e1 = threading.Event()
    e2 = threading.Event()
    e3 = threading.Event()

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_1():
      self.assertTrue(e1.wait(timeout=5.0))
      return 1

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_2():
      self.assertTrue(e2.wait(timeout=5.0))
      return 2

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_and_return_3():
      self.assertTrue(e3.wait(timeout=5.0))
      return 3

    def body(i):
      return user_facing_kd.namedtuple(
          yields_interleaved=user_facing_kd.if_(
              i == 1,
              lambda: user_facing_kd.iterables.make(wait_and_return_1()),
              lambda: user_facing_kd.iterables.make(
                  wait_and_return_2(), wait_and_return_3()
              ),
              return_type_as=user_facing_kd.iterables.make(),
          ),
      )

    def g():
      return user_facing_kd.functor.for_(
          user_facing_kd.iterables.make(1, 2),
          body,
          yields_interleaved=user_facing_kd.iterables.make(),
      )

    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), g
    )
    res = transformed_fn(
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()

    reader = res.make_reader()
    e2.set()
    self._wait_until_n_items(res, 1)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(2))
    )
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    e1.set()
    self._wait_until_n_items(res, 2)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(1))
    )
    time.sleep(0.1)
    testing.assert_equal(arolla.tuple(*reader.read_available()), arolla.tuple())

    e3.set()
    self._wait_until_n_items(res, 4)
    testing.assert_equal(
        arolla.tuple(*reader.read_available()), arolla.tuple(ds(3))
    )
    self.assertIsNone(reader.read_available())

  @parameterized.parameters('yields', 'yields_interleaved')
  def test_for_yields_no_yield_statement(self, yield_mode):
    factorial = functor_factories.trace_py_fn(
        lambda vals: user_facing_kd.functor.for_(
            vals,
            lambda n, res: user_facing_kd.namedtuple(
                res=res * n,
            ),
            res=1,
            **{yield_mode: user_facing_kd.iterables.make(2, 3)},
        )
    )
    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), factorial
    )
    res = transformed_fn(
        vals=koda_internal_parallel.stream_make(*range(1, 6)),
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=5.0)),
        arolla.tuple(ds(2), ds(3)),
    )

  def test_for_returns_finalize_fn(self):
    factorial = functor_factories.trace_py_fn(
        lambda vals: user_facing_kd.functor.for_(
            vals,
            lambda n, returns: user_facing_kd.namedtuple(
                returns=returns + n,
            ),
            finalize_fn=lambda returns: user_facing_kd.namedtuple(
                returns=-returns,
            ),
            returns=0,
        )
    )
    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), factorial
    )
    res = koda_internal_parallel.stream_from_future(
        transformed_fn(
            vals=koda_internal_parallel.stream_make(*range(1, 6)),
            return_type_as=koda_internal_parallel.as_future(None),
        )
    ).eval()
    testing.assert_equal(res.read_all(timeout=5.0)[0], ds(-15))

  @parameterized.parameters('yields', 'yields_interleaved')
  def test_for_yields_finalize_fn(self, yield_mode):
    factorial = functor_factories.trace_py_fn(
        lambda vals: user_facing_kd.functor.for_(
            vals,
            lambda n: user_facing_kd.namedtuple(),
            finalize_fn=lambda: user_facing_kd.namedtuple(
                **{yield_mode: user_facing_kd.iterables.make(2, 3)}
            ),
            **{yield_mode: user_facing_kd.iterables.make()},
        )
    )
    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), factorial
    )
    res = transformed_fn(
        vals=koda_internal_parallel.stream_make(*range(1, 6)),
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=5.0)),
        arolla.tuple(ds(2), ds(3)),
    )

  def test_for_returns_condition_fn(self):
    factorial = functor_factories.trace_py_fn(
        lambda vals: user_facing_kd.functor.for_(
            vals,
            lambda n, returns: user_facing_kd.namedtuple(
                returns=returns + n,
            ),
            condition_fn=lambda returns: returns < 5,
            returns=0,
        )
    )
    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), factorial
    )
    res = koda_internal_parallel.stream_from_future(
        transformed_fn(
            vals=koda_internal_parallel.stream_make(*range(1, 6)),
            return_type_as=koda_internal_parallel.as_future(None),
        )
    ).eval()
    testing.assert_equal(res.read_all(timeout=5.0)[0], ds(6))

  @parameterized.parameters('yields', 'yields_interleaved')
  def test_for_yields_condition_fn(self, yield_mode):
    factorial = functor_factories.trace_py_fn(
        lambda vals: user_facing_kd.functor.for_(
            vals,
            lambda n, last_n: user_facing_kd.namedtuple(
                last_n=n,
                **{yield_mode: user_facing_kd.iterables.make(n)},
            ),
            condition_fn=lambda last_n: last_n < 1,
            last_n=0,
            **{yield_mode: user_facing_kd.iterables.make()},
        )
    )
    transformed_fn = koda_internal_parallel.transform(
        koda_internal_parallel.get_default_execution_context(), factorial
    )
    res = transformed_fn(
        vals=koda_internal_parallel.stream_make(*range(1, 6)),
        return_type_as=koda_internal_parallel.stream_make(),
    ).eval()
    testing.assert_equal(
        arolla.tuple(*res.read_all(timeout=5.0)),
        arolla.tuple(ds(1)),
    )

  def test_map_simple_positional(self):
    f = lambda x: user_facing_kd.functor.map(
        ((lambda x: x + 1) & (x >= 3)) | (lambda x: x - 1),
        x=x,
    )
    x = ds([1, 2, 3, 4])
    testing.assert_equal(_parallel_eval(f, x), ds([0, 1, 4, 5]))

  def test_map_simple_keyword(self):
    f = lambda a, b: a + b
    x = ds([[1, 2], [3, 4], [5, 6]])
    y = ds([[7, 8], [9, 10], [11, 12]])
    testing.assert_equal(
        _parallel_eval(
            lambda f, x, y: user_facing_kd.functor.map(fn=f, a=x, b=y),
            f=f,
            x=x,
            y=y,
        ),
        ds([[1 + 7, 2 + 8], [3 + 9, 4 + 10], [5 + 11, 6 + 12]]),
    )

  def test_map_item(self):
    f = functor_factories.fn(I.x + I.y)
    x = ds(1)
    y = ds(2)
    testing.assert_equal(
        _parallel_eval(
            lambda f, x, y: user_facing_kd.functor.map(fn=f, x=x, y=y),
            f=f,
            x=x,
            y=y,
        ),
        ds(3),
    )

  def test_map_include_missing(self):
    f = functor_factories.fn((I.x | 0) + (I.y | 0))
    x = ds(None)
    y = ds(2)
    testing.assert_equal(
        _parallel_eval(
            lambda f, x, y: user_facing_kd.functor.map(fn=f, x=x, y=y),
            f=f,
            x=x,
            y=y,
        ),
        ds(None),
    )
    testing.assert_equal(
        _parallel_eval(
            lambda f, x, y: user_facing_kd.functor.map(fn=f, x=y, y=x),
            f=f,
            x=y,
            y=x,
        ),
        ds(None),
    )
    testing.assert_equal(
        _parallel_eval(
            lambda f, x, y: user_facing_kd.functor.map(
                x=x, y=y, include_missing=False, fn=f
            ),
            f=f,
            x=x,
            y=y,
        ),
        ds(None),
    )
    testing.assert_equal(
        _parallel_eval(
            lambda f, x, y: user_facing_kd.functor.map(
                x=x, y=y, include_missing=True, fn=f
            ),
            f=f,
            x=x,
            y=y,
        ),
        ds(2),
    )

  def test_map_item_missing(self):
    f = ds(None)
    x = ds(1)
    y = ds(2)
    testing.assert_equal(
        _parallel_eval(
            lambda f, x, y: user_facing_kd.functor.map(fn=f, x=x, y=y),
            f=f,
            x=x,
            y=y,
        ),
        ds(None),
    )

  def test_map_per_item(self):
    def f(x):
      self.assertEqual(x.get_ndim(), 0)
      return x + 1

    x = ds([[1, 2], [3]])
    f = functor_factories.fn(f, use_tracing=False)
    testing.assert_equal(
        _parallel_eval(
            lambda f, x: user_facing_kd.functor.map(fn=f, x=x), f=f, x=x
        ),
        ds([[2, 3], [4]]),
    )

  def test_map_or_all_present(self):
    f1 = functor_factories.fn(lambda x: x + 1)
    f2 = functor_factories.fn(lambda x: x - 1)
    x = ds([1, 2, 3, 4])
    testing.assert_equal(
        _parallel_eval(
            lambda x, f1, f2: user_facing_kd.functor.map(
                fn=f1 & (x >= 3) | f2, x=x
            ),
            x=x,
            f1=f1,
            f2=f2,
        ),
        ds([0, 1, 4, 5]),
    )

  def test_map_or_some_missing(self):
    f = functor_factories.fn(lambda x: x + 1)
    x = ds([1, 2, 3, 4])
    testing.assert_equal(
        _parallel_eval(
            lambda f, x: user_facing_kd.functor.map(fn=f & (x >= 3), x=x),
            f=f,
            x=x,
        ),
        ds([None, None, 4, 5]),
    )

  def test_map_or_with_default(self):
    f = functor_factories.fn(lambda x: (x + 1) | 1)
    x = ds([1, 2, 3, 4])
    testing.assert_equal(
        _parallel_eval(
            lambda f, x: user_facing_kd.functor.map(
                fn=f, x=x, include_missing=True
            ),
            x=x & (x >= 3),
            f=f,
        ),
        ds([1, 1, 4, 5]),
    )
    testing.assert_equal(
        _parallel_eval(
            lambda f, x: user_facing_kd.functor.map(
                fn=f, x=x, include_missing=False
            ),
            x=x & (x >= 3),
            f=f,
        ),
        ds([None, None, 4, 5]),
    )
    testing.assert_equal(
        _parallel_eval(
            lambda f, x: user_facing_kd.functor.map(fn=f, x=x),
            x=x & (x >= 3),
            f=f,
        ),
        ds([None, None, 4, 5]),
    )

  def test_map_empty_output(self):
    f = functor_factories.fn(lambda x: x + 1)
    testing.assert_equal(
        _parallel_eval(
            lambda f, x: user_facing_kd.functor.map(fn=f, x=x),
            f=f,
            x=ds([]),
        ),
        ds([]),
    )
    testing.assert_equal(
        _parallel_eval(
            lambda f, x: user_facing_kd.functor.map(fn=f, x=x),
            f=f,
            x=ds([None]),
        ),
        ds([None]),
    )

  def test_map_different_shapes(self):
    f1 = functor_factories.fn(lambda x, y: x + y)
    f2 = functor_factories.fn(lambda x, y: x - y)
    x = ds([[1, None, 3], [4, 5, 6]])
    y = ds(1)
    testing.assert_equal(
        _parallel_eval(
            lambda f, x, y: user_facing_kd.functor.map(fn=f, x=x, y=y),
            f=ds([f1, f2]),
            x=x,
            y=y,
        ),
        ds([[2, None, 4], [3, 4, 5]]),
    )

    testing.assert_equal(
        _parallel_eval(
            lambda f, x, y: user_facing_kd.functor.map(fn=f, x=x, y=y),
            f=ds([f1, None]),
            x=x,
            y=y,
        ),
        ds([[2, None, 4], [None, None, None]]),
    )

    # Even with include_missing=True, the missing functor is not called.
    testing.assert_equal(
        _parallel_eval(
            lambda f, x, y: user_facing_kd.functor.map(
                include_missing=True, fn=f, x=x, y=y
            ),
            f=ds([f1, None]),
            x=x,
            y=y,
        ),
        ds([[2, None, 4], [None, None, None]]),
    )

  def test_map_return_slice(self):
    def f(x):
      return x + ds([1, 2])

    x = ds([1, 2, 3])
    f = functor_factories.fn(f)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('the functor in kd.map must evaluate to a DataItem'),
    ):
      _ = _parallel_eval(
          lambda f, x: user_facing_kd.functor.map(fn=f, x=x), f=f, x=x
      )

  def test_map_return_bag(self):
    def f(unused_x):
      return user_facing_kd.bag()

    x = ds([1, 2, 3])
    f = functor_factories.fn(f)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'The functor was called with `FUTURE[DATA_SLICE]` as the output'
            ' type, but the computation resulted in type `FUTURE[DATA_BAG]`'
            ' instead.'
        ),
    ):
      _ = _parallel_eval(
          lambda f, x: user_facing_kd.functor.map(fn=f, unused_x=x),
          f=f,
          x=x,
      )

  def test_map_adoption(self):
    f = functor_factories.fn(lambda x: user_facing_kd.obj(x=x + 1))
    x = ds([1, 2, 3])
    testing.assert_equal(
        _parallel_eval(
            lambda f, x: user_facing_kd.functor.map(fn=f, x=x), f=f, x=x
        ).x.no_bag(),
        ds([2, 3, 4]),
    )

  def test_map_incompatible_shapes(self):
    f = functor_factories.fn(I.x + I.y)
    f = ds([f, f])
    x = ds([[1, 2], [3, 4], [5, 6]])
    y = ds([[7, 8], [9, 10], [11, 12]])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'DataSlice with shape=JaggedShape(2) cannot be expanded to'
            ' shape=JaggedShape(3, 2)'
        ),
    ):
      _ = _parallel_eval(
          lambda f, x, y: user_facing_kd.functor.map(fn=f, x=x, y=y),
          f=f,
          x=x,
          y=y,
      )

  def test_map_common_schema(self):
    f1 = functor_factories.fn(lambda x: x.foo)
    f2 = functor_factories.fn(lambda x: x.bar)
    f = ds([f1, f2])
    x = fns.new(foo=ds([1, 2]), bar=ds(['3', '4']))
    testing.assert_equal(
        _parallel_eval(
            lambda f, x: user_facing_kd.functor.map(fn=f, x=x),
            f=f,
            x=x,
        ),
        ds([1, '4']).with_bag(x.get_bag()),
    )

    y = fns.new(foo=fns.new(a=ds([1, 2])), bar=fns.new(a=ds([3, 4])))
    with self.assertRaisesRegex(ValueError, 'cannot find a common schema'):
      _ = _parallel_eval(
          lambda f, x: user_facing_kd.functor.map(fn=f, x=x),
          f=f,
          x=y,
      )

  def test_map_cancellable(self):
    expr = lambda x: user_facing_kd.functor.map(
        functor_factories.expr_fn(
            arolla.M.core._identity_with_cancel(I.self, 'cancelled')
        ),
        x,
    )
    x = ds([1, 2, 3])
    with self.assertRaisesRegex(ValueError, re.escape('cancelled')):
      _parallel_eval(expr, x=x)

  def test_map_non_functor_input_error(self):
    with self.assertRaisesRegex(
        ValueError, 'expected DATA_SLICE, got x: INT32'
    ):
      _parallel_eval(
          lambda f: user_facing_kd.functor.map(fn=f), f=arolla.int32(1)
      )

  def test_map_actually_parallel(self):
    barrier = threading.Barrier(2)

    @optools.as_py_function_operator(
        name='aux',
    )
    def wait_and_return_1():
      barrier.wait()
      return ds(1)

    @optools.as_py_function_operator(
        name='aux',
    )
    def wait_and_return_2():
      barrier.wait()
      return ds(2)

    fs = ds([
        functor_factories.trace_py_fn(wait_and_return_1),
        functor_factories.trace_py_fn(wait_and_return_2),
    ])
    testing.assert_equal(
        _parallel_eval(lambda fs: user_facing_kd.functor.map(fn=fs), fs),
        ds([1, 2]),
    )


if __name__ == '__main__':
  absltest.main()
