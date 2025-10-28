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

import functools
import re
import threading
import time
from typing import Any, Iterator

from absl.testing import absltest
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.expr import input_container
from koladata.functions import functions as fns
from koladata.functions import parallel
from koladata.functor import functor_factories
from koladata.functor import tracing_decorator
from koladata.operators import functor
from koladata.operators import iterables
from koladata.operators import tuple as tuple_ops
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import iterable_qvalue
from koladata.types import signature_utils


I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
S = I.self
ds = data_slice.DataSlice.from_vals


class YieldMultithreadedTest(absltest.TestCase):

  def test_yield_simple(self):
    fn = functor_factories.expr_fn(
        returns=iterables.make(I.x, V.foo),
        foo=I.y * I.x,
    )
    res_iter = parallel.yield_multithreaded(fn, x=2, y=3)
    self.assertIsInstance(res_iter, Iterator)
    testing.assert_equal(arolla.tuple(*res_iter), arolla.tuple(ds(2), ds(6)))

  def test_positional(self):
    fn = functor_factories.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_ONLY
            ),
        ]),
    )
    testing.assert_equal(
        arolla.tuple(
            *parallel.yield_multithreaded(fn, iterable_qvalue.Iterable(1, 2))
        ),
        arolla.tuple(ds(1), ds(2)),
    )
    with self.assertRaisesRegex(
        ValueError, re.escape('unknown keyword arguments: [x]')
    ):
      _ = list(
          *parallel.yield_multithreaded(fn, x=iterable_qvalue.Iterable(1, 2))
      )

  def test_keyword(self):
    fn = functor_factories.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.KEYWORD_ONLY
            ),
        ]),
    )
    testing.assert_equal(
        arolla.tuple(
            *parallel.yield_multithreaded(fn, x=iterable_qvalue.Iterable(1, 2))
        ),
        arolla.tuple(ds(1), ds(2)),
    )
    with self.assertRaisesRegex(
        ValueError, re.escape('too many positional arguments')
    ):
      _ = list(
          *parallel.yield_multithreaded(fn, iterable_qvalue.Iterable(1, 2))
      )

  def test_default_value(self):
    fn = functor_factories.expr_fn(
        returns=iterables.make(I.x),
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_ONLY, 57
            ),
        ]),
    )
    testing.assert_equal(
        arolla.tuple(*[x.no_bag() for x in parallel.yield_multithreaded(fn)]),
        arolla.tuple(ds(57)),
    )
    testing.assert_equal(
        arolla.tuple(
            *[x.no_bag() for x in parallel.yield_multithreaded(fn, 43)]
        ),
        arolla.tuple(ds(43)),
    )

  def test_eval_error(self):
    fn = functor_factories.expr_fn(
        returns=iterables.make(I.x.foo),
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
        ]),
    )
    x = fns.new(foo=57)
    testing.assert_equal(
        arolla.tuple(*parallel.yield_multithreaded(fn, x)),
        arolla.tuple(x.foo),
    )
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            "the attribute 'foo' is missing"
        ),
    ):
      _ = list(*parallel.yield_multithreaded(fn, fns.new(bar=57)))

  def test_non_dataslice_inputs(self):
    fn = functor_factories.expr_fn(iterables.make(tuple_ops.get_nth(I.x, 1)))
    testing.assert_equal(
        arolla.tuple(
            *parallel.yield_multithreaded(
                fn, x=arolla.tuple(ds(1), ds(2), ds(3))
            )
        ),
        arolla.tuple(ds(2)),
    )

  def test_yields_non_dataslice(self):
    fn = functor_factories.expr_fn(iterables.make(I.x))
    res = parallel.yield_multithreaded(
        fn,
        x=arolla.tuple(1, 2),
        value_type_as=arolla.tuple(5, 7),
    )
    testing.assert_equal(arolla.tuple(*res), arolla.tuple(arolla.tuple(1, 2)))

  def test_yields_returns_databag(self):
    fn = functor_factories.expr_fn(iterables.make(I.x.get_bag()))
    obj = fns.obj(x=1)
    res = parallel.yield_multithreaded(
        fn,
        x=obj,
        value_type_as=data_bag.DataBag,
    )
    testing.assert_equal(arolla.tuple(*res), arolla.tuple(obj.get_bag()))

  def test_functor_as_input(self):
    fn = functor_factories.expr_fn(iterables.make(I.x + I.y))
    testing.assert_equal(
        arolla.tuple(
            *parallel.yield_multithreaded(
                functor_factories.expr_fn(
                    functor.call(
                        I.func, x=I.u, y=I.v, return_type_as=iterables.make()
                    )
                ),
                func=fn,
                u=2,
                v=3,
            )
        ),
        arolla.tuple(ds(5)),
    )

  def test_computed_functor(self):
    fn = functor_factories.expr_fn(iterables.make(I.x + I.y))
    testing.assert_equal(
        arolla.tuple(
            *parallel.yield_multithreaded(
                functor_factories.expr_fn(
                    functor.call(
                        I.my_functors.fn,
                        x=I.u,
                        y=I.v,
                        return_type_as=iterables.make(),
                    )
                ),
                my_functors=fns.new(fn=fn),
                u=2,
                v=3,
            )
        ),
        arolla.tuple(ds(5)),
    )

  def test_really_parallel(self):
    first_barrier = threading.Barrier(3)
    second_barrier = threading.Barrier(3)

    # f3 and f4 would only proceed when they are executed in parallel.
    # f3 will call f1 and f2 that will only proceed if they are executed in
    # parallel.
    # Therefore this checks that independent computations are really parallel,
    # including nested ones.

    def wait_for_barrier_and_return_x(
        barrier: threading.Barrier, x: Any
    ) -> Any:
      barrier.wait(timeout=5.0)
      return x

    @tracing_decorator.TraceAsFnDecorator(
        return_type_as=iterable_qvalue.Iterable()
    )
    def f1(x):
      x = user_facing_kd.apply_py(
          functools.partial(wait_for_barrier_and_return_x, second_barrier), x
      ).with_name('wait')
      return user_facing_kd.iterables.make(x * 10 + 1)

    @tracing_decorator.TraceAsFnDecorator(
        return_type_as=iterable_qvalue.Iterable()
    )
    def f2(x):
      x = user_facing_kd.apply_py(
          functools.partial(wait_for_barrier_and_return_x, second_barrier), x
      ).with_name('wait')
      return user_facing_kd.iterables.make(x * 10 + 2)

    @tracing_decorator.TraceAsFnDecorator(
        return_type_as=iterable_qvalue.Iterable()
    )
    def f3(x):
      x = user_facing_kd.apply_py(
          functools.partial(wait_for_barrier_and_return_x, first_barrier), x
      ).with_name('wait')
      y = f1(x)
      z = f2(x)
      return user_facing_kd.iterables.chain(
          user_facing_kd.iterables.make(x * 10 + 3), y, z
      )

    @tracing_decorator.TraceAsFnDecorator(
        return_type_as=iterable_qvalue.Iterable()
    )
    def f4(x):
      x = user_facing_kd.apply_py(
          functools.partial(wait_for_barrier_and_return_x, first_barrier), x
      ).with_name('wait')
      return user_facing_kd.iterables.make(x * 10 + 4)

    @tracing_decorator.TraceAsFnDecorator(
        return_type_as=iterable_qvalue.Iterable()
    )
    def f5(x):
      return user_facing_kd.iterables.chain(f3(x), f4(x))

    res = parallel.yield_multithreaded(
        functor_factories.trace_py_fn(f5), x=ds(1), max_threads=2
    )
    first_barrier.wait(timeout=5.0)
    testing.assert_equal(next(res), ds(13))
    second_barrier.wait(timeout=5.0)
    testing.assert_equal(next(res), ds(11))
    testing.assert_equal(next(res), ds(12))
    testing.assert_equal(next(res), ds(14))
    with self.assertRaises(StopIteration):
      next(res)

  def test_cancellation(self):
    first_barrier = threading.Barrier(2, action=arolla.abc.simulate_SIGINT)
    second_barrier = threading.Barrier(2)

    @tracing_decorator.TraceAsFnDecorator(
        functor_factory=functor_factories.py_fn
    )
    def wait_for_cancellation(_: Any):
      first_barrier.wait(timeout=5.0)
      while True:
        arolla.abc.raise_if_cancelled()
        time.sleep(0.01)
      second_barrier.wait(timeout=5.0)

    @tracing_decorator.TraceAsFnDecorator(
        return_type_as=iterable_qvalue.Iterable()
    )
    def fn(x):
      y = wait_for_cancellation(x + 1)
      z = wait_for_cancellation(x + 2)
      return user_facing_kd.iterables.make(y, z)

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            r'\[CANCELLED\].*interrupted'
        ),
    ):
      _ = list(
          *parallel.yield_multithreaded(
              functor_factories.trace_py_fn(fn), x=ds(1), max_threads=2
          )
      )

  def test_non_iterable_return_value(self):
    fn = functor_factories.expr_fn(I.x)
    # TODO: Make this error mention call_multithreaded.
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'The functor was called with `STREAM[DATA_SLICE]` as the output'
            ' type, but the computation resulted in type `FUTURE[DATA_SLICE]`'
            ' instead.'
        ),
    ):
      _ = list(
          *parallel.yield_multithreaded(
              fn,
              x=1,
          )
      )

  def test_structured_return_value(self):
    fn = functor_factories.expr_fn(tuple_ops.tuple_(iterables.make(I.x)))
    # TODO: Make this error say that structured return values
    # with a stream inside are not supported yet.
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'The functor was called with `STREAM[DATA_SLICE]` as the output'
            ' type, but the computation resulted in type'
            ' `tuple<STREAM[DATA_SLICE]>` instead.'
        ),
    ):
      _ = list(
          *parallel.yield_multithreaded(
              fn,
              x=1,
          )
      )

  @arolla.abc.add_default_cancellation_context
  def test_timeout_respected(self):

    @tracing_decorator.TraceAsFnDecorator(
        functor_factory=functor_factories.py_fn
    )
    def wait_and_return_x(x):
      time.sleep(0.1)
      return x

    def body(i, n, s):
      del n  # Unused.
      i = wait_and_return_x(i)
      return user_facing_kd.namedtuple(
          i=i + 1, s=s + i, yields=user_facing_kd.iterables.make(s)
      )

    fn = functor_factories.expr_fn(
        functor.while_(
            lambda i, n, s: i <= n,
            body,
            s=I.s,
            n=I.n,
            i=1,
            yields=iterables.make(),
        )
    )
    testing.assert_equal(
        arolla.tuple(*parallel.yield_multithreaded(fn, n=3, s=0, timeout=5.0)),
        arolla.tuple(ds(0), ds(1), ds(3)),
    )

    res_iter = parallel.yield_multithreaded(fn, n=50, s=0, timeout=0.5)
    with self.assertRaises(TimeoutError):
      # Note that we leave enough time for each particular item to finish,
      # to verify that the timeout is global and not per-item.
      _ = list(*res_iter)

    # Cancel the remaining computation, otherwise we get crashes on process
    # shutdown.
    # TODO: Remove the need for this.
    context = arolla.abc.current_cancellation_context()
    self.assertIsNotNone(context)
    context.cancel('Boom!')
    # Since cancellation might happen during time.sleep above, we need to
    # wait a bit to make sure that the cancellation propagated. I could not
    # find a more reliable way.
    time.sleep(0.5)


if __name__ == '__main__':
  absltest.main()
