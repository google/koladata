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
from typing import Any

from absl.testing import absltest
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.expr import input_container
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.functor import tracing_decorator
from koladata.operators import functor
from koladata.operators import iterables
from koladata.operators import tuple as tuple_ops
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import signature_utils


I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
S = I.self
ds = data_slice.DataSlice.from_vals


class CallMultithreadedTest(absltest.TestCase):

  def test_call_simple(self):
    fn = functor_factories.expr_fn(
        returns=I.x + V.foo,
        foo=I.y * I.x,
    )
    testing.assert_equal(fns.parallel.call_multithreaded(fn, x=2, y=3), ds(8))
    # Unused inputs are ignored with the "default" signature.
    testing.assert_equal(
        fns.parallel.call_multithreaded(fn, x=2, y=3, z=4), ds(8)
    )

  def test_call_with_self(self):
    fn = functor_factories.expr_fn(
        returns=S.x + V.foo,
        foo=S.y * S.x,
    )
    testing.assert_equal(
        fns.parallel.call_multithreaded(fn, fns.new(x=2, y=3)), ds(8)
    )

  def test_call_explicit_signature(self):
    fn = functor_factories.expr_fn(
        returns=I.x + V.foo,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
            signature_utils.parameter(
                'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
        ]),
        foo=I.y,
    )
    testing.assert_equal(fns.parallel.call_multithreaded(fn, 1, 2), ds(3))
    testing.assert_equal(fns.parallel.call_multithreaded(fn, 1, y=2), ds(3))

  def test_call_with_no_expr(self):
    fn = functor_factories.expr_fn(57, signature=signature_utils.signature([]))
    testing.assert_equal(fns.parallel.call_multithreaded(fn).no_bag(), ds(57))

  def test_positional_only(self):
    fn = functor_factories.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_ONLY
            ),
        ]),
    )
    testing.assert_equal(fns.parallel.call_multithreaded(fn, 57), ds(57))
    with self.assertRaisesRegex(
        ValueError, re.escape('unknown keyword arguments: [x]')
    ):
      _ = fns.parallel.call_multithreaded(fn, x=57)

  def test_keyword_only(self):
    fn = functor_factories.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.KEYWORD_ONLY
            ),
        ]),
    )
    testing.assert_equal(fns.parallel.call_multithreaded(fn, x=57), ds(57))
    with self.assertRaisesRegex(ValueError, 'too many positional arguments'):
      _ = fns.parallel.call_multithreaded(fn, 57)

  def test_var_positional(self):
    fn = functor_factories.expr_fn(
        returns=tuple_ops.get_nth(I.x, 1),
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.VAR_POSITIONAL
            ),
        ]),
    )
    testing.assert_equal(fns.parallel.call_multithreaded(fn, 1, 2, 3), ds(2))

  def test_var_keyword(self):
    fn = functor_factories.expr_fn(
        returns=arolla.M.namedtuple.get_field(I.x, 'y'),
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.VAR_KEYWORD
            ),
        ]),
    )
    testing.assert_equal(
        fns.parallel.call_multithreaded(fn, x=1, y=2, z=3), ds(2)
    )

  def test_default_value(self):
    fn = functor_factories.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_ONLY, 57
            ),
        ]),
    )
    testing.assert_equal(fns.parallel.call_multithreaded(fn).no_bag(), ds(57))
    testing.assert_equal(fns.parallel.call_multithreaded(fn, 43), ds(43))

  def test_obj_as_default_value(self):
    fn = functor_factories.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x',
                signature_utils.ParameterKind.POSITIONAL_ONLY,
                fns.new(foo=57),
            ),
        ]),
    )
    testing.assert_equal(
        fns.parallel.call_multithreaded(fn).foo.no_bag(), ds(57)
    )
    testing.assert_equal(fns.parallel.call_multithreaded(fn, 43), ds(43))

  def test_call_eval_error(self):
    fn = functor_factories.expr_fn(
        returns=I.x.foo,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
        ]),
    )
    testing.assert_equal(
        fns.parallel.call_multithreaded(fn, fns.new(foo=57)).no_bag(), ds(57)
    )
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            "the attribute 'foo' is missing"
        ),
    ):
      _ = fns.parallel.call_multithreaded(fn, fns.new(bar=57))

  def test_call_non_dataslice_inputs(self):
    fn = functor_factories.expr_fn(tuple_ops.get_nth(I.x, 1))
    testing.assert_equal(
        fns.parallel.call_multithreaded(
            fn, x=arolla.tuple(ds(1), ds(2), ds(3))
        ),
        ds(2),
    )

  def test_call_returns_non_dataslice(self):
    fn = functor_factories.expr_fn(I.x)
    res = fns.parallel.call_multithreaded(
        fn,
        x=arolla.tuple(1, 2),
        return_type_as=arolla.tuple(5, 7),
    )
    testing.assert_equal(res, arolla.tuple(1, 2))

  def test_call_returns_databag(self):
    fn = functor_factories.expr_fn(I.x.get_bag())
    obj = fns.obj(x=1)
    res = fns.parallel.call_multithreaded(
        fn,
        x=obj,
        return_type_as=data_bag.DataBag,
    )
    testing.assert_equal(res, obj.get_bag())

  def test_call_with_functor_as_input(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    testing.assert_equal(
        fns.parallel.call_multithreaded(
            functor_factories.expr_fn(functor.call(I.func, x=I.u, y=I.v)),
            func=fn,
            u=2,
            v=3,
        ),
        ds(5),
    )

  def test_call_with_computed_functor(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    testing.assert_equal(
        fns.parallel.call_multithreaded(
            functor_factories.expr_fn(
                functor.call(I.my_functors.fn, x=I.u, y=I.v)
            ),
            my_functors=fns.new(fn=fn),
            u=2,
            v=3,
        ),
        ds(5),
    )

  def test_call_is_really_parallel(self):
    first_barrier = threading.Barrier(2)
    second_barrier = threading.Barrier(2)

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

    @tracing_decorator.TraceAsFnDecorator()
    def f1(x):
      x = user_facing_kd.apply_py(
          functools.partial(wait_for_barrier_and_return_x, second_barrier), x
      ).with_name('wait')
      return x

    @tracing_decorator.TraceAsFnDecorator()
    def f2(x):
      x = user_facing_kd.apply_py(
          functools.partial(wait_for_barrier_and_return_x, second_barrier), x
      ).with_name('wait')
      return x

    @tracing_decorator.TraceAsFnDecorator()
    def f3(x):
      x = user_facing_kd.apply_py(
          functools.partial(wait_for_barrier_and_return_x, first_barrier), x
      ).with_name('wait')
      y = f1(x)
      z = f2(x)
      return x + y + z

    @tracing_decorator.TraceAsFnDecorator()
    def f4(x):
      x = user_facing_kd.apply_py(
          functools.partial(wait_for_barrier_and_return_x, first_barrier), x
      ).with_name('wait')
      return x

    @tracing_decorator.TraceAsFnDecorator()
    def f5(x):
      return f3(x) + f4(x)

    testing.assert_equal(
        fns.parallel.call_multithreaded(
            functor_factories.trace_py_fn(f5), x=ds(1), max_threads=2
        ),
        ds(4),
    )

  def test_cancellation(self):
    first_barrier = threading.Barrier(2, action=arolla.abc.simulate_SIGINT)
    second_barrier = threading.Barrier(2)

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def wait_for_cancellation(_: Any):
      first_barrier.wait(timeout=5.0)
      while True:
        arolla.abc.raise_if_cancelled()
        time.sleep(0.01)
      second_barrier.wait(timeout=5.0)

    @tracing_decorator.TraceAsFnDecorator()
    def fn(x):
      y = wait_for_cancellation(x + 1)
      z = wait_for_cancellation(x + 2)
      return y + z

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(r'\[CANCELLED\].*interrupted'),
    ):
      fns.parallel.call_multithreaded(
          functor_factories.trace_py_fn(fn), x=ds(1), max_threads=2
      )

  def test_iterable_return_value(self):
    fn = functor_factories.expr_fn(iterables.make(I.x))
    # TODO: Make this error mention yield_multithreaded.
    with self.assertRaisesRegex(
        ValueError,
        'future_from_parallel can only be applied to a parallel non-stream'
        ' type',
    ):
      _ = fns.parallel.call_multithreaded(
          fn,
          x=1,
          return_type_as=iterables.make().eval(),
      )

  def test_structured_return_value(self):
    fn = functor_factories.expr_fn(
        tuple_ops.tuple_(I.x, tuple_ops.namedtuple_(y=I.y, z=I.z))
    )
    res = fns.parallel.call_multithreaded(
        fn,
        x=1,
        y=2,
        z=3,
        return_type_as=arolla.tuple(
            ds(None), arolla.namedtuple(y=ds(None), z=ds(None))
        ),
    )
    testing.assert_equal(
        res, arolla.tuple(ds(1), arolla.namedtuple(y=ds(2), z=ds(3)))
    )

  def test_timeout_respected(self):

    def my_fn(x):
      time.sleep(5.0)
      return x

    fn = functor_factories.py_fn(my_fn)
    with self.assertRaises(TimeoutError):
      _ = fns.parallel.call_multithreaded(fn, x=1, timeout=0.1)


if __name__ == '__main__':
  absltest.main()
