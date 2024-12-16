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

import functools
import threading
import time
from typing import Any

from absl.testing import absltest
from arolla import arolla
from koladata import kd
from koladata.ext.experimental.parallel_call import call
from koladata.operators import kde_operators
from koladata.testing import testing

I = kd.I
V = kd.V
S = kd.S
ds = kd.slice
kde = kde_operators.kde
signature_utils = kd.functor.signature_utils


class FunctorCallTest(absltest.TestCase):

  def test_call_simple(self):
    fn = kd.functor.expr_fn(
        returns=I.x + V.foo,
        foo=I.y * I.x,
    )
    testing.assert_equal(call.call_multithreaded(fn, x=2, y=3), ds(8))
    # Unused inputs are ignored with the "default" signature.
    testing.assert_equal(call.call_multithreaded(fn, x=2, y=3, z=4), ds(8))

  def test_call_with_self(self):
    fn = kd.functor.expr_fn(
        returns=S.x + V.foo,
        foo=S.y * S.x,
    )
    testing.assert_equal(call.call_multithreaded(fn, kd.new(x=2, y=3)), ds(8))

  def test_call_explicit_signature(self):
    fn = kd.functor.expr_fn(
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
    testing.assert_equal(call.call_multithreaded(fn, 1, 2), ds(3))
    testing.assert_equal(call.call_multithreaded(fn, 1, y=2), ds(3))

  def test_call_with_no_expr(self):
    fn = kd.functor.expr_fn(57, signature=signature_utils.signature([]))
    testing.assert_equal(call.call_multithreaded(fn).no_bag(), ds(57))

  def test_positional_only(self):
    fn = kd.functor.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_ONLY
            ),
        ]),
    )
    testing.assert_equal(call.call_multithreaded(fn, 57), ds(57))
    with self.assertRaisesRegex(TypeError, 'positional.*keyword'):
      _ = call.call_multithreaded(fn, x=57)

  def test_keyword_only(self):
    fn = kd.functor.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.KEYWORD_ONLY
            ),
        ]),
    )
    testing.assert_equal(call.call_multithreaded(fn, x=57), ds(57))
    with self.assertRaisesRegex(TypeError, 'too many positional arguments'):
      _ = call.call_multithreaded(fn, 57)

  def test_var_positional(self):
    fn = kd.functor.expr_fn(
        returns=kde.tuple.get_nth(I.x, 1),
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.VAR_POSITIONAL
            ),
        ]),
    )
    testing.assert_equal(call.call_multithreaded(fn, 1, 2, 3), ds(2))

  def test_var_keyword(self):
    fn = kd.functor.expr_fn(
        returns=arolla.M.namedtuple.get_field(I.x, 'y'),
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.VAR_KEYWORD
            ),
        ]),
    )
    testing.assert_equal(call.call_multithreaded(fn, x=1, y=2, z=3), ds(2))

  def test_default_value(self):
    fn = kd.functor.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_ONLY, 57
            ),
        ]),
    )
    testing.assert_equal(call.call_multithreaded(fn).no_bag(), ds(57))
    testing.assert_equal(call.call_multithreaded(fn, 43), ds(43))

  def test_obj_as_default_value(self):
    fn = kd.functor.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x',
                signature_utils.ParameterKind.POSITIONAL_ONLY,
                kd.new(foo=57),
            ),
        ]),
    )
    testing.assert_equal(call.call_multithreaded(fn).foo.no_bag(), ds(57))
    testing.assert_equal(call.call_multithreaded(fn, 43), ds(43))

  def test_call_eval_error(self):
    fn = kd.functor.expr_fn(
        returns=I.x.foo,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
        ]),
    )
    testing.assert_equal(
        call.call_multithreaded(fn, kd.new(foo=57)).no_bag(), ds(57)
    )
    with self.assertRaisesRegex(ValueError, "the attribute 'foo' is missing"):
      _ = call.call_multithreaded(fn, kd.new(bar=57))

  def test_call_non_dataslice_inputs(self):
    fn = kd.functor.expr_fn(kde.tuple.get_nth(I.x, 1))
    testing.assert_equal(
        call.call_multithreaded(fn, x=arolla.tuple(ds(1), ds(2), ds(3))), ds(2)
    )

  def test_call_returns_non_dataslice(self):
    fn = kd.functor.expr_fn(I.x)
    # So far we ignore return_type_as.
    res = call.call_multithreaded(fn, x=arolla.tuple(1, 2))
    testing.assert_equal(res, arolla.tuple(1, 2))
    res = call.call_multithreaded(
        fn,
        x=arolla.tuple(1, 2),
        return_type_as=arolla.tuple(5, 7),
    )
    testing.assert_equal(res, arolla.tuple(1, 2))

  def test_call_returns_databag(self):
    fn = kd.functor.expr_fn(I.x.get_bag())
    obj = kd.obj(x=1)
    res = call.call_multithreaded(
        fn,
        x=obj,
        return_type_as=kd.types.DataBag,
    )
    testing.assert_equal(res, obj.get_bag())

  def test_call_with_functor_as_input(self):
    fn = kd.functor.expr_fn(I.x + I.y)
    testing.assert_equal(
        call.call_multithreaded(
            kd.functor.expr_fn(kde.call(I.fn, x=I.u, y=I.v)),
            fn=fn,
            u=2,
            v=3,
        ),
        ds(5),
    )

  def test_call_with_computed_functor(self):
    fn = kd.functor.expr_fn(I.x + I.y)
    testing.assert_equal(
        call.call_multithreaded(
            kd.functor.expr_fn(kde.call(I.my_functors.fn, x=I.u, y=I.v)),
            my_functors=kd.new(fn=fn),
            u=2,
            v=3,
        ),
        ds(5),
    )

  def test_call_is_really_parallel(self):
    first_barrier = threading.Barrier(2)
    second_barrier = threading.Barrier(2)

    def wait_for_barrier_and_return_x(
        barrier: threading.Barrier, x: Any
    ) -> Any:
      barrier.wait()
      return x

    @kd.trace_as_fn()
    def f1(x):
      x = kd.apply_py(
          functools.partial(wait_for_barrier_and_return_x, second_barrier), x
      ).with_name('wait')
      return x

    @kd.trace_as_fn()
    def f2(x):
      x = kd.apply_py(
          functools.partial(wait_for_barrier_and_return_x, second_barrier), x
      ).with_name('wait')
      return x

    @kd.trace_as_fn()
    def f3(x):
      x = kd.apply_py(
          functools.partial(wait_for_barrier_and_return_x, first_barrier), x
      ).with_name('wait')
      y = f1(x)
      z = f2(x)
      return x + y + z

    @kd.trace_as_fn()
    def f4(x):
      x = kd.apply_py(
          functools.partial(wait_for_barrier_and_return_x, first_barrier), x
      ).with_name('wait')
      return x

    @kd.trace_as_fn()
    def f5(x):
      return f3(x) + f4(x)

    testing.assert_equal(call.call_multithreaded(kd.fn(f5), x=ds(1)), ds(4))

  def test_debug(self):

    def sleep_and_return_x(sleep_time: kd.types.DataItem, x: Any) -> Any:
      time.sleep(sleep_time.to_py())
      return x

    @kd.trace_as_fn()
    def f1(x):
      x = kd.apply_py(sleep_and_return_x, 0.1, x)
      return x

    @kd.trace_as_fn()
    def f2(x):
      x = kd.apply_py(sleep_and_return_x, 0.1, x)
      x = f1(x)
      x = kd.apply_py(sleep_and_return_x, 0.1, x)
      return x

    fn = kd.fn(f2)
    res, debug = call.call_multithreaded_with_debug(fn, x=ds(1))
    testing.assert_equal(res, ds(1))
    self.assertFalse(debug.is_mutable())
    self.assertEqual(debug.name, '<root>')
    self.assertLen(debug.children[:].L, 1)
    self.assertEqual(debug.children[0].name, 'f2_result')
    self.assertLen(debug.children[0].children[:].L, 1)
    self.assertEqual(debug.children[0].children[0].name, 'f1_result')
    self.assertEmpty(debug.children[0].children[0].children[:].L)
    self.assertLess(debug.start_time, debug.children[0].start_time)
    self.assertGreater(debug.end_time, debug.children[0].end_time)
    self.assertLess(
        debug.children[0].start_time, debug.children[0].children[0].start_time
    )
    self.assertGreater(
        debug.children[0].end_time, debug.children[0].children[0].end_time
    )
    self.assertLess(
        debug.children[0].children[0].start_time,
        debug.children[0].children[0].end_time,
    )
    # To make sure the units are seconds. Expected time is 0.3s, so we allow
    # a range to account for the overhead.
    self.assertLess(0.3, debug.end_time - debug.start_time)
    self.assertGreater(1.0, debug.end_time - debug.start_time)


if __name__ == '__main__':
  absltest.main()
