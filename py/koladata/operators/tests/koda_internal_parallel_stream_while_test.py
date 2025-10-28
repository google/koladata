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

import math
import random
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
from koladata.functor import boxing as _
from koladata.functor import functions
from koladata.functor.parallel import clib as stream_clib
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import py_boxing
from koladata.types import qtypes


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kdf = functions.functor
expr_fn = kdf.expr_fn
py_fn = user_facing_kd.py_fn
koda_internal_parallel = kde_operators.internal.parallel


def delayed_stream_make(*items, value_type_as=None, delay_per_item=0.005):
  items = list(map(py_boxing.as_qvalue, items))
  if items:
    value_qtype = items[0].qtype
  elif value_type_as is not None:
    value_qtype = value_type_as.qtype
  else:
    value_qtype = qtypes.DATA_SLICE
  result, writer = stream_clib.Stream.new(value_qtype)

  def delay_fn():
    try:
      for item in items:
        # randomize using the exponential distribution
        time.sleep(-math.log(1.0 - random.random()) * delay_per_item)
        writer.write(item)
      time.sleep(-math.log(1.0 - random.random()) * delay_per_item)
      writer.close()
    except Exception as e:  # pylint: disable=broad-exception-caught
      writer.close(e)

  threading.Thread(target=delay_fn, daemon=True).start()
  return result


class KodaInternalParallelStreamWhileTest(parameterized.TestCase):

  def test_while_returns(self):
    factorial = kdf.fn(
        lambda n: koda_internal_parallel.stream_while(
            koda_internal_parallel.get_default_executor(),
            lambda n, returns: n > 0,
            lambda n, returns: user_facing_kd.namedtuple(
                returns=returns * n,
                n=n - 1,
            ),
            n=n,
            returns=1,
        )
    )
    [returns] = factorial(n=5, return_type_as=delayed_stream_make()).read_all(
        timeout=1
    )
    testing.assert_equal(returns, ds(120))

  def test_while_returns_databag(self):
    factorial = kdf.fn(
        lambda n, x: koda_internal_parallel.stream_while(
            koda_internal_parallel.get_default_executor(),
            lambda x, returns: x.updated(returns).n > 0,
            lambda x, returns: user_facing_kd.namedtuple(
                returns=user_facing_kd.attrs(
                    x,
                    n=x.updated(returns).n - 1,
                    result=x.updated(returns).result * x.updated(returns).n,
                )
            ),
            x=x,
            returns=user_facing_kd.attrs(x, n=n, result=1),
        )
    )
    x = user_facing_kd.new()
    [returns] = factorial(
        n=5, x=x, return_type_as=delayed_stream_make(user_facing_kd.bag())
    ).read_all(timeout=1)
    self.assertEqual(x.updated(returns).result.to_py(), 120)

  def test_while_returns_with_databag_variable(self):
    def _factorial(n):
      x = user_facing_kd.new()
      initial_state_bag = user_facing_kd.attrs(
          x,
          n=n,
          result=1,
      )

      def _body_fn(x, state_bag, returns):
        _ = returns

        last_x = x.updated(state_bag)
        next_state_bag = user_facing_kd.attrs(
            x,
            n=last_x.n - 1,
            result=last_x.result * last_x.n,
        )
        return user_facing_kd.namedtuple(
            state_bag=next_state_bag,
            returns=x.updated(next_state_bag).result,
        )

      return koda_internal_parallel.stream_while(
          koda_internal_parallel.get_default_executor(),
          lambda x, state_bag, returns: x.updated(state_bag).n > 0,
          _body_fn,
          x=x,
          state_bag=initial_state_bag,
          returns=None,
      )

    factorial = kdf.fn(_factorial)
    [returns] = factorial(n=5, return_type_as=delayed_stream_make()).read_all(
        timeout=1
    )
    self.assertEqual(returns.to_py(), 120)

  def test_while_yields(self):
    factorial = kdf.fn(
        lambda n: koda_internal_parallel.stream_while(
            koda_internal_parallel.get_default_executor(),
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.namedtuple(
                yields=koda_internal_parallel.stream_make(res * n),
                n=n - 1,
                res=res * n,
            ),
            n=n,
            res=1,
            yields=koda_internal_parallel.stream_make(),
        )
    )
    self.assertEqual(
        factorial(n=5, return_type_as=delayed_stream_make()).read_all(
            timeout=1
        ),
        [5, 20, 60, 120, 120],
    )

  def test_while_yields_no_yield_in_body(self):
    factorial = kdf.fn(
        lambda n: koda_internal_parallel.stream_while(
            koda_internal_parallel.get_default_executor(),
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.namedtuple(
                n=n - 1,
                res=res * n,
            ),
            n=n,
            res=1,
            yields=koda_internal_parallel.stream_make(5),
        )
    )
    self.assertEqual(
        factorial(n=5, return_type_as=delayed_stream_make()).read_all(
            timeout=1
        ),
        [5],
    )

  def test_while_yields_databag(self):
    def _factorial(n, x):
      initial_update_bag = user_facing_kd.attrs(
          x,
          n=n,
          result=1,
      )

      def _body_fn(x):
        update_bag = user_facing_kd.attrs(x, n=x.n - 1, result=x.result * x.n)
        return user_facing_kd.namedtuple(
            x=x.updated(update_bag),
            yields=koda_internal_parallel.stream_make(update_bag),
        )

      return koda_internal_parallel.stream_while(
          koda_internal_parallel.get_default_executor(),
          lambda x: x.n > 0,
          _body_fn,
          x=x.updated(initial_update_bag),
          yields=koda_internal_parallel.stream_make(initial_update_bag),
      )

    factorial = kdf.fn(_factorial)

    x = user_facing_kd.new()
    steps = []
    for step_bag in factorial(
        n=5,
        x=x,
        return_type_as=delayed_stream_make(user_facing_kd.bag()),
    ).read_all(timeout=1):
      step_x = x.updated(step_bag)
      steps.append([step_x.n.to_py(), step_x.result.to_py()])
    self.assertEqual(
        steps,
        [
            [5, 1],
            [4, 5],
            [3, 20],
            [2, 60],
            [1, 120],
            [0, 120],
        ],
    )

  def test_while_yields_interleaved(self):
    factorial = kdf.fn(
        lambda n: koda_internal_parallel.stream_while(
            koda_internal_parallel.get_default_executor(),
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.namedtuple(
                yields_interleaved=koda_internal_parallel.stream_make(res * n),
                n=n - 1,
                res=res * n,
            ),
            n=n,
            res=1,
            yields_interleaved=koda_internal_parallel.stream_make(),
        ),
    )
    self.assertCountEqual(
        [
            x.to_py()
            for x in factorial(
                n=5, return_type_as=delayed_stream_make()
            ).read_all(timeout=1)
        ],
        [5, 20, 60, 120, 120],
    )

  def test_while_yields_interleaved_no_yield_in_body(self):
    factorial = kdf.fn(
        lambda n: koda_internal_parallel.stream_while(
            koda_internal_parallel.get_default_executor(),
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.namedtuple(
                n=n - 1,
                res=res * n,
            ),
            n=n,
            res=1,
            yields_interleaved=koda_internal_parallel.stream_make(5, 7),
        )
    )
    self.assertEqual(
        factorial(n=5, return_type_as=delayed_stream_make()).read_all(
            timeout=1
        ),
        [5, 7],
    )

  def test_while_yields_interleaved_order(self):
    stream0, writer0 = stream_clib.Stream.new(qtypes.DATA_SLICE)
    stream1, writer1 = stream_clib.Stream.new(qtypes.DATA_SLICE)
    stream2, writer2 = stream_clib.Stream.new(qtypes.DATA_SLICE)

    condition_fn = expr_fn(I.n < 2)

    def body_fn(*, n):
      return arolla.namedtuple(
          yields_interleaved=[stream1, stream2][n], n=n + 1
      )

    res = koda_internal_parallel.stream_while(
        koda_internal_parallel.get_eager_executor(),
        condition_fn,
        py_fn(body_fn, return_type_as=body_fn(n=ds(0))),
        yields_interleaved=stream0,
        n=0,
    ).eval()

    reader = res.make_reader()
    self.assertEqual(reader.read_available(), [])
    writer0.write(ds(1))
    self.assertEqual(reader.read_available(), [1])
    writer1.write(ds(2))
    self.assertEqual(reader.read_available(), [2])
    writer2.write(ds(3))
    self.assertEqual(reader.read_available(), [3])
    writer0.close()
    self.assertEqual(reader.read_available(), [])
    writer1.close()
    self.assertEqual(reader.read_available(), [])
    writer2.close()
    self.assertIsNone(reader.read_available())

  def test_while_yields_chained_order(self):
    stream0, writer0 = stream_clib.Stream.new(qtypes.DATA_SLICE)
    stream1, writer1 = stream_clib.Stream.new(qtypes.DATA_SLICE)
    stream2, writer2 = stream_clib.Stream.new(qtypes.DATA_SLICE)

    condition_fn = expr_fn(I.n < 2)

    def body_fn(*, n):
      return arolla.namedtuple(yields=[stream1, stream2][n], n=n + 1)

    res = koda_internal_parallel.stream_while(
        koda_internal_parallel.get_eager_executor(),
        condition_fn,
        py_fn(body_fn, return_type_as=body_fn(n=ds(0))),
        yields=stream0,
        n=0,
    ).eval()

    reader = res.make_reader()
    self.assertEqual(reader.read_available(), [])
    writer0.write(ds(1))
    self.assertEqual(reader.read_available(), [1])
    writer1.write(ds(2))
    self.assertEqual(reader.read_available(), [])
    writer2.write(ds(3))
    self.assertEqual(reader.read_available(), [])
    writer0.close()
    self.assertEqual(reader.read_available(), [2])
    writer1.close()
    self.assertEqual(reader.read_available(), [3])
    self.assertEqual(reader.read_available(), [])
    writer2.close()
    self.assertIsNone(reader.read_available())

  def test_cancel(self):
    expr = koda_internal_parallel.stream_while(
        koda_internal_parallel.get_default_executor(),
        kdf.fn(lambda returns: mask_constants.present),
        kdf.fn(
            lambda returns: arolla.M.core._identity_with_cancel(
                user_facing_kd.namedtuple(), 'cancelled'
            )
        ),
        returns=None,
    )
    with self.assertRaisesRegex(ValueError, re.escape('cancelled')):
      expr_eval.eval(expr).read_all(timeout=1)

  def test_no_return_yield(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'exactly one of `returns`, `yields`, or `yields_interleaved` must'
            ' be specified'
        ),
    ):
      _ = koda_internal_parallel.stream_while(
          koda_internal_parallel.get_default_executor(),
          lambda **unused_kwargs: user_facing_kd.present,
          lambda **unused_kwargs: user_facing_kd.namedtuple(),
      )

  def test_return_and_yield(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'exactly one of `returns`, `yields`, or `yields_interleaved` must'
            ' be specified'
        ),
    ):
      _ = koda_internal_parallel.stream_while(
          koda_internal_parallel.get_default_executor(),
          lambda **unused_kwargs: user_facing_kd.present,
          lambda **unused_kwargs: user_facing_kd.namedtuple(),
          returns=1,
          yields=delayed_stream_make(),
      )

  def test_return_outside_yield_inside_body(self):
    loop_expr = koda_internal_parallel.stream_while(
        koda_internal_parallel.get_default_executor(),
        lambda **unused_kwargs: user_facing_kd.present,
        lambda **unused_kwargs: user_facing_kd.namedtuple(
            yields=koda_internal_parallel.stream_make()
        ),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "unexpected variable 'yields'; the body functor must return a"
            ' namedtuple with a subset of initial variables'
        ),
    ):
      _ = expr_eval.eval(loop_expr).read_all(timeout=1)

  def test_wrong_type_for_variable(self):
    loop_expr = koda_internal_parallel.stream_while(
        koda_internal_parallel.get_default_executor(),
        lambda **unused_kwargs: user_facing_kd.present,
        lambda **unused_kwargs: user_facing_kd.namedtuple(
            returns=user_facing_kd.tuple(1, 2)
        ),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "variable 'returns' has type DATA_SLICE, but the provided value has"
            ' type tuple<DATA_SLICE,DATA_SLICE>; the body functor must return a'
            ' namedtuple with a subset of initial variables'
        ),
    ):
      _ = expr_eval.eval(loop_expr).read_all(timeout=1)

  def test_wrong_inner_type_for_yields(self):
    loop_expr = koda_internal_parallel.stream_while(
        koda_internal_parallel.get_default_executor(),
        lambda **unused_kwargs: user_facing_kd.present,
        lambda **unused_kwargs: user_facing_kd.namedtuple(
            yields=koda_internal_parallel.stream_make(user_facing_kd.bag())
        ),
        yields=koda_internal_parallel.stream_make(),
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "variable 'yields' has type STREAM[DATA_SLICE], but the provided"
            ' value has type STREAM[DATA_BAG]; the body functor must return a'
            " namedtuple with a subset of initial variables and 'yields'"
        ),
    ):
      _ = expr_eval.eval(loop_expr).read_all(timeout=1)

  def test_return_dataslice(self):
    loop_expr = koda_internal_parallel.stream_while(
        koda_internal_parallel.get_default_executor(),
        lambda **unused_kwargs: user_facing_kd.present,
        lambda **unused_kwargs: 2,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a namedtupe with a subset of initial variables, got type'
            ' DATA_SLICE; the body functor must return a namedtuple with a'
            ' subset of initial variables'
        ),
    ):
      _ = expr_eval.eval(loop_expr).read_all(timeout=1)

  def test_yield_dataslice(self):
    loop_expr = koda_internal_parallel.stream_while(
        koda_internal_parallel.get_default_executor(),
        lambda **unused_kwargs: user_facing_kd.present,
        lambda **unused_kwargs: 2,
        yields=koda_internal_parallel.stream_make(1),
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a namedtupe with a subset of initial variables, got type'
            ' DATA_SLICE; the body functor must return a namedtuple with a'
            " subset of initial variables and 'yields'"
        ),
    ):
      _ = expr_eval.eval(loop_expr).read_all(timeout=1)

  def test_non_mask_condition(self):
    loop_expr = koda_internal_parallel.stream_while(
        koda_internal_parallel.get_default_executor(),
        lambda **unused_kwargs: 1,
        lambda **unused_kwargs: user_facing_kd.namedtuple(),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the condition value must be a data-item with schema MASK, got'
            ' DataItem(1, schema: INT32)'
        ),
    ):
      _ = expr_eval.eval(loop_expr).read_all(timeout=1)

  def test_non_scalar_condition(self):
    loop_expr = koda_internal_parallel.stream_while(
        koda_internal_parallel.get_default_executor(),
        lambda **unused_kwargs: user_facing_kd.slice(
            [user_facing_kd.missing]
        ).no_bag(),
        lambda **unused_kwargs: user_facing_kd.namedtuple(),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the condition value must be a data-item with schema MASK, got'
            ' DataSlice([missing], schema: MASK, shape: JaggedShape(1))'
        ),
    ):
      _ = expr_eval.eval(loop_expr).read_all(timeout=1)

  def test_non_data_slice_body(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected DATA_SLICE, got body_fn: namedtuple<>'),
    ):
      _ = koda_internal_parallel.stream_while(
          koda_internal_parallel.get_default_executor(),
          lambda **unused_kwargs: user_facing_kd.present,
          kde.namedtuple(),
          returns=1,
      )

  def test_non_functor_body(self):
    loop_expr = koda_internal_parallel.stream_while(
        koda_internal_parallel.get_default_executor(),
        lambda **unused_kwargs: user_facing_kd.present,
        57,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape('the first argument of kd.call must be a functor'),
    ):
      _ = expr_eval.eval(loop_expr).read_all(timeout=1)

  def test_non_data_slice_condition(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected DATA_SLICE, got condition_fn: namedtuple<>'),
    ):
      _ = koda_internal_parallel.stream_while(
          koda_internal_parallel.get_default_executor(),
          kde.namedtuple(),
          lambda **unused_kwargs: user_facing_kd.namedtuple(),
          returns=1,
      )

  def test_non_functor_condition(self):
    loop_expr = koda_internal_parallel.stream_while(
        koda_internal_parallel.get_default_executor(),
        user_facing_kd.present,
        lambda **unused_kwargs: user_facing_kd.namedtuple(),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape('the first argument of kd.call must be a functor'),
    ):
      _ = expr_eval.eval(loop_expr).read_all(timeout=1)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.stream_while(
                I.executor, I.condition, I.body, returns=I.returns, x=I.x
            )
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            koda_internal_parallel.stream_while(
                I.executor,
                I.condition,
                I.body,
                returns=I.returns,
                yields=I.yields,
                yields_interleaved=I.yields_interleaved,
                x=I.x,
            )
        ),
        'koda_internal.parallel.stream_while(I.executor, I.condition,'
        ' I.body, returns=I.returns, yields=I.yields,'
        ' yields_interleaved=I.yields_interleaved, x=I.x)',
    )


if __name__ == '__main__':
  absltest.main()
