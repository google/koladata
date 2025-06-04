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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor import functions
from koladata.functor.parallel import clib as _
from koladata.operators import kde_operators
from koladata.operators import koda_internal_parallel
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import mask_constants


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kdf = functions.functor


def stream_make(*args, **kwargs):
  return arolla.abc.aux_eval_op(
      'koda_internal.parallel.stream_make', *args, **kwargs
  )


class KodaInternalParallelStreamWhileLoopTest(parameterized.TestCase):

  def test_while_returns(self):
    factorial = kdf.fn(
        lambda n: koda_internal_parallel.stream_while_loop(
            koda_internal_parallel.get_default_executor(),
            lambda n, returns: n > 0,
            lambda n, returns: user_facing_kd.make_namedtuple(
                returns=returns * n,
                n=n - 1,
            ),
            n=n,
            returns=1,
        )
    )
    [returns] = factorial(n=5, return_type_as=stream_make()).read_all(timeout=1)
    testing.assert_equal(returns, ds(120))

  def test_while_returns_databag(self):
    factorial = kdf.fn(
        lambda n, x: koda_internal_parallel.stream_while_loop(
            koda_internal_parallel.get_default_executor(),
            lambda x, returns: x.updated(returns).n > 0,
            lambda x, returns: user_facing_kd.make_namedtuple(
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
        n=5, x=x, return_type_as=stream_make(user_facing_kd.bag())
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
        return user_facing_kd.make_namedtuple(
            state_bag=next_state_bag,
            returns=x.updated(next_state_bag).result,
        )

      return koda_internal_parallel.stream_while_loop(
          koda_internal_parallel.get_default_executor(),
          lambda x, state_bag, returns: x.updated(state_bag).n > 0,
          _body_fn,
          x=x,
          state_bag=initial_state_bag,
          returns=None,
      )

    factorial = kdf.fn(_factorial)
    [returns] = factorial(n=5, return_type_as=stream_make()).read_all(timeout=1)
    self.assertEqual(returns.to_py(), 120)

  def test_while_yields(self):
    factorial = kdf.fn(
        lambda n: koda_internal_parallel.stream_while_loop(
            koda_internal_parallel.get_default_executor(),
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.make_namedtuple(
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
        factorial(n=5, return_type_as=stream_make()).read_all(timeout=1),
        [5, 20, 60, 120, 120],
    )

  def test_while_yields_no_yield_in_body(self):
    factorial = kdf.fn(
        lambda n: koda_internal_parallel.stream_while_loop(
            koda_internal_parallel.get_default_executor(),
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.make_namedtuple(
                n=n - 1,
                res=res * n,
            ),
            n=n,
            res=1,
            yields=koda_internal_parallel.stream_make(5),
        )
    )
    self.assertEqual(
        factorial(n=5, return_type_as=stream_make()).read_all(timeout=1),
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
        return user_facing_kd.make_namedtuple(
            x=x.updated(update_bag),
            yields=koda_internal_parallel.stream_make(update_bag),
        )

      return koda_internal_parallel.stream_while_loop(
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
        return_type_as=stream_make(user_facing_kd.bag()),
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
        lambda n: koda_internal_parallel.stream_while_loop(
            koda_internal_parallel.get_default_executor(),
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.make_namedtuple(
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
            for x in factorial(n=5, return_type_as=stream_make()).read_all(
                timeout=1
            )
        ],
        [5, 20, 60, 120, 120],
    )

  def test_while_yields_interleaved_no_yield_in_body(self):
    factorial = kdf.fn(
        lambda n: koda_internal_parallel.stream_while_loop(
            koda_internal_parallel.get_default_executor(),
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.make_namedtuple(
                n=n - 1,
                res=res * n,
            ),
            n=n,
            res=1,
            yields_interleaved=koda_internal_parallel.stream_make(5, 7),
        )
    )
    self.assertEqual(
        factorial(n=5, return_type_as=stream_make()).read_all(timeout=1),
        [5, 7],
    )

  def test_cancel(self):
    expr = koda_internal_parallel.stream_while_loop(
        koda_internal_parallel.get_default_executor(),
        kdf.fn(lambda returns: mask_constants.present),
        kdf.fn(
            lambda returns: arolla.M.core._identity_with_cancel(
                user_facing_kd.make_namedtuple(), 'cancelled'
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
      _ = koda_internal_parallel.stream_while_loop(
          koda_internal_parallel.get_default_executor(),
          lambda **unused_kwargs: user_facing_kd.present,
          lambda **unused_kwargs: user_facing_kd.make_namedtuple(),
      )

  def test_return_and_yield(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'exactly one of `returns`, `yields`, or `yields_interleaved` must'
            ' be specified'
        ),
    ):
      _ = koda_internal_parallel.stream_while_loop(
          koda_internal_parallel.get_default_executor(),
          lambda **unused_kwargs: user_facing_kd.present,
          lambda **unused_kwargs: user_facing_kd.make_namedtuple(),
          returns=1,
          yields=stream_make(),
      )

  def test_return_outside_yield_inside_body(self):
    loop_expr = koda_internal_parallel.stream_while_loop(
        koda_internal_parallel.get_default_executor(),
        lambda **unused_kwargs: user_facing_kd.present,
        lambda **unused_kwargs: user_facing_kd.make_namedtuple(
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
    loop_expr = koda_internal_parallel.stream_while_loop(
        koda_internal_parallel.get_default_executor(),
        lambda **unused_kwargs: user_facing_kd.present,
        lambda **unused_kwargs: user_facing_kd.make_namedtuple(
            returns=user_facing_kd.make_tuple(1, 2)
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
    loop_expr = koda_internal_parallel.stream_while_loop(
        koda_internal_parallel.get_default_executor(),
        lambda **unused_kwargs: user_facing_kd.present,
        lambda **unused_kwargs: user_facing_kd.make_namedtuple(
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
    loop_expr = koda_internal_parallel.stream_while_loop(
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
    loop_expr = koda_internal_parallel.stream_while_loop(
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
    loop_expr = koda_internal_parallel.stream_while_loop(
        koda_internal_parallel.get_default_executor(),
        lambda **unused_kwargs: 1,
        lambda **unused_kwargs: user_facing_kd.make_namedtuple(),
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
    loop_expr = koda_internal_parallel.stream_while_loop(
        koda_internal_parallel.get_default_executor(),
        lambda **unused_kwargs: user_facing_kd.slice(
            [user_facing_kd.missing]
        ).no_bag(),
        lambda **unused_kwargs: user_facing_kd.make_namedtuple(),
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
      _ = koda_internal_parallel.stream_while_loop(
          koda_internal_parallel.get_default_executor(),
          lambda **unused_kwargs: user_facing_kd.present,
          kde.make_namedtuple(),
          returns=1,
      )

  def test_non_functor_body(self):
    loop_expr = koda_internal_parallel.stream_while_loop(
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
      _ = koda_internal_parallel.stream_while_loop(
          koda_internal_parallel.get_default_executor(),
          kde.make_namedtuple(),
          lambda **unused_kwargs: user_facing_kd.make_namedtuple(),
          returns=1,
      )

  def test_non_functor_condition(self):
    loop_expr = koda_internal_parallel.stream_while_loop(
        koda_internal_parallel.get_default_executor(),
        user_facing_kd.present,
        lambda **unused_kwargs: user_facing_kd.make_namedtuple(),
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
            koda_internal_parallel.stream_while_loop(
                I.executor, I.condition, I.body, returns=I.returns, x=I.x
            )
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            koda_internal_parallel.stream_while_loop(
                I.executor,
                I.condition,
                I.body,
                returns=I.returns,
                yields=I.yields,
                yields_interleaved=I.yields_interleaved,
                x=I.x,
            )
        ),
        'koda_internal.parallel.stream_while_loop(I.executor, I.condition,'
        ' I.body, returns=I.returns, yields=I.yields,'
        ' yields_interleaved=I.yields_interleaved, x=I.x)',
    )


if __name__ == '__main__':
  absltest.main()
