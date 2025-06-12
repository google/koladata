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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor import functions
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import iterable_qvalue as _
from koladata.types import mask_constants


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kdf = functions.functor


class FunctorWhileTest(parameterized.TestCase):

  def test_while_returns(self):
    factorial = kdf.fn(
        lambda n: user_facing_kd.functor.while_(
            lambda n, returns: n > 0,
            lambda n, returns: user_facing_kd.make_namedtuple(
                returns=returns * n,
                n=n - 1,
            ),
            n=n,
            returns=1,
        )
    )
    testing.assert_equal(factorial(n=5), ds(120))

  def test_while_returns_databag(self):
    factorial = kdf.fn(
        lambda n, x: user_facing_kd.functor.while_(
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
    self.assertEqual(
        x.updated(
            factorial(n=5, x=x, return_type_as=user_facing_kd.bag())
        ).result.to_py(),
        120,
    )

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

      return user_facing_kd.functor.while_(
          lambda x, state_bag, returns: x.updated(state_bag).n > 0,
          _body_fn,
          x=x,
          state_bag=initial_state_bag,
          returns=None,
      )

    factorial = kdf.fn(_factorial)
    self.assertEqual(factorial(n=5).to_py(), 120)

  def test_while_yields(self):
    factorial = kdf.fn(
        lambda n: user_facing_kd.functor.while_(
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.make_namedtuple(
                yields=user_facing_kd.iterables.make(res * n),
                n=n - 1,
                res=res * n,
            ),
            n=n,
            res=1,
            yields=user_facing_kd.iterables.make(),
        )
    )
    testing.assert_equal(
        factorial(n=5, return_type_as=user_facing_kd.iterables.make()),
        user_facing_kd.iterables.make(ds(5), ds(20), ds(60), ds(120), ds(120)),
    )

  def test_while_yields_no_yield_in_body(self):
    factorial = kdf.fn(
        lambda n: user_facing_kd.functor.while_(
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.make_namedtuple(
                n=n - 1,
                res=res * n,
            ),
            n=n,
            res=1,
            yields=user_facing_kd.iterables.make(5),
        )
    )
    testing.assert_equal(
        factorial(n=5, return_type_as=user_facing_kd.iterables.make()),
        user_facing_kd.iterables.make(ds(5)),
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
            yields=user_facing_kd.iterables.make(update_bag),
        )

      return user_facing_kd.functor.while_(
          lambda x: x.n > 0,
          _body_fn,
          x=x.updated(initial_update_bag),
          yields=user_facing_kd.iterables.make(initial_update_bag),
      )

    factorial = kdf.fn(_factorial)

    x = user_facing_kd.new()
    steps = []
    for step_bag in factorial(
        n=5,
        x=x,
        return_type_as=user_facing_kd.iterables.make(user_facing_kd.bag()),
    ):
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
        lambda n: user_facing_kd.functor.while_(
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.make_namedtuple(
                yields_interleaved=user_facing_kd.iterables.make(res * n),
                n=n - 1,
                res=res * n,
            ),
            n=n,
            res=1,
            yields_interleaved=user_facing_kd.iterables.make(),
        ),
    )
    self.assertCountEqual(
        [
            x.to_py()
            for x in factorial(
                n=5, return_type_as=user_facing_kd.iterables.make()
            )
        ],
        [5, 20, 60, 120, 120],
    )

  def test_while_yields_interleaved_no_yield_in_body(self):
    factorial = kdf.fn(
        lambda n: user_facing_kd.functor.while_(
            lambda n, res: n > 0,
            lambda n, res: user_facing_kd.make_namedtuple(
                n=n - 1,
                res=res * n,
            ),
            n=n,
            res=1,
            yields_interleaved=user_facing_kd.iterables.make(5, 7),
        )
    )
    testing.assert_equal(
        factorial(n=5, return_type_as=user_facing_kd.iterables.make()),
        user_facing_kd.iterables.make(ds(5), ds(7)),
    )

  def test_cancel(self):
    expr = kde.functor.while_(
        kdf.fn(lambda returns: mask_constants.present),
        kdf.fn(
            lambda returns: arolla.M.core._identity_with_cancel(
                user_facing_kd.make_namedtuple(), 'cancelled'
            )
        ),
        returns=None,
    )
    with self.assertRaisesRegex(ValueError, re.escape('cancelled')):
      expr_eval.eval(expr)

  def test_interleave_nondeterminism(self):
    # These exprs should be identical
    def _make_expr():
      return kde.functor.while_(
          kdf.fn(lambda i, n: i < n),
          kdf.fn(
              lambda i, n: user_facing_kd.make_namedtuple(
                  i=i + 1, yields_interleaved=user_facing_kd.iterables.make(i)
              )
          ),
          i=0,
          n=20,
          yields_interleaved=user_facing_kd.iterables.make(),
      )
    expr1 = _make_expr()
    expr2 = _make_expr()

    values = arolla.M.core.make_tuple(expr1, expr2)

    values1 = [
        v.to_py() for v in expr_eval.eval(arolla.M.core.get_nth(values, 0))
    ]
    values2 = [
        v.to_py() for v in expr_eval.eval(arolla.M.core.get_nth(values, 1))
    ]
    self.assertCountEqual(values1, range(20))
    self.assertCountEqual(values2, range(20))

    # values1 and values2 should be shuffled independently, so the probability
    # of values1 == values2 should be 1 / 20! = 4e-19.
    self.assertNotEqual(values1, values2)

  def test_no_return_yield(self):
    with self.assertRaisesRegex(
        ValueError,
        'exactly one of `returns`, `yields`, or `yields_interleaved` must be'
        ' specified',
    ):
      _ = kde.functor.while_(
          lambda **unused_kwargs: user_facing_kd.present,
          lambda **unused_kwargs: user_facing_kd.make_namedtuple(),
      )

  def test_return_and_yield(self):
    with self.assertRaisesRegex(
        ValueError,
        'exactly one of `returns`, `yields`, or `yields_interleaved` must be'
        ' specified',
    ):
      _ = kde.functor.while_(
          lambda **unused_kwargs: user_facing_kd.present,
          lambda **unused_kwargs: user_facing_kd.make_namedtuple(),
          returns=1,
          yields=kde.iterables.make(),
      )

  def test_return_outside_yield_inside_body(self):
    loop_expr = kde.functor.while_(
        lambda **unused_kwargs: user_facing_kd.present,
        lambda **unused_kwargs: user_facing_kd.make_namedtuple(
            yields=kde.iterables.make()
        ),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.functor.while_: body_fn must return a namedtuple with field'
            ' names that are a subset of the state variable names, got an'
            ' unknown field: `yields`'
        ),
    ):
      _ = expr_eval.eval(loop_expr)

  def test_wrong_type_for_variable(self):
    loop_expr = kde.functor.while_(
        lambda **unused_kwargs: user_facing_kd.present,
        lambda **unused_kwargs: user_facing_kd.make_namedtuple(
            returns=user_facing_kd.make_tuple(1, 2)
        ),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.functor.while_: expected `returns` value to have type'
            ' `DATA_SLICE`, got `tuple<DATA_SLICE,DATA_SLICE>`'
        ),
    ):
      _ = expr_eval.eval(loop_expr)

  def test_wrong_inner_type_for_yields(self):
    loop_expr = kde.functor.while_(
        lambda **unused_kwargs: user_facing_kd.present,
        lambda **unused_kwargs: user_facing_kd.make_namedtuple(
            yields=user_facing_kd.iterables.make(user_facing_kd.bag())
        ),
        yields=kde.iterables.make(),
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.functor.while_: expected `yields` value to have type'
            ' `ITERABLE[DATA_SLICE]`, got `ITERABLE[DATA_BAG]`'
        ),
    ):
      _ = expr_eval.eval(loop_expr)

  def test_return_dataslice(self):
    loop_expr = kde.functor.while_(
        lambda **unused_kwargs: user_facing_kd.present,
        lambda **unused_kwargs: 2,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.functor.while_: body_fn must return a namedtuple, got type'
            ' `DATA_SLICE`'
        ),
    ):
      _ = expr_eval.eval(loop_expr)

  def test_yield_dataslice(self):
    loop_expr = kde.functor.while_(
        lambda **unused_kwargs: user_facing_kd.present,
        lambda **unused_kwargs: 2,
        yields=user_facing_kd.iterables.make(1),
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.functor.while_: body_fn must return a namedtuple, got type'
            ' `DATA_SLICE`'
        ),
    ):
      _ = expr_eval.eval(loop_expr)

  def test_non_mask_condition(self):
    loop_expr = kde.functor.while_(
        lambda **unused_kwargs: 1,
        lambda **unused_kwargs: user_facing_kd.make_namedtuple(),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.functor.while_: condition_fn must return a MASK DataItem, got'
            ' DataItem(1, schema: INT32)'
        ),
    ):
      _ = expr_eval.eval(loop_expr)

  def test_non_scalar_condition(self):
    loop_expr = kde.functor.while_(
        lambda **unused_kwargs: user_facing_kd.slice(
            [user_facing_kd.missing]
        ).no_bag(),
        lambda **unused_kwargs: user_facing_kd.make_namedtuple(),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.functor.while_: condition_fn must return a MASK DataItem, got'
            ' DataSlice([missing], schema: MASK, shape: JaggedShape(1))'
        ),
    ):
      _ = expr_eval.eval(loop_expr)

  def test_non_data_slice_body(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE, got body_fn: namedtuple<>',
    ):
      _ = kde.functor.while_(
          lambda **unused_kwargs: user_facing_kd.present,
          kde.make_namedtuple(),
          returns=1,
      )

  def test_non_functor_body(self):
    loop_expr = kde.functor.while_(
        lambda **unused_kwargs: user_facing_kd.present,
        57,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        'the first argument of kd.call must be a functor',
    ):
      _ = expr_eval.eval(loop_expr)

  def test_non_data_slice_condition(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE, got condition_fn: namedtuple<>',
    ):
      _ = kde.functor.while_(
          kde.make_namedtuple(),
          lambda **unused_kwargs: user_facing_kd.make_namedtuple(),
          returns=1,
      )

  def test_non_functor_condition(self):
    loop_expr = kde.functor.while_(
        user_facing_kd.present,
        lambda **unused_kwargs: user_facing_kd.make_namedtuple(),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        'the first argument of kd.call must be a functor',
    ):
      _ = expr_eval.eval(loop_expr)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.functor.while_(I.condition, I.body, returns=I.returns, x=I.x)
        )
    )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.functor.while_, kde.while_))

  def test_repr(self):
    self.assertEqual(
        repr(
            kde.functor.while_(
                I.condition,
                I.body,
                returns=I.returns,
                yields=I.yields,
                yields_interleaved=I.yields_interleaved,
                x=I.x,
            )
        ),
        'kd.functor.while_(I.condition, I.body, returns=I.returns, '
        'yields=I.yields, yields_interleaved=I.yields_interleaved, x=I.x)',
    )


if __name__ == '__main__':
  absltest.main()
