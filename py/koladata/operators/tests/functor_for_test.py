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
from koladata import kd as user_facing_kd
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import iterable_qvalue
from koladata.types import mask_constants

ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
I = input_container.InputContainer('I')


class FunctorForTest(absltest.TestCase):

  def test_for_simple(self):
    product = kde.functor.for_(
        I.input_seq,
        lambda item, other, returns: user_facing_kd.make_namedtuple(
            returns=returns * item,
        ),
        returns=1,
        other=2,
    )
    testing.assert_equal(
        product.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6))), ds(120)
    )

  def test_for_early_stop(self):
    product = kde.functor.for_(
        I.input_seq,
        lambda item, returns: user_facing_kd.make_namedtuple(
            returns=returns * item,
        ),
        condition_fn=lambda returns: returns < 10,
        returns=1,
    )
    testing.assert_equal(
        product.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6))), ds(24)
    )

  def test_for_finalize(self):
    product = kde.functor.for_(
        I.input_seq,
        lambda item, returns: user_facing_kd.make_namedtuple(
            returns=returns * item,
        ),
        finalize_fn=lambda returns: user_facing_kd.make_namedtuple(
            returns=-returns
        ),
        returns=1,
    )
    testing.assert_equal(
        product.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6))), ds(-120)
    )

  def test_for_finalize_empty_iterable(self):
    product = kde.functor.for_(
        I.input_seq,
        lambda item, returns: user_facing_kd.make_namedtuple(
            returns=returns * item,
        ),
        finalize_fn=lambda returns: user_facing_kd.make_namedtuple(
            returns=-returns
        ),
        returns=1,
    )
    testing.assert_equal(
        product.eval(input_seq=iterable_qvalue.Iterable()), ds(-1)
    )

  def test_for_early_stop_before_first_iteration(self):
    product = kde.functor.for_(
        I.input_seq,
        lambda item, returns: user_facing_kd.make_namedtuple(
            returns=returns * item,
        ),
        condition_fn=lambda returns: returns < 0,
        returns=1,
    )
    testing.assert_equal(
        product.eval(input_seq=iterable_qvalue.Iterable(*range(2, 6))), ds(1)
    )

  def test_for_early_stop_after_last_iteration_finalize_not_called(self):
    product = kde.functor.for_(
        I.input_seq,
        lambda item, returns: user_facing_kd.make_namedtuple(
            returns=returns * item,
        ),
        finalize_fn=lambda returns: user_facing_kd.make_namedtuple(
            returns=-returns
        ),
        condition_fn=lambda returns: returns < 120,
        returns=1,
    )
    testing.assert_equal(
        product.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6))), ds(120)
    )

  def test_for_no_return_statement(self):
    product = kde.functor.for_(
        I.input_seq,
        lambda item, other, returns: user_facing_kd.make_namedtuple(
            other=other * item,
        ),
        returns=1,
        other=2,
    )
    testing.assert_equal(
        product.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6))), ds(1)
    )

  def test_for_yields(self):
    product = kde.functor.for_(
        I.input_seq,
        lambda item, res: user_facing_kd.make_namedtuple(
            yields=user_facing_kd.iterables.make(res * item),
            res=res * item,
        ),
        res=1,
        yields=kde.iterables.make(),
    )
    testing.assert_equal(
        product.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6))),
        iterable_qvalue.Iterable(ds(1), ds(2), ds(6), ds(24), ds(120)),
    )

  def test_for_yields_interleaved(self):
    product = kde.functor.for_(
        I.input_seq,
        lambda item, res: user_facing_kd.make_namedtuple(
            yields_interleaved=user_facing_kd.iterables.make(res * item),
            res=res * item,
        ),
        res=1,
        yields_interleaved=kde.iterables.make(),
    )
    self.assertCountEqual(
        [
            x.to_py()
            for x in product.eval(
                input_seq=iterable_qvalue.Iterable(*range(1, 6))
            )
        ],
        [1, 2, 6, 24, 120],
    )

  def test_for_yields_no_yield_statement(self):
    product = kde.functor.for_(
        I.input_seq,
        lambda item, res: user_facing_kd.make_namedtuple(
            res=res * item,
        ),
        res=1,
        yields=kde.iterables.make(5),
    )
    testing.assert_equal(
        product.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6))),
        iterable_qvalue.Iterable(ds(5)),
    )

  def test_for_yields_interleaved_no_yield_statement(self):
    product = kde.functor.for_(
        I.input_seq,
        lambda item, res: user_facing_kd.make_namedtuple(
            res=res * item,
        ),
        res=1,
        yields_interleaved=kde.iterables.make(5, 7),
    )
    testing.assert_equal(
        product.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6))),
        iterable_qvalue.Iterable(ds(5), ds(7)),
    )

  def test_for_returns_bag(self):
    many_attrs = kde.functor.for_(
        I.input_seq,
        lambda item, root, returns: user_facing_kd.make_namedtuple(
            returns=user_facing_kd.enriched_bag(
                returns,
                user_facing_kd.attr(
                    root, user_facing_kd.fstr(f'x{item:s}'), item
                ),
            ),
        ),
        returns=kde.bag(),
        root=I.root,
    )
    root = kde.new().eval()
    self.assertEqual(
        root.updated(
            many_attrs.eval(
                input_seq=iterable_qvalue.Iterable(*range(1, 4)), root=root
            )
        ).to_py(obj_as_dict=True),
        {'x1': 1, 'x2': 2, 'x3': 3},
    )

  def test_for_yields_bag(self):
    many_attrs = kde.functor.for_(
        I.input_seq,
        lambda item, root: user_facing_kd.make_namedtuple(
            yields=user_facing_kd.iterables.make(
                user_facing_kd.attr(
                    root, user_facing_kd.fstr(f'x{item:s}'), item
                ),
            ),
        ),
        yields=kde.iterables.make(value_type_as=kde.bag()),
        root=I.root,
    )
    root = kde.new().eval()
    self.assertEqual(
        root.updated(
            *many_attrs.eval(
                input_seq=iterable_qvalue.Iterable(*range(1, 4)), root=root
            )
        ).to_py(obj_as_dict=True),
        {'x1': 1, 'x2': 2, 'x3': 3},
    )

  def test_no_return_yield(self):
    with self.assertRaisesRegex(
        ValueError,
        'exactly one of `returns`, `yields`, or `yields_interleaved` must be'
        ' specified',
    ):
      _ = kde.functor.for_(
          I.input_seq,
          lambda item: user_facing_kd.make_namedtuple(),
      )

  def test_return_and_yield(self):
    with self.assertRaisesRegex(
        ValueError,
        'exactly one of `returns`, `yields`, or `yields_interleaved` must be'
        ' specified',
    ):
      _ = kde.functor.for_(
          I.input_seq,
          lambda item: user_facing_kd.make_namedtuple(),
          returns=1,
          yields=kde.iterables.make(),
      )

  def test_return_outside_yield_inside_body(self):
    loop_expr = kde.functor.for_(
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.make_namedtuple(
            yields=kde.iterables.make()
        ),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        'the functor returned a namedtuple with field `yields`, but the'
        ' original namedtuple does not have such a field',
    ):
      _ = loop_expr.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6)))

  def test_unknown_variable_inside_finalize(self):
    loop_expr = kde.functor.for_(
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.make_namedtuple(),
        finalize_fn=lambda **unused_kwargs: user_facing_kd.make_namedtuple(
            foo=1
        ),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        'the functor returned a namedtuple with field `foo`, but the'
        ' original namedtuple does not have such a field',
    ):
      _ = loop_expr.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6)))

  def test_wrong_type_for_variable(self):
    loop_expr = kde.functor.for_(
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.make_namedtuple(
            returns=user_facing_kd.make_tuple(1, 2)
        ),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        'the functor returned a namedtuple with field `returns` of type'
        ' `tuple<DATA_SLICE,DATA_SLICE>`, but the original namedtuple has type'
        ' `DATA_SLICE` for it',
    ):
      _ = loop_expr.eval(input_seq=iterable_qvalue.Iterable(*range(1, 2)))

  def test_wrong_type_for_yields_interleaved(self):
    loop_expr = kde.functor.for_(
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.make_namedtuple(
            yields_interleaved=user_facing_kd.iterables.make(
                user_facing_kd.bag()
            )
        ),
        yields_interleaved=kde.iterables.make(),
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the functor returned a namedtuple with field `yields_interleaved`'
            ' of type `ITERABLE[DATA_BAG]`, but the original namedtuple has'
            ' type `ITERABLE[DATA_SLICE]` for it'
        ),
    ):
      _ = loop_expr.eval(input_seq=iterable_qvalue.Iterable(*range(1, 2)))

  def test_return_dataslice(self):
    loop_expr = kde.functor.for_(
        I.input_seq,
        lambda item, **unused_kwargs: 2,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        'the functor must return a namedtuple, but it returned `DATA_SLICE`',
    ):
      _ = loop_expr.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6)))

  def test_non_mask_condition(self):
    loop_expr = kde.functor.for_(
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.make_namedtuple(),
        condition_fn=lambda **unused_kwargs: 1,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'condition_fn must return a MASK DataItem, got DataItem(1, schema:'
            ' INT32)'
        ),
    ):
      _ = loop_expr.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6)))

  def test_non_scalar_condition(self):
    loop_expr = kde.functor.for_(
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.make_namedtuple(),
        condition_fn=lambda **unused_kwargs: user_facing_kd.slice(
            [mask_constants.missing]
        ),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'condition_fn must return a MASK DataItem, got DataSlice([missing]'
        ),
    ):
      _ = loop_expr.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6)))

  def test_non_iterable(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected an iterable type, got iterable: DATA_SLICE',
    ):
      _ = kde.functor.for_(
          kde.slice([1, 2, 3]),
          lambda item: user_facing_kd.make_namedtuple(),
          returns=1,
      )

  def test_non_data_slice_body(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE, got body_fn: namedtuple<>',
    ):
      _ = kde.functor.for_(
          I.input_seq,
          kde.make_namedtuple(),
          returns=1,
      )

  def test_non_functor_body(self):
    loop_expr = kde.functor.for_(
        I.input_seq,
        57,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        'the first argument of kd.call must be a functor',
    ):
      _ = loop_expr.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6)))

  def test_non_data_slice_condition(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE, got condition_fn: namedtuple<>',
    ):
      _ = kde.functor.for_(
          I.input_seq,
          lambda item, **unused_kwargs: user_facing_kd.make_namedtuple(),
          condition_fn=kde.make_namedtuple(),
          returns=1,
      )

  def test_non_functor_condition(self):
    loop_expr = kde.functor.for_(
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.make_namedtuple(),
        condition_fn=mask_constants.present,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        'the first argument of kd.call must be a functor',
    ):
      _ = loop_expr.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6)))

  def test_non_data_slice_finalize(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE, got finalize_fn: namedtuple<>',
    ):
      _ = kde.functor.for_(
          I.input_seq,
          lambda item: user_facing_kd.make_namedtuple(),
          finalize_fn=kde.make_namedtuple(),
          returns=1,
      )

  def test_non_functor_finalize(self):
    loop_expr = kde.functor.for_(
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.make_namedtuple(),
        finalize_fn=mask_constants.present,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        'the first argument of kd.call must be a functor',
    ):
      _ = loop_expr.eval(input_seq=iterable_qvalue.Iterable(*range(1, 6)))

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.functor.for_(I.input_seq, I.body_fn, returns=I.returns)
        )
    )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.functor.for_, kde.for_))


if __name__ == '__main__':
  absltest.main()
