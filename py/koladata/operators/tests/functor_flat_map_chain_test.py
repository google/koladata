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

from absl.testing import absltest
from koladata import kd as user_facing_kd
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.operators import kde_operators
from koladata.operators import koda_internal_iterables
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import iterable_qvalue


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
I = input_container.InputContainer('I')


class FlatMapChainTest(absltest.TestCase):

  def test_flat_map_chain(self):
    py_fn = lambda x: kde.iterables.make(x, 2 * x)

    expr = kde.functor.flat_map_chain(I.input_seq, I.fn)

    res = expr.eval(input_seq=user_facing_kd.iterables.make(1, 10), fn=py_fn)

    testing.assert_equal(res, user_facing_kd.iterables.make(1, 2, 10, 20))

  def test_flat_map_chain_with_return_type_as(self):
    py_fn = lambda x: user_facing_kd.iterables.make(x, 2 * x)

    expr = kde.functor.flat_map_chain(
        I.input_seq,
        I.fn,
        value_type_as=data_slice.DataSlice,
    )

    res = expr.eval(input_seq=iterable_qvalue.Iterable(*range(2, 5)), fn=py_fn)

    testing.assert_equal(res, user_facing_kd.iterables.make(2, 4, 3, 6, 4, 8))

  def test_flat_map_chain_with_data_bag(self):
    obj = user_facing_kd.new(a=1)
    db1 = user_facing_kd.attrs(obj, a=2)
    db2 = user_facing_kd.attrs(obj, a=3)

    def py_fn(x):
      return kde.iterables.make(x, user_facing_kd.attrs(obj, b=obj.a + 5))

    expr = kde.functor.flat_map_chain(
        I.input_seq, I.fn, value_type_as=data_bag.DataBag
    )

    res = expr.eval(input_seq=user_facing_kd.iterables.make(db1, db2), fn=py_fn)

    res = expr_eval.eval(
        koda_internal_iterables.to_sequence(I.input_seq), input_seq=res
    )
    self.assertLen(res, 4)
    self.assertEqual(obj.with_bag(res[0]).to_py(obj_as_dict=True), {'a': 2})
    self.assertEqual(obj.with_bag(res[1]).to_py(obj_as_dict=True), {'b': 6})
    self.assertEqual(obj.with_bag(res[2]).to_py(obj_as_dict=True), {'a': 3})
    self.assertEqual(obj.with_bag(res[3]).to_py(obj_as_dict=True), {'b': 6})

  def test_flat_map_chain_with_data_bag_input_as_data_slice(self):
    obj1 = user_facing_kd.new(a=1, b=2)
    obj2 = user_facing_kd.new(a=10, b=20)

    def py_fn(x):
      return kde.iterables.make(
          user_facing_kd.attrs(x, a=x.a + 1), user_facing_kd.attrs(x, b=x.b + 5)
      )

    expr = kde.functor.flat_map_chain(
        I.input_seq, I.fn, value_type_as=data_bag.DataBag
    )

    res = expr.eval(
        input_seq=user_facing_kd.iterables.make(obj1, obj2), fn=py_fn
    )

    res = expr_eval.eval(
        koda_internal_iterables.to_sequence(I.input_seq), input_seq=res
    )
    self.assertLen(res, 4)
    self.assertEqual(obj1.with_bag(res[0]).to_py(obj_as_dict=True), {'a': 2})
    self.assertEqual(obj1.with_bag(res[1]).to_py(obj_as_dict=True), {'b': 7})
    self.assertEqual(obj2.with_bag(res[2]).to_py(obj_as_dict=True), {'a': 11})
    self.assertEqual(obj2.with_bag(res[3]).to_py(obj_as_dict=True), {'b': 25})

  def test_flat_map_chain_wrong_return_type_as(self):
    py_fn = lambda x: user_facing_kd.iterables.make(x, 2 * x)

    expr = kde.functor.flat_map_chain(
        I.input_seq,
        I.fn,
        value_type_as=data_bag.DataBag,
    )

    with self.assertRaisesRegex(
        ValueError,
        'The functor was called with .+ as the output type, but the computation'
        ' resulted in type .+ instead',
    ):
      _ = expr.eval(input_seq=iterable_qvalue.Iterable(*range(2, 5)), fn=py_fn)

  def test_flat_map_chain_empty_iterable(self):
    py_fn = lambda x: user_facing_kd.iterables.make(x, 2 * x)

    expr = kde.functor.flat_map_chain(
        I.input_seq,
        I.fn,
    )

    res = expr.eval(input_seq=user_facing_kd.iterables.make(), fn=py_fn)

    testing.assert_equal(res, user_facing_kd.iterables.make())

  def test_flat_map_chain_fn_returns_empty_iterable(self):
    py_fn = lambda x: user_facing_kd.iterables.make()

    expr = kde.functor.flat_map_chain(
        I.input_seq,
        I.fn,
    )

    res = expr.eval(input_seq=user_facing_kd.iterables.make(1, 2, 3), fn=py_fn)

    testing.assert_equal(res, user_facing_kd.iterables.make())

  def test_non_iterable(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected an iterable type, got iterable: DATA_SLICE',
    ):
      py_fn = lambda x: user_facing_kd.iterables.make(x, 2 * x)

      expr = kde.functor.flat_map_chain(I.input_seq, I.fn)
      _ = expr.eval(input_seq=kde.slice([1, 2, 3]).eval(), fn=py_fn)

  def test_non_functor_fn(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE, got fn',
    ):
      expr = kde.functor.flat_map_chain(I.input_seq, I.fn)
      _ = expr.eval(
          input_seq=iterable_qvalue.Iterable(*range(2, 5)),
          fn=user_facing_kd.iterables.make(1, 2, 3),
      )

  def test_fn_does_not_return_iterable(self):
    py_fn = lambda x: x * 2

    expr = kde.functor.flat_map_chain(I.input_seq, I.fn)

    with self.assertRaisesRegex(
        ValueError,
        'The functor was called with .+ as the output type,'
        ' but the computation resulted in type .+ instead',
    ):
      _ = expr.eval(input_seq=iterable_qvalue.Iterable(*range(2, 5)), fn=py_fn)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.functor.flat_map_chain(I.input_seq, I.body_fn))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.functor.flat_map_chain, kde.flat_map_chain)
    )


if __name__ == '__main__':
  absltest.main()
