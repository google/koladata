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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import iterable_qvalue
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class IterablesInterleaveTest(absltest.TestCase):

  def test_interleave(self):
    res = expr_eval.eval(
        kde.iterables.interleave(
            kde.iterables.make(1, 2), kde.iterables.make(3)
        )
    )
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = list(res)
    self.assertLen(res_list, 3)
    self.assertCountEqual([x.to_py() for x in res_list], [1, 2, 3])
    pos_in_list = {x.to_py(): i for i, x in enumerate(res_list)}
    self.assertLess(pos_in_list[1], pos_in_list[2])

  def test_possible_orders(self):
    expr = kde.iterables.interleave(
        kde.iterables.make(1, 2), kde.iterables.make(3, 4, 5)
    )
    seen = set()
    # Assuming that each permutation has probability 1/10, then
    # we have a chance of only (1-1/10)**300<2e-14 of not seeing each
    # permutation, which is negligible.
    for _ in range(300):
      res = expr_eval.eval(expr)
      t = tuple(x.to_py() for x in res)
      seen.add(t)
    self.assertCountEqual(
        seen,
        [
            (1, 2, 3, 4, 5),
            (1, 3, 2, 4, 5),
            (1, 3, 4, 2, 5),
            (1, 3, 4, 5, 2),
            (3, 1, 2, 4, 5),
            (3, 1, 4, 2, 5),
            (3, 1, 4, 5, 2),
            (3, 4, 1, 2, 5),
            (3, 4, 1, 5, 2),
            (3, 4, 5, 1, 2),
        ],
    )

  def test_possible_orders_three_iterables(self):
    expr = kde.iterables.interleave(
        kde.iterables.make(1), kde.iterables.make(2), kde.iterables.make(3, 4)
    )
    seen = set()
    # Roughly assuming that each permutation has probability 1/12, then
    # we have a chance of only (1-1/12)**300<5e-12 of not seeing each
    # permutation, which is negligible.
    for _ in range(300):
      res = expr_eval.eval(expr)
      t = tuple(x.to_py() for x in res)
      seen.add(t)
    self.assertCountEqual(
        seen,
        [
            (1, 2, 3, 4),
            (1, 3, 2, 4),
            (1, 3, 4, 2),
            (2, 1, 3, 4),
            (2, 3, 1, 4),
            (2, 3, 4, 1),
            (3, 1, 2, 4),
            (3, 1, 4, 2),
            (3, 4, 1, 2),
            (3, 2, 1, 4),
            (3, 2, 4, 1),
            (3, 4, 2, 1),
        ],
    )

  def test_interleave_with_bags(self):
    db1 = data_bag.DataBag.empty()
    res = expr_eval.eval(
        kde.iterables.interleave(
            kde.iterables.make(db1),
            kde.iterables.make(value_type_as=data_bag.DataBag),
        )
    )
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_BAG)
    res_list = list(res)
    self.assertLen(res_list, 1)
    testing.assert_equal(res_list[0], db1)

  def test_interleave_with_bags_explicit_value_type_as(self):
    db1 = data_bag.DataBag.empty()
    res = expr_eval.eval(
        kde.iterables.interleave(
            kde.iterables.make(db1),
            kde.iterables.make(value_type_as=data_bag.DataBag),
            value_type_as=data_bag.DataBag,
        )
    )
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_BAG)
    res_list = list(res)
    self.assertLen(res_list, 1)
    testing.assert_equal(res_list[0], db1)

  def test_interleave_with_bags_wrong_value_type_as(self):
    db1 = data_bag.DataBag.empty()
    with self.assertRaisesRegex(
        ValueError,
        'when value_type_as is specified, all iterables must have that value'
        ' type',
    ):
      _ = expr_eval.eval(
          kde.iterables.interleave(kde.iterables.make(db1), value_type_as=ds(1))
      )

  def test_interleave_with_empty_bags_wrong_value_type_as(self):
    with self.assertRaisesRegex(
        ValueError,
        'when value_type_as is specified, all iterables must have that value'
        ' type',
    ):
      _ = expr_eval.eval(
          kde.iterables.interleave(
              kde.iterables.make(value_type_as=data_bag.DataBag),
              value_type_as=ds(1),
          )
      )

  def test_interleave_mixed_types(self):
    with self.assertRaisesRegex(ValueError, 'must have the same value type'):
      _ = expr_eval.eval(
          kde.iterables.interleave(
              kde.iterables.make(1),
              kde.iterables.make(data_bag.DataBag.empty()),
          )
      )

  def test_interleave_empty(self):
    res = expr_eval.eval(kde.iterables.interleave())
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_SLICE)
    self.assertEmpty(list(res))

  def test_interleave_empty_with_value_type_as(self):
    res = expr_eval.eval(
        kde.iterables.interleave(value_type_as=data_bag.DataBag)
    )
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_BAG)
    self.assertEmpty(list(res))

  def test_interleave_with_only_empty_iterable(self):
    res = expr_eval.eval(
        kde.iterables.interleave(
            kde.iterables.make(value_type_as=data_bag.DataBag)
        )
    )
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_BAG)
    self.assertEmpty(list(res))

  def test_non_iterable_arg(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('all inputs must be iterables'),
    ):
      _ = expr_eval.eval(kde.iterables.interleave(ds(1)))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.iterables.interleave()))

  def test_repr(self):
    self.assertEqual(
        repr(kde.iterables.interleave(I.a, I.b)),
        'kd.iterables.interleave(I.a, I.b, value_type_as=unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
