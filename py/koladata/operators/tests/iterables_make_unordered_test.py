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


class IterablesMakeUnorderedTest(absltest.TestCase):

  def test_make_unordered(self):
    res = expr_eval.eval(kde.iterables.make_unordered(1, 2))
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = list(res)
    self.assertLen(res_list, 2)
    self.assertCountEqual([x.to_py() for x in res_list], [1, 2])

  def test_make_unordered_does_not_have_fixed_order(self):
    expr = kde.iterables.make_unordered(1, 2)
    seen = data_bag.DataBag.empty().dict()
    # This has probability 2**(-99) of failing.
    for _ in range(100):
      res = expr_eval.eval(expr)
      first = list(res)[0]
      seen[first] = (seen[first] | 0) + 1
    self.assertGreater(seen[1], 0)
    self.assertGreater(seen[2], 0)

  def test_make_unordered_with_bags(self):
    db1 = data_bag.DataBag.empty()
    res = expr_eval.eval(kde.iterables.make_unordered(db1))
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_BAG)
    res_list = list(res)
    self.assertLen(res_list, 1)
    testing.assert_equal(res_list[0], db1)

  def test_make_unordered_with_bags_explicit_value_type_as(self):
    db1 = data_bag.DataBag.empty()
    res = expr_eval.eval(
        kde.iterables.make_unordered(db1, value_type_as=data_bag.DataBag)
    )
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_BAG)
    res_list = list(res)
    self.assertLen(res_list, 1)
    testing.assert_equal(res_list[0], db1)

  def test_make_unordered_with_bags_wrong_value_type_as(self):
    with self.assertRaisesRegex(ValueError, 'should be all of the same type'):
      _ = expr_eval.eval(
          kde.iterables.make_unordered(
              data_bag.DataBag.empty(), value_type_as=ds(1)
          )
      )

  def test_make_unordered_mixed_types(self):
    with self.assertRaisesRegex(ValueError, 'should be all of the same type'):
      _ = expr_eval.eval(
          kde.iterables.make_unordered(1, data_bag.DataBag.empty())
      )

  def test_make_unordered_empty(self):
    res = expr_eval.eval(kde.iterables.make_unordered())
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_SLICE)
    self.assertEmpty(list(res))

  def test_make_unordered_empty_with_value_type_as(self):
    res = expr_eval.eval(
        kde.iterables.make_unordered(value_type_as=data_bag.DataBag)
    )
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_BAG)
    self.assertEmpty(list(res))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.iterables.make_unordered()))

  def test_repr(self):
    self.assertEqual(
        repr(kde.iterables.make_unordered(I.a, I.b)),
        'kd.iterables.make_unordered(I.a, I.b, value_type_as=unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
