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


class IterablesMakeTest(absltest.TestCase):

  def test_make(self):
    res = expr_eval.eval(kde.iterables.make(1, 2))
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = list(res)
    self.assertLen(res_list, 2)
    testing.assert_equal(res_list[0], ds(1))
    testing.assert_equal(res_list[1], ds(2))

  def test_make_with_bags(self):
    db1 = data_bag.DataBag.empty()
    db2 = data_bag.DataBag.empty()
    res = expr_eval.eval(kde.iterables.make(db1, db2))
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_BAG)
    res_list = list(res)
    self.assertLen(res_list, 2)
    testing.assert_equal(res_list[0], db1)
    testing.assert_equal(res_list[1], db2)

  def test_make_with_bags_explicit_value_type_as(self):
    db1 = data_bag.DataBag.empty()
    db2 = data_bag.DataBag.empty()
    res = expr_eval.eval(
        kde.iterables.make(db1, db2, value_type_as=data_bag.DataBag)
    )
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_BAG)
    res_list = list(res)
    self.assertLen(res_list, 2)
    testing.assert_equal(res_list[0], db1)
    testing.assert_equal(res_list[1], db2)

  def test_make_with_bags_wrong_value_type_as(self):
    db1 = data_bag.DataBag.empty()
    db2 = data_bag.DataBag.empty()
    with self.assertRaisesRegex(ValueError, 'should be all of the same type'):
      _ = expr_eval.eval(kde.iterables.make(db1, db2, value_type_as=ds(1)))

  def test_make_mixed_types(self):
    with self.assertRaisesRegex(ValueError, 'should be all of the same type'):
      _ = expr_eval.eval(kde.iterables.make(1, data_bag.DataBag.empty()))

  def test_make_empty(self):
    res = expr_eval.eval(kde.iterables.make())
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_SLICE)
    self.assertEmpty(list(res))

  def test_make_empty_with_value_type_as(self):
    res = expr_eval.eval(kde.iterables.make(value_type_as=data_bag.DataBag))
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_BAG)
    self.assertEmpty(list(res))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.iterables.make()))

  def test_repr(self):
    self.assertEqual(
        repr(kde.iterables.make(I.a, I.b)),
        'kd.iterables.make(I.a, I.b, value_type_as=unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
