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
from arolla import arolla
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import iterable_qvalue
from koladata.types import qtypes


ds = data_slice.DataSlice.from_vals


class IterableQValueTest(absltest.TestCase):

  def test_basic(self):
    res = iterable_qvalue.Iterable(1, 2, 3.0)
    self.assertIsInstance(res, iterable_qvalue.Iterable)
    self.assertIsInstance(res, arolla.QValue)
    self.assertEqual(res.qtype.name, 'ITERABLE[DATA_SLICE]')
    # Check that we can iterate multiple times.
    for _ in range(2):
      res_list = list(res)
      self.assertLen(res_list, 3)
      testing.assert_equal(res_list[0], ds(1))
      testing.assert_equal(res_list[1], ds(2))
      testing.assert_equal(res_list[2], ds(3.0))

  def test_specifying_type_and_elements(self):
    res = iterable_qvalue.Iterable(
        data_bag.DataBag.empty(), value_type_as=data_bag.DataBag.empty()
    )
    self.assertEqual(res.qtype.name, 'ITERABLE[DATA_BAG]')
    res_list = list(res)
    self.assertLen(res_list, 1)
    self.assertIsInstance(res_list[0], data_bag.DataBag)

  def test_mixed_type(self):
    with self.assertRaisesRegex(
        TypeError,
        re.escape(
            'expected all elements to be DATA_SLICE, got values[1]: DATA_BAG'
        ),
    ):
      _ = iterable_qvalue.Iterable(1, data_bag.DataBag.empty())
    with self.assertRaisesRegex(
        TypeError,
        re.escape(
            'expected all elements to be DATA_BAG, got values[0]: DATA_SLICE'
        ),
    ):
      _ = iterable_qvalue.Iterable(1, value_type_as=data_bag.DataBag.empty())

  def test_empty(self):
    res = iterable_qvalue.Iterable()
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res = iterable_qvalue.Iterable(value_type_as=data_bag.DataBag.empty())
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_BAG)
    res = iterable_qvalue.Iterable(value_type_as=data_bag.DataBag)
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_BAG)

  def test_serialization_does_not_work(self):
    # It seems that we do not need serialization for now, so keeping it disabled
    # to avoid users depending on it accidentally.
    a = iterable_qvalue.Iterable(1, 2, 3.0)
    with self.assertRaisesRegex(ValueError, 'cannot serialize value'):
      _ = arolla.s11n.dumps(a)


if __name__ == '__main__':
  absltest.main()
