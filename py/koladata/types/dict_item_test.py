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

"""Tests for dict_item."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
# Register kde ops for e.g. jagged_shape.create_shape().
from koladata.operators import kde_operators as _
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import dict_item
from koladata.types import jagged_shape
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals


class DictItemTest(parameterized.TestCase):

  def test_qvalue(self):
    self.assertTrue(issubclass(dict_item.DictItem, arolla.QValue))
    self.assertTrue(issubclass(dict_item.DictItem, data_slice.DataSlice))
    self.assertTrue(issubclass(dict_item.DictItem, data_item.DataItem))
    l = data_bag.DataBag.empty().dict()
    self.assertIsInstance(l, dict_item.DictItem)
    self.assertIsInstance(l, arolla.QValue)

  def test_hash(self):
    db = data_bag.DataBag.empty()
    py_dicts = [
        None,
        {'a': 42},
        {'a': {b'x': 42, b'y': 12}, 'b': {b'z': 15}},
    ]
    for py_dict_1, py_dict_2 in itertools.combinations(py_dicts, 2):
      self.assertNotEqual(hash(db.dict(py_dict_1)), hash(db.dict(py_dict_2)))

  def test_db(self):
    db = data_bag.DataBag.empty()
    testing.assert_equal(db.dict({'a': 42}).db, db)

  def test_get_shape(self):
    d = data_bag.DataBag.empty().dict({'a': 42})
    testing.assert_equal(d.get_shape(), jagged_shape.create_shape())

  def test_str_and_repr(self):
    db = data_bag.DataBag.empty()
    # float32, float64 and expr are not allowed in dict keys
    py_keys = [
        ds(1),
        ds(2, schema_constants.INT64),
        ds(arolla.present()),
        ds('a'),
        ds(b'b'),
        ds(True),
        ds(False),
    ]
    py_values = [
        ds(1),
        ds(2, schema_constants.INT64),
        ds(0.5),
        ds(0.5, schema_constants.FLOAT64),
        ds(arolla.present()),
        ds('a'),
        ds(b'b'),
        ds(True),
        ds(False),
        db.dict({1: 2}),
    ]
    bag_id = '$' + str(db.fingerprint)[-4:]
    for key, value in itertools.product(py_keys, py_values):
      d = db.dict({key: value})
      self.assertEqual(str(d), f'Dict{{{str(key)}={str(value)}}}')
      self.assertEqual(
          repr(d),
          f'DataItem(Dict{{{str(key)}={str(value)}}}, schema:'
          f' DICT{{{str(key.get_schema())}, {str(value.get_schema())}}},'
          f' bag_id: {bag_id})',
      )

  def test_len(self):
    db = data_bag.DataBag.empty()

    d1 = db.dict()
    self.assertEmpty(d1)

    d2 = db.dict({1: 2, 3: 4})
    self.assertLen(d2, 2)

  def test_iter(self):
    db = data_bag.DataBag.empty()
    d = db.dict({1: 2, 3: 4})
    self.assertCountEqual(list(d), [ds(1), ds(3)])
    self.assertIn(1, d)
    self.assertIn(3, d)
    self.assertNotIn(2, d)


if __name__ == '__main__':
  absltest.main()
