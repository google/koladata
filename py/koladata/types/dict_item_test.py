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

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals


class DictItemTest(parameterized.TestCase):

  def test_qvalue(self):
    self.assertTrue(issubclass(dict_item.DictItem, arolla.QValue))
    self.assertTrue(issubclass(dict_item.DictItem, data_slice.DataSlice))
    self.assertTrue(issubclass(dict_item.DictItem, data_item.DataItem))
    l = bag().dict()
    self.assertIsInstance(l, dict_item.DictItem)
    self.assertIsInstance(l, arolla.QValue)

  def test_hash(self):
    db = bag()
    py_dicts = [
        None,
        {'a': 42},
        {'a': db.dict({b'x': 42, b'y': 12}), 'b': db.dict({b'z': 15})},
    ]
    for py_dict_1, py_dict_2 in itertools.combinations(py_dicts, 2):
      self.assertNotEqual(hash(db.dict(py_dict_1)), hash(db.dict(py_dict_2)))

  def test_bag(self):
    db = bag()
    testing.assert_equal(db.dict({'a': 42}).get_bag(), db)

  def test_get_shape(self):
    d = bag().dict({'a': 42})
    testing.assert_equal(d.get_shape(), jagged_shape.create_shape())

  def test_str_and_repr(self):
    db = bag()
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
    for key, value in itertools.product(py_keys, py_values):
      d = db.dict({key: value})
      key_str = str(key)
      key_str = key_str if key_str != 'a' else "'a'"
      value_str = str(value)
      value_str = value_str if value_str != 'a' else "'a'"
      self.assertEqual(str(d), f'Dict{{{key_str}={value_str}}}')
      bag_id = '$' + str(db.fingerprint)[-4:]
      self.assertEqual(
          repr(d),
          f'DataItem(Dict{{{key_str}={value_str}}}, schema:'
          f' DICT{{{str(key.get_schema())}, {str(value.get_schema())}}}, '
          f'bag_id: {bag_id})',
      )

  def test_len(self):
    db = bag()

    d1 = db.dict()
    self.assertEmpty(d1)

    d2 = db.dict({1: 2, 3: 4})
    self.assertLen(d2, 2)

  def test_iter(self):
    db = bag()
    d = db.dict({1: 2, 3: 4})
    self.assertCountEqual(list(d), [ds(1), ds(3)])
    self.assertIn(1, d)
    self.assertIn(3, d)
    self.assertNotIn(2, d)

  def test_contains(self):
    db = bag()
    self.assertIn(1, db.dict({1: 2, 3: 42}))
    self.assertIn(ds(1), db.dict({1: 2, 3: 42}))
    self.assertNotIn(1, db.dict({42: 2, 3: 42}))
    self.assertNotIn(ds(1), db.dict({42: 2, 3: 42}))

  def test_pop(self):
    db = bag()
    d = db.dict({1: 2, 3: 4})
    self.assertLen(d, 2)
    d.pop(1)
    self.assertNotIn(1, d)
    self.assertIsNone(d[1].to_py())
    self.assertLen(d, 2)

    with self.assertRaises(KeyError):
      d.pop(2)

  def test_del(self):
    db = bag()
    d = db.dict({1: 2, 3: 4})
    self.assertLen(d, 2)

    del d[1]
    self.assertNotIn(1, d)
    self.assertLen(d, 2)

    self.assertNotIn(2, d)
    del d[2]
    self.assertNotIn(2, d)
    self.assertLen(d, 3)

  def test_assign_none(self):
    db = bag()
    d = db.dict({1: 2, 3: 4})
    self.assertLen(d, 2)

    d[1] = None
    self.assertNotIn(1, d)
    self.assertLen(d, 2)

    self.assertNotIn(2, d)
    d[2] = None
    self.assertNotIn(2, d)
    self.assertLen(d, 3)

  def test_empty_dict(self):
    db = bag()
    d = db.dict({'x': None})
    self.assertLen(d, 1)


if __name__ == '__main__':
  absltest.main()
