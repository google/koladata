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

import itertools

from absl.testing import absltest
from arolla import arolla
from koladata.exceptions import exceptions
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import schema_constants
from koladata.types import schema_item

kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals


class SchemaItemTest(absltest.TestCase):

  def test_qvalue(self):
    self.assertTrue(issubclass(schema_item.SchemaItem, arolla.QValue))
    self.assertTrue(issubclass(schema_item.SchemaItem, data_slice.DataSlice))
    self.assertTrue(issubclass(schema_item.SchemaItem, data_item.DataItem))
    self.assertIsInstance(schema_constants.INT32, schema_item.SchemaItem)
    self.assertIsInstance(schema_constants.INT32, schema_item.SchemaItem)
    self.assertNotIsInstance(
        schema_constants.INT32.as_any(),
        schema_item.SchemaItem,
    )
    self.assertIsInstance(bag().new().get_schema(), schema_item.SchemaItem)

  def test_hash(self):
    schema_1 = bag().new().get_schema()
    schema_2 = bag().new().get_schema()
    schema_3 = schema_1.no_bag()
    schema_4 = schema_constants.INT32
    schema_5 = schema_constants.ANY
    for s_1, s_2 in itertools.combinations(
        [schema_1, schema_2, schema_3, schema_4, schema_5], 2
    ):
      self.assertNotEqual(hash(s_1), hash(s_2))

  def test_bag(self):
    db = bag()
    testing.assert_equal(db.new().get_schema().get_bag(), db)

  def test_get_shape(self):
    testing.assert_equal(
        bag().new().get_schema().get_shape(), jagged_shape.create_shape()
    )
    testing.assert_equal(
        schema_constants.ANY.get_shape(), jagged_shape.create_shape()
    )

  def test_new_schema_self_ref(self):
    s = fns.schema.new_schema(value=schema_constants.INT32)
    s.child = s
    s.parent = s
    child = s(value=42)
    entity = s(value=42, child=child)
    child.parent = entity
    testing.assert_equal(child.parent.no_bag(), entity.no_bag())
    testing.assert_equal(entity.child.no_bag(), child.no_bag())

  def test_get_nofollowed_schema(self):
    db = bag()
    orig_schema = db.new().get_schema()
    nofollow = kde.nofollow_schema(orig_schema).eval()
    testing.assert_equal(nofollow.get_nofollowed_schema(), orig_schema)

  def test_creating_entity(self):
    s = fns.schema.new_schema(
        a=schema_constants.FLOAT32, b=schema_constants.STRING
    )
    self.assertFalse(s.is_list_schema())
    self.assertFalse(s.is_dict_schema())
    self.assertTrue(s.is_entity_schema())
    entity = s(a=42, b='xyz')
    testing.assert_equal(entity.a, ds(42.0).with_bag(entity.get_bag()))
    testing.assert_equal(entity.b, ds('xyz').with_bag(entity.get_bag()))
    with self.assertRaises(AssertionError):
      testing.assert_equal(entity.get_bag(), s.get_bag())

  def test_creating_obj(self):
    o = fns.obj(a=schema_constants.FLOAT32, b=schema_constants.STRING)
    self.assertFalse(o.is_entity_schema())

  def test_creating_list(self):
    l = fns.list_schema(item_schema=fns.list_schema(schema_constants.FLOAT32))
    self.assertTrue(l.is_list_schema())
    self.assertTrue(l.is_entity_schema())
    lst = l([[1., 2], [3]])
    testing.assert_equal(
        lst[:][:], ds([[1.0, 2.0], [3.0]]).with_bag(lst.get_bag())
    )
    with self.assertRaises(AssertionError):
      testing.assert_equal(lst.get_bag(), l.get_bag())

  def test_creating_dict(self):
    d = fns.dict_schema(
        key_schema=schema_constants.STRING,
        value_schema=schema_constants.FLOAT32,
    )
    self.assertTrue(d.is_dict_schema())
    self.assertTrue(d.is_entity_schema())
    dct = d({'a': 42, 'b': 37})
    testing.assert_dicts_keys_equal(dct, ds(['a', 'b']))
    testing.assert_equal(
        dct[ds(['a', 'b'])], ds([42.0, 37.0]).with_bag(dct.get_bag())
    )
    with self.assertRaises(AssertionError):
      testing.assert_equal(dct.get_bag(), d.get_bag())

  def test_creating_dict_keys_and_values_separately(self):
    d = fns.dict_schema(
        key_schema=schema_constants.STRING,
        value_schema=schema_constants.FLOAT32,
    )
    self.assertTrue(d.is_dict_schema())

    dct = d(ds(['a', 'b']), ds([42, 37]))
    testing.assert_dicts_keys_equal(dct, ds(['a', 'b']))
    testing.assert_equal(
        dct[ds(['a', 'b'])], ds([42.0, 37.0]).with_bag(dct.get_bag())
    )
    with self.assertRaises(AssertionError):
      testing.assert_equal(dct.get_bag(), d.get_bag())

    dct = d(values=ds([42, 37]), items_or_keys=ds(['a', 'b']))
    testing.assert_dicts_keys_equal(dct, ds(['a', 'b']))
    testing.assert_equal(
        dct[ds(['a', 'b'])], ds([42.0, 37.0]).with_bag(dct.get_bag())
    )
    with self.assertRaises(AssertionError):
      testing.assert_equal(dct.get_bag(), d.get_bag())

  def test_creating_entities_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        'only SchemaItem with DataBags can be used for creating Entities',
    ):
      fns.schema.new_schema(
          a=schema_constants.INT32, b=schema_constants.STRING
      ).with_bag(None)([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError, 'expected List schema, got INT32'
    ):
      schema_constants.INT32.with_bag(bag())([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        'processing Entity attributes requires Entity schema, got INT32',
    ):
      schema_constants.INT32.with_bag(bag())({'a': 42})
    with self.assertRaisesRegex(
        exceptions.KodaError, 'requires Entity schema, got INT32'
    ):
      schema_constants.INT32.with_bag(bag())(a=1, b=2)

  def test_is_primitive_schema(self):
    a = ds(1)
    self.assertFalse(a.is_primitive_schema())
    self.assertTrue(a.get_schema().is_primitive_schema())
    self.assertFalse(ds([a.get_schema()]).is_primitive_schema())
    self.assertTrue(ds([a.get_schema()]).S[0].is_primitive_schema())
    self.assertTrue(ds('a').get_schema().is_primitive_schema())
    self.assertFalse(fns.list([1, 2]).get_schema().is_primitive_schema())
    self.assertFalse(fns.new(a=1).get_schema().is_primitive_schema())

  def test_is_any_schema(self):
    with self.subTest('item'):
      a = bag().obj(x=1)
      self.assertFalse(a.is_any_schema())
      self.assertFalse(a.get_schema().is_any_schema())
      self.assertTrue(a.as_any().get_schema().is_any_schema())

    with self.subTest('slice'):
      db = bag()
      a = ds([db.obj(x=1), db.obj(x=2)])
      self.assertFalse(a.is_any_schema())
      self.assertFalse(a.get_schema().is_any_schema())
      self.assertTrue(a.as_any().get_schema().is_any_schema())

  def test_is_itemid_schema(self):
    with self.subTest('item'):
      a = bag().obj(x=1)
      self.assertFalse(a.is_itemid_schema())
      self.assertFalse(a.get_schema().is_itemid_schema())
      self.assertTrue(a.get_itemid().get_schema().is_itemid_schema())

    with self.subTest('slice'):
      db = bag()
      a = ds([db.obj(x=1), db.obj(x=2)])
      self.assertFalse(a.is_itemid_schema())
      self.assertFalse(a.get_schema().is_itemid_schema())
      self.assertTrue(a.get_itemid().get_schema().is_itemid_schema())

if __name__ == '__main__':
  absltest.main()
