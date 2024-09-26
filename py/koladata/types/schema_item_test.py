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

"""Tests for schema_item."""

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
    schema_3 = schema_1.no_db()
    schema_4 = schema_constants.INT32
    schema_5 = schema_constants.ANY
    for s_1, s_2 in itertools.combinations(
        [schema_1, schema_2, schema_3, schema_4, schema_5], 2
    ):
      self.assertNotEqual(hash(s_1), hash(s_2))

  def test_db(self):
    db = bag()
    testing.assert_equal(db.new().get_schema().db, db)

  def test_get_shape(self):
    testing.assert_equal(
        bag().new().get_schema().get_shape(), jagged_shape.create_shape()
    )
    testing.assert_equal(
        schema_constants.ANY.get_shape(), jagged_shape.create_shape()
    )

  def test_new_schema_self_ref(self):
    s = fns.new_schema(value=schema_constants.INT32)
    s.child = s
    s.parent = s
    child = s(value=42)
    entity = s(value=42, child=child)
    child.parent = entity
    testing.assert_equal(child.parent.no_db(), entity.no_db())
    testing.assert_equal(entity.child.no_db(), child.no_db())

  def test_get_nofollowed_schema(self):
    db = bag()
    orig_schema = db.new().get_schema()
    nofollow = kde.nofollow_schema._eval(orig_schema)
    testing.assert_equal(nofollow.get_nofollowed_schema(), orig_schema)

  def test_creating_entity(self):
    s = fns.new_schema(a=schema_constants.FLOAT32, b=schema_constants.TEXT)
    self.assertFalse(s.is_list_schema())
    self.assertFalse(s.is_dict_schema())
    entity = s(a=42, b='xyz')
    testing.assert_equal(entity.a, ds(42.0).with_db(entity.db))
    testing.assert_equal(entity.b, ds('xyz').with_db(entity.db))
    with self.assertRaises(AssertionError):
      testing.assert_equal(entity.db, s.db)

  def test_creating_list(self):
    l = fns.list_schema(item_schema=fns.list_schema(schema_constants.FLOAT32))
    self.assertTrue(l.is_list_schema())
    lst = l([[1., 2], [3]])
    testing.assert_equal(lst[:][:], ds([[1., 2.], [3.]]).with_db(lst.db))
    with self.assertRaises(AssertionError):
      testing.assert_equal(lst.db, l.db)

  def test_creating_dict(self):
    d = fns.dict_schema(
        key_schema=schema_constants.TEXT, value_schema=schema_constants.FLOAT32
    )
    self.assertTrue(d.is_dict_schema())
    dct = d({'a': 42, 'b': 37})
    testing.assert_dicts_keys_equal(dct, ds(['a', 'b']))
    testing.assert_equal(dct[['a', 'b']], ds([42., 37.]).with_db(dct.db))
    with self.assertRaises(AssertionError):
      testing.assert_equal(dct.db, d.db)

  def test_creating_dict_keys_and_values_separately(self):
    d = fns.dict_schema(
        key_schema=schema_constants.TEXT, value_schema=schema_constants.FLOAT32
    )
    self.assertTrue(d.is_dict_schema())

    dct = d(ds(['a', 'b']), ds([42, 37]))
    testing.assert_dicts_keys_equal(dct, ds(['a', 'b']))
    testing.assert_equal(dct[['a', 'b']], ds([42., 37.]).with_db(dct.db))
    with self.assertRaises(AssertionError):
      testing.assert_equal(dct.db, d.db)

    dct = d(values=ds([42, 37]), items_or_keys=ds(['a', 'b']))
    testing.assert_dicts_keys_equal(dct, ds(['a', 'b']))
    testing.assert_equal(dct[['a', 'b']], ds([42., 37.]).with_db(dct.db))
    with self.assertRaises(AssertionError):
      testing.assert_equal(dct.db, d.db)

  def test_creating_entities_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        'only SchemaItem with DataBags can be used for creating Entities',
    ):
      fns.new_schema(a=schema_constants.INT32, b=schema_constants.TEXT).with_db(
          None
      )([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError, 'expected List schema, got INT32'
    ):
      schema_constants.INT32.with_db(bag())([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError, 'expected Dict schema, got INT32'
    ):
      schema_constants.INT32.with_db(bag())({'a': 42})
    with self.assertRaisesRegex(
        exceptions.KodaError, 'requires Entity schema, got INT32'
    ):
      schema_constants.INT32.with_db(bag())(a=1, b=2)

  def test_is_primitive_schema(self):
    a = ds(1)
    self.assertFalse(a.is_primitive_schema())
    self.assertTrue(a.get_schema().is_primitive_schema())
    self.assertFalse(ds([a.get_schema()]).is_primitive_schema())
    self.assertTrue(ds([a.get_schema()]).S[0].is_primitive_schema())
    self.assertTrue(ds('a').get_schema().is_primitive_schema())
    self.assertFalse(fns.list([1, 2]).get_schema().is_primitive_schema())
    self.assertFalse(fns.new(a=1).get_schema().is_primitive_schema())


if __name__ == '__main__':
  absltest.main()
