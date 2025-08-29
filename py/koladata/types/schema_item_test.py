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

import itertools

from absl.testing import absltest
from arolla import arolla
from koladata.expr import py_expr_eval_py_ext
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import schema_constants
from koladata.types import schema_item

eval_op = py_expr_eval_py_ext.eval_op
kde = kde_operators.kde
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals


class SchemaItemTest(absltest.TestCase):

  def test_qvalue(self):
    self.assertTrue(issubclass(schema_item.SchemaItem, arolla.QValue))
    self.assertTrue(issubclass(schema_item.SchemaItem, data_slice.DataSlice))
    self.assertTrue(issubclass(schema_item.SchemaItem, data_item.DataItem))
    self.assertIsInstance(schema_constants.INT32, schema_item.SchemaItem)
    self.assertIsInstance(schema_constants.INT32, schema_item.SchemaItem)
    self.assertIsInstance(bag().new().get_schema(), schema_item.SchemaItem)

  def test_hash(self):
    schema_1 = bag().new().get_schema()
    schema_2 = bag().new().get_schema()
    schema_3 = schema_1.no_bag()
    schema_4 = schema_constants.INT32
    schema_5 = schema_constants.OBJECT
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
        schema_constants.OBJECT.get_shape(), jagged_shape.create_shape()
    )

  def test_new_schema_self_ref(self):
    s = kde.schema.new_schema(value=schema_constants.INT32).eval()
    s = s.with_attrs(child=s, parent=s)
    child = s.new(value=42).fork_bag()
    entity = s.new(value=42, child=child)
    child.parent = entity
    testing.assert_equal(child.parent.no_bag(), entity.no_bag())
    testing.assert_equal(entity.child.no_bag(), child.no_bag())

  def test_get_nofollowed_schema(self):
    db = bag()
    orig_schema = db.new().get_schema()
    nofollow = eval_op('kd.schema.nofollow_schema', orig_schema)
    testing.assert_equal(nofollow.get_nofollowed_schema(), orig_schema)

  def test_creating_entity(self):
    s = kde.schema.new_schema(
        a=schema_constants.FLOAT32, b=schema_constants.STRING
    ).eval()
    self.assertTrue(s.is_entity_schema())
    entity = s.new(a=42, b='xyz')
    testing.assert_equal(entity.a, ds(42.0).with_bag(entity.get_bag()))
    testing.assert_equal(entity.b, ds('xyz').with_bag(entity.get_bag()))
    self.assertNotEqual(entity.get_bag().fingerprint, s.get_bag().fingerprint)

  def test_creating_entity_with_expr_args(self):
    s = kde.schema.new_schema(
        a=schema_constants.FLOAT32, b=schema_constants.STRING
    ).eval()
    self.assertTrue(s.is_entity_schema())
    entity_expr = s.new(a=42, b=kde.item('xyz'))
    testing.assert_non_deterministic_exprs_equal(
        entity_expr, kde.new(schema=s, a=42, b=kde.item('xyz'))
    )

  def test_creating_entities_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        'only SchemaItem with DataBags can be used for creating Entities',
    ):
      kde.schema.new_schema(
          a=schema_constants.INT32, b=schema_constants.STRING
      ).eval().with_bag(None).new()

  def test_is_primitive_schema(self):
    a = ds(1)
    self.assertFalse(a.is_primitive_schema())
    self.assertTrue(a.get_schema().is_primitive_schema())
    self.assertFalse(ds([a.get_schema()]).is_primitive_schema())
    self.assertTrue(ds([a.get_schema()]).S[0].is_primitive_schema())
    self.assertTrue(ds('a').get_schema().is_primitive_schema())
    self.assertFalse(bag().list([1, 2]).get_schema().is_primitive_schema())
    self.assertFalse(bag().new(a=1).get_schema().is_primitive_schema())

  def test_internal_is_itemid_schema(self):
    with self.subTest('item'):
      a = bag().obj(x=1)
      self.assertFalse(a.internal_is_itemid_schema())
      self.assertFalse(a.get_schema().internal_is_itemid_schema())
      self.assertTrue(a.get_itemid().get_schema().internal_is_itemid_schema())

    with self.subTest('slice'):
      db = bag()
      a = ds([db.obj(x=1), db.obj(x=2)])
      self.assertFalse(a.internal_is_itemid_schema())
      self.assertFalse(a.get_schema().internal_is_itemid_schema())
      self.assertTrue(a.get_itemid().get_schema().internal_is_itemid_schema())

if __name__ == '__main__':
  absltest.main()
