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

"""Tests for data_item."""

import gc
import itertools
import re
import sys

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.functor import functor_factories
# Register kde ops for e.g. jagged_shape.create_shape().
from koladata.operators import kde_operators as _
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals
I = input_container.InputContainer('I')
S = I.self
bag = data_bag.DataBag.empty


class DataItemTest(parameterized.TestCase):

  def test_ref_count(self):
    gc.collect()
    diff_count = 10
    base_count = sys.getrefcount(data_item.DataItem)
    slices = []
    for _ in range(diff_count):
      slices.append(ds([1]))  # Adding DataSlice(s)

    self.assertEqual(sys.getrefcount(data_item.DataItem), base_count)

    items = []
    for _ in range(diff_count):
      items.append(ds(1))  # Adding DataItem(s)
    self.assertEqual(
        sys.getrefcount(data_item.DataItem), base_count + diff_count
    )

    del items
    gc.collect()
    self.assertEqual(sys.getrefcount(data_item.DataItem), base_count)

    _ = ds(1) + ds(2)
    self.assertEqual(sys.getrefcount(data_item.DataItem), base_count + 1)

    _ = ds([1, 2, 3]).S[0]
    self.assertEqual(sys.getrefcount(data_item.DataItem), base_count + 1)

  def test_qvalue(self):
    self.assertTrue(issubclass(data_item.DataItem, arolla.QValue))
    self.assertTrue(issubclass(data_item.DataItem, data_slice.DataSlice))
    x = data_item.DataItem.from_vals(12)
    self.assertIsInstance(x, data_item.DataItem)
    self.assertIsInstance(x, arolla.QValue)

  def test_from_vals_error(self):
    with self.assertRaisesRegex(
        TypeError, 'DataItem.* cannot create multi-dim slices'
    ):
      _ = data_item.DataItem.from_vals([1, 2, 3])

  def test_hash(self):
    items = [ds(12), ds(121), ds('abc'), data_bag.DataBag.empty().new(x=12)]
    for item in items:
      self.assertEqual(hash(item), hash(ds(item.internal_as_py())))
    for item_1, item_2 in itertools.combinations(items, 2):
      self.assertNotEqual(hash(item_1), hash(item_2))

  def test_db(self):
    x = ds(12)
    self.assertIsNone(x.db)

    db = data_bag.DataBag.empty()
    x = x.with_db(db)
    self.assertIsNotNone(x.db)

  def test_add_method(self):
    # We are adding a method to DataSlice and should be visible in DataItem
    # after the fact.
    self.assertFalse(hasattr(data_item.DataItem, 'foo'))

    @data_slice.DataSlice.add_method('foo')
    def foo(self):
      """Converts DataSlice to DenseArray."""
      return self.as_dense_array()

    self.assertTrue(hasattr(data_item.DataItem, 'foo'))

    x = ds('abc')
    arolla.testing.assert_qvalue_allequal(x.foo(), arolla.dense_array(['abc']))

  def test_set_get_attr(self):
    db = data_bag.DataBag.empty()
    x = db.new(abc=ds(3.14))
    x.get_schema().xyz = schema_constants.TEXT
    x.xyz = ds('abc')
    self.assertIsInstance(x.abc, data_item.DataItem)
    testing.assert_allclose(x.abc, ds(3.14).with_db(db))
    self.assertIsInstance(x.xyz, data_item.DataItem)
    testing.assert_equal(x.xyz, ds('abc').with_db(db))

  def test_as_arolla_value(self):
    arolla.testing.assert_qvalue_allequal(
        ds(12).as_arolla_value(), arolla.int32(12)
    )
    arolla.testing.assert_qvalue_allclose(
        ds(3.14).as_arolla_value(), arolla.float32(3.14)
    )
    arolla.testing.assert_qvalue_allequal(
        ds('abc').as_arolla_value(), arolla.text('abc')
    )
    # NOTE: Optional part is lost in this conversion.
    arolla.testing.assert_qvalue_allequal(
        ds(arolla.optional_int32(1)).as_arolla_value(), arolla.int32(1)
    )

    arolla.testing.assert_qvalue_allequal(
        ds(None, schema_constants.TEXT).as_arolla_value(),
        arolla.optional_text(None),
    )

    arolla.testing.assert_qvalue_allequal(
        ds(arolla.optional_int32(None)).as_arolla_value(),
        arolla.optional_int32(None),
    )

  def test_get_shape(self):
    testing.assert_equal(ds(12).get_shape(), jagged_shape.create_shape())

  @parameterized.parameters(
      (ds(1), schema_constants.INT32),
      (ds('a'), schema_constants.TEXT),
      (ds(b'a'), schema_constants.BYTES),
      (ds(1, schema_constants.INT64), schema_constants.INT64),
      (ds(1, schema_constants.FLOAT64), schema_constants.FLOAT64),
  )
  def test_get_schema(self, x, expected_schema):
    testing.assert_equal(x.get_schema(), expected_schema)
    testing.assert_equal(x.get_schema().get_schema(), schema_constants.SCHEMA)

  def test_bool(self):
    self.assertTrue(ds(arolla.unit()))
    self.assertTrue(ds(arolla.unit(), schema_constants.ANY))
    self.assertTrue(ds(arolla.unit(), schema_constants.OBJECT))
    self.assertFalse(ds(None))
    self.assertFalse(ds(None, schema_constants.MASK))
    self.assertFalse(ds(None, schema_constants.ANY))
    self.assertFalse(ds(None, schema_constants.OBJECT))

    with self.assertRaisesRegex(
        ValueError, 'Cannot cast a non-MASK DataItem to bool'
    ):
      bool(ds(None, schema_constants.INT32))

    with self.assertRaisesRegex(
        ValueError, 'Cannot cast a non-MASK DataItem to bool'
    ):
      bool(ds(True))

    with self.assertRaisesRegex(
        ValueError, 'Cannot cast a non-MASK DataItem to bool'
    ):
      bool(ds(5))

  def test_int(self):
    self.assertEqual(int(ds(42)), 42)
    self.assertEqual(int(ds(42, schema_constants.INT64)), 42)
    with self.assertRaisesRegex(ValueError, 'Only INT32/INT64 DataItem'):
      int(ds('42'))

  def test_index(self):
    self.assertEqual(ds(42).__index__(), 42)
    self.assertEqual(ds(42, schema_constants.INT64).__index__(), 42)
    with self.assertRaisesRegex(ValueError, 'Only INT32/INT64 DataItem'):
      ds('42').__index__()

  def test_float(self):
    self.assertAlmostEqual(float(ds(2.71)), 2.71)
    self.assertAlmostEqual(float(ds(2.71, schema_constants.FLOAT64)), 2.71)
    with self.assertRaisesRegex(ValueError, 'Only FLOAT32/FLOAT64 DataItem'):
      float(ds(42))

  @parameterized.named_parameters(
      ('int32', ds(12), 'DataItem(12, schema: INT32)'),
      ('int64', ds(arolla.int64(12)), 'DataItem(12, schema: INT64)'),
      ('float32', ds(12.0), 'DataItem(12.0, schema: FLOAT32)'),
      (
          'float64',
          ds(arolla.float64(12.0)),
          'DataItem(12.0, schema: FLOAT64)',
      ),
      ('boolean', ds(True), 'DataItem(True, schema: BOOLEAN)'),
      ('present_mask', ds(arolla.unit()), 'DataItem(present, schema: MASK)'),
      ('text', ds('a'), "DataItem('a', schema: TEXT)"),
      ('bytes', ds(b'a'), "DataItem(b'a', schema: BYTES)"),
      ('int32_with_any', ds(12).as_any(), 'DataItem(12, schema: ANY)'),
      (
          'int32_with_object',
          ds(12).with_schema(schema_constants.OBJECT),
          'DataItem(12, schema: OBJECT)',
      ),
      ('none', ds(None), 'DataItem(None, schema: NONE)'),
  )
  def test_repr_no_db(self, item, expected_repr):
    self.assertEqual(repr(item), expected_repr)

  def test_repr_with_db(self):
    db = data_bag.DataBag.empty()
    item = ds(12).with_db(db)
    bag_id = '$' + str(db.fingerprint)[-4:]
    self.assertEqual(
        repr(item), f'DataItem(12, schema: INT32, bag_id: {bag_id})'
    )

  def test_call(self):
    fn = functor_factories.fn(I.x * I.y)
    self.assertEqual(fn(x=2, y=3), 6)
    self.assertIsInstance(fn(x=2, y=I.z), arolla.Expr)
    self.assertEqual(fn(x=2, y=I.z).eval(z=3), 6)

    fn = functor_factories.fn(S.x * S.y)
    self.assertEqual(fn(bag().new(x=2, y=3)), 6)

    with self.assertRaisesRegex(
        ValueError, re.escape('the first argument of kd.call must be a functor')
    ):
      ds(1)()


if __name__ == '__main__':
  absltest.main()
