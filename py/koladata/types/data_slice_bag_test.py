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
from absl.testing import parameterized
from arolla import arolla
from koladata.functions import functions as fns
from koladata.functions.tests import test_pb2
from koladata.functor import boxing as _
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import dict_item as _
from koladata.types import jagged_shape
from koladata.types import list_item as _
from koladata.types import mask_constants
from koladata.types import schema_constants


kde = kde_operators.kde
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals

present = mask_constants.present
missing = mask_constants.missing


class DataSliceBagTest(parameterized.TestCase):

  def test_get_bag(self):
    x = ds([1, 2, 3])
    self.assertFalse(x.has_bag())

    db = bag()
    x = x.with_bag(db)
    self.assertTrue(x.has_bag())

    self.assertFalse(x.with_bag(None).has_bag())
    self.assertFalse(x.no_bag().has_bag())

    x = db.new_shaped(jagged_shape.create_shape([1]))
    self.assertTrue(x.has_bag())
    # NOTE: At the moment x.get_bag() is not db. If this is needed, we could
    # store the db PyObject* reference in PyDataSlice object. The underlying
    # DataBag that PyObject points to, is the same.
    self.assertIsNot(x.get_bag(), db)

    with self.assertRaisesRegex(
        TypeError, 'expecting db to be a DataBag, got list'
    ):
      x.with_bag([1, 2, 3])  # pyrefly: ignore[bad-argument-type]

    with self.assertRaisesRegex(
        TypeError, 'expecting db to be a DataBag, got DenseArray'
    ):
      x.with_bag(arolla.dense_array([1, 2, 3]))

  def test_has_bag(self):
    x = ds([1, 2, 3])
    self.assertFalse(x.has_bag())
    testing.assert_equal(x.has_bag(), missing)

    db = bag()
    x = x.with_bag(db)
    self.assertTrue(x.has_bag())
    testing.assert_equal(x.has_bag(), present)

    self.assertFalse(x.with_bag(None).has_bag())
    self.assertFalse(x.no_bag().has_bag())

    x = db.new_shaped(jagged_shape.create_shape([1]))
    self.assertTrue(x.has_bag())

  def test_get_attr_on_none(self):
    x = ds([None]).with_bag(bag())
    testing.assert_equal(x.x, ds([None]).with_bag(x.get_bag()))
    x = ds(None).with_bag(bag())
    testing.assert_equal(x.x, ds(None).with_bag(x.get_bag()))

  def test_get_attr_with_default_none(self):
    x = ds([None]).with_bag(bag())
    default = ds(42).with_bag(bag())
    testing.assert_equal(
        x.get_attr('x', default).no_bag(), ds([None], schema_constants.INT32)
    )

  def test_get_attr_with_default_extraction(self):
    # Regression test for b/408434629.
    db = bag()
    entities = db.new(x=ds([db.list([1, 2]), db.list([3, 4])])).freeze_bag()
    updated_lists = (
        entities.x & ds([None, arolla.present()])
    ).with_list_append_update(8)
    filtered_entities = entities.with_attr(
        'x', entities.x & ds([arolla.present(), None])
    )
    result = filtered_entities.get_attr('x', updated_lists)
    testing.assert_equal(result[:].no_bag(), ds([[1, 2], [3, 4, 8]]))

  def test_get_keys_on_none(self):
    x = ds([None]).with_bag(bag())
    testing.assert_equal(
        x.get_keys(), ds([[]], schema_constants.NONE).with_bag(x.get_bag())
    )
    x = ds(None).with_bag(bag())
    testing.assert_equal(
        x.get_keys(), ds([], schema_constants.NONE).with_bag(x.get_bag())
    )

  # More comprehensive tests are in the core_get_values_test.py.
  def test_get_values(self):
    db = bag()
    d1 = db.dict({1: 2, 3: 4})
    d2 = db.dict({3: 5})
    d = ds([d1, None, d2])

    testing.assert_unordered_equal(
        d.get_values(),
        ds([[2, 4], [], [5]]).with_bag(db),
    )
    testing.assert_equal(d.get_values(), d[d.get_keys()])

    testing.assert_equal(
        d.get_values(ds([[3, 1], [1], [3]])),
        ds([[4, 2], [None], [5]]).with_bag(db),
    )

  def test_get_values_on_none(self):
    x = ds([None]).with_bag(bag())
    testing.assert_equal(
        x.get_values(), ds([[]], schema_constants.NONE).with_bag(x.get_bag())
    )
    x = ds(None).with_bag(bag())
    testing.assert_equal(
        x.get_values(), ds([], schema_constants.NONE).with_bag(x.get_bag())
    )

  def test_get_present_keys(self):
    db = bag()
    d1 = db.dict({1: 2, 3: 4})
    del d1[1]
    d2 = db.dict({3: 5})
    d = ds([d1, None, d2])

    testing.assert_unordered_equal(
        d.get_present_keys(),
        ds([[3], [], [3]]).with_bag(db),
    )

  def test_get_present_keys_on_none(self):
    x = ds([None]).with_bag(bag())
    testing.assert_equal(
        x.get_present_keys(),
        ds([[]], schema_constants.NONE).with_bag(x.get_bag()),
    )
    x = ds(None).with_bag(bag())
    testing.assert_equal(
        x.get_present_keys(),
        ds([], schema_constants.NONE).with_bag(x.get_bag()),
    )

  def test_get_present_values(self):
    db = bag()
    d1 = db.dict({1: 2, 3: 4})
    del d1[1]
    d2 = db.dict({3: 5})
    d = ds([d1, None, d2])

    testing.assert_unordered_equal(
        d.get_present_values(),
        ds([[4], [], [5]]).with_bag(db),
    )

  def test_get_present_values_on_none(self):
    x = ds([None]).with_bag(bag())
    testing.assert_equal(
        x.get_present_values(),
        ds([[]], schema_constants.NONE).with_bag(x.get_bag()),
    )
    x = ds(None).with_bag(bag())
    testing.assert_equal(
        x.get_present_values(),
        ds([], schema_constants.NONE).with_bag(x.get_bag()),
    )

  def test_fork_bag(self):
    x = ds([1, 2, 3])

    with self.assertRaisesRegex(
        ValueError, 'fork_bag expects the DataSlice to have a DataBag attached'
    ):
      x.fork_bag()

    db = bag()
    x = x.with_bag(db)

    x1 = x.fork_bag()
    self.assertIsNot(x, x1)
    self.assertIsNot(x1.get_bag(), x.get_bag())
    self.assertIsNot(x1.get_bag(), db)
    self.assertTrue(x1.get_bag().is_mutable())

  def test_freeze_bag(self):
    x = ds([1, 2, 3])
    testing.assert_equal(x, x.freeze_bag())

    db = bag()
    x = x.with_bag(db)
    x1 = x.freeze_bag()
    self.assertIsNot(x, x1)
    self.assertIsNot(x1.get_bag(), x.get_bag())
    self.assertIsNot(x1.get_bag(), x1.get_bag())
    self.assertFalse(x1.get_bag().is_mutable())

  def test_with_merged_bag(self):
    db1 = bag()
    x = db1.new(a=1).freeze_bag()
    db2 = bag()
    y = x.with_bag(db2)
    y.set_attr('a', 2)
    y.set_attr('b', 2)
    z = x.enriched(db2)

    new_z = z.with_merged_bag()
    self.assertIsNot(new_z.get_bag(), db1)
    self.assertIsNot(new_z.get_bag(), db2)
    self.assertIsNot(new_z.get_bag(), z.get_bag())
    self.assertFalse(new_z.get_bag().is_mutable())
    testing.assert_equal(new_z.a.no_bag(), ds(1))
    testing.assert_equal(new_z.b.no_bag(), ds(2))

  def test_enriched(self):
    db1 = bag()
    schema = db1.new_schema(a=schema_constants.INT32)
    x = db1.new(a=1, schema=schema)

    db2 = data_bag.DataBag.empty()
    x = x.with_bag(db2)

    x = x.enriched(db1)
    self.assertNotEqual(x.get_bag().fingerprint, db1.fingerprint)
    self.assertNotEqual(x.get_bag().fingerprint, db2.fingerprint)
    testing.assert_equivalent(x.a, ds(1))

    x = x.with_bag(db2).enriched(db1, db1)
    self.assertNotEqual(x.get_bag().fingerprint, db1.fingerprint)
    self.assertNotEqual(x.get_bag().fingerprint, db2.fingerprint)
    testing.assert_equivalent(x.a, ds(1))

  def test_updated(self):
    schema = kde.schema.new_schema(a=schema_constants.INT32).eval()

    db1 = bag()
    db1.merge_inplace(schema.get_bag())
    x = db1.new(a=1, schema=schema).freeze_bag()

    db2 = bag()
    db2.merge_inplace(schema.get_bag())
    x.with_bag(db2).a = 2

    x = x.updated(db2)
    self.assertNotEqual(x.get_bag().fingerprint, db1.fingerprint)
    self.assertNotEqual(x.get_bag().fingerprint, db2.fingerprint)
    testing.assert_equivalent(x.a, ds(2))

    x = x.with_bag(db1).freeze_bag().updated(db2, db2)
    self.assertNotEqual(x.get_bag().fingerprint, db1.fingerprint)
    self.assertNotEqual(x.get_bag().fingerprint, db2.fingerprint)
    testing.assert_equivalent(x.a, ds(2))

  def test_ref(self):
    x = ds([1, 2, 3])

    with self.assertRaisesRegex(
        ValueError,
        'casting a DataSlice with schema INT32 to ITEMID is not supported',
    ):
      x.ref()

    db = bag()
    x = db.obj(x=x)
    testing.assert_equal(x.ref(), x.with_bag(None))

  def test_reserved_ipython_method_names(self):
    db = bag()
    x = db.new(getdoc=1, trait_names=2, normal=3)
    self.assertEqual(x.normal, 3)
    with self.assertRaises(AttributeError):
      _ = x.getdoc
    with self.assertRaises(AttributeError):
      _ = x.trait_names

  def test_dir(self):
    db = bag()
    # No attrs (primitive with no bag).
    self.assertEqual(
        dir(ds([1, 2, 3])),
        sorted(dir(data_slice.DataSlice)),
    )
    # With attrs.
    self.assertEqual(
        dir(db.new(a=ds([1]), b=ds(['abc']))),
        sorted({'a', 'b'} | set(dir(data_slice.DataSlice))),
    )
    # With attrs DataItem (more methods than DataSlice)
    self.assertEqual(
        dir(db.new(a=1, b='abc')),
        sorted({'a', 'b'} | set(dir(data_item.DataItem))),
    )
    # No available attrs (no db).
    self.assertEqual(
        dir(db.new(a=ds([1]), b=ds(['abc'])).no_bag()),
        sorted(dir(data_slice.DataSlice)),
    )
    # No available attrs (no __schema__ attr on object).
    self.assertEqual(
        dir(db.new(a=ds([1])).with_schema(schema_constants.OBJECT)),
        sorted(dir(data_slice.DataSlice)),
    )
    # Intersection of attrs.
    self.assertEqual(
        dir(ds([db.obj(a=1, b='abc'), db.obj(a='def', c=123)])),
        sorted({'a'} | set(dir(data_slice.DataSlice))),
    )

  def test_dir_reserved_names(self):
    db = bag()
    x = db.new(_x=1, getdoc=2, reshape=3, y=4)
    setattr(x, '', 5)
    # Reserved names and names starting with `_` are _not_ included. Reshape is
    # included in dir(DataItem) since it's a method there, and `''` is included
    # since it's a valid attribute name even though it's only accessible via
    # getattr(x, '') and not via x.<smth>.
    self.assertEqual(dir(x), sorted({'y', ''} | set(dir(data_item.DataItem))))

  def test_get_attr_names_entity(self):
    db = bag()
    fb = bag()
    x = db.new(a=1, b='abc')
    db = db.freeze()
    testing.assert_equal(x.get_attr_names(), ds(['a', 'b']))
    testing.assert_equal(ds([x]).get_attr_names(), ds([['a', 'b']]))
    x.with_bag(fb).set_attr('c', 42)
    testing.assert_equal(
        x.with_bag(db).enriched(fb).get_attr_names(),
        ds(['a', 'b', 'c']),
    )
    testing.assert_equal(
        ds([x]).with_bag(db).enriched(fb).get_attr_names(),
        ds([['a', 'b', 'c']]),
    )
    with self.assertRaisesRegex(
        ValueError, 'cannot get available attributes without a DataBag'
    ):
      x.no_bag().get_attr_names()

  def test_get_attr_names_object(self):
    db = bag()
    x = db.obj(a=1, b='abc')
    testing.assert_equal(x.get_attr_names(), ds(['a', 'b']))
    testing.assert_equal(ds([x]).get_attr_names(), ds([['a', 'b']]))
    testing.assert_equal(
        ds([x, db.obj(a='def', c=123)]).get_attr_names(),
        ds([['a', 'b'], ['a', 'c']]),
    )
    with self.assertRaisesRegex(
        ValueError, 'cannot get available attributes without a DataBag'
    ):
      x.no_bag().get_attr_names()
    with self.assertRaisesRegex(
        ValueError, 'object schema is missing for the DataItem'
    ):
      db.new(a=1, b='abc').with_schema(schema_constants.OBJECT).get_attr_names()

  def test_get_attr_names_primitive(self):
    x = ds([1, 2, 3]).with_bag(bag())
    testing.assert_equal(
        x.get_attr_names(),
        ds([[], [], []], schema_constants.STRING),
    )

  def test_get_attr_names_schema(self):
    db = bag()
    testing.assert_equal(
        schema_constants.INT32.with_bag(db).get_attr_names(),
        ds([], schema_constants.STRING),
    )
    schema1 = db.new_schema(
        a=schema_constants.INT32, b=schema_constants.FLOAT32
    )
    schema2 = db.new_schema(
        a=schema_constants.INT32, c=schema_constants.FLOAT32
    )
    schemas = ds([schema1, schema2])
    testing.assert_equal(schemas.get_attr_names(), ds([['a', 'b'], ['a', 'c']]))

  def test_get_attr_names_reserved_names(self):
    db = bag()
    x = db.new(_x=1, getdoc=2, reshape=3)
    testing.assert_equal(
        x.get_attr_names(),
        ds(['_x', 'getdoc', 'reshape']),
    )

  def test_internal_as_py(self):
    x = ds([[1, 2], [3], [4, 5]])
    self.assertEqual(x.internal_as_py(), [[1, 2], [3], [4, 5]])

  def test_to_proto_minimal(self):
    # NOTE: more tests for to_proto in
    # py/koladata/functions/tests/to_proto_test.py

    message = fns.new()._to_proto(test_pb2.EmptyMessage)  # pylint: disable=protected-access
    self.assertIsInstance(message, test_pb2.EmptyMessage)
    self.assertEqual(message, test_pb2.EmptyMessage())

    messages = ds([fns.new()])._to_proto(test_pb2.EmptyMessage)  # pylint: disable=protected-access
    self.assertIsInstance(messages, list)
    self.assertLen(messages, 1)
    self.assertIsInstance(messages[0], test_pb2.EmptyMessage)
    self.assertEqual(messages, [test_pb2.EmptyMessage()])

  def test_to_proto_errors(self):
    with self.assertRaisesRegex(
        ValueError, 'to_proto accepts exactly 1 arguments, got 0'
    ):
      _ = ds([])._to_proto()  # pylint: disable=protected-access  # pyrefly: ignore[missing-argument]

    with self.assertRaisesRegex(
        TypeError,
        'to_proto expects message_class to be a proto class, got NoneType',
    ):
      _ = ds([])._to_proto(None)  # pylint: disable=protected-access

    with self.assertRaisesRegex(
        ValueError,
        r'message cast from python to C\+\+ failed, got type int',
    ):
      _ = ds([])._to_proto(int)  # pylint: disable=protected-access

    with self.assertRaisesRegex(
        ValueError,
        'to_proto expects a DataSlice with ndim 0 or 1, got ndim=2',
    ):
      _ = ds([[]])._to_proto(test_pb2.EmptyMessage)  # pylint: disable=protected-access


if __name__ == '__main__':
  absltest.main()
