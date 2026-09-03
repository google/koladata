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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import dict_item as _
from koladata.types import jagged_shape
from koladata.types import list_item as _
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants


kde = kde_operators.kde
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals

present = mask_constants.present
missing = mask_constants.missing


class DataSliceAttrsTest(parameterized.TestCase):

  def test_assignment_rhs_koda_iterables(self):
    db = bag()
    x = db.obj()
    # Text
    x.a = 'abc'
    self.assertEqual(x.a, 'abc')
    # Bytes
    x.b = b'abc'
    self.assertEqual(x.b, b'abc')
    x.a = []
    testing.assert_equal(
        x.a.get_schema().get_attr('__items__'),
        schema_constants.NONE.with_bag(db),
    )
    x.a = ()
    testing.assert_equal(
        x.a.get_schema().get_attr('__items__'),
        schema_constants.NONE.with_bag(db),
    )
    # Other iterables are not supported in boxing code.
    with self.assertRaisesRegex(ValueError, 'object with unsupported type'):
      x.a = set()
    with self.assertRaisesRegex(ValueError, 'object with unsupported type'):
      x.a = iter([1, 2, 3])

  def test_set_get_attr(self):
    db = bag()
    x = db.new(abc=ds([42]))
    x.get_schema().xyz = schema_constants.INT64
    x.xyz = ds([12], schema_constants.INT64)
    testing.assert_equal(x.abc, ds([42]).with_bag(db))
    testing.assert_equal(
        x.abc.get_schema(), schema_constants.INT32.with_bag(db)
    )
    testing.assert_equal(x.xyz, ds([12], schema_constants.INT64).with_bag(db))
    testing.assert_equal(
        x.xyz.get_schema(), schema_constants.INT64.with_bag(db)
    )

  def test_get_itemid(self):
    o = bag().obj(x=ds([1, 2, 3]))
    testing.assert_equal(o.get_itemid(), o.with_schema(schema_constants.ITEMID))
    with self.assertRaisesRegex(
        ValueError,
        'casting a DataSlice with schema INT32 to ITEMID is not supported',
    ):
      ds([1, 2, 3]).get_itemid()
    with self.assertRaisesRegex(
        ValueError, 'casting data of type INT32 to ITEMID is not supported'
    ):
      ds([1, 2, 3], schema_constants.OBJECT).get_itemid()

  def test_has_attr(self):
    db = bag()

    with self.subTest('entity item'):
      x = db.new(a=ds(42), b=ds(None))
      testing.assert_equal(x.has_attr('a'), ds(present))
      testing.assert_equal(x.has_attr('b'), ds(missing))
      testing.assert_equal(x.has_attr('c'), ds(missing))

    with self.subTest('entity slice'):
      x = db.new(a=ds([42]), b=ds([None]))
      testing.assert_equal(x.has_attr('a'), ds([present]))
      testing.assert_equal(x.has_attr('b'), ds([missing]))
      testing.assert_equal(x.has_attr('c'), ds([missing]))

    with self.subTest('obj item'):
      x = db.obj(a=ds(42), b=ds(None))
      testing.assert_equal(x.has_attr('a'), ds(present))
      testing.assert_equal(x.has_attr('b'), ds(missing))
      testing.assert_equal(x.has_attr('c'), ds(missing))

    with self.subTest('obj slice'):
      x = db.obj(a=ds([42]), b=ds([None]))
      testing.assert_equal(x.has_attr('a'), ds([present]))
      testing.assert_equal(x.has_attr('b'), ds([missing]))
      testing.assert_equal(x.has_attr('c'), ds([missing]))

  def test_set_get_attr_methods(self):
    db = bag()

    with self.subTest('entity'):
      x = db.new(abc=ds([42], schema_constants.INT64))
      testing.assert_equal(
          x.get_attr('abc'), ds([42], schema_constants.INT64).with_bag(db)
      )
      testing.assert_equal(
          x.get_attr('abc').get_schema(), schema_constants.INT64.with_bag(db)
      )
      # Missing
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(r"attribute 'xyz' is missing"),
      ):
        x.get_attr('xyz')
      testing.assert_equal(x.get_attr('xyz', None), ds([None]).with_bag(db))
      testing.assert_equal(x.get_attr('xyz', b'b'), ds([b'b']).with_bag(db))

      x.set_attr('xyz', ds(b'12'))

      with self.assertRaisesRegex(
          ValueError, r'schema for attribute \'xyz\' is incompatible'
      ):
        x.set_attr('xyz', ds([12]), overwrite_schema=False)

      x.set_attr('xyz', ds([12]), overwrite_schema=True)
      testing.assert_equal(x.get_attr('xyz'), ds([12]).with_bag(db))
      testing.assert_equal(
          x.get_attr('xyz').get_schema(), schema_constants.INT32.with_bag(db)
      )

    with self.subTest('object'):
      x = db.obj(abc=ds([42], schema_constants.INT64))
      testing.assert_equal(
          x.get_attr('abc'), ds([42], schema_constants.INT64).with_bag(db)
      )
      testing.assert_equal(
          x.get_attr('abc').get_schema(), schema_constants.INT64.with_bag(db)
      )

      for attr, val, overwrite_schema, res_schema in [
          ('xyz', ds([b'12']), True, schema_constants.BYTES),
          ('pqr', ds(['123']), False, schema_constants.STRING),
      ]:
        x.set_attr(attr, val, overwrite_schema=overwrite_schema)
        testing.assert_equal(x.get_attr(attr), val.with_bag(db))
        testing.assert_equal(
            x.get_attr(attr).get_schema(), res_schema.with_bag(db)
        )
        testing.assert_equal(
            x.get_attr('__schema__').get_attr(attr),
            ds([res_schema]).with_bag(db),
        )

    with self.subTest('objects with explicit schema'):
      x = db.obj(abc=ds([42, 12]))
      e_schema = db.new(abc=ds(42, schema_constants.INT64)).get_schema()
      x.set_attr('__schema__', e_schema)
      testing.assert_equal(
          x.get_attr('abc'),
          ds([42, 12], schema_constants.INT64).with_bag(db)
      )
      self.assertEqual(x.get_attr('abc').internal_as_py(), [42, 12])
      testing.assert_equal(
          x.get_attr('abc').get_schema(), schema_constants.INT64.with_bag(db)
      )

      x.set_attr(
          'abc',
          # Casting INT32 -> INT64 is allowed and done automatically.
          ds([1, 2], schema_constants.INT32),
      )
      testing.assert_equal(
          x.get_attr('abc'), ds([1, 2], schema_constants.INT64).with_bag(db)
      )
      testing.assert_equal(
          x.get_attr('abc').get_schema(), schema_constants.INT64.with_bag(db)
      )

      with self.assertRaisesRegex(
          ValueError,
          r'the schema for attribute \'abc\' is incompatible',
      ):
        x.set_attr('abc', ds([b'x', b'y']), overwrite_schema=False)
      # Overwrite with overwriting schema.
      x.set_attr('abc', ds([b'x', b'y']), overwrite_schema=True)
      testing.assert_equal(x.get_attr('abc'), ds([b'x', b'y']).with_bag(db))
      testing.assert_equal(
          x.get_attr('abc').get_schema(), schema_constants.BYTES.with_bag(db)
      )
      testing.assert_equal(
          x.get_attr('__schema__').get_attr('abc'),
          ds([schema_constants.BYTES, schema_constants.BYTES]).with_bag(db),
      )

    with self.subTest('errors'):
      x = db.new(abc=ds([42], schema_constants.INT64))
      with self.assertRaisesRegex(
          TypeError, 'expecting attr_name to be a DataSlice'
      ):
        x.set_attr(b'invalid_attr', 1)  # pyrefly: ignore[bad-argument-type]
      with self.assertRaises(ValueError):
        x.set_attr('invalid__val', ValueError)
      with self.assertRaisesRegex(TypeError, 'expected bool'):
        x.set_attr('invalid__overwrite_schema_type', 1, overwrite_schema=42)  # pyrefly: ignore[bad-argument-type]
      with self.assertRaisesRegex(
          TypeError, 'accepts 2 to 3 positional arguments'
      ):
        x.set_attr('invalid__overwrite_schema_type', 1, None, 42)  # pyrefly: ignore[bad-argument-type, bad-argument-count]

  def test_get_attr_ds_attr_name(self):
    db = bag()
    x = db.obj(a=ds([1, 2]), b=ds([3, 4], schema_constants.INT32))
    with self.subTest('smoke_test'):
      x1 = x.get_attr(ds(['a', 'b']))
      testing.assert_equal(x1.no_bag(), ds([1, 4]))
    with self.subTest('with_default'):
      x1 = x.get_attr(ds(['a', 'c']), None)
      testing.assert_equal(x1.no_bag(), ds([1, None]))
    with self.subTest('invalid_attr_name'):
      with self.assertRaisesRegex(
          TypeError, 'expecting attr_name to be a DataSlice'
      ):
        x.get_attr(db)  # pyrefly: ignore[bad-argument-type]
    with self.subTest('py_list_attr_name'):
      with self.assertRaisesRegex(
          TypeError, 'expecting attr_name to be a DataSlice'
      ):
        x.get_attr(['a', 'b'])  # pyrefly: ignore[bad-argument-type]

  def test_set_attr_ds_attr_name(self):
    db = bag()
    x = db.obj()
    with self.subTest('smoke_test'):
      x.set_attr(ds(['a', 'b']), ds([123, 456], schema_constants.INT32))
      testing.assert_equal(x.get_attr('a'), ds(123).with_bag(db))
      testing.assert_equal(x.get_attr('b'), ds(456).with_bag(db))
      testing.assert_equal(
          x.get_attr('__schema__').get_attr('a'),
          ds(schema_constants.INT32).with_bag(db),
      )
      testing.assert_equal(
          x.get_attr('__schema__').get_attr('b'),
          ds(schema_constants.INT32).with_bag(db),
      )

    with self.subTest('schema_narrowing'):
      x.set_attr(
          ds(['a', 'b'], schema_constants.OBJECT),
          ds([123, 456], schema_constants.INT32),
      )
      testing.assert_equal(x.get_attr('a'), ds(123).with_bag(db))
      testing.assert_equal(x.get_attr('b'), ds(456).with_bag(db))

    with self.subTest('set_list'):
      x.set_attr(ds(['a']), ds([db.list([1, 2])]), overwrite_schema=True)
      testing.assert_equivalent(x.get_attr('a'), db.list([1, 2]))

    with self.subTest('set_attr_on_schema_slice'):
      schema_slice = ds([
          db.new_schema(a=schema_constants.INT32),
          db.new_schema(b=schema_constants.STRING),
      ])
      schema_slice.set_attr(ds(['a', 'b']), schema_constants.FLOAT32)
      testing.assert_equal(
          schema_slice.get_attr('a', None),
          ds([schema_constants.FLOAT32, None]).with_bag(db),
      )
      testing.assert_equal(
          schema_slice.get_attr('b', None),
          ds([None, schema_constants.FLOAT32]).with_bag(db),
      )

    with self.subTest('set_attr_on_schema_attr_slice'):
      schema_item = db.new_schema(
          a=schema_constants.INT32, b=schema_constants.STRING
      )
      schema_item.set_attr(
          ds(['a', 'b']),
          ds([schema_constants.FLOAT32, schema_constants.FLOAT64]),
      )
      testing.assert_equal(
          schema_item.get_attr('a'), schema_constants.FLOAT32.with_bag(db)
      )
      testing.assert_equal(
          schema_item.get_attr('b'), schema_constants.FLOAT64.with_bag(db)
      )

  def test_set_attr_ds_attr_name_errors(self):
    db = bag()
    x = db.obj()

    with self.subTest('str_attr_name'):
      with self.assertRaisesRegex(ValueError, 'must be a slice of STRING'):
        x.set_attr(ds([1, 2]), ds([123, 456]))

  def test_set_attr_incompatible_schema(self):
    db = bag()
    db2 = bag()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            """the schema for attribute 'x' is incompatible.

Expected schema for 'x': ENTITY(c=INT32)
Assigned schema for 'x': ENTITY(b=STRING)

To fix this, explicitly override schema of 'x' in the original schema by passing overwrite_schema=True."""
        ),
    ):
      db.new(x=db.new(c=1)).x = db2.new(b='a')

    o = db.new(x='a').embed_schema()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            """the schema for attribute 'x' is incompatible.

Expected schema for 'x': STRING
Assigned schema for 'x': INT32

To fix this, explicitly override schema of 'x' in the Object schema by passing overwrite_schema=True."""
        ),
    ):
      o.x = 1

    o1 = db.new(x=1).embed_schema()
    o2 = db.new(x=1.0).embed_schema()
    o = ds([o1, o2])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            """the schema for attribute 'x' is incompatible.

Expected schema for 'x': FLOAT32
Assigned schema for 'x': INT32

To fix this, explicitly override schema of 'x' in the Object schema by passing overwrite_schema=True."""
        ),
    ):
      o.x = 1

  def test_set_get_attr_empty_attr_name(self):
    db = bag()
    x = db.new()
    setattr(x.get_schema(), '', schema_constants.INT32)
    setattr(x, '', 1)
    testing.assert_equal(getattr(x, ''), ds(1).with_bag(db))

  def test_set_attr_auto_broadcasting(self):
    db = bag()
    x = db.new_shaped(jagged_shape.create_shape([3]))
    x.get_schema().xyz = schema_constants.INT32
    x.xyz = ds(12)
    testing.assert_equal(x.xyz, ds([12, 12, 12]).with_bag(db))

    x_abc = ds([12, 12])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            f'DataSlice with shape={x_abc.get_shape()} cannot be expanded to'
            f' shape={x.get_shape()}'
        ),
    ):
      x.abc = x_abc

  def test_set_get_attr_empty_entity(self):
    x = bag().new(a=1) & ds(None)
    testing.assert_equal(
        x.a, ds(None, schema_constants.INT32).with_bag(x.get_bag())
    )
    x = bag().new_shaped(jagged_shape.create_shape([2]), a=1) & ds(None)
    testing.assert_equal(
        x.a, ds([None, None], schema_constants.INT32).with_bag(x.get_bag())
    )

  def test_set_get_attr_empty_object(self):
    x = bag().obj(a=1) & ds(None)
    testing.assert_equal(x.a, ds(None).with_bag(x.get_bag()))
    x = bag().obj_shaped(jagged_shape.create_shape([2]), a=1) & ds(None)
    testing.assert_equal(x.a, ds([None, None]).with_bag(x.get_bag()))

  def test_get_attr_object_mixed_data_implicit_cast(self):
    db = bag()
    x = ds([db.obj(a=1), db.obj(a=2.0)])
    testing.assert_equal(x.a, ds([1.0, 2.0]).with_bag(db))

  def test_set_get_attr_object_missing_schema_attr(self):
    obj = bag().obj(a=1)
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex(
            'object schema is missing for the DataItem'
        ),
    ):
      _ = obj.with_bag(bag()).a
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'object schema is missing for the DataItem'
        ),
    ):
      obj.with_bag(bag()).a = 1
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            r'object schema is missing for the DataItem'
        ),
    ):
      del obj.with_bag(bag()).a  # pyrefly: ignore[missing-attribute]

  def test_set_get_attr_slice_of_objects_missing_schema_attr(self):
    db = bag()
    obj_1 = db.obj(a=1)
    obj_2 = db.new(a=1).with_schema(schema_constants.OBJECT)
    obj = ds([obj_1, obj_2])
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex(
            re.escape('object schema(s) are missing')
        ),
    ):
      _ = obj.a
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('object schema(s) are missing')
        ),
    ):
      obj.a = 1
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('object schema(s) are missing')
        ),
    ):
      del obj.a  # pyrefly: ignore[missing-attribute]

  def test_repr_with_removed(self):
    o = bag().obj(x=1, y=2)
    o.z = 3
    self.assertEqual(str(o), 'Obj(x=1, y=2, z=3)')
    del o.z  # pyrefly: ignore[missing-attribute]
    self.assertEqual(str(o), 'Obj(x=1, y=2)')

  def test_set_get_attr_object_wrong_schema_attr(self):
    obj = bag().obj(a=1)
    obj.set_attr('__schema__', schema_constants.INT32)
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex(
            'cannot get or set attributes on schema: INT32'
        ),
    ):
      _ = obj.a
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'cannot get or set attributes on schema: INT32'
        ),
    ):
      obj.a = 1
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                r'objects must have ObjectId(s) as __schema__ attribute, got'
                r' INT32'
            )
        ),
    ):
      del obj.a  # pyrefly: ignore[missing-attribute]

  def test_set_attr_merging(self):
    db1 = bag()
    db2 = bag()

    obj1 = db1.obj(a=1)
    obj2 = db1.obj(a=2)
    obj3 = db1.obj(a=3)
    root = db2.obj(b=ds([4, 5, 6]))
    root.c = ds([obj1, obj2, obj3])

    testing.assert_equal(root.c.a, ds([1, 2, 3]).with_bag(root.get_bag()))

  def test_set_get_attr_on_qvalue_properties(self):
    x = bag().obj()
    # qtype.
    x.set_attr('qtype', 42)
    testing.assert_equal(x.get_attr('qtype'), ds(42).with_bag(x.get_bag()))
    self.assertEqual(x.qtype, qtypes.DATA_SLICE)
    with self.assertRaisesRegex(
        AttributeError, r'attribute \'qtype\'.*is not writable'
    ):
      x.qtype = 42  # pyrefly: ignore[read-only]
    # fingerprint.
    x.set_attr('fingerprint', 42)
    testing.assert_equal(
        x.get_attr('fingerprint'), ds(42).with_bag(x.get_bag())
    )
    with self.assertRaisesRegex(
        AttributeError, r'attribute \'fingerprint\'.*is not writable'
    ):
      x.fingerprint = 42  # pyrefly: ignore[read-only]
    # DataSlice's specific property `db`.
    x.db = 42
    testing.assert_equal(x.db, ds(42).with_bag(x.get_bag()))

  def test_getattr_errors(self):
    x = ds([1, 2, 3])
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex("failed to get attribute 'abc'"),
    ):
      _ = x.abc
    with self.assertRaisesRegex(TypeError, r'attribute name must be string'):
      getattr(x, 12345)  # pyrefly: ignore[bad-argument-type]

  def test_set_get_attr_implicit_schema_slice_error(self):
    # NOTE: Regression test for b/364826956.
    db = bag()
    obj = db.obj(a=db.obj(x=1, y=3.14))
    entity = db.new(a=db.new(x=1, y=3.14))
    entity.get_schema().a = obj.a.get_attr('__schema__')
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex(
            "failed to get attribute 'a': DataSlice cannot have an implicit"
            ' schema as its schema'
        ),
    ):
      _ = entity.a  # Has implicit schema.

  def test_set_attr_none(self):
    with self.subTest('entity'):
      e = bag().new(x=42)
      e.x = None
      testing.assert_equal(
          e.x.get_schema(), schema_constants.INT32.with_bag(e.get_bag())
      )

      db = bag()
      e = db.new(x=db.new())
      e.x = None
      testing.assert_equal(e.x.get_schema(), e.get_schema().x)

    with self.subTest('object'):
      o = bag().obj(x=42)
      o.x = None
      testing.assert_equal(
          o.x.get_schema(), schema_constants.NONE.with_bag(o.get_bag())
      )

    with self.subTest('incompatible schema'):
      with self.assertRaisesRegex(
          ValueError,
          r'the schema for attribute \'x\' is incompatible',
      ):
        db = bag()
        db.new(x=db.new()).x = ds(None, schema_constants.ITEMID)

    with self.subTest('schema'):
      s = bag().new().get_schema()
      s.x = schema_constants.INT32
      s.x = None
      with self.assertRaisesRegex(
          AttributeError,
          r'the attribute \'x\' is missing on the schema',
      ):
        _ = s.x

  def test_clone_schema_with_removed_attr(self):
    sc1 = bag().new_schema(x=schema_constants.INT32, y=schema_constants.INT32)
    sc2 = sc1.fork_bag()
    del sc2.x  # pyrefly: ignore[missing-attribute]
    sc3 = sc2.clone()
    testing.assert_equal(sc3.y.no_bag(), schema_constants.INT32)

  def test_setattr_assignment_rhs_scalar(self):
    x = bag().obj(a=1)
    x.b = 4
    testing.assert_equal(x.b, ds(4).with_bag(x.get_bag()))

  def test_setattr_assignment_rhs_auto_packing_list(self):
    x = bag().obj(a=1)
    x.b = [1, 2, 3]
    testing.assert_equal(x.b[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_equal(
        x.b.get_schema().get_attr('__items__'),
        schema_constants.INT32.with_bag(x.get_bag()),
    )

  def test_setattr_assignment_rhs_auto_packing_dicts(self):
    x = bag().obj(a=1)
    x.b = {'a': {42: 3.14}, 'b': {37: 2.0}}
    testing.assert_dicts_keys_equal(x.b, ds(['a', 'b']))
    testing.assert_allclose(
        x.b[ds(['a', 'b', 'a'])][42],
        ds([3.14, None, 3.14]).with_bag(x.get_bag()),
    )
    testing.assert_allclose(x.b['b'][37], ds(2.0).with_bag(x.get_bag()))
    self.assertEqual(
        x.b.get_schema().get_attr('__keys__'), schema_constants.STRING
    )
    self.assertEqual(
        x.b.get_schema().get_attr('__values__').get_attr('__keys__'),
        schema_constants.INT32,
    )
    self.assertEqual(
        x.b.get_schema().get_attr('__values__').get_attr('__values__'),
        schema_constants.FLOAT32,
    )

  def test_setattr_assignment_rhs_error(self):
    x = bag().obj(a=ds([1, 2, 3]))
    with self.assertRaisesRegex(
        ValueError, 'only supported for Koda List DataItem'
    ):
      x.b = [4, 5, 6]
    with self.assertRaisesRegex(
        ValueError, re.escape('got DataSlice with shape JaggedShape(2)')
    ):
      x.b = [1, 2, ds([3, 4])]
    with self.assertRaisesRegex(
        ValueError, 'only supported for Koda Dict DataItem'
    ):
      x.b = {'abc': 42}

  def test_setattr_assignment_rhs_dict_error(self):
    x = bag().obj()
    with self.assertRaisesRegex(ValueError, 'unsupported type: Obj'):

      class Obj:
        pass

      x.b = {'a': Obj()}

    with self.assertRaisesRegex(
        ValueError, re.escape('got DataSlice with shape JaggedShape(3)')
    ):
      x.b = {'a': ds([1, 2, 3])}

    with self.assertRaisesRegex(
        ValueError, re.escape('got DataSlice with shape JaggedShape(3)')
    ):
      x.b = {'a': {42: ds([1, 2, 3])}}

  def test_set_multiple_attrs(self):
    x = bag().new(a=1, b='a')
    x.set_attrs(a=2, b='abc')
    testing.assert_equal(x.a, ds(2).with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds('abc').with_bag(x.get_bag()))

    with self.assertRaisesRegex(
        ValueError, r'schema for attribute \'b\' is incompatible'
    ):
      x.set_attrs(a=2, b=b'abc')

    x.set_attrs(a=2, b=b'abc', overwrite_schema=True)
    testing.assert_equal(x.a, ds(2).with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds(b'abc').with_bag(x.get_bag()))

  def test_set_multiple_attrs_with_merging(self):
    o = bag().obj(a=1)
    b = bag().new(x='abc', y=1234)
    o.set_attrs(b=b, d={'a': 42}, l=[1, 2, 3])

    testing.assert_equal(o.a, ds(1).with_bag(o.get_bag()))
    # Merged DataBag from another object / entity.
    testing.assert_equal(o.b.x, ds('abc').with_bag(o.get_bag()))
    testing.assert_equal(o.b.y, ds(1234).with_bag(o.get_bag()))
    # Merged DataBag from creating a DataBag during boxing of complex Python
    # values.
    testing.assert_equivalent(o.d, bag().dict({'a': 42}))
    testing.assert_equal(o.l[:], ds([1, 2, 3]).with_bag(o.get_bag()))

  def test_set_multiple_attrs_wrong_overwrite_schema_type(self):
    o = bag().obj()
    with self.assertRaisesRegex(
        TypeError, 'expected bool for overwrite_schema, got int'
    ):
      o.set_attrs(overwrite_schema=42)  # pyrefly: ignore[bad-argument-type]

  def test_del_attr(self):
    db = bag()

    with self.subTest('entity'):
      e = db.new(a=1, b=2)
      del e.a  # pyrefly: ignore[missing-attribute]
      testing.assert_equal(e.a, ds(None, schema_constants.INT32).with_bag(db))
      testing.assert_equal(
          e.a.get_schema(), schema_constants.INT32.with_bag(db)
      )
      del e.get_schema().b  # pyrefly: ignore[missing-attribute]
      with self.assertRaisesWithPredicateMatch(
          AttributeError,
          arolla.testing.any_cause_message_regex(
              re.escape(
                  """the attribute 'b' is missing on the schema.

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('b')"""
              )
          ),
      ):
        _ = e.b
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              re.escape(
                  """the attribute 'c' is missing on the schema.

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('c')"""
              )
          ),
      ):
        del e.get_schema().c  # pyrefly: ignore[missing-attribute]
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              re.escape(
                  """the attribute 'c' is missing on the schema.

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('c')"""
              )
          ),
      ):
        del e.c  # pyrefly: ignore[missing-attribute]

    with self.subTest('object'):
      o = db.obj(a=1, b=2)
      del o.a  # pyrefly: ignore[missing-attribute]
      with self.assertRaisesWithPredicateMatch(
          AttributeError,
          arolla.testing.any_cause_message_regex(
              re.escape(
                  """the attribute 'a' is missing on the schema.

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('a')"""
              )
          ),
      ):
        _ = o.a
      del o.get_attr('__schema__').b  # pyrefly: ignore[missing-attribute]
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              re.escape(
                  """the attribute 'b' is missing on the schema.

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('b')"""
              )
          ),
      ):
        del o.b  # pyrefly: ignore[missing-attribute]
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              re.escape(
                  """the attribute 'c' is missing on the schema.

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('c')"""
              )
          ),
      ):
        del o.c  # pyrefly: ignore[missing-attribute]

    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                """failed to get attribute 'a': the attribute 'a' is missing for at least one object at ds.flatten().S[1]

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('a')"""
            )
        ),
    ):
      _ = ds([[db.obj(a=1), db.obj(b=2)]]).a

  def test_maybe_method(self):
    db = bag()
    obj = ds([db.obj(a=1), db.obj(x=42), db.obj(a=3)])
    testing.assert_equal(obj.maybe('a'), ds([1, None, 3]).with_bag(db))
    testing.assert_equal(
        obj.maybe(ds(['a', 'a', 'c'])), ds([1, None, None]).with_bag(db)
    )

  def test_set_metadata(self):
    x = bag().new(a=1, b='a')
    y = bag().new(foo='bar')
    x.get_schema().set_metadata(a=2, b=y)
    x = x.freeze_bag()
    meta_a = kde.get_metadata(x.get_schema()).a.eval()
    meta_b_foo = kde.get_metadata(x.get_schema()).b.foo.eval()
    testing.assert_equal(meta_a, ds(2).with_bag(x.get_bag()))
    testing.assert_equal(meta_b_foo, ds('bar').with_bag(x.get_bag()))

  def test_set_metadata_multielement_slice(self):
    db = bag()
    schema1 = db.new_schema(a=schema_constants.INT32)
    schema2 = db.new_schema(b=schema_constants.FLOAT32)
    schemas = ds([schema1, schema2])
    schemas.set_metadata(abc='bar')
    meta_abc = user_facing_kd.get_metadata(schemas).abc
    testing.assert_equal(meta_abc, ds(['bar', 'bar']).with_bag(db))

  def test_set_metadata_no_bag_error(self):
    db = bag()
    schema = db.new_schema(a=schema_constants.INT32).no_bag()
    with self.assertRaisesRegex(
        ValueError, 'is a reference without a bag'
    ):
      schema.set_metadata(foo='bar')

  def test_set_metadata_immutable_bag_error(self):
    db = bag()
    schema = db.new_schema(a=schema_constants.INT32).with_bag(db.freeze())
    with self.assertRaisesRegex(
        ValueError, 'cannot modify/create item'
    ):
      schema.set_metadata(foo='bar')

  def test_set_metadata_non_schema_error(self):
    db = bag()
    x = db.new(a=1)
    with self.assertRaisesRegex(
        ValueError, 'cannot set for a DataSlice with'
    ):
      x.set_metadata(foo='bar')


if __name__ == '__main__':
  absltest.main()
