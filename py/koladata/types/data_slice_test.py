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

# Tests for DataSlice are split across multiple files to avoid linter timeouts:
# - data_slice_test.py: Core DataSlice creation, basic properties, operators.
# - data_slice_attrs_test.py: DataSlice attribute operations.
# - data_slice_bag_test.py: DataSlice DataBag operations, keys/values, and dir.
# - data_slice_dict_list_test.py: DataSlice dict and list operations.
# - data_slice_misc_test.py: Additional test classes (merging, fallback,
#   slicing).
# - data_slice_repr_test.py: DataSlice string and debug representation tests.
# - data_slice_shape_test.py: DataSlice shape, reshape, and selection tests.
import gc
import inspect
import sys

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.functions import functions as fns
from koladata.functor import boxing as _
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import list_item as _
from koladata.types import schema_constants


kde = kde_operators.kde
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals


class DataSliceTest(parameterized.TestCase):

  def test_ref_count(self):
    gc.collect()
    diff_count = 10
    base_count = sys.getrefcount(data_slice.DataSlice)
    slices = []
    for _ in range(diff_count):
      slices.append(ds([1]))  # Adding DataSlice(s)

    self.assertEqual(
        sys.getrefcount(data_slice.DataSlice), base_count + diff_count
    )

    # NOTE: ds() invokes `PyDataSlice_Type()` C Python function multiple times
    # and this verifies there are no leaking references.
    del slices
    gc.collect()
    self.assertEqual(sys.getrefcount(data_slice.DataSlice), base_count)

    # NOTE: with_schema() invokes `PyDataSlice_Type()` C Python function and
    # this verifies there are no leaking references.
    ds(1).with_schema(schema_constants.OBJECT)
    self.assertEqual(sys.getrefcount(data_slice.DataSlice), base_count)

    items = []
    for _ in range(diff_count):
      items.append(ds(1))  # Adding DataItem(s)
    self.assertEqual(sys.getrefcount(data_slice.DataSlice), base_count)

    res = ds([1]) + ds([2])
    self.assertEqual(sys.getrefcount(data_slice.DataSlice), base_count + 1)
    del res

    _ = ds([1, 2, 3]).S[0]
    self.assertEqual(sys.getrefcount(data_slice.DataSlice), base_count)

  def test_qvalue(self):
    self.assertTrue(issubclass(data_slice.DataSlice, arolla.QValue))
    x = ds([1, 2, 3])
    self.assertIsInstance(x, arolla.QValue)

  def test_fingerprint(self):
    x1 = ds(arolla.dense_array_int64([None, None]))
    x2 = ds([None, None], schema_constants.INT64)
    self.assertEqual(x1.fingerprint, x2.fingerprint)
    x3 = x2.with_schema(schema_constants.INT32)
    self.assertNotEqual(x1.fingerprint, x3.fingerprint)
    x4 = x2.with_schema(schema_constants.STRING)
    self.assertNotEqual(x1.fingerprint, x4.fingerprint)

    db = bag()
    self.assertNotEqual(x1.with_bag(db).fingerprint, x2.fingerprint)
    self.assertEqual(x1.with_bag(db).fingerprint, x2.with_bag(db).fingerprint)

    shape = jagged_shape.create_shape([2], [1, 1])
    self.assertNotEqual(x1.reshape(shape).fingerprint, x2.fingerprint)
    self.assertEqual(
        x1.reshape(shape).fingerprint, x2.reshape(shape).fingerprint
    )

    self.assertEqual(ds([42, 'abc']).fingerprint, ds([42, 'abc']).fingerprint)
    self.assertNotEqual(
        ds([42, 'abc']).fingerprint,
        ds([42, b'abc']).fingerprint,
    )

    self.assertNotEqual(ds([1]).fingerprint, ds(1).fingerprint)

  def test_unspecified(self):
    testing.assert_equal(data_slice.unspecified(), data_slice.unspecified())
    testing.assert_not_equal(ds(42), data_slice.unspecified())
    testing.assert_not_equal(
        data_slice.unspecified().with_bag(bag()), data_slice.unspecified()
    )

  # NOTE: DataSlice has custom __eq__ which works pointwise and returns another
  # DataSlice. So multi-dim DataSlices cannot be used as Python dict keys.
  def test_non_hashable(self):
    with self.assertRaisesRegex(TypeError, 'unhashable type'):
      hash(ds([1, 2, 3]))

  @parameterized.parameters(
      ([1, 2, 3], None, schema_constants.INT32),
      (['a', 'b'], None, schema_constants.STRING),
      ([b'a', b'b', b'c'], None, schema_constants.BYTES),
      (['a', b'b', 34], None, schema_constants.OBJECT),
      ([1, 2, 3], schema_constants.INT64, schema_constants.INT64),
      ([1, 2, 3], schema_constants.FLOAT64, schema_constants.FLOAT64),
  )
  def test_get_schema(self, inputs, qtype, expected_schema):
    x = ds(inputs, qtype)
    testing.assert_equal(x.get_schema(), expected_schema)
    testing.assert_equal(x.get_schema().get_schema(), schema_constants.SCHEMA)

  def test_with_schema(self):
    db = bag()
    x = db.new(x=ds([1, 2, 3]), y='abc')
    testing.assert_equal(x.get_schema().x, schema_constants.INT32.with_bag(db))
    testing.assert_equal(x.get_schema().y, schema_constants.STRING.with_bag(db))

    with self.assertRaisesRegex(
        TypeError, 'expecting schema to be a DataSlice, '
                   'got koladata.types.data_bag.DataBag'
    ):
      x.with_schema(db)  # pyrefly: ignore[bad-argument-type]

    with self.assertRaisesRegex(ValueError, "schema's schema must be SCHEMA"):
      x.with_schema(x)

    schema = db.new(x=1, y='abc').get_schema()
    testing.assert_equal(x.with_schema(schema).get_schema(), schema)

    non_schema = db.new().with_schema(schema_constants.SCHEMA)
    with self.assertRaisesRegex(
        ValueError, 'schema must contain either a DType or valid schema ItemId'
    ):
      x.with_schema(non_schema)

    with self.assertRaisesRegex(
        ValueError, 'a non-schema item cannot be present in a schema DataSlice'
    ):
      ds(1).with_schema(schema_constants.SCHEMA)

    # NOTE: Works without deep schema verification.
    ds([1, 'abc']).with_schema(schema_constants.SCHEMA)

  def test_set_schema(self):
    db = bag()
    x = db.new(x=ds([1, 2, 3]))

    with self.assertRaisesRegex(
        TypeError, 'expecting schema to be a DataSlice, got '
                   'koladata.types.data_bag.DataBag'
    ):
      x.set_schema(db)  # pyrefly: ignore[bad-argument-type]

    with self.assertRaisesRegex(ValueError, "schema's schema must be SCHEMA"):
      x.set_schema(x)

    schema = db.new(x=1, y='abc').get_schema()
    testing.assert_equal(x.set_schema(schema).get_schema(), schema)

    db_2 = bag()
    schema = db_2.new(x=1, y='abc').get_schema()
    res_schema = x.set_schema(schema).get_schema()
    testing.assert_equal(res_schema, schema.with_bag(db))
    testing.assert_equal(res_schema.y, schema_constants.STRING.with_bag(db))

    non_schema = db.new().set_schema(schema_constants.SCHEMA)
    with self.assertRaisesRegex(
        ValueError, 'schema must contain either a DType or valid schema ItemId'
    ):
      x.set_schema(non_schema)

    with self.assertRaisesRegex(
        ValueError,
        'cannot set an Entity schema on a DataSlice without a DataBag.',
    ):
      ds(1).set_schema(schema)

    with self.assertRaisesRegex(
        ValueError, 'a non-schema item cannot be present in a schema DataSlice'
    ):
      ds(1).with_bag(db).set_schema(schema_constants.SCHEMA)

    # NOTE: Works without deep schema verification.
    ds([1, 'abc']).with_bag(db).set_schema(schema_constants.SCHEMA)

  def test_magic_methods(self):
    x = ds([1, 2, 3])
    y = ds([4, 5, 6])
    z = ds([1, 2, None])
    mask = ds([arolla.present(), None, arolla.present()])
    with self.subTest('add'):
      testing.assert_equal(x + y, ds([5, 7, 9]))
      # With auto-boxing
      testing.assert_equal(x + 4, ds([5, 6, 7]))
      # __radd__ with auto-boxing
      testing.assert_equal(4 + x, ds([5, 6, 7]))
    with self.subTest('sub'):
      testing.assert_equal(x - y, ds([-3, -3, -3]))
      # With auto-boxing
      testing.assert_equal(x - 4, ds([-3, -2, -1]))
      # __rsub__ with auto-boxing
      testing.assert_equal(4 - x, ds([3, 2, 1]))
    with self.subTest('mul'):
      testing.assert_equal(x * y, ds([4, 10, 18]))
      # With auto-boxing
      testing.assert_equal(x * 4, ds([4, 8, 12]))
      # __rmul__ with auto-boxing
      testing.assert_equal(4 * x, ds([4, 8, 12]))
    with self.subTest('div'):
      testing.assert_equal(y / x, ds([4, 2.5, 2]))
      # With auto-boxing
      testing.assert_equal(z / 2, ds([0.5, 1, None]))
      # __rtruediv__ with auto-boxing
      testing.assert_equal(2 / z, ds([2.0, 1.0, None]))
    with self.subTest('floordiv'):
      testing.assert_equal(y // x, ds([4, 2, 2]))
      # With auto-boxing
      testing.assert_equal(z // 2, ds([0, 1, None]))
      # __rfloordiv__ with auto-boxing
      testing.assert_equal(2 // z, ds([2, 1, None]))
    with self.subTest('mod'):
      testing.assert_equal(y % x, ds([0, 1, 0]))
      # With auto-boxing
      testing.assert_equal(z % 2, ds([1, 0, None]))
      # __rmod__ with auto-boxing
      testing.assert_equal(2 % z, ds([0, 0, None]))
    with self.subTest('pow'):
      testing.assert_equal(x**z, ds([1.0, 4.0, None]))
      # With auto-boxing
      testing.assert_equal(z**2, ds([1.0, 4.0, None]))
      # __rpow__ with auto-boxing
      testing.assert_equal(2**z, ds([2.0, 4.0, None]))
    with self.subTest('and'):
      testing.assert_equal(x & mask, ds([1, None, 3]))
      # only __rand__ with auto-boxing
      testing.assert_equal(1 & mask, ds([1, None, 1]))
    with self.subTest('eq'):
      testing.assert_equal(
          x == z, ds([arolla.present(), arolla.present(), None])
      )
      # With auto-boxing
      testing.assert_equal(x == 2, ds([None, arolla.present(), None]))
      testing.assert_equal(
          2 == x, ds([None, arolla.present(), None])  # pyrefly: ignore[bad-argument-type]
      )
    with self.subTest('ne'):
      testing.assert_equal(
          x != z, ds([None, None, None], schema_constants.MASK)
      )
      # With auto-boxing
      testing.assert_equal(
          x != 2, ds([arolla.present(), None, arolla.present()])
      )
      testing.assert_equal(
          2 != x, ds([arolla.present(), None, arolla.present()])  # pyrefly: ignore[bad-argument-type]
      )
    with self.subTest('gt'):
      testing.assert_equal(
          y > z, ds([arolla.present(), arolla.present(), None])
      )
      # With auto-boxing
      testing.assert_equal(
          x > 1, ds([None, arolla.present(), arolla.present()])
      )
      testing.assert_equal(
          2 > x, ds([arolla.present(), None, None])  # pyrefly: ignore[bad-argument-type]
      )
    with self.subTest('ge'):
      testing.assert_equal(
          y >= z, ds([arolla.present(), arolla.present(), None])
      )
      # With auto-boxing
      testing.assert_equal(
          x >= 2, ds([None, arolla.present(), arolla.present()])
      )
      testing.assert_equal(
          1 >= x, ds([arolla.present(), None, None])  # pyrefly: ignore[bad-argument-type]
      )
    with self.subTest('lt'):
      testing.assert_equal(y < z, ds([None, None, None], schema_constants.MASK))
      # With auto-boxing
      testing.assert_equal(x < 2, ds([arolla.present(), None, None]))
      testing.assert_equal(
          2 < x, ds([None, None, arolla.present()])  # pyrefly: ignore[bad-argument-type]
      )
    with self.subTest('le'):
      testing.assert_equal(
          y <= z, ds([None, None, None], schema_constants.MASK)
      )
      # With auto-boxing
      testing.assert_equal(
          x <= 2, ds([arolla.present(), arolla.present(), None])
      )
      testing.assert_equal(
          2 <= x, ds([None, arolla.present(), arolla.present()])  # pyrefly: ignore[bad-argument-type]
      )
    with self.subTest('or'):
      testing.assert_equal((x & mask) | y, ds([1, 5, 3]))
      # With auto-boxing
      testing.assert_equal((x & mask) | 4, ds([1, 4, 3]))
      # __ror__ with auto-boxing
      testing.assert_equal(None | y, ds([4, 5, 6]))
    with self.subTest('xor'):
      testing.assert_equal(
          ds([arolla.present(), None]) ^ ds([None, arolla.present()]),
          ds([arolla.present(), arolla.present()])
      )
      # With auto-boxing
      testing.assert_equal(
          ds([arolla.present(), None]) ^ None, ds([arolla.present(), None])
      )
      # __rxor__ with auto-boxing
      testing.assert_equal(
          None ^ ds([arolla.present(), None]), ds([arolla.present(), None])
      )
    with self.subTest('not'):
      testing.assert_equal(~mask, ds([None, arolla.present(), None]))
    with self.subTest('lshift'):
      o = fns.new(x=1, y=2)
      db = kde.attrs(o, x=3, z=4).eval()
      testing.assert_equal((o << db).no_bag(), o.no_bag())
      testing.assert_equivalent(
          o << db, fns.new(x=3, y=2, z=4), schemas_equality=False
      )
      testing.assert_equal((db << o).no_bag(), o.no_bag())
      testing.assert_equivalent(
          db << o, fns.new(x=1, y=2, z=4), schemas_equality=False
      )
      with self.assertRaisesRegex(
          ValueError,
          'at least one argument must be a DATA_BAG, this operation is not'
          ' supported on two DATA_SLICEs',
      ):
        _ = o << o
    with self.subTest('rshift'):
      o = fns.new(x=1, y=2)
      db = kde.attrs(o, x=3, z=4).eval()
      testing.assert_equal((o >> db).no_bag(), o.no_bag())
      testing.assert_equivalent(
          o >> db, fns.new(x=1, y=2, z=4), schemas_equality=False
      )
      testing.assert_equal((db >> o).no_bag(), o.no_bag())
      testing.assert_equivalent(
          db >> o, fns.new(x=3, y=2, z=4), schemas_equality=False
      )
      with self.assertRaisesRegex(
          ValueError,
          'at least one argument must be a DATA_BAG, this operation is not'
          ' supported on two DATA_SLICEs',
      ):
        _ = o >> o

  def test_embed_schema_entity(self):
    db = bag()
    x = db.new(a=ds([1, 2]))
    x_object = x.embed_schema()
    testing.assert_equal(
        x_object.get_schema(), schema_constants.OBJECT.with_bag(db)
    )
    testing.assert_equal(x_object.a, x.a)
    schema_attr = x_object.get_attr('__schema__')
    testing.assert_equal(
        schema_attr == x.get_schema(), ds([arolla.present(), arolla.present()])
    )

  def test_embed_schema_primitive(self):
    testing.assert_equal(
        ds([1, 2, 3]).embed_schema(), ds([1, 2, 3], schema_constants.OBJECT)
    )

  def test_embed_schema_none(self):
    testing.assert_equal(
        ds(None).embed_schema(), ds(None, schema_constants.OBJECT)
    )
    testing.assert_equal(
        ds([None]).embed_schema(), ds([None], schema_constants.OBJECT)
    )
    testing.assert_equal(
        ds([[None, None], [None], []]).embed_schema(),
        ds([[None, None], [None], []], schema_constants.OBJECT),
    )

  # More comprehensive tests are in the schema_get_primitive_schema_test.py.
  def test_get_dtype(self):
    testing.assert_equal(ds([1, 2, 3]).get_dtype(), schema_constants.INT32)
    testing.assert_equal(
        bag().new(x=1).get_dtype(), schema_constants.INT32 & None
    )

  def test_get_obj_schema(self):
    x = ds([1, None, 1.1], schema_constants.OBJECT)
    expected = ds([schema_constants.INT32, None, schema_constants.FLOAT32])
    testing.assert_equal(x.get_obj_schema(), expected)

    db = bag()
    obj = db.obj(x=1)
    x = ds([1, 1.2, obj, 'a'])
    expected = ds([
        schema_constants.INT32,
        schema_constants.FLOAT32,
        obj.get_attr('__schema__'),
        schema_constants.STRING,
    ])
    testing.assert_equal(x.get_obj_schema(), expected)

  def test_with_schema_from_obj(self):
    entity = bag().new(x=1)
    obj = entity.embed_schema()
    testing.assert_equal(obj.with_schema_from_obj(), entity)

    with self.assertRaisesRegex(
        ValueError, 'DataSlice cannot have an implicit schema as its schema'
    ):
      bag().obj(x=1).with_schema_from_obj()

  def test_follow(self):
    x = kde.new().eval()
    testing.assert_equal(kde.nofollow(x).eval().follow(), x)
    with self.assertRaisesRegex(ValueError, 'a nofollow schema is required'):
      ds([1, 2, 3]).follow()

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_extract(self, pass_schema):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = o.extract(o.get_schema())
    else:
      result = o.extract()

    self.assertNotEqual(o.get_bag().fingerprint, result.get_bag().fingerprint)
    testing.assert_equivalent(result, o, ids_equality=True)

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_extract_update(self, pass_schema):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = o.extract_update(o.get_schema())
    else:
      result = o.extract_update()

    self.assertNotEqual(o.get_bag().fingerprint, result.fingerprint)
    res_o = o.with_bag(result)
    testing.assert_equivalent(res_o, o, ids_equality=True)

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_clone(self, pass_schema):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = o.clone(schema=o.get_schema())
    else:
      result = o.clone()

    testing.assert_not_equal(result.no_bag(), o.no_bag())
    testing.assert_equivalent(result, o)
    testing.assert_equivalent(result.b, o.b, ids_equality=True)

  def test_clone_with_overrides(self):
    x = bag().obj(y=bag().obj(a=1), z=bag().list([2, 3]))
    res = x.clone(z=bag().list([12]), t=bag().obj(b=5))
    testing.assert_equivalent(res.y, x.y, ids_equality=True)
    testing.assert_equal(res.z[:].no_bag(), ds([12]))
    testing.assert_equal(res.t.b.no_bag(), ds(5))

  def test_clone_non_deterministic(self):
    x = bag().obj(a=1)
    self.assertNotEqual(x.clone(), x.clone())

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_shallow_clone(self, pass_schema):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = o.shallow_clone(schema=o.get_schema())
    else:
      result = o.shallow_clone()

    testing.assert_not_equal(result.no_bag(), o.no_bag())
    testing.assert_equal(result.b.no_bag(), o.b.no_bag())
    testing.assert_equal(result.c.no_bag(), o.c.no_bag())
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex("attribute 'a' is missing"),
    ):
      _ = result.b.a

  def test_shallow_clone_with_overrides(self):
    x = bag().obj(y=bag().obj(a=1), z=bag().list([2, 3]))
    res = x.shallow_clone(z=bag().list([12]), t=bag().obj(b=5))
    testing.assert_equivalent(
        res.y.with_schema(schema_constants.ITEMID),
        x.y.with_schema(schema_constants.ITEMID),
        ids_equality=True,
    )
    testing.assert_equivalent(res.z[:], ds([12]))
    testing.assert_equivalent(res.t.b, ds(5))

  def test_shallow_clone_non_deterministic(self):
    x = bag().obj(a=1)
    self.assertNotEqual(x.shallow_clone(), x.shallow_clone())

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_deep_clone(self, pass_schema):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    if pass_schema:
      result = o.deep_clone(o.get_schema())
    else:
      result = o.deep_clone()

    testing.assert_not_equal(result.no_bag(), o.no_bag())
    testing.assert_not_equal(result.b.no_bag(), o.b.no_bag())
    testing.assert_equivalent(result, o)

  def test_deep_clone_with_overrides(self):
    x = bag().obj(y=bag().obj(a=1), z=bag().list([2, 3]))
    res = x.deep_clone(z=bag().list([12]), t=bag().obj(b=5))
    testing.assert_equal(res.y.a.no_bag(), ds(1))
    testing.assert_equal(res.z[:].no_bag(), ds([12]))
    testing.assert_equal(res.t.b.no_bag(), ds(5))

  def test_deep_clone_non_deterministic(self):
    x = bag().obj(a=1)
    self.assertNotEqual(x.deep_clone(), x.deep_clone())

  def test_clone_as_full(self):
    x = bag().new(a=ds([1, 2])) & ds([arolla.present(), None])
    cloned = x.clone_as_full(b=ds([3, 4]))
    testing.assert_equal(
        cloned.get_present_count(), ds(2, schema=schema_constants.INT64)
    )
    testing.assert_equal(cloned.a.no_bag(), ds([1, None]))
    testing.assert_equal(cloned.b.no_bag(), ds([3, 4]))
    testing.assert_equivalent(
        cloned.get_schema(),
        x.get_schema().freeze_bag().with_attrs(b=schema_constants.INT32),
        ids_equality=True,
    )

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_deep_uuid(self, pass_schema):
    db = data_bag.DataBag.empty_mutable()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = o.deep_uuid(o.get_schema())
    else:
      result = o.deep_uuid()
    testing.assert_not_equal(result.S[0], result.S[1])
    odb = data_bag.DataBag.empty_mutable()
    o2 = odb.obj(b=odb.new(a=1), c='foo')
    result2 = o2.deep_uuid()
    testing.assert_equal(result2, result.S[0])

  def test_deep_uuid_with_schema_and_seed(self):
    s = bag().new_schema(x=schema_constants.INT32)
    x = bag().obj(x=42, y='abc')
    _ = x.deep_uuid(schema=s, seed='seed')

  def test_with_name(self):
    x = ds([1, 2, 3])
    y = x.with_name('foo')
    self.assertIs(y, x)

  def test_signatures(self):
    # Tests that all methods have an inspectable signature. This is not added
    # automatically for methods defined in CPython and requires the docstring
    # to follow a specific format.
    for fn_name in dir(data_slice.DataSlice):
      if fn_name.startswith('_'):
        continue
      fn = getattr(data_slice.DataSlice, fn_name)
      if callable(fn):
        _ = inspect.signature(fn)  # Shouldn't raise.

  def test_new(self):
    with self.assertRaisesRegex(ValueError, 'only Schema'):
      _ = ds([1, 2, 3]).new()

  def test_strict_new(self):
    with self.assertRaisesRegex(ValueError, 'only Schema'):
      _ = ds([1, 2, 3]).strict_new()

  def test_get_item_schema(self):
    with self.assertRaisesRegex(ValueError, 'only List SchemaItem'):
      _ = ds([1, 2, 3]).get_item_schema()

  def test_get_key_schema(self):
    with self.assertRaisesRegex(ValueError, 'only Dict SchemaItem'):
      _ = ds([1, 2, 3]).get_key_schema()

  def test_get_value_schema(self):
    with self.assertRaisesRegex(ValueError, 'only Dict SchemaItem'):
      _ = ds([1, 2, 3]).get_value_schema()

  def test_get_nofollowed_schema(self):
    with self.assertRaisesRegex(ValueError, 'only SchemaItem'):
      _ = ds([1, 2, 3]).get_nofollowed_schema()

  def test_bind(self):
    with self.assertRaisesRegex(ValueError, 'only a Functor'):
      _ = ds([1, 2, 3]).bind()

  def test_call(self):
    with self.assertRaisesRegex(
        ValueError, 'only a Functor can be called'
    ):
      _ = ds([1, 2, 3])()

  def test_int(self):
    with self.assertRaisesRegex(
        ValueError, 'only a scalar DataSlice can be converted to int'
    ):
      _ = int(ds([42]))

  def test_float(self):
    with self.assertRaisesRegex(
        ValueError, 'only a scalar DataSlice can be converted to float'
    ):
      _ = float(ds([3.14]))

  def test_index(self):
    with self.assertRaisesRegex(
        ValueError, 'only a scalar DataSlice can be converted to index'
    ):
      _ = [4, 5, 6][ds([4]) : 7]

  def test_len(self):
    with self.assertRaisesRegex(
        ValueError, 'only ListItem and DictItem have a __len__ method'
    ):
      _ = len(ds([1, 2, 3]))

  def test_contains(self):
    with self.assertRaisesRegex(
        ValueError, 'only ListItem and DictItem have a __contains__ method'
    ):
      _ = 1 in ds([1, 2, 3])


if __name__ == '__main__':
  absltest.main()
