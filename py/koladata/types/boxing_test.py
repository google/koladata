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

"""Tests for data_slice."""

import gc

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
# Needed for self.assertEqual(item_1, item_2).
from koladata.exceptions import exceptions
from koladata.operators import comparison as _  # pylint: disable=unused-import
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

INT32 = schema_constants.INT32
INT64 = schema_constants.INT64
FLOAT32 = schema_constants.FLOAT32
FLOAT64 = schema_constants.FLOAT64
MASK = schema_constants.MASK
BOOLEAN = schema_constants.BOOLEAN
BYTES = schema_constants.BYTES
TEXT = schema_constants.TEXT
EXPR = schema_constants.EXPR
OBJECT = schema_constants.OBJECT
SCHEMA = schema_constants.SCHEMA
ANY = schema_constants.ANY
NONE = schema_constants.NONE

ds = data_slice.DataSlice.from_vals


class BoxingTest(parameterized.TestCase):

  @parameterized.parameters(
      ([None], None, [None], NONE),
      ([], None, [], OBJECT),
      ([1, 2, 3], None, [1, 2, 3], INT32),
      ([3.14], FLOAT64, [3.14], FLOAT64),
      (
          [[[1, 2], [3]], [[4], [5, 6]]],
          None,
          [[[1, 2], [3]], [[4], [5, 6]]],
          INT32,
      ),
      ('abc', None, 'abc', TEXT),
      ("a'b'c", None, "a'b'c", TEXT),
      ([b'abc', b'xyz'], None, [b'abc', b'xyz'], BYTES),
      ([1, None, 4], None, [1, None, 4], INT32),
      ([1, None, 4], INT64, [1, None, 4], INT64),
      ([1, None, 4], FLOAT32, [1, None, 4], FLOAT32),
      ([b'xyz', None], BYTES, [b'xyz', None], BYTES),
      ([True, False, None], None, [True, False, None], BOOLEAN),
      ([True, False, None], INT32, [1, 0, None], INT32),
      ([True, False, None], FLOAT32, [1.0, 0.0, None], FLOAT32),
      (
          [arolla.present(), arolla.unit(), None, arolla.missing()],
          None,
          [arolla.present(), arolla.present(), None, None],
          MASK,
      ),
      # Mixed.
      (
          [[1, 1.0, 'abc'], [b'xyz', True]],
          None,
          [[1, 1.0, 'abc'], [b'xyz', True]],
          OBJECT,
      ),
      (['abc', b'xyz', None], None, ['abc', b'xyz', None], OBJECT),
      ([1, 2.0, None], None, [1.0, 2.0, None], FLOAT32),
      # DataSlice inputs.
      (ds([1, 2, 3]), None, [1, 2, 3], INT32),
      (ds([1, 2, 3]), FLOAT32, [1.0, 2.0, 3.0], FLOAT32),
      (
          ds([[1, 2, 3], [4, 5]]),
          None,
          [[1, 2, 3], [4, 5]],
          INT32,
      ),
      ([ds('abc'), ds(12), None], None, ['abc', 12, None], OBJECT),
      ([ds(3.14), ds(12), None], INT32, [3, 12, None], INT32),
      # DenseArray input.
      (arolla.dense_array([1, 2, 3]), None, [1, 2, 3], INT32),
      (arolla.dense_array(['a', None, 'b']), None, ['a', None, 'b'], TEXT),
      # Arolla Array inputs.
      (arolla.array([1, 2, 3]), None, [1, 2, 3], INT32),
      (arolla.array(['a', None, 'b']), None, ['a', None, 'b'], TEXT),
      # Arolla scalar values.
      ([arolla.int32(1), arolla.int32(12), 134], None, [1, 12, 134], INT32),
      ([arolla.int32(1), arolla.int32(12), 134], INT64, [1, 12, 134], INT64),
      (
          [arolla.int32(1), arolla.float32(12), None],
          None,
          [1.0, 12.0, None],
          FLOAT32,
      ),
      ([arolla.text('abc'), None], None, ['abc', None], TEXT),
      ([arolla.bytes(b'abc'), None], None, [b'abc', None], BYTES),
      (
          [arolla.float64(3.0), arolla.int64(1_000_000_000_000)],
          None,
          [3.0, float(1_000_000_000_000)],
          FLOAT64,
      ),
      # 3.14 is parsed as FLOAT32 unless explicitly casted to FLOAT64. This
      # causes some precision loss, but 1) in the case of implicit casting, it
      # allows us to avoid two passes over the data, and 2) in the case of
      # OBJECT, we want to avoid FLOAT64 if possible.
      ([3.14, arolla.float64(1.0)], None, [3.140000104904175, 1.0], FLOAT64),
      ([3.14, arolla.float64(1.0)], OBJECT, [3.140000104904175, 1.0], OBJECT),
      ([3.14, arolla.float64(1.0)], FLOAT64, [3.14, 1.0], FLOAT64),
      # Arolla QTypes become DataItem(s).
      (
          [arolla.INT32, None, arolla.FLOAT64],
          None,
          [INT32, None, FLOAT64],
          SCHEMA,
      ),
      # Arolla optional values.
      (
          [arolla.optional_int32(1234), arolla.optional_int32(None)],
          None,
          [1234, None],
          INT32,
      ),
      (
          [
              arolla.optional_float32(None),
              arolla.optional_float64(12.0),
              arolla.optional_text(None),
              arolla.optional_bytes(b'abc'),
              arolla.optional_int64(None),
          ],
          None,
          [None, 12.0, None, b'abc', None],
          OBJECT,
      ),
      (
          [
              arolla.quote(arolla.M.math.add(arolla.L.x, arolla.L.y)),
              None,
              arolla.quote(arolla.L.y),
          ],
          None,
          [
              arolla.quote(arolla.M.math.add(arolla.L.x, arolla.L.y)),
              None,
              arolla.quote(arolla.L.y),
          ],
          EXPR,
      ),
  )
  def test_roundtrip(self, val, dtype, expected, expected_schema):
    x = ds(val, dtype=dtype)
    self.assertEqual(x.internal_as_py(), expected)
    testing.assert_equal(x.get_schema(), expected_schema)

  @parameterized.parameters(
      ('abc', None, 'abc', TEXT),
      (b'abc', None, b'abc', BYTES),
      ('abc', BYTES, b'abc', BYTES),
      (b'abc', TEXT, 'abc', TEXT),
      (12, None, 12, INT32),
      (12, INT32, 12, INT32),
      # The following needs arolla.INT64 to succeed.
      (1 << 43, INT64, 1 << 43, INT64),
      (12, INT64, 12, INT64),
      (3.14, None, 3.14, FLOAT32),
      (2.71, FLOAT64, 2.71, FLOAT64),
      (True, None, True, BOOLEAN),
      (False, None, False, BOOLEAN),
      (None, None, None, NONE),
      (None, TEXT, None, TEXT),
      (True, INT32, 1, INT32),
      (True, FLOAT32, 1.0, FLOAT32),
      (arolla.present(), None, arolla.present(), MASK),
      (arolla.unit(), None, arolla.present(), MASK),
      # DataItem input.
      (ds(1), None, 1, INT32),
      (ds(1), FLOAT32, 1.0, FLOAT32),
      # Arolla scalar value.
      (arolla.int32(2), FLOAT64, 2.0, FLOAT64),
      # Arolla optional value.
      (arolla.optional_int32(2), None, 2, INT32),
      (arolla.optional_int32(None), None, None, INT32),
      (
          arolla.quote(arolla.M.math.add(arolla.L.x, arolla.L.y)),
          None,
          arolla.quote(arolla.M.math.add(arolla.L.x, arolla.L.y)),
          EXPR,
      ),
  )
  def test_scalars_roundtrip(self, value, dtype, expected, expected_schema):
    x = ds(value, dtype=dtype)
    self.assertIsInstance(x, data_item.DataItem)
    self.assertAlmostEqual(x.internal_as_py(), expected, places=5)
    testing.assert_equal(x.get_schema(), expected_schema)

  def test_missing_unit_schema(self):
    testing.assert_equal(ds(arolla.missing()).get_schema(), MASK)

  def test_dtype_none(self):
    x = ds([1, 2, 3], dtype=None)
    self.assertEqual(x.internal_as_py(), [1, 2, 3])
    self.assertEqual(x.get_schema(), INT32)

    x = ds([1, 2, 3], None)
    self.assertEqual(x.internal_as_py(), [1, 2, 3])
    self.assertEqual(x.get_schema(), INT32)

  def test_invalid_dtype_argument_usage(self):
    with self.assertRaisesRegex(TypeError, "got an unexpected keyword 'c'"):
      ds(None, c=12)

    with self.assertRaisesRegex(TypeError, "got an unexpected keyword 'c'"):
      ds(None, c=12, dtype=INT64)

    with self.assertRaisesRegex(
        TypeError, r'got multiple values for argument \'dtype\''
    ):
      ds([1, 2, 3], INT64, dtype=INT32)

  def test_roundtrip_for_schema(self):
    inputs = [INT32, TEXT, ANY]
    x = ds(inputs)
    self.assertIsInstance(x, data_slice.DataSlice)
    testing.assert_equal(x.get_schema(), SCHEMA)
    for o, expected_o in zip(x.internal_as_py(), inputs):
      testing.assert_equal(o, expected_o)

  def test_entities(self):
    db = data_bag.DataBag.empty()
    with self.assertRaisesRegex(
        exceptions.KodaError, 'cannot find a common schema'
    ):
      ds([db.new(), db.new()])

    with self.assertRaisesRegex(
        exceptions.KodaError, 'cannot find a common schema'
    ):
      ds([db.new(), 42])

    e1 = db.new()
    e2 = db.new().with_schema(e1.get_schema())
    x = ds([e1, e2])
    testing.assert_equal(x.internal_as_py()[0], e1)
    testing.assert_equal(x.internal_as_py()[1], e2)

  def test_no_common_schema_error_message(self):
    db = data_bag.DataBag.empty()
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""cannot find a common schema for provided schemas

 the common schema\(s\) [0-9a-f]{32}: SCHEMA\(x=INT32\)
 the first conflicting schema [0-9a-f]{32}: SCHEMA\(\)""",
    ):
      ds([db.new(x=1), db.new()])

    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""cannot find a common schema for provided schemas

 the common schema\(s\) OBJECT: OBJECT
 the first conflicting schema [0-9a-f]{32}: SCHEMA\(\)""",
    ):
      ds([1, 'a', db.new()])

    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""cannot find a common schema for provided schemas

 the common schema\(s\) OBJECT: OBJECT
 the first conflicting schema [0-9a-f]{32}: SCHEMA\(\)""",
    ):
      ds([db.new(), 1, 'a'])

    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""cannot find a common schema for provided schemas

 the common schema\(s\) [0-9a-f]{32}: SCHEMA\(\)
 the first conflicting schema [0-9a-f]{32}: SCHEMA\(\)""",
    ):
      ds([db.new(), 1, 'a', db.new()])

  def test_schema_embedding(self):
    db1 = data_bag.DataBag.empty()
    db2 = data_bag.DataBag.empty()
    e1 = db1.new()
    e2 = db2.new()
    res = ds([e1, e2], OBJECT)
    testing.assert_equal(res.get_schema(), OBJECT.with_db(res.db))
    testing.assert_equal(
        res.internal_as_py()[0].get_attr('__schema__'),
        e1.get_schema().with_db(res.db),
    )
    testing.assert_equal(
        res.internal_as_py()[1].get_attr('__schema__'),
        e2.get_schema().with_db(res.db),
    )
    # The original bags are unaffected.
    testing.assert_equal(
        e1.get_attr('__schema__'), ds(None, SCHEMA).with_db(db1)
    )
    testing.assert_equal(
        e2.get_attr('__schema__'), ds(None, SCHEMA).with_db(db2)
    )

  def test_single_entity_schema_embedding(self):
    db = data_bag.DataBag.empty()
    e1 = db.new()
    res = ds(e1, OBJECT)
    testing.assert_equal(res, e1.with_db(res.db).with_schema(OBJECT))
    testing.assert_equal(
        res.get_attr('__schema__'),
        e1.get_schema().with_db(res.db),
    )
    # The original bag is unaffected.
    testing.assert_equal(
        e1.get_attr('__schema__'), ds(None, SCHEMA).with_db(db)
    )

  def test_schema_embedding_conflicting_schema(self):
    db1 = data_bag.DataBag.empty()
    e1 = db1.new().embed_schema()
    with self.assertRaisesRegex(
        ValueError, 'conflicting values for __schema__'
    ):
      # Try to embed a schema that conflicts with the existing one.
      ds([e1.with_schema(db1.new().get_schema())], OBJECT)

  def test_objects(self):
    db = data_bag.DataBag.empty()
    o1 = db.obj()
    o2 = db.obj()
    x = ds([o1, o2, 42])
    testing.assert_equal(x.internal_as_py()[0], o1)
    testing.assert_equal(x.internal_as_py()[1], o2)
    self.assertEqual(x.internal_as_py()[2], 42)

    e1 = db.new()
    with self.assertRaisesRegex(
        exceptions.KodaError, 'cannot find a common schema'
    ):
      ds([o1, o2, e1])
    x = ds([o1, o2, e1.embed_schema()])
    testing.assert_equal(x.internal_as_py()[0], o1)
    testing.assert_equal(x.internal_as_py()[1], o2)
    testing.assert_equal(x.internal_as_py()[2], e1.embed_schema())

  def test_list_of_entities(self):
    db = data_bag.DataBag.empty()
    # Same schema and db for items from internal_as_py.
    x = db.new(x=ds([1, 2, 3]), y=ds(['a', 'b', 'c']))
    self.assertIsInstance(x.internal_as_py()[0], data_item.DataItem)
    testing.assert_equal(x.internal_as_py()[0].db, x.db)
    testing.assert_equal(x.internal_as_py()[0].get_schema(), x.get_schema())
    x = x.as_any()
    testing.assert_equal(x.internal_as_py()[0].get_schema(), ANY.with_db(db))

  def test_scalars_overflow_handling(self):
    with self.subTest('int'):
      self.assertEqual(ds(1 << 43).internal_as_py(), 1 << 43)
      self.assertEqual(ds(1 << 43).get_schema(), INT64)
      self.assertEqual(ds(1 << 43, INT64).internal_as_py(), 1 << 43)
      self.assertEqual(ds(1 << 43, INT64).get_schema(), INT64)
      # Overflows int64.
      self.assertEqual(ds(1 << 63).internal_as_py(), -(1 << 63))
      self.assertEqual(ds(1 << 63).get_schema(), INT64)
      self.assertEqual(ds((1 << 100) + 43).internal_as_py(), 43)
      self.assertEqual(ds((1 << 100) + 43).get_schema(), INT64)
      # Overflow error for int32.
      with self.assertRaisesRegex(ValueError, 'cannot cast'):
        ds(1 << 43, INT32)

    with self.subTest('float'):
      py_val = 3.14371857238947
      self.assertEqual(ds(py_val, FLOAT64).internal_as_py(), py_val)
      for dtype in (None, FLOAT32):
        self.assertNotEqual(ds(py_val, dtype).internal_as_py(), py_val)
        self.assertAlmostEqual(
            ds(py_val, dtype).internal_as_py(), py_val, places=5
        )

  def test_from_vals_all_empty(self):
    with self.subTest('no dtype provided'):
      x = ds([[None, None, None], [None, None]])
      testing.assert_equal(x.get_schema(), NONE)
      self.assertEqual(x.internal_as_py(), [[None, None, None], [None, None]])
      with self.assertRaisesRegex(
          ValueError,
          'empty slices can be converted to Arolla value only if they have '
          'primitive schema',
      ):
        x.as_arolla_value()

      schema = ds(None, SCHEMA)
      self.assertIsNone(schema.internal_as_py())
      testing.assert_equal(schema.get_schema(), SCHEMA)

      x = ds([None, None], ANY)
      self.assertEqual(x.internal_as_py(), [None, None])
      testing.assert_equal(x.get_schema(), ANY)

    with self.subTest('dtype provided'):
      x = ds([None, None, None], INT32)
      testing.assert_equal(x.get_schema(), INT32)
      self.assertEqual(x.internal_as_py(), [None, None, None])

  def test_from_vals_invalid_nested_list(self):
    with self.assertRaisesRegex(
        ValueError, 'input has to be a valid nested list'
    ):
      ds([[1, 2, 3], 4])

  def test_from_vals_type_errors(self):
    class Klass:
      pass

    with self.assertRaisesRegex(
        ValueError, 'object with unsupported type: "Klass" in nested list'
    ):
      ds(Klass())
    with self.assertRaisesRegex(
        ValueError, 'object with unsupported type: "Klass" in nested list'
    ):
      ds([[Klass(), Klass()], [Klass()]])
    with self.assertRaisesRegex(
        ValueError,
        r'list containing multi-dim DataSlice\(s\) is not convertible to a '
        'DataSlice',
    ):
      ds([ds([1, 2])])
    with self.assertRaisesRegex(ValueError, 'cannot cast INT32 to BYTES'):
      ds(12, BYTES)
    with self.assertRaisesRegex(
        ValueError, 'schema can only be 0-rank schema slice, got: rank: 1'
    ):
      ds(12, ds([None], SCHEMA))

  def test_from_vals_errors(self):
    with self.assertRaisesRegex(
        TypeError, 'accepts 1 to 2 positional arguments'
    ):
      ds(1, 2, 3)
    with self.assertRaisesRegex(
        TypeError, 'expected DataItem for `dtype`, got: .*QType'
    ):
      ds(1, arolla.INT32)
    with self.assertRaisesRegex(
        ValueError, 'unsupported array element type: UINT64'
    ):
      ds(arolla.dense_array([1, 2], arolla.types.UINT64))
    with self.assertRaisesRegex(
        ValueError, 'unsupported array element type: UINT64'
    ):
      ds(arolla.array([1, 2], arolla.types.UINT64))
    with self.assertRaisesRegex(ValueError, 'unsupported QType: UINT64'):
      ds(arolla.types.UINT64)
    with self.assertRaisesRegex(
        ValueError, '`dtype` should not be passed.*from Arolla Array'
    ):
      ds(arolla.array([1, 2]), INT64)
    with self.assertRaisesRegex(
        ValueError, '`dtype` should not be passed.*from Arolla DenseArray'
    ):
      ds(arolla.dense_array([1, 2]), INT64)

  def test_internal_as_py_error(self):
    @arolla.optools.as_backend_operator(
        'test.invalid_unicode_op',
        qtype_inference_expr=qtypes.DATA_SLICE,
    )
    def invalid_unicode_op():
      raise NotImplementedError('provided by backend')

    with self.assertRaisesRegex(
        UnicodeDecodeError,
        r'\'utf-8\' codec can\'t decode byte 0xaa in position 0',
    ):
      _ = arolla.eval(invalid_unicode_op()).internal_as_py()
    gc.collect()

  def test_db_merging(self):
    db1 = data_bag.DataBag.empty()
    db2 = data_bag.DataBag.empty()
    i1 = db1.obj(a=42)
    i2 = db2.obj(a=24)
    s = ds([i1, i2])
    self.assertIsNotNone(s.db)
    testing.assert_equal(s.a, ds([42, 24]).with_db(s.db))


if __name__ == '__main__':
  absltest.main()
