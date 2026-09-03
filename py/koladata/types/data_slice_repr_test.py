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
from koladata.expr import input_container
from koladata.operators import kde_operators as _
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import dict_item as _
from koladata.types import list_item as _
from koladata.types import mask_constants
from koladata.types import schema_constants


I = input_container.InputContainer('I')
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals


class DataSliceReprTest(parameterized.TestCase):

  @parameterized.named_parameters(
      (
          'data_item',
          ds(12),
          'DataItem(12, schema: INT32)',
      ),
      (
          'int32',
          ds([1, 2]),
          (
              'DataSlice([1, 2], schema: INT32, present: 2/2, shape:'
              ' JaggedShape(2))'
          ),
      ),
      (
          'int64',
          ds([1, 2], schema_constants.INT64),
          (
              'DataSlice([1, 2], schema: INT64, present: 2/2, shape:'
              ' JaggedShape(2))'
          ),
      ),
      (
          'int64_as_object',
          bag().obj(ds([1, 2], schema_constants.INT64)).no_bag(),
          (
              'DataSlice([int64{1}, int64{2}], schema: OBJECT, present: 2/2,'
              ' shape: JaggedShape(2))'
          ),
      ),
      (
          'float32',
          ds([1.0, 1.5]),
          (
              'DataSlice([1.0, 1.5], schema: FLOAT32, present: 2/2, shape:'
              ' JaggedShape(2))'
          ),
      ),
      (
          'float64',
          ds([1.0, 1.5], schema_constants.FLOAT64),
          (
              'DataSlice([1.0, 1.5], schema: FLOAT64, present: 2/2, shape:'
              ' JaggedShape(2))'
          ),
      ),
      (
          'float32_as_object',
          bag().obj(ds([1.0, 1.5], schema_constants.FLOAT32)).no_bag(),
          (
              'DataSlice([1.0, 1.5], schema: OBJECT, present: 2/2, shape:'
              ' JaggedShape(2))'
          ),
      ),
      (
          'float64_as_object',
          bag().obj(ds([1.0, 1.5], schema_constants.FLOAT64)).no_bag(),
          (
              'DataSlice([float64{1.0}, float64{1.5}], schema: OBJECT, present:'
              ' 2/2, shape: JaggedShape(2))'
          ),
      ),
      (
          'boolean',
          ds([True, False]),
          (
              'DataSlice([True, False], schema: BOOLEAN, present: 2/2, shape:'
              ' JaggedShape(2))'
          ),
      ),
      (
          'missing mask DataItem',
          mask_constants.missing,
          'DataItem(missing, schema: MASK)',
      ),
      (
          'present mask DataItem',
          mask_constants.present,
          'DataItem(present, schema: MASK)',
      ),
      (
          'mask DataSlice',
          ds([mask_constants.present, mask_constants.missing]),
          (
              'DataSlice([present, missing], schema: MASK, present: 1/2, shape:'
              ' JaggedShape(2))'
          ),
      ),
      (
          'mask DataSlice with OBJECT schema',
          ds([mask_constants.present, mask_constants.missing]).with_schema(
              schema_constants.OBJECT
          ),
          (
              'DataSlice([present, None], schema: OBJECT, present: 1/2, shape:'
              ' JaggedShape(2))'
          ),
      ),
      (
          'text',
          ds('a'),
          "DataItem('a', schema: STRING)",
      ),
      (
          'text list',
          ds(['a', 'b']),
          (
              "DataSlice(['a', 'b'], schema: STRING, present: 2/2, shape:"
              ' JaggedShape(2))'
          ),
      ),
      (
          'bytes',
          ds([b'a', b'b']),
          (
              "DataSlice([b'a', b'b'], schema: BYTES, present: 2/2, shape:"
              ' JaggedShape(2))'
          ),
      ),
      (
          'int32_with_object',
          ds([1, 2]).with_schema(schema_constants.OBJECT),
          (
              'DataSlice([1, 2], schema: OBJECT, present: 2/2, shape:'
              ' JaggedShape(2))'
          ),
      ),
      (
          'mixed_data',
          ds([1, 'abc', True, 1.0, arolla.int64(1), arolla.float64(1.0)]),
          (
              "DataSlice([1, 'abc', True, 1.0, int64{1}, float64{1.0}], schema:"
              ' OBJECT, present: 6/6, shape: JaggedShape(6))'
          ),
      ),
      (
          'int32_with_none',
          ds([1, None]),
          (
              'DataSlice([1, None], schema: INT32, present: 1/2, shape:'
              ' JaggedShape(2))'
          ),
      ),
      (
          'empty',
          ds([], schema_constants.INT64),
          'DataSlice([], schema: INT64, present: 0/0, shape: JaggedShape(0))',
      ),
      (
          'empty_int64_internal',
          ds(arolla.dense_array_int64([])),
          'DataSlice([], schema: INT64, present: 0/0, shape: JaggedShape(0))',
      ),
      (
          'multidim',
          ds([[[1], [2]], [[3], [4], [5]]]),
          (
              'DataSlice([[[1], [2]], [[3], [4], [5]]], schema: INT32, present:'
              ' 5/5, shape: JaggedShape(2, [2, 3], 1))'
          ),
      ),
  )
  def test_debug_repr(self, x, expected_repr):
    self.assertEqual(x._debug_repr(), expected_repr)

  def test_debug_large_repr(self):
    db = bag()
    x = db.new(x=ds([[x for x in range(5)] for y in range(4)]))
    self.assertRegex(
        x._debug_repr(),
        r"""DataSlice\(\[
  \[
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
  \],
  \[
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
  \],
  \[
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
  \],
  \[
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
  \],
\], schema: ENTITY\(x=INT32\), present: 20/20, shape: JaggedShape\(4, 5\), bag_id: \$[0-9a-f]{4}\)""",
    )
    o = db.obj(x=ds([[x for x in range(5)] for y in range(4)]))
    self.assertRegex(
        o._debug_repr(),
        r'''DataSlice\(\[
  \[
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
  \],
  \[
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
  \],
  \[
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
  \],
  \[
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
  \],
\], schema: OBJECT, present: 20/20, shape: JaggedShape\(4, 5\), bag_id: \$[0-9a-f]{4}\)''',
    )

  def test_debug_repr_with_large_expr_quote(self):
    expr_slice = ds(arolla.quote(sum([I.x] * 4000, start=I.x)))
    self.assertEqual(
        expr_slice._debug_repr(),
        'DataItem('
        + ' + '.join(['I.x' for _ in range(4001)])
        + ', schema: EXPR)',
    )

  def test_debug_repr_with_bag(self):
    db = bag()
    x = ds([1, 2]).with_bag(db)
    bag_id = '$' + str(db.fingerprint)[-4:]
    self.assertEqual(
        x._debug_repr(),
        'DataSlice([1, 2], schema: INT32, present: 2/2, shape:'
        f' JaggedShape(2), bag_id: {bag_id})',
    )

  @parameterized.named_parameters(
      (
          'data_item',
          ds(12),
          'DataItem(12, schema: INT32)',
          '12',
      ),
      (
          'int32',
          ds([1, 2]),
          'DataSlice([1, 2], schema: INT32, present: 2/2)',
          '[1, 2]',
      ),
      (
          'int64',
          ds([1, 2], schema_constants.INT64),
          'DataSlice([1, 2], schema: INT64, present: 2/2)',
          '[1, 2]',
      ),
      (
          'int64_as_object',
          bag().obj(ds([1, 2], schema_constants.INT64)).no_bag(),
          'DataSlice([int64{1}, int64{2}], schema: OBJECT, present: 2/2)',
          '[int64{1}, int64{2}]',
      ),
      (
          'float32',
          ds([1.0, 1.5]),
          'DataSlice([1.0, 1.5], schema: FLOAT32, present: 2/2)',
          '[1.0, 1.5]',
      ),
      (
          'float64',
          ds([1.0, 1.5], schema_constants.FLOAT64),
          'DataSlice([1.0, 1.5], schema: FLOAT64, present: 2/2)',
          '[1.0, 1.5]',
      ),
      (
          'float32_as_object',
          bag().obj(ds([1.0, 1.5], schema_constants.FLOAT32)).no_bag(),
          'DataSlice([1.0, 1.5], schema: OBJECT, present: 2/2)',
          '[1.0, 1.5]',
      ),
      (
          'float64_as_object',
          bag().obj(ds([1.0, 1.5], schema_constants.FLOAT64)).no_bag(),
          (
              'DataSlice([float64{1.0}, float64{1.5}], schema: OBJECT, present:'
              ' 2/2)'
          ),
          '[float64{1.0}, float64{1.5}]',
      ),
      (
          'boolean',
          ds([True, False]),
          'DataSlice([True, False], schema: BOOLEAN, present: 2/2)',
          '[True, False]',
      ),
      (
          'missing mask DataItem',
          mask_constants.missing,
          'DataItem(missing, schema: MASK)',
          'missing',
      ),
      (
          'present mask DataItem',
          mask_constants.present,
          'DataItem(present, schema: MASK)',
          'present',
      ),
      (
          'mask DataSlice',
          ds([mask_constants.present, mask_constants.missing]),
          'DataSlice([present, missing], schema: MASK, present: 1/2)',
          '[present, missing]',
      ),
      (
          'mask DataSlice with OBJECT schema',
          ds([mask_constants.present, mask_constants.missing]).with_schema(
              schema_constants.OBJECT
          ),
          'DataSlice([present, None], schema: OBJECT, present: 1/2)',
          '[present, None]',
      ),
      (
          'text',
          ds('a'),
          "DataItem('a', schema: STRING)",
          'a',
      ),
      (
          'text list',
          ds(['a', 'b']),
          "DataSlice(['a', 'b'], schema: STRING, present: 2/2)",
          "['a', 'b']",
      ),
      (
          'bytes',
          ds([b'a', b'b']),
          "DataSlice([b'a', b'b'], schema: BYTES, present: 2/2)",
          "[b'a', b'b']",
      ),
      (
          'int32_with_object',
          ds([1, 2]).with_schema(schema_constants.OBJECT),
          'DataSlice([1, 2], schema: OBJECT, present: 2/2)',
          '[1, 2]',
      ),
      (
          'mixed_data',
          ds([1, 'abc', True, 1.0, arolla.int64(1), arolla.float64(1.0)]),
          (
              "DataSlice([1, 'abc', True, 1.0, int64{1}, float64{1.0}], schema:"
              ' OBJECT, present: 6/6)'
          ),
          "[1, 'abc', True, 1.0, int64{1}, float64{1.0}]",
      ),
      (
          'int32_with_none',
          ds([1, None]),
          'DataSlice([1, None], schema: INT32, present: 1/2)',
          '[1, None]',
      ),
      (
          'empty',
          ds([], schema_constants.INT64),
          'DataSlice([], schema: INT64, present: 0/0)',
          '[]',
      ),
      (
          'empty_int64_internal',
          ds(arolla.dense_array_int64([])),
          'DataSlice([], schema: INT64, present: 0/0)',
          '[]',
      ),
      (
          'multidim',
          ds([[[1], [2]], [[3], [4], [5]]]),
          (
              'DataSlice([[[1], [2]], [[3], [4], [5]]], schema: INT32, present:'
              ' 5/5)'
          ),
          '[[[1], [2]], [[3], [4], [5]]]',
      ),
      (
          'large_string',
          ds(['a' * 1000]),
          'DataSlice([\n'
          f"  '{'a' * 256}'... (1000 chars total),\n"
          '], schema: STRING, present: 1/1)',
          f"[\n  '{'a' * 1000}',\n]",  # No truncation.
      ),
      (
          'large_bytestring',
          ds([b'a' * 1000]),
          'DataSlice([\n'
          f"  b'{'a' * 256}'... (1000 bytes total),\n"
          '], schema: BYTES, present: 1/1)',
          f"[\n  b'{'a' * 1000}',\n]",  # No truncation.
      ),
  )
  def test_repr_and_str_no_bag(self, x, expected_repr, expected_str):
    self.assertEqual(repr(x), expected_repr)
    self.assertEqual(str(x), expected_str)

  def test_repr_entity_and_obj(self):
    db = bag()
    x = db.new(x=ds([1, 2, 3]))
    bag_id = '$' + str(db.fingerprint)[-4:]
    self.assertEqual(
        repr(x),
        (
            'DataSlice([Entity(x=1), Entity(x=2), Entity(x=3)], schema:'
            f' ENTITY(x=INT32), present: 3/3, bag_id: {bag_id})'
        ),
    )
    self.assertEqual(
        str(x),
        '[Entity(x=1), Entity(x=2), Entity(x=3)]',
    )

    y = db.new(x=ds([1, 2, 3]), schema='foo')
    self.assertEqual(
        repr(y),
        (
            'DataSlice([Entity(x=1), Entity(x=2), Entity(x=3)], schema:'
            f' foo(x=INT32), present: 3/3, bag_id: {bag_id})'
        ),
    )
    self.assertEqual(
        str(y),
        '[Entity(x=1), Entity(x=2), Entity(x=3)]',
    )

    z = db.obj(x=ds([1, 2, 3]))
    self.assertEqual(
        repr(z),
        'DataSlice([Obj(x=1), Obj(x=2), Obj(x=3)], schema: OBJECT,'
        f' present: 3/3, bag_id: {bag_id})',
    )
    self.assertEqual(
        str(z),
        '[Obj(x=1), Obj(x=2), Obj(x=3)]',
    )

  def test_repr_large_entity_and_obj(self):
    db = bag()
    x = db.new(x=ds([[x for x in range(5)] for y in range(4)]))
    bag_id = '$' + str(db.fingerprint)[-4:]
    self.assertEqual(
        repr(x),
        'DataSlice(attrs: [x], schema: ENTITY(x=INT32),'
        f' present: 20/20, bag_id: {bag_id})',
    )

    y = db.obj(x=ds([[x for x in range(5)] for y in range(4)]))
    self.assertEqual(
        repr(y),
        'DataSlice(attrs: [x], schema: OBJECT, present: 20/20, bag_id:'
        f' {bag_id})',
    )

  def test_repr_large_schema(self):
    db = bag()
    attrs = {f'x{i:02d}': 1 for i in range(25)}
    x = db.new(**attrs)  # pyrefly: ignore[bad-argument-type]
    schema = x.get_schema()
    bag_id = '$' + str(db.fingerprint)[-4:]

    expected = f"""DataItem(ENTITY(
  x00=INT32,
  x01=INT32,
  x02=INT32,
  x03=INT32,
  x04=INT32,
  x05=INT32,
  x06=INT32,
  x07=INT32,
  x08=INT32,
  x09=INT32,
  x10=INT32,
  x11=INT32,
  x12=INT32,
  x13=INT32,
  x14=INT32,
  x15=INT32,
  x16=INT32,
  x17=INT32,
  x18=INT32,
  x19=INT32,
  ...,
), schema: SCHEMA, bag_id: {bag_id})"""
    self.assertEqual(repr(schema), expected)

  def test_repr_entity_with_large_schema(self):
    db = bag()
    attrs = {f'x{i:02d}': i for i in range(25)}
    x = db.new(**attrs)  # pyrefly: ignore[bad-argument-type]
    bag_id = '$' + str(db.fingerprint)[-4:]

    expected = f"""DataItem(Entity(
  x00=0,
  x01=1,
  x02=2,
  x03=3,
  x04=4,
  x05=5,
  x06=6,
  x07=7,
  x08=8,
  x09=9,
  x10=10,
  x11=11,
  x12=12,
  x13=13,
  x14=14,
  x15=15,
  x16=16,
  x17=17,
  x18=18,
  x19=19,
  ...,
), schema: ENTITY(
  x00=INT32,
  x01=INT32,
  x02=INT32,
  x03=INT32,
  x04=INT32,
  x05=INT32,
  x06=INT32,
  x07=INT32,
  x08=INT32,
  x09=INT32,
  x10=INT32,
  x11=INT32,
  x12=INT32,
  x13=INT32,
  x14=INT32,
  x15=INT32,
  x16=INT32,
  x17=INT32,
  x18=INT32,
  x19=INT32,
  ...,
), bag_id: {bag_id})"""
    self.assertEqual(repr(x), expected)

  def test_empty_slice_with_rank_repr(self):
    x = ds([])
    y = x.repeat(0)
    self.assertEqual(repr(x), 'DataSlice([], schema: NONE, present: 0/0)')
    self.assertEqual(
        repr(y),
        'DataSlice([], schema: NONE, present: 0/0, shape: JaggedShape(0, []))',
    )

  # Special case for itemid, since it includes a non-deterministic id.
  def test_str_repr_itemid_works(self):
    x = bag().list()
    self.assertRegex(str(x.get_itemid()), r'.*:.*')
    self.assertRegex(repr(x.get_itemid()), r'DataItem(.*:.*, schema: ITEMID)')


if __name__ == '__main__':
  absltest.main()
