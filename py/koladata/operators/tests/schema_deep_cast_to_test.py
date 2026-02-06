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
from absl.testing import parameterized
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE
DB = data_bag.DataBag.empty_mutable()
ENTITY = DB.new()


class SchemaDeepCastToTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(1, schema_constants.INT64), schema_constants.INT32, ds(1)),
      (ds([1], schema_constants.INT64), schema_constants.INT32, ds([1])),
      (ENTITY, ENTITY.get_schema(), ENTITY),
      (ENTITY.embed_schema(), ENTITY.get_schema(), ENTITY),
      (
          kd.list([1, 2, 3], item_schema=schema_constants.INT64),
          kd.schema.list_schema(schema_constants.FLOAT32),
          kd.list([1.0, 2.0, 3.0], item_schema=schema_constants.FLOAT32),
      ),
      (
          kd.new(x=1, y=2),
          kd.uu_schema(x=schema_constants.FLOAT32, y=schema_constants.INT64),
          kd.uu_schema(
              x=schema_constants.FLOAT32,
              y=schema_constants.INT64,
          ).new(x=1, y=2),
      ),
      (  # cast primitives
          kd.new(x=1.0, y=2.0),
          kd.uu_schema(x=schema_constants.INT32, y=schema_constants.INT64),
          kd.uu_schema(x=schema_constants.INT32, y=schema_constants.INT64).new(
              x=1, y=2
          ),
      ),
      (  # cast primitives deep and filter attributes
          kd.new(x=1.0, y=kd.new(a=1.0, b=2.0)),
          kd.uu_schema(
              x=schema_constants.INT32, y=kd.uu_schema(a=schema_constants.INT32)
          ),
          kd.uu_schema(
              x=schema_constants.INT32, y=kd.uu_schema(a=schema_constants.INT32)
          ).new(
              x=1,
              y=kd.schema.uu_schema(a=schema_constants.INT32).new(a=1),
          ),
      ),
      (  # cast primitives deep and add attributes
          kd.new(x=1.0, y=kd.new(a=1.0)),
          kd.uu_schema(
              x=schema_constants.INT32,
              y=kd.uu_schema(
                  a=schema_constants.INT32, b=schema_constants.INT32
              ),
          ),
          kd.uu_schema(
              x=schema_constants.INT32,
              y=kd.uu_schema(
                  a=schema_constants.INT32, b=schema_constants.INT32
              ),
          ).new(
              x=1,
              y=kd.schema.uu_schema(
                  a=schema_constants.INT32, b=schema_constants.INT32
              ).new(a=1),
          ),
      ),
  )
  def test_eval(self, x, schema, expected):
    res = kd.schema.deep_cast_to(
        x, schema, allow_removing_attrs=True, allow_new_attrs=True
    )
    testing.assert_equivalent(res, expected)

  def test_same_schema_id(self):
    bag1 = data_bag.DataBag.empty_mutable()
    bag2 = data_bag.DataBag.empty_mutable()
    entity = bag1.new(x=1)
    schema = entity.get_schema().with_bag(bag2)
    schema.x = schema_constants.FLOAT32
    result = kd.schema.deep_cast_to(entity, schema)
    testing.assert_equivalent(result, kd.new(x=1.0), schemas_equality=False)

  def test_object_to_entity_casting_implicit_schema_error(self):
    db = data_bag.DataBag.empty_mutable()
    obj = db.obj(x=1)
    with self.assertRaisesRegex(
        ValueError, 'DataSlice cannot have an implicit schema as its schema'
    ):
      kd.schema.deep_cast_to(obj, db.obj(x=1).get_obj_schema())

  def test_object_to_entity_casting(self):
    db = data_bag.DataBag.empty_mutable()
    obj = db.new(x=1).embed_schema()
    expected = db.new(x=1)
    result = kd.schema.deep_cast_to(obj, expected.get_schema())
    testing.assert_equivalent(result, expected)

  def test_object_to_entity_casting_incompatible_schema_error(self):
    db = data_bag.DataBag.empty_mutable()
    obj = db.new(x=1).embed_schema()
    schema = kd.schema.new_schema(y=schema_constants.INT32)
    with self.assertRaisesRegex(
        ValueError,
        r'DataSlice with schema ENTITY\(x=INT32\) with id Schema:\$[a-zA-Z0-9]*'
        r'\n\ncannot be cast to entity schema ENTITY\(y=INT32\) with id'
        r' Schema:\$[a-zA-Z0-9]*',
    ):
      _ = kd.schema.deep_cast_to(obj, schema)

  def test_object_to_entity_casting_no_common_schema_error(self):
    db = data_bag.DataBag.empty_mutable()
    entity = db.new(x=1)
    x = ds([1, entity.embed_schema()])
    with self.assertRaisesRegex(
        ValueError,
        'cannot find a common schema',
    ):
      kd.schema.deep_cast_to(x, entity.get_schema())

  def test_entity_attributes_error(self):
    db = data_bag.DataBag.empty_mutable()
    x = db.new(x='bar')
    with self.assertRaisesRegex(
        ValueError,
        "unable to parse INT32: 'bar'",
    ):
      kd.schema.deep_cast_to(x, kd.schema.new_schema(x=schema_constants.INT32))

  def test_entity_attributes_error_no_data(self):
    db = data_bag.DataBag.empty_mutable()
    schema1 = db.new_schema(
        a=schema_constants.BOOLEAN, b=schema_constants.INT32
    )
    schema2 = db.new_schema(a=schema_constants.MASK, b=schema_constants.INT32)
    x = schema1.new(b=1)
    with self.assertRaisesRegex(
        ValueError,
        r'kd.schema.deep_cast_to: DataSlice with schema ENTITY\(a=BOOLEAN,'
        r' b=INT32\) with id Schema:\$[a-zA-Z0-9]*\n\ncannot be cast to entity'
        r' schema ENTITY\(a=MASK, b=INT32\) with id Schema:\$[a-zA-Z0-9]*',
    ):
      kd.schema.deep_cast_to(x, schema2)

  def test_casting_object_to_named_schema(self):
    db = data_bag.DataBag.empty_mutable()
    obj = db.new(x=1).embed_schema()
    schema = db.named_schema('foo', x=schema_constants.FLOAT32)
    result = kd.schema.deep_cast_to(obj, schema)
    testing.assert_equivalent(result, schema.new(x=1.), schemas_equality=True)

  def test_casting_to_named_schemas(self):
    db = data_bag.DataBag.empty_mutable()
    e1 = db.new(x=1, y=2)
    schema = db.named_schema('foo', x=schema_constants.FLOAT32)
    result = kd.schema.deep_cast_to(e1, schema, allow_removing_attrs=True)
    testing.assert_equivalent(result, schema.new(x=1.), schemas_equality=True)

  def test_casting_named_schemas_remove_attr(self):
    db = data_bag.DataBag.empty_mutable()
    schema1 = db.named_schema(
        'bar', x=schema_constants.INT32, y=schema_constants.INT32
    )
    e1 = schema1.new(x=1, y=2)
    schema = db.named_schema('foo', x=schema_constants.FLOAT32)
    result = kd.schema.deep_cast_to(e1, schema, allow_removing_attrs=True)
    testing.assert_equivalent(result, schema.new(x=1.0), schemas_equality=True)

  def test_casting_named_schemas_new_attr(self):
    db = data_bag.DataBag.empty_mutable()
    schema1 = db.named_schema(
        'bar', x=schema_constants.INT32
    )
    e1 = schema1.new(x=1)
    schema = db.named_schema(
        'foo', x=schema_constants.FLOAT32, y=schema_constants.FLOAT32
    )
    result = kd.schema.deep_cast_to(e1, schema, allow_new_attrs=True)
    testing.assert_equivalent(result, schema.new(x=1.0), schemas_equality=True)

  def test_casting_named_schemas_same_name(self):
    db = data_bag.DataBag.empty_mutable()
    schema1 = db.named_schema(
        'bar', x=schema_constants.INT32, y=schema_constants.INT32
    )
    db2 = data_bag.DataBag.empty_mutable()
    schema2 = db2.named_schema(
        'bar', x=schema_constants.FLOAT32
    )
    e1 = schema1.new(x=1, y=2)
    result = kd.schema.deep_cast_to(e1, schema2, allow_removing_attrs=True)
    testing.assert_equivalent(result, schema2.new(x=1.0), schemas_equality=True)

  def test_casting_to_schema_with_metadata(self):
    db = data_bag.DataBag.empty_mutable()
    schema = db.named_schema('foo', x=schema_constants.FLOAT32)
    e1 = kd.extract(schema.new(x=1, y=2))
    schema = kd.with_metadata(schema.freeze_bag(), source='test')
    result = kd.schema.deep_cast_to(e1, schema, allow_removing_attrs=True)
    testing.assert_equivalent(result, schema.new(x=1.))
    testing.assert_equivalent(
        kd.get_metadata(result.get_schema()), kd.get_metadata(schema)
    )

  def test_casting_to_schema_with_different_metadata(self):
    db = data_bag.DataBag.empty_mutable()
    schema = db.new_schema(x=schema_constants.INT32, y=schema_constants.INT32)
    schema1 = kd.with_metadata(schema.freeze_bag(), foo=schema_constants.ITEMID)
    e1 = schema1.new(x=1, y=2)
    schema2 = kd.with_metadata(
        schema.freeze_bag(), source='test', foo=schema_constants.FLOAT64
    )
    schema2 = kd.with_attr(schema2.freeze_bag(), 'x', schema_constants.FLOAT32)
    result = kd.schema.deep_cast_to(e1, schema2)
    testing.assert_equivalent(
        result, schema2.new(x=1.0, y=2), schemas_equality=True
    )
    testing.assert_equivalent(
        kd.get_metadata(result.get_schema()), kd.get_metadata(schema2)
    )

  @parameterized.parameters(*itertools.product([True, False], repeat=3))
  def test_entity_to_object_casting(self, freeze, fork, fallback):
    db = data_bag.DataBag.empty_mutable()
    e1 = db.new(x=1)
    if fork:
      e1 = e1.fork_bag()
    if fallback:
      e1 = e1.with_bag(data_bag.DataBag.empty()).enriched(e1.get_bag())
    if freeze:
      e1 = e1.freeze_bag()
    res = kd.schema.deep_cast_to(e1, schema_constants.OBJECT)
    testing.assert_equivalent(res, kd.obj(x=1))
    testing.assert_equivalent(res.get_obj_schema(), e1.get_schema())

  def test_not_schema_error(self):
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      kd.schema.deep_cast_to(ds(1), ds(1))

  def test_casting_raises_on_new_attrs(self):
    db = data_bag.DataBag.empty_mutable()
    schema1 = db.named_schema(
        'bar', x=schema_constants.INT32
    )
    e1 = schema1.new(x=1)
    schema = db.named_schema(
        'foo', x=schema_constants.FLOAT32, y=schema_constants.FLOAT32
    )
    with self.assertRaisesRegex(
        ValueError,
        r'DataSlice with schema bar\(x=INT32\) with id Schema:\#[a-zA-Z0-9]*'
        r'\n\ncannot be cast to entity schema ENTITY\(x=FLOAT32, y=FLOAT32\)'
        r' with id Schema:\$[a-zA-Z0-9]*',
    ):
      _ = kd.schema.deep_cast_to(e1, schema)

  def test_casting_raises_on_removing_attrs(self):
    db = data_bag.DataBag.empty_mutable()
    schema1 = db.named_schema(
        'bar', x=schema_constants.INT32, y=schema_constants.INT32
    )
    e1 = schema1.new(x=1, y=2)
    schema = db.named_schema(
        'foo', x=schema_constants.FLOAT32
    )
    with self.assertRaisesRegex(
        ValueError,
        r'DataSlice with schema bar\(x=INT32, y=INT32\) with id'
        r' Schema:\#[a-zA-Z0-9]*\n\ncannot be cast to entity schema '
        r'ENTITY\(x=FLOAT32\) with id Schema:\$[a-zA-Z0-9]*;\n\n'
        r'deleted:\nold_schema.y:\nDataItem\(INT32, schema: SCHEMA\)',
    ):
      _ = kd.schema.deep_cast_to(e1, schema)

  def test_unsupported_schema_deep_error(self):
    db = data_bag.DataBag.empty_mutable()
    e1 = db.new(x=1)
    e2_schema = kd.schema.new_schema(
        x=schema_constants.INT32, y=schema_constants.INT32
    )
    with self.assertRaisesRegex(
        ValueError,
        r'kd.schema.deep_cast_to: DataSlice with schema ENTITY\(x=INT32\) with'
        r' id Schema:\$[a-zA-Z0-9]*\n\ncannot be cast to entity schema '
        r'ENTITY\(x=INT32, y=INT32\) with id Schema:\$[a-zA-Z0-9]*;\n\n'
        r'added:\nnew_schema.y:\nDataItem\(INT32, schema: SCHEMA\)'
    ):
      _ = kd.schema.deep_cast_to(e1, e2_schema)

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.deep_cast_to(I.x, I.y)))

  def test_repr(self):
    self.assertEqual(
        repr(kde.schema.deep_cast_to(I.x, I.schema)),
        'kd.schema.deep_cast_to(I.x, I.schema, DataItem(False, schema:'
        ' BOOLEAN), DataItem(False, schema: BOOLEAN))',
    )


if __name__ == '__main__':
  absltest.main()
