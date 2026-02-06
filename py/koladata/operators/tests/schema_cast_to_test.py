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
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import schema_constants

I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE
DB = data_bag.DataBag.empty_mutable()
ENTITY = DB.new()


class SchemaCastToTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(1, schema_constants.INT64), schema_constants.INT32, ds(1)),
      (ds([1], schema_constants.INT64), schema_constants.INT32, ds([1])),
      (
          ds([1, 2.0], schema_constants.OBJECT),
          schema_constants.INT64,
          ds([1, 2], schema_constants.INT64),
      ),
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
              ).new(a=1, b=2),
          ),
      ),
  )
  def test_eval(self, x, schema, expected):
    res = kd.schema.cast_to(x, schema)
    testing.assert_equivalent(res, expected)

  def test_adoption(self):
    bag1 = data_bag.DataBag.empty_mutable()
    entity = bag1.new(x=ds([1]))
    schema = entity.get_schema().extract()
    del entity.get_schema().x

    result = kd.schema.cast_to(entity, schema)
    testing.assert_equal(result.x.no_bag(), ds([1]))
    testing.assert_equal(
        result.no_bag(), entity.no_bag().with_schema(schema.no_bag())
    )
    self.assertNotEqual(result.x.get_bag().fingerprint, bag1.fingerprint)
    self.assertNotEqual(
        result.x.get_bag().fingerprint, schema.get_bag().fingerprint
    )

  def test_same_schema_id_is_noop(self):
    bag1 = data_bag.DataBag.empty_mutable()
    bag2 = data_bag.DataBag.empty_mutable()
    entity = bag1.new(x=1)
    schema = entity.get_schema().with_bag(bag2)
    schema.x = schema_constants.FLOAT32
    result = kd.schema.cast_to(entity, schema)
    with self.assertRaisesRegex(
        ValueError,
        'kd.core.extract: during extract/clone, while processing the attribute'
        " 'x', got a slice with primitive type FLOAT32 while the actual content"
        ' has type INT32',
    ):
      _ = kd.extract(result)

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
    res = kd.schema.cast_to(e1, schema_constants.OBJECT)
    testing.assert_equal(res.get_itemid().no_bag(), e1.get_itemid().no_bag())
    testing.assert_equal(res.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        res.get_obj_schema().no_bag(), e1.get_schema().no_bag()
    )
    self.assertNotEqual(res.get_bag().fingerprint, e1.get_bag().fingerprint)
    self.assertFalse(res.get_bag().is_mutable())
    # Sanity check
    testing.assert_equal(res.x, ds(1).with_bag(res.get_bag()))

  def test_object_to_entity_casting_implicit_schema_error(self):
    db = data_bag.DataBag.empty_mutable()
    obj = db.obj(x=1)
    with self.assertRaisesRegex(
        ValueError, 'DataSlice cannot have an implicit schema as its schema'
    ):
      kd.schema.cast_to(obj, db.obj(x=1).get_obj_schema())

  def test_object_to_entity_casting(self):
    db = data_bag.DataBag.empty_mutable()
    obj = db.new(x=1).embed_schema()
    expected = db.new(x=1)
    result = kd.schema.cast_to(obj, expected.get_schema())
    testing.assert_equivalent(result, expected)

  def test_casting_object_to_named_schema(self):
    db = data_bag.DataBag.empty_mutable()
    obj = db.new(x=1).embed_schema()
    schema = db.named_schema('foo', x=schema_constants.FLOAT32)
    result = kd.schema.cast_to(obj, schema)
    testing.assert_equivalent(result, schema.new(x=1.), schemas_equality=True)

  def test_casting_to_named_schemas(self):
    db = data_bag.DataBag.empty_mutable()
    e1 = db.new(x=1)
    schema = db.named_schema('foo', x=schema_constants.FLOAT32)
    result = kd.schema.cast_to(e1, schema)
    testing.assert_equivalent(result, schema.new(x=1.), schemas_equality=True)

  def test_casting_named_schemas(self):
    db = data_bag.DataBag.empty_mutable()
    schema1 = db.named_schema(
        'bar', x=schema_constants.INT32, y=schema_constants.INT32
    )
    e1 = schema1.new(x=1, y=2)
    schema = db.named_schema(
        'foo', x=schema_constants.FLOAT32, y=schema_constants.FLOAT32
    )
    result = kd.schema.cast_to(e1, schema)
    testing.assert_equivalent(
        result, schema.new(x=1.0, y=2.0), schemas_equality=True
    )

  def test_casting_to_schema_with_metadata(self):
    db = data_bag.DataBag.empty_mutable()
    e1 = db.new(x=1, y=2)
    schema = db.named_schema(
        'foo', x=schema_constants.FLOAT32, y=schema_constants.FLOAT32
    )
    schema = kd.with_metadata(schema.freeze_bag(), source='test')
    result = kd.schema.cast_to(e1, schema)
    testing.assert_equivalent(
        result, schema.new(x=1.0, y=2.0), schemas_equality=True
    )
    testing.assert_equivalent(
        kd.get_metadata(result.get_schema()), kd.get_metadata(schema)
    )

  def test_casting_to_schema_with_different_metadata(self):
    db = data_bag.DataBag.empty_mutable()
    schema1 = db.new_schema(x=schema_constants.INT32, y=schema_constants.INT32)
    schema1 = kd.with_metadata(
        schema1.freeze_bag(), foo=schema_constants.ITEMID
    )
    e1 = schema1.new(x=1, y=2)
    schema = db.named_schema(
        'foo', x=schema_constants.FLOAT32, y=schema_constants.FLOAT32
    )
    schema = kd.with_metadata(
        schema.freeze_bag(), source='test', foo=schema_constants.FLOAT64
    )
    result = kd.schema.cast_to(e1, schema)
    testing.assert_equivalent(
        result, schema.new(x=1.0, y=2.0), schemas_equality=True
    )
    testing.assert_equivalent(
        kd.get_metadata(result.get_schema()), kd.get_metadata(schema)
    )

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
        r'\n\ncannot be cast to entity schema foo\(x=FLOAT32, y=FLOAT32\) with'
        r' id Schema:\#[a-zA-Z0-9]*;\n\nadded:\nnew_schema.y:\n'
        r'DataItem\(FLOAT32, schema: SCHEMA\)',
    ):
      _ = kd.schema.cast_to(e1, schema)

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
        r'foo\(x=FLOAT32\) with id Schema:\#[a-zA-Z0-9]*;\n\ndeleted:\n'
        r'old_schema.y:\nDataItem\(INT32, schema: SCHEMA\)',
    ):
      _ = kd.schema.cast_to(e1, schema)

  def test_object_to_entity_casting_incompatible_schema_error(self):
    db = data_bag.DataBag.empty_mutable()
    obj = db.new(x=1).embed_schema()
    schema = kd.schema.new_schema(y=schema_constants.INT32)
    with self.assertRaisesRegex(
        ValueError,
        r'DataSlice with schema ENTITY\(x=INT32\) with id Schema:\$[a-zA-Z0-9]*'
        r'\n\ncannot be cast to entity schema ENTITY\(y=INT32\) with id'
        r' Schema:\$[a-zA-Z0-9]*;',
    ):
      _ = kd.schema.cast_to(obj, schema)

  def test_entity_to_object_casting_error(self):
    db = data_bag.DataBag.empty_mutable()
    e1 = db.new(x=db.new(y=1))
    schema = kd.schema.new_schema(x=schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError,
        r'DataSlice with schema ENTITY\(x=ENTITY\(y=INT32\)\) with id '
        r'Schema:\$[a-zA-Z0-9]*\n\ncannot be cast to entity schema '
        r'ENTITY\(x=OBJECT\) with id Schema:\$[a-zA-Z0-9]*;\n\nmodified:\n'
        r'old_schema.x:\nDataItem\(ENTITY\(y=INT32\), schema: SCHEMA\)\n'
        r'-> new_schema.x:\nDataItem\(OBJECT, schema: SCHEMA\)',
    ):
      _ = kd.schema.cast_to(e1, schema)

  def test_object_to_entity_casting_no_common_schema_error(self):
    db = data_bag.DataBag.empty_mutable()
    entity = db.new(x=1)
    x = ds([1, entity.embed_schema()])
    with self.assertRaisesRegex(
        ValueError,
        'cannot find a common schema',
    ):
      kd.schema.cast_to(x, entity.get_schema())

  def test_entity_attributes_error(self):
    db = data_bag.DataBag.empty_mutable()
    x = db.new(x='bar')
    with self.assertRaisesRegex(
        ValueError,
        "unable to parse INT32: 'bar'",
    ):
      kd.schema.cast_to(x, kd.schema.new_schema(x=schema_constants.INT32))

  def test_not_schema_error(self):
    with self.assertRaisesRegex(
        ValueError, 'schema must be SCHEMA, got: INT32'
    ):
      kd.schema.cast_to(ds(1), ds(1))

  def test_unsupported_schema_deep_error(self):
    db = data_bag.DataBag.empty_mutable()
    e1 = db.new(x=1)
    e2_schema = kd.schema.new_schema(
        x=schema_constants.INT32, y=schema_constants.INT32
    )
    with self.assertRaisesRegex(
        ValueError,
        r'kd.schema.cast_to: DataSlice with schema ENTITY\(x=INT32\) with id '
        r'Schema:\$[a-zA-Z0-9]*\n\ncannot be cast to entity schema '
        r'ENTITY\(x=INT32, y=INT32\) with id Schema:\$[a-zA-Z0-9]*;'
        r'\n\nadded:\nnew_schema.y:\nDataItem\(INT32, schema: SCHEMA\)'
    ):
      _ = kd.schema.cast_to(e1, e2_schema)

  def test_unsupported_schema_error(self):
    e1 = kd.slice([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        'kd.schema.cast_to: casting a DataSlice with schema INT32 to EXPR is'
        ' not supported',
    ):
      _ = kd.schema.cast_to(e1, schema_constants.EXPR)

  def test_multidim_schema_error(self):
    with self.assertRaisesRegex(
        ValueError, 'schema can only be 0-rank schema slice'
    ):
      kd.schema.cast_to(
          ds(1), ds([schema_constants.INT32, schema_constants.INT64])
      )

  def test_boxing(self):
    testing.assert_equal(
        kde.schema.cast_to(1, schema_constants.INT64),
        arolla.abc.bind_op(
            kde.schema.cast_to,
            literal_operator.literal(ds(1)),
            literal_operator.literal(schema_constants.INT64),
        ),
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.schema.cast_to,
        [(DATA_SLICE, DATA_SLICE, DATA_SLICE)],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.cast_to(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.schema.cast_to, kde.cast_to))


if __name__ == '__main__':
  absltest.main()
