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
  )
  def test_eval(self, x, schema, expected):
    res = kd.schema.cast_to(x, schema)
    testing.assert_equal(res, expected)

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

  def test_casting_conflict(self):
    bag1 = data_bag.DataBag.empty_mutable()
    bag2 = data_bag.DataBag.empty_mutable()
    entity = bag1.new(x=1)
    schema = entity.get_schema().with_bag(bag2)
    schema.x = schema_constants.FLOAT32
    result = kd.schema.cast_to(entity, schema)
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex(
            'FLOAT32 schema can only be assigned to a DataSlice that contains'
            ' only primitives of FLOAT32'
        ),
    ):
      _ = result.x

  @parameterized.parameters(*itertools.product([True, False], repeat=3))
  def test_entity_to_object_casting(self, freeze, fork, fallback):
    db = data_bag.DataBag.empty_mutable()
    e1 = db.new(x=1)
    if fork:
      e1 = e1.fork_bag()
    if fallback:
      e1 = e1.with_bag(data_bag.DataBag.empty_mutable()).enriched(e1.get_bag())
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
      kd.schema.cast_to(obj, obj.get_obj_schema())

  def test_object_to_entity_casting_incompatible_schema_error(self):
    db = data_bag.DataBag.empty_mutable()
    obj = db.new(x=1).embed_schema()
    with self.assertRaisesRegex(
        ValueError,
        r'casting from OBJECT with common __schema__ ENTITY\(x=INT32\) with id'
        r' Schema:.*to entity schema ENTITY\(x=INT32\) with id Schema:.*is'
        r' currently not supported',
    ):
      kd.schema.cast_to(obj, db.new(x=1).get_schema())

  def test_object_to_entity_casting_no_common_schema_error(self):
    db = data_bag.DataBag.empty_mutable()
    entity = db.new(x=1)
    x = ds([1, entity.embed_schema()])
    with self.assertRaisesRegex(
        ValueError,
        '(?s)cannot find a common schema.*INT32.*ENTITY.*when validating'
        ' equivalence of existing __schema__',
    ):
      kd.schema.cast_to(x, entity.get_schema())

  def test_not_schema_error(self):
    with self.assertRaisesRegex(
        ValueError, 'schema must be SCHEMA, got: INT32'
    ):
      kd.schema.cast_to(ds(1), ds(1))

  def test_unsupported_schema_error(self):
    db = data_bag.DataBag.empty_mutable()
    e1 = db.new(x=1)
    e2_schema = db.new(x=1).get_schema()
    with self.assertRaisesRegex(
        ValueError,
        r'casting from ENTITY\(x=INT32\) with id Schema:.*to entity schema'
        r' ENTITY\(x=INT32\) with id Schema:.*is currently not supported',
    ):
      kd.schema.cast_to(e1, e2_schema)

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
