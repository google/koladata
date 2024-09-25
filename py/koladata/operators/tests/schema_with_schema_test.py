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

"""Tests for kde.schema.with_schema."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE

db = data_bag.DataBag.empty()
obj1 = db.obj()
obj2 = db.obj()
entity1 = db.new()
entity2 = db.new()
s1 = entity1.get_schema()
s2 = entity2.get_schema()


class SchemaWithSchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      # Scalar primitive schema
      (ds(None), schema_constants.INT32, ds(None, schema_constants.INT32)),
      (ds(1), schema_constants.ANY, ds(1, schema_constants.ANY)),
      (ds(1), schema_constants.OBJECT, ds(1, schema_constants.OBJECT)),
      (ds(1, schema_constants.ANY), schema_constants.INT32, ds(1)),
      (ds(1, schema_constants.OBJECT), schema_constants.INT32, ds(1)),
      # 1D primitive schema
      (
          ds([None, None]),
          schema_constants.INT32,
          ds([None, None], schema_constants.INT32),
      ),
      (ds([1, 2]), schema_constants.ANY, ds([1, 2], schema_constants.ANY)),
      (
          ds([1, 2]),
          schema_constants.OBJECT,
          ds([1, 2], schema_constants.OBJECT),
      ),
      (ds([1, 2], schema_constants.ANY), schema_constants.INT32, ds([1, 2])),
      (ds([1, 2], schema_constants.OBJECT), schema_constants.INT32, ds([1, 2])),
      # mixed
      (
          ds([1, '2'], schema_constants.OBJECT),
          schema_constants.ANY,
          ds([1, '2'], schema_constants.ANY),
      ),
  )
  def test_primitives(self, x, schema, expected):
    res = expr_eval.eval(kde.schema.with_schema(x, schema))
    testing.assert_equal(res, expected)

  @parameterized.parameters(
      # Entity schema -> the same Entity schema
      (entity1, s1),
      # Entity schema -> different entity schema
      (entity1, s2),
      # Entity schema -> OBJECT/ANY
      (entity1, schema_constants.OBJECT),
      (entity1, schema_constants.ANY),
      # OBJECT -> Entity schema
      (ds([obj1, obj2]), s2),
      # OBJECT -> ANY
      (ds([obj1, obj2]), schema_constants.ANY),
      # mixed OBJECT -> ANY
      (ds([obj1, 2]), schema_constants.ANY),
  )
  def test_entities_and_objects(self, x, schema):
    res = expr_eval.eval(kde.schema.with_schema(x, schema))
    testing.assert_equal(res.get_schema().no_db(), schema.no_db())

  def test_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        'INT64 schema can only be assigned to a DataSlice that contains only'
        ' primitives of INT64',
    ):
      expr_eval.eval(kde.schema.with_schema(1, schema_constants.INT64))

    with self.assertRaisesRegex(
        ValueError,
        'DataSlice with an Entity schema must hold Entities or Objects',
    ):
      expr_eval.eval(kde.schema.with_schema(ds(1).with_db(s1.db), s1))

    with self.assertRaisesRegex(
        ValueError,
        'INT64 schema can only be assigned to a DataSlice that contains only'
        ' primitives of INT64',
    ):
      expr_eval.eval(kde.schema.with_schema(entity1, schema_constants.INT64))

    with self.assertRaisesRegex(
        ValueError,
        'with_schema does not accept schemas with different DataBag attached',
    ):
      expr_eval.eval(kde.schema.with_schema(data_bag.DataBag.empty().new(), s1))

    with self.assertRaisesRegex(
        ValueError,
        'INT64 schema can only be assigned to a DataSlice that contains only'
        ' primitives of INT64',
    ):
      expr_eval.eval(kde.schema.with_schema(obj1, schema_constants.INT64))

    with self.assertRaisesRegex(
        ValueError,
        'INT32 schema can only be assigned to a DataSlice that contains only'
        ' primitives of INT32',
    ):
      expr_eval.eval(
          kde.schema.with_schema(ds([1, '2']), schema_constants.INT32)
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.with_schema,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(
        view.has_data_slice_view(kde.schema.with_schema(I.x, I.schema))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.schema.with_schema, kde.with_schema)
    )


if __name__ == '__main__':
  absltest.main()
