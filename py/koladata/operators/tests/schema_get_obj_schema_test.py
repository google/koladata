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

"""Tests for kde.schema.get_obj_schema."""

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


I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE

db = data_bag.DataBag.empty()
obj = db.obj(x=1)
objs = db.obj(x=ds([1, 2]))


class SchemaGetObjSchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      # Missing DataItem
      (ds(None, schema_constants.OBJECT), ds(None, schema_constants.SCHEMA)),
      # Primitive DataItem
      (ds(1, schema_constants.OBJECT), schema_constants.INT32),
      # DataSlice with mixed primitives
      (
          ds([1, 1.2, None, "a"]),
          ds([
              schema_constants.INT32,
              schema_constants.FLOAT32,
              None,
              schema_constants.TEXT,
          ]),
      ),
      # All missing DataSlice
      (
          ds([None, None, None, None], schema_constants.OBJECT),
          ds([None, None, None, None], schema_constants.SCHEMA),
      ),
      # Object DataItem
      (obj, obj.get_attr("__schema__")),
      # Object DataSlice
      (objs, objs.get_attr("__schema__")),
      # DataSlice with mixed primitives and object
      (
          ds([1, 1.2, obj, "a"]),
          ds([
              schema_constants.INT32,
              schema_constants.FLOAT32,
              obj.get_attr("__schema__"),
              schema_constants.TEXT,
          ]),
      ),
  )
  def test_eval(self, x, expected):
    res = expr_eval.eval(kde.schema.get_obj_schema(x))
    testing.assert_equal(res, expected)

  def test_non_object_schema(self):
    with self.assertRaisesRegex(
        ValueError,
        "DataSlice must have OBJECT schema",
    ):
      expr_eval.eval(kde.schema.get_obj_schema(db.new(x=1)))

    with self.assertRaisesRegex(
        ValueError,
        "DataSlice must have OBJECT schema",
    ):
      expr_eval.eval(kde.schema.get_obj_schema(db.new(x=ds([1, 2]))))

  def test_invalid_items(self):
    with self.assertRaisesRegex(
        ValueError,
        "DataSlice cannot contain primitive schema items",
    ):
      expr_eval.eval(
          kde.schema.get_obj_schema(
              schema_constants.INT32.with_schema(schema_constants.OBJECT)
          )
      )

    with self.assertRaisesRegex(
        ValueError,
        "DataSlice cannot contain primitive schema items",
    ):
      expr_eval.eval(
          kde.schema.get_obj_schema(
              ds(
                  [schema_constants.INT32, schema_constants.FLOAT32]
              ).with_schema(schema_constants.OBJECT)
          )
      )

    with self.assertRaisesRegex(
        ValueError,
        "missing __schema__ attribute",
    ):
      expr_eval.eval(
          kde.schema.get_obj_schema(
              db.new_schema(x=schema_constants.INT32).with_schema(
                  schema_constants.OBJECT
              )
          )
      )

    with self.assertRaisesRegex(
        ValueError,
        "missing __schema__ attribute",
    ):
      expr_eval.eval(
          kde.schema.get_obj_schema(
              db.new(x=1).with_schema(schema_constants.OBJECT)
          )
      )

  def test_no_db(self):
    with self.assertRaisesRegex(
        ValueError,
        "DataSlice with Objects must have a DataBag attached",
    ):
      expr_eval.eval(kde.schema.get_obj_schema(obj.with_db(None)))

  def test_boxing(self):
    with self.assertRaisesRegex(
        ValueError,
        "expected DATA_SLICE, got x: QTYPE",
    ):
      kde.schema.get_obj_schema(arolla.INT32)

    with self.assertRaisesRegex(
        ValueError,
        "expected DATA_SLICE, got x: ARRAY_INT32",
    ):
      kde.schema.get_obj_schema([1])

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.get_obj_schema,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.schema.get_obj_schema(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.schema.get_obj_schema, kde.get_obj_schema)
    )


if __name__ == "__main__":
  absltest.main()
