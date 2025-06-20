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

"""Tests for kde.schema.to_schema.

Extensive testing is done in C++.
"""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import py_expr_eval_py_ext
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import qtypes
from koladata.types import schema_constants


eval_op = py_expr_eval_py_ext.eval_op
I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
ENTITY_SCHEMA = data_bag.DataBag.empty().new().get_schema()


class SchemaToSchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(None, schema_constants.OBJECT), ds(None, schema_constants.SCHEMA)),
      (schema_constants.INT32, schema_constants.INT32),
      (ENTITY_SCHEMA, ENTITY_SCHEMA),
      (
          ds(schema_constants.INT32, schema_constants.OBJECT),
          schema_constants.INT32,
      ),
      (
          ds([None], schema_constants.OBJECT),
          ds([None], schema_constants.SCHEMA),
      ),
      (ds([schema_constants.INT32]), ds([schema_constants.INT32])),
      (ds([ENTITY_SCHEMA]), ds([ENTITY_SCHEMA])),
      (
          ds([schema_constants.INT32, ENTITY_SCHEMA], schema_constants.OBJECT),
          ds([schema_constants.INT32, ENTITY_SCHEMA]),
      ),
  )
  def test_eval(self, x, expected):
    res = eval_op("kd.schema.to_schema", x)
    testing.assert_equal(res, expected)

  def test_not_schema_object_id(self):
    e = data_bag.DataBag.empty().new()
    with self.assertRaisesRegex(
        ValueError, "casting item.*to SCHEMA is not supported"
    ):
      expr_eval.eval(kde.schema.to_schema(e))

  @parameterized.parameters(
      ds(None, schema_constants.STRING), ds("a"), ds(arolla.present())
  )
  def test_not_castable_error(self, value):
    with self.assertRaisesRegex(
        ValueError,
        f"casting a DataSlice with schema {value.get_schema()} to SCHEMA is"
        " not supported",
    ):
      expr_eval.eval(kde.schema.to_schema(value))

  def test_not_castable_internal_value(self):
    x = ds("a", schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError,
        "casting data of type STRING to SCHEMA is not supported",
    ):
      expr_eval.eval(kde.schema.to_schema(x))

  def test_boxing(self):
    testing.assert_equal(
        kde.schema.to_schema(schema_constants.INT32),
        arolla.abc.bind_op(
            kde.schema.to_schema,
            literal_operator.literal(schema_constants.INT32),
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.to_schema,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.to_schema(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.schema.to_schema, kde.to_schema))


if __name__ == "__main__":
  absltest.main()
