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

"""Tests for kde.schema.get_schema."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
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

db = data_bag.DataBag.empty()
entity_s = db.new_schema(x=schema_constants.INT32)
entity = db.new(x=1, schema=entity_s)
obj = db.obj(x=1)


class SchemaGetSchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(None), ds(schema_constants.NONE)),
      (ds(None, schema_constants.FLOAT64), ds(schema_constants.FLOAT64)),
      (ds(1), ds(schema_constants.INT32)),
      (entity, entity_s),
      (obj, schema_constants.OBJECT.with_bag(db)),
      (
          entity.with_schema(schema_constants.ITEMID),
          schema_constants.ITEMID.with_bag(db),
      ),
      (schema_constants.INT32, schema_constants.SCHEMA),
  )
  def test_eval(self, x, expected):
    res = eval_op("kd.schema.get_schema", x)
    testing.assert_equal(res, expected)

  def test_boxing(self):
    testing.assert_equal(
        kde.schema.get_schema(1),
        arolla.abc.bind_op(
            kde.schema.get_schema, literal_operator.literal(ds(1))
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.get_schema,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.get_schema(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.schema.get_schema, kde.get_schema))


if __name__ == "__main__":
  absltest.main()
