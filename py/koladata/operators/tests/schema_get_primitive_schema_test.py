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
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
])


class SchemaGetPrimitiveSchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(1), schema_constants.INT32),
      (ds(1, schema=schema_constants.INT64), schema_constants.INT64),
      (ds(1.0), schema_constants.FLOAT32),
      (
          ds(1.0, schema=schema_constants.FLOAT64),
          schema_constants.FLOAT64,
      ),
      (ds('1'), schema_constants.STRING),
      (ds(True), schema_constants.BOOLEAN),
      (ds(None, schema_constants.INT32), schema_constants.INT32),
      (ds([1, 2, 3]), schema_constants.INT32),
      (ds([1, 2, 3], schema=schema_constants.INT64), schema_constants.INT64),
      (ds([1.0, 2.0, 3.0]), schema_constants.FLOAT32),
      (
          ds([1.0, 2.0, 3.0], schema=schema_constants.FLOAT64),
          schema_constants.FLOAT64,
      ),
      (ds(['1', '2', '3']), schema_constants.STRING),
      (ds([True, False, True]), schema_constants.BOOLEAN),
      (ds([1, 2, 3], schema_constants.OBJECT), schema_constants.INT32),
      (ds([None, None, None], schema_constants.INT32), schema_constants.INT32),
  )
  def test_eval(self, x, expected):
    result = expr_eval.eval(kde.schema.get_primitive_schema(x))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      (ds(None),),
      (bag().list(),),
      (bag().obj(),),
      (schema_constants.INT32,),
      (ds([schema_constants.INT32, schema_constants.INT64]),),
  )
  def test_eval_with_missing_schema(self, x):
    result = expr_eval.eval(kde.schema.get_primitive_schema(x))
    missing_schema = schema_constants.INT32 & None
    testing.assert_equal(result, missing_schema)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.get_primitive_schema,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.get_primitive_schema(I.ds)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.schema.get_primitive_schema, kde.get_primitive_schema
        )
    )
    self.assertTrue(
        optools.equiv_to_op(
            kde.schema.get_primitive_schema, kde.schema.get_dtype
        )
    )
    self.assertTrue(
        optools.equiv_to_op(kde.schema.get_primitive_schema, kde.get_dtype)
    )


if __name__ == '__main__':
  absltest.main()
