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

"""Tests for kde.core.get_primitive_schema."""

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


class CoreGetPrimitiveSchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(1), schema_constants.INT32),
      (ds(1, dtype=schema_constants.INT64), schema_constants.INT64),
      (ds(1.0), schema_constants.FLOAT32),
      (
          ds(1.0, dtype=schema_constants.FLOAT64),
          schema_constants.FLOAT64,
      ),
      (ds('1'), schema_constants.TEXT),
      (ds(True), schema_constants.BOOLEAN),
      (ds(None, schema_constants.INT32), schema_constants.INT32),
      (ds([1, 2, 3]), schema_constants.INT32),
      (ds([1, 2, 3], dtype=schema_constants.INT64), schema_constants.INT64),
      (ds([1.0, 2.0, 3.0]), schema_constants.FLOAT32),
      (
          ds([1.0, 2.0, 3.0], dtype=schema_constants.FLOAT64),
          schema_constants.FLOAT64,
      ),
      (ds(['1', '2', '3']), schema_constants.TEXT),
      (ds([True, False, True]), schema_constants.BOOLEAN),
      (ds([1, 2, 3], schema_constants.OBJECT), schema_constants.INT32),
      (ds([1, 2, 3]).as_any(), schema_constants.INT32),
      (ds([None, None, None], schema_constants.INT32), schema_constants.INT32),
  )
  def test_eval(self, x, expected):
    result = expr_eval.eval(kde.core.get_primitive_schema(x))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      (ds(None),),
      (bag().list(),),
      (bag().obj(),),
      (schema_constants.INT32,),
      (ds([schema_constants.INT32, schema_constants.INT64]),),
  )
  def test_eval_with_exception(self, x):
    with self.assertRaisesRegex(
        ValueError,
        'the primitive schema of the DataSlice cannot be inferred - it is empty'
        ' with no primitive schema, has non-primitive items, or it has items of'
        ' mixed primitive dtypes',
    ):
      _ = expr_eval.eval(kde.core.get_primitive_schema(x))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.get_primitive_schema,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_data_slice_view(kde.core.get_primitive_schema(I.ds))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.core.get_primitive_schema, kde.get_primitive_schema
        )
    )


if __name__ == '__main__':
  absltest.main()
