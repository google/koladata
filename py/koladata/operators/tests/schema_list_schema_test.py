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
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
M = arolla.M
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
kde = kde_operators.kde


class SchemaListSchemaTest(parameterized.TestCase):

  def test_get_attr(self):
    schema = expr_eval.eval(kde.schema.list_schema(schema_constants.INT32))
    testing.assert_equal(
        schema.get_attr('__items__'), schema_constants.INT32.with_db(schema.db)
    )
    self.assertFalse(schema.is_mutable())

  def test_db_adoption(self):
    schema = expr_eval.eval(
        kde.schema.list_schema(
            kde.schema.new_schema(
                a=schema_constants.INT32, b=schema_constants.TEXT
            ),
        )
    )
    testing.assert_equal(
        schema.get_attr('__items__').a,
        schema_constants.INT32.with_db(schema.db),
    )

  def test_invalid_arguments(self):
    with self.assertRaisesRegex(
        ValueError,
        "schema's schema must be SCHEMA, got: INT32",
    ):
      _ = expr_eval.eval(kde.schema.list_schema(ds(1)))

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.schema.list_schema(I.x)))

  def test_qtype_signature(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.list_schema,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.schema.list_schema, kde.list_schema)
    )


if __name__ == '__main__':
  absltest.main()
