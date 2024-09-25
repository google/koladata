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

"""Tests for kde.schema.get_value_schema."""

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
dict_s1 = db.dict_schema(schema_constants.TEXT, schema_constants.INT32)
dict_s2 = db.dict_schema(schema_constants.TEXT, dict_s1)


class SchemaGetValueSchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      (dict_s1, schema_constants.INT32.with_db(db)),
      (dict_s2, dict_s1),
  )
  def test_eval(self, x, expected):
    res = expr_eval.eval(kde.schema.get_value_schema(x))
    testing.assert_equal(res, expected)

  def test_non_dict_schema(self):
    with self.assertRaisesRegex(
        ValueError,
        "expected Dict schema for get_value_schema",
    ):
      expr_eval.eval(kde.schema.get_value_schema(db.new(x=1)))

    with self.assertRaisesRegex(
        ValueError,
        "expected Dict schema for get_value_schema",
    ):
      expr_eval.eval(kde.schema.get_value_schema(schema_constants.ANY))

    with self.assertRaisesRegex(
        ValueError,
        "expected Dict schema for get_value_schema",
    ):
      expr_eval.eval(
          kde.schema.get_value_schema(db.new_schema(x=schema_constants.INT32))
      )

  def test_boxing(self):
    with self.assertRaisesRegex(
        ValueError,
        "expected DATA_SLICE, got dict_schema: QTYPE",
    ):
      kde.schema.get_value_schema(arolla.INT32)

    with self.assertRaisesRegex(
        ValueError,
        "expected DATA_SLICE, got dict_schema: ARRAY_INT32",
    ):
      kde.schema.get_value_schema([1])

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.get_value_schema,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.schema.get_value_schema(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.schema.get_value_schema, kde.get_value_schema)
    )


if __name__ == "__main__":
  absltest.main()
