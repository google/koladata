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

"""Tests for kde.schema.get_item_schema."""

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
list_s1 = db.list_schema(schema_constants.INT32)
list_s2 = db.list_schema(list_s1)


class SchemaGetItemSchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      (list_s1, schema_constants.INT32.with_db(db)),
      (list_s2, list_s1),
  )
  def test_eval(self, x, expected):
    res = expr_eval.eval(kde.schema.get_item_schema(x))
    testing.assert_equal(res, expected)

  def test_non_list_schema(self):
    with self.assertRaisesRegex(
        ValueError,
        "expected List schema for get_item_schema",
    ):
      expr_eval.eval(kde.schema.get_item_schema(db.new(x=1)))

    with self.assertRaisesRegex(
        ValueError,
        "expected List schema for get_item_schema",
    ):
      expr_eval.eval(kde.schema.get_item_schema(schema_constants.ANY))

    with self.assertRaisesRegex(
        ValueError,
        "expected List schema for get_item_schema",
    ):
      expr_eval.eval(
          kde.schema.get_item_schema(db.new_schema(x=schema_constants.INT32))
      )

  def test_boxing(self):
    with self.assertRaisesRegex(
        ValueError,
        "expected DATA_SLICE, got list_schema: QTYPE",
    ):
      kde.schema.get_item_schema(arolla.INT32)

    with self.assertRaisesRegex(
        ValueError,
        "expected DATA_SLICE, got list_schema: ARRAY_INT32",
    ):
      kde.schema.get_item_schema([1])

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.get_item_schema,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.schema.get_item_schema(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.schema.get_item_schema, kde.get_item_schema)
    )


if __name__ == "__main__":
  absltest.main()
