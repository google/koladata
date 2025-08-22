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

"""Tests for kde.schema.to_itemid.

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
from koladata.types import qtypes
from koladata.types import schema_constants


eval_op = py_expr_eval_py_ext.eval_op
I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
OBJ = data_bag.DataBag.empty_mutable().obj()
ENTITY = data_bag.DataBag.empty_mutable().new()


class SchemaToItemidTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(None, schema_constants.OBJECT), ds(None, schema_constants.ITEMID)),
      (OBJ, OBJ.with_schema(schema_constants.ITEMID)),
      (ENTITY, ENTITY.with_schema(schema_constants.ITEMID)),
      (
          ds([None], schema_constants.OBJECT),
          ds([None], schema_constants.ITEMID),
      ),
      (ds([OBJ]), ds([OBJ.with_schema(schema_constants.ITEMID)])),
      (ds([ENTITY]), ds([ENTITY.with_schema(schema_constants.ITEMID)])),
  )
  def test_eval(self, x, expected):
    res = eval_op("kd.schema.to_itemid", x)
    testing.assert_equal(res, expected)

  @parameterized.parameters(
      ds(None, schema_constants.STRING), ds("a"), ds(arolla.present())
  )
  def test_not_castable_error(self, value):
    with self.assertRaisesRegex(
        ValueError,
        f"casting a DataSlice with schema {value.get_schema()} to ITEMID is"
        " not supported",
    ):
      expr_eval.eval(kde.schema.to_itemid(value))

  def test_not_castable_internal_value(self):
    x = ds("a", schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError,
        "casting data of type STRING to ITEMID is not supported",
    ):
      expr_eval.eval(kde.schema.to_itemid(x))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.to_itemid,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.to_itemid(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.schema.to_itemid, kde.to_itemid))
    self.assertTrue(
        optools.equiv_to_op(kde.schema.to_itemid, kde.schema.get_itemid)
    )
    self.assertTrue(optools.equiv_to_op(kde.schema.to_itemid, kde.get_itemid))


if __name__ == "__main__":
  absltest.main()
