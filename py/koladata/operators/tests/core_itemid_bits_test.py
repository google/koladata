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

"""Tests for kde.core.itemid_bits."""

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
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreItemIdBitsTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds([None, None]).as_any(), ds([None, None], schema_constants.INT64)),
      (ds(None).as_any(), ds(None, schema_constants.INT64)),
  )
  def test_eval(self, val, expected):
    result = expr_eval.eval(
        kde.core.itemid_bits(
            val,
            10,
        )
    )
    testing.assert_equal(result, expected)

  def test_invalid_parameter(self):
    db = bag()
    obj = db.obj(x=ds(1))

    with self.assertRaisesRegex(ValueError, "between 0 and 64"):
      expr_eval.eval(kde.core.itemid_bits(obj, -1))

    with self.assertRaisesRegex(ValueError, "between 0 and 64"):
      expr_eval.eval(kde.core.itemid_bits(obj, 65))

    with self.assertRaisesRegex(ValueError, "must be an integer"):
      expr_eval.eval(kde.core.itemid_bits(obj, "a"))

  def test_invalid_schema(self):
    val = ds([1, 2])
    with self.assertRaisesRegex(
        ValueError, "the schema of the ds must be itemid, any, or object"
    ):
      expr_eval.eval(kde.core.itemid_bits(val, 10))

  def test_view(self):
    self.assertTrue(
        view.has_data_slice_view(kde.core.itemid_bits(I.ds, I.last))
    )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.itemid_bits, kde.itemid_bits))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.itemid_bits,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )


if __name__ == "__main__":
  absltest.main()
