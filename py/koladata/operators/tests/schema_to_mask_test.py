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

"""Tests for kde.schema.to_mask.

Extensive testing is done in C++.
"""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class SchemaToMaskTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(None, schema_constants.OBJECT), ds(None, schema_constants.MASK)),
      (ds(arolla.present()), ds(arolla.present())),
      (ds(arolla.missing()), ds(arolla.missing())),
      (ds(arolla.present(), schema_constants.OBJECT), ds(arolla.present())),
      (ds([None], schema_constants.OBJECT), ds([None], schema_constants.MASK)),
      (ds([arolla.present()]), ds([arolla.present()])),
      (ds([arolla.missing()]), ds([arolla.missing()])),
      (
          ds([arolla.present(), None], schema_constants.OBJECT),
          ds([arolla.present(), None]),
      ),
  )
  def test_eval(self, x, expected):
    res = expr_eval.eval(kde.schema.to_mask(x))
    testing.assert_equal(res, expected)

  @parameterized.parameters(
      ds(None, schema_constants.STRING), ds("a"), ds(True)
  )
  def test_not_castable_error(self, value):
    with self.assertRaisesRegex(
        ValueError, f"unsupported schema: {value.get_schema()}"
    ):
      expr_eval.eval(kde.schema.to_mask(value))

  def test_not_castable_internal_value(self):
    x = ds("a", schema_constants.OBJECT)
    with self.assertRaisesRegex(ValueError, "cannot cast STRING to MASK"):
      expr_eval.eval(kde.schema.to_mask(x))

  def test_boxing(self):
    testing.assert_equal(
        kde.schema.to_mask(ds(arolla.present())),
        arolla.abc.bind_op(
            kde.schema.to_mask, literal_operator.literal(ds(arolla.present()))
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.to_mask,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.to_mask(I.x)))


if __name__ == "__main__":
  absltest.main()
