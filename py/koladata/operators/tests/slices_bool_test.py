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

import re

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
from koladata.types import literal_operator
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class SlicesBoolTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(None, schema_constants.OBJECT), ds(None, schema_constants.BOOLEAN)),
      (ds(True), ds(True)),
      (ds(True, schema_constants.OBJECT), ds(True)),
      ([False, True, None], ds([False, True, None])),
      (
          [False, literal_operator.literal(ds(True)), None],
          ds([False, True, None]),
      ),
  )
  def test_eval(self, x, expected):
    res = expr_eval.eval(kde.slices.bool(x))
    testing.assert_equal(res, expected)

  def test_error(self):
    x = ds("a", schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, "casting data of type STRING to BOOLEAN is not supported"
    ):
      expr_eval.eval(kde.slices.bool(x))

  def test_mask_error(self):
    x = ds(arolla.present(), schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "casting data of type MASK to BOOLEAN is not supported; try"
            " `kd.cond(slice, True, False)` instead"
        ),
    ):
      expr_eval.eval(kde.slices.bool(x))

  def test_boxing(self):
    testing.assert_equal(
        kde.slices.bool(True),
        arolla.abc.bind_op(kde.slices.bool, literal_operator.literal(ds(True))),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.bool,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.bool(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.bool, kde.bool))


if __name__ == "__main__":
  absltest.main()
