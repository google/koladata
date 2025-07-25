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
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE

present = arolla.present()
missing = arolla.missing()


QTYPES = frozenset([(DATA_SLICE, DATA_SLICE, DATA_SLICE)])


class LogicalMaskNotEqualTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(present), ds(present), ds(missing)),
      (ds(present), ds(missing), ds(present)),
      (ds(missing), ds(present), ds(present)),
      (ds(missing), ds(missing), ds(missing)),
      (
          ds([present, present, missing, missing]),
          ds([present, missing, present, missing]),
          ds([missing, present, present, missing]),
      ),
      (
          ds([[present, present], [missing, missing]]),
          ds([[present, missing], [present, missing]]),
          ds([[missing, present], [present, missing]]),
      ),
      (
          ds([present, missing]),
          ds([[present, missing], [present, missing]]),
          ds([[missing, present], [present, missing]]),
      ),
      (
          ds([present, present, missing, missing], schema_constants.OBJECT),
          ds([present, missing, present, missing]),
          ds([missing, present, present, missing]),
      ),
  )
  def test_eval(self, x, y, expected):
    result = expr_eval.eval(kde.masking.mask_not_equal(x, y))
    testing.assert_equal(result, expected)

  def test_invalid_input(self):

    with self.assertRaisesRegex(
        ValueError,
        re.escape('argument `x` must be a slice of MASK, got a slice of INT32'),
    ):
      _ = expr_eval.eval(kde.masking.mask_not_equal(ds(1), ds(present)))

    with self.assertRaisesRegex(
        ValueError,
        re.escape('argument `y` must be a slice of MASK, got a slice of INT32'),
    ):
      _ = expr_eval.eval(kde.masking.mask_not_equal(ds(present), ds(1)))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.masking.mask_not_equal,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.masking.mask_not_equal(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.masking.mask_not_equal, kde.mask_not_equal)
    )


if __name__ == '__main__':
  absltest.main()
