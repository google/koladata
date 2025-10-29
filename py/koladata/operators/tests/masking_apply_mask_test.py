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
from koladata.operators.tests.testdata import masking_apply_mask_testdata
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class LogicalApplyMaskTest(parameterized.TestCase):

  @parameterized.parameters(*masking_apply_mask_testdata.TEST_CASES)
  def test_eval(self, values, mask, expected):
    result = expr_eval.eval(kde.masking.apply_mask(values, mask))
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.masking.apply_mask,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.masking.apply_mask: argument `mask` must be a slice of MASK,'
            ' got a slice of INT32'
        ),
    ):
      expr_eval.eval(kde.masking.apply_mask(ds([1, 2, 3]), ds([1, None, 3])))

  def test_repr(self):
    self.assertEqual(repr(kde.masking.apply_mask(I.x, I.y)), 'I.x & I.y')
    self.assertEqual(repr(kde.apply_mask(I.x, I.y)), 'I.x & I.y')

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.masking.apply_mask(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.masking.apply_mask, kde.apply_mask))


if __name__ == '__main__':
  absltest.main()
