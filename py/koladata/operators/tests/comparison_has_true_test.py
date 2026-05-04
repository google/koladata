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
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import mask_constants

I = input_container.InputContainer('I')

kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
missing = mask_constants.missing
present = mask_constants.present


class ComparisonHasTrueTest(parameterized.TestCase):

  @parameterized.parameters(
      # Empty
      (ds(None), missing),
      # True / False
      (ds(True), present),
      (ds(False), missing),
      (ds([True, False, None]), ds([present, missing, missing])),
  )
  def test_eval(self, param, expected):
    result = kd.comparison.has_true(param)
    testing.assert_equal(result, expected)

  def test_eval_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.assertion.assert_primitive: argument `x` must be a slice of'
            ' BOOLEAN, got a slice of INT32'
        ),
    ):
      kd.comparison.has_true(ds(1))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.comparison.has_true(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.comparison.has_true, kde.has_true))


if __name__ == '__main__':
  absltest.main()
