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

from absl.testing import absltest
from absl.testing import parameterized
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.operators import kde_operators
from koladata.operators.tests.util import curves_test_case
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde

CURVE = ds([[-10, -100], [1, 5], [100, 10], [300, 20]])


class CurvesSymmetricLogP1PwlCurveTest(curves_test_case.BaseCurveTestcase):

  @property
  def operator(self):
    return kde.curves.symmetric_log_p1_pwl_curve

  @parameterized.parameters((
      ds(-3.5),
      CURVE,
      ds(-69.6378, schema_constants.FLOAT64),
  ))
  def test_eval_middle_points(self, p, adjustments, expected):
    """Testing curve in the middle points."""
    result = expr_eval.eval(
        self.operator(I.p, I.adjustments), p=p, adjustments=adjustments
    )
    testing.assert_allclose(result, expected, rtol=1e-4)


if __name__ == '__main__':
  absltest.main()
