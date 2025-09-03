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
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from scipy import stats

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class TDistributionInverseCdfTest(parameterized.TestCase):

  @parameterized.parameters(
      *zip([0, 0.01, 0.1, 0.5, 0.9, 0.99, 1], [0.1, 1, 2, 3, 10, 100, 1e6])
  )
  def test_eval_numeric_defaults(self, x, degrees_of_freedom):
    expected = float(stats.t.ppf(x, degrees_of_freedom))
    result = expr_eval.eval(
        kde.math.t_distribution_inverse_cdf(I.x, I.degrees_of_freedom),
        x=x,
        degrees_of_freedom=degrees_of_freedom,
    )
    testing.assert_allclose(result, ds(expected), rtol=1e-6, atol=1e-15)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.math.t_distribution_inverse_cdf,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.math.t_distribution_inverse_cdf(I.x, I.degrees_of_freedom)),
        'kd.math.t_distribution_inverse_cdf(I.x, I.degrees_of_freedom)',
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.math.t_distribution_inverse_cdf(I.x, I.degrees_of_freedom)
        )
    )


if __name__ == '__main__':
  absltest.main()
