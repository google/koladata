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
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class MathSigmoidTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([-1.0, 0.0, 1.0, 2.0, None]),
          ds([0.26894143, 0.5, 0.7310586, 0.880797, None]),
      ),
      (
          ds([-1, 0, 1, 2, None]),
          ds([0.26894143, 0.5, 0.7310586, 0.880797, None]),
      ),
  )
  def test_eval_numeric_defaults(self, x, expected):
    result = expr_eval.eval(kde.math.sigmoid(I.x), x=x)
    testing.assert_allclose(result, expected)

  @parameterized.parameters(
      (
          ds([-1.0, 0.0, 1.0, 2.0, None]),
          1,
          2,
          ds([0.0179862100630, 0.1192029193043, 0.5, 0.880797028541, None]),
      ),
      (
          ds([[-1.0, 0.0, 1.0], [2.0, None]]),
          ds([[1, 1, 1], [1, 1]]),
          2,
          ds([[0.0179862100630, 0.1192029193043, 0.5], [0.880797028541, None]]),
      ),
      (
          ds([-1.0, 0.0, None]),
          ds([[1, 1, 1], [1], [1]]),
          2,
          ds([[0.01798621, 0.01798621, 0.01798621], [0.11920292], [None]]),
      ),
      (
          ds([-1.0, 0.0, None]),
          1,
          ds([[2, 2, 2], [2], [2]]),
          ds([[0.01798621, 0.01798621, 0.01798621], [0.11920292], [None]]),
      )
  )
  def test_eval_numeric_half_slope(self, x, half, slope, expected):
    result = expr_eval.eval(
        kde.math.sigmoid(I.x, I.half, I.slope), x=x, half=half, slope=slope
    )
    testing.assert_allclose(result, expected)

  def test_errors(self):
    x = ds(['1', '2', '3'])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.log: argument `x` must be a slice of numeric values, got'
            ' a slice of STRING'
        ),
    ):
      expr_eval.eval(kde.math.log(I.x), x=x)

  @parameterized.parameters(
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None, None]),
          ds([None, None, None], schema_constants.FLOAT32),
      ),
      (
          ds([None, None, None], schema_constants.FLOAT32),
          ds([None, None, None], schema_constants.FLOAT32),
      ),
      (
          ds([None, None, None], schema_constants.INT32),
          ds([None, None, None], schema_constants.FLOAT32),
      ),
  )
  def test_eval_non_numeric(self, x, expected):
    result = expr_eval.eval(kde.math.sigmoid(I.x), x=x)
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.math.sigmoid,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.math.sigmoid(I.x)),
        'kd.math.sigmoid(I.x, DataItem(0.0, schema: FLOAT32), DataItem(1.0,'
        ' schema: FLOAT32))',
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.math.sigmoid(I.x)))


if __name__ == '__main__':
  absltest.main()
