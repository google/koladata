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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.exceptions import exceptions
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
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class MathCumMaxTest(parameterized.TestCase):

  @parameterized.parameters(
      # INT32
      (
          ds([2, None, 0, 3, 1], schema_constants.INT32),
          ds(
              [2, None, 2, 3, 3],
              schema_constants.INT32,
          ),
      ),
      # INT64
      (
          ds([2, None, 0, 3, 1], schema_constants.INT64),
          ds(
              [2, None, 2, 3, 3],
              schema_constants.INT64,
          ),
      ),
      # FLOAT32
      (
          ds(
              [2, None, 0, -2, float('-inf'), 3, float('inf'), float('nan'), 5],
              schema_constants.FLOAT32,
          ),
          ds(
              [
                  2,
                  None,
                  2,
                  2,
                  2,
                  3,
                  float('inf'),
                  float('nan'),
                  float('nan'),
              ],
              schema_constants.FLOAT32,
          ),
      ),
      # FLOAT64
      (
          ds(
              [2, None, 0, -2, float('-inf'), 3, float('inf'), float('nan'), 5],
              schema_constants.FLOAT64,
          ),
          ds(
              [
                  2,
                  None,
                  2,
                  2,
                  2,
                  3,
                  float('inf'),
                  float('nan'),
                  float('nan'),
              ],
              schema_constants.FLOAT64,
          ),
      ),
      # multi-dimensional.
      (
          ds([[3, -4], [None, None], [None, 10]]),
          ds(
              [[3, 3], [None, None], [None, 10]],
              schema_constants.INT32,
          ),
      ),
  )
  def test_eval_numeric(self, x, expected):
    result = expr_eval.eval(kde.math.cum_max(I.x), x=x)
    testing.assert_allclose(result, expected)

  @parameterized.parameters(
      (
          ds([[3, -4], [None, None], [None, 1]]),
          arolla.unspecified(),
          ds(
              [[3, 3], [None, None], [None, 1]],
              schema_constants.INT32,
          ),
      ),
      (
          ds([[3, -4], [None, None], [None, 1]]),
          ds(0),
          ds(
              [[3, -4], [None, None], [None, 1]],
              schema_constants.INT32,
          ),
      ),
      (
          ds(3),
          ds(0),
          ds(3, schema_constants.INT32),
      ),
      (
          ds([[3, -4], [None, None], [None, 1]]),
          ds(1),
          ds(
              [[3, 3], [None, None], [None, 1]],
              schema_constants.INT32,
          ),
      ),
      (
          ds([[3, -4], [None, None], [None, 1]]),
          ds(2),
          ds(
              [[3, 3], [None, None], [None, 3]],
              schema_constants.INT32,
          ),
      ),
  )
  def test_eval_numeric_with_into(self, x, into, expected):
    result = expr_eval.eval(kde.math.cum_max(x, into))
    testing.assert_allclose(result, expected)

  # Empty inputs of concrete and None types.
  @parameterized.parameters(
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None, None]),
          ds([None, None, None]),
      ),
      (
          ds([None, None, None], schema_constants.FLOAT32),
          ds([None, None, None], schema_constants.FLOAT32),
      ),
      (
          ds([None, None, None], schema_constants.INT32),
          ds([None, None, None], schema_constants.INT32),
      ),
      (
          ds([None, None, None], schema_constants.ANY),
          ds([None, None, None], schema_constants.ANY),
      ),
  )
  def test_eval_non_numeric(self, x, expected):
    result = expr_eval.eval(kde.math.cum_max(I.x), x=x)
    testing.assert_equal(result, expected)

  def test_errors(self):
    x = data_slice.DataSlice.from_vals(['1', '2', '3'])
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kde.math.cum_max: argument `x` must be a slice of numeric values,'
            ' got a slice of STRING'
        ),
    ):
      expr_eval.eval(kde.math.cum_max(I.x), x=x)

    scalar = ds(-2.0, schema_constants.FLOAT32)
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape('expected rank(x) > 0'),
    ):
      expr_eval.eval(kde.math.cum_max(I.x), x=scalar)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.math.cum_max,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.math.cum_max(I.x)), 'kde.math.cum_max(I.x, unspecified)'
    )
    self.assertEqual(
        repr(kde.math.cum_max(I.x, I.ndim)), 'kde.math.cum_max(I.x, I.ndim)'
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.math.cum_max(I.x)))


if __name__ == '__main__':
  absltest.main()
