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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.exceptions import exceptions
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

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
])


class MathLogTest(parameterized.TestCase):

  @parameterized.parameters(
      # INT32 casted to FLOAT32
      (
          ds([10, None, 0, -1, 100], schema_constants.INT32),
          ds(
              [1.0, None, float('-inf'), float('nan'), 2.0],
              schema_constants.FLOAT32,
          ),
      ),
      # INT64 casted to FLOAT32
      (
          ds([10, None, 0, 100], schema_constants.INT64),
          ds(
              [1.0, None, float('-inf'), 2.0],
              schema_constants.FLOAT32,
          ),
      ),
      # FLOAT32
      (
          ds(
              [
                  10.0,
                  None,
                  0,
                  100.0,
                  float('inf'),
                  float('nan'),
                  float('-inf'),
              ],
              schema_constants.FLOAT32,
          ),
          ds(
              [
                  1.0,
                  None,
                  float('-inf'),
                  2.0,
                  float('inf'),
                  float('nan'),
                  float('nan'),
              ],
              schema_constants.FLOAT32,
          ),
      ),
      # FLOAT64
      (
          ds([10.0, None, 0, 100.0], schema_constants.FLOAT64),
          ds(
              [1.0, None, float('-inf'), 2.0],
              schema_constants.FLOAT64,
          ),
      ),
      # scalar inputs, scalar output.
      (
          ds(10.0, schema_constants.FLOAT32),
          ds(1.0, schema_constants.FLOAT32),
      ),
      # multi-dimensional.
      (
          ds([[0.1, 1000], [None, None], [None, 10]]),
          ds(
              [[-1.0, 3.0], [None, None], [None, 1.0]],
              schema_constants.FLOAT32,
          ),
      ),
  )
  def test_eval_numeric(self, x, expected):
    result = expr_eval.eval(kde.math.log10(I.x), x=x)
    testing.assert_allclose(result, expected)

  # Empty and unknown inputs.
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
          ds([None, None, None], schema_constants.FLOAT32),
      ),
      (
          ds([None, None, None], schema_constants.ANY),
          ds([None, None, None], schema_constants.ANY),
      ),
  )
  def test_eval_non_numeric(self, x, expected):
    result = expr_eval.eval(kde.math.log10(I.x), x=x)
    testing.assert_equal(result, expected)

  def test_errors(self):
    x = ds(['1', '2', '3'])
    with self.assertRaisesRegex(
        exceptions.KodaError,
        # TODO: Make errors Koda friendly.
        'expected numerics, got x: DENSE_ARRAY_TEXT',
    ):
      expr_eval.eval(kde.math.log10(I.x), x=x)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.math.log10,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(repr(kde.math.log10(I.x)), 'kde.math.log10(I.x)')
    self.assertEqual(repr(kde.log10(I.x)), 'kde.log10(I.x)')

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.math.log10(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.math.log10, kde.log10))


if __name__ == '__main__':
  absltest.main()
