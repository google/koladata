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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class MathModTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([5, None, -5, 3]),
          ds([2, 1, 2, 1]),
          ds([1, None, 1, 0]),
      ),
      # Auto-broadcasting
      (
          ds([2, None, 5], schema_constants.INT64),
          ds(2, schema_constants.INT64),
          ds([0, None, 1], schema_constants.INT64),
      ),
      # scalar inputs, scalar output.
      (ds(5), ds(2), ds(1)),
      # multi-dimensional.
      (
          ds([[5, -5], [None, None], [None, 3]]),
          ds([2, 3, 1]),
          ds([[1, 1], [None, None], [None, 0]]),
      ),
      # Float
      (
          ds([2.0, None, 5.0]),
          ds(2.0),
          ds([0.0, None, 1.0]),
      ),
      (
          ds([2.0, None, 5.0], schema_constants.FLOAT64),
          ds(2.0),
          ds([0.0, None, 1.0], schema_constants.FLOAT64),
      ),
      # OBJECT
      (
          ds([5, None, -5], schema_constants.OBJECT),
          ds([2, 1, 2], schema_constants.INT64),
          ds([1, None, 1], schema_constants.INT64).with_schema(
              schema_constants.OBJECT
          ),
      ),
      # Empty and unknown inputs.
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None, None]),
          ds([None, None, None]),
          ds([None, None, None]),
      ),
      (
          ds([None, None, None]),
          ds([None, None, None], schema_constants.FLOAT32),
          ds([None, None, None], schema_constants.FLOAT32),
      ),
      (
          ds([None, None, None], schema_constants.INT32),
          ds([None, None, None], schema_constants.FLOAT32),
          ds([None, None, None], schema_constants.FLOAT32),
      ),
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.FLOAT32),
          ds([None, None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None, None]),
          ds([4, 1, 0]),
          ds([None, None, None], schema_constants.INT32),
      ),
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([4, 1, 0]),
          ds([None, None, None], schema_constants.OBJECT),
      ),
  )
  def test_eval(self, x, y, expected):
    result = expr_eval.eval(kde.math.mod(I.x, I.y), x=x, y=y)
    testing.assert_equal(result, expected)

  def test_errors(self):
    x = ds([1, 2, 3])
    y = ds(['1', '2', '3'])
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.math.mod: argument `y` must be a slice of numeric values, got'
            ' a slice of STRING'
        ),
    ):
      expr_eval.eval(kde.math.mod(I.x, I.y), x=x, y=y)

    z = ds([[1, 2], [3]])
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'shapes are not compatible',
    ):
      expr_eval.eval(kde.math.mod(I.x, I.z), x=x, z=z)

    w = ds(0)
    with self.assertRaisesRegex(
        exceptions.KodaError,
        # TODO: Make errors Koda friendly.
        'division by zero',
    ):
      expr_eval.eval(kde.math.mod(I.x, I.w), x=x, w=w)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.math.mod,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(repr(kde.math.mod(I.x, I.y)), 'I.x % I.y')

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.math.mod(I.x, I.y)))


if __name__ == '__main__':
  absltest.main()
