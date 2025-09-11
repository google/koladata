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
])


class MathSqrtTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([4, None, 0, float('inf')]),
          ds([2, None, 0, float('inf')], schema_constants.FLOAT32),
      ),
      # scalar inputs, scalar output.
      (ds(4), ds(2, schema_constants.FLOAT32)),
      # multi-dimensional.
      (
          ds([[4, 9], [None, None], [None, 16]]),
          ds([[2, 3], [None, None], [None, 4]], schema_constants.FLOAT32),
      ),
      # Float
      (
          ds([1.0, None, 9.0]),
          ds([1.0, None, 3.0]),
      ),
      (
          ds([1.0, None, 9.0], schema_constants.FLOAT64),
          ds([1.0, None, 3.0], schema_constants.FLOAT64),
      ),
      # OBJECT
      (
          ds([4, None, 0], schema_constants.OBJECT),
          ds([2.0, None, 0.0]).with_schema(schema_constants.OBJECT),
      ),
      # Empty and unknown inputs.
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None, None]),
          ds([None, None, None]),
      ),
      (
          ds([None, None, None], schema_constants.INT32),
          ds([None, None, None], schema_constants.FLOAT32),
      ),
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.OBJECT),
      ),
  )
  def test_eval(self, x, expected):
    result = expr_eval.eval(kde.math.sqrt(I.x), x=x)
    testing.assert_equal(result, expected)

  def test_errors(self):
    x = ds([True, False, True])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.sqrt: argument `x` must be a slice of numeric values, got'
            ' a slice of BOOLEAN'
        ),
    ):
      expr_eval.eval(kde.math.sqrt(I.x), x=x)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.math.sqrt,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(repr(kde.math.sqrt(I.x)), 'kd.math.sqrt(I.x)')

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.math.sqrt(I.x)))


if __name__ == '__main__':
  absltest.main()
