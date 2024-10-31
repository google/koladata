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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class MathMultiplyTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([1, None, 3]),
          ds([4, 1, 0]),
          ds([4, None, 0]),
      ),
      # Auto-broadcasting
      (
          ds([1, None, 3], schema_constants.INT64),
          ds(2, schema_constants.INT64),
          ds([2, None, 6], schema_constants.INT64),
      ),
      # scalar inputs, scalar output.
      (ds(3), ds(4), ds(12)),
      # multi-dimensional.
      (
          ds([1, None, 3]),
          ds([[1, 2], [3, 4], [None, 6]]),
          ds([[1, 2], [None, None], [None, 18]]),
      ),
      # Float
      (
          ds([1.0, None, 3.0]),
          ds(2.0),
          ds([2.0, None, 6.0]),
      ),
      # OBJECT/ANY
      (
          ds([2, None, 3], schema_constants.OBJECT),
          ds([4, 1, 0], schema_constants.INT64).with_schema(
              schema_constants.ANY
          ),
          ds([8, None, 0], schema_constants.INT64).with_schema(
              schema_constants.ANY
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
          ds([None, None, None], schema_constants.ANY),
          ds([None, None, None], schema_constants.FLOAT32),
          ds([None, None, None], schema_constants.ANY),
      ),
      (
          ds([None, None, None]),
          ds([4, 1, 0]),
          ds([None, None, None], schema_constants.INT32),
      ),
      (
          ds([None, None, None], schema_constants.ANY),
          ds([4, 1, 0]),
          ds([None, None, None], schema_constants.ANY),
      ),
  )
  def test_eval(self, x, y, expected):
    result = expr_eval.eval(kde.math.multiply(I.x, I.y), x=x, y=y)
    testing.assert_equal(result, expected)

  def test_errors(self):
    x = ds([1, 2, 3])
    y = ds(['1', '2', '3'])
    with self.assertRaisesRegex(
        exceptions.KodaError,
        # TODO: Make errors Koda friendly.
        'expected numerics, got y: DENSE_ARRAY_TEXT',
    ):
      expr_eval.eval(kde.math.multiply(I.x, I.y), x=x, y=y)

    z = ds([[1, 2], [3]])
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'shapes are not compatible',
    ):
      expr_eval.eval(kde.math.multiply(I.x, I.z), x=x, z=z)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.math.multiply,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(repr(kde.math.multiply(I.x, I.y)), 'I.x * I.y')
    self.assertEqual(repr(kde.multiply(I.x, I.y)), 'I.x * I.y')

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.math.multiply(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.math.multiply, kde.multiply))


if __name__ == '__main__':
  absltest.main()
