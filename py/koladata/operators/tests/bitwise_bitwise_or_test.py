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


class BitwiseOrTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(5), ds(3), ds(7)),
      (
          ds(5, schema_constants.INT64),
          ds(3, schema_constants.INT64),
          ds(7, schema_constants.INT64),
      ),
      (ds([5, 5, 5]), ds([3, 5, 0]), ds([7, 5, 5])),
      (ds(5), ds(None), ds(None, schema_constants.INT32)),
      (ds(5), ds([3, 5, 0]), ds([7, 5, 5])),
      (
          ds(5, schema_constants.INT32),
          ds(3, schema_constants.INT64),
          ds(7, schema_constants.INT64),
      ),
      (
          ds(5, schema_constants.OBJECT),
          ds(3, schema_constants.INT32),
          ds(7, schema_constants.OBJECT)
      ),
      (ds(None), ds(None), ds(None),),
  )
  def test_eval(self, x, y, expected):
    result = expr_eval.eval(kde.bitwise.bitwise_or(I.x, I.y), x=x, y=y)
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      (
          ds(3.0),
          ds(1),
          (
              'kd.bitwise.bitwise_or: argument `x` must be a slice of integer'
              ' values, got a slice of FLOAT32'
          ),
      ),
      (
          ds(1),
          ds(3.0),
          (
              'kd.bitwise.bitwise_or: argument `y` must'
              ' be a slice of integer values, got a slice'
              ' of FLOAT32'
          ),
      ),
  )
  def test_errors(self, x, y, expected_error):
    with self.assertRaisesWithLiteralMatch(ValueError, expected_error):
      expr_eval.eval(kde.bitwise.bitwise_or(I.x, I.y), x=x, y=y)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.bitwise.bitwise_or,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.bitwise.bitwise_or(I.x, I.y)),
        'kd.bitwise.bitwise_or(I.x, I.y)',
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.bitwise.bitwise_or, kde.bitwise_or)
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.bitwise.bitwise_or(I.x, I.y)))


if __name__ == '__main__':
  absltest.main()
