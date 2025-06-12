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
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
])


class StringsLengthTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('abcde'), ds(5, schema_constants.INT64)),
      (
          ds(['abc', None, '', 'ef']),
          ds([3, None, 0, 2], schema_constants.INT64),
      ),
      (
          ds([['abc', None], ['', 'ef']]),
          ds([[3, None], [0, 2]], schema_constants.INT64),
      ),
      (ds('你好'), ds(2, schema_constants.INT64)),
      # Bytes
      (
          ds([b'abc', None, b'', b'ef']),
          ds([3, None, 0, 2], schema_constants.INT64),
      ),
      (ds('你好'.encode()), ds(6, schema_constants.INT64)),
      # OBJECT
      (
          ds(['abc', None, '', 'ef'], schema_constants.OBJECT),
          ds([3, None, 0, 2], schema_constants.INT64),
      ),
      # Empty and unknown inputs.
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.INT64),
      ),
      (
          ds([None, None, None]),
          ds([None, None, None], schema_constants.INT64),
      ),
      (
          ds([None, None, None], schema_constants.STRING),
          ds([None, None, None], schema_constants.INT64),
      ),
  )
  def test_eval(self, x, expected):
    result = expr_eval.eval(kde.strings.length(I.x), x=x)
    testing.assert_equal(result, expected)

  def test_errors(self):
    x = ds([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        'kd.strings.length: argument `x` must be a slice of either STRING or'
        ' BYTES, got a slice of INT32',
    ):
      expr_eval.eval(kde.strings.length(I.x), x=x)

    x = ds(['abc', b'def'])
    with self.assertRaisesRegex(
        ValueError,
        'kd.strings.length: argument `x` must be a slice of either STRING or'
        ' BYTES, got a slice of OBJECT containing BYTES and STRING values',
    ):
      expr_eval.eval(kde.strings.length(I.x), x=x)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.strings.length,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.strings.length(I.x)))


if __name__ == '__main__':
  absltest.main()
