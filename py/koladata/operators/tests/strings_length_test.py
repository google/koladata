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

"""Tests for kde.strings.length."""

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
      (ds('abcde'), ds(5)),
      (
          ds(['abc', None, '', 'ef']),
          ds([3, None, 0, 2]),
      ),
      (
          ds([['abc', None], ['', 'ef']]),
          ds([[3, None], [0, 2]]),
      ),
      (ds('你好'), ds(2)),
      # Bytes
      (
          ds([b'abc', None, b'', b'ef']),
          ds([3, None, 0, 2]),
      ),
      (ds('你好'.encode()), ds(6)),
      # OBJECT/ANY
      (
          ds(['abc', None, '', 'ef'], schema_constants.OBJECT),
          ds([3, None, 0, 2]),
      ),
      (
          ds(['abc', None, '', 'ef'], schema_constants.ANY),
          ds([3, None, 0, 2]),
      ),
      # Empty and unknown inputs.
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.INT32),
      ),
      (
          ds([None, None, None]),
          ds([None, None, None], schema_constants.INT32),
      ),
      (
          ds([None, None, None], schema_constants.ANY),
          ds([None, None, None], schema_constants.INT32),
      ),
      (
          ds([None, None, None], schema_constants.TEXT),
          ds([None, None, None], schema_constants.INT32),
      ),
  )
  def test_eval(self, x, expected):
    result = expr_eval.eval(kde.strings.length(I.x), x=x)
    testing.assert_equal(result, expected)

  def test_errors(self):
    x = data_slice.DataSlice.from_vals([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        # TODO: Make errors Koda friendly.
        'expected texts/byteses or corresponding array, got x:'
        ' DENSE_ARRAY_INT32',
    ):
      expr_eval.eval(kde.strings.length(I.x), x=x)

    x = data_slice.DataSlice.from_vals(['abc', b'def'])
    with self.assertRaisesRegex(
        ValueError, 'DataSlice with mixed types is not supported'
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
    self.assertTrue(view.has_data_slice_view(kde.strings.length(I.x)))


if __name__ == '__main__':
  absltest.main()