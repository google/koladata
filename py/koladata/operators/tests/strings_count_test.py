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

"""Tests for kde.strings.count."""

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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class StringsCountTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('foo'), ds('oo'), ds(1)),
      (ds('foo'), ds('zoo'), ds(0)),
      (ds(b'foo'), ds(b'o'), ds(2)),
      (ds(b'foo'), ds(b'boo'), ds(0)),
      (ds(['foo', 'zoo', 'bar']), ds('o'), ds([2, 2, 0])),
      (
          ds([b'foo', b'zoo', b'bar']),
          ds(b'o'),
          ds([2, 2, 0]),
      ),
      (
          ds([['foo', 'zooo'], ['bar']]),
          ds(['oo', 'a']),
          ds([[1, 2], [1]]),
      ),
      (
          ds([['foo', 'zoo'], ['baba']]),
          ds(['zo', 'ba']),
          ds([[0, 1], [2]]),
      ),
      (ds('foo'), ds(None), ds(None, schema_constants.INT32)),
      (
          ds('foo'),
          ds(None, schema_constants.OBJECT),
          ds(None, schema_constants.INT32),
      ),
      (
          ds(['foo'], schema_constants.ANY),
          ds('foo'),
          ds([1]),
      ),
      # Empty and unknown.
      (
          ds([None, None]),
          ds([None, None]),
          ds([None, None], schema_constants.INT32),
      ),
      (
          ds([None, None], schema_constants.TEXT),
          ds(None, schema_constants.TEXT),
          ds([None, None], schema_constants.INT32)
      ),
      (
          ds([None, None], schema_constants.BYTES),
          ds(None, schema_constants.BYTES),
          ds([None, None], schema_constants.INT32)
      ),
      (
          ds([None, None], schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds([None, None], schema_constants.INT32)
      ),
      (ds([None, None]), ds('abc'), ds([None, None], schema_constants.INT32)),
  )
  def test_eval(self, s, substr, expected):
    result = expr_eval.eval(kde.strings.count(s, substr))
    testing.assert_equal(result, expected)

  def test_incompatible_types_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('unsupported argument types (TEXT,BYTES)'),
    ):
      expr_eval.eval(kde.strings.count(ds('foo'), ds(b'f')))

  def test_another_incompatible_types_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('unsupported argument types (DENSE_ARRAY_TEXT,INT32)'),
    ):
      expr_eval.eval(
          kde.strings.count(ds([None], schema_constants.TEXT), ds(123))
      )

  def test_mixed_slice_error(self):
    with self.assertRaisesRegex(
        ValueError, 'DataSlice with mixed types is not supported'
    ):
      expr_eval.eval(kde.strings.count(ds('foo'), ds([1, 'fo'])))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.count,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.strings.count(I.x, I.y)))


if __name__ == '__main__':
  absltest.main()
