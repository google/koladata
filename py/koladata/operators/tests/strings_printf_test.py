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


class StringsFormatTest(parameterized.TestCase):

  @parameterized.parameters(
      ([ds('foo')], ds('foo')),
      ([ds(b'foo')], ds(b'foo')),
      ([ds('%v foo'), ds('bar')], ds('bar foo')),
      ([ds(b'%v foo'), ds(b'bar')], ds(b'bar foo')),
      ([ds('%v foo'), ds(1)], ds('1 foo')),
      ([ds(b'%v foo'), ds(1)], ds(b'1 foo')),
      ([ds('%s foo'), ds(None)], ds(None, schema_constants.TEXT)),
      (
          [ds('%s foo'), ds(None, schema_constants.OBJECT)],
          ds(None, schema_constants.TEXT),
      ),
      ([ds('%s foo'), ds('bar'), ds(1)], ds('bar foo')),  # ds(1) ignored.
      ([ds('%s %v'), ds('bar'), ds(1)], ds('bar 1')),
      ([ds(['+%s', '-%s']), ds('bar')], ds(['+bar', '-bar'])),
      (
          [ds(['+%s', '-%s'], schema_constants.ANY), ds('bar')],
          ds(['+bar', '-bar'], schema_constants.ANY),
      ),
      (
          [ds('%v + %v'), ds([1, 2]), ds([[4, 5], [6]])],
          ds([['1 + 4', '1 + 5'], ['2 + 6']]),
      ),
      # Large arity.
      ([ds('%s'), *([ds('foo')] * 30)], ds('foo')),
      # Empty and unknown.
      ([ds([None, None])], ds([None, None])),
      (
          [ds([None, None], schema_constants.TEXT)],
          ds([None, None], schema_constants.TEXT),
      ),
      (
          [ds([None, None], schema_constants.BYTES)],
          ds([None, None], schema_constants.BYTES),
      ),
      (
          [ds([None, None], schema_constants.OBJECT)],
          ds([None, None], schema_constants.OBJECT),
      ),
      ([ds([None, None]), ds('abc')], ds([None, None])),
  )
  def test_eval(self, args, expected):
    result = expr_eval.eval(kde.strings.printf(*args))
    testing.assert_equal(result, expected)

  def test_incompatible_types_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('unsupported argument types (TEXT,BYTES)')
    ):
      expr_eval.eval(kde.strings.printf(ds('%s'), ds(b'foo')))

  def test_missing_input_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("format specification '%s' doesn't match format arguments"),
    ):
      expr_eval.eval(kde.strings.printf(ds('%s')))

  def test_incompatible_format_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("format specification '%s' doesn't match format arguments"),
    ):
      expr_eval.eval(kde.strings.printf(ds('%s'), ds(1)))

  def test_mixed_slice_error(self):
    with self.assertRaisesRegex(
        ValueError, 'DataSlice with mixed types is not supported'
    ):
      expr_eval.eval(kde.strings.printf(ds('%v'), ds([1, 'foo'])))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.printf,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        max_arity=3,
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.strings.printf(I.x)))


if __name__ == '__main__':
  absltest.main()
