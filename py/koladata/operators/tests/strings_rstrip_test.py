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
])


class StringsRstripTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('foo'), ds('foo')),
      (ds(' fo o  '), ds(' fo o')),
      (ds(b'foo'), ds(b'foo')),
      (ds(b' foo   '), ds(b' foo')),
      (ds(['\tfoo \n', 'zoo ', '\nbar']), ds(['\tfoo', 'zoo', '\nbar'])),
      (ds([b'\tfoo \n', b'zoo ', b'\nbar']), ds([b'\tfoo', b'zoo', b'\nbar'])),
      (
          ds([[' foo', 'bzoo '], ['  bari  \t']]),
          ds([[' foo', 'bzoo'], ['  bari']]),
      ),
      (ds([' foo '], schema_constants.ANY), ds([' foo'], schema_constants.ANY)),
      (ds(['   spacious   ', '\t text \n']), ds(['   spacious', '\t text'])),
      # Empty and unknown.
      (ds([None, None]), ds([None, None])),
      (
          ds([None, None], schema_constants.TEXT),
          ds([None, None], schema_constants.TEXT),
      ),
      (
          ds([None, None], schema_constants.BYTES),
          ds([None, None], schema_constants.BYTES),
      ),
      (
          ds([None, None], schema_constants.OBJECT),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None], schema_constants.ANY),
          ds([None, None], schema_constants.ANY),
      ),
  )
  def test_one_arg(self, s, expected):
    result = expr_eval.eval(kde.strings.rstrip(s))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      # 2nd operand is missing.
      (ds('foo'), None, ds('foo')),
      (ds(' fo o  '), None, ds(' fo o')),
      (ds(b'foo'), None, ds(b'foo')),
      (ds(b' foo   '), ds(None), ds(b' foo')),
      # 2nd operand is present.
      (ds(' fo o  '), ds(None, schema_constants.TEXT), ds(' fo o')),
      (ds(b' foo  '), ds(None, schema_constants.BYTES), ds(b' foo')),
      (ds('foo'), ds('o'), ds('f')),
      (ds(' foo  '), ds('o '), ds(' f')),
      (ds(b'foo'), ds(b'o'), ds(b'f')),
      (ds(b' foo  '), ds(b'o '), ds(b' f')),
      (
          ds(['foo', 'zoo', 'boar', None]),
          ds('o'),
          ds(['f', 'z', 'boar', None]),
      ),
      (
          ds([b'foo', b'zoo', b'boar', None]),
          ds(b'o'),
          ds([b'f', b'z', b'boar', None]),
      ),
      (
          ds([['foo', 'bzoo'], ['bari']]),
          ds(['o', 'i']),
          ds([['f', 'bz'], ['bar']]),
      ),
      (
          ds(['foo'], schema_constants.ANY),
          ds('f'),
          ds(['foo'], schema_constants.ANY),
      ),
      (ds(['www.example.com']), ds(['cmowz.']), ds(['www.example'])),
      (
          ds([['#... Section 3.1 Issue #32 ...'], ['# ...']]),
          ds('.#! '),
          ds([['#... Section 3.1 Issue #32'], ['']]),
      ),
      # Empty and unknown.
      (
          ds([None, None]),
          ds([None, None]),
          ds([None, None]),
      ),
      (
          ds([None, None], schema_constants.TEXT),
          ds(None, schema_constants.TEXT),
          ds([None, None], schema_constants.TEXT),
      ),
      (
          ds([None, None], schema_constants.BYTES),
          ds(None, schema_constants.BYTES),
          ds([None, None], schema_constants.BYTES),
      ),
      (
          ds([None, None], schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None], schema_constants.OBJECT),
          ds(None, schema_constants.TEXT),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None], schema_constants.TEXT),
          ds(None, schema_constants.OBJECT),
          ds([None, None], schema_constants.OBJECT),
      ),
      (ds([None, None]), ds('abc'), ds([None, None], schema_constants.TEXT)),
  )
  def test_two_args(self, s, chars, expected):
    result = expr_eval.eval(kde.strings.rstrip(s, chars))
    testing.assert_equal(result, expected)

  def test_incompatible_types(self):
    with self.assertRaisesRegex(
        ValueError,
        # TODO: Make Koda errors user friendly.
        re.escape('unsupported argument types (TEXT,BYTES)'),
    ):
      expr_eval.eval(kde.strings.rstrip(ds('foo'), ds(b'f')))

    with self.assertRaisesRegex(
        ValueError,
        # TODO: Make Koda errors user friendly.
        re.escape(
            'expected `chars` to be bytes, text, or unspecified, got chars:'
            ' INT32'
        ),
    ):
      expr_eval.eval(
          kde.strings.rstrip(ds([None], schema_constants.TEXT), ds(123))
      )

    with self.assertRaisesRegex(
        ValueError,
        # TODO: Make Koda errors user friendly.
        re.escape(
            'expected texts/byteses or corresponding array, got s:'
            ' OPTIONAL_INT32'
        ),
    ):
      expr_eval.eval(kde.strings.rstrip(None, 123))

  def test_mixed_slice_error(self):
    with self.assertRaisesRegex(
        ValueError, 'DataSlice with mixed types is not supported'
    ):
      expr_eval.eval(kde.strings.rstrip(ds('foo'), ds(['fo', 123])))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.rstrip,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.strings.rstrip(I.a)))


if __name__ == '__main__':
  absltest.main()
