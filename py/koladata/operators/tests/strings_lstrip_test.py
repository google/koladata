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
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class StringsLstripTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('foo'), ds('foo')),
      (ds(' fo o  '), ds('fo o  ')),
      (ds(b'foo'), ds(b'foo')),
      (ds(b' foo   '), ds(b'foo   ')),
      (ds(['\tfoo \n', 'zoo ', '\nbar']), ds(['foo \n', 'zoo ', 'bar'])),
      (ds([b'\tfoo \n', b'zoo ', b'\nbar']), ds([b'foo \n', b'zoo ', b'bar'])),
      (
          ds([[' foo', 'bzoo '], ['  bari  \t']]),
          ds([['foo', 'bzoo '], ['bari  \t']]),
      ),
      (ds([' foo '], schema_constants.ANY), ds(['foo '], schema_constants.ANY)),
      (ds(['   spacious   ', '\t text \n']), ds(['spacious   ', 'text \n'])),
      # Empty and unknown.
      (ds([None, None]), ds([None, None])),
      (
          ds([None, None], schema_constants.STRING),
          ds([None, None], schema_constants.STRING),
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
    result = expr_eval.eval(kde.strings.lstrip(s))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      # 2nd operand is missing.
      (ds('foo'), None, ds('foo')),
      (ds(' fo o  '), None, ds('fo o  ')),
      (ds(b'foo'), None, ds(b'foo')),
      (ds(b' foo   '), ds(None), ds(b'foo   ')),
      # 2nd operand is present.
      (ds(' fo o  '), ds(None, schema_constants.STRING), ds('fo o  ')),
      (ds(b' foo  '), ds(None, schema_constants.BYTES), ds(b'foo  ')),
      (ds('foo'), ds('o'), ds('foo')),
      (ds(' foo  '), ds('f '), ds('oo  ')),
      (ds(b'foo'), ds(b'f'), ds(b'oo')),
      (ds(b' f foo  '), ds(b'f '), ds(b'oo  ')),
      (
          ds(['oofo', 'ooz', 'boar', None]),
          ds('o'),
          ds(['fo', 'z', 'boar', None]),
      ),
      (
          ds([b'oofo', b'ooz', b'boar', None]),
          ds(b'o'),
          ds([b'fo', b'z', b'boar', None]),
      ),
      (
          ds([['oof', 'oobz'], ['ibari']]),
          ds(['o', 'i']),
          ds([['f', 'bz'], ['bari']]),
      ),
      (
          ds(['foo'], schema_constants.ANY),
          ds('f'),
          ds(['oo'], schema_constants.ANY),
      ),
      (ds(['www.example.com']), ds(['cmowz.']), ds(['example.com'])),
      (
          ds([['#... Section 3.1 Issue #32 ...'], ['# ...']]),
          ds('.#! '),
          ds([['Section 3.1 Issue #32 ...'], ['']]),
      ),
      # Empty and unknown.
      (
          ds([None, None]),
          ds([None, None]),
          ds([None, None]),
      ),
      (
          ds([None, None], schema_constants.STRING),
          ds(None, schema_constants.STRING),
          ds([None, None], schema_constants.STRING),
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
          ds(None, schema_constants.STRING),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None], schema_constants.STRING),
          ds(None, schema_constants.OBJECT),
          ds([None, None], schema_constants.OBJECT),
      ),
      (ds([None, None]), ds('abc'), ds([None, None], schema_constants.STRING)),
  )
  def test_two_args(self, s, chars, expected):
    result = expr_eval.eval(kde.strings.lstrip(s, chars))
    testing.assert_equal(result, expected)

  def test_incompatible_types(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'mixing STRING and BYTES arguments is not allowed, but `s` contains'
            ' STRING and `chars` contains BYTES'
        ),
    ):
      expr_eval.eval(kde.strings.lstrip(ds('foo'), ds(b'f')))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.strings.lstrip: argument `chars` must be a slice of either'
            ' STRING or BYTES, got a slice of INT32'
        ),
    ):
      expr_eval.eval(
          kde.strings.lstrip(ds([None], schema_constants.STRING), ds(123))
      )

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.strings.lstrip: argument `chars` must be a slice of either'
            ' STRING or BYTES, got a slice of INT32'
        ),
    ):
      expr_eval.eval(kde.strings.lstrip(None, 123))

  def test_mixed_slice_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.strings.lstrip: argument `chars` must be a slice of either STRING'
        ' or BYTES, got a slice of OBJECT containing INT32 and STRING values',
    ):
      expr_eval.eval(kde.strings.lstrip(ds('foo'), ds(['fo', 123])))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.lstrip,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.strings.lstrip(I.a)))


if __name__ == '__main__':
  absltest.main()
