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
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class StringsReplaceTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('fooaaoo'), ds('oo'), ds('i'), ds('fiaai')),
      (ds('fooaaoo'), ds(''), ds('-'), ds('-f-o-o-a-a-o-o-')),
      (ds(b'fooaaoo'), ds(b'oo'), ds(b'i'), ds(b'fiaai')),
      (
          ds(['fooaaoo', 'zoo', 'bar']),
          ds('oo'),
          ds('e'),
          ds(['feaae', 'ze', 'bar']),
      ),
      (
          ds(['fooaaoo', 'zoo', 'bar']),
          ds(['oo', 'z', None]),
          ds(['e', 'k', 'cc']),
          ds(['feaae', 'koo', None]),
      ),
      (
          ds([['fo', 'bzboo'], ['barioooooi']]),
          ds(['oo', 'i']),
          ds(['e', 'z']),
          ds([['fo', 'bzbe'], ['barzoooooz']]),
      ),
      (
          ds('foo'),
          ds('o'),
          ds(None),
          ds(None, schema_constants.STRING),
      ),
      (
          ds(['foo'], schema_constants.ANY),
          ds('f'),
          ds('z'),
          ds(['zoo'], schema_constants.ANY),
      ),
      # Empty and unknown.
      (
          ds([None, None]),
          ds([None, None]),
          ds([None, None]),
          ds([None, None]),
      ),
      (
          ds([None, None], schema_constants.STRING),
          ds(None, schema_constants.STRING),
          ds(None, schema_constants.STRING),
          ds([None, None], schema_constants.STRING),
      ),
      (
          ds([None, None], schema_constants.BYTES),
          ds(None, schema_constants.BYTES),
          ds(None, schema_constants.BYTES),
          ds([None, None], schema_constants.BYTES),
      ),
      (
          ds([None, None], schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None]),
          ds('abc'),
          ds('def'),
          ds([None, None]),
      ),
  )
  def test_three_args(self, s, substr, start, expected):
    result = expr_eval.eval(kde.strings.replace(s, substr, start))
    testing.assert_equal(result, expected)

  def test_three_args_wrong_types(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.strings.replace: mixing STRING and BYTES arguments is not'
            ' allowed, but `s` contains STRING and `old_substr` contains BYTES'
        ),
    ):
      expr_eval.eval(kde.strings.replace('foo', b'oo', 'ar'))

  @parameterized.parameters(
      (ds('fooaaoo'), ds('oo'), ds('e'), ds(0), ds('fooaaoo')),
      (ds('fooaaoo'), ds('oo'), ds('e'), ds(1), ds('feaaoo')),
      (ds('fooaaoo'), ds('oo'), ds('e'), ds(2), ds('feaae')),
      (ds('fooaaoo'), ds(''), ds('-'), ds(2), ds('-f-ooaaoo')),
      (ds('fooaaoo'), ds(''), ds('-'), ds(-2), ds('-f-o-o-a-a-o-o-')),
      (ds('fooaaoo'), ds(''), ds('-'), ds(-1), ds('-f-o-o-a-a-o-o-')),
      (
          ds(['fooaaoo', 'zbboo', 'oobaroo']),
          ds('oo'),
          ds('X'),
          ds([1, 2, 2]),
          ds(['fXaaoo', 'zbbX', 'XbarX']),
      ),
      (
          ds([['foo', 'bzboo', 'bzbooa'], ['barioooooi']]),
          ds(['o', 'i']),
          ds(['X', 'Y']),
          ds([1, None]),
          ds([['fXo', 'bzbXo', 'bzbXoa'], ['barYoooooY']]),
      ),
      # Empty and unknown.
      (
          ds([None, None]),
          ds([None, None]),
          ds([None, None]),
          ds([None, None]),
          ds([None, None]),
      ),
      (
          ds([None, None], schema_constants.STRING),
          ds(None, schema_constants.STRING),
          ds(None, schema_constants.STRING),
          ds(None, schema_constants.INT32),
          ds([None, None], schema_constants.STRING),
      ),
      (
          ds([None, None], schema_constants.BYTES),
          ds(None, schema_constants.BYTES),
          ds(None, schema_constants.BYTES),
          ds(None, schema_constants.INT32),
          ds([None, None], schema_constants.BYTES),
      ),
      (
          ds([None, None], schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None], schema_constants.ANY),
          ds(None, schema_constants.ANY),
          ds(None, schema_constants.ANY),
          ds(None, schema_constants.INT32),
          ds([None, None], schema_constants.ANY),
      ),
      (
          ds([None, None]),
          ds('abc'),
          ds('b'),
          ds(2),
          ds([None, None]),
      ),
  )
  def test_four_args(self, s, substr, start, end, expected):
    result = expr_eval.eval(kde.strings.replace(s, substr, start, end))
    testing.assert_equal(result, expected)

  def test_incompatible_types_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.strings.replace: mixing STRING and BYTES arguments is not'
            ' allowed, but `s` contains STRING and `old_substr` contains BYTES'
        ),
    ):
      expr_eval.eval(kde.strings.replace(ds('foo'), ds(b'f'), ds('bar')))

  def test_another_incompatible_types_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.strings.replace: argument `old_substr` must be a slice of'
            ' either STRING or BYTES, got a slice of INT32'
        ),
    ):
      expr_eval.eval(
          kde.strings.replace(
              ds([None], schema_constants.STRING), ds(123), ds('bar')
          )
      )

  def test_mixed_slice_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.strings.replace: argument `old_substr` must be a slice of either'
        ' STRING or BYTES, got a slice of OBJECT containing INT32 and STRING'
        ' values',
    ):
      expr_eval.eval(kde.strings.replace(ds('foo'), ds([1, 'fo']), ds('bar')))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.replace,
        QTYPES,
        # Limit the allowed qtypes to speed up the test.
        possible_qtypes=(
            arolla.UNSPECIFIED,
            qtypes.DATA_SLICE,
            arolla.INT64,
            arolla.BYTES,
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.strings.replace(I.a, I.b, I.c)))


if __name__ == '__main__':
  absltest.main()
