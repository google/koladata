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

present = arolla.present()
missing = arolla.missing()

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class StringsRegexExtractTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('foo'), ds('f(.)'), ds('o')),
      (ds('foobar'), ds('o(..)'), ds('ob')),
      (ds('foobar'), ds('^o(..)$'), ds(None, schema_constants.STRING)),
      (ds('foobar'), ds('^.o(..)a.$'), ds('ob')),
      ('foobar', '.*(b.*r)$', ds('bar')),
      (ds(['foo', 'zoo', 'bar']), ds('(.)o'), ds(['f', 'z', None])),
      (
          ds([['fool', 'solo'], ['bar', 'boat']]),
          ds('.*o(.*)'),
          ds([['l', ''], [None, 'at']]),
      ),
      (
          ds(['foo'], schema_constants.OBJECT),
          ds('(.o)'),
          ds(['fo'], schema_constants.OBJECT),
      ),
      (ds(['abcd', None, '']), ds('b(.*)'), ds(['cd', None, None])),
      # Empty and unknown.
      (
          ds([None, None], schema_constants.OBJECT),
          ds('abc'),
          ds([None, None], schema_constants.OBJECT),
      ),
      (ds([None, None]), ds('abc'), ds([None, None])),
  )
  def test_eval(self, text, regex, expected):
    result = expr_eval.eval(kde.strings.regex_extract(text, regex))
    testing.assert_equal(result, expected)

  def test_regex_without_capturing_group_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'ExtractRegexOp expected regular expression with exactly one'
            ' capturing group; got `foo` which contains 0 capturing groups'
        ),
    ):
      expr_eval.eval(kde.strings.regex_extract(ds('foo'), ds('foo')))

  def test_regex_with_multiple_capturing_groups_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'ExtractRegexOp expected regular expression with exactly one'
            ' capturing group; got `(.)(.)` which contains 2 capturing groups'
        ),
    ):
      expr_eval.eval(kde.strings.regex_extract(ds('foo'), ds('(.)(.)')))

  def test_incompatible_types_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `regex` must be an item holding STRING, got an item of'
            ' BYTES'
        ),
    ):
      expr_eval.eval(kde.strings.regex_extract(ds('foo'), ds(b'f')))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `regex` must be an item holding STRING, got an item of'
            ' INT32'
        ),
    ):
      expr_eval.eval(
          kde.strings.regex_extract(
              ds([None], schema_constants.STRING), ds(123)
          )
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `regex` must be an item holding STRING, got missing'
        ),
    ):
      expr_eval.eval(
          kde.strings.regex_extract(
              ds(['foo']), ds(None, schema_constants.STRING)
          )
      )

  def test_mixed_slice_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'kd.strings.regex_extract: argument `text` must be a slice of STRING,'
        ' got a slice of OBJECT containing INT32 and STRING values',
    ):
      expr_eval.eval(kde.strings.regex_extract(ds([1, 'fo']), ds('foo')))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.regex_extract,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.strings.regex_extract(I.x, I.y)))


if __name__ == '__main__':
  absltest.main()
