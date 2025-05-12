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
from koladata.types import jagged_shape
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
create_shape = jagged_shape.create_shape
missing = arolla.missing()

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class StringsRegexReplaceAllTest(parameterized.TestCase):

  @parameterized.parameters(
      # Basic:
      (ds('foopo'), ds('oo'), ds('aa'), ds('faapo')),
      (ds('foopo'), ds('o'), ds('a'), ds('faapa')),
      # With non-scalar text/replacement arguments:
      (ds('foopo'), ds('o'), ds(['a', 'e']), ds(['faapa', 'feepe'])),
      (ds(['foopo', 'foo']), ds('o'), ds('a'), ds(['faapa', 'faa'])),
      (ds(['foopo', 'foo']), ds('o'), ds(['a', 'i']), ds(['faapa', 'fii'])),
      # No matches:
      (ds('foopo'), ds('x'), ds('a'), ds('foopo')),
      (ds('foopo'), ds(r'\d'), ds('a', schema_constants.STRING), ds('foopo')),
      (ds(['foopo', 'foo']), ds('x'), ds(['a', 'i']), ds(['foopo', 'foo'])),
      # Reference capturing groups in the replacement:
      (ds('abcd'), ds('(.)(.)'), ds(r'\2\1'), ds('badc')),
      (ds('abcd'), ds('(.)(.)'), ds(r'\2\1\0'), ds('baabdccd')),
      (
          ds(['foobor', 'foo', None, 'bar']),
          ds('o(.)'),
          ds([r'\0x\1', 'ly', 'a', 'o']),
          ds(['fooxoborxr', 'fly', None, 'bar']),
      ),
      (
          ds(['foobar', 'atubap']),
          ds('^(.)(.*)(.)$'),
          ds(r'\0, no, \3\2\1'),
          ds(['foobar, no, roobaf', 'atubap, no, ptubaa']),
      ),
      # With missing text/replacement arguments:
      (
          ds('foopo'),
          ds('o'),
          ds(None, schema_constants.STRING),
          ds(None, schema_constants.STRING),
      ),
      (
          ds(None, schema_constants.STRING),
          ds('o'),
          ds('a'),
          ds(None, schema_constants.STRING),
      ),
      (
          ds('abcdef'),
          ds('abc'),
          ds([None, None], schema_constants.OBJECT),
          ds([None, None], schema_constants.STRING),
      ),
      (
          ds('abcdef'),
          ds('abc'),
          ds([None, None]),
          ds([None, None], schema_constants.STRING),
      ),
      # OBJECT schemas:
      (
          ds('baabaa', schema_constants.OBJECT),
          ds('(.)aa'),
          ds(r'\1oo'),
          ds('booboo', schema_constants.OBJECT),
      ),
      (
          ds('baabaa'),
          ds('(.)aa', schema_constants.OBJECT),
          ds(r'\1oo'),
          ds('booboo'),
      ),
      # Empty and unknown:
      (
          ds([None, None], schema_constants.OBJECT),
          ds('abc'),
          ds('def'),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([None, None]),
          ds('abc'),
          ds('def'),
          ds([None, None]),
      ),
  )
  def test_eval(self, text, regex, replacement, expected):
    result = expr_eval.eval(
        kde.strings.regex_replace_all(text, regex, replacement)
    )
    testing.assert_equal(result, expected)

  def test_regex_incompatible_type_errors(self):
    # TODO: The error messages here are not very good. See
    # kd.strings.regex_find_all for ideas on how to improve them.

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'unsupported narrowing cast to STRING for the given BYTES DataSlice'
        ),
    ):
      expr_eval.eval(
          kde.strings.regex_replace_all(ds('foo'), ds(b'f'), ds('b'))
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'unsupported narrowing cast to STRING for the given INT32 DataSlice'
        ),
    ):
      expr_eval.eval(
          kde.strings.regex_replace_all(
              ds([None], schema_constants.STRING),
              ds(123),
              ds(None, schema_constants.STRING),
          )
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a present value'),
    ):
      expr_eval.eval(
          kde.strings.regex_replace_all(
              ds(['foo']), ds(None, schema_constants.STRING), ds('a')
          )
      )

  def test_mixed_slice_errors(self):
    # TODO: The error messages here are not very good. See
    # kd.strings.regex_find_all for ideas on how to improve them.

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'unsupported narrowing cast to STRING for the given OBJECT'
            ' DataSlice'
        ),
    ):
      expr_eval.eval(
          kde.strings.regex_replace_all(ds([1, 'fo']), ds('o'), ds('a'))
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'unsupported narrowing cast to STRING for the given OBJECT'
            ' DataSlice'
        ),
    ):
      expr_eval.eval(
          kde.strings.regex_replace_all(ds('foo'), ds('o'), ds([1, 'fo']))
      )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.regex_replace_all,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.strings.regex_replace_all(I.x, I.y, I.z))
    )


if __name__ == '__main__':
  absltest.main()
