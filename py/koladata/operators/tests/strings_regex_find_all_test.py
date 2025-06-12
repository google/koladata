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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class StringsRegexFindAllTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('foo'), ds('oo'), ds([[]], schema_constants.STRING)),
      (ds('foo'), ds('o'), ds([[], []], schema_constants.STRING)),
      (
          ds('foo'),
          ds('zoo'),
          ds([], schema_constants.STRING).reshape(create_shape(0, [])),
      ),
      (ds('foo'), ds('f(.)'), ds([['o']])),
      (ds('foo'), ds('(.)'), ds([['f'], ['o'], ['o']])),
      (ds('foobar'), ds('(.)(.)'), ds([['f', 'o'], ['o', 'b'], ['a', 'r']])),
      (
          ds('foobar'),
          ds('((.)(.))'),
          ds([['fo', 'f', 'o'], ['ob', 'o', 'b'], ['ar', 'a', 'r']]),
      ),
      (
          ds(['foobar', 'jazz']),
          ds('(.)(.)'),
          ds([[['f', 'o'], ['o', 'b'], ['a', 'r']], [['j', 'a'], ['z', 'z']]]),
      ),
      (
          # This example is shown in the docstring of kd.strings.regex_find_all:
          ds(['fooz', 'bar', '', None]),
          ds('(.)(.)'),
          ds([[['f', 'o'], ['o', 'z']], [['b', 'a']], [], []]),
      ),
      (ds(['foobar', 'atubap']), ds('^.(.*).$'), ds([[['ooba']], [['tuba']]])),
      (
          ds([['fool', 'solo'], ['bar', 'boat']]),
          ds('((.*)o)'),
          ds([[[['foo', 'fo']], [['solo', 'sol']]], [[], [['bo', 'b']]]]),
      ),
      (
          ds('baabaa', schema_constants.OBJECT),
          ds('(.aa)'),
          ds([['baa'], ['baa']], schema_constants.OBJECT),
      ),
      (
          ds('baabaa'),
          ds('(.aa)', schema_constants.OBJECT),
          ds([['baa'], ['baa']]),
      ),
      # Empty and unknown.
      (
          ds([None, None], schema_constants.OBJECT),
          ds('abc'),
          ds([], schema_constants.OBJECT).reshape(create_shape(2, 0, [])),
      ),
      (ds([None, None]), ds('abc'), ds([]).reshape(create_shape(2, 0, []))),
  )
  def test_eval(self, text, regex, expected):
    result = expr_eval.eval(kde.strings.regex_find_all(text, regex))
    testing.assert_equal(result, expected)

  def test_incompatible_types_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'argument `regex` must be an item holding STRING, got an item of BYTES',
    ):
      expr_eval.eval(kde.strings.regex_find_all(ds('foo'), ds(b'f')))

    with self.assertRaisesRegex(
        ValueError,
        'argument `regex` must be an item holding STRING, got an item of INT32',
    ):
      expr_eval.eval(
          kde.strings.regex_find_all(
              ds([None], schema_constants.STRING), ds(123)
          )
      )

    with self.assertRaisesRegex(
        ValueError,
        'argument `regex` must be an item holding STRING, got missing',
    ):
      expr_eval.eval(
          kde.strings.regex_find_all(
              ds(['foo']), ds(None, schema_constants.STRING)
          )
      )

  def test_mixed_slice_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'argument `text` must be a slice of STRING, got a slice of OBJECT'
        ' containing INT32 and STRING values',
    ):
      expr_eval.eval(kde.strings.regex_find_all(ds([1, 'fo']), ds('foo')))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.regex_find_all,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.strings.regex_find_all(I.x, I.y)))


if __name__ == '__main__':
  absltest.main()
