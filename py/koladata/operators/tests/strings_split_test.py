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
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import literal_operator
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


class StringsSplitTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(['Hello world!']), ds([['Hello', 'world!']])),
      (ds([' ']), ds([[]], schema_constants.TEXT)),
      (ds([], schema_constants.TEXT), ds([], schema_constants.TEXT).repeat(0)),
      (ds([b'Hello world!']), ds([[b'Hello', b'world!']])),
      (ds([b'Hello world!']), ds([[b'Hello', b'world!']])),
      (ds('Hello world!'), ds(['Hello', 'world!'])),
      (
          ds([
              ['Hello world!', 'Greetings world!'],
              ['Goodbye world!', 'Farewell world!'],
          ]),
          ds([
              [['Hello', 'world!'], ['Greetings', 'world!']],
              [['Goodbye', 'world!'], ['Farewell', 'world!']],
          ]),
      ),
      # OBJECT/ANY
      (
          ds([['a b'], [None]], schema_constants.OBJECT),
          ds([[['a', 'b']], [[]]], schema_constants.OBJECT),
      ),
      (
          ds([[b'a b'], [None]], schema_constants.OBJECT),
          ds([[[b'a', b'b']], [[]]], schema_constants.OBJECT),
      ),
      # Empty and unknown inputs.
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          ds([[[], []], [[]]], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]]),
          ds([[[], []], [[]]], schema_constants.NONE),
      ),
      (
          ds(None),
          ds([], schema_constants.NONE),
      ),
      (
          ds([[None, None], [None]], schema_constants.TEXT),
          ds([[[], []], [[]]], schema_constants.TEXT),
      ),
      (
          ds([[None, None], [None]], schema_constants.BYTES),
          ds([[[], []], [[]]], schema_constants.BYTES),
      ),
      (
          ds([[None, None], [None]], schema_constants.ANY),
          ds([[[], []], [[]]], schema_constants.ANY),
      ),
  )
  def test_eval_one_arg(self, x, expected_value):
    actual_value = expr_eval.eval(kde.strings.split(x))
    testing.assert_equal(actual_value, expected_value)

  @parameterized.parameters(
      (ds(['Hello world!']), ds(None), ds([['Hello', 'world!']])),
      (ds(['Hello world!']), ds(None), ds([['Hello', 'world!']])),
      (
          ds(['Hello world!']),
          ds(None, schema_constants.TEXT),
          ds([['Hello', 'world!']]),
      ),
      (ds(['Hello world!']), ds('world'), ds([['Hello ', '!']])),
      (ds(['']), ds('ab'), ds([['']])),
      (ds([b'Byte split']), ds(b'sp'), ds([[b'Byte ', b'lit']])),
      (
          ds(['splits seven syllables by `s`']),
          ds('s'),
          ds([['', 'plit', ' ', 'even ', 'yllable', ' by `', '`']]),
      ),
      # OBJECT/ANY
      (
          ds([['a b', 'b c'], [None]], schema_constants.OBJECT),
          ds(' '),
          ds([[['a', 'b'], ['b', 'c']], [[]]], schema_constants.OBJECT),
      ),
      (
          ds([[b'a b', b'b c'], [None]], schema_constants.ANY),
          ds(b' '),
          ds([[[b'a', b'b'], [b'b', b'c']], [[]]], schema_constants.ANY),
      ),
      (
          ds([['a b', 'b c'], [None]]),
          ds(' ', schema_constants.OBJECT),
          ds([[['a', 'b'], ['b', 'c']], [[]]], schema_constants.OBJECT),
      ),
      (
          ds([[b'a b', b'b c'], [None]]),
          ds(b' ', schema_constants.ANY),
          ds([[[b'a', b'b'], [b'b', b'c']], [[]]], schema_constants.ANY),
      ),
      # Empty and unknown inputs.
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          ds(' '),
          ds([[[], []], [[]]], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]]),
          ds(' '),
          ds([[[], []], [[]]], schema_constants.TEXT),
      ),
      (
          ds([[None, None], [None]], schema_constants.TEXT),
          ds(' ', schema_constants.OBJECT),
          ds([[[], []], [[]]], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]], schema_constants.BYTES),
          ds(b' ', schema_constants.OBJECT),
          ds([[[], []], [[]]], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]], schema_constants.ANY),
          ds(' ', schema_constants.OBJECT),
          ds([[[], []], [[]]], schema_constants.ANY),
      ),
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          ds(None, schema_constants.ANY),
          ds([[[], []], [[]]], schema_constants.ANY),
      ),
  )
  def test_eval_two_args(self, x, sep, expected_value):
    actual_value = expr_eval.eval(kde.strings.split(x, sep))
    testing.assert_equal(actual_value, expected_value)

  def test_boxing(self):
    testing.assert_equal(
        kde.strings.split('hello'),
        arolla.abc.bind_op(
            kde.strings.split,
            literal_operator.literal(data_slice.DataSlice.from_vals('hello')),
            literal_operator.literal(data_slice.DataSlice.from_vals(None)),
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.strings.split,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr_without_separator(self):
    self.assertEqual(
        repr(kde.strings.split(I.x)),
        'kde.strings.split(I.x, DataItem(None, schema: NONE))',
    )

  def test_repr_with_separator(self):
    self.assertEqual(
        repr(kde.strings.split(I.x, I.sep)), 'kde.strings.split(I.x, I.sep)'
    )

  def test_data_slice_sep_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected rank(sep) == 0')
    ):
      expr_eval.eval(kde.strings.split(ds(['foo', 'bar']), sep=ds(['', ''])))

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.strings.split(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.strings.split, kde.split))


if __name__ == '__main__':
  absltest.main()
