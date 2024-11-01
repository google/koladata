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
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    # (x, return_type):
    (DATA_SLICE, DATA_SLICE),
    # (x, start, return_type):
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    # (x, start, end, return_type):
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class StringsSubstrTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([['Hello World!', 'Ciao bella'], ['Dolly!']]),
          ds([['Hello World!', 'Ciao bella'], ['Dolly!']]),
      ),
      (
          ds([[b'Hello World!', b'Ciao bella'], [b'Dolly!']]),
          ds([[b'Hello World!', b'Ciao bella'], [b'Dolly!']]),
      ),
      (
          ds([[None, b'Ciao bella'], [None]]),
          ds([[None, b'Ciao bella'], [None]]),
      ),
      (
          ds([[None, b'Ciao bella'], [None]], schema_constants.OBJECT),
          ds([[None, b'Ciao bella'], [None]], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]]),
          ds([[None, None], [None]]),
      ),
      (
          ds([[None, None], [None]], schema_constants.STRING),
          ds([[None, None], [None]], schema_constants.STRING),
      ),
      (
          ds([[None, None], [None]], schema_constants.ANY),
          ds([[None, None], [None]], schema_constants.ANY),
      ),
  )
  def test_eval_one_arg(self, x, expected_value):
    actual_value = expr_eval.eval(kde.strings.substr(x))
    testing.assert_equal(actual_value, expected_value)

  @parameterized.parameters(
      (
          ds([['Hello World!', 'Ciao bella'], ['Dolly!']]),
          ds(1),
          ds([['ello World!', 'iao bella'], ['olly!']]),
      ),
      (
          ds([[b'Hello World!', b'Ciao bella'], [b'Dolly!']]),
          ds(3),
          ds([[b'lo World!', b'o bella'], [b'ly!']]),
      ),
      (
          ds([[b'Hello World!', b'Ciao bella'], [b'Dolly!']]),
          ds(5),
          ds([[b' World!', b'bella'], [b'!']]),
      ),
      (
          ds([['Hello World!', 'Ciao bella'], ['Dolly!']]),
          ds(-2),
          ds([['d!', 'la'], ['y!']]),
      ),
      (
          ds('Hello World!'),
          ds(1),
          ds('ello World!'),
      ),
      (
          ds('Hello World!'),
          ds([1, 3, None]),
          ds(['ello World!', 'lo World!', 'Hello World!']),
      ),
      (
          ds([[b'Hello World!', b'Ciao bella'], [None]]),
          ds([1, None]),
          ds([[b'ello World!', b'iao bella'], [None]]),
      ),
      (
          ds([[b'Hello World!', b'Ciao bella'], [None]]),
          ds([None, None]),
          ds([[b'Hello World!', b'Ciao bella'], [None]]),
      ),
      (
          ds([[b'Hello World!', b'Ciao bella'], [None]]),
          ds([None, None], schema_constants.OBJECT),
          ds([[b'Hello World!', b'Ciao bella'], [None]]),
      ),
      (
          ds([[None, None], [None]]),
          ds([1, 2]),
          ds([[None, None], [None]]),
      ),
      (
          ds([[None, None], [None]], schema_constants.ANY),
          ds([1, 2]),
          ds([[None, None], [None]], schema_constants.ANY),
      ),
  )
  def test_eval_two_args(self, x, start, expected_value):
    actual_value = expr_eval.eval(kde.strings.substr(x, start))
    testing.assert_equal(actual_value, expected_value)

  @parameterized.parameters(
      (
          ds([['Hello World!', 'Ciao bella'], ['Dolly!']]),
          ds(1),
          ds(5),
          ds([['ello', 'iao '], ['olly']]),
      ),
      (
          ds([[b'Hello World!', b'Ciao bella'], [b'Dolly!']]),
          ds(1),
          ds(5),
          ds([[b'ello', b'iao '], [b'olly']]),
      ),
      (
          ds([['Hello World!', 'Ciao bella'], ['Dolly!']]),
          ds(5),
          ds(-1),
          ds([[' World', 'bell'], ['']]),
      ),
      (
          ds([['Hello World!', 'Ciao bella'], ['Dolly!']]),
          ds(4),
          ds(100),
          ds([['o World!', ' bella'], ['y!']]),
      ),
      (
          ds([['Hello World!', 'Ciao bella'], ['Dolly!']]),
          ds(-1),
          ds(-2),
          ds([['', ''], ['']]),
      ),
      (
          ds([['Hello World!', 'Ciao bella'], ['Dolly!']]),
          ds(-2),
          ds(-1),
          ds([['d', 'l'], ['y']]),
      ),
      (
          ds('Hello World!'),
          ds(1),
          ds(3),
          ds('el'),
      ),
      (
          ds('Hello World!'),
          ds([1, 3, None]),
          ds(3),
          ds(['el', '', 'Hel']),
      ),
      (
          ds([[b'Hello World!', b'Ciao bella'], [None]]),
          ds([1, None]),
          ds(3),
          ds([[b'el', b'ia'], [None]]),
      ),
      (
          ds([[b'Hello World!', b'Ciao bella'], [None]]),
          ds([None, None]),
          ds(3),
          ds([[b'Hel', b'Cia'], [None]]),
      ),
      (
          ds([[b'Hello World!', b'Ciao bella'], [None]]),
          ds([None, None]),
          ds(None),
          ds([[b'Hello World!', b'Ciao bella'], [None]]),
      ),
      (
          ds([[b'Hello World!', b'Ciao bella'], [None]]),
          ds([None, None], schema_constants.OBJECT),
          ds([None, None], schema_constants.ANY),
          ds([[b'Hello World!', b'Ciao bella'], [None]]),
      ),
      (
          ds([[None, None], [None]]),
          ds([1, 2]),
          ds([3, 4]),
          ds([[None, None], [None]]),
      ),
      (
          ds([[None, None], [None]], schema_constants.ANY),
          ds([1, 2]),
          ds([3, 4]),
          ds([[None, None], [None]], schema_constants.ANY),
      ),
      (
          ds([[None, None], [None]]),
          ds([None, None]),
          ds([None, None]),
          ds([[None, None], [None]]),
      ),
  )
  def test_eval_three_args(self, x, start, end, expected_value):
    actual_value = expr_eval.eval(kde.strings.substr(x, start, end))
    testing.assert_equal(actual_value, expected_value)

  def test_eval_three_args_wrong_types(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'unsupported narrowing cast to INT64 for the given TEXT DataSlice'
        ),
    ):
      expr_eval.eval(kde.strings.substr('meeting', start=1, end='foo'))

  def test_boxing(self):
    testing.assert_equal(
        kde.strings.substr('hello', 1, 3),
        arolla.abc.bind_op(
            kde.strings.substr,
            literal_operator.literal(data_slice.DataSlice.from_vals('hello')),
            literal_operator.literal(data_slice.DataSlice.from_vals(1)),
            literal_operator.literal(data_slice.DataSlice.from_vals(3)),
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.strings.substr,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_entity_slice_error(self):
    db = data_bag.DataBag.empty()
    x = db.new(x=ds([1]))
    with self.assertRaisesRegex(
        exceptions.KodaError, 'DataSlice with Entity schema is not supported'
    ):
      expr_eval.eval(kde.strings.substr(x))

  def test_repr(self):
    self.assertEqual(
        repr(kde.strings.substr(I.x, I.start, I.end)),
        'kde.strings.substr(I.x, I.start, I.end)',
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.strings.substr(I.x)))
    self.assertTrue(view.has_data_slice_view(kde.strings.substr(I.x, I.start)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.strings.substr, kde.substr))


if __name__ == '__main__':
  absltest.main()
