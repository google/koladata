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

"""Tests for kde.strings.agg_join."""

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
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class StringsAggJoinTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([['aB', 'c'], ['a', 'BD', 'eF'], ['Ef']]),
          ds(['aBc', 'aBDeF', 'Ef']),
      ),
      (ds(['a', 'b', 'c']), ds('abc')),
      (ds([['a ', None], [None, None]]), ds(['a ', None])),
      (
          ds([[b'el', b'psy', b'congroo'], [b'a', b'c']]),
          ds([b'elpsycongroo', b'ac']),
      ),
      # OBJECT/ANY
      (
          ds([['a', 'b'], [None]], schema_constants.OBJECT),
          ds(['ab', None], schema_constants.OBJECT),
      ),
      (
          ds([[b'a', b'b'], [None]], schema_constants.ANY),
          ds([b'ab', None], schema_constants.ANY),
      ),
      # Empty and unknown inputs.
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]]),
          ds([None, None]),
      ),
      (
          ds([[None, None], [None]], schema_constants.TEXT),
          ds([None, None], schema_constants.TEXT),
      ),
      (
          ds([[None, None], [None]], schema_constants.BYTES),
          ds([None, None], schema_constants.BYTES),
      ),
      (
          ds([[None, None], [None]], schema_constants.ANY),
          ds([None, None], schema_constants.ANY),
      ),
  )
  def test_eval_one_arg(self, x, expected):
    actual_value = expr_eval.eval(kde.strings.agg_join(x))
    testing.assert_equal(actual_value, expected)

  @parameterized.parameters(
      (
          ds([['aB', 'c'], ['a', 'BD', 'eF'], ['Ef']]),
          ds(' '),
          ds(['aB c', 'a BD eF', 'Ef']),
      ),
      (ds([['a', 'b'], ['c'], []]), ds('?'), ds(['a?b', 'c', None])),
      (
          ds([[b'el', b'psy', b'congroo'], [b'a', b'c']]),
          ds(b' '),
          ds([b'el psy congroo', b'a c']),
      ),
      # OBJECT/ANY
      (
          ds([['a', 'b'], [None]], schema_constants.OBJECT),
          ds(' '),
          ds(['a b', None], schema_constants.OBJECT),
      ),
      (
          ds([[b'a', b'b'], [None]], schema_constants.ANY),
          ds(b' '),
          ds([b'a b', None], schema_constants.ANY),
      ),
      (
          ds([['a', 'b'], [None]]),
          ds(' ', schema_constants.OBJECT),
          ds(['a b', None], schema_constants.OBJECT),
      ),
      (
          ds([[b'a', b'b'], [None]]),
          ds(b' ', schema_constants.ANY),
          ds([b'a b', None], schema_constants.ANY),
      ),
      # Empty and unknown inputs.
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          ds(' '),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]]),
          ds(' '),
          ds([None, None], schema_constants.TEXT),
      ),
      (
          ds([[None, None], [None]], schema_constants.TEXT),
          ds(' ', schema_constants.OBJECT),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]], schema_constants.BYTES),
          ds(b' ', schema_constants.OBJECT),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]], schema_constants.ANY),
          ds(' ', schema_constants.OBJECT),
          ds([None, None], schema_constants.ANY),
      ),
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          ds(None, schema_constants.ANY),
          ds([None, None], schema_constants.ANY),
      ),
  )
  def test_eval_two_args(self, x, sep, expected):
    actual_value = expr_eval.eval(kde.strings.agg_join(x, sep=sep))
    testing.assert_equal(actual_value, expected)

  @parameterized.parameters(
      (
          ds([['aB', 'c'], ['a', 'BD', 'eF'], ['Ef']]),
          ds(' '),
          ds(0),
          ds([['aB', 'c'], ['a', 'BD', 'eF'], ['Ef']]),
      ),
      (
          ds([['aB', 'c'], ['a', 'BD', 'eF'], ['Ef']]),
          ds(' '),
          ds(1),
          ds(['aB c', 'a BD eF', 'Ef']),
      ),
      (
          ds([['aB', 'c'], ['a', 'BD', 'eF'], ['Ef']]),
          ds(' '),
          ds(2),
          ds('aB c a BD eF Ef'),
      ),
      (
          ds([[b'aB', b'c'], [b'a', b'BD', b'eF'], [b'Ef']]),
          ds(b' '),
          ds(0),
          ds([[b'aB', b'c'], [b'a', b'BD', b'eF'], [b'Ef']]),
      ),
      (
          ds([[b'aB', b'c'], [b'a', b'BD', b'eF'], [b'Ef']]),
          ds(b' '),
          ds(1),
          ds([b'aB c', b'a BD eF', b'Ef']),
      ),
      (
          ds([[b'aB', b'c'], [b'a', b'BD', b'eF'], [b'Ef']]),
          ds(b' '),
          ds(2),
          ds(b'aB c a BD eF Ef'),
      ),
  )
  def test_eval_three_args(self, x, sep, ndim, expected):
    actual_value = expr_eval.eval(kde.strings.agg_join(x, sep=sep, ndim=ndim))
    testing.assert_equal(actual_value, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.strings.agg_join,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr_without_optional_args(self):
    self.assertEqual(
        repr(kde.strings.agg_join(I.x)),
        'kde.strings.agg_join(I.x, unspecified, unspecified)',
    )

  def test_repr_with_optional_args(self):
    self.assertEqual(
        repr(kde.strings.agg_join(I.x, I.sep, I.ndim)),
        'kde.strings.agg_join(I.x, I.sep, I.ndim)',
    )

  def test_data_item_input_error(self):
    with self.assertRaisesRegex(ValueError, re.escape('expected rank(x) > 0')):
      expr_eval.eval(kde.strings.agg_join(ds(1)))

  def test_data_slice_sep_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected rank(sep) == 0')
    ):
      expr_eval.eval(kde.strings.agg_join(ds(['foo', 'bar']), sep=ds(['', ''])))

  @parameterized.parameters(-1, 2)
  def test_out_of_bounds_ndim_error(self, ndim):
    x = data_slice.DataSlice.from_vals(['a', 'b', 'c'])
    with self.assertRaisesRegex(ValueError, 'expected 0 <= ndim <= rank'):
      expr_eval.eval(kde.strings.agg_join(x, ndim=ndim))

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.strings.agg_join(I.x)))


if __name__ == '__main__':
  absltest.main()
