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

import inspect
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
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
])


class StringsFstrTest(parameterized.TestCase):

  @parameterized.parameters(
      (f'{I.v:s} foo', dict(v=ds('bar')), ds('bar foo')),
      (f'{I.v:s} foo', dict(v=ds(1)), ds('1 foo')),
      (f'{I.v:s} foo', dict(v=ds(None)), ds(None, schema_constants.STRING)),
      (
          f'{I.v:s} foo',
          dict(v=ds(None, schema_constants.OBJECT)),
          ds(None, schema_constants.STRING),
      ),
      # ds(1) ignored.
      (f'{I.v:s} foo', dict(v=ds('bar'), q=ds(1)), ds('bar foo')),
      # 2 args
      (f'{I.v:s} foo {I.q:s}', dict(v=ds('bar'), q=ds(1)), ds('bar foo 1')),
      # used twice
      (
          f'{I.q:s}) {I.v:s} foo {I.q:s}',
          dict(v=ds('bar'), q=ds(1)),
          ds('1) bar foo 1'),
      ),
      (
          f'={I.v:s}',
          dict(v=ds([[[['bar', 'foo']]]])),
          ds([[[['=bar', '=foo']]]]),
      ),
      # Format respect type
      (f'{I.v:03d} != {I.q:02}', dict(v=ds(5), q=ds(7)), ds('005 != 07')),
      (
          f'{I.v:06.3f} != {I.q:07.4}',
          dict(v=ds(5.7), q=ds(7.5)),
          ds('05.700 != 07.5000'),
      ),
      (
          f'{I.v:017} != {I.q:017}',
          dict(v=ds(57.0), q=ds(57)),
          ds('0000000057.000000 != 00000000000000057'),
      ),
  )
  def test_eval(self, fmt, kwargs, expected):
    result = expr_eval.eval(kde.strings.fstr(fmt), **kwargs)
    testing.assert_equal(result, expected)

  def test_eval_float(self):
    testing.assert_equal(
        expr_eval.eval(
            kde.strings.fstr(f'{ds(3.5):0.2} + {ds(2.2):0.3f} = {ds(5.7):s}')
        ),
        ds('3.50 + 2.200 = 5.7'),
    )
    testing.assert_equal(
        expr_eval.eval(kde.strings.fstr(f'{ds(57.0):017} != {ds(57):017}')),
        ds('0000000057.000000 != 00000000000000057'),
    )

  @parameterized.parameters(
      (ds('foo'), ds('foo')),
      (ds(1.0), ds('1.')),
      (ds(1), ds('1')),
      (
          ds(None, schema_constants.OBJECT),
          ds(None, schema_constants.STRING),
      ),
      (ds(['foo', 'bar']), ds(['foo', 'bar'])),
      (ds([1.0, 5.7]), ds(['1.', '5.7'])),
      (ds([1, 2, 3]), ds(['1', '2', '3'])),
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.STRING),
      ),
      # multidimensional
      (
          ds([[[1, 3]], [[2]]]),
          ds([[['1', '3']], [['2']]]),
      ),
      (
          ds([['bar', 'foo'], ['zzz']]),
          ds([['bar', 'foo'], ['zzz']]),
      ),
      # Empty and unknown.
      (ds(None), ds(None, schema_constants.STRING)),
      (ds([None, None]), ds([None, None], schema_constants.STRING)),
      (
          ds([[None, None], [None]]),
          ds([[None, None], [None]], schema_constants.STRING),
      ),
  )
  def test_eval_single_arg_default_spec(self, arg, expected):
    result = expr_eval.eval(kde.strings.fstr(f'{arg:s}'))
    testing.assert_equal(result, expected)

  def test_examples_from_docstring(self):
    greeting_expr = kde.fstr(f'Hello, {I.countries:s}!')

    countries = ds(['USA', 'Schweiz'])
    testing.assert_equal(
        expr_eval.eval(greeting_expr, countries=countries),
        ds(['Hello, USA!', 'Hello, Schweiz!']),
    )
    local_greetings = ds(['Hello', 'Gruezi'])
    # Data slice is interpreted as literal.
    local_greeting_expr = kde.fstr(f'{local_greetings:s}, {I.countries:s}!')
    testing.assert_equal(
        expr_eval.eval(local_greeting_expr, countries=countries),
        ds(['Hello, USA!', 'Gruezi, Schweiz!']),
    )

    price_expr = kde.fstr(
        f'Lunch price in {I.countries:s} is {I.prices:.2f} {I.currencies:s}.'
    )
    testing.assert_equal(
        expr_eval.eval(
            price_expr,
            countries=countries,
            prices=ds([35.5, 49.2]),
            currencies=ds(['USD', 'CHF']),
        ),
        ds([
            'Lunch price in USA is 35.50 USD.',
            'Lunch price in Schweiz is 49.20 CHF.',
        ]),
    )

  def test_incompatible_text_bytes_types_error(self):
    with self.assertRaisesRegex(
        ValueError,
        # TODO: Make errors Koda friendly.
        re.escape('unsupported argument types (TEXT,TEXT,BYTES)'),
    ):
      expr_eval.eval(kde.strings.fstr(f'{ds(b"foo"):s}'))

  def test_unsupported_types_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'cannot format argument `x` of type ITEMID',
    ):
      expr_eval.eval(kde.strings.fstr(f'{kde.uuid():s}'))
    with self.assertRaisesRegex(
        ValueError,
        'cannot format argument `x` of type OBJECT containing non-primitive'
        ' values',
    ):
      expr_eval.eval(
          kde.strings.fstr(
              f'{kde.with_schema(kde.uuid(), schema_constants.OBJECT):s}'
          )
      )
    with self.assertRaisesRegex(
        ValueError,
        'cannot format argument `x` of type MASK',
    ):
      expr_eval.eval(kde.strings.fstr(f'{ds(arolla.present()):s}'))

  def test_mixed_slice_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'cannot format argument `x` of type OBJECT containing INT32 and STRING'
        ' values',
    ):
      expr_eval.eval(kde.strings.fstr(f'{ds([1, "foo"]):s}'))

  def test_binding_policy(self):
    self.assertEqual(
        inspect.signature(kde.strings.fstr),
        inspect.signature(lambda fstr, /: None),
    )
    with self.assertRaisesRegex(TypeError, 'expected a string'):
      kde.strings.fstr(b'a')

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.fstr,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.strings.fstr(f'{I.x:s}')))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.strings.fstr, kde.fstr))


if __name__ == '__main__':
  absltest.main()
