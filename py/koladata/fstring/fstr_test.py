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
from koladata.expr import input_container
from koladata.fstring import fstring
from koladata.functions import functions as fns
from koladata.operators import kde_operators as _
from koladata.testing import testing
from koladata.types import data_slice


ds = data_slice.DataSlice.from_vals
I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
S = I.self


class FstrTest(absltest.TestCase):

  def test_fstr_from_docstring(self):
    countries = ds(['USA', 'Schweiz'])
    testing.assert_equal(
        fstring.fstr(f'Hello, {countries:s}!'),
        ds(['Hello, USA!', 'Hello, Schweiz!']),
    )
    greetings = ds(['Hello', 'Gruezi'])
    testing.assert_equal(
        fstring.fstr(f'{greetings:s}, {countries:s}!'),
        ds(['Hello, USA!', 'Gruezi, Schweiz!']),
    )
    states = ds([['California', 'Arizona', 'Nevada'], ['Zurich', 'Bern']])
    testing.assert_equal(
        fstring.fstr(f'{greetings:s}, {states:s} in {countries:s}!'),
        ds([
            [
                'Hello, California in USA!',
                'Hello, Arizona in USA!',
                'Hello, Nevada in USA!',
            ],
            ['Gruezi, Zurich in Schweiz!', 'Gruezi, Bern in Schweiz!'],
        ]),
    )
    prices = ds([35.5, 49.2])
    currencies = ds(['USD', 'CHF'])
    testing.assert_equal(
        fstring.fstr(
            f'Lunch price in {countries:s} is {prices:.2f} {currencies:s}.'
        ),
        ds([
            'Lunch price in USA is 35.50 USD.',
            'Lunch price in Schweiz is 49.20 CHF.',
        ]),
    )

  def test_fstr_item(self):
    self.assertEqual(f'{ds(None)}', 'None')
    testing.assert_equal(fstring.fstr(f'{ds("abc"):s}'), ds('abc'))
    testing.assert_equal(
        fstring.fstr(f'Hello, {ds("World"):s}!'), ds('Hello, World!')
    )
    testing.assert_equal(
        fstring.fstr(f'h:{ds(12):s},w:{ds(1.7):s}'), ds('h:12,w:1.7')
    )

  def test_fstr_expr_not_allowed(self):
    with self.assertRaisesRegex(
        ValueError, 'contains expression.*eager kd.fstr call'
    ):
      fstring.fstr(f'{I.x:s}')

  def test_fstr_flat(self):
    testing.assert_equal(
        fstring.fstr(f'{ds(["abc", "cde"]):s}'), ds(['abc', 'cde'])
    )
    testing.assert_equal(
        fstring.fstr(f'Hello, {ds(["World", "Piece"]):s}!'),
        ds(['Hello, World!', 'Hello, Piece!']),
    )
    testing.assert_equal(
        fstring.fstr(f'h:{ds([12, 21]):s},w:{ds([1.7, 7.1]):s}'),
        ds(['h:12,w:1.7', 'h:21,w:7.1']),
    )

  def test_fstr_nested(self):
    testing.assert_equal(
        fstring.fstr(f'h:{ds([[12, 25], [21]]):s},w:{ds([1.7, 7.1]):s}'),
        ds([['h:12,w:1.7', 'h:25,w:1.7'], ['h:21,w:7.1']]),
    )

  def test_fstr_format_spec(self):
    testing.assert_equal(
        fstring.fstr(f'Hello, {ds([7.5, 5.7]):e}!'),
        ds(['Hello, 7.500000e+00!', 'Hello, 5.700000e+00!']),
    )
    testing.assert_equal(
        fstring.fstr(f'Hello, {ds([7.5, 5.7]):.2f}!'),
        ds(['Hello, 7.50!', 'Hello, 5.70!']),
    )

  def test_fstr_complex_inside(self):
    r = 'REALLY'
    testing.assert_equal(
        fstring.fstr(f'{ds(f"Is it {r} allowed?"):s}'),
        ds('Is it REALLY allowed?'),
    )
    end = ds('?')
    testing.assert_equal(
        fstring.fstr(f'{ds("Why not") + fstring.fstr(f"{end:s}"):s}'),
        ds('Why not?'),
    )

  def test_fstr_other_types(self):
    t = 'things'
    testing.assert_equal(
        fstring.fstr(f'{ds("Cool"):s} {t}!'),
        ds('Cool things!'),
    )

  def test_fstr_errors(self):
    o = fns.obj(x=ds('X'))
    with self.assertRaisesRegex(
        ValueError,
        'cannot format argument `x` of type OBJECT containing non-primitive'
        ' values',
    ):
      fstring.fstr(f'h:{o:s}-y')
    with self.assertRaisesRegex(
        ValueError,
        'cannot format argument `x` of type OBJECT containing non-primitive'
        ' values',
    ):
      fstring.fstr(f'w:{ds([1]):s}h:{o:s}')

  def test_fstr_nothing_to_format_error(self):
    with self.assertRaisesRegex(ValueError, 'nothing to format'):
      fstring.fstr(f'h:{ds(1)}-y')

  def test_fstr_error_outside_of_fstr(self):
    o = fns.obj(x=ds('X'))
    with self.assertRaisesRegex(ValueError, 'math.subtract'):
      fstring.fstr(f'h:{o - 1:s}-y')

  def test_fstr_error_outside_of_fstr_2nd_arg(self):
    o = fns.obj(x=ds('X'))
    with self.assertRaisesRegex(ValueError, 'math.subtract'):
      fstring.fstr(f'w:{ds([1]):s} h:{o - 1:s}')


if __name__ == '__main__':
  absltest.main()
