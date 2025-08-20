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
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice

kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class TuplesGetNthTest(parameterized.TestCase):

  @parameterized.parameters(
      (('a', 'b', 'c'), 0, ds('a')),
      (('a', 'b', 'c'), 1, ds('b')),
      (('a', 'b', 'c'), 2, ds('c')),
      (kde.tuple('a', 'b', 'c').eval(), ds(2), ds('c')),
      (kde.tuple(ds([1, 2, 3]), 'b', 'c').eval(), ds(0), ds([1, 2, 3])),
      (kde.tuple('a', 'b', arolla.text('c')).eval(), ds(2), arolla.text('c')),
      (slice('a', 'b', 'c'), 0, ds('a')),
      (slice('a', 'b', 'c'), 1, ds('b')),
      (slice('a', 'b', 'c'), 2, ds('c')),
      (arolla.types.Slice(ds('a'), ds('b'), ds('c')), ds(0), ds('a')),
      (arolla.types.Slice(ds('a'), ds('b'), ds('c')), ds(1), ds('b')),
      (arolla.types.Slice(ds('a'), ds('b'), ds('c')), ds(2), ds('c')),
  )
  def test_get_nth(self, tpl, n, expected):
    testing.assert_equal(fns.tuples.get_nth(tpl, n), expected)

  @parameterized.parameters('abc', ds('abc'), ds([1]))
  def test_not_index_error(self, n):
    with self.assertRaisesRegex(
        TypeError, re.escape(f'expected an index-value, got: {n}')
    ):
      fns.tuples.get_nth((0, 1, 2), n)

  def test_not_convertible_to_qvalue_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'failed to construct a QValue from the provided input containing an'
            ' Expr: (1, L.x)'
        ),
    ):
      fns.tuples.get_nth((1, arolla.L.x), 0)

  def test_not_tuple_error(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('expected a value convertible to a Tuple, got: 1')
    ):
      fns.tuples.get_nth(1, 0)

  @parameterized.parameters(-1, 3)
  def test_oob_error(self, n):
    with self.assertRaisesRegex(
        ValueError, re.escape(f'expected 0 <= n < len(x), got: n={n}, len(x)=3')
    ):
      fns.tuples.get_nth((0, 1, 2), n)


if __name__ == '__main__':
  absltest.main()
