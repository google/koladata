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
from koladata.operators import view_overloads
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes

M = arolla.M
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class TupleGetNthTest(parameterized.TestCase):

  @parameterized.parameters(
      (arolla.tuple(ds(0), ds([1, 2, 3]), arolla.int32(1)), ds(0), ds(0)),
      (
          arolla.tuple(ds(0), ds([1, 2, 3]), arolla.int32(1)),
          ds(1),
          ds([1, 2, 3]),
      ),
      (
          arolla.tuple(ds(0), ds([1, 2, 3]), arolla.int32(1)),
          ds(2),
          arolla.int32(1),
      ),
      # Slices are also supported, as they are derived from tuples.
      (arolla.types.Slice(ds(0), ds(1)), ds(1), ds(1)),
  )
  def test_eval(self, tpl, n, expected):
    result = expr_eval.eval(kde.tuple.get_nth(tpl, n))
    view_result = expr_eval.eval(view_overloads.get_item(tpl, n))
    testing.assert_equal(result, expected)
    testing.assert_equal(view_result, expected)

  def test_eval_non_literal_x(self):
    result = expr_eval.eval(kde.tuple.get_nth(I.tpl, 0), tpl=(1, 2, 3))
    testing.assert_equal(result, ds(1))

  def test_non_literal_n_error(self):
    with self.assertRaisesRegex(ValueError, '`n` must be literal'):
      expr_eval.eval(kde.tuple.get_nth((1, 2, 3), I.n), n=ds(1))

  def test_negative_n_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a non-negative integer, got n=-1')
    ):
      expr_eval.eval(kde.tuple.get_nth((1, 2, 3), -1))

  def test_oob_n_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('index out of range: n=3')
    ):
      expr_eval.eval(kde.tuple.get_nth((1, 2, 3), 3))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.tuple.get_nth(I.x, 0)))


if __name__ == '__main__':
  absltest.main()
