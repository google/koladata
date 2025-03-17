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

"""Tests for kde.slices.index.

Note that there are more extensive tests that reuse the existing Arolla tests
for the M.array.agg_index operator.
"""

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
INT64 = schema_constants.INT64


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class SlicesIndexTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([[5, None, 5], [6, 7], [None, None]]),
          ds(0),
          ds([[0, 0, 0], [1, 1], [None, None]], INT64),
      ),
      (
          ds([[5, None, 5], [6, 7], [None, None]]),
          ds(1),
          ds([[0, None, 2], [0, 1], [None, None]], INT64),
      ),
      (
          ds([[5, None, 5], [6, 7], [None, None]]),
          ds(-1),
          ds([[0, None, 2], [0, 1], [None, None]], INT64),
      ),
      (
          ds([[['a', 'b', 'c'], ['d', 'e']], [['f', 'g'], ['h', 'i', 'j']]]),
          ds(0),
          ds([[[0, 0, 0], [0, 0]], [[1, 1], [1, 1, 1]]], INT64),
      ),
      (
          ds([[['a', 'b', 'c'], ['d', 'e']], [['f', 'g'], ['h', 'i', 'j']]]),
          ds(1),
          ds([[[0, 0, 0], [1, 1]], [[0, 0], [1, 1, 1]]], INT64),
      ),
      (
          ds([[['a', 'b', 'c'], ['d', 'e']], [['f', 'g'], ['h', 'i', 'j']]]),
          ds(-2),
          ds([[[0, 0, 0], [1, 1]], [[0, 0], [1, 1, 1]]], INT64),
      ),
      (
          ds([[['a', 'b', 'c'], ['d', 'e']], [['f', 'g'], ['h', 'i', 'j']]]),
          ds(2),
          ds([[[0, 1, 2], [0, 1]], [[0, 1], [0, 1, 2]]], INT64),
      ),
      (
          ds([[['a', 'b', 'c'], ['d', 'e']], [['f', 'g'], ['h', 'i', 'j']]]),
          ds(-1),
          ds([[[0, 1, 2], [0, 1]], [[0, 1], [0, 1, 2]]], INT64),
      ),
  )
  def test_eval(self, x, dim, expected_value):
    actual_value = expr_eval.eval(kde.slices.index(x, dim))
    testing.assert_equal(actual_value, expected_value)

  @parameterized.parameters(
      (ds([5, 6, None, 7]), ds([0, 1, None, 3], INT64)),
      (ds([None]), ds([None], INT64)),
      (
          ds([[5, None, 5], [6, 7], [None, None]]),
          ds([[0, None, 2], [0, 1], [None, None]], INT64),
      ),
      (ds([1, 'a', None, 2.0]), ds([0, 1, None, 3], INT64)),
      (
          ds([[['a', 'b', 'c'], ['d', 'e']], [['f', 'g'], ['h', 'i', 'j']]]),
          ds([[[0, 1, 2], [0, 1]], [[0, 1], [0, 1, 2]]], INT64),
      ),
  )
  def test_eval_unspecified_dim(self, x, expected_value):
    value_with_unspecified_dim = expr_eval.eval(kde.slices.index(x))
    ndim = x.get_ndim()
    value_with_dim_equal_ndim_minus_1 = expr_eval.eval(
        kde.slices.index(x, ndim - ds(1))
    )
    testing.assert_equal(
        value_with_unspecified_dim, value_with_dim_equal_ndim_minus_1
    )
    testing.assert_equal(value_with_unspecified_dim, expected_value)

  def test_data_item_input_error(self):
    x = ds(1)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('kd.slices.index: argument `x` must have non-zero rank'),
    ):
      expr_eval.eval(kde.slices.index(x))

  @parameterized.parameters(2, -2)
  def test_out_of_bounds_ndim_error(self, ndim):
    x = data_slice.DataSlice.from_vals([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.index: expected -get_ndim(x) <= dim < get_ndim(x)'
        ),
    ):
      expr_eval.eval(kde.slices.index(x, ndim))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.index,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.index(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.index, kde.index))


if __name__ == '__main__':
  absltest.main()
