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

"""Tests for core_cum_count."""

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
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreCumCountTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(0), 0, ds(1, dtype=INT64)),
      (ds([0, 0, None, 0]), 0, ds([1, 1, None, 1], dtype=INT64)),
      (ds([0, 0, None, 0]), 1, ds([1, 2, None, 3], dtype=INT64)),
      (ds([0, 0, None, 0]), ds(0), ds([1, 1, None, 1], dtype=INT64)),
      (ds([0, 0, None, 0]), ds(1), ds([1, 2, None, 3], dtype=INT64)),
      (ds(['a', 1, None]), 0, ds([1, 1, None], dtype=INT64)),
      (ds(['a', 1, None]), 1, ds([1, 2, None], dtype=INT64)),
      (
          ds([[[0, 0], [None, 0]], [[None, None]], [[0, None, 0]]]),
          0,
          ds(
              [[[1, 1], [None, 1]], [[None, None]], [[1, None, 1]]],
              dtype=INT64,
          ),
      ),
      (
          ds([[[0, 0], [None, 0]], [[None, None]], [[0, None, 0]]]),
          arolla.unspecified(),
          ds(
              [[[1, 2], [None, 1]], [[None, None]], [[1, None, 2]]],
              dtype=INT64,
          ),
      ),
      (
          ds([[[0, 0], [None, 0]], [[None, None]], [[0, None, 0]]]),
          1,
          ds(
              [[[1, 2], [None, 1]], [[None, None]], [[1, None, 2]]],
              dtype=INT64,
          ),
      ),
      (
          ds([[[0, 0], [None, 0]], [[None, None]], [[0, None, 0]]]),
          2,
          ds(
              [[[1, 2], [None, 3]], [[None, None]], [[1, None, 2]]],
              dtype=INT64,
          ),
      ),
      (
          ds([[[0, 0], [None, 0]], [[None, None]], [[0, None, 0]]]),
          3,
          ds(
              [[[1, 2], [None, 3]], [[None, None]], [[4, None, 5]]],
              dtype=INT64,
          ),
      ),
  )
  def test_eval(self, x, ndim, expected):
    result = expr_eval.eval(kde.core.cum_count(x, ndim))
    testing.assert_equal(result, expected)

  def test_out_of_bounds_ndim_error(self):
    x = ds([1, 1, 1])
    with self.assertRaisesRegex(
        ValueError, re.escape('expected 0 <= ndim <= rank')
    ):
      expr_eval.eval(kde.core.cum_count(x, -1))
    with self.assertRaisesRegex(
        ValueError, re.escape('expected 0 <= ndim <= rank')
    ):
      expr_eval.eval(kde.core.cum_count(x, 2))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.cum_count,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.core.cum_count(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.cum_count, kde.cum_count))


if __name__ == '__main__':
  absltest.main()
