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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE, DATA_SLICE),
])


class CoreSortTest(parameterized.TestCase):

  @parameterized.parameters(
      # x.ndim = 1
      (ds([0, 3, None, 6]), False, ds([0, 3, 6, None])),
      (ds([0, 3, None, 6]), True, ds([6, 3, 0, None])),
      # x.ndim = 2
      (
          ds([[0, 3, None, 6], [5, None, 2, 1]]),
          False,
          ds([[0, 3, 6, None], [1, 2, 5, None]]),
      ),
      (
          ds([[0, 3, None, 6], [5, None, 2, 1]]),
          True,
          ds([[6, 3, 0, None], [5, 2, 1, None]]),
      ),
      # descending as DataItem
      (ds([0, 3, None, 6]), ds(True), ds([6, 3, 0, None])),
      # OBJECT, ANY schemas
      (
          ds([0, 3, None, 6], schema_constants.OBJECT),
          True,
          ds([6, 3, 0, None], schema_constants.OBJECT),
      ),
      (ds([0, 3, None, 6]).as_any(), True, ds([6, 3, 0, None]).as_any()),
      # BOOLEAN
      (
          ds([True, False, None, True]),
          True,
          ds([True, True, False, None]),
      ),
      # STRING
      (ds(['a', 'b', None, 'c']), True, ds(['c', 'b', 'a', None])),
      # BYTES
      (ds([b'a', b'b', None, b'c']), True, ds([b'c', b'b', b'a', None])),
      # FLOAT32
      (ds([1.0, 3.0, None, 6.0]), True, ds([6.0, 3.0, 1.0, None])),
      # FLOAT64
      (
          ds([1.0, 3.0, None, 6.0], schema=schema_constants.FLOAT64),
          True,
          ds([6.0, 3.0, 1.0, None], schema=schema_constants.FLOAT64),
      ),
      # INT64
      (
          ds([0, 3, None, 6], schema=INT64),
          True,
          ds([6, 3, 0, None], schema=INT64),
      ),
      # empty x
      (ds([], schema=INT64), False, ds([], schema=INT64)),
      # all missing items
      (ds([None, None], schema=INT64), False, ds([None, None], schema=INT64)),
  )
  def test_eval_without_sort_by(self, x, descending, expected):
    result = expr_eval.eval(kde.slices.sort(x, descending=descending))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      # x.ndim = 1
      (
          ds(['a', 'b', 'c', None]),
          ds([3, 6, 0, None]),
          ds(['c', 'a', 'b', None]),
      ),
      # x.ndim = 2
      (
          ds([['a', 'b', 'c', None], ['b', 'c', 'a', None]]),
          ds([[3, 6, 0, None], [5, 0, 0, 1]]),
          ds([['c', 'a', 'b', None], ['c', 'a', None, 'b']]),
      ),
  )
  def test_eval_with_sort_by(self, x, sort_by, expected):
    result = expr_eval.eval(kde.slices.sort(x, sort_by))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      # x.ndim = 1
      (ds([0, 3, None, 6]), ds([0, 3, 6, None])),
      # x.ndim = 2
      (
          ds([[0, 3, None, 6], [5, None, 2, 1]]),
          ds([[0, 3, 6, None], [1, 2, 5, None]]),
      ),
  )
  def test_eval_with_descending_unspecified(self, x, expected):
    result = expr_eval.eval(kde.slices.sort(x))
    testing.assert_equal(result, expected)

  def test_sort_by_more_sparse(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('more sparse')
    ):
      expr_eval.eval(kde.slices.sort(ds([0, 3, 6]), ds([2, 1, None])))

  def test_data_item(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        # TODO: b/375621456 - Must start with 'kde.slices.sort: '.
        re.escape('array.ordinal_rank: expected rank(x) > 0'),
    ):
      expr_eval.eval(kde.slices.sort(ds(0)))

  def test_different_shape(self):
    with self.assertRaisesRegex(
        # TODO: b/375621456 - Raise KodaError.
        ValueError,
        re.escape(
            'kde.slices.sort: arguments `x` and `sort_by` must have the same'
            ' shape'
        ),
    ):
      expr_eval.eval(kde.slices.sort(ds([0, 3, 6]), ds([0, 3, 6, 1])))

    with self.assertRaisesRegex(
        # TODO: b/375621456 - Raise KodaError.
        ValueError,
        re.escape(
            'kde.slices.sort: arguments `x` and `sort_by` must have the same'
            ' shape'
        ),
    ):
      expr_eval.eval(kde.slices.sort(ds([0, 3, 6]), ds([[1], [2, 3], [4]])))

  def test_qtype_signatures(self):
    # Limit the allowed qtypes and a random QType to speed up the test.
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.sort,
            possible_qtypes=(
                arolla.UNSPECIFIED,
                qtypes.DATA_SLICE,
                arolla.INT64,
            ),
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.sort(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.sort, kde.sort))


if __name__ == '__main__':
  absltest.main()
