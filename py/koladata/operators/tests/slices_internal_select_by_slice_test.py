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
NON_DETERMINISTIC_TOKEN = qtypes.NON_DETERMINISTIC_TOKEN

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class SlicesInternalSelectTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([1, 2, 3]),
          ds([None, None, None], schema_constants.MASK),
          ds([], schema_constants.INT32),
      ),
      # Multi-dimensional.
      (
          ds([[1], [2], [3]]),
          ds(
              [[None], [arolla.present()], [None]],
              schema_constants.MASK,
          ),
          ds([[], [2], []]),
      ),
      # Object schema
      (
          ds([[1], [2], [3]]),
          ds(
              [[None], [arolla.present()], [None]],
              schema_constants.OBJECT,
          ),
          ds([[], [2], []]),
      ),
      (
          ds([[1], [None], [3]]),
          ds([[None], [None], [None]], schema_constants.MASK),
          ds([[], [], []], schema_constants.INT32),
      ),
      (
          ds([[1], [None], [3]]),
          ds([[None], [None], [None]], schema_constants.OBJECT),
          ds([[], [], []], schema_constants.INT32),
      ),
      # Mixed types
      (
          ds(['a', 1, None, 1.5]),
          ds(
              [None, None, None, arolla.present()],
              schema_constants.MASK,
          ),
          ds([1.5], schema_constants.OBJECT),
      ),
      # Empty
      (
          ds([]),
          ds([], schema_constants.MASK),
          ds([]),
      ),
      (
          ds([[], [], []]),
          ds([[], [], []], schema_constants.MASK),
          ds([[], [], []]),
      ),
      # one scalar input.
      (
          ds([1, None, 3]),
          ds(arolla.present()),
          ds([1, None, 3]),
      ),
      # Expand by default.
      (
          ds([[1], [2], [3]]),
          ds(arolla.present(), schema_constants.MASK),
          ds([[1], [2], [3]]),
      ),
      (
          ds([1, None, 3]),
          ds(None, schema_constants.MASK),
          ds([], schema_constants.INT32),
      ),
  )
  def test_eval(self, values, filter_arr, expected):
    result = expr_eval.eval(
        kde.slices.internal_select_by_slice(values, filter_arr)
    )
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      (
          ds([1, 2, 3]),
          ds(
              [arolla.missing(), arolla.present(), arolla.present()],
              schema_constants.MASK,
          ),
          True,
          ds([2, 3]),
      ),
      (
          ds([[1], [2], [3]]),
          ds(
              [[arolla.missing()], [arolla.present()], [arolla.missing()]],
              schema_constants.MASK,
          ),
          True,
          ds([[], [2], []]),
      ),
      # Mixed types
      (
          ds(['a', 1, None, 1.5]),
          ds(
              [
                  arolla.missing(),
                  arolla.missing(),
                  arolla.missing(),
                  arolla.present(),
              ],
              schema_constants.MASK,
          ),
          True,
          ds([1.5], schema_constants.OBJECT),
      ),
      # Scalar input
      (
          ds([[1], [2], [3]]),
          ds(arolla.unit(), schema_constants.MASK),
          True,
          ds([[1], [2], [3]]),
      ),
      (
          ds([[1], [2], [3]]),
          ds(None, schema_constants.MASK),
          True,
          ds([[], [], []], schema_constants.INT32),
      ),
      # disable expand filter
      (
          ds([[1], [2], [3]]),
          ds(
              [arolla.missing(), arolla.present(), arolla.present()],
              schema_constants.MASK,
          ),
          False,
          ds([[2], [3]]),
      ),
      # Mixed types
      (
          ds([['a'], [2.5], [3, None]]),
          ds(
              [arolla.missing(), arolla.present(), arolla.present()],
              schema_constants.MASK,
          ),
          False,
          ds([[2.5], [3, None]], schema_constants.OBJECT),
      ),
      # Empty
      (
          ds([]),
          ds([], schema_constants.MASK),
          False,
          ds([]),
      ),
      (
          ds([[[]], [[]], [[]]]),
          ds([[None], [None], [None]], schema_constants.MASK),
          False,
          ds([[], [], []]).repeat(0),
      ),
  )
  def test_eval_with_expand_filter(
      self,
      values,
      filter_arr,
      expand_filter,
      expected,
  ):
    result = expr_eval.eval(
        kde.slices.internal_select_by_slice(values, filter_arr, expand_filter)
    )
    testing.assert_equal(result, expected)

  def test_select_wrong_filter_schema(self):
    val = data_slice.DataSlice.from_vals([1, 2, None, 4])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.select: the schema of the `fltr` DataSlice'
            ' should only be OBJECT or MASK'
        ),
    ):
      expr_eval.eval(kde.slices.internal_select_by_slice(val, val))

  def test_select_on_data_item_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.select: cannot select from DataItem because its'
            ' size is always 1. Consider calling .flatten() beforehand to'
            ' convert it to a 1-dimensional DataSlice'
        ),
    ):
      expr_eval.eval(
          kde.slices.internal_select_by_slice(ds(1), ds(arolla.present()))
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.select: cannot filter using a DataItem when'
            ' expand_filter=False because its size is always 1.'
        ),
    ):
      expr_eval.eval(
          kde.slices.internal_select_by_slice(
              ds([1, 2]), ds(arolla.present()), expand_filter=False
          )
      )

  def test_select_expand_to_shape(self):
    x = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    y = ds([arolla.present(), arolla.present(), None])
    result = expr_eval.eval(kde.slices.internal_select_by_slice(x, y))
    testing.assert_equal(result, ds([[1, 2, None, 4], [None, None], []]))

  def test_select_expand_to_shape_fails(self):
    x = data_slice.DataSlice.from_vals(
        [[1, 2, None, 4], [None, None], [7, 8, 9]]
    )
    y = data_slice.DataSlice.from_vals(
        [arolla.present(), arolla.present(), None, arolla.present()]
    )

    with self.assertRaisesRegex(
        ValueError,
        re.escape('kd.slices.select: failed to broadcast `fltr` to `ds`'),
    ):
      _ = expr_eval.eval(kde.slices.internal_select_by_slice(x, y))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.internal_select_by_slice,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.slices.internal_select_by_slice(I.x, I.y, I.expand_filter)
        )
    )


if __name__ == '__main__':
  absltest.main()
