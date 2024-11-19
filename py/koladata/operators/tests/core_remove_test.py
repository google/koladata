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
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreRemoveTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.remove,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  @parameterized.parameters(
      (
          ds([1, 2, 3]),
          ds(
              [arolla.present(), arolla.present(), arolla.present()],
              schema_constants.MASK,
          ),
          ds([], schema_constants.INT32),
      ),
      # Multi-dimensional.
      (
          ds([[1], [2], [3]]),
          ds(
              [[arolla.present()], [None], [arolla.present()]],
              schema_constants.MASK,
          ),
          ds([[], [2], []]),
      ),
      # Object schema
      (
          ds([[1], [2], [3]]),
          ds(
              [[arolla.present()], [None], [arolla.present()]],
              schema_constants.OBJECT,
          ),
          ds([[], [2], []]),
      ),
      (
          ds([[1], [None], [3]]),
          ds(
              [[arolla.present()], [arolla.present()], [arolla.present()]],
              schema_constants.MASK,
          ),
          ds([[], [], []], schema_constants.INT32),
      ),
      (
          ds([[1], [None], [3]]),
          ds(
              [[arolla.present()], [arolla.present()], [arolla.present()]],
          ).as_any(),
          ds([[], [], []], schema_constants.INT32),
      ),
      # Mixed types
      (
          ds(['a', 1, None, 1.5]),
          ds(
              [arolla.present(), arolla.present(), arolla.present(), None],
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
          ds(None, schema_constants.MASK),
          ds([1, None, 3]),
      ),
      # Expand by default.
      (
          ds([[1], [2], [3]]),
          ds(None, schema_constants.MASK),
          ds([[1], [2], [3]]),
      ),
      (
          ds([1, None, 3]),
          ds(arolla.present(), schema_constants.MASK),
          ds([], schema_constants.INT32),
      ),
      # Two scalar input, scalar output.
      (ds(1), ds(None, schema_constants.MASK), ds(1)),
      (ds(1), ds(arolla.present()), ds(None, schema_constants.INT32)),
  )
  def test_eval(self, values, filter_arr, expected):
    result = expr_eval.eval(kde.core.remove(values, filter_arr))
    testing.assert_equal(result, expected)

  def test_remove_wrong_filter_schema(self):
    val = data_slice.DataSlice.from_vals([1, 2, None, 4])
    with self.assertRaisesRegex(
        ValueError,
        '`fltr` must have kd.MASK dtype.',
    ):
      expr_eval.eval(kde.core.remove(val, val))

  def test_remove_wrong_filter_type(self):
    val = data_slice.DataSlice.from_vals([1, 2, None, 4]).as_any()
    with self.assertRaisesRegex(
        ValueError,
        '`fltr` must have kd.MASK dtype.',
    ):
      expr_eval.eval(kde.core.remove(val, val))

  def test_remove_expand_to_shape(self):
    x = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    y = ds([None, None, arolla.present()])
    result = expr_eval.eval(kde.core.remove(x, y))
    testing.assert_equal(result, ds([[1, 2, None, 4], [None, None], []]))

  def test_remove_expand_to_shape_fails(self):
    x = data_slice.DataSlice.from_vals(
        [[1, 2, None, 4], [None, None], [7, 8, 9]]
    )
    y = data_slice.DataSlice.from_vals([arolla.present(), None])

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape('kd.select: failed to broadcast `fltr` to `ds`'),
    ):
      _ = expr_eval.eval(kde.core.remove(x, y))

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.core.remove(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.remove, kde.remove))


if __name__ == '__main__':
  absltest.main()
