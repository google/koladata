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

"""Tests for kde.core.reverse_select."""

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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreReverseSelectTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([2, 3]),
          ds([None, arolla.present(), arolla.present()], schema_constants.MASK),
          ds([None, 2, 3]),
      ),
      # Multi-dimensional.
      (
          ds([[], [2], [3]]),
          ds(
              [[None], [arolla.present()], [arolla.present()]],
              schema_constants.MASK,
          ),
          ds([[None], [2], [3]]),
      ),
      # OBJECT filter
      (
          ds([[], [2], [3]]),
          ds(
              [[None], [arolla.present()], [arolla.present()]],
              schema_constants.OBJECT,
          ),
          ds([[None], [2], [3]]),
      ),
      # ANY filter
      (
          ds([[], [2], [3]]),
          ds(
              [[None], [arolla.present()], [arolla.present()]],
          ).as_any(),
          ds([[None], [2], [3]]),
      ),
      # Empty ds
      (
          ds([[], [], []]),
          ds([[None], [None], [None]], schema_constants.MASK),
          ds([[None], [None], [None]], schema_constants.OBJECT),
      ),
      # Mixed types
      (
          ds(['a', 1.5]),
          ds(
              [None, arolla.present(), None, arolla.present()],
              schema_constants.MASK,
          ),
          ds([None, 'a', None, 1.5]),
      ),
      # Empty ds and empty filter
      (
          ds([[], [], []]),
          ds([[], [], []], schema_constants.MASK),
          ds([[], [], []]),
      ),
      # Two scalar input, scalar output.
      (ds(1), ds(arolla.present()), ds(1)),
      (
          ds(None, schema_constants.INT32),
          ds(arolla.missing()),
          ds(None, schema_constants.INT32),
      ),
  )
  def test_eval(self, values, fltr, expected):
    result = expr_eval.eval(kde.core.reverse_select(values, fltr))
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.reverse_select,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_invalid_type_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'the schema of the filter DataSlice should only be Any, Object or Mask',
    ):
      expr_eval.eval(kde.core.reverse_select(ds([1]), ds([1, 2])))

    with self.assertRaisesRegex(
        ValueError,
        'second argument to operator reverse_select must have all items of MASK'
        ' dtype',
    ):
      expr_eval.eval(kde.core.reverse_select(ds([1]), ds([1, 2]).as_any()))

  def test_invalid_shape_error(self):
    with self.assertRaisesRegex(
        ValueError, 'the rank of the ds and filter DataSlice must be the same'
    ):
      expr_eval.eval(
          kde.core.reverse_select(ds(1), ds([arolla.present(), None]))
      )

    with self.assertRaisesRegex(
        ValueError,
        'the shape of the DataSlice is different from the shape after applying'
        ' the filter.',
    ):
      expr_eval.eval(
          kde.core.reverse_select(
              ds([1, 2]), ds([arolla.present(), None, None])
          )
      )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.core.reverse_select(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.reverse_select, kde.reverse_select)
    )


if __name__ == '__main__':
  absltest.main()
