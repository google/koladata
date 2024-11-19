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

"""Tests for kde.logical.apply_mask."""

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


class LogicalApplyMaskTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([1, None, 2, 3, None]),
          ds(
              [arolla.present(), None, arolla.present(), None, arolla.present()]
          ),
          ds([1, None, 2, None, None]),
      ),
      # Mixed types.
      (
          ds(['a', 1, None, 1.5, 'b', 2.5, 2]),
          ds([
              None,
              arolla.present(),
              arolla.present(),
              None,
              arolla.present(),
              arolla.present(),
              arolla.present(),
          ]),
          ds([None, 1, None, None, 'b', 2.5, 2]),
      ),
      # Scalar input, scalar output.
      (
          ds(1, schema_constants.INT64),
          ds(arolla.present()),
          ds(1, schema_constants.INT64),
      ),
      (
          1,
          ds(arolla.missing()),
          ds(arolla.missing(), schema_constants.INT32),
      ),
      (
          ds(None, schema_constants.MASK),
          ds(arolla.present()),
          ds(None, schema_constants.MASK),
      ),
      (
          ds(None),
          ds(arolla.missing()),
          ds(None),
      ),
      # Auto-broadcasting.
      (
          ds([1, 2, 3]),
          ds(arolla.present()),
          ds([1, 2, 3]),
      ),
      (
          ds([1, 2, 3]),
          ds(arolla.missing()),
          ds([None, None, None], schema_constants.INT32),
      ),
      (
          1,
          ds([arolla.present(), arolla.missing(), arolla.present()]),
          ds([1, None, 1]),
      ),
      # Multi-dimensional.
      (
          ds([[1, 2], [4, 5, 6], [7, 8]]),
          ds([arolla.missing(), arolla.present(), arolla.present()]),
          ds([[None, None], [4, 5, 6], [7, 8]]),
      ),
      # Mixed types.
      (
          ds([[1, 2], ['a', 'b', 'c'], [arolla.present()]]),
          ds([
              [arolla.present(), arolla.missing()],
              [arolla.present(), arolla.present(), arolla.missing()],
              [arolla.missing()],
          ]),
          ds([[1, None], ['a', 'b', None], [None]]),
      ),
  )
  def test_eval(self, values, mask, expected):
    result = expr_eval.eval(kde.logical.apply_mask(values, mask))
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.logical.apply_mask,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(repr(kde.logical.apply_mask(I.x, I.y)), 'I.x & I.y')
    self.assertEqual(repr(kde.apply_mask(I.x, I.y)), 'I.x & I.y')

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.logical.apply_mask(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.logical.apply_mask, kde.apply_mask))


if __name__ == '__main__':
  absltest.main()
