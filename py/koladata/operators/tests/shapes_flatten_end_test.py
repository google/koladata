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

"""Tests for kde.shapes.flatten."""

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


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class ShapesFlattenTest(parameterized.TestCase):

  @parameterized.parameters(
      # x, expected
      (ds([[[1, 2], ['a']], [[4, 'b']]]), ds([[1, 2, 'a'], [4, 'b']])),
      # x, n_times, expected
      (ds([[[1, 2], ['a']], [[4, 'b']]]), ds(1), ds([[1, 2, 'a'], [4, 'b']])),
      (ds([[[1, 2], ['a']], [[4, 'b']]]), ds(2), ds([1, 2, 'a', 4, 'b'])),
      (
          ds([[[1, 2], ['a']], [[4, 'b']]]),
          ds(0),
          ds([[[1, 2], ['a']], [[4, 'b']]]),
      ),
      (ds([[[1, 2], ['a']], [[4, 'b']]]), ds(3), ds([1, 2, 'a', 4, 'b'])),
  )
  def test_eval(self, *args_and_expected):
    args, expected_value = args_and_expected[:-1], args_and_expected[-1]
    actual_value = expr_eval.eval(kde.shapes.flatten_end(*args))
    testing.assert_equal(actual_value, expected_value)

  @parameterized.parameters(
      # x, n_times
      (ds([[1, 2], [3]]), ds(3)),
      (ds([[[1, 2], ['a']], [[4, 'b']]]), ds(-1)),
      (ds([[[1, 2], ['a']], [[4, 'b']]]), ds(4)),
  )
  def test_errors(self, *args):
    with self.assertRaisesRegex(ValueError, 'expected 0 <= n_times <= rank'):
      expr_eval.eval(kde.shapes.flatten_end(*args))

  @parameterized.parameters(
      # x
      (ds(1)),
      # x, n_times
      (ds(1), ds(0)),
  )
  def test_unsupported_item(self, *args):
    with self.assertRaisesRegex(
        ValueError, 'expected multidim DataSlice, got DataItem'
    ):
      expr_eval.eval(kde.shapes.flatten_end(*args))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.shapes.flatten_end,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.shapes.flatten_end(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.shapes.flatten_end, kde.flatten_end)
    )


if __name__ == '__main__':
  absltest.main()
