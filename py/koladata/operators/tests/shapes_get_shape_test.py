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

"""Tests for kde.shapes.get_shape."""

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
from koladata.types import jagged_shape
from koladata.types import qtypes


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class ShapesGetShapeTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(1), jagged_shape.create_shape()),
      (ds([1]), jagged_shape.create_shape([1])),
      (ds([[1, 2], [3]]), jagged_shape.create_shape([2], [2, 1])),
  )
  def test_get_shape(self, inputs, expected_shape):
    res = expr_eval.eval(kde.shapes.get_shape(I.x), x=inputs)
    testing.assert_equal(res, expected_shape)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.shapes.get_shape,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((qtypes.DATA_SLICE, qtypes.JAGGED_SHAPE),),
    )

  def test_view(self):
    x = data_slice.DataSlice.from_vals([1])
    self.assertTrue(view.has_koda_view(kde.shapes.get_shape(x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.shapes.get_shape, kde.get_shape))


if __name__ == '__main__':
  absltest.main()
