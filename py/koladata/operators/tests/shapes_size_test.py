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

"""Tests for kde.shapes.size."""

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
from koladata.types import jagged_shape
from koladata.types import qtypes
from koladata.types import schema_constants


INT64 = schema_constants.INT64
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class ShapesSizeTest(parameterized.TestCase):

  @parameterized.parameters(
      (jagged_shape.create_shape(), ds(1, INT64)),
      (jagged_shape.create_shape([2]), ds(2, INT64)),
      (jagged_shape.create_shape([2], [2, 1]), ds(3, INT64)),
  )
  def test_eval(self, shape, expected_res):
    res = expr_eval.eval(kde.shapes.size(I.shape), shape=shape)
    testing.assert_equal(res, expected_res)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.shapes.size,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((qtypes.JAGGED_SHAPE, qtypes.DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.shapes.size(I.x)))


if __name__ == '__main__':
  absltest.main()
