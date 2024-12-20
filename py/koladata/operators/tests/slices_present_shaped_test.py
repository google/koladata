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


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
JAGGED_SHAPE = qtypes.JAGGED_SHAPE

present = arolla.present()


QTYPES = frozenset([(JAGGED_SHAPE, DATA_SLICE)])


class CorePresentShapedTest(parameterized.TestCase):

  @parameterized.parameters(
      (jagged_shape.create_shape(), ds(present)),
      (jagged_shape.create_shape([3]), ds([present, present, present])),
      (
          jagged_shape.create_shape([2], [1, 2]),
          ds([[present], [present, present]]),
      ),
  )
  def test_eval(self, shape, expected):
    res = expr_eval.eval(kde.slices.present_shaped(shape))
    testing.assert_equal(res, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.present_shaped,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.present_shaped(I.shape)))


if __name__ == '__main__':
  absltest.main()
