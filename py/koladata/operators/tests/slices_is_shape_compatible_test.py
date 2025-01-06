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
from koladata import kd
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes


I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = [
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
]


class SlicesIsShapeCompatibleTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(0), ds(1), kd.present),
      (ds(0), ds([1]), kd.present),
      (ds([0]), ds([1]), kd.present),
      (ds([0]), ds([1, 2]), kd.missing),
      (ds(0), ds([[[1], [2, 3]], [[4, 5, 6]]]), kd.present),
      (ds([0]), ds([[[1], [2, 3]], [[4, 5, 6]]]), kd.missing),
      (ds([0, 0]), ds([[[1], [2, 3]], [[4, 5, 6]]]), kd.present),
      (ds([[0, 0], [0]]), ds([[[1], [2, 3]], [[4, 5, 6]]]), kd.present),
      (ds([[0, 0], [0, 0]]), ds([[[1], [2, 3]], [[4, 5, 6]]]), kd.missing),
      (
          ds([[[0], [0, 0]], [[0, 0, 0]]]),
          ds([[[1], [2, 3]], [[4, 5, 6]]]),
          kd.present,
      ),
      (
          ds([[[0], [0, 0]], [[0, 0]]]),
          ds([[[1], [2, 3]], [[4, 5, 6]]]),
          kd.missing,
      ),
  )
  def test_eval(self, x, y, expected):
    result = expr_eval.eval(kde.is_shape_compatible(x, y))
    testing.assert_equal(result, expected)

    # Verify commutativity.
    result = expr_eval.eval(kde.is_shape_compatible(y, x))
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.is_shape_compatible,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.slices.is_shape_compatible(I.x, I.y))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.slices.is_shape_compatible, kde.is_shape_compatible
        )
    )


if __name__ == "__main__":
  absltest.main()
