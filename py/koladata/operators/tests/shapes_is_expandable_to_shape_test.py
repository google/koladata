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

"""Tests for core_is_expandable_to."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import qtypes

I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
create_shape = jagged_shape.create_shape
DATA_SLICE = qtypes.DATA_SLICE
JAGGED_SHAPE = qtypes.JAGGED_SHAPE


class ShapesIsExpandableToShapeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.shapes.is_expandable_to_shape,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        [
            (DATA_SLICE, JAGGED_SHAPE, arolla.UNSPECIFIED, DATA_SLICE),
            (DATA_SLICE, JAGGED_SHAPE, DATA_SLICE, DATA_SLICE),
            (DATA_SLICE, JAGGED_SHAPE, DATA_SLICE),
        ],
    )

  @parameterized.parameters(
      # Requiring ndim=0
      (ds(1), create_shape(3), arolla.unspecified(), kd.present),
      (ds(1), create_shape(3), 0, kd.present),
      # Requiring ndim=1
      (ds([1, 2]), create_shape(3), arolla.unspecified(), kd.missing),
      (ds([1, 2]), create_shape(3), 0, kd.missing),
      (ds([1, 2]), create_shape(3), 1, kd.present),
      # Requiring ndim=2
      (ds([[1], [2, 3]]), create_shape(3), arolla.unspecified(), kd.missing),
      (ds([[1], [2, 3]]), create_shape(3), 0, kd.missing),
      (ds([[1], [2, 3]]), create_shape(3), 1, kd.missing),
      (ds([[1], [2, 3]]), create_shape(3), 2, kd.present),
      # Different types
      (ds("a"), create_shape(3), arolla.unspecified(), kd.present),
      (ds("a"), create_shape(3), 0, kd.present),
      # Mixed types
      (ds(["a", 2, 3]), create_shape(3, 1), arolla.unspecified(), kd.present),
      (ds(["a", 2, 3]), create_shape(3, 1), 0, kd.present),
      (ds(["a", 2, 3]), create_shape(3, 1), 1, kd.present),
  )
  def test_eval(self, x, target, ndim, expected):
    result = expr_eval.eval(kde.shapes.is_expandable_to_shape(x, target, ndim))
    testing.assert_equal(result, expected)

    # Ensure result is consistent with `kde.expand_to_shape` operator.
    if expected:
      expr_eval.eval(kde.expand_to_shape(x, target, ndim))
    else:
      with self.assertRaisesRegex(
          ValueError, r"(DataSlice with shape|Cannot expand 'x').*"
      ):
        expr_eval.eval(kde.expand_to_shape(x, target, ndim))

  def test_invalid_ndim_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("expected 0 <= ndim <= rank"),
    ):
      expr_eval.eval(
          kde.shapes.is_expandable_to_shape(ds(1), create_shape(), -1)
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape("expected 0 <= ndim <= rank"),
    ):
      expr_eval.eval(
          kde.shapes.is_expandable_to_shape(ds(1), create_shape(), 1)
      )

  def test_view(self):
    self.assertTrue(
        view.has_data_slice_view(kde.shapes.is_expandable_to_shape(I.x, I.y))
    )


if __name__ == "__main__":
  absltest.main()
