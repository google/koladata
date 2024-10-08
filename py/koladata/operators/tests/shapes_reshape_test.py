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

"""Tests for kde.shapes.reshape."""

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
from koladata.types import literal_operator
from koladata.types import qtypes

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
JAGGED_SHAPE = qtypes.JAGGED_SHAPE


QTYPES = frozenset([
    (DATA_SLICE, JAGGED_SHAPE, DATA_SLICE),
])


class ShapesReshapeTest(parameterized.TestCase):

  @parameterized.parameters(
      # Shapes.
      (ds(1), jagged_shape.create_shape([1]), ds([1])),
      (ds(1), jagged_shape.create_shape([1], [1], [1]), ds([[[1]]])),
      (ds([1]), jagged_shape.create_shape(), ds(1)),
      (ds([[[1]]]), jagged_shape.create_shape([1]), ds([1])),
      (ds([[[1]]]), jagged_shape.create_shape(), ds(1)),
      (ds([[1, 2], [3]]), jagged_shape.create_shape([3]), ds([1, 2, 3])),
      (
          ds([1, 2, 3]),
          jagged_shape.create_shape([2], [2, 1]),
          ds([[1, 2], [3]]),
      ),
      # Dimension tuples.
      (ds(1), arolla.tuple(ds([1])), ds([1])),
      (
          ds([1, 2, 3]),
          arolla.tuple(
              ds(1), ds([2]), arolla.types.DenseArrayEdge.from_sizes([2, 1])
          ),
          ds([[[1, 2], [3]]]),
      ),
  )
  def test_eval(self, x, shape, expected_output):
    res = expr_eval.eval(kde.shapes.reshape(I.x, I.shape), x=x, shape=shape)
    testing.assert_equal(res, expected_output)

  def test_incompatible_shape_exception(self):
    with self.assertRaisesRegex(
        ValueError,
        'shape size must be compatible with number of items: shape_size=2 !='
        ' items_size=3',
    ):
      expr_eval.eval(
          kde.shapes.reshape(ds([1, 2, 3]), jagged_shape.create_shape([2]))
      )

  def test_boxing(self):
    testing.assert_equal(
        kde.shapes.reshape(ds(1), jagged_shape.create_shape([1])),
        arolla.abc.bind_op(
            kde.shapes.reshape,
            literal_operator.literal(ds(1)),
            literal_operator.literal(jagged_shape.create_shape([1])),
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.shapes.reshape,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.shapes.reshape(I.x, I.shape)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.shapes.reshape, kde.reshape))


if __name__ == '__main__':
  absltest.main()
