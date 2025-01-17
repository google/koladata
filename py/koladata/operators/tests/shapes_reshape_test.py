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
from koladata.types import jagged_shape
from koladata.types import literal_operator
from koladata.types import qtypes

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
JAGGED_SHAPE = qtypes.JAGGED_SHAPE


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
      (ds([]), arolla.tuple(ds(0), ds(0)), ds([]).repeat(0)),
      (ds([]), arolla.tuple(ds(0), ds(3)), ds([]).repeat(3)),
      (ds([]), arolla.tuple(ds(3), ds(0)), ds([[], [], []])),
      (
          ds([1, 2, 3]),
          arolla.tuple(
              ds(1), ds([2]), arolla.types.DenseArrayEdge.from_sizes([2, 1])
          ),
          ds([[[1, 2], [3]]]),
      ),
      # -1 dimension.
      (ds(1), arolla.tuple(ds(-1)), ds([1])),
      (ds([]), arolla.tuple(ds(0), ds(-1)), ds([]).repeat(0)),
      (ds([]), arolla.tuple(ds(0), ds(-1), ds(2)), ds([]).repeat(0).repeat(2)),
      (ds([]), arolla.tuple(ds(3), ds(-1)), ds([[], [], []])),
      (
          # Original shape:    ([2], [2, 1], [3, 2, 1], [1, 2, 3, 4, 5, 6])
          # Transformed shape: ([2], [2, 1], [2, 2, 2], [1, 2, 3, 4, 5, 6])
          ds([
              [[[1], [1, 2], [1, 2, 3]], [[1, 2, 3, 4], [1, 2, 3, 4, 5]]],
              [[[1, 2, 3, 4, 5, 6]]],
          ]),
          arolla.tuple(ds([2]), ds([2, 1]), ds(-1), ds([1, 2, 3, 4, 5, 6])),
          ds([
              [[[1], [1, 2]], [[1, 2, 3], [1, 2, 3, 4]]],
              [[[1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 6]]],
          ]),
      ),
      (ds([1, 2, 3, 4]), arolla.tuple(ds(2), ds(-1)), ds([[1, 2], [3, 4]])),
      (ds([1, 2, 3, 4]), arolla.tuple(ds(-1), ds(2)), ds([[1, 2], [3, 4]])),
  )
  def test_eval(self, x, shape, expected_output):
    x_shaped = expr_eval.eval(
        kde.shapes.reshape(I.x, I.shape), x=x, shape=shape
    )
    testing.assert_equal(x_shaped, expected_output)
    res = expr_eval.eval(kde.shapes.reshape_as(I.x, I.y), x=x, y=x_shaped)
    testing.assert_equal(res, expected_output)

  def test_unresolvable_placeholder_dim_exception(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.shapes.reshape: parent_size=2 does not divide child_size=3, so the'
        ' placeholder dimension at index 1 cannot be resolved',
    ):
      expr_eval.eval(
          kde.shapes.reshape(ds([1, 2, 3]), arolla.tuple(ds([2]), ds(-1)))
      )

  def test_multiple_placeholder_dims_exception(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.shapes.reshape: only one dimension can be a placeholder',
    ):
      expr_eval.eval(kde.shapes.reshape(ds(1), arolla.tuple(ds(-1), ds(-1))))

  def test_incompatible_dimension_specification_exception(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.shapes.reshape: invalid dimension specification - the resulting'
        ' shape size=3 != the expected size=2',
    ):
      expr_eval.eval(kde.shapes.reshape(ds([1, 2]), arolla.tuple(ds(3))))

  def test_incompatible_shape_exception(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.shapes.reshape: shape size must be compatible with number of items:'
        ' shape_size=2 != items_size=3',
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
    testing.assert_equal(
        kde.shapes.reshape(ds([1, 2, 3]), (1, 3)),
        arolla.abc.bind_op(
            kde.shapes.reshape,
            literal_operator.literal(ds([1, 2, 3])),
            literal_operator.literal(arolla.tuple(ds(1), ds(3))),
        ),
    )

  def test_unsupported_boxing(self):
    # Tuple size (1, 2) which is not supported.
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'all arguments must be DataSlices or Edges, got: *dimensions:'
            ' (DATA_SLICE, tuple<DATA_SLICE,DATA_SLICE>)'
        ),
    ):
      kde.shapes.reshape(ds([1, 2, 3]), (2, (1, 2)))

    # List -> DataSlice boxing is not supported.
    with self.assertRaises(ValueError):
      kde.shapes.reshape(ds([1, 2, 3]), (2, [1, 2]))

  def test_qtype_signatures(self):
    expected_qtype_signatures = [
        (DATA_SLICE, JAGGED_SHAPE, DATA_SLICE),
        *(
            (DATA_SLICE, shape_qtype, DATA_SLICE)
            for shape_qtype in test_qtypes.TUPLES_OF_DATA_SLICES
        ),
    ]
    arolla.testing.assert_qtype_signatures(
        kde.shapes.reshape,
        expected_qtype_signatures,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.shapes.reshape(I.x, I.shape)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.shapes.reshape, kde.reshape))


class ShapesReshapeAsTest(absltest.TestCase):

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.shapes.reshape_as,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        frozenset([(DATA_SLICE, DATA_SLICE, DATA_SLICE)]),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.shapes.reshape_as(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.shapes.reshape_as, kde.reshape_as))


if __name__ == '__main__':
  absltest.main()
