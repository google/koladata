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
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import literal_operator


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals

edge_from_sizes = arolla.types.DenseArrayEdge.from_sizes


def shape_from_sizes(*sizes_seq):
  return jagged_shape.JaggedShape.from_edges(
      *(edge_from_sizes(sizes) for sizes in sizes_seq)
  )


class ShapesNewWithSizeTest(parameterized.TestCase):

  @parameterized.parameters(
      (1, [], shape_from_sizes()),
      (2, [edge_from_sizes([2])], shape_from_sizes([2])),
      (2, [ds(2)], shape_from_sizes([2])),
      (2, [ds([2])], shape_from_sizes([2])),
      (
          9,
          [ds([2]), ds(3), edge_from_sizes([1, 2, 3, 2, 1, 0]), ds(1)],
          shape_from_sizes(
              [2], [3, 3], [1, 2, 3, 2, 1, 0], [1, 1, 1, 1, 1, 1, 1, 1, 1]
          ),
      ),
      # placeholder dimension (-1).
      (1, [ds(-1)], shape_from_sizes([1])),
      (2, [ds(-1)], shape_from_sizes([2])),
      (2, [ds(1), ds(-1)], shape_from_sizes([1], [2])),
      (2, [ds(-1), ds(1)], shape_from_sizes([2], [1, 1])),
      (6, [ds(1), ds(-1), ds(3)], shape_from_sizes([1], [2], [3, 3])),
      (
          5,
          [ds(2), ds(-1), ds([2, 1, 1, 1])],
          shape_from_sizes([2], [2, 2], [2, 1, 1, 1]),
      ),
      (
          15,
          [ds(2), ds(-1), edge_from_sizes([2, 1, 1, 1]), ds(3)],
          shape_from_sizes([2], [2, 2], [2, 1, 1, 1], [3, 3, 3, 3, 3]),
      ),
      # 0 sized.
      (0, [ds(0), ds(0)], ds([]).repeat(0).get_shape()),
      (0, [ds(0), ds(3)], ds([]).repeat(3).get_shape()),
      (0, [ds(3), ds(0)], shape_from_sizes([3], [0, 0, 0])),
      (0, [ds(0), ds(-1)], ds([]).repeat(0).get_shape()),
      (0, [ds(0), ds(-1), ds(2)], ds([]).repeat(0).repeat(2).get_shape()),
  )
  def test_eval(self, size, args, expected_value):
    actual_value = expr_eval.eval(kde.shapes._new_with_size(size, *args))
    testing.assert_equal(actual_value, expected_value)

  def test_boxing(self):
    testing.assert_equal(
        kde.shapes._new_with_size(1, [1]),
        arolla.abc.bind_op(
            kde.shapes._new_with_size,
            literal_operator.literal(ds(1)),
            literal_operator.literal(ds([1])),
        ),
    )

  def test_unsupported_qtype_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'all arguments must be DataSlices or Edges, got: *dimensions:'
            ' (FLOAT32)'
        ),
    ):
      kde.shapes._new_with_size(ds(1), arolla.float32(1.0))

  def test_unsupported_dimension_rank_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError, 'kd.shapes.reshape: unsupported DataSlice rank: 2'
    ):
      expr_eval.eval(kde.shapes._new_with_size(ds(1), ds([[1]])))

  def test_unsupported_size_rank_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.shapes.reshape: expected rank 0, but got rank=1',
    ):
      expr_eval.eval(kde.shapes._new_with_size(ds([0])))

  def test_unsupported_dimension_sizes_type_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.shapes.reshape: unsupported narrowing cast to INT64',
    ):
      expr_eval.eval(kde.shapes._new_with_size(ds(1), ds(1.0)))

  def test_unsupported_size_type_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.shapes.reshape: unsupported narrowing cast to INT64',
    ):
      expr_eval.eval(kde.shapes._new_with_size(ds(1.0)))

  def test_incompatible_dimensions_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError, 'kd.shapes.reshape: incompatible dimensions'
    ):
      expr_eval.eval(kde.shapes._new_with_size(ds(2), ds(2), ds([1])))

  def test_unresolvable_placeholder_dim_exception(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.shapes.reshape: parent_size=2 does not divide child_size=3, so the'
        ' placeholder dimension at index 1 cannot be resolved',
    ):
      expr_eval.eval(kde.shapes._new_with_size(ds(3), ds([2]), ds(-1)))

  def test_multiple_placeholder_dims_exception(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.shapes.reshape: only one dimension can be a placeholder',
    ):
      expr_eval.eval(kde.shapes._new_with_size(ds(2), ds(-1), ds(-1)))

  def test_incompatible_dimension_specification_exception(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.shapes.reshape: invalid dimension specification - the resulting'
        ' shape size=3 != the expected size=2',
    ):
      expr_eval.eval(kde.shapes._new_with_size(ds(2), ds(3)))

  def test_negative_size_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.shapes.reshape: size must be a non-negative integer, got: -1',
    ):
      expr_eval.eval(kde.shapes._new_with_size(ds(-1)))

  def test_zero_group_size_after_placeholder_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.shapes.reshape: expected a non-zero group size',
    ):
      expr_eval.eval(kde.shapes._new_with_size(ds(0), ds(2), ds(-1), ds(0)))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.shapes._new_with_size(ds(1))))


if __name__ == '__main__':
  absltest.main()
