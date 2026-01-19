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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
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


class ShapesNewTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('empty', shape_from_sizes()),
      ('edge', edge_from_sizes([2]), shape_from_sizes([2])),
      ('uniform_size', ds(2), shape_from_sizes([2])),
      ('sizes', ds([2]), shape_from_sizes([2])),
      (
          'mix',
          ds([2]),
          ds(3),
          edge_from_sizes([1, 2, 3, 2, 1, 0]),
          ds(1),
          shape_from_sizes(
              [2], [3, 3], [1, 2, 3, 2, 1, 0], [1, 1, 1, 1, 1, 1, 1, 1, 1]
          ),
      ),
  )
  def test_eval(self, *args_and_expected):
    args, expected_value = args_and_expected[:-1], args_and_expected[-1]
    actual_value = expr_eval.eval(kde.shapes.new(*args))
    testing.assert_equal(actual_value, expected_value)

  def test_boxing(self):
    testing.assert_equal(
        kde.shapes.new([1]),
        arolla.abc.bind_op(
            kde.shapes.new,
            literal_operator.literal(
                arolla.tuple(data_slice.DataSlice.from_vals([1]))
            ),
        ),
    )

  def test_unsupported_qtype_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected all arguments to be data slices or edges, got'
            ' *dimensions: (FLOAT32)'
        ),
    ):
      kde.shapes.new(arolla.float32(1.0))

  def test_unsupported_rank_error(self):
    with self.assertRaisesRegex(
        ValueError, 'kd.shapes.new: unsupported DataSlice rank: 2'
    ):
      expr_eval.eval(kde.shapes.new(ds([[1]])))

  def test_unsupported_sizes_type_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'kd.shapes.new: unsupported narrowing cast to INT64',
    ):
      expr_eval.eval(kde.shapes.new(ds(1.0)))

  def test_incompatible_dimensions_error(self):
    with self.assertRaisesRegex(
        ValueError, 'kd.shapes.new: incompatible dimensions'
    ):
      expr_eval.eval(kde.shapes.new(ds(2), ds([1])))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.shapes.new()))


if __name__ == '__main__':
  absltest.main()
