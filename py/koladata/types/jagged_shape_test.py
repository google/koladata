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

"""Tests for jagged_shape."""

from absl.testing import absltest
from arolla import arolla
# Register kde ops for e.g. jagged_shape.create_shape().
from koladata.operators import kde_operators as _
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import jagged_shape

M = arolla.M


def _koda_shape_3d():
  return jagged_shape.KodaJaggedShape.from_edges(
      arolla.types.DenseArrayEdge.from_sizes([2]),
      arolla.types.DenseArrayEdge.from_sizes([2, 1]),
      arolla.types.DenseArrayEdge.from_sizes([1, 2, 1]),
  )


class JaggedShapeTest(absltest.TestCase):

  def test_jagged_shape(self):
    shape = data_slice.DataSlice.from_vals([1]).get_shape()
    self.assertIsInstance(shape, jagged_shape.JaggedShape)

  def test_create_shape(self):
    testing.assert_equal(
        jagged_shape.create_shape(),
        jagged_shape.JaggedShape.from_edges(),
    )
    testing.assert_equal(
        jagged_shape.create_shape(
            2, [2, 1], arolla.types.DenseArrayEdge.from_sizes([1, 2, 1])
        ),
        jagged_shape.JaggedShape.from_edges(
            arolla.types.DenseArrayEdge.from_sizes([2]),
            arolla.types.DenseArrayEdge.from_sizes([2, 1]),
            arolla.types.DenseArrayEdge.from_sizes([1, 2, 1]),
        ),
    )


class KodaJaggedShapeTest(absltest.TestCase):

  def test_from_edges(self):
    shape = _koda_shape_3d()
    testing.assert_equal(
        shape.qtype,
        jagged_shape.KODA_JAGGED_SHAPE,
    )
    self.assertIsInstance(shape, jagged_shape.KodaJaggedShape)

  def test_edges(self):
    shape = _koda_shape_3d()

    # TODO: Add support for this.
    with self.assertRaises(NotImplementedError):
      _ = shape.edges()

  def test_rank(self):
    shape = _koda_shape_3d()

    # TODO: Add support for this.
    with self.assertRaises(NotImplementedError):
      _ = shape.rank()

  def test_getitem(self):
    shape = _koda_shape_3d()

    # TODO: Add support for this.
    with self.assertRaises(NotImplementedError):
      _ = shape[0]

  def test_eq(self):
    shape = _koda_shape_3d()
    shape2 = _koda_shape_3d()

    # TODO: Add support for this.
    with self.assertRaises(NotImplementedError):
      _ = shape == shape2

  def test_ne(self):
    shape = _koda_shape_3d()
    shape2 = _koda_shape_3d()

    # TODO: Add support for this.
    with self.assertRaises(NotImplementedError):
      _ = shape != shape2


if __name__ == '__main__':
  absltest.main()
