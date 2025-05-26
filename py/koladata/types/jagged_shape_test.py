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
  return jagged_shape.JaggedShape.from_edges(
      arolla.types.DenseArrayEdge.from_sizes([2]),
      arolla.types.DenseArrayEdge.from_sizes([2, 1]),
      arolla.types.DenseArrayEdge.from_sizes([1, 2, 1]),
  )


def _koda_shape_2d():
  return jagged_shape.JaggedShape.from_edges(
      arolla.types.DenseArrayEdge.from_sizes([2]),
      arolla.types.DenseArrayEdge.from_sizes([2, 1]),
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

  def test_from_edges(self):
    shape = _koda_shape_3d()
    testing.assert_equal(
        shape.qtype,
        jagged_shape.JAGGED_SHAPE,
    )
    self.assertIsInstance(shape, jagged_shape.JaggedShape)

  def test_edges(self):
    edges = _koda_shape_3d().edges()
    self.assertLen(edges, 3)
    testing.assert_equivalent(
        edges[0], arolla.types.DenseArrayEdge.from_sizes([2])
    )
    testing.assert_equivalent(
        edges[1], arolla.types.DenseArrayEdge.from_sizes([2, 1])
    )
    testing.assert_equivalent(
        edges[2], arolla.types.DenseArrayEdge.from_sizes([1, 2, 1])
    )

  def test_rank(self):
    shape = _koda_shape_3d()
    self.assertEqual(shape.rank(), 3)

  def test_getitem_with_index(self):
    shape = _koda_shape_3d()
    testing.assert_equivalent(
        shape[1], arolla.types.DenseArrayEdge.from_sizes([2, 1])
    )

  def test_getitem_with_slice(self):
    shape = _koda_shape_3d()
    self.assertEqual(shape[:2], _koda_shape_2d())

  def test_getitem_with_invalid_index(self):
    shape = _koda_shape_3d()
    with self.assertRaises(ValueError):
      _ = shape[1:]
    with self.assertRaises(ValueError):
      _ = shape[0:2:2]
    with self.assertRaises(TypeError):
      _ = shape['hello']

  def test_eq(self):
    # Disable generic assert since we want to make sure we test the __eq__
    # operator.
    self.assertTrue(_koda_shape_3d() == _koda_shape_3d())  # pylint: disable=g-generic-assert
    self.assertFalse(_koda_shape_3d() == _koda_shape_2d())  # pylint: disable=g-generic-assert
    with self.assertRaises(NotImplementedError):
      _ = _koda_shape_3d() == 42

  def test_ne(self):
    # Disable generic assert since we want to make sure we test the __ne__
    # operator.
    self.assertTrue(_koda_shape_3d() != _koda_shape_2d())  # pylint: disable=g-generic-assert
    self.assertFalse(_koda_shape_3d() != _koda_shape_3d())  # pylint: disable=g-generic-assert
    with self.assertRaises(NotImplementedError):
      _ = _koda_shape_3d() != 42


if __name__ == '__main__':
  absltest.main()
