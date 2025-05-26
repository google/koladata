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
from arolla.jagged_shape import jagged_shape as arolla_jagged_shape
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import arolla_bridge
from koladata.operators import jagged_shape as jagged_shape_ops  # pylint: disable=unused-import
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import jagged_shape


I = input_container.InputContainer('I')


def create_arolla_shape(*sizes):
  return arolla_jagged_shape.JaggedDenseArrayShape.from_edges(*[
      arolla.types.DenseArrayEdge.from_sizes(size_array) for size_array in sizes
  ])


def create_array_shape(*sizes):
  return arolla_jagged_shape.JaggedArrayShape.from_edges(*[
      arolla.types.DenseArrayEdge.from_sizes(size_array) for size_array in sizes
  ])


class ShapesFromArollaShapeTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          create_arolla_shape(),
          jagged_shape.create_shape(),
      ),
      (create_arolla_shape([2]), jagged_shape.create_shape([2])),
      (
          create_arolla_shape([2], [2, 1]),
          jagged_shape.create_shape([2], [2, 1]),
      ),
  )
  def test_eval(self, shape, expected_res):
    res = expr_eval.eval(
        arolla_bridge.from_arolla_jagged_shape(I.shape), shape=shape
    )
    testing.assert_equal(res, expected_res)

  def test_qtype_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected JAGGED_DENSE_ARRAY_SHAPE, got shape: JAGGED_ARRAY_SHAPE',
    ):
      expr_eval.eval(
          arolla_bridge.from_arolla_jagged_shape(I.shape),
          shape=create_array_shape(),
      )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        arolla_bridge.from_arolla_jagged_shape,
        (
            (
                arolla_jagged_shape.JAGGED_DENSE_ARRAY_SHAPE,
                jagged_shape.JAGGED_SHAPE,
            ),
        ),
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES +
        (arolla_jagged_shape.JAGGED_DENSE_ARRAY_SHAPE,),
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(arolla_bridge.from_arolla_jagged_shape(I.x))
    )


if __name__ == '__main__':
  absltest.main()
