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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import qtypes
from koladata.types import schema_constants


INT64 = schema_constants.INT64
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty_mutable


class ShapesGetSizesTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          jagged_shape.create_shape(),
          ds([], schema_constants.INT64).repeat(0),
      ),
      (
          jagged_shape.create_shape([0]),
          ds([[0]], schema_constants.INT64),
      ),
      (
          jagged_shape.create_shape([2], [2, 1]),
          ds([[2], [2, 1]], schema_constants.INT64),
      ),
      (
          jagged_shape.create_shape(
              [3],
              [2, 2, 1],
              [4, 3, 2, 2, 3],
          ),
          ds([[3], [2, 2, 1], [4, 3, 2, 2, 3]], schema_constants.INT64),
      ),
      (
          ds('hello'),
          ds([], schema_constants.INT64).repeat(0),
      ),
      (
          ds([], schema_constants.STRING),
          ds([[0]], schema_constants.INT64),
      ),
      (
          ds(['hello', 'world']),
          ds([[2]], schema_constants.INT64),
      ),
      (
          ds([['hello', 'world'], ['!']]),
          ds([[2], [2, 1]], schema_constants.INT64),
      ),
      (
          ds([['hello', 'world'], [0]]),
          ds([[2], [2, 1]], schema_constants.INT64),
      ),
      (
          bag().new(a=ds([['hello', 'world'], ['!']])),
          ds([[2], [2, 1]], schema_constants.INT64),
      ),
  )
  def test_eval(self, shape, expected_res):
    res = expr_eval.eval(kde.shapes.get_sizes(I.shape), shape=shape)
    testing.assert_equal(res, expected_res)

  def test_qtype_mismatch_error_message(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected a JaggedShape or a DataSlice, got x: INT64'
    ):
      expr_eval.eval(kde.shapes.get_sizes(I.x), x=arolla.int64(0))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.shapes.get_sizes,
        (
            (qtypes.JAGGED_SHAPE, qtypes.DATA_SLICE),
            (qtypes.DATA_SLICE, qtypes.DATA_SLICE),
        ),
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.shapes.get_sizes(I.x)))


if __name__ == '__main__':
  absltest.main()
