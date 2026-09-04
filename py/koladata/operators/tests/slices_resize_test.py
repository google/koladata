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
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kd = kde_operators.kd
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
shape_new = jagged_shape.create_shape
DATA_SLICE = qtypes.DATA_SLICE
JAGGED_SHAPE = qtypes.JAGGED_SHAPE


class SlicesResizeTest(parameterized.TestCase):

  @parameterized.parameters(
      # 1D clip
      (
          ds([1, 2, 3, 4, 5]),
          shape_new([3]),
          ds([1, 2, 3]),
      ),
      # 1D pad
      (
          ds([1, 2]),
          shape_new([5]),
          ds([1, 2, None, None, None]),
      ),
      # 1D same size
      (
          ds([1, 2, 3]),
          shape_new([3]),
          ds([1, 2, 3]),
      ),
      # 1D zero size
      (
          ds([1, 2, 3]),
          shape_new([0]),
          ds([], schema_constants.INT32),
      ),
      # 1D empty slice pad
      (
          ds([], schema_constants.FLOAT32),
          shape_new([3]),
          ds([None, None, None], schema_constants.FLOAT32),
      ),
      # 2D uniform clip & pad (matrix square)
      (
          ds([[1, 2], [3, 4, 5, 6], [7]]),
          shape_new([4], [4, 4, 4, 4]),
          ds([
              [1, 2, None, None],
              [3, 4, 5, 6],
              [7, None, None, None],
              [None, None, None, None],
          ]),
      ),
      # 2D ragged target shape
      (
          ds([[1, 2], [3, 4, 5]]),
          shape_new([3], [3, 2, 4]),
          ds([
              [1, 2, None],
              [3, 4],
              [None, None, None, None],
          ]),
      ),
      # 2D clipping rows and columns
      (
          ds([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
          shape_new([2], [2, 2]),
          ds([[1, 2], [4, 5]]),
      ),
      # 2D empty inner dimension
      (
          ds([[1, 2], [3, 4]]),
          shape_new([2], [0, 0]),
          ds([[], []], schema_constants.INT32),
      ),
      # 2D empty rows
      (
          ds([[1, 2], [3, 4]]),
          shape_new([0], []),
          ds([], schema_constants.INT32).reshape(shape_new([0], [])),
      ),
      # 3D tensor resizing
      (
          ds([[[1, 2], [3]], [[4, 5, 6]]]),
          shape_new([2], [2, 1], [2, 2, 2]),
          ds([
              [[1, 2], [3, None]],
              [[4, 5]],
          ]),
      ),
      # Preserves missing items
      (
          ds([[1, None, 3], [4, 5]]),
          shape_new([2], [2, 3]),
          ds([[1, None], [4, 5, None]]),
      ),
      # Float values
      (
          ds([[1.0, 2.0], [3.0]]),
          shape_new([2], [3, 3]),
          ds([[1.0, 2.0, None], [3.0, None, None]]),
      ),
      # String values
      (
          ds(['a', 'b', 'c']),
          shape_new([4]),
          ds(['a', 'b', 'c', None]),
      ),
      # OBJECT schema
      (
          ds([1, 2, 3], schema_constants.OBJECT),
          shape_new([4]),
          ds([1, 2, 3, None], schema_constants.OBJECT),
      ),
      # Mixed data
      (
          ds([1, 'a', 3.0]),
          shape_new([4]),
          ds([1, 'a', 3.0, None]),
      ),
      (
          ds([[1, 'a'], [3.0]]),
          shape_new([2], [3, 2]),
          ds([[1, 'a', None], [3.0, None]]),
      ),
      # NONE schema
      (
          ds([None, None, None]),
          shape_new([4]),
          ds([None, None, None, None]),
      ),
      (
          ds([[None, None], [None]]),
          shape_new([2], [3, 2]),
          ds([[None, None, None], [None, None]]),
      ),
      # OBJECT schema with all missing values
      (
          ds([None, None, None], schema_constants.OBJECT),
          shape_new([4]),
          ds([None, None, None, None], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          shape_new([2], [3, 2]),
          ds([[None, None, None], [None, None]], schema_constants.OBJECT),
      ),
      # Rank 0 scalar
      (
          ds(1),
          shape_new(),
          ds(1),
      ),
  )
  def test_eval(self, x, shape, expected):
    result = kd.slices.resize(x, shape)
    testing.assert_equal(result, expected)
    testing.assert_equal(kd.resize(x, shape), expected)

  def test_dataslice_method(self):
    x = ds([[1, 2, 3], [4, 5], [6]])
    result = x.resize(shape_new([3], [3, 3, 3]))
    expected = ds([[1, 2, 3], [4, 5, None], [6, None, None]])
    testing.assert_equal(result, expected)

  def test_preserves_databag(self):
    db = data_bag.DataBag.empty_mutable()
    obj = db.obj(a=1, b=2)
    x = ds([obj])
    result = kd.resize(x, shape_new([2]))
    testing.assert_equal_by_fingerprint(result.get_bag(), x.get_bag())
    self.assertEqual(result.get_schema(), x.get_schema())
    testing.assert_equal(result.S[0].a.no_bag(), ds(1))
    testing.assert_equal(
        result.S[1].a.no_bag(), ds(None, schema_constants.NONE)
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.resize,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
        ),
        frozenset([(DATA_SLICE, JAGGED_SHAPE, DATA_SLICE)]),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.resize(I.x, I.shape)))
    self.assertTrue(view.has_koda_view(I.x.resize(I.shape)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.resize, kde.resize))

  def test_expr_eval(self):
    x = ds([[1, 2, 3], [4, 5], [6]])
    shape = shape_new([3], [3, 3, 3])
    expr = kde.slices.resize(I.x, I.shape)
    result = expr_eval.eval(expr, x=x, shape=shape)
    expected = ds([[1, 2, 3], [4, 5, None], [6, None, None]])
    testing.assert_equal(result, expected)

  def test_rank_mismatch_error(self):
    with self.assertRaisesRegex(
        ValueError, r'rank of `x` must match rank of `shape`: 1 vs 2'
    ):
      kd.slices.resize(ds([1, 2, 3]), shape_new([2], [2, 1]))

    with self.assertRaisesRegex(
        ValueError, r'rank of `x` must match rank of `shape`: 0 vs 1'
    ):
      kd.slices.resize(ds(1), shape_new([2]))

    with self.assertRaisesRegex(
        ValueError, r'rank of `x` must match rank of `shape`: 2 vs 1'
    ):
      kd.slices.resize(ds([[1, 2], [3, 4]]), shape_new([4]))


if __name__ == '__main__':
  absltest.main()
