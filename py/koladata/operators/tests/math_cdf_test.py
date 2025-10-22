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

"""Tests for kde.math.cdf operator.

Note that there are more extensive tests that reuse the existing Arolla tests
for the M.math.cdf operator.
"""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE

QTYPES = [
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
]


class MathCdfTest(parameterized.TestCase):

  @parameterized.parameters(
      # INT32
      (
          ds([3, None, 1, 4], schema_constants.INT32),
          ds([2 / 3, None, 1 / 3, 1.0], schema_constants.FLOAT32),
      ),
      # With weights
      (
          ds([2, 3, 5], schema_constants.INT32),
          ds([1, 2, 3], schema_constants.INT32),
          ds([1 / 6, 0.5, 1.0], schema_constants.FLOAT32),
      ),
      # Weights are normalized
      (
          ds([1, None, 3, 4], schema_constants.INT32),
          ds([1, 1, 1, 1], schema_constants.INT32),
          ds([1 / 3, None, 2 / 3, 1.0], schema_constants.FLOAT32),
      ),
      (
          ds([1, None, 3, 4], schema_constants.INT32),
          ds([123, 123, 123, 123], schema_constants.INT32),
          ds([1 / 3, None, 2 / 3, 1.0], schema_constants.FLOAT32),
      ),
      # INT64
      (
          ds([1, None, 3, 4], schema_constants.INT64),
          ds([1 / 3, None, 2 / 3, 1.0], schema_constants.FLOAT32),
      ),
      # FLOAT32
      (
          ds([1.0, None, 3.0, 4.0], schema_constants.FLOAT32),
          ds([1 / 3, None, 2 / 3, 1.0], schema_constants.FLOAT32),
      ),
      (
          ds([1.0, float('inf'), float('-inf'), 4.0], schema_constants.FLOAT32),
          ds([0.5, 1.0, 0.25, 0.75], schema_constants.FLOAT32),
      ),
      # FLOAT64
      (
          ds([1.0, None, 3.0, 4.0], schema_constants.FLOAT64),
          ds([1 / 3, None, 2 / 3, 1.0], schema_constants.FLOAT64),
      ),
      # Scalar value produces 1
      (ds([10], schema_constants.INT32), ds([1.0], schema_constants.FLOAT32)),
      # Multi-dimensional
      (
          ds([[1, None], [3, 4, 5], [None, None], []]),
          ds([[1.0, None], [1 / 3, 2 / 3, 1.0], [None, None], []]),
      ),
      (
          ds([[[1, None], [3, 4, 5]], [[None, None]]]),
          ds([[[1.0, None], [1 / 3, 2 / 3, 1.0]], [[None, None]]]),
      ),
      # OBJECT
      (
          ds([[2, None], [None]], schema_constants.OBJECT),
          ds([[1.0, None], [None]], schema_constants.OBJECT),
      ),
      # Empty and unknown inputs.
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          ds([[None, None], [None]], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]]),
          ds([[None, None], [None]], schema_constants.FLOAT32),
      ),
      (
          ds([[None, None], [None]], schema_constants.FLOAT32),
          ds([[None, None], [None]], schema_constants.FLOAT32),
      ),
  )
  def test_eval(self, *args_and_expected):
    args, expected_value = args_and_expected[:-1], args_and_expected[-1]
    result = kd.math.cdf(*args)
    testing.assert_equal(result.get_shape(), args[0].get_shape())
    testing.assert_equal(result, expected_value)

  @parameterized.parameters(
      (
          ds([[[1, None], [3, 4, 5]], [[None, None]]]),
          ds(0),
          ds([[[1.0, None], [1.0, 1.0, 1.0]], [[None, None]]]),
      ),
      (
          ds([[[1, None], [3, 4, 5]], [[None, None]]]),
          ds(1),
          ds([[[1.0, None], [1 / 3, 2 / 3, 1.0]], [[None, None]]]),
      ),
      (
          ds([[[1, None], [3, 4, 5]], [[None, None]]]),
          ds(2),
          ds([[[1 / 4, None], [2 / 4, 3 / 4, 1.0]], [[None, None]]]),
      ),
  )
  def test_eval_with_ndim(self, x, ndim, expected_value):
    result = kd.math.cdf(x, ndim=ndim)
    testing.assert_equal(result.get_shape(), x.get_shape())
    testing.assert_equal(result, expected_value)

  def test_eval_with_weights_and_ndim(self):
    x = ds([[2, 3], [5]])
    weights = ds([[1, 2], [3]])
    expected_value = ds([[0.16666667, 0.5], [1.0]])
    result = kd.math.cdf(x, weights, ndim=2)
    testing.assert_equal(result.get_shape(), x.get_shape())
    testing.assert_equal(result, expected_value)

  def test_data_item_input_error(self):
    x = ds(1)
    with self.assertRaisesRegex(ValueError, re.escape('expected rank(x) > 0')):
      kd.math.cdf(x)

  def test_x_and_weights_different_shapes_error(self):
    x = ds([1, 2, 3])
    weights = ds([1, 2, 3, 4])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'DataSlice with shape=JaggedShape(4) cannot be expanded to'
            ' shape=JaggedShape(3)'
        ),
    ):
      kd.math.cdf(x, weights)

  def test_mixed_slice_error(self):
    x = data_slice.DataSlice.from_vals([1, 2.0], schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'DataSlice with mixed types is not supported'
    ):
      kd.math.cdf(x)

  def test_entity_slice_error(self):
    db = data_bag.DataBag.empty_mutable()
    x = db.new(x=ds([1]))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.cdf: argument `x` must be a slice of numeric values,'
            ' got a slice of ENTITY(x=INT32)'
        ),
    ):
      kd.math.cdf(x)

  def test_object_slice_error(self):
    db = data_bag.DataBag.empty_mutable()
    x = db.obj(x=ds([1]))
    with self.assertRaisesRegex(
        ValueError,
        'kd.math.cdf: argument `x` must be a slice of numeric values, got'
        ' a slice of OBJECT',
    ):
      kd.math.cdf(x)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.math.cdf,
        QTYPES,
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.math.cdf(I.x)))


if __name__ == '__main__':
  absltest.main()
