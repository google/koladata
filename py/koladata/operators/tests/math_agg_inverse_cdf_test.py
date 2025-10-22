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

"""Tests for kde.math.agg_inverse_cdf operator.

Note that there are more extensive tests that reuse the existing Arolla tests
for the M.math.inverse_cdf operator.
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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
]


class MathAggInverseCdfTest(parameterized.TestCase):

  @parameterized.parameters(
      # INT32
      (
          ds([7, 9, 4, 1, 13, 2], schema_constants.INT32),
          ds(0.1),
          ds(1, schema_constants.INT32),
      ),
      (
          ds([7, 9, 4, 1, 13, 2], schema_constants.INT32),
          ds(1),
          ds(13, schema_constants.INT32),
      ),
      # INT64
      (
          ds([7, 9, 4, 1, 13, 2], schema_constants.INT64),
          ds(0.1),
          ds(1, schema_constants.INT64),
      ),
      # FLOAT32
      (
          ds([7, 9, 4, 1, 13, 2], schema_constants.FLOAT32),
          ds(0.1),
          ds(1.0, schema_constants.FLOAT32),
      ),
      (
          ds([7, 9, 4, 1, 13, 2], schema_constants.FLOAT32),
          ds(0.4),
          ds(4.0, schema_constants.FLOAT32),
      ),
      # FLOAT64
      (
          ds([7, 9, 4, 1, 13, 2], schema_constants.FLOAT64),
          ds(0.1),
          ds(1.0, schema_constants.FLOAT64),
      ),
      (
          ds([1.0, float('inf'), float('-inf'), 4.0], schema_constants.FLOAT32),
          ds(0.1),
          ds(float('-inf'), schema_constants.FLOAT32),
      ),
      (
          ds([1.0, float('nan'), 1.0, 4.0], schema_constants.FLOAT32),
          ds(0.1),
          ds(float('nan'), schema_constants.FLOAT32),
      ),
      # Scalar value produces the same value
      (
          ds([10], schema_constants.INT32),
          ds(0.1),
          ds(10, schema_constants.INT32),
      ),
      # Multi-dimensional
      (
          ds([[1, None], [3, 4, 5], [None, None], []]),
          ds(0.1),
          ds([1, 3, None, None]),
      ),
      (
          ds([[[1, None], [3, 4, 5]], [[None, None]]]),
          ds(0.1),
          ds([[1, 3], [None]]),
      ),
      # OBJECT
      (
          ds([[2, None], [None]], schema_constants.OBJECT),
          ds(0.1, schema_constants.OBJECT),
          ds([2, None], schema_constants.OBJECT),
      ),
      # Empty and unknown inputs.
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          ds(0.1),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]]),
          ds(0.1),
          ds([None, None], schema_constants.NONE),
      ),
      (
          ds([[None, None], [None]], schema_constants.FLOAT32),
          ds(0.1),
          ds([None, None], schema_constants.FLOAT32),
      ),
  )
  def test_eval(self, x, cdf_arg, expected_value):
    result = kd.math.agg_inverse_cdf(x, cdf_arg)
    self.assertEqual(result.get_shape().rank(), x.get_shape().rank() - 1)
    testing.assert_equal(result, expected_value)

  @parameterized.parameters(
      (
          ds([[[1, None], [3, 4, 5]], [[None, None]]]),
          ds(0),
          ds([[[1, None], [3, 4, 5]], [[None, None]]]),
      ),
      (
          ds([[[1, None], [3, 4, 5]], [[None, None]]]),
          ds(1),
          ds([[1, 3], [None]]),
      ),
      (
          ds([[[1, None], [3, 4, 5]], [[None, None]]]),
          ds(2),
          ds([1, None]),
      ),
  )
  def test_eval_with_ndim(self, x, ndim, expected_value):
    result = kd.math.agg_inverse_cdf(x, cdf_arg=ds(0.1), ndim=ndim)
    self.assertEqual(result.get_shape().rank(), x.get_shape().rank() - ndim)
    testing.assert_equal(result, expected_value)

  def test_data_item_input_error(self):
    x = ds(1)
    with self.assertRaisesRegex(ValueError, re.escape('expected rank(x) > 0')):
      kd.math.agg_inverse_cdf(x, ds(0.1))

  def test_wrong_cdf_arg_error(self):
    x = ds([7, 9, 4, 1, 13, 2], schema_constants.INT32)
    with self.assertRaisesRegex(
        ValueError, re.escape('invalid cdf_arg, cdf_arg must be in [0, 1]')
    ):
      kd.math.agg_inverse_cdf(x, ds(float('nan')))

    with self.assertRaisesRegex(
        ValueError, re.escape('invalid cdf_arg, cdf_arg must be in [0, 1]')
    ):
      kd.math.agg_inverse_cdf(x, ds(float('inf')))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected `cdf_arg` argument to contain a scalar float value, got'
            ' DataSlice([0.1, 0.2]'
        ),
    ):
      kd.math.agg_inverse_cdf(x, ds([0.1, 0.2]))

  def test_no_cdf_arg_error(self):
    x = ds([1, 2, 3])
    with self.assertRaisesRegex(
        TypeError,
        re.escape("missing 1 required positional argument: 'cdf_arg'"),
    ):
      kd.math.agg_inverse_cdf(x)

  def test_mixed_slice_error(self):
    x = data_slice.DataSlice.from_vals([1, 2.0], schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'DataSlice with mixed types is not supported'
    ):
      kd.math.agg_inverse_cdf(x, ds(0.1))

  def test_entity_slice_error(self):
    db = data_bag.DataBag.empty_mutable()
    x = db.new(x=ds([1]))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.agg_inverse_cdf: argument `x` must be a slice of numeric'
            ' values, got a slice of ENTITY(x=INT32)'
        ),
    ):
      kd.math.agg_inverse_cdf(x, ds(0.1))

  def test_object_slice_error(self):
    db = data_bag.DataBag.empty_mutable()
    x = db.obj(x=ds([1]))
    with self.assertRaisesRegex(
        ValueError,
        'kd.math.agg_inverse_cdf: argument `x` must be a slice of numeric'
        ' values, got a slice of OBJECT',
    ):
      kd.math.agg_inverse_cdf(x, ds(0.1))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.math.agg_inverse_cdf,
        QTYPES,
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.math.agg_inverse_cdf(I.x, I.cdf_arg))
    )


if __name__ == '__main__':
  absltest.main()
