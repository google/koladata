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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
]


class MathAggMedianTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds([1, 2, None, 3]), ds(2)),
      (ds([None], schema_constants.INT32), ds(None, schema_constants.INT32)),
      (ds([[3, None], [5, 4, 9], [None, None], []]), ds([3, 5, None, None])),
      (
          ds([[[3, None], [5, 4, 9]], [[None, None]]]),
          ds([[3, 5], [None]]),
      ),
      (
          ds([[3.0, None], [5.0, 4.0, 9.0], [None, None], []]),
          ds([3.0, 5.0, None, None]),
      ),
      (
          ds([[[3.0, None], [5.0, 4.0, 9.0]], [[None, None]]]),
          ds([[3.0, 5.0], [None]]),
      ),
      (
          ds([[3, None, None], [5, 4, 9], [None, None]]),
          arolla.unspecified(),
          ds([3, 5, None]),
      ),
      (
          ds([[3, None, None], [5, 4, 9], [None, None]]),
          ds(1),
          ds([3, 5, None]),
      ),
      (
          ds([[3, None, None], [5, 4, 9], [None, None]]),
          ds(2),
          ds(4),
      ),
      (
          ds([[3, None, None], [5, 4, 6], [None, None]]),
          ds(0),
          ds([[3, None, None], [5, 4, 6], [None, None]]),
      ),
      # Missing values don't count towards the median.
      (
          ds([None, None, None, None, 5, 4, 9]),
          ds(5),
      ),
      # With even number of values,
      # the median is the next value down from the middle.
      (
          ds([1, 2]),
          ds(1),
      ),
      # OBJECT
      (
          ds([[2, None], [None]], schema_constants.OBJECT),
          ds([2, None], schema_constants.OBJECT),
      ),
      # Empty and unknown inputs.
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]]),
          ds([None, None]),
      ),
      (
          ds([[None, None], [None]], schema_constants.FLOAT32),
          ds([None, None], schema_constants.FLOAT32),
      ),
      (
          ds([None, None, None]),
          ds(None),
      ),
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
      ),
  )
  def test_eval(self, *args_and_expected):
    args, expected_value = args_and_expected[:-1], args_and_expected[-1]
    result = kd.math.agg_median(*args)
    testing.assert_equal(result, expected_value)

  def test_data_item_input_error(self):
    x = ds(1)
    with self.assertRaisesRegex(ValueError, re.escape('expected rank(x) > 0')):
      kd.math.agg_median(x)

  @parameterized.parameters(-1, 2)
  def test_out_of_bounds_ndim_error(self, ndim):
    x = data_slice.DataSlice.from_vals([1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'expected 0 <= ndim <= rank'):
      kd.math.agg_median(x, ndim=ndim)

  def test_mixed_slice_error(self):
    x = data_slice.DataSlice.from_vals([1, 2.0], schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'DataSlice with mixed types is not supported'
    ):
      kd.math.agg_median(x)

  def test_entity_slice_error(self):
    db = data_bag.DataBag.empty_mutable()
    x = db.new(x=ds([1]))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.agg_median: argument `x` must be a slice of numeric'
            ' values, got a slice of ENTITY(x=INT32)'
        ),
    ):
      kd.math.agg_median(x)

  def test_object_slice_error(self):
    db = data_bag.DataBag.empty_mutable()
    x = db.obj(x=ds([1]))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.agg_median: argument `x` must be a slice of numeric'
            ' values, got a slice of OBJECT'
        ),
    ):
      kd.math.agg_median(x)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.math.agg_median,
        QTYPES,
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.math.agg_median(I.x)))


if __name__ == '__main__':
  absltest.main()
