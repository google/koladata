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
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
])


class MathAggMeanTest(parameterized.TestCase):

  @parameterized.parameters(
      # INT32
      (
          ds([1, 2, 3, 4], schema_constants.INT32),
          ds(2.5, schema_constants.FLOAT32),
      ),
      # INT64
      (
          ds([1, 2, None, 6], schema_constants.INT64),
          ds(3.0, schema_constants.FLOAT32),
      ),
      # FLOAT32
      (
          ds([1.5, 2.5, None, 3.5], schema_constants.FLOAT32),
          ds(2.5, schema_constants.FLOAT32),
      ),
      # FLOAT64
      (
          ds([1.5, 2.5, None, 3.5], schema_constants.FLOAT64),
          ds(2.5, schema_constants.FLOAT64),
      ),
      (ds([None], schema_constants.INT32), ds(None, schema_constants.FLOAT32)),
      (ds([[1, None], [3, 4], [None, None], []]), ds([1, 3.5, None, None])),
      (
          ds([[[1, None], [3, 4]], [[None, None]]]),
          ds([[1, 3.5], [None]]),
      ),
      (
          ds([[1.0, None], [3.0, 4.0], [None, None], []]),
          ds([1.0, 3.5, None, None]),
      ),
      (
          ds([[[1.0, None], [3.0, 4.0]], [[None, None]]]),
          ds([[1.0, 3.5], [None]]),
      ),
      (
          ds([[1, None, None], [3, 4], [None, None]]),
          arolla.unspecified(),
          ds([1, 3.5, None]),
      ),
      (
          ds([[1, None, None], [3, 4], [None, None]]),
          ds(1),
          ds([1, 3.5, None]),
      ),
      (
          ds([[1, None, None], [3, 4], [None, None]]),
          ds(2),
          ds(2.6666666666666),
      ),
      (
          ds([[1, None, None], [3, 4], [None, None]]),
          ds(0),
          ds([[1.0, None, None], [3.0, 4.0], [None, None]]),
      ),
      # OBJECT
      (
          ds([[2, None], [None]], schema_constants.OBJECT),
          ds([2.0, None], schema_constants.OBJECT),
      ),
      # Empty and unknown inputs.
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]]),
          ds([None, None], schema_constants.NONE),
      ),
      (
          ds([[None, None], [None]], schema_constants.FLOAT32),
          ds([None, None], schema_constants.FLOAT32),
      ),
      (
          ds([None, None, None]),
          ds(None, schema_constants.NONE),
      ),
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
      ),
  )
  def test_eval(self, *args_and_expected):
    args, expected_value = args_and_expected[:-1], args_and_expected[-1]
    result = expr_eval.eval(kde.math.agg_mean(*args))
    testing.assert_equal(result, expected_value)

  def test_data_item_input_error(self):
    x = ds(1)
    with self.assertRaisesRegex(
        exceptions.KodaError, re.escape('expected rank(x) > 0')
    ):
      expr_eval.eval(kde.math.agg_mean(x))

  @parameterized.parameters(-1, 2)
  def test_out_of_bounds_ndim_error(self, ndim):
    x = data_slice.DataSlice.from_vals([1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'expected 0 <= ndim <= rank'):
      expr_eval.eval(kde.math.agg_mean(x, ndim=ndim))

  def test_mixed_slice_error(self):
    x = data_slice.DataSlice.from_vals([1, 2.0], schema_constants.OBJECT)
    with self.assertRaisesRegex(
        exceptions.KodaError, 'DataSlice with mixed types is not supported'
    ):
      expr_eval.eval(kde.math.agg_mean(x))

  def test_entity_slice_error(self):
    db = data_bag.DataBag.empty()
    x = db.new(x=ds([1]))
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.math.agg_mean: argument `x` must be a slice of numeric values,'
            ' got a slice of SCHEMA(x=INT32)'
        ),
    ):
      expr_eval.eval(kde.math.agg_mean(x))

  def test_object_slice_error(self):
    db = data_bag.DataBag.empty()
    x = db.obj(x=ds([1]))
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.math.agg_mean: argument `x` must be a slice of numeric values,'
            ' got a slice of OBJECT'
        ),
    ):
      expr_eval.eval(kde.math.agg_mean(x))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.math.agg_mean,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.math.agg_mean(I.x)))


if __name__ == '__main__':
  absltest.main()
