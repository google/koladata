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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreAggCountTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([[1, None, None], [3.0, 4], [None, None]]),
          ds([1, 2, 0], schema_constants.INT64),
      ),
      (
          ds([[1, None, None], [3.0, 4], [None, None]]),
          arolla.unspecified(),
          ds([1, 2, 0], schema_constants.INT64),
      ),
      (
          ds([[1, None, None], [3, 4], [None, None]]),
          ds(2),
          ds(3, schema_constants.INT64),
      ),
      (
          ds([[1, None, None], [3, 4], [None, None]]),
          ds(1),
          ds([1, 2, 0], schema_constants.INT64),
      ),
      (
          ds([[1, None, None], [3.0, 4], [None, None]]),
          ds(0),
          ds([[1, 0, 0], [1, 1], [0, 0]], schema_constants.INT64),
      ),
      (ds([1, None, 'b']), ds(2, schema_constants.INT64)),
      (ds([None], schema_constants.INT64), ds(0, schema_constants.INT64)),
      (ds(['a', None]), ds(1, schema_constants.INT64)),
      (
          ds([[[1, None], [3, 4]], [[None, None]]]),
          ds([[1, 2], [0]], schema_constants.INT64),
      ),
  )
  def test_eval(self, *args_and_expected):
    args, expected_value = args_and_expected[:-1], args_and_expected[-1]
    result = expr_eval.eval(kde.slices.agg_count(*args))
    testing.assert_equal(result, expected_value)

  def test_data_item_input_error(self):
    x = ds(1)
    with self.assertRaisesRegex(
        ValueError, re.escape('expected rank > 0, but got rank = 0')
    ):
      expr_eval.eval(kde.slices.agg_count(x))

  @parameterized.parameters(-1, 2)
  def test_out_of_bounds_ndim_error(self, ndim):
    x = data_slice.DataSlice.from_vals([1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'expected 0 <= ndim <= rank'):
      expr_eval.eval(kde.slices.agg_count(x, ndim))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.agg_count,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.agg_count(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.agg_count, kde.agg_count))


if __name__ == '__main__':
  absltest.main()
