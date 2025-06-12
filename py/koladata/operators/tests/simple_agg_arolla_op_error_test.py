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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.operators import kde_operators
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class SimpleAggArollaOpErrorTest(absltest.TestCase):

  def test_mixed_types_input_error(self):
    x = data_slice.DataSlice.from_vals([1, 2.0], schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.agg_max: DataSlice with mixed types is not supported:'
            ' DataSlice([1, 2.0], schema: OBJECT, shape: JaggedShape(2))'
        ),
    ):
      expr_eval.eval(kde.agg_max(x))

  def test_expect_rank_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('kd.math.agg_max: expected rank(x) > 0'),
    ):
      expr_eval.eval(kde.agg_max(ds(1)))

  def test_arolla_operrors(self):
    x = data_slice.DataSlice.from_vals(['1', '2', '3'])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.agg_max: argument `x` must be a slice of numeric values,'
            ' got a slice of STRING'
        ),
    ):
      expr_eval.eval(kde.agg_max(x))

  def test_unbiased_non_scalar_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.agg_var: argument `unbiased` must be an item holding'
            ' BOOLEAN, got a slice of rank 1 > 0'
        ),
    ):
      expr_eval.eval(kde.math.agg_var(ds([1]), unbiased=ds([True])))


if __name__ == '__main__':
  absltest.main()
