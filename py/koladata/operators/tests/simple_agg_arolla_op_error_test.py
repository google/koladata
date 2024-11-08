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
from koladata.exceptions import exceptions
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
        exceptions.KodaError,
        re.escape(
            """operator math.max failed during evaluation: invalid inputs

The cause is: DataSlice with mixed types is not supported: DataSlice([1, 2.0], schema: OBJECT, shape: JaggedShape(2))"""
        ),
    ):
      expr_eval.eval(kde.agg_max(x))

  def test_entity_input_error(self):
    db = data_bag.DataBag.empty()
    x = db.new(x=ds([1]))
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            """operator math.max failed during evaluation: invalid inputs

The cause is: DataSlice with Entity schema is not supported:"""
        ),
    ):
      expr_eval.eval(kde.agg_max(x))

  def test_object_slice_error(self):
    db = data_bag.DataBag.empty()
    x = db.obj(x=ds([1]))
    with self.assertRaisesRegex(
        exceptions.KodaError,
        """operator math.max failed during evaluation: invalid inputs

The cause is: DataSlice has no primitive schema""",
    ):
      expr_eval.eval(kde.math.agg_max(x))

  def test_expect_rank_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            """operator math.max failed during evaluation: expected rank(x) > 0"""
        ),
    ):
      expr_eval.eval(kde.agg_max(ds(1)))

  def test_arolla_operrors(self):
    x = data_slice.DataSlice.from_vals(['1', '2', '3'])
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            """operator math.max failed during evaluation: successfully converted input DataSlice(s) to DenseArray(s) but failed to evaluate the Arolla operator

The cause is: expected numerics, got x: DENSE_ARRAY_TEXT;"""
        ),
    ):
      expr_eval.eval(kde.agg_max(x))

  def test_unbiased_non_scalar_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            """operator math.var failed during evaluation: expected unbiased to be a scalar boolean value, got DataSlice([True], schema: BOOLEAN, shape: JaggedShape(1))"""
        ),
    ):
      expr_eval.eval(kde.math.agg_var(ds([1]), unbiased=ds([True])))


if __name__ == '__main__':
  absltest.main()