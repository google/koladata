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

I = input_container.InputContainer('I')
kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class PointwiseArollaOpErrorTest(absltest.TestCase):

  def test_incompatible_shapes(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            """operator math.subtract failed during evaluation: cannot align all inputs to a common shape

The cause is: shapes are not compatible: JaggedShape(3) vs JaggedShape(2, [2, 1])"""
        ),
    ):
      expr_eval.eval(kde.subtract(ds([1, 2, 3]), ds([[1, 2], [3]])))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            """operator math.subtract failed during evaluation: cannot align all inputs to a common shape

The cause is: shapes are not compatible: JaggedShape(3) vs JaggedShape(2, [2, 1])"""
        ),
    ):
      expr_eval.eval(
          kde.subtract(ds([None, None, None]), ds([[None, None], [None]]))
      )

  def test_entity_input_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            """operator math.subtract failed during evaluation: invalid inputs

The cause is: DataSlice with Entity schema is not supported:"""
        ),
    ):
      expr_eval.eval(kde.subtract(bag().new(x=ds([1, 2, 3])), ds([1, 2, 3])))

  def test_object_input_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            """operator math.subtract failed during evaluation: invalid inputs

The cause is: DataSlice has no primitive schema"""
        ),
    ):
      expr_eval.eval(kde.subtract(bag().obj(x=ds([1, 2, 3])), ds([1, 2, 3])))

  def test_mixed_types_input_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            """operator math.subtract failed during evaluation: invalid inputs

The cause is: DataSlice with mixed types is not supported: DataSlice([[1, '2'], [3]], schema: OBJECT, shape: JaggedShape(2, [2, 1]))"""
        ),
    ):
      expr_eval.eval(kde.subtract(ds([1, 2, 3]), ds([[1, '2'], [3]])))

  def test_mixed_types_non_primary_input_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            """operator strings.substr failed during evaluation: invalid end argument

The cause is: unsupported narrowing cast to INT64 for the given STRING DataSlice"""
        ),
    ):
      expr_eval.eval(kde.strings.substr(ds(['abc', 'def']), 1, ds('2')))

  def test_errors(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            """operator math.subtract failed during evaluation: successfully converted input DataSlice(s) to DenseArray(s) but failed to evaluate the Arolla operator

The cause is: expected numerics, got y: DENSE_ARRAY_TEXT"""
        ),
    ):
      expr_eval.eval(kde.subtract(ds([1, 2, 3]), ds(['1', '2', '3'])))


if __name__ == '__main__':
  absltest.main()