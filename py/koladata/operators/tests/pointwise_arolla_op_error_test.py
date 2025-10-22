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
from arolla import arolla
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.types import data_bag
from koladata.types import data_slice

kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals


class PointwiseArollaOpErrorTest(absltest.TestCase):

  def test_incompatible_shapes(self):
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                'shapes are not compatible: JaggedShape(3) vs '
                'JaggedShape(2, [2, 1])'
            ),
        ),
    ):
      kd.math.subtract(ds([1, 2, 3]), ds([[1, 2], [3]]))

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                'shapes are not compatible: JaggedShape(3) vs '
                'JaggedShape(2, [2, 1])'
            ),
        ),
    ):
      kd.math.subtract(ds([None, None, None]), ds([[None, None], [None]]))

  def test_entity_input_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.subtract: argument `x` must be a slice of numeric values,'
            ' got a slice of ENTITY(x=INT32)',
        ),
    ):
      kd.math.subtract(bag().new(x=ds([1, 2, 3])), ds([1, 2, 3]))

  def test_object_input_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.subtract: argument `x` must be a slice of numeric values,'
            ' got a slice of OBJECT'
        ),
    ):
      kd.math.subtract(bag().obj(x=ds([1, 2, 3])), ds([1, 2, 3]))

  def test_mixed_types_input_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.subtract: argument `y` must be a slice of numeric values,'
            ' got a slice of OBJECT',
        ),
    ):
      kd.math.subtract(ds([1, 2, 3]), ds([[1, '2'], [3]]))

  def test_mixed_types_non_primary_input_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.strings.substr: argument `end` must be a slice of integer'
            ' values, got a slice of STRING'
        ),
    ):
      kd.strings.substr(ds(['abc', 'def']), 1, ds('2'))

  def test_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.math.subtract: argument `y` must be a slice of numeric values,'
            ' got a slice of STRING'
        ),
    ):
      kd.math.subtract(ds([1, 2, 3]), ds(['1', '2', '3']))


if __name__ == '__main__':
  absltest.main()
