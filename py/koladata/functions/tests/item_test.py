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
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import schema_constants

kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class ItemTest(parameterized.TestCase):

  @parameterized.parameters(
      (1, ds(1)),
      (ds(1), ds(1)),
      (1.5, ds(1.5)),
  )
  def test_single_arg(self, x, expected):
    testing.assert_equal(fns.item(x), expected)

  @parameterized.parameters(
      (1, schema_constants.INT32, ds(1)),
      (ds(1), schema_constants.INT64, ds(1, schema_constants.INT64)),
      (1.3, schema_constants.INT32, ds(1)),
  )
  def test_two_args(self, x, schema, expected):
    testing.assert_equal(fns.item(x, schema), expected)

  @parameterized.parameters(
      (
          mask_constants.present,
          schema_constants.INT32,
          ValueError,
          'cannot cast MASK to INT32',
      ),
      (
          [1, 2, 3],
          None,
          TypeError,
          'cannot create multi-dim DataSlice',
      ),
      (
          ds([1, 2, 3]),
          None,
          ValueError,
          'can only contain DataItems',
      ),
  )
  def test_errors(self, x, schema, expected_error, expected_error_msg):
    with self.assertRaisesRegex(expected_error, re.escape(expected_error_msg)):
      fns.item(x, schema)

  def test_alias(self):
    self.assertIs(fns.item, fns.slices.item)


if __name__ == '__main__':
  absltest.main()
