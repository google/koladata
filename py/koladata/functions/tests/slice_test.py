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


class SliceTest(parameterized.TestCase):

  @parameterized.parameters(
      (1, ds(1)),
      (ds(1), ds(1)),
      (1.5, ds(1.5)),
      ([1, 2, 3], ds([1, 2, 3])),
      ([1, 2, 3.0], ds([1, 2, 3.0])),
      ([[1, 2], [3, 4, 5]], ds([[1, 2], [3, 4, 5]])),
      (((1, 2), [3, 4, 5]), ds([[1, 2], [3, 4, 5]])),
      (ds([1, 2, 3]), ds([1, 2, 3])),
  )
  def test_single_arg(self, x, expected):
    testing.assert_equal(fns.slice(x), expected)

  @parameterized.parameters(
      (1, schema_constants.INT32, ds(1)),
      (ds(1), schema_constants.INT64, ds(1, schema_constants.INT64)),
      (1.3, schema_constants.INT32, ds(1)),
      ([1, 2, 3], schema_constants.FLOAT32, ds([1.0, 2.0, 3.0])),
  )
  def test_two_args(self, x, schema, expected):
    testing.assert_equal(fns.slice(x, schema), expected)

  @parameterized.parameters(
      (
          mask_constants.present,
          schema_constants.INT32,
          'unsupported schema: MASK',
      ),
  )
  def test_errors(self, x, schema, expected_error_msg):
    with self.assertRaisesRegex(ValueError, re.escape(expected_error_msg)):
      fns.slice(x, schema)

  def test_alias(self):
    self.assertIs(fns.slice, fns.slices.slice)


if __name__ == '__main__':
  absltest.main()
