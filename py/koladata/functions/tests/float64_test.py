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
from koladata.types import schema_constants

kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class Float64Test(parameterized.TestCase):

  @parameterized.parameters(
      [1], [[1, 2, 3]], [None], [1.0], [ds(1.0, schema_constants.FLOAT64)]
  )
  def test_float64(self, x):
    testing.assert_equal(fns.float64(x), ds(x, schema_constants.FLOAT64))

  @parameterized.parameters(
      ('foo', 'cannot cast TEXT to FLOAT64'),
      (b'test', 'cannot cast BYTES to FLOAT64'),
  )
  def test_float64_errors(self, x, expected_error_msg):
    with self.assertRaisesRegex(ValueError, re.escape(expected_error_msg)):
      fns.float64(x)


if __name__ == '__main__':
  absltest.main()