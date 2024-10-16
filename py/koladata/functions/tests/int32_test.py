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


class Int32Test(parameterized.TestCase):

  @parameterized.parameters([1], [[1, 2, 3]], [None], [1.0], [ds(1.0)])
  def test_int32(self, x):
    testing.assert_equal(fns.int32(x), ds(x, schema_constants.INT32))

  @parameterized.parameters(
      ('foo', 'cannot cast TEXT to INT32'),
      (b'test', 'cannot cast BYTES to INT32'),
      (2**45, f'cannot cast int64{{{2**45}}} to int32')
  )
  def test_int32_errors(self, x, expected_error_msg):
    with self.assertRaisesRegex(ValueError, re.escape(expected_error_msg)):
      fns.int32(x)


if __name__ == '__main__':
  absltest.main()