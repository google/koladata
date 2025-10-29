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

from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd
from koladata.ext.view import kv
from koladata.ext.view import test_utils
from koladata.operators.tests.testdata import masking_apply_mask_testdata


class ApplyMaskTest(parameterized.TestCase):

  @parameterized.parameters(*masking_apply_mask_testdata.TEST_CASES)
  def test_call(self, x, y, expected):
    x = test_utils.from_ds(x) if isinstance(x, kd.types.DataSlice) else x
    y = test_utils.from_ds(y) if isinstance(y, kd.types.DataSlice) else y
    expected = test_utils.from_ds(expected)
    res = kv.apply_mask(x, y)
    test_utils.assert_equal(res, expected)

  def test_error(self):
    a = kv.view(1)
    b = kv.view(2)
    with self.assertRaisesRegex(
        ValueError,
        'the second argument of & must have only kv.present and None values,'
        ' got: 2',
    ):
      kv.apply_mask(a, b)


if __name__ == '__main__':
  absltest.main()
