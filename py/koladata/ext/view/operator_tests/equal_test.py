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
from koladata.operators.tests.testdata import comparison_equal_testdata


class EqualTest(parameterized.TestCase):

  @parameterized.parameters(*comparison_equal_testdata.TEST_CASES)
  def test_call(self, lhs, rhs, expected):
    lhs = (
        test_utils.from_ds(lhs) if isinstance(lhs, kd.types.DataSlice) else lhs
    )
    rhs = (
        test_utils.from_ds(rhs) if isinstance(rhs, kd.types.DataSlice) else rhs
    )
    expected = test_utils.from_ds(expected)
    res = kv.equal(lhs, rhs)
    test_utils.assert_equal(res, expected)


if __name__ == '__main__':
  absltest.main()
