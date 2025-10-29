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
from absl.testing import parameterized
from koladata.ext.view import kv
from koladata.ext.view import test_utils
from koladata.operators.tests.testdata import slices_group_by_testdata


class GroupByTest(parameterized.TestCase):

  @parameterized.parameters(*slices_group_by_testdata.TEST_CASES)
  def test_call(self, args, kwargs, expected):
    args = [test_utils.from_ds(arg) for arg in args]
    expected = test_utils.from_ds(expected)
    result = kv.group_by(*args, **kwargs)
    print(args, kwargs, result)
    test_utils.assert_equal(result, expected)
    # Repeating the keys should be equivalent to passing them once.
    if len(args) == 1:
      result = kv.group_by(*args, *args, *args, **kwargs)
    else:
      result = kv.group_by(*args, *args[1:], *args[1:], **kwargs)
    test_utils.assert_equal(result, expected)

  def test_different_depth(self):
    with self.assertRaisesRegex(
        ValueError,
        'all arguments must have the same shape',
    ):
      kv.group_by(kv.view([[1]])[:][:], kv.view([1])[:])

  def test_different_shape(self):
    with self.assertRaisesRegex(
        ValueError,
        # Not sure if we want to improve the error message here,
        # given the experimental status of the API.
        re.escape('zip() argument 2 is longer than argument 1'),
    ):
      kv.group_by(kv.view([[1]])[:][:], kv.view([[1, None]])[:][:])

  def test_auto_boxing_and_zero_depth(self):
    with self.assertRaisesRegex(
        ValueError,
        'the argument being grouped must have at least one dimension',
    ):
      kv.group_by(1)


if __name__ == '__main__':
  absltest.main()
