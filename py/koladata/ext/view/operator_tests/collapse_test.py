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
from arolla import arolla
from koladata.ext.view import kv
from koladata.ext.view import test_utils
from koladata.operators.tests.testdata import slices_collapse_testdata


class CollapseTest(parameterized.TestCase):

  @parameterized.parameters(*slices_collapse_testdata.TEST_CASES)
  def test_call(self, *args_and_expected):
    x, *other_args, expected = args_and_expected
    x = test_utils.from_ds(x)
    if other_args:
      other_args = [
          1 if isinstance(arg, arolla.abc.Unspecified) else arg.to_py()
          for arg in other_args
      ]
    expected = test_utils.from_ds(expected)
    result = kv.collapse(x, *other_args)
    test_utils.assert_equal(result, expected)

  def test_ndim_error(self):
    x = kv.view([1, 2, 3])[:]
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the number of dimensions to collapse must be in range [0, 1],'
            ' got 2'
        ),
    ):
      kv.collapse(x, 2)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the number of dimensions to collapse must be in range [0, 1],'
            ' got -1'
        ),
    ):
      kv.collapse(x, -1)

  def test_auto_boxing(self):
    test_utils.assert_equal(kv.collapse(57, 0), kv.view(57))
    test_utils.assert_equal(kv.collapse(None, 0), kv.view(None))


if __name__ == '__main__':
  absltest.main()
