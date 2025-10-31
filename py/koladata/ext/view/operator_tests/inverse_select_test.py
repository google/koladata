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
from koladata.operators.tests.testdata import slices_inverse_select_testdata


class InverseSelectTest(parameterized.TestCase):

  @parameterized.parameters(*slices_inverse_select_testdata.TEST_CASES)
  def test_call(self, values, fltr, expected):
    values = test_utils.from_ds(values)
    fltr = test_utils.from_ds(fltr)
    expected = test_utils.from_ds(expected)
    result = kv.inverse_select(values, fltr)
    test_utils.assert_equal(result, expected)

  def test_filter_different_depth_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'the filter must have the same depth as the data being selected',
    ):
      kv.inverse_select(kv.view([1])[:], kv.view([[1]])[:][:])

  def test_inverse_select_on_scalar_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'cannot select from a scalar view, maybe use .flatten() first?'
        ),
    ):
      kv.inverse_select(kv.view(1), kv.view(kv.present))

  def test_wrong_filter_type(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the filter must have only kv.present and None values, got: 1'
        ),
    ):
      kv.inverse_select(kv.view([1])[:], kv.view([1])[:])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the filter must have only kv.present and None values, got: True'
        ),
    ):
      kv.inverse_select(kv.view([kv.present])[:], kv.view([True])[:])

  def test_length_mismatch_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'the number of items in the data does not match the number of present'
        ' values in the filter',
    ):
      kv.inverse_select(kv.view([1])[:], kv.view([kv.present, kv.present])[:])
    with self.assertRaisesRegex(
        ValueError,
        'the number of items in the data does not match the number of present'
        ' values in the filter',
    ):
      kv.inverse_select(kv.view([1, 2])[:], kv.view([kv.present])[:])


if __name__ == '__main__':
  absltest.main()
