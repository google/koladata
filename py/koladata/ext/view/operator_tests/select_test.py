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
from koladata.operators.tests.testdata import slices_select_testdata


class SelectTest(parameterized.TestCase):

  @parameterized.parameters(*slices_select_testdata.TEST_CASES)
  def test_call(self, values, filter_arr, kwargs, expected):
    values = test_utils.from_ds(values)
    filter_arr = test_utils.from_ds(filter_arr)
    expected = test_utils.from_ds(expected)
    result = kv.select(values, filter_arr, **kwargs)
    test_utils.assert_equal(result, expected)

  def test_filter_higher_depth_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'the filter cannot have a higher depth than the data being selected',
    ):
      kv.select(kv.view([1])[:], kv.view([[1]])[:][:])

  def test_select_on_scalar_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'cannot select from a scalar view, maybe use .flatten() first?'
        ),
    ):
      kv.select(kv.view(1), kv.view(kv.present), expand_filter=False)

  def test_scalar_filter_expand_filter_false_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'the filter must have at least one dimension when expand_filter=False',
    ):
      kv.select(kv.view([1])[:], kv.view(kv.present), expand_filter=False)

  def test_zip_strict_error(self):
    with self.assertRaisesRegex(
        ValueError,
        # Not sure if we want to improve the error message here,
        # given the experimental status of the API.
        re.escape('zip() argument 2 is shorter than argument 1'),
    ):
      kv.select(
          kv.view([1, 2])[:], kv.view([kv.present])[:], expand_filter=False
      )

  def test_wrong_filter_type(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the filter must have only kv.present and None values, got: 1'
        ),
    ):
      kv.select(kv.view([1])[:], kv.view([1])[:])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the filter must have only kv.present and None values, got: True'
        ),
    ):
      kv.select(kv.view([1])[:], kv.view([True])[:])


if __name__ == '__main__':
  absltest.main()
