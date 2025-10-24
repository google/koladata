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
from koladata.ext.view import kv
from koladata.ext.view import test_utils
from koladata.operators.tests.testdata import slices_align_testdata


class AlignTest(parameterized.TestCase):

  @parameterized.parameters(*slices_align_testdata.TEST_DATA)
  def test_call(self, args, expected):
    args = [test_utils.from_ds(x) for x in args]
    expected = [test_utils.from_ds(x) for x in expected]
    res = kv.align(*args)
    self.assertLen(res, len(expected))
    for i in range(len(res)):
      test_utils.assert_equal(res[i], expected[i])

  def test_boxing(self):
    l1 = kv.view([10, 20])[:]
    res1, res2 = kv.align(1, l1)
    self.assertEqual(res1.get(), (1, 1))
    self.assertEqual(res2.get(), (10, 20))
    res2, res1 = kv.align(l1, 1)
    self.assertEqual(res1.get(), (1, 1))
    self.assertEqual(res2.get(), (10, 20))

  def test_errors(self):
    l1 = kv.view([10, 20])[:]
    l2 = kv.view([1, 2, 3])[:]
    with self.assertRaisesRegex(
        TypeError,
        'expected all tuples to be the same length when depth > 0, got 2 and 3',
    ):
      _ = kv.align(l1, l2)

    l3 = kv.view([[1, 2], [3]])[:][:]
    with self.assertRaisesRegex(
        TypeError,
        'expected all tuples to be the same length when depth > 0, got 2 and 3',
    ):
      _ = kv.align(l3, l2)


if __name__ == '__main__':
  absltest.main()
