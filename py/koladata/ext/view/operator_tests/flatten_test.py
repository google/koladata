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
from arolla import arolla
from koladata.ext.view import kv
from koladata.ext.view import test_utils
from koladata.operators.tests.testdata import shapes_flatten_testdata


class FlattenTest(parameterized.TestCase):

  @parameterized.parameters(*shapes_flatten_testdata.TEST_DATA)
  def test_call(self, *args_and_expected):
    x = test_utils.from_ds(args_and_expected[0])
    expected = test_utils.from_ds(args_and_expected[-1])
    other_args = [
        None if isinstance(x, arolla.abc.Unspecified) else x.to_py()
        for x in args_and_expected[1:-1]
    ]
    res = kv.flatten(x, *other_args)
    test_utils.assert_equal(res, expected)

  def test_auto_boxing(self):
    self.assertEqual(kv.flatten(None).get(), (None,))
    self.assertEqual(kv.flatten('foo').get(), ('foo',))
    with self.assertRaisesRegex(ValueError, 'Cannot automatically box'):
      _ = kv.flatten([1, 2])  # pytype: disable=wrong-arg-types


if __name__ == '__main__':
  absltest.main()
