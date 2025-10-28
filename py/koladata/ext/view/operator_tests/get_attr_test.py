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

import types

from absl.testing import absltest
from absl.testing import parameterized
from koladata.ext.view import kv
from koladata.ext.view import test_utils
from koladata.operators.tests.testdata import core_get_attr_testdata

Obj = types.SimpleNamespace


class GetAttrTest(parameterized.TestCase):

  @parameterized.parameters(*core_get_attr_testdata.TEST_CASES)
  def test_call(self, *args_and_expected):
    x, *other_args, expected = args_and_expected
    x = test_utils.from_ds(x)
    expected = test_utils.from_ds(expected)
    test_utils.assert_equal(
        kv.get_attr(x, *other_args),
        expected,
    )

  def test_unboxes_default(self):
    x = Obj(a=[Obj(b=1), Obj()])
    self.assertEqual(
        kv.get_attr(kv.view(x).a[:], 'b', kv.view(42)).get(), (1, 42)
    )

  def test_non_scalar_default(self):
    x = Obj(a=[Obj(b=1), Obj()])
    with self.assertRaisesRegex(
        ValueError,
        'Default value for get_attr must be a scalar, got a view with depth 1',
    ):
      _ = kv.get_attr(kv.view(x).a[:], 'b', kv.view([42, 43])[:])

  def test_missing_attr_fails(self):
    x = Obj(a=[Obj(b=1), Obj()])
    with self.assertRaisesRegex(AttributeError, "object has no attribute 'b'"):
      _ = kv.get_attr(kv.view(x).a[:], 'b')

  def test_auto_boxing(self):
    self.assertIsNone(kv.get_attr(None, 'a').get())
    with self.assertRaisesRegex(ValueError, 'Cannot automatically box'):
      _ = kv.get_attr(Obj(a=1), 'a')  # pytype: disable=wrong-arg-types


if __name__ == '__main__':
  absltest.main()
