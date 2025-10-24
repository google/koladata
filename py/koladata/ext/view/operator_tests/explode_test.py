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
from koladata.operators.tests.testdata import lists_explode_testdata

Obj = types.SimpleNamespace


class ExplodeTest(parameterized.TestCase):

  @parameterized.parameters(*lists_explode_testdata.TEST_CASES)
  def test_call(self, x, ndim, expected):
    if ndim < 0:
      # We do not support ndim=-1, because for example for strings Python
      # allows to explode them infinitely:
      # kd.view('abc').explode(ndim=2).get() --> (('a',), ('b',), ('c',))
      return
    x = test_utils.from_ds(x)
    expected = test_utils.from_ds(expected)
    res = kv.explode(x, ndim=ndim)
    test_utils.assert_equal(res, expected)

  def test_errors(self):
    x_mix = [[1], None, [2, 3]]
    with self.assertRaisesRegex(
        ValueError,
        'the number of dimensions to explode must be non-negative, got -1',
    ):
      _ = kv.explode(kv.view(x_mix), ndim=-1)
    with self.assertRaisesRegex(TypeError, "'int' object is not iterable"):
      _ = kv.explode(kv.view(x_mix), ndim=3)

  def test_auto_boxing(self):
    self.assertEqual(kv.explode(None).get(), ())
    with self.assertRaisesRegex(ValueError, 'Cannot automatically box'):
      _ = kv.explode([1, 2])  # pytype: disable=wrong-arg-types


if __name__ == '__main__':
  absltest.main()
