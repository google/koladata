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
from koladata.ext.view import kv


class AppendTest(absltest.TestCase):

  def test_scalar_list(self):
    x = []
    kv.append(kv.view(x), 1)
    self.assertEqual(x, [1])
    kv.append(kv.view(x), 2)
    self.assertEqual(x, [1, 2])

  def test_1d_list(self):
    x = [[], [10], []]
    kv.append(kv.view(x)[:], 3)
    self.assertEqual(x, [[3], [10, 3], [3]])
    kv.append(kv.view(x)[:], kv.view([4, 5, 6])[:])
    self.assertEqual(x, [[3, 4], [10, 3, 5], [3, 6]])

  def test_2d_list(self):
    x = [[[]], [[], [1]]]
    v = kv.view(x)[:][:]
    kv.append(v, 3)
    self.assertEqual(x, [[[3]], [[3], [1, 3]]])

  def test_append_none_in_view(self):
    x = [[], None, [1]]
    v = kv.view(x)[:]
    kv.append(v, 3)
    self.assertEqual(x, [[3], None, [1, 3]])

  def test_append_none_in_value(self):
    x = [[], [1]]
    v = kv.view(x)[:]
    kv.append(v, kv.view([3, None])[:])
    self.assertEqual(x, [[3], [1, None]])

  def test_broadcasting_value(self):
    x = [[[]], [[], []]]
    v = kv.view(x)[:][:]
    kv.append(v, kv.view([4, 5])[:])
    self.assertEqual(x, [[[4]], [[5], [5]]])

  def test_multiple_values_same_list(self):
    x = []
    v = kv.view(x)
    kv.append(v, kv.view([10, 20])[:])
    self.assertEqual(x, [10, 20])
    kv.append(v, kv.view([30, 40])[:])
    self.assertEqual(x, [10, 20, 30, 40])

  def test_auto_boxing_scalar_value(self):
    x = []
    kv.append(kv.view(x), 10)
    self.assertEqual(x, [10])

  def test_no_auto_boxing_of_list(self):
    x = []
    with self.assertRaisesRegex(
        ValueError,
        re.escape('Cannot automatically box [] of type'),
    ):
      kv.append(x, kv.view(10))  # pytype: disable=wrong-arg-types

  def test_append_with_view_value(self):
    x = []
    kv.append(kv.view(x), kv.view(11))
    self.assertEqual(x, [11])

  def test_auto_boxing_none(self):
    # This should not fail.
    kv.append(None, 1)

  def test_multiple_instances_of_same_object(self):
    x = []
    kv.append(kv.view([x, x])[:], kv.view([2, 3])[:])
    self.assertEqual(x, [2, 3])


if __name__ == '__main__':
  absltest.main()
