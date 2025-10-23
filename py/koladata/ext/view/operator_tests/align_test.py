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


class AlignTest(absltest.TestCase):

  def test_call(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('align() missing 1 required positional argument')
    ):
      kv.align()  # pytype: disable=missing-parameter

    l1 = kv.view(1)
    (res1,) = kv.align(l1)
    self.assertEqual(res1.get(), 1)

    l2 = kv.view([10, 20])[:]
    res1, res2 = kv.align(l1, l2)
    self.assertEqual(res1.get(), (1, 1))
    self.assertEqual(res2.get(), (10, 20))

    res2, res1 = kv.align(l2, l1)
    self.assertEqual(res1.get(), (1, 1))
    self.assertEqual(res2.get(), (10, 20))

    l3 = kv.view([30, 40])[:]
    res2, res3 = kv.align(l2, l3)
    self.assertEqual(res2.get(), (10, 20))
    self.assertEqual(res3.get(), (30, 40))

    l4 = kv.view([[1, 2], [3]])[:][:]
    res2, res4 = kv.align(l2, l4)
    self.assertEqual(res2.get(), ((10, 10), (20,)))
    self.assertEqual(res4.get(), ((1, 2), (3,)))

    l5 = kv.view([1, 2, 3])[:]
    res1, res5 = kv.align(l1, l5)
    self.assertEqual(res1.get(), (1, 1, 1))
    self.assertEqual(res5.get(), (1, 2, 3))

    res1, res2 = kv.align(1, l2)
    self.assertEqual(res1.get(), (1, 1))
    self.assertEqual(res2.get(), (10, 20))
    res2, res1 = kv.align(l2, 1)
    self.assertEqual(res1.get(), (1, 1))
    self.assertEqual(res2.get(), (10, 20))

    with self.assertRaisesRegex(
        TypeError,
        'expected all tuples to be the same length when depth > 0, got 2 and 3',
    ):
      _ = kv.align(l2, l5)
    with self.assertRaisesRegex(
        TypeError,
        'expected all tuples to be the same length when depth > 0, got 2 and 3',
    ):
      _ = kv.align(l4, l5)


if __name__ == '__main__':
  absltest.main()
