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
from koladata.ext.view import kv


class KodaViewTest(absltest.TestCase):

  def test_map_and_view(self):
    # More comprehensive tests are in view_test.py.
    self.assertEqual(
        kv.map(lambda x: x + 1, kv.view([1, 2])[:]).get(),
        [2, 3],
    )

  def test_align(self):
    # More comprehensive tests are in view_test.py.
    res1, res2 = kv.align(
        kv.view(1),
        kv.view([10, 20])[:],
    )
    self.assertEqual(res1.get(), [1, 1])
    self.assertEqual(res2.get(), [10, 20])

  def test_types(self):
    self.assertIsInstance(kv.view(1), kv.types.View)


if __name__ == '__main__':
  absltest.main()
