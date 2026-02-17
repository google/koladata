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
from koladata.ext.pseudo_random import pseudo_random


class PseudoRandomTest(parameterized.TestCase):

  def test_epoch_id(self):
    epoch_id = pseudo_random.epoch_id()
    self.assertNotEqual(epoch_id, 0)
    self.assertEqual(pseudo_random.epoch_id(), epoch_id)

  def test_next_uint64(self):
    n = 100000
    items = {pseudo_random.next_uint64() for _ in range(n)}
    self.assertEqual(len(items), n)  # expect no collisions  # pylint: disable=g-generic-assert

  def test_next_fingerprint(self):
    n = 100000
    items = {pseudo_random.next_fingerprint() for _ in range(n)}
    self.assertEqual(len(items), n)  # expect no collisions  # pylint: disable=g-generic-assert


if __name__ == '__main__':
  absltest.main()
