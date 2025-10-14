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
from koladata.ext.konstructs import konstructs


class KonstructsTest(absltest.TestCase):

  def test_lens(self):
    # More comprehensive tests are in lens_test.py.
    self.assertEqual(
        konstructs.lens([1, 2])[:].map(lambda x: x + 1).get(), [2, 3]
    )

  def test_types(self):
    self.assertIsInstance(konstructs.lens(1), konstructs.types.Lens)


if __name__ == '__main__':
  absltest.main()
