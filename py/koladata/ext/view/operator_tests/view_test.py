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


class ViewTest(absltest.TestCase):

  def test_call(self):
    l = [1, 2]
    v = kv.view(l)
    self.assertIs(v.get(), l)
    self.assertEqual(v[:].get(), (1, 2))


if __name__ == '__main__':
  absltest.main()
