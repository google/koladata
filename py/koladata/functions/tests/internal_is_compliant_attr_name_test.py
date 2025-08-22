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
from koladata.functions import functions as fns


class InternalIsCompliantAttrNameTest(absltest.TestCase):

  def test_is_compliant_attr_name(self):
    self.assertTrue(fns.slices.internal_is_compliant_attr_name('foo'))
    self.assertTrue(fns.slices.internal_is_compliant_attr_name('query'))
    self.assertTrue(fns.slices.internal_is_compliant_attr_name('doc'))

    # Start with underscore.
    self.assertFalse(fns.slices.internal_is_compliant_attr_name('_foo'))
    self.assertFalse(fns.slices.internal_is_compliant_attr_name('__my_attr'))

    # Reserved names.
    self.assertFalse(fns.slices.internal_is_compliant_attr_name('fingerprint'))
    self.assertFalse(fns.slices.internal_is_compliant_attr_name('get_schema'))


if __name__ == '__main__':
  absltest.main()
