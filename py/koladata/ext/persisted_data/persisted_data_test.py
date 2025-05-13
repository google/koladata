# Copyright 2024 Google LLC
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
from koladata import kd
from koladata.ext.persisted_data import persisted_data


class PersistedDataTest(absltest.TestCase):

  def test_contains_modules(self):
    modules = dir(persisted_data)
    self.assertIn('fs_interface', modules)
    self.assertIn('fs_implementation', modules)
    self.assertIn('fs_util', modules)

  def test_fs_implementation(self):
    fs = persisted_data.fs_util.get_default_file_system_interaction()
    dirname = self.create_tempdir().full_path
    self.assertTrue(fs.exists(dirname))

  def test_persisted_incremental_data_bag_manager(self):
    self.assertTrue(
        hasattr(persisted_data, 'PersistedIncrementalDataBagManager')
    )

    persistence_dir = self.create_tempdir().full_path
    dbm = persisted_data.PersistedIncrementalDataBagManager(persistence_dir)
    self.assertEqual(dbm.get_available_bag_names(), {''})
    self.assertEqual(dbm.get_loaded_bag_names(), {''})
    kd.testing.assert_equivalent(
        dbm.get_loaded_bag().merge_fallbacks(), kd.bag()
    )


if __name__ == '__main__':
  absltest.main()
