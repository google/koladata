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
from koladata import kd
from koladata.ext.persisted_data import composite_initial_data_manager as _composite_initial_data_manager
from koladata.ext.persisted_data import persisted_data


class PersistedDataTest(absltest.TestCase):

  def test_contains_modules(self):
    modules = dir(persisted_data)
    self.assertIn('fs_interface', modules)
    self.assertIn('fs_implementation', modules)
    self.assertIn('fs_util', modules)
    self.assertIn('data_slice_path', modules)
    self.assertIn('persisted_incremental_data_slice_manager', modules)

  def test_fs_implementation(self):
    fs = persisted_data.fs_util.get_default_file_system_interaction()
    dirname = self.create_tempdir().full_path
    self.assertTrue(fs.exists(dirname))

  def test_persisted_incremental_data_bag_manager(self):
    self.assertTrue(hasattr(persisted_data, 'BagToAdd'))
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
    bag1 = kd.new(a=1).get_bag()
    bag2 = kd.new(a=2).get_bag()
    dbm.add_bags([
        persisted_data.BagToAdd(
            'bag1',
            bag1,
            dependencies=('',),
        ),
        persisted_data.BagToAdd(
            'bag2',
            bag2,
            dependencies=('bag1',),
        ),
    ])
    self.assertEqual(dbm.get_available_bag_names(), {'', 'bag1', 'bag2'})
    self.assertEqual(dbm.get_loaded_bag_names(), {'', 'bag1', 'bag2'})
    kd.testing.assert_equivalent(
        dbm.get_loaded_bag().merge_fallbacks(),
        kd.bags.updated(bag1, bag2).merge_fallbacks(),
    )

  def test_data_slice_manager_functionality(self):
    self.assertTrue(
        hasattr(persisted_data, 'PersistedIncrementalDataSliceManager')
    )
    self.assertTrue(hasattr(persisted_data, 'DataSliceManagerInterface'))
    self.assertTrue(hasattr(persisted_data, 'DataSlicePath'))
    self.assertTrue(hasattr(persisted_data, 'DataSliceManagerView'))

    persistence_dir = self.create_tempdir().full_path
    manager = persisted_data.PersistedIncrementalDataSliceManager.create_new(
        persistence_dir
    )
    self.assertIsInstance(manager, persisted_data.DataSliceManagerInterface)
    self.assertEqual(
        persisted_data.DataSlicePath.from_actions([]),
        persisted_data.DataSlicePath.parse_from_string(''),
    )
    kd.testing.assert_equivalent(
        manager.get_data_slice(
            populate={persisted_data.DataSlicePath.from_actions([])}
        ),
        kd.new(),
        schemas_equality=False,
    )
    root = persisted_data.DataSliceManagerView(manager)
    root.x = kd.item(1)
    kd.testing.assert_equivalent(
        root.x.get_data_slice(),
        kd.item(1),
    )

  def test_composite_initial_data_manager_is_exposed(self):
    self.assertTrue(hasattr(persisted_data, 'CompositeInitialDataManager'))
    self.assertIs(
        persisted_data.CompositeInitialDataManager,
        _composite_initial_data_manager.CompositeInitialDataManager,
    )


if __name__ == '__main__':
  absltest.main()
