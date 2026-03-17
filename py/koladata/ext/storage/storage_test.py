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
from koladata.ext.storage import composite_initial_data_manager as _composite_initial_data_manager
from koladata.ext.storage import global_cache_lib as _global_cache_lib
from koladata.ext.storage import storage


class StorageTest(absltest.TestCase):

  def test_contains_modules(self):
    modules = dir(storage)
    self.assertIn('data_slice_path', modules)
    self.assertIn('persisted_incremental_data_slice_manager', modules)

  def test_data_slice_manager_functionality(self):
    self.assertTrue(
        hasattr(storage, 'PersistedIncrementalDataSliceManager')
    )
    self.assertTrue(hasattr(storage, 'DataSliceManagerInterface'))
    self.assertTrue(hasattr(storage, 'DataSlicePath'))
    self.assertTrue(hasattr(storage, 'DataSliceManagerView'))

    persistence_dir = self.create_tempdir().full_path
    manager = storage.PersistedIncrementalDataSliceManager.create_new(
        persistence_dir
    )
    self.assertIsInstance(manager, storage.DataSliceManagerInterface)
    self.assertEqual(
        storage.DataSlicePath.from_actions([]),
        storage.DataSlicePath.parse_from_string(''),
    )
    kd.testing.assert_equivalent(
        manager.get_data_slice(
            populate={storage.DataSlicePath.from_actions([])}
        ),
        kd.new(),
        schemas_equality=False,
    )
    root = storage.DataSliceManagerView(manager)
    root.x = kd.item(1)
    kd.testing.assert_equivalent(
        root.x.get_data_slice(),
        kd.item(1),
    )

  def test_composite_initial_data_manager_is_exposed(self):
    self.assertTrue(hasattr(storage, 'CompositeInitialDataManager'))
    self.assertIs(
        storage.CompositeInitialDataManager,
        _composite_initial_data_manager.CompositeInitialDataManager,
    )

  def test_global_cache_is_exposed(self):
    self.assertTrue(hasattr(storage, 'get_internal_global_cache'))
    self.assertIs(
        storage.get_internal_global_cache(),
        _global_cache_lib.get_global_cache(),
    )


if __name__ == '__main__':
  absltest.main()
