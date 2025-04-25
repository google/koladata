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

import os
import re
import shutil
from absl.testing import absltest
from koladata import kd
from koladata.ext import persisted_incremental_databag_manager

pidm = persisted_incremental_databag_manager


class PersistedIncrementalDatabagManagerTest(absltest.TestCase):

  def assert_equivalent_bags(self, bag0, bag1):
    kd.testing.assert_equivalent(bag0.merge_fallbacks(), bag1.merge_fallbacks())

  def test_typical_usage(self):
    persistence_dir = os.path.join(self.create_tempdir().full_path, 'bags')
    o = kd.new(a=1, b=2, c=3)

    manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
    self.assertEqual(manager.get_available_bag_names(), {''})
    self.assertEqual(manager.get_loaded_bag_names(), {''})
    self.assert_equivalent_bags(manager.get_bag(), kd.bag())
    self.assert_equivalent_bags(manager.get_bag(''), kd.bag())
    self.assertEqual(manager._metadata, {'version': '1.0.0'})

    bag0 = o.get_bag()
    manager.add_bag('bag0', bag0, dependencies=[''])
    self.assertEqual(manager.get_available_bag_names(), {'', 'bag0'})
    self.assertEqual(manager.get_loaded_bag_names(), {'', 'bag0'})
    self.assert_equivalent_bags(manager.get_bag(), bag0)
    self.assert_equivalent_bags(manager.get_bag('bag0'), bag0)
    self.assertEqual(manager._metadata, {'version': '1.0.0'})

    bag1 = kd.attrs(o, c=4, d=5, e=6)
    manager.add_bag('bag1', bag1, dependencies=['bag0'])
    self.assertEqual(manager.get_available_bag_names(), {'', 'bag0', 'bag1'})
    self.assertEqual(manager.get_loaded_bag_names(), {'', 'bag0', 'bag1'})
    expected_loaded_bags = kd.bags.updated(bag0, bag1)
    self.assert_equivalent_bags(manager.get_bag(''), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag0'), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag1'), expected_loaded_bags)

    bag2 = kd.attrs(o, e=7, f=8)
    manager.add_bag('bag2', bag2, dependencies=['bag1'])
    self.assertEqual(
        manager.get_available_bag_names(), {'', 'bag0', 'bag1', 'bag2'}
    )
    self.assertEqual(
        manager.get_loaded_bag_names(), {'', 'bag0', 'bag1', 'bag2'}
    )
    expected_loaded_bags = kd.bags.updated(bag0, bag1, bag2)
    self.assert_equivalent_bags(manager.get_bag(''), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag0'), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag1'), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag2'), expected_loaded_bags)

    bag3 = kd.attrs(o, d=9, g=10, h=11)
    manager.add_bag('bag3', bag3, dependencies=['bag1'])
    self.assertEqual(
        manager.get_available_bag_names(), {'', 'bag0', 'bag1', 'bag2', 'bag3'}
    )
    self.assertEqual(
        manager.get_loaded_bag_names(), {'', 'bag0', 'bag1', 'bag2', 'bag3'}
    )
    expected_loaded_bags = kd.bags.updated(bag0, bag1, bag2, bag3)
    self.assert_equivalent_bags(manager.get_bag(''), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag0'), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag1'), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag2'), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag3'), expected_loaded_bags)

    bag4 = kd.attrs(o, i=12, j=13)
    manager.add_bag('bag4', bag4, dependencies=['bag0'])
    self.assertEqual(
        manager.get_available_bag_names(),
        {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4'},
    )
    self.assertEqual(
        manager.get_loaded_bag_names(),
        {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4'},
    )
    expected_loaded_bags = kd.bags.updated(bag0, bag1, bag2, bag3, bag4)
    self.assert_equivalent_bags(manager.get_bag(''), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag0'), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag1'), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag2'), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag3'), expected_loaded_bags)
    self.assert_equivalent_bags(manager.get_bag('bag4'), expected_loaded_bags)

    # Each of the following subtests will initialize a new manager from the same
    # persistence_dir that was populated above.

    with self.subTest('InitializeFromPersistenceDir'):
      manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
      self.assertEqual(
          manager.get_available_bag_names(),
          {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4'},
      )
      self.assertEqual(manager.get_loaded_bag_names(), {''})
      self.assert_equivalent_bags(manager.get_bag(''), kd.bag())
      self.assertEqual(manager._metadata, {'version': '1.0.0'})

    with self.subTest('LoadAllBags'):
      manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
      for bag_name in manager.get_available_bag_names():
        manager.get_bag(bag_name)
      self.assertEqual(
          manager.get_loaded_bag_names(),
          {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4'},
      )
      self.assert_equivalent_bags(
          manager.get_bag(),
          kd.bags.updated(bag0, bag1, bag2, bag3, bag4),
      )

    with self.subTest('LoadInitialBagTree'):
      manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
      # Load all the bags that depend transitively on the initial bag, which is
      # the same as loading all the bags:
      self.assert_equivalent_bags(
          manager.get_bag('', with_all_dependents=True),
          kd.bags.updated(bag0, bag1, bag2, bag3, bag4),
      )
      self.assertEqual(
          manager.get_loaded_bag_names(),
          {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4'},
      )

    with self.subTest('LoadsAllTransitiveDependencies'):
      manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
      self.assert_equivalent_bags(
          manager.get_bag('bag3'),
          kd.bags.updated(bag0, bag1, bag3),
      )
      self.assertEqual(
          manager.get_loaded_bag_names(),
          {'', 'bag0', 'bag1', 'bag3'},
      )

    with self.subTest('LoadsAllTransitiveDependents'):
      manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
      self.assert_equivalent_bags(
          manager.get_bag('bag1', with_all_dependents=True),
          kd.bags.updated(bag0, bag1, bag2, bag3),  # bag4 is not loaded
      )
      self.assertEqual(
          manager.get_loaded_bag_names(),
          {'', 'bag0', 'bag1', 'bag2', 'bag3'},
      )

    # Next, we test that the persistence_dir is hermetic by moving it to a
    # new location and then initializing a new manager from the new location.

    new_persistence_dir = os.path.join(self.create_tempdir().full_path, 'copy')
    shutil.move(persistence_dir, new_persistence_dir)
    persistence_dir = new_persistence_dir

    manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
    self.assertEqual(
        manager.get_available_bag_names(),
        {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4'},
    )
    self.assertEqual(manager.get_loaded_bag_names(), {''})
    self.assert_equivalent_bags(manager.get_bag('bag0'), bag0)
    self.assert_equivalent_bags(
        manager.get_bag('bag3'), kd.bags.updated(bag0, bag1, bag3)
    )
    self.assertEqual(
        manager.get_loaded_bag_names(), {'', 'bag0', 'bag1', 'bag3'}
    )
    self.assertEqual(manager._metadata, {'version': '1.0.0'})

    # We can add further bags to a manager that was initialized from a
    # persistence_dir that was already populated.

    manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
    bag5 = kd.attrs(o, h=14, k=15, l=16)
    manager.add_bag('bag5', bag5, dependencies=['bag3'])
    self.assertEqual(
        manager.get_available_bag_names(),
        {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4', 'bag5'},
    )
    self.assertEqual(
        manager.get_loaded_bag_names(),
        {'', 'bag0', 'bag1', 'bag3', 'bag5'},
    )
    self.assert_equivalent_bags(
        manager.get_bag(''), kd.bags.updated(bag0, bag1, bag3, bag5)
    )

    # These additional bags are also persisted, and can be picked up by new
    # manager instances.

    manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
    self.assertEqual(
        manager.get_available_bag_names(),
        {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4', 'bag5'},
    )
    self.assertEqual(
        manager.get_loaded_bag_names(),
        {''},
    )
    self.assert_equivalent_bags(
        manager.get_bag('bag5'), kd.bags.updated(bag0, bag1, bag3, bag5)
    )
    self.assertEqual(
        manager.get_loaded_bag_names(),
        {'', 'bag0', 'bag1', 'bag3', 'bag5'},
    )
    self.assertEqual(manager._metadata, {'version': '1.0.0'})

    # The dependencies of dependents are also loaded.
    # Add a new bag6, which depends on bag5 and bag4. As a result, bag5 will get
    # a new dependent, namely bag6, which in turn depends on bag4, which isn't
    # in the transitive dependencies of bag5, but which must be loaded when we
    # ask to load bag5 and all its dependents.
    manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
    bag6 = kd.attrs(o, j=17, l=18, m=19)
    manager.add_bag('bag6', bag6, dependencies=['bag4', 'bag5'])
    # Start using a new manager initialized from the persistence dir.
    manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
    self.assertEqual(manager.get_loaded_bag_names(), {''})
    self.assert_equivalent_bags(
        manager.get_bag('bag5', with_all_dependents=True),
        kd.bags.updated(bag0, bag1, bag3, bag4, bag5, bag6),
    )
    self.assertEqual(
        manager.get_loaded_bag_names(),
        {'', 'bag0', 'bag1', 'bag3', 'bag4', 'bag5', 'bag6'},
    )

  def test_non_existing_persistence_dir_with_initial_bag(self):
    persistence_dir = os.path.join(self.create_tempdir().full_path, 'fresh_dir')
    manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
    self.assertEqual(manager.get_available_bag_names(), {''})
    self.assertEqual(manager.get_loaded_bag_names(), {''})
    self.assert_equivalent_bags(manager.get_bag(''), kd.bag())
    self.assertEqual(manager._metadata, {'version': '1.0.0'})

  def test_empty_persistence_dir_initialization(self):
    persistence_dir = self.create_tempdir().full_path  # Exists and empty.
    manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
    self.assertEqual(manager.get_available_bag_names(), {''})
    self.assertEqual(manager.get_loaded_bag_names(), {''})
    self.assert_equivalent_bags(manager.get_bag(''), kd.bag())
    self.assertEqual(manager._metadata, {'version': '1.0.0'})

  def test_get_bag_with_wrong_name(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
    with self.assertRaisesRegex(
        ValueError,
        re.escape("There is no bag with name 'bag1'. Valid bag names: ['']"),
    ):
      manager.get_bag('bag1')

  def test_add_bag_with_conflicting_name(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
    with self.assertRaisesRegex(
        ValueError,
        "A bag with name '' was already added.",
    ):
      manager.add_bag('', kd.bag(), dependencies=[])

    manager.add_bag('bag1', kd.bag(), dependencies=[''])
    with self.assertRaisesRegex(
        ValueError,
        "A bag with name 'bag1' was already added.",
    ):
      manager.add_bag('bag1', kd.bag(), dependencies=[''])

  def test_add_bag_with_empty_dependencies(self):
    persistence_dir = os.path.join(self.create_tempdir().full_path, 'fresh_dir')
    manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
    with self.assertRaisesRegex(
        ValueError,
        "The dependencies must not be empty. Use dependencies={''} to depend"
        ' only on the initial empty bag.',
    ):
      manager.add_bag('bag1', kd.bag(), dependencies=[])

  def test_add_bag_with_invalid_dependencies(self):
    persistence_dir = os.path.join(self.create_tempdir().full_path, 'fresh_dir')
    manager = pidm.PersistedIncrementalDataBagManager(persistence_dir)
    with self.assertRaisesRegex(
        ValueError,
        "A dependency on a bag with name 'non_existent_bag' is invalid, because"
        ' such a bag was not added before.',
    ):
      manager.add_bag('bag1', kd.bag(), dependencies=['non_existent_bag'])

    # The dependency graph is a DAG, so self-cycles are not allowed:
    with self.assertRaisesRegex(
        ValueError,
        "A dependency on a bag with name 'bag1' is invalid, because such a bag"
        ' was not added before.',
    ):
      manager.add_bag('bag1', kd.bag(), dependencies=['bag1'])


class DataSliceAndBagPersistenceTest(absltest.TestCase):

  def test_dataslice_write_read(self):
    ds = kd.slice([1, 2, 3])
    filepath = os.path.join(self.create_tempdir().full_path, 'test_slice.kd')
    pidm.write_slice_to_file(ds, filepath)
    kd.testing.assert_equal(pidm.read_slice_from_file(filepath), ds)

    new_ds = kd.slice([4, 5, 6])
    with self.assertRaisesRegex(ValueError, 'already exists'):
      pidm.write_slice_to_file(new_ds, filepath)
    pidm.write_slice_to_file(new_ds, filepath, overwrite=True)
    kd.testing.assert_equal(pidm.read_slice_from_file(filepath), new_ds)

  def test_databag_write_read(self):
    db = kd.new(x=1, y=2).get_bag()
    filepath = os.path.join(self.create_tempdir().full_path, 'test_bag.kd')
    pidm.write_bag_to_file(db, filepath)
    kd.testing.assert_equivalent(pidm.read_bag_from_file(filepath), db)

    new_db = kd.new(x=3, y=4).get_bag()
    with self.assertRaisesRegex(ValueError, 'already exists'):
      pidm.write_bag_to_file(new_db, filepath)
    pidm.write_bag_to_file(new_db, filepath, overwrite=True)
    kd.testing.assert_equivalent(pidm.read_bag_from_file(filepath), new_db)


if __name__ == '__main__':
  absltest.main()
