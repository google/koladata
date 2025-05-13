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

import itertools
import os
import re
import shutil
from unittest import mock
from absl.testing import absltest
from koladata import kd
from koladata.ext.persisted_data import fs_implementation
from koladata.ext.persisted_data import persisted_incremental_data_bag_manager as pidbm


class PersistedIncrementalDatabagManagerTest(absltest.TestCase):

  def assert_equivalent_bags(self, bag0, bag1):
    kd.testing.assert_equivalent(bag0.merge_fallbacks(), bag1.merge_fallbacks())

  def test_typical_usage(self):
    persistence_dir = os.path.join(self.create_tempdir().full_path, 'bags')
    o = kd.new(a=1, b=2, c=3)

    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
    self.assertEqual(manager.get_available_bag_names(), {''})
    self.assertEqual(manager.get_loaded_bag_names(), {''})
    self.assert_equivalent_bags(manager.get_loaded_bag(), kd.bag())
    self.assertEqual(manager._metadata.version, '1.0.0')

    bag0 = o.get_bag()
    manager.add_bag('bag0', bag0, dependencies=[''])
    self.assertEqual(manager.get_available_bag_names(), {'', 'bag0'})
    self.assertEqual(manager.get_loaded_bag_names(), {'', 'bag0'})
    self.assert_equivalent_bags(manager.get_loaded_bag(), bag0)
    self.assertEqual(manager._metadata.version, '1.0.0')

    bag1 = kd.attrs(o, c=4, d=5, e=6)
    manager.add_bag('bag1', bag1, dependencies=['bag0'])
    self.assertEqual(manager.get_available_bag_names(), {'', 'bag0', 'bag1'})
    self.assertEqual(manager.get_loaded_bag_names(), {'', 'bag0', 'bag1'})
    expected_loaded_bag = kd.bags.updated(bag0, bag1)
    self.assert_equivalent_bags(manager.get_loaded_bag(), expected_loaded_bag)

    bag2 = kd.attrs(o, e=7, f=8)
    manager.add_bag('bag2', bag2, dependencies=['bag1'])
    self.assertEqual(
        manager.get_available_bag_names(), {'', 'bag0', 'bag1', 'bag2'}
    )
    self.assertEqual(
        manager.get_loaded_bag_names(), {'', 'bag0', 'bag1', 'bag2'}
    )
    expected_loaded_bag = kd.bags.updated(bag0, bag1, bag2)
    self.assert_equivalent_bags(manager.get_loaded_bag(), expected_loaded_bag)

    bag3 = kd.attrs(o, d=9, g=10, h=11)
    manager.add_bag('bag3', bag3, dependencies=['bag1'])
    self.assertEqual(
        manager.get_available_bag_names(), {'', 'bag0', 'bag1', 'bag2', 'bag3'}
    )
    self.assertEqual(
        manager.get_loaded_bag_names(), {'', 'bag0', 'bag1', 'bag2', 'bag3'}
    )
    expected_loaded_bag = kd.bags.updated(bag0, bag1, bag2, bag3)
    self.assert_equivalent_bags(manager.get_loaded_bag(), expected_loaded_bag)

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
    expected_loaded_bag = kd.bags.updated(bag0, bag1, bag2, bag3, bag4)
    self.assert_equivalent_bags(manager.get_loaded_bag(), expected_loaded_bag)

    # Each of the following subtests will initialize a new manager from the same
    # persistence_dir that was populated above.

    with self.subTest('InitializeFromPersistenceDir'):
      manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
      self.assertEqual(
          manager.get_available_bag_names(),
          {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4'},
      )
      self.assertEqual(manager.get_loaded_bag_names(), {''})
      self.assert_equivalent_bags(manager.get_loaded_bag(), kd.bag())
      self.assertEqual(manager._metadata.version, '1.0.0')

    with self.subTest('LoadAllBags'):
      manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
      manager.load_bags(manager.get_available_bag_names())
      self.assertEqual(
          manager.get_loaded_bag_names(),
          {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4'},
      )
      self.assert_equivalent_bags(
          manager.get_loaded_bag(),
          kd.bags.updated(bag0, bag1, bag2, bag3, bag4),
      )

    with self.subTest('LoadInitialBagDag'):
      manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
      # Load all the bags that depend transitively on the initial bag, which is
      # the same as loading all the bags:
      manager.load_bags({''}, with_all_dependents=True)
      self.assert_equivalent_bags(
          manager.get_loaded_bag(),
          kd.bags.updated(bag0, bag1, bag2, bag3, bag4),
      )
      self.assertEqual(
          manager.get_loaded_bag_names(),
          {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4'},
      )

    with self.subTest('LoadsAllTransitiveDependencies'):
      manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
      manager.load_bags({'bag3'})
      self.assert_equivalent_bags(
          manager.get_loaded_bag(),
          kd.bags.updated(bag0, bag1, bag3),
      )
      self.assertEqual(
          manager.get_loaded_bag_names(),
          {'', 'bag0', 'bag1', 'bag3'},
      )

    with self.subTest('LoadsAllTransitiveDependents'):
      manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
      manager.load_bags({'bag1'}, with_all_dependents=True)
      self.assert_equivalent_bags(
          manager.get_loaded_bag(),
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

    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
    self.assertEqual(
        manager.get_available_bag_names(),
        {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4'},
    )
    self.assertEqual(manager.get_loaded_bag_names(), {''})
    manager.load_bags({'bag0'})
    self.assert_equivalent_bags(manager.get_loaded_bag(), bag0)
    manager.load_bags({'bag3'})
    self.assert_equivalent_bags(
        manager.get_loaded_bag(), kd.bags.updated(bag0, bag1, bag3)
    )
    self.assertEqual(
        manager.get_loaded_bag_names(), {'', 'bag0', 'bag1', 'bag3'}
    )
    self.assertEqual(manager._metadata.version, '1.0.0')

    # We can add further bags to a manager that was initialized from a
    # persistence_dir that was already populated.

    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
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
        manager.get_loaded_bag(), kd.bags.updated(bag0, bag1, bag3, bag5)
    )

    # These additional bags are also persisted, and can be picked up by new
    # manager instances.

    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
    self.assertEqual(
        manager.get_available_bag_names(),
        {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4', 'bag5'},
    )
    self.assertEqual(
        manager.get_loaded_bag_names(),
        {''},
    )
    manager.load_bags({'bag5'})
    self.assert_equivalent_bags(
        manager.get_loaded_bag(), kd.bags.updated(bag0, bag1, bag3, bag5)
    )
    self.assertEqual(
        manager.get_loaded_bag_names(),
        {'', 'bag0', 'bag1', 'bag3', 'bag5'},
    )
    self.assertEqual(manager._metadata.version, '1.0.0')

    # The dependencies of dependents are also loaded.
    # Add a new bag6, which depends on bag5 and bag4. As a result, bag5 will get
    # a new dependent, namely bag6, which in turn depends on bag4, which isn't
    # in the transitive dependencies of bag5, but which must be loaded when we
    # ask to load bag5 and all its dependents.
    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
    bag6 = kd.attrs(o, j=17, l=18, m=19)
    manager.add_bag('bag6', bag6, dependencies=['bag4', 'bag5'])
    # Start using a new manager initialized from the persistence dir.
    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
    self.assertEqual(manager.get_loaded_bag_names(), {''})
    manager.load_bags({'bag5'}, with_all_dependents=True)
    self.assert_equivalent_bags(
        manager.get_loaded_bag(),
        kd.bags.updated(bag0, bag1, bag3, bag4, bag5, bag6),
    )
    self.assertEqual(
        manager.get_loaded_bag_names(),
        {'', 'bag0', 'bag1', 'bag3', 'bag4', 'bag5', 'bag6'},
    )

    # We can get a minimal bag that includes only the requested bags and their
    # transitive dependencies, without any extra bags that happen to be loaded.

    with self.subTest('GetMinimalBag'):
      manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
      self.assertEqual(manager.get_loaded_bag_names(), {''})
      self.assert_equivalent_bags(
          manager.get_minimal_bag({'bag5'}),
          kd.bags.updated(bag0, bag1, bag3, bag5),
      )
      self.assertEqual(
          manager.get_loaded_bag_names(), {'', 'bag0', 'bag1', 'bag3', 'bag5'}
      )

      # Loading more bags should not affect the minimal bag:
      manager.load_bags({''}, with_all_dependents=True)  # Load all the bags.
      self.assertEqual(
          manager.get_loaded_bag_names(),
          {'', 'bag0', 'bag1', 'bag2', 'bag3', 'bag4', 'bag5', 'bag6'},
      )
      self.assert_equivalent_bags(
          manager.get_minimal_bag({'bag5'}),
          kd.bags.updated(bag0, bag1, bag3, bag5),
      )

    with self.subTest('GetMinimalBagWithAllDependents'):
      manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
      self.assert_equivalent_bags(
          manager.get_minimal_bag({'bag5'}, with_all_dependents=True),
          kd.bags.updated(bag0, bag1, bag3, bag4, bag5, bag6),
      )
      self.assert_equivalent_bags(
          manager.get_minimal_bag({'bag5'}, with_all_dependents=False),
          kd.bags.updated(bag0, bag1, bag3, bag5),
      )

    # We can also extract the bags from the manager, as the following subtests
    # show.

    with self.subTest('ExtractBags'):
      new_persistence_dir = self.create_tempdir().full_path
      manager.extract_bags(
          bag_names={'bag5'},
          output_dir=new_persistence_dir,
      )
      extracted_manager = pidbm.PersistedIncrementalDataBagManager(
          new_persistence_dir
      )
      self.assertEqual(
          extracted_manager.get_available_bag_names(),
          {'', 'bag0', 'bag1', 'bag3', 'bag5'},
      )
      self.assertEqual(
          extracted_manager.get_loaded_bag_names(),
          {''},
      )
      extracted_manager.load_bags({'bag3'})
      self.assert_equivalent_bags(
          extracted_manager.get_loaded_bag(), kd.bags.updated(bag0, bag1, bag3)
      )
      self.assertEqual(
          extracted_manager.get_loaded_bag_names(),
          {'', 'bag0', 'bag1', 'bag3'},
      )

    with self.subTest('ExtractBagsWithAllDependents'):
      new_persistence_dir = self.create_tempdir().full_path
      manager.extract_bags(
          bag_names={'bag5'},
          with_all_dependents=True,
          output_dir=new_persistence_dir,
      )
      extracted_manager = pidbm.PersistedIncrementalDataBagManager(
          new_persistence_dir
      )
      self.assertEqual(
          extracted_manager.get_available_bag_names(),
          {'', 'bag0', 'bag1', 'bag3', 'bag4', 'bag5', 'bag6'},
      )
      self.assertEqual(
          extracted_manager.get_loaded_bag_names(),
          {''},
      )
      extracted_manager.load_bags({'bag5'})
      self.assert_equivalent_bags(
          extracted_manager.get_loaded_bag(),
          kd.bags.updated(bag0, bag1, bag3, bag5),
      )
      self.assertEqual(
          extracted_manager.get_loaded_bag_names(),
          {'', 'bag0', 'bag1', 'bag3', 'bag5'},
      )

  def test_non_existing_persistence_dir_with_initial_bag(self):
    persistence_dir = os.path.join(self.create_tempdir().full_path, 'fresh_dir')
    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
    self.assertEqual(manager.get_available_bag_names(), {''})
    self.assertEqual(manager.get_loaded_bag_names(), {''})
    self.assert_equivalent_bags(manager.get_loaded_bag(), kd.bag())
    self.assertEqual(manager._metadata.version, '1.0.0')

  def test_empty_persistence_dir_initialization(self):
    persistence_dir = self.create_tempdir().full_path  # Exists and empty.
    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
    self.assertEqual(manager.get_available_bag_names(), {''})
    self.assertEqual(manager.get_loaded_bag_names(), {''})
    self.assert_equivalent_bags(manager.get_loaded_bag(), kd.bag())
    self.assertEqual(manager._metadata.version, '1.0.0')

  def test_canonical_topological_sorting(self):
    bag_names = ['bag0', 'bag1', 'bag2']
    for name0, name1, name2 in itertools.permutations(bag_names):
      persistence_dir = self.create_tempdir().full_path
      manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
      manager.add_bag(name0, kd.bag(), dependencies={''})
      manager.add_bag(name1, kd.bag(), dependencies={''})
      manager.add_bag(name2, kd.bag(), dependencies={''})
      # The above 3 bags have no inter-dependencies, so any permutation of them
      # is a valid topological sorting wrt the dependency relation. However, the
      # *canonical* topological sorting reflects the order in which the bags
      # were added. It is always deterministic and fixed:
      expected_canonical_sorting = [name0, name1, name2]
      self.assertEqual(
          manager._canonical_topological_sorting(set(bag_names)),
          expected_canonical_sorting,
      )

  def test_use_of_provided_file_system_interaction_object(self):
    # The assertions below check that the sequence of method names called on the
    # mocked file system are as expected. If that turns out to be a maintenance
    # burden, then we can just check that the *sets* of method calls are equal.
    # For the most part, we want to make sure that interactions with the file
    # system are happening via the provided object. The extract_bags case is the
    # most interesting one, because it involves interaction with two directories
    # via two different file system interaction objects.

    with self.subTest('EmptyInitialPersistenceDir'):
      persistence_dir = self.create_tempdir().full_path
      mocked_fs = mock.Mock(wraps=fs_implementation.FileSystemInteraction())
      _ = pidbm.PersistedIncrementalDataBagManager(
          persistence_dir, fs=mocked_fs
      )
      method_names_called = [c[0] for c in mocked_fs.method_calls]
      self.assertEqual(
          method_names_called,
          [
              'exists',  # Check if persistence_dir exists.
              'glob',  # It does exist. Check if it is empty.
              'glob',  # To compute the filename for the initial bag.
              'exists',  # Check if such a file already exists. It does not.
              'open',  # To write the initial bag.
              'open',  # To write the metadata.
          ],
      )

    with self.subTest('NonEmptyInitialPersistenceDir'):
      persistence_dir = self.create_tempdir().full_path
      # Initialize the directory:
      _ = pidbm.PersistedIncrementalDataBagManager(persistence_dir)

      # Start a new manager with the already initialized persistence_dir.
      mocked_fs = mock.Mock(wraps=fs_implementation.FileSystemInteraction())
      _ = pidbm.PersistedIncrementalDataBagManager(
          persistence_dir, fs=mocked_fs
      )
      method_names_called = [c[0] for c in mocked_fs.method_calls]
      self.assertEqual(
          method_names_called,
          [
              'exists',  # Check if persistence_dir exists. It does.
              'glob',  # See whether it is empty. It is not.
              'open',  # To read the metadata.
              'open',  # To read the initial bag.
          ],
      )

    with self.subTest('add_bag'):
      persistence_dir = self.create_tempdir().full_path
      mocked_fs = mock.Mock(wraps=fs_implementation.FileSystemInteraction())
      manager = pidbm.PersistedIncrementalDataBagManager(
          persistence_dir, fs=mocked_fs
      )

      mocked_fs.reset_mock()
      manager.add_bag('bag1', kd.bag(), dependencies=[''])
      method_names_called = [c[0] for c in mocked_fs.method_calls]
      self.assertEqual(
          method_names_called,
          [
              'glob',  # To find the filename for the new bag.
              'exists',  # Check if the new bag already exists. It does not.
              'open',  # To write the new bag.
              'open',  # To write the metadata.
          ],
      )

    with self.subTest('load_bags'):
      persistence_dir = self.create_tempdir().full_path
      manager = pidbm.PersistedIncrementalDataBagManager(
          persistence_dir, fs=mocked_fs
      )
      manager.add_bag('bag1', kd.bag(), dependencies=[''])
      manager.add_bag('bag2', kd.bag(), dependencies=[''])

      mocked_fs = mock.Mock(wraps=fs_implementation.FileSystemInteraction())
      manager = pidbm.PersistedIncrementalDataBagManager(
          persistence_dir, fs=mocked_fs
      )
      mocked_fs.reset_mock()
      manager.load_bags({'bag1', 'bag2'})
      method_names_called = [c[0] for c in mocked_fs.method_calls]
      self.assertEqual(
          method_names_called,
          [
              'open',  # To read one bag.
              'open',  # To read the other bag.
          ],
      )

    with self.subTest('get_minimal_bag'):
      persistence_dir = self.create_tempdir().full_path
      manager = pidbm.PersistedIncrementalDataBagManager(
          persistence_dir,
      )
      manager.add_bag('bag1', kd.bag(), dependencies=[''])

      mocked_fs = mock.Mock(wraps=fs_implementation.FileSystemInteraction())
      manager = pidbm.PersistedIncrementalDataBagManager(
          persistence_dir, fs=mocked_fs
      )
      mocked_fs.reset_mock()

      _ = manager.get_minimal_bag(bag_names=['bag1'])

      method_names_called = [c[0] for c in mocked_fs.method_calls]
      self.assertEqual(method_names_called, ['open'])  # To read bag1.

    # This is the most interesting subtest. The reason is that extract_bags()
    # uses two file system interaction objects: one for the original
    # persistence_dir, and one for the output_dir.
    with self.subTest('extract_bags'):
      persistence_dir = self.create_tempdir().full_path
      manager = pidbm.PersistedIncrementalDataBagManager(
          persistence_dir,
      )
      manager.add_bag('bag1', kd.bag(), dependencies=[''])

      original_fs = mock.Mock(wraps=fs_implementation.FileSystemInteraction())
      manager = pidbm.PersistedIncrementalDataBagManager(
          persistence_dir, fs=original_fs
      )
      original_fs.reset_mock()

      output_dir = self.create_tempdir().full_path
      output_fs = mock.Mock(wraps=fs_implementation.FileSystemInteraction())
      manager.extract_bags(
          bag_names=['bag1'], output_dir=output_dir, fs=output_fs
      )

      self.assertEqual(
          original_fs.method_calls,
          # To read bag1 from persistence_dir:
          [
              mock.call.open(
                  os.path.join(persistence_dir, 'bag-000000000001.kd'), 'rb'
              )
          ],
      )

      output_fs_method_names_called = [c[0] for c in output_fs.method_calls]
      output_fs_method_calls_arg_0 = [c[1][0] for c in output_fs.method_calls]
      # All the calls to the output_fs are for the output_dir:
      self.assertEqual(
          [
              arg0.startswith(output_dir)
              for arg0 in output_fs_method_calls_arg_0
          ],
          [True] * len(output_fs.method_calls),
      )
      self.assertEqual(
          output_fs_method_names_called,
          [
              # For nice error messages, `manager` does the following:
              'exists',  # Check if output_dir exists. It does.
              'glob',  # Check if output_dir is empty. It is.
              # The implementation goes ahead and creates a new manager, which
              # will do all the following:
              'exists',  # Check if the output_dir exists. It does.
              'glob',  # Check if output_dir is empty. It is.
              # The new manager adds two bags. For each one, it calls the
              # methods documented in the sub-test `load_bag` above.
              *(['glob', 'exists', 'open', 'open'] * 2),
          ],
      )

  def test_load_bags_with_wrong_name(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
    with self.assertRaisesRegex(
        ValueError,
        re.escape("The following bags are not available: ['bag1']"),
    ):
      manager.load_bags(['', 'bag1'])

  def test_add_bag_with_conflicting_name(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
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
    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
    with self.assertRaisesRegex(
        ValueError,
        "The dependencies must not be empty. Use dependencies={''} to depend"
        ' only on the initial empty bag.',
    ):
      manager.add_bag('bag1', kd.bag(), dependencies=[])

  def test_add_bag_with_invalid_dependencies(self):
    persistence_dir = os.path.join(self.create_tempdir().full_path, 'fresh_dir')
    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)
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

  def test_get_minimal_bag_with_empty_bag_names(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)

    with self.assertRaisesRegex(
        ValueError,
        'bag_names must not be empty.',
    ):
      _ = manager.get_minimal_bag(bag_names=[])

  def test_get_minimal_bag_with_unknown_bag_names(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'bag_names must be a subset of get_available_bag_names(). The'
            " following bags are not available: ['bar', 'foo']"
        ),
    ):
      _ = manager.get_minimal_bag(bag_names=['foo', 'bar'])

  def test_extract_bags_with_empty_bag_names(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)

    output_dir = self.create_tempdir().full_path
    with self.assertRaisesRegex(
        ValueError,
        'bag_names must not be empty.',
    ):
      manager.extract_bags(bag_names=[], output_dir=output_dir)

  def test_extract_bags_with_unknown_bag_names(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)

    output_dir = self.create_tempdir().full_path
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'bag_names must be a subset of get_available_bag_names(). The'
            " following bags are not available: ['bar', 'foo']"
        ),
    ):
      manager.extract_bags(bag_names=['foo', 'bar'], output_dir=output_dir)

  def test_extract_bags_with_non_empty_output_dir(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidbm.PersistedIncrementalDataBagManager(persistence_dir)

    tempdir = self.create_tempdir()
    tempdir.create_file()  # Non-empty.
    output_dir = tempdir.full_path
    with self.assertRaisesRegex(
        ValueError,
        'The output_dir must be empty or not exist yet.',
    ):
      manager.extract_bags(bag_names=[''], output_dir=output_dir)


if __name__ == '__main__':
  absltest.main()
