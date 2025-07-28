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

"""Management of a DataBag that is assembled from smaller bags.

The main user-facing abstraction in this module is the class
PersistedIncrementalDataBagManager.
"""

import collections
import concurrent.futures
import os
from typing import AbstractSet, Collection, Iterable

from koladata import kd
from koladata.ext.persisted_data import fs_interface
from koladata.ext.persisted_data import fs_util
from koladata.ext.persisted_data import persisted_incremental_data_bag_manager_metadata_pb2 as metadata_pb2


_INITIAL_BAG_NAME = ''


class PersistedIncrementalDataBagManager:
  """Manager of a DataBag that is assembled from multiple smaller bags.

  It is often convenient to create a DataBag by incrementally adding smaller
  bags, where each of the smaller bags is an update to the large DataBag.

  This also provides the opportunity to persist the smaller bags separately,
  along with the stated dependencies among the smaller bags.

  Then at a later point, usually in a different process, one can reassemble the
  large DataBag. But instead of loading the entire DataBag, one can load only
  the smaller bags that are needed, thereby saving loading time and memory. In
  fact the smaller bags can be loaded incrementally, so that decisions about
  which bags to load can be made on the fly instead of up-front. In that way,
  the incremental creation of the large DataBag is mirrored by its incremental
  consumption.

  To streamline the consumption, you have to specify dependencies between the
  smaller bags when they are added. It is trivial to specify a linear chain of
  dependencies, but setting up a dependency DAG is easy and can significantly
  improve the loading time and memory usage of data consumers. In fact this
  class will always manage a rooted DAG of small bags, and a chain of bags is
  just a special case.

  This class manages the smaller bags, which are named, and their
  interdependencies. It also handles the persistence of the smaller bags along
  with some metadata to facilitate the later consumption of the data and also
  its further augmentation. The persistence uses a filesystem directory, which
  is hermetic in the sense that it can be moved or copied. The persistence
  directory is consistent after each public operation of this class, provided
  that it is not modified externally and that there is sufficient space to
  accommodate the writes.

  This class is not thread-safe. However, multiple instances of this class can
  concurrently read from the same persistence directory. When an instance
  modifies the persistence directory, which happens when add_bag() is called,
  then there should be no other instances where read/write operations straddle
  the modification. I.e. all the read/write operations of another instance must
  happen completely before or completely after the modification.
  """

  def __init__(
      self,
      persistence_dir: str,
      *,
      fs: fs_interface.FileSystemInterface | None = None,
  ):
    """Initializes the manager.

    Args:
      persistence_dir: The directory where the small bags and metadata will be
        persisted. If it does not exist, or it is empty, then it will be
        populated with an empty bag named ''. Otherwise, the manager will be
        initialized from the existing artifacts in the directory.
      fs: All interactions with the file system will go through this instance.
        If None, then the default interaction with the file system is used.
    """
    self._persistence_dir: str = persistence_dir
    self._fs = fs or fs_util.get_default_file_system_interaction()
    del persistence_dir, fs  # Forces the use of the attributes henceforth.
    self._loaded_bags_cache: dict[str, kd.types.DataBag] = dict()
    if not self._fs.exists(self._persistence_dir):
      self._fs.make_dirs(self._persistence_dir)
    if not self._fs.glob(os.path.join(self._persistence_dir, '*')):  # Empty dir
      self._metadata = metadata_pb2.PersistedIncrementalDataBagManagerMetadata(
          version='1.0.0'
      )
      self._add_bag(_INITIAL_BAG_NAME, kd.bag(), dependencies=[])
    else:
      self._metadata = self._read_metadata()
      self._load_bags_into_cache({_INITIAL_BAG_NAME})

  def get_available_bag_names(self) -> AbstractSet[str]:
    """Returns the names of all bags that are managed by this manager.

    They include the initial empty bag (named ''), all bags that have been added
    to this manager instance, and all bags that were already persisted in the
    persistence directory before this manager instance was created.
    """
    return frozenset([m.name for m in self._metadata.data_bag_metadata])

  def get_loaded_bag_names(self) -> AbstractSet[str]:
    """Returns the names of all bags that are currently loaded in this manager.

    The initial empty bag (with name '') is always loaded, and the bags that
    have been added to this manager instance or loaded by previous calls to
    load_bags() and their transitive dependencies are also considered loaded.

    Some methods, such as get_minimal_bag() or extract_bags(), may load bags as
    a side effect when they are needed but not loaded yet.
    """
    return frozenset(self._loaded_bags_cache.keys())

  def load_bags(
      self,
      bag_names: Collection[str],
      *,
      with_all_dependents: bool = False,
  ):
    """Loads the requested bags and their transitive dependencies.

    Args:
      bag_names: The names of the bags that should be loaded. They must be a
        subset of get_available_bag_names(). All their transitive dependencies
        will be loaded as well.
      with_all_dependents: If True, then all the dependents of bag_names will
        also be loaded. The dependents are computed transitively. All transitive
        dependencies of the dependents will also be loaded.
    """
    self._load_bags(bag_names, with_all_dependents=with_all_dependents)

  def get_loaded_bag(self) -> kd.types.DataBag:
    """Returns a bag consisting of all the small bags that are currently loaded."""
    return self._make_single_bag(self.get_loaded_bag_names())

  def add_bag(
      self,
      bag_name: str,
      bag: kd.types.DataBag,
      *,
      dependencies: Collection[str],
  ):
    """Adds a bag to the manager, which will persist it.

    Args:
      bag_name: The name of the bag to add. This must be a name that is not
        already present in get_available_bag_names().
      bag: The bag to add.
      dependencies: A non-empty collection of the names of the bags that `bag`
        depends on. It should include all the direct dependencies. There is no
        need to include transitive dependencies. All the names mentioned here
        must already be present in get_available_bag_names(). After this
        function returns, the bag and all its transitive dependencies will be
        loaded and will hence be present in get_loaded_bag_names().
    """
    available_bag_names = self.get_available_bag_names()
    if bag_name in available_bag_names:
      raise ValueError(f"A bag with name '{bag_name}' was already added.")
    if not dependencies:
      raise ValueError(
          "The dependencies must not be empty. Use dependencies={''} to "
          'depend only on the initial empty bag.'
      )
    for d in dependencies:
      if d not in available_bag_names:
        raise ValueError(
            f"A dependency on a bag with name '{d}' is invalid, because such a"
            ' bag was not added before.'
        )
    self._add_bag(bag_name, bag, dependencies)

  def get_minimal_bag(
      self,
      bag_names: Collection[str],
      *,
      with_all_dependents: bool = False,
  ) -> kd.types.DataBag:
    """Returns a minimal bag that includes bag_names and all their dependencies.

    Args:
      bag_names: The name of the bags whose data must be included in the result.
        It must be a non-empty subset of get_available_bag_names(). These bags
        and their transitive dependencies will be loaded if they are not loaded
        yet.
      with_all_dependents: If True, then the returned bag will also include all
        the dependents of bag_names. The dependents are computed transitively.
        All transitive dependencies of the dependents are then also included in
        the result.

    Returns:
      A minimal bag that has the data of the requested small bags. It will not
      include any unrelated bags that are already loaded.
    """
    needed_bag_names = self._load_bags(
        bag_names, with_all_dependents=with_all_dependents
    )
    return self._make_single_bag(needed_bag_names)

  def extract_bags(
      self,
      bag_names: Collection[str],
      *,
      with_all_dependents: bool = False,
      output_dir: str,
      fs: fs_interface.FileSystemInterface | None = None,
  ):
    """Extracts the requested bags to the given output directory.

    To extract all the bags managed by this manager, you can call this function
    with the arguments bag_names=[''], with_all_dependents=True.

    Args:
      bag_names: The names of the bags that will be extracted. They must be a
        non-empty subset of get_available_bag_names(). The extraction will also
        include their transitive dependencies.
      with_all_dependents: If True, then the extracted bags will also include
        all dependents of bag_names. The dependents are computed transitively.
        All transitive dependencies of the dependents will also be included in
        the extraction.
      output_dir: The directory to which the bags will be extracted. It must
        either be empty or not exist yet.
      fs: All interactions with the file system for output_dir will happen via
        this instance.
    """
    bags_to_extract = self._load_bags(
        bag_names, with_all_dependents=with_all_dependents
    )
    bag_dependencies = self._get_dependency_relation()

    fs = fs or fs_util.get_default_file_system_interaction()
    if not fs.exists(output_dir):
      fs.make_dirs(output_dir)
    if fs.glob(os.path.join(output_dir, '*')):
      raise ValueError(
          f'The output_dir must be empty or not exist yet. Got {output_dir}'
      )
    # Using a new manager is convenient, yet it might become problematic because
    # it will load all the bags into memory and keep them in memory. An
    # alternative would be to populate the output_dir directly, i.e. without
    # using a new manager. Or to provide a way to unload the bags from the
    # memory of the new manager.
    new_manager = PersistedIncrementalDataBagManager(output_dir, fs=fs)
    for bag_name in self._canonical_topological_sorting(bags_to_extract):
      if not bag_name:
        # Skip the initial empty bag. The new_manager already has one.
        continue
      new_manager.add_bag(
          bag_name,
          self._loaded_bags_cache[bag_name],
          dependencies=bag_dependencies[bag_name],
      )

  def _load_bags(
      self,
      bag_names: Collection[str],
      *,
      with_all_dependents: bool,
  ) -> AbstractSet[str]:
    """Loads the requested bags and their transitive dependencies.

    Args:
      bag_names: The names of the bags that should be loaded. It must be a
        subset of get_available_bag_names(). All their transitive dependencies
        will be loaded as well.
      with_all_dependents: If True, then all the dependents of bag_names will
        also be loaded. The dependents are computed transitively. All transitive
        dependencies of the dependents will also be loaded.

    Returns:
      The names of the requested bags. Guaranteed to be the same as
      self._get_dependency_closure(
          bags_to_load, with_all_dependents=with_all_dependents
      )
    """
    # bag_names must be a collection of strings, not a single string that is
    # interpreted as a collection of character strings:
    assert not isinstance(bag_names, (str, bytes))
    bags_to_load = set(bag_names)
    unknown_bags = bags_to_load - self.get_available_bag_names()
    if unknown_bags:
      raise ValueError(
          'bag_names must be a subset of get_available_bag_names(). The'
          f' following bags are not available: {sorted(unknown_bags)}'
      )

    bags_to_load = self._get_dependency_closure(
        bags_to_load, with_all_dependents=with_all_dependents
    )
    self._load_bags_into_cache(bags_to_load)
    return bags_to_load

  def _add_bag(
      self, bag_name: str, bag: kd.types.DataBag, dependencies: Collection[str]
  ):
    """Adds the given bag, which might be the initial one, to this manager.

    Args:
      bag_name: The name of the bag to add. Must not be used already.
      bag: The bag to add.
      dependencies: The names of the bags that `bag` depends on. Can be empty
        for the initial bag. All the names mentioned here are assumed to be
        present in get_available_bag_names().
    """
    self._load_bags_into_cache(
        self._get_reflexive_and_transitive_closure_image(
            self._get_dependency_relation(), set(dependencies)
        )
    )
    bag_filename = self._get_fresh_bag_filename()
    self._write_bag_to_file(bag, bag_filename)
    self._loaded_bags_cache[bag_name] = bag
    self._metadata.data_bag_metadata.append(
        metadata_pb2.DataBagMetadata(
            name=bag_name,
            filename=bag_filename,
            dependencies=dependencies,
        )
    )
    self._persist_metadata()

  def _make_single_bag(self, bag_names: AbstractSet[str]) -> kd.types.DataBag:
    """Returns a single bag with the data of bag_names.

    Args:
      bag_names: The names of the bags whose data will be put in the result. It
        must be a non-empty subset of get_loaded_bag_names(). The result will
        contain exactly these bags, so all the needed dependencies must already
        be included in bag_names.

    Returns:
      A single bag with the data of bag_names, which are combined in the
      order given by their canonical topological sorting.
    """
    bags = [
        self._loaded_bags_cache[bn]
        for bn in self._canonical_topological_sorting(bag_names)
    ]
    return kd.bags.updated(*bags)

  def _get_dependency_closure(
      self, bag_names: AbstractSet[str], *, with_all_dependents: bool
  ) -> AbstractSet[str]:
    bags_to_consider = set(bag_names)
    if with_all_dependents:
      bags_to_consider.update(
          self._get_reflexive_and_transitive_closure_image(
              self._get_reverse_dependency_relation(), bags_to_consider
          )
      )
    return self._get_reflexive_and_transitive_closure_image(
        self._get_dependency_relation(), bags_to_consider
    )

  def _get_dependency_relation(self) -> dict[str, Collection[str]]:
    """Returns a dictionary with the full dependency graph."""
    return {m.name: m.dependencies for m in self._metadata.data_bag_metadata}

  def _get_reverse_dependency_relation(self) -> dict[str, AbstractSet[str]]:
    """Returns a dictionary with the full reverse dependency graph."""
    result = collections.defaultdict(set)
    for bag_name, dependencies in self._get_dependency_relation().items():
      for dependency in dependencies:
        result[dependency].add(bag_name)
    return result

  def _get_reflexive_and_transitive_closure_image(
      self,
      relation: dict[str, Iterable[str]],
      bag_names: AbstractSet[str],
  ) -> AbstractSet[str]:
    """Computes the closure of `relation` and returns the image of `bag_names`."""
    unvisited = set(bag_names)
    result = set()
    while unvisited:
      current = unvisited.pop()
      if current in result:
        continue
      result.add(current)
      unvisited.update(relation[current])
    return result

  def _load_bags_into_cache(self, bags_to_load: AbstractSet[str]):
    """Loads the requested bags into the cache.

    If a bag mentioned in bags_to_load is already cached, then it won't be
    processed again.

    Args:
      bags_to_load: The names of the bags to load into the cache. They must be a
        subset of get_available_bag_names().
    """
    needed_bags = [
        bn for bn in bags_to_load if bn not in self._loaded_bags_cache
    ]
    if not needed_bags:
      return
    with concurrent.futures.ThreadPoolExecutor() as executor:
      futures = [
          executor.submit(self._read_bag_from_file, bag_name)
          for bag_name in needed_bags
      ]
    for bag_name, future in zip(needed_bags, futures):
      self._loaded_bags_cache[bag_name] = future.result()

  def _canonical_topological_sorting(
      self,
      bag_names: AbstractSet[str],
  ) -> list[str]:
    """Returns the canonical sorting of the bags in bag_names wrt dependencies.

    The canonical sorting agrees with the order in which the bags were added.
    Since all the dependencies must already be available when a new bag is
    added, the total order in which the bags were added serves as the canonical
    topological sorting of the bags wrt the dependency relation.

    The result of this function will only contain the elements of bag_names, so
    callers must make sure that bag_names includes all the bags, dependent bags
    and bag dependencies that they are interested in.

    Using a canonical sorting helps to make the code more deterministic. Users
    might forget to specify a dependency between two bags that have a
    conflicting value for an attribute. With the canonical ordering, such bags
    will always be merged in a fixed order when they are both needed, so the
    behavior will be more predictable and easier to debug.

    Args:
      bag_names: The names of the bags to sort. They must be a subset of
        get_available_bag_names().

    Returns:
      A topological sorting of the bags in bag_names wrt dependencies.
    """
    bag_name_to_add_index = {
        m.name: i for i, m in enumerate(self._metadata.data_bag_metadata)
    }
    return sorted(bag_names, key=lambda name: bag_name_to_add_index[name])

  def _get_fresh_bag_filename(self) -> str:
    """Returns a filename that has not been used before for a bag."""
    # If desired, the naming scheme employed here can be used in the future to
    # detect some forms of concurrent writes. As the class docstring mentions,
    # concurrent writes by multiple managers for the same persistence directory
    # are disallowed.
    # The idea of the naming/detection scheme is as follows:
    # 1. We list all the bag files in the persistence directory.
    # 2. We extract the bag number from each filename.
    # 3. We compute the maximum bag number and add 1 to it. This is the new bag
    #    number. It is a local variable of this manager instance, i.e. it is
    #    kept in memory and not persisted anywhere.
    # 4. We persist the new bag in a temp file whose name is based on a uuid.
    # 5. When the temp file is fully written, we rename it to the new bag
    #    filename from step 3. Since renaming is atomic on most filesystems, and
    #    will raise an error if the target filename exists, it will ensure that
    #    no other process persisted a bag while we were working.
    # It should be noted that this scheme is far from complete, as it will still
    # allow interleaved writes by concurrent manager instances. Such writes are
    # still problematic, because a write by one instance won't be detected by
    # the other instance, and hence inconsistencies will arise. Moreover,
    # multiple other files get persisted, such as the bag dependencies, and the
    # scheme described above does not achieve/detect atomicity of multi-file
    # writes/overwrites.
    # If desired, one can design much more foolproof schemes by using
    # a readers-writer lock file in the persistence directory.
    # See https://en.wikipedia.org/wiki/Readers%E2%80%93writer_lock.
    # Such an implementation would enforce the contract of this class by
    # allowing concurrent readers but enforcing exclusive access for a writer.
    all_bag_filenames = self._fs.glob(
        os.path.join(self._persistence_dir, 'bag-[0-9]*.kd')
    )
    bag_numbers = [
        int(
            filepath.removeprefix(self._persistence_dir)
            .removeprefix(os.sep)
            .removeprefix('bag-')
            .removesuffix('.kd')
        )
        for filepath in all_bag_filenames
    ]
    max_bag_number = max(bag_numbers) if bag_numbers else -1
    new_bag_number = max_bag_number + 1
    return f'bag-{new_bag_number:012d}.kd'

  def _write_bag_to_file(self, bag: kd.types.DataBag, bag_filename: str):
    fs_util.write_bag_to_file(
        self._fs, bag, self._get_bag_filepath_from_filename(bag_filename)
    )

  def _read_bag_from_file(self, bag_name: str) -> kd.types.DataBag:
    bag_filename = next(
        m.filename
        for m in self._metadata.data_bag_metadata
        if m.name == bag_name
    )
    bag_filepath = self._get_bag_filepath_from_filename(bag_filename)
    return fs_util.read_bag_from_file(self._fs, bag_filepath)

  def _get_bag_filepath_from_filename(self, bag_filename: str) -> str:
    return os.path.join(self._persistence_dir, bag_filename)

  def _persist_metadata(self):
    with self._fs.open(self._get_metadata_filepath(), 'wb') as f:
      f.write(self._metadata.SerializeToString())

  def _read_metadata(
      self,
  ) -> metadata_pb2.PersistedIncrementalDataBagManagerMetadata:
    with self._fs.open(self._get_metadata_filepath(), 'rb') as f:
      return metadata_pb2.PersistedIncrementalDataBagManagerMetadata.FromString(
          f.read()
      )

  def _get_metadata_filepath(self) -> str:
    return os.path.join(self._persistence_dir, 'metadata.pb')
