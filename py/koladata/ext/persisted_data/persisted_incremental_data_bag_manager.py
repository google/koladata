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
import dataclasses
import itertools
import os
from typing import AbstractSet, Collection, Iterable
import uuid

from koladata import kd
from koladata.ext.persisted_data import fs_interface
from koladata.ext.persisted_data import fs_util
from koladata.ext.persisted_data import persisted_incremental_data_bag_manager_metadata_pb2 as metadata_pb2


_INITIAL_BAG_NAME = ''


@dataclasses.dataclass(frozen=True)
class BagToAdd:
  # For the semantics of the fields, please see the docstring of
  # PersistedIncrementalDataBagManager.add_bags().
  bag_name: str
  bag: kd.types.DataBag
  dependencies: tuple[str, ...]


class PersistedIncrementalDataBagManager:
  """Manager of a DataBag that is assembled from multiple smaller bags.

  Short version of the contract:
  * Instances are not thread-safe.
  * Multiple instances can be created for the same persistence directory:
    * Multiple readers are allowed.
    * The effects of write operations (calls to add_bags()) are not propagated
      to other instances that already exist.
    * Concurrent writers are not allowed. A write operation will fail if the
      state of the persistence directory was modified in the meantime by another
      instance.

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
  is hermetic in the sense that it can be moved or copied (although doing so
  will break branches if any exist - see the docstring of create_branch()). The
  persistence directory is consistent after each public operation of this class,
  provided that it is not modified externally and that there is sufficient space
  to accommodate the writes.

  This class is not thread-safe. When an instance is created for a persistence
  directory that is already populated, then the instance is initialized with
  the current state found in the persistence directory at that point in time.
  Write operations (calls to add_bags()) by other instances for the same
  persistence directory are not propagated to this instance. A write operation
  will fail if the state of the persistence directory was modified in the
  meantime by another instance.
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
          version='1.0.0',
          # Using -1 here looks strange, but the self._add_bags() call below to
          # add the initial bag will bump it to 0 in a moment.
          metadata_update_number=-1,
      )
      self._add_bags(
          [BagToAdd(_INITIAL_BAG_NAME, kd.bag(), dependencies=tuple())]
      )
    else:
      self._metadata = _read_latest_metadata(self._fs, self._persistence_dir)
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

  def add_bags(self, bags_to_add: list[BagToAdd]):
    """Adds the given bags to the manager, which will persist them.

    Conceptually, the items of bags_to_add are added one by one in the order
    specified by the list. Each item of bags_to_add is a BagToAdd object with:
      - bag_name: The name of the bag to add. This must be a name that is not
        already present in get_available_bag_names() or any preceding item of
        bags_to_add.
      - bag: The DataBag to add.
      - dependencies: A non-empty collection of the names of the bags that `bag`
        depends on. It should include all the direct dependencies. There is no
        need to include transitive dependencies. All the names mentioned here
        must already be present in get_available_bag_names() or must be the name
        of some preceding item in bags_to_add.

    The implementation does not simply add the bags one by one - internally it
    persists them in parallel.

    After this function returns, the bags and all their transitive dependencies
    will be loaded and will hence be present in get_loaded_bag_names().

    Args:
      bags_to_add: A list of bags to add. They are added in the order given by
        the list.
    """
    available_bag_names = set(self.get_available_bag_names())
    for bag_to_add in bags_to_add:
      bag_name = bag_to_add.bag_name
      dependencies = bag_to_add.dependencies
      if bag_name in available_bag_names:
        raise ValueError(f"A bag with name '{bag_name}' was already added.")
      if not dependencies:
        raise ValueError(
            "The dependencies must not be empty. Use dependencies=('',) to "
            'depend only on the initial empty bag.'
        )
      for d in dependencies:
        if d not in available_bag_names:
          raise ValueError(
              f"A dependency on a bag with name '{d}' is invalid, because such"
              ' a bag was not added before.'
          )
      available_bag_names.add(bag_name)

    self._add_bags(bags_to_add)

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
    bags_to_add = [
        BagToAdd(
            bag_name,
            bag=self._loaded_bags_cache[bag_name],
            dependencies=tuple(bag_dependencies[bag_name]),
        )
        for bag_name in self._canonical_topological_sorting(bags_to_extract)
        if bag_name  # Skip the initial empty bag; new_manager already has one.
    ]
    new_manager.add_bags(bags_to_add)

  def create_branch(
      self,
      bag_names: Collection[str],
      *,
      with_all_dependents: bool = False,
      output_dir: str,
      fs: fs_interface.FileSystemInterface | None = None,
  ):
    """Creates a branch of the current manager in a new persistence directory.

    This function is very similar to extract_bags(), but it does not copy any of
    the bags. Instead, it will simply point to the original files. A manager
    for output_dir will therefore depend on the persistence directory of the
    current manager, which should not be moved or deleted as long as output_dir
    is used. After this function returns, the current manager and its branch
    are independent: adding bags to the current manager will not affect the
    branch, and similarly the branch won't affect the current manager.

    To create a branch with all the bags managed by this manager, you can call
    this function with the arguments bag_names=[''], with_all_dependents=True.

    Args:
      bag_names: The names of the bags that must be included in the branch. They
        must be a non-empty subset of get_available_bag_names(). The branch will
        also include their transitive dependencies.
      with_all_dependents: If True, then the branch will also include all the
        dependents of bag_names. The dependents are computed transitively. All
        the transitive dependencies of the dependents will also be included in
        the branch.
      output_dir: The directory in which the branch will be created. It must
        either be empty or not exist yet.
      fs: All interactions with the file system for output_dir will happen via
        this instance.
    """
    bags_to_branch = self._get_dependency_closure(
        bag_names, with_all_dependents=with_all_dependents
    )
    fs = fs or fs_util.get_default_file_system_interaction()
    if not fs.exists(output_dir):
      fs.make_dirs(output_dir)
    if fs.glob(os.path.join(output_dir, '*')):
      raise ValueError(
          f'The output_dir must be empty or not exist yet. Got {output_dir}'
      )

    branch_metadata = metadata_pb2.PersistedIncrementalDataBagManagerMetadata()
    branch_metadata.version = self._metadata.version
    branch_metadata.metadata_update_number = 0
    # The branch will be consistent with the total order of the bags in the
    # current manager, i.e. the order in which the bags were added.
    for bag_metadata in self._metadata.data_bag_metadata:
      if bag_metadata.name not in bags_to_branch:
        continue
      branch_bag_metadata = metadata_pb2.DataBagMetadata()
      branch_bag_metadata.CopyFrom(bag_metadata)
      if not branch_bag_metadata.HasField('directory'):
        branch_bag_metadata.directory = self._persistence_dir
      branch_metadata.data_bag_metadata.append(branch_bag_metadata)

    _persist_metadata(fs, output_dir, branch_metadata)

  def clear_cache(self):
    """Clears the cache of loaded bags.

    After this method returns, get_loaded_bag_names() will return only {''},
    i.e. only the initial empty bag with name '' will still be loaded.
    """
    initial_empty_bag = self._loaded_bags_cache[_INITIAL_BAG_NAME]
    self._loaded_bags_cache = {_INITIAL_BAG_NAME: initial_empty_bag}

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
    bags_to_load = self._get_dependency_closure(
        bag_names, with_all_dependents=with_all_dependents
    )
    self._load_bags_into_cache(bags_to_load)
    return bags_to_load

  def _add_bags(self, bags_to_add: list[BagToAdd]):
    """Adds the given bags in the given order to this manager.

    Callers must make sure that all the bags have fresh names and that all the
    dependencies are valid.

    After this function returns, the bags and all their transitive dependencies
    will be loaded and will hence be present in get_loaded_bag_names().

    Args:
      bags_to_add: A list of bags to add. They are added in the order given by
        the list.
    """
    # Load all the (transitive) dependencies that are already available. They
    # are not necessarily loaded yet.
    self._load_bags_into_cache(
        self._get_reflexive_and_transitive_closure_image(
            self._get_dependency_relation(),
            set(
                itertools.chain.from_iterable(
                    b.dependencies for b in bags_to_add
                )
            )
            & self.get_available_bag_names(),
        )
    )

    # Write the bags to files in parallel.
    bags = [bag_to_add.bag for bag_to_add in bags_to_add]
    bag_filenames = self._get_fresh_bag_filenames(len(bags_to_add))
    with concurrent.futures.ThreadPoolExecutor() as executor:
      futures = [
          executor.submit(self._write_bag_to_file, bag, bag_filename)
          for bag, bag_filename in zip(bags, bag_filenames)
      ]
    # Make sure all the writes completed successfully.
    for future in futures:
      future.result()

    new_metadata = metadata_pb2.PersistedIncrementalDataBagManagerMetadata()
    new_metadata.CopyFrom(self._metadata)
    new_metadata.metadata_update_number += 1
    for bag_to_add, bag_filename in zip(bags_to_add, bag_filenames):
      new_metadata.data_bag_metadata.append(
          metadata_pb2.DataBagMetadata(
              name=bag_to_add.bag_name,
              filename=bag_filename,
              dependencies=bag_to_add.dependencies,
          )
      )

    # Persist the new metadata in a transactional way. Concurrent writers are
    # detected and prevented from overwriting each other's changes when self._fs
    # supports atomic file-to-file renames.
    _persist_metadata(self._fs, self._persistence_dir, new_metadata)
    # The new metadata has be committed to disk. The persistence directory is
    # now at a new revision, but `self` is still at the revision where it was
    # when update() was called. Next, we want to update the state of "self" in a
    # transactional way to the new revision.
    try:
      # This assignment is atomic. The only purpose of this try block is to
      # show where to put assignments to future attributes that are conceptually
      # part of the transaction. Note that populating the cache is not part of
      # the transaction; it is done below after this try-except block.
      self._metadata = new_metadata
    except BaseException as e:
      e.add_note(
          'The manager is in an inconsistent state. Please use a new manager'
          ' instance to avoid data corruption.'
      )
      raise

    # Populate the cache of "self" with data of the new revision. No changes to
    # the cache should happen before the state of "self" is updated to the new
    # revision, which is why this code must follow the try-except block above.
    # If the cache is partially updated, e.g. when a KeyboardInterrupt happens
    # during the loop below, then the state of the current manager remains
    # consistent.
    for bag_to_add in bags_to_add:
      self._loaded_bags_cache[bag_to_add.bag_name] = bag_to_add.bag

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
      self, bag_names: Collection[str], *, with_all_dependents: bool
  ) -> AbstractSet[str]:
    """Returns the specified bags and all their transitive dependencies.

    Args:
      bag_names: The names of the bags that should be included in the closure.
        It must be a subset of get_available_bag_names(). All their transitive
        dependencies will also be included in the closure.
      with_all_dependents: If True, then all the dependents of bag_names will
        also be included in the closure. The dependents are computed
        transitively. All transitive dependencies of the dependents will also be
        included in the closure.

    Returns:
      The names of the bags in the minimal closure specified by the arguments.
    """
    # bag_names must be a collection of strings, not a single string that is
    # interpreted as a collection of character strings:
    assert not isinstance(bag_names, (str, bytes))
    bags_to_consider = set(bag_names)
    unknown_bags = bags_to_consider - self.get_available_bag_names()
    if unknown_bags:
      raise ValueError(
          'bag_names must be a subset of get_available_bag_names(). The'
          f' following bags are not available: {sorted(unknown_bags)}'
      )

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

  def _get_fresh_bag_filenames(self, num_filenames: int) -> list[str]:
    """Returns `num_filenames` filenames for bags that have not been used before."""
    session_id = _get_uuid()
    return [f'bag-{session_id}-{i:012d}.kd' for i in range(num_filenames)]

  def _write_bag_to_file(self, bag: kd.types.DataBag, bag_filename: str):
    fs_util.write_bag_to_file(
        self._fs, bag, self._get_bag_filepath_from_filename(bag_filename)
    )

  def _get_bag_filepath(self, bag_name: str) -> str:
    bag_metadata = next(
        m for m in self._metadata.data_bag_metadata if m.name == bag_name
    )
    if bag_metadata.HasField('directory'):
      return os.path.join(bag_metadata.directory, bag_metadata.filename)
    else:
      return self._get_bag_filepath_from_filename(bag_metadata.filename)

  def _read_bag_from_file(self, bag_name: str) -> kd.types.DataBag:
    bag_filepath = self._get_bag_filepath(bag_name)
    return fs_util.read_bag_from_file(self._fs, bag_filepath)

  def _get_bag_filepath_from_filename(self, bag_filename: str) -> str:
    return os.path.join(self._persistence_dir, bag_filename)


def _get_uuid() -> str:
  return str(uuid.uuid1())


# The update number is padded to 12 digits in filenames.
_UPDATE_NUMBER_NUM_DIGITS = 12


def _persist_metadata(
    fs: fs_interface.FileSystemInterface,
    persistence_dir: str,
    metadata: metadata_pb2.PersistedIncrementalDataBagManagerMetadata,
):
  """Persists the given metadata to disk.

  The writing of the metadata is atomic if `fs.rename(src_file, dst_file,
  overwrite=False)` is atomic, i.e. if the given `fs` object to interact with
  the file system supports atomic file-to-file renaming. In that case,
  concurrent writers are detected and prevented from overwriting each other's
  changes.

  Args:
    fs: The filesystem interface to use.
    persistence_dir: The persistence directory of the manager.
    metadata: The metadata to persist.
  """
  update_number = str(metadata.metadata_update_number).zfill(
      _UPDATE_NUMBER_NUM_DIGITS
  )
  final_filepath = os.path.join(persistence_dir, f'metadata-{update_number}.pb')
  if fs.exists(final_filepath):
    # Fail fast if the metadata already exists. No point in writing a temporary
    # file that will end up not being used because of this error.
    raise ValueError(
        f'While attemping to commit update {metadata.metadata_update_number},'
        ' we detected that it is already present in the destination directory'
        f' {persistence_dir}. Some other process must have updated the'
        ' directory in parallel. Updates via this manager are not possible'
        ' anymore. Consider calling create_branch() with a new directory to'
        ' create a new manager instance with the same state as this one but'
        ' that can perform updates, or create a new manager instance for'
        f' {persistence_dir} and attempt the update again.'
    )
  temp_filepath = os.path.join(
      persistence_dir,
      f'metadata-{update_number}-{_get_uuid()}.pb',
  )
  with fs.open(temp_filepath, 'wb') as f:
    f.write(metadata.SerializeToString())
  fs.rename(temp_filepath, final_filepath, overwrite=False)


def _read_latest_metadata(
    fs: fs_interface.FileSystemInterface,
    persistence_dir: str,
) -> metadata_pb2.PersistedIncrementalDataBagManagerMetadata:
  update_number_pattern = '[0-9]' * _UPDATE_NUMBER_NUM_DIGITS
  committed_metadata_filepaths = fs.glob(
      os.path.join(persistence_dir, f'metadata-{update_number_pattern}.pb')
  )
  latest_metadata_filepath = max(committed_metadata_filepaths)
  with fs.open(latest_metadata_filepath, 'rb') as f:
    return metadata_pb2.PersistedIncrementalDataBagManagerMetadata.FromString(
        f.read()
    )
