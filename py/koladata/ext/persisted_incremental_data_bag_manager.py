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

"""Management of a DataBag that is assembled from smaller bags.

The main user-facing abstraction in this module is the class
PersistedIncrementalDataBagManager.
"""

import collections
import glob
import os
from typing import Collection, IO, Iterable

from koladata import kd


class FileSystemInterface:
  """Interface to interact with the file system."""

  def exists(self, filepath: str) -> bool:
    raise NotImplementedError

  def remove(self, filepath: str):
    raise NotImplementedError

  def open(self, filepath: str, mode: str) -> IO[bytes | str]:
    raise NotImplementedError

  def make_dirs(self, dirpath: str):
    raise NotImplementedError

  def glob(self, pattern: str) -> Collection[str]:
    raise NotImplementedError


class FileSystemInteraction(FileSystemInterface):
  """Interacts with the file system."""

  def exists(self, filepath: str) -> bool:
    return os.path.exists(filepath)

  def remove(self, filepath: str):
    os.remove(filepath)

  def open(self, filepath: str, mode: str) -> IO[bytes | str]:
    return open(filepath, mode)

  def make_dirs(self, dirpath: str):
    os.makedirs(dirpath, exist_ok=True)

  def glob(self, pattern: str) -> Collection[str]:
    return glob.glob(pattern)


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
      fs: FileSystemInterface = FileSystemInteraction(),
  ):
    """Initializes the manager.

    Args:
      persistence_dir: The directory where the small bags and metadata will be
        persisted. If it does not exist, or it is empty, then it will be
        populated with an empty bag named ''. Otherwise, the manager will be
        initialized from the existing artifacts in the directory.
      fs: All interactions with the file system will go through this instance.
    """
    self._persistence_dir: str = persistence_dir
    self._fs = fs
    del persistence_dir, fs  # Forces the use of the attributes henceforth.
    if not self._fs.exists(self._persistence_dir):
      self._fs.make_dirs(self._persistence_dir)
    if not self._fs.glob(os.path.join(self._persistence_dir, '*')):  # Empty dir
      self._bag: kd.types.DataBag = kd.bag()
      bag_filename = self._get_fresh_bag_filename()
      write_bag_to_file(
          self._bag, self._get_bag_filepath(bag_filename), fs=self._fs
      )
      self._bag_name_to_filename: dict[str, str] = {
          _INITIAL_BAG_NAME: bag_filename
      }
      self._persist_bag_name_to_filepath()
      self._bag_dependencies: dict[str, list[str]] = {_INITIAL_BAG_NAME: []}
      self._persist_bag_dependencies()
      self._loaded_bag_names: set[str] = {_INITIAL_BAG_NAME}
      self._metadata = dict(version='1.0.0')
      self._persist_metadata()
    else:
      self._bag_name_to_filename = self._read_bag_name_to_filename()
      self._bag_dependencies = self._read_bag_dependencies()
      bag_filename = self._bag_name_to_filename[_INITIAL_BAG_NAME]
      self._bag = read_bag_from_file(
          self._get_bag_filepath(bag_filename), fs=self._fs
      )
      self._loaded_bag_names = {_INITIAL_BAG_NAME}
      self._metadata = self._read_metadata()

  def get_available_bag_names(self) -> frozenset[str]:
    """Returns the names of all bags that are managed by this manager.

    They include the initial empty bag (named ''), all bags that have been added
    to this manager instance, and all bags that were already persisted in the
    persistence directory before this manager instance was created.
    """
    return frozenset(self._bag_name_to_filename.keys())

  def get_loaded_bag_names(self) -> frozenset[str]:
    """Returns the names of all bags that are currently loaded in this manager.

    The initial empty bag (with name '') is always loaded, and the bags that
    have been added to this manager instance or loaded by previous calls to
    get_bag() are also considered loaded.
    """
    return frozenset(self._loaded_bag_names)

  def get_bag(
      self,
      bag_name: str = _INITIAL_BAG_NAME,
      with_all_dependents: bool = False,
  ) -> kd.types.DataBag:
    """Returns a bag that includes bag_name and its transitive dependencies.

    The result will also include all bags that have already been loaded. Their
    names can be found by calling get_loaded_bag_names(). You can also call
    get_loaded_bag_names() after this function returns to determine the names of
    the newly loaded bags.

    Args:
      bag_name: The name of the bag whose data must be part of the result. It
        must be a member of get_available_bag_names(). If not loaded yet, then
        it will be loaded after all its transitive dependencies are loaded. If
        not specified, then it is assumed to refer to the initial empty bag
        (with name ''), which is always loaded.
      with_all_dependents: If True, then the returned bag will also include all
        dependents of bag_name. The dependents are computed transitively. All
        transitive dependencies of the dependents are then also included in the
        result.

    Returns:
      The currently loaded bag, which includes the previously loaded bags, the
      bag called bag_name, its transitive dependencies, and if requested, the
      transitive dependents of bag_name and their transitive dependencies.
    """
    if bag_name not in self._bag_name_to_filename:
      raise ValueError(
          f"There is no bag with name '{bag_name}'. Valid bag names:"
          f' {sorted(self.get_available_bag_names())}'
      )

    bags_to_consider = {bag_name}
    if with_all_dependents:
      bags_to_consider.update(
          self._get_reflexive_and_transitive_closure_image(
              self._get_reverse_dependencies(), bags_to_consider
          )
      )

    self._load_bags(
        self._get_reflexive_and_transitive_closure_image(
            self._bag_dependencies, bags_to_consider
        )
        - self._loaded_bag_names
    )

    return self._bag

  def add_bag(
      self,
      bag_name: str,
      bag: kd.types.DataBag,
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
    if bag_name in self._bag_dependencies:
      raise ValueError(f"A bag with name '{bag_name}' was already added.")
    if not dependencies:
      raise ValueError(
          "The dependencies must not be empty. Use dependencies={''} to "
          'depend only on the initial empty bag.'
      )
    for d in dependencies:
      if d not in self._bag_dependencies:
        raise ValueError(
            f"A dependency on a bag with name '{d}' is invalid, because such a"
            ' bag was not added before.'
        )

    self._load_bags(
        self._get_reflexive_and_transitive_closure_image(
            self._bag_dependencies, set(dependencies)
        )
        - self._loaded_bag_names
    )

    bag_filename = self._get_fresh_bag_filename()
    write_bag_to_file(bag, self._get_bag_filepath(bag_filename), fs=self._fs)
    self._bag_name_to_filename[bag_name] = bag_filename
    self._persist_bag_name_to_filepath()
    self._bag_dependencies[bag_name] = list(dependencies)
    self._persist_bag_dependencies()
    self._bag = kd.bags.updated(self._bag, bag)
    self._loaded_bag_names.add(bag_name)

  def _get_reverse_dependencies(self) -> dict[str, set[str]]:
    """Returns a dictionary of the reverse dependency graph."""
    result = collections.defaultdict(set)
    for bag_name, dependencies in self._bag_dependencies.items():
      for dependency in dependencies:
        result[dependency].add(bag_name)
    return result

  def _get_reflexive_and_transitive_closure_image(
      self,
      relation: dict[str, Iterable[str]],
      bag_names: set[str],
  ) -> set[str]:
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

  def _load_bags(self, bags_to_load: set[str]):
    """Loads all the requested bags into self._bag.

    The loading will make sure that, before loading a bag, all its dependencies
    are already loaded.

    Args:
      bags_to_load: The names of the bags to load into self._bag. They must be a
        subset of get_available_bag_names() and disjoint from
        get_loaded_bag_names(). They must be closed with respect to the
        dependency graph, i.e. it must be the case that `{d for bn in
        bags_to_load for d in self._bag_dependencies[bn]}` is included in
        bags_to_load | self.get_loaded_bag_names(). The provided bags_to_load
        set will be modified in place, so callers should not make assumptions
        about its state after calling this private helper function.
    """
    # If this is too slow, we can consider reading the bags in parallel from the
    # filesystem.
    while bags_to_load:
      for bag_name in bags_to_load:
        if not all(
            d in self._loaded_bag_names
            for d in self._bag_dependencies[bag_name]
        ):
          # All the dependencies of this bag are not loaded yet. Consider it
          # later.
          continue
        self._bag = kd.bags.updated(
            self._bag,
            read_bag_from_file(
                self._get_bag_filepath(self._bag_name_to_filename[bag_name]),
                fs=self._fs,
            ),
        )
        self._loaded_bag_names.add(bag_name)
        bags_to_load.remove(bag_name)
        break  # Because we updated bags_to_load.

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

  def _get_bag_filepath(self, bag_name: str) -> str:
    return os.path.join(self._persistence_dir, bag_name)

  def _persist_bag_dependencies(self):
    item = kd.from_py(
        self._bag_dependencies,
        schema=kd.dict_schema(kd.STRING, kd.list_schema(kd.STRING)),
    )
    write_slice_to_file(
        item,
        filepath=self._get_bag_dependencies_filepath(),
        overwrite=True,
        fs=self._fs,
    )

  def _read_bag_dependencies(self) -> dict[str, list[str]]:
    item = read_slice_from_file(
        self._get_bag_dependencies_filepath(), fs=self._fs
    )
    return item.to_py(max_depth=-1)

  def _get_bag_dependencies_filepath(self) -> str:
    return os.path.join(self._persistence_dir, 'bag_dependencies.kd')

  def _persist_bag_name_to_filepath(self):
    item = kd.dict(
        self._bag_name_to_filename,
        key_schema=kd.STRING,
        value_schema=kd.STRING,
    )
    write_slice_to_file(
        item,
        filepath=self._get_bag_name_to_filepath_filepath(),
        overwrite=True,
        fs=self._fs,
    )

  def _read_bag_name_to_filename(self) -> dict[str, str]:
    item = read_slice_from_file(
        self._get_bag_name_to_filepath_filepath(), fs=self._fs
    )
    return item.to_py(max_depth=-1)

  def _get_bag_name_to_filepath_filepath(self) -> str:
    return os.path.join(self._persistence_dir, 'bag_name_to_filepath.kd')

  def _persist_metadata(self):
    item = kd.dict(self._metadata)
    write_slice_to_file(
        item,
        filepath=self._get_metadata_filepath(),
        overwrite=True,
        fs=self._fs,
    )

  def _read_metadata(self) -> dict[str, str]:
    item = read_slice_from_file(self._get_metadata_filepath(), fs=self._fs)
    return item.to_py(max_depth=-1)

  def _get_metadata_filepath(self) -> str:
    return os.path.join(self._persistence_dir, 'metadata.kd')


def write_slice_to_file(
    ds: kd.types.DataSlice,
    filepath: str,
    *,
    overwrite: bool = False,
    fs: FileSystemInterface = FileSystemInteraction(),
):
  """Writes the given DataSlice to a file; overwrites the file if requested."""
  if fs.exists(filepath):
    if overwrite:
      fs.remove(filepath)
    else:
      raise ValueError(f'File {filepath} already exists.')
  with fs.open(filepath, 'wb') as f:
    f.write(kd.dumps(ds, riegeli_options='snappy'))


def read_slice_from_file(
    filepath: str, *, fs: FileSystemInterface = FileSystemInteraction()
) -> kd.types.DataSlice:
  with fs.open(filepath, 'rb') as f:
    return kd.loads(f.read())


def write_bag_to_file(
    ds: kd.types.DataBag,
    filepath: str,
    *,
    overwrite: bool = False,
    fs: FileSystemInterface = FileSystemInteraction(),
):
  """Writes the given DataBag to a file; overwrites the file if requested."""
  if fs.exists(filepath):
    if overwrite:
      fs.remove(filepath)
    else:
      raise ValueError(f'File {filepath} already exists.')
  with fs.open(filepath, 'wb') as f:
    f.write(kd.dumps(ds, riegeli_options='snappy'))


def read_bag_from_file(
    filepath: str, *, fs: FileSystemInterface = FileSystemInteraction()
) -> kd.types.DataBag:
  with fs.open(filepath, 'rb') as f:
    return kd.loads(f.read())
