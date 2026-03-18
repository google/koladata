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

"""Interface to manage the initial data of a DataSliceManager."""

from __future__ import annotations

from typing import AbstractSet, Collection, Self

from koladata import kd
from koladata.ext.storage import data_slice_path as data_slice_path_lib


class InitialDataManagerInterface:
  """Manages the initial data of a DataSliceManager.

  The initial data must be immutable. Updates to it are accepted only by
  DataSliceManager, and never in this class/subclasses.

  Management of the initial data typically involves loading parts of it on
  demand from the underlying storage. The loaded data is typically cached in the
  global cache to speed up subsequent calls to get_data_slice(). For data that
  can consume significant memory, implementations should take care to cache at
  most one copy of the data (across globally cached DataBags and deeper caches
  in the initial data sourcing infrastructure), because otherwise out-of-memory
  errors might happen.
  """

  @classmethod
  def get_id(cls) -> str:
    """Returns a unique id for this kind of initial data manager."""
    raise NotImplementedError(type(cls))

  def serialize(
      self, persistence_dir: str, *, fs: kd.file_io.FileSystemInterface
  ):
    """Serializes the initial data and metadata to the given directory."""
    raise NotImplementedError(type(self))

  @classmethod
  def deserialize(
      cls, persistence_dir: str, *, fs: kd.file_io.FileSystemInterface
  ) -> InitialDataManagerInterface:
    """Deserializes the initial data and metadata from the given directory."""
    raise NotImplementedError(type(cls))

  def get_schema(self) -> kd.types.SchemaItem:
    """Returns the schema of the full initial data.

    All itemids therein must be stable, in the sense that they are always the
    same for repeated calls, even after serialization and deserialization
    roundtrips.

    The schema is never missing, and always for the full initial data.
    """
    raise NotImplementedError(type(self))

  def get_schema_at(
      self, path: data_slice_path_lib.DataSlicePath
  ) -> kd.types.SchemaItem:
    """Returns the schema of the DataSlice at the given path."""
    raise NotImplementedError(type(self))

  def get_all_schema_node_names(self) -> AbstractSet[str]:
    """Returns all the schema node names of self.get_schema().

    Always equivalent to
    SchemaHelper(self.get_schema()).get_all_schema_node_names(), but an
    implementation would typically cache the names.
    """
    raise NotImplementedError(type(self))

  def get_data_slice_for_schema_node_names(
      self, schema_node_names: Collection[str]
  ) -> kd.types.DataSlice:
    """Returns the root data slice populated only with the requested data.

    The root data slice must be a scalar with an entity schema. It is always
    present, i.e. never missing. All itemids in the returned data slice must be
    stable, in the sense that they are always the same for repeated calls, even
    after serialization and deserialization roundtrips.

    Args:
      schema_node_names: The names of the schema nodes whose data should be
        included in the returned data slice. Must be a subset of
        self.get_all_schema_node_names(). Care should be taken that the result
        contains only the root and the requested data, and nothing more than
        that. The reason is that all the data returned by DataSliceManager
        should be up-to-date. If this manager returns more data than requested,
        and the extra data has been updated in the meantime, then the extra data
        will be outdated because DataSliceManager will not apply the updates
        before returning the data to the user.
    """
    raise NotImplementedError(type(self))

  def get_data_bag_for_schema_node_names(
      self, schema_node_names: Collection[str]
  ) -> kd.types.DataBag:
    """Returns a DataBag with the data for the given schema node names.

    All itemids in the returned data bag must be stable, in the sense that they
    are always the same for repeated calls, even after serialization and
    deserialization roundtrips.

    Args:
      schema_node_names: The names of the schema nodes whose data should be
        included in the returned data bag. Must be a subset of
        self.get_all_schema_node_names(). Care should be taken that the result
        contains only the requested data, and nothing more than that. The reason
        is that all the data returned by DataSliceManager should be up-to-date.
        If this manager returns more data than requested, and the extra data has
        been updated in the meantime, then the extra data will be outdated
        because DataSliceManager will not apply the updates before returning the
        data to the user.
    """
    raise NotImplementedError(type(self))

  def exists(self, path: data_slice_path_lib.DataSlicePath) -> bool:
    """Returns whether the given data slice path exists for this manager."""
    raise NotImplementedError(type(self))

  def get_data_slice(
      self,
      populate: Collection[data_slice_path_lib.DataSlicePath] | None = None,
      populate_including_descendants: (
          Collection[data_slice_path_lib.DataSlicePath] | None
      ) = None,
  ) -> kd.types.DataSlice:
    """Returns the dataslice with data for the requested data slice paths.

    The DataBags of the returned DataSlices are guaranteed to be compatible with
    each other. For example,
    manager.get_data_slice({p1}).updated(manager.get_data_slice({p2}).get_bag())
    will be a DataSlice populated with data for paths p1 and p2, and will be
    equivalent to manager.get_data_slice({p1, p2}).

    The result might contain more data than requested. All the data in the
    result is guaranteed to be valid and up-to-date.

    Args:
      populate: The set of paths whose data must be populated in the result.
        Each path must be valid, i.e. self.exists(path) must be True.
      populate_including_descendants: A set of paths whose data must be
        populated in the result; the data of all their descendant paths must
        also be populated. Descendants are computed with respect to the schema,
        i.e. self.get_schema(). Each path must be valid, i.e. self.exists(path)
        must be True.

    Returns:
      The root dataslice populated with data for the requested data slice paths.
    """
    raise NotImplementedError(type(self))

  def get_data_slice_at(
      self,
      path: data_slice_path_lib.DataSlicePath,
      with_all_descendants: bool = False,
  ) -> kd.types.DataSlice:
    """Returns the data slice managed by this manager at the given path.

    Args:
      path: The path for which the data slice is requested. It must be valid:
        self.exists(path) must be True.
      with_all_descendants: If True, then the result will also include the data
        of all the descendant paths of `path`.

    Returns:
      The data slice managed by this manager at the given path.
    """
    raise NotImplementedError(type(self))

  def get_description(self) -> str:
    """Returns a brief description of the initial data.

    If the user does not provide a description when creating a DataSliceManager,
    then the manager will craft a description of the form
    f'Initial state with {initial_data_manager.get_description()}'.
    """
    raise NotImplementedError(type(self))

  def copy(self) -> Self:
    """Returns a copy of this manager.

    It should be faster to create a copy than to serialize+deserialize this
    manager.

    It should be possible to use the copy in another thread. If there is
    internal mutable state, such as objects to access data sources, that are
    shared between this instance and the copy, then that state must be properly
    synchronized.
    """
    raise NotImplementedError(type(self))
