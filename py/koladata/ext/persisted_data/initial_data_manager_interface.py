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

"""Interface to manage the initial data of a PersistedIncrementalDataSliceManager."""

from __future__ import annotations

from typing import AbstractSet, Collection

from koladata import kd
from koladata.ext.persisted_data import fs_interface


class InitialDataManagerInterface:
  """Manages the initial data of a PersistedIncrementalDataSliceManager.

  The initial data must be immutable. Updates to it are accepted only by
  PersistedIncrementalDataSliceManager, and never in this class/subclasses.

  Management of the initial data typically involves loading parts of it on
  demand from the underlying storage. The loaded data is typically cached to
  speed up subsequent calls to get_data_slice(). For data that can consume
  significant memory, implementations should take care to cache at most one
  copy of the data (across locally cached DataBags and deeper caches in the data
  sourcing infrastructure), because otherwise out-of-memory errors might happen.
  Clients can also call clear_cache() to remove the cached data; the expectation
  is that after calling clear_cache(), the memory footprint of an instance
  should be very small.
  """

  @classmethod
  def get_id(cls) -> str:
    """Returns a unique id for this kind of initial data manager."""
    raise NotImplementedError(type(cls))

  def serialize(
      self, persistence_dir: str, *, fs: fs_interface.FileSystemInterface
  ):
    """Serializes the initial data and metadata to the given directory."""
    raise NotImplementedError(type(self))

  @classmethod
  def deserialize(
      cls, persistence_dir: str, *, fs: fs_interface.FileSystemInterface
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

  def get_all_schema_node_names(self) -> AbstractSet[str]:
    """Returns all the schema node names of self.get_schema().

    Always equivalent to
    SchemaHelper(self.get_schema()).get_all_schema_node_names(), but an
    implementation would typically cache the names and map them to data sources
    in order to implement self.get_data_slice().
    """
    raise NotImplementedError(type(self))

  def get_data_slice(
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
        that. The reason is that all the data returned by
        PersistedIncrementalDataSliceManager should be up-to-date. If this
        manager returns more data than requested, and the extra data has been
        updated in the meantime, then the extra data will be outdated because
        PersistedIncrementalDataSliceManager will not apply the updates before
        returning the data to the user.
    """
    raise NotImplementedError(type(self))

  def clear_cache(self):
    """Clears the cache of loaded data (if applicable) to release memory.

    Whether and what data is cached is implementation-specific. The expectation
    is that data that takes up significant memory space should be removed from
    any caches by this method. When the data is requested again in the future,
    then it will be loaded from the underlying storage and might be cached
    again.

    Calling this method will not affect the functional behavior of this manager.
    For example, the result of get_schema() will remain unchanged, and calling
    get_data_slice(...) will simply load the data again and return the same
    result as before.
    """
    raise NotImplementedError(type(self))

  def get_description(self) -> str:
    """Returns a brief description of the initial data.

    If the user does not provide a description when creating a
    PersistedIncrementalDataSliceManager, then the manager will craft a
    description of the form
    f'Initial state with {initial_data_manager.get_description()}'.
    """
    raise NotImplementedError(type(self))
