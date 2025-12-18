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

"""Initial data that is a given empty entity or kd.new() if not provided."""

import os
from typing import AbstractSet, Collection, cast

from koladata import kd
from koladata.ext.persisted_data import fs_interface
from koladata.ext.persisted_data import fs_util
from koladata.ext.persisted_data import initial_data_manager_interface
from koladata.ext.persisted_data import initial_data_manager_registry
from koladata.ext.persisted_data import schema_helper


class BareRootInitialDataManager(
    initial_data_manager_interface.InitialDataManagerInterface
):
  """Initial data that is a given empty entity or kd.new() if not provided."""

  @classmethod
  def get_id(cls) -> str:
    return 'BareRootInitialDataManager'

  def __init__(self, root_item: kd.types.DataItem | None = None):
    if root_item is None:
      root_item = kd.new()

    # Get rid of data that happens to be inside the DataBag of root_item, but
    # that is not referenced (transitively) by the root_item.
    try:
      root_item = root_item.extract()
    except ValueError:
      # Failure to extract means that the root_item does not have a DataBag. No
      # bag means no extra unreferenced data, so all is fine.
      pass

    self._root_item = root_item
    del root_item  # Forces the use of the attribute henceforth.
    if not kd.is_item(self._root_item):
      raise ValueError(
          f'the root must be a scalar, i.e. a DataItem. Got: {self._root_item}'
      )
    if not kd.has(self._root_item):
      raise ValueError(f'the root must be present. Got: {self._root_item}')
    root_schema = self._root_item.get_schema()
    if not root_schema.is_entity_schema():
      raise ValueError(
          f'the root must have an entity schema. Got: {self._root_item}'
      )
    if kd.dir(root_schema):
      raise ValueError(
          f'the root must not have any attributes. Got: {self._root_item}'
      )
    # Checks that the root schema is acceptable, e.g. that it does not contain
    # non-primitive metadata attributes:
    self._schema_helper = schema_helper.SchemaHelper(root_schema)

  def serialize(
      self, persistence_dir: str, *, fs: fs_interface.FileSystemInterface
  ):
    if not fs.exists(persistence_dir):
      fs.make_dirs(persistence_dir)
    if fs.glob(os.path.join(persistence_dir, '*')):
      raise ValueError(
          f'the given persistence_dir {persistence_dir} is not empty'
      )
    fs_util.write_slice_to_file(
        fs,
        self._root_item,
        _get_root_dataslice_filepath(persistence_dir),
    )

  @classmethod
  def deserialize(
      cls, persistence_dir: str, *, fs: fs_interface.FileSystemInterface
  ) -> initial_data_manager_interface.InitialDataManagerInterface:
    filepath = _get_root_dataslice_filepath(persistence_dir)
    if not fs.exists(filepath):
      if not fs.exists(persistence_dir):
        raise ValueError(f'persistence_dir not found: {persistence_dir}')
      raise ValueError(f'file not found: {filepath}')
    return BareRootInitialDataManager(
        root_item=cast(
            kd.types.DataItem, fs_util.read_slice_from_file(fs, filepath)
        )
    )

  def get_schema(self) -> kd.types.SchemaItem:
    return self._root_item.get_schema()

  def get_all_schema_node_names(self) -> AbstractSet[str]:
    return self._schema_helper.get_all_schema_node_names()

  def get_data_slice(
      self, schema_node_names: Collection[str]
  ) -> kd.types.DataSlice:
    invalid_schema_node_names = (
        set(schema_node_names) - self.get_all_schema_node_names()
    )
    if invalid_schema_node_names:
      raise ValueError(
          'schema_node_names contains invalid entries:'
          f' {invalid_schema_node_names}'
      )
    return self._root_item

  def clear_cache(self):
    pass

  def get_description(self) -> str:
    return 'an empty root'


def _get_root_dataslice_filepath(persistence_dir: str) -> str:
  return os.path.join(persistence_dir, 'root.kd')


initial_data_manager_registry.register_initial_data_manager(
    BareRootInitialDataManager
)
