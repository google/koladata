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

"""InitialDataManager that provides some functionality via a SchemaHelper."""

from __future__ import annotations

from typing import Collection

from koladata import kd
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import fs_interface
from koladata.ext.persisted_data import initial_data_manager_interface
from koladata.ext.persisted_data import schema_helper
from koladata.ext.persisted_data import schema_helper_mixin


class InitialDataManagerWithSchemaHelper(
    schema_helper_mixin.SchemaHelperMixin,
    initial_data_manager_interface.InitialDataManagerInterface,
):
  """InitialDataManager that provides common functionality via a SchemaHelper.

  Subclasses must implement the _get_schema_helper() method. By doing that, they
  will gain extra functionality, e.g. a fully implemented get_data_slice()
  method.
  """

  @classmethod
  def get_id(cls) -> str:
    raise NotImplementedError(type(cls))

  def serialize(
      self, persistence_dir: str, *, fs: fs_interface.FileSystemInterface
  ):
    raise NotImplementedError(type(self))

  @classmethod
  def deserialize(
      cls, persistence_dir: str, *, fs: fs_interface.FileSystemInterface
  ) -> InitialDataManagerWithSchemaHelper:
    raise NotImplementedError(type(cls))

  def _get_schema_helper(self) -> schema_helper.SchemaHelper:
    raise NotImplementedError(type(self))

  def get_data_slice(
      self,
      populate: Collection[data_slice_path_lib.DataSlicePath] | None = None,
      populate_including_descendants: (
          Collection[data_slice_path_lib.DataSlicePath] | None
      ) = None,
  ) -> kd.types.DataSlice:
    return self.get_data_slice_for_schema_node_names(
        self._get_schema_helper().get_schema_node_names_needed_to(
            populate=populate,
            populate_including_descendants=populate_including_descendants,
        )
    )

  def get_data_slice_for_schema_node_names(
      self, schema_node_names: Collection[str]
  ) -> kd.types.DataSlice:
    raise NotImplementedError(type(self))

  def get_data_bag_for_schema_node_names(
      self, schema_node_names: Collection[str]
  ) -> kd.types.DataBag:
    raise NotImplementedError(type(self))

  def clear_cache(self):
    raise NotImplementedError(type(self))

  def get_description(self) -> str:
    raise NotImplementedError(type(self))
