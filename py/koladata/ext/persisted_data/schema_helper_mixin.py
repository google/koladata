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

"""Data manager mixin that uses a SchemaHelper to provide common functionality."""

from typing import Collection

from koladata import kd
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import schema_helper as schema_helper_lib


class SchemaHelperMixin:
  """Data manager mixin that uses a SchemaHelper to provide common functionality.

  Subclasses must implement the _get_schema_helper() method and the
  get_data_slice() methods.

  By doing that, they will gain extra functionality, e.g. fully implemented
  get_data_slice_at(), exists() and get_schema() methods.
  """

  def _get_schema_helper(self) -> schema_helper_lib.SchemaHelper:
    raise NotImplementedError(type(self))

  def get_data_slice(
      self,
      populate: Collection[data_slice_path_lib.DataSlicePath] | None = None,
      populate_including_descendants: (
          Collection[data_slice_path_lib.DataSlicePath] | None
      ) = None,
  ) -> kd.types.DataSlice:
    raise NotImplementedError(type(self))

  def get_schema(self) -> kd.types.SchemaItem:
    return self._get_schema_helper().get_schema()

  def get_schema_at(
      self, path: data_slice_path_lib.DataSlicePath
  ) -> kd.types.SchemaItem:
    """Returns the schema of the DataSlice at the given path."""
    schema_helper = self._get_schema_helper()
    return schema_helper.get_subschema_at(
        schema_helper.get_schema_node_name_for_data_slice_path(path)
    )

  def exists(self, path: data_slice_path_lib.DataSlicePath) -> bool:
    return self._get_schema_helper().exists(path)

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
    if with_all_descendants:
      populate = None
      populate_including_descendants = {path}
    else:
      populate = {path}
      populate_including_descendants = None
    ds = self.get_data_slice(
        populate=populate,
        populate_including_descendants=populate_including_descendants,
    )
    return path.evaluate(ds)
