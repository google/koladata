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

"""A simple DataSlice manager useful for testing."""

from typing import Generator

from koladata import kd
from koladata.ext.persisted_data import data_slice_manager_interface
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import schema_helper as schema_helper_lib


class SimpleInMemoryDataSliceManager(
    data_slice_manager_interface.DataSliceManagerInterface
):
  """Simple in-memory DataSlice manager.

  It manages the entire DataSlice in memory. It is not persisted
  anywhere, and everything is always loaded. It implements the simple behavior
  of vanilla Koda.

  This manager is useful for testing, mainly to make sure that the behavior of
  an advanced manager is consistent with the behavior of vanilla Koda, or more
  rarely when we want to point out differences in behavior.
  """

  def __init__(self):
    self._ds = kd.new()

  def get_schema(self) -> kd.types.DataSlice:
    return self._ds.get_schema()

  def generate_paths(
      self, *, max_depth: int
  ) -> Generator[data_slice_path_lib.DataSlicePath, None, None]:
    yield from self._schema_helper().generate_available_data_slice_paths(
        max_depth=max_depth
    )

  def exists(self, path: data_slice_path_lib.DataSlicePath) -> bool:
    return self._schema_helper().is_valid_data_slice_path(path)

  def get_data_slice(
      self,
      at_path: data_slice_path_lib.DataSlicePath | None = None,
      with_all_descendants: bool = False,
  ) -> kd.types.DataSlice:
    del with_all_descendants
    self._check_is_valid_data_slice_path(at_path)
    return data_slice_path_lib.get_subslice(self._ds, at_path)

  def update(
      self,
      *,
      at_path: data_slice_path_lib.DataSlicePath,
      attr_name: str,
      attr_value: kd.types.DataSlice,
      overwrite_schema: bool = False,
  ):
    self._check_is_valid_data_slice_path(at_path)
    try:
      extracted_attr_value = attr_value.extract()
    except ValueError:
      # Some slices don't have DataBags. In that case, we just pass the slice
      # through, since it is already minimal without superfluous data.
      extracted_attr_value = attr_value
    del attr_value  # To avoid accidental misuse.
    ds = self._ds.updated(
        kd.attrs(
            data_slice_path_lib.get_subslice(self._ds, at_path),
            **{attr_name: extracted_attr_value},
            overwrite_schema=overwrite_schema,
        )
    )
    # Check that the new schema is valid. For example, it must not use kd.OBJECT
    # anywhere, and schema metadata must be primitives.
    schema_helper_lib.SchemaHelper(ds.get_schema())
    self._ds = ds

  def _schema_helper(self) -> schema_helper_lib.SchemaHelper:
    return schema_helper_lib.SchemaHelper(self._ds.get_schema())

  def _check_is_valid_data_slice_path(
      self,
      data_slice_path: data_slice_path_lib.DataSlicePath,
  ):
    if not self.exists(data_slice_path):
      raise ValueError(
          f'Data slice path {data_slice_path} is not valid for schema'
          f' {self._ds.get_schema()}'
      )
