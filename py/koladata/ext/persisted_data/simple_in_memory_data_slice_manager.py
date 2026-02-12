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

from typing import Collection, Generator

from koladata import kd
from koladata.ext.persisted_data import data_slice_manager_interface
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import schema_helper as schema_helper_lib
from koladata.ext.persisted_data import schema_helper_mixin


class SimpleInMemoryDataSliceManager(
    schema_helper_mixin.SchemaHelperMixin,
    data_slice_manager_interface.DataSliceManagerInterface,
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
    self._is_read_only = False

  def generate_paths(
      self, *, max_depth: int
  ) -> Generator[data_slice_path_lib.DataSlicePath, None, None]:
    yield from self._get_schema_helper().generate_available_data_slice_paths(
        max_depth=max_depth
    )

  def get_data_slice(
      self,
      populate: Collection[data_slice_path_lib.DataSlicePath] | None = None,
      populate_including_descendants: (
          Collection[data_slice_path_lib.DataSlicePath] | None
      ) = None,
  ) -> kd.types.DataSlice:
    populate = populate or set()
    populate_including_descendants = populate_including_descendants or set()
    for path in populate:
      if not self.exists(path):
        raise ValueError(
            f"data slice path '{path}' passed in argument 'populate' is invalid"
        )
    for path in populate_including_descendants:
      if not self.exists(path):
        raise ValueError(
            f"data slice path '{path}' passed in argument"
            " 'populate_including_descendants' is invalid"
        )

    return self._ds

  @property
  def is_read_only(self) -> bool:
    """Returns whether this manager is in read-only mode."""
    return self._is_read_only

  def set_read_only(self):
    """Sets this manager instance to read-only mode.

    Update operations will henceforth raise a ValueError.
    """
    self._is_read_only = True

  def update(
      self,
      *,
      at_path: data_slice_path_lib.DataSlicePath,
      attr_name: str,
      attr_value: kd.types.DataSlice,
      description: str | None = None,
  ):
    self._check_is_not_read_only()
    self._check_is_valid_data_slice_path(at_path)
    self._check_has_entity_schema(at_path)
    try:
      extracted_attr_value = attr_value.extract()
    except ValueError:
      # Some slices don't have DataBags. In that case, we just pass the slice
      # through, since it is already minimal without superfluous data.
      extracted_attr_value = attr_value
    del attr_value  # To avoid accidental misuse.
    ds = self._ds.updated(
        kd.attrs(
            at_path.evaluate(self._ds),
            **{attr_name: extracted_attr_value},
            overwrite_schema=True,
        )
    )
    # Check that the new schema is valid. For example, it must not use kd.OBJECT
    # anywhere, and schema metadata must be primitives.
    schema_helper_lib.SchemaHelper(ds.get_schema())
    self._ds = ds

  def _get_schema_helper(self) -> schema_helper_lib.SchemaHelper:
    return schema_helper_lib.SchemaHelper(self._ds.get_schema())

  def _check_is_not_read_only(self):
    if self.is_read_only:
      raise ValueError('this manager is in read-only mode')

  def _check_is_valid_data_slice_path(
      self,
      data_slice_path: data_slice_path_lib.DataSlicePath,
  ):
    if not self.exists(data_slice_path):
      raise ValueError(f"invalid data slice path: '{data_slice_path}'")

  def _check_has_entity_schema(
      self,
      data_slice_path: data_slice_path_lib.DataSlicePath,
  ):
    schema_node_name = (
        self._get_schema_helper().get_schema_node_name_for_data_slice_path(
            data_slice_path
        )
    )
    schema = self._get_schema_helper().get_subschema_at(schema_node_name)
    if not schema.is_entity_schema():
      raise ValueError(
          f"the schema at data slice path '{data_slice_path}' is {schema},"
          " which does not support updates. Please pass a data slice path that"
          " is associated with an entity schema"
      )
