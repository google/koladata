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

"""Test-only utilities to help with schema node names and sets of them."""

from koladata import kd
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import schema_helper


def schema_node_name(
    schema: kd.types.DataItem,
    *,
    action: data_slice_path_lib.DataSliceAction | None = None,
) -> str:
  """Returns the schema node name of `schema`, or a subschema thereof.

  Args:
    schema: The parent schema. If `action` is None, then the schema node name of
      `schema` is returned. Otherwise, the schema node name of the subschema
      obtained by applying `action.get_subschema(schema)` is returned.
    action: The action to get the subschema of `schema`.
  """
  snn = schema_helper.get_schema_node_name_from_schema_having_an_item_id(
      schema
  )
  if action is None:
    return snn
  child_schema_item = action.get_subschema(schema)
  if schema_helper._get_item_id(child_schema_item) is not None:  # pylint: disable=protected-access
    raise ValueError(
        f'the child schema {child_schema_item} has an item id. Please use'
        ' schema_node_name(child_schema) instead, i.e. without the'
        ' "action" kwarg'
    )
  return schema_helper.get_schema_node_name(
      parent_schema_item=schema,
      action=action,
      child_schema_item=child_schema_item,
  )


def to_same_len_set(schema_node_names: list[str]) -> set[str]:
  """Converts a list of schema node names to a set, checking for equal cardinality."""
  result = set(schema_node_names)
  assert len(result) == len(
      schema_node_names
  ), f'duplicate schema node names: {schema_node_names}'
  return result
