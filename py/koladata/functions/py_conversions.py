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

"""Koda functions for converting to and from Python structures."""

from typing import Any

from koladata.types import data_bag
from koladata.types import data_item as _  # pylint: disable=unused-import
from koladata.types import data_slice
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals


def from_py(
    py_obj: Any,
    *,
    dict_as_obj: bool = False,
    itemid: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice = schema_constants.OBJECT,
    from_dim: int = 0,
) -> data_slice.DataSlice:
  """Converts Python object into DataSlice.

  Can convert nested lists/dicts into Koda objects recursively as well.

  Args:
    py_obj: Python object to convert.
    dict_as_obj: If True, will convert dicts with string keys into Koda objects
      instead of Koda dicts.
    itemid: The ItemId to use for the root object. If not specified, will
      allocate a new id. If specified, will also infer the ItemIds for all child
      items such as list items from this id, so that repeated calls to this
      method on the same input will produce the same id for everything. Use this
      with care to avoid unexpected collisions.
    schema: The schema to use for the return value. When this schema or one of
      its attributes is OBJECT (which is also the default), recursively creates
      objects from that point on.
    from_dim: The dimension to start creating Koda objects/lists/dicts from.
      `py_obj` must be a nested list of at least from_dim depth, and the outer
      from_dim dimensions will become the returned DataSlice dimensions. When
      from_dim is 0, the return value is therefore a DataItem.

  Returns:
    A DataItem with the converted data.
  """
  if itemid is not None:
    raise NotImplementedError('passing itemid is not yet supported')
  if from_dim != 0:
    raise NotImplementedError('passing from_dim is not yet supported')
  return data_bag.DataBag.empty()._from_py_impl(  # pylint: disable=protected-access
      py_obj, dict_as_obj, itemid, schema, from_dim
  )


def to_str(x: data_slice.DataSlice) -> data_slice.DataSlice:
  """Converts given DataSlice to string using str(x)."""
  return ds(str(x))


def to_repr(x: data_slice.DataSlice) -> data_slice.DataSlice:
  """Converts given DataSlice to string using repr(x)."""
  return ds(repr(x))


def to_pylist(x: data_slice.DataSlice) -> list[Any]:
  """Expands the outermost DataSlice dimension into a list of DataSlices."""
  return list(x.L)
