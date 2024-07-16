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


def from_py(
    py_obj: Any,
    *,
    dict_as_obj: bool = False,
    itemid: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice = schema_constants.OBJECT,
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

  Returns:
    A DataItem with the converted data.
  """
  if dict_as_obj:
    raise NotImplementedError('dict_as_obj is not yet supported')
  if itemid is not None:
    raise NotImplementedError('passing itemid is not yet supported')
  if schema != schema_constants.OBJECT:
    raise NotImplementedError('non-OBJECT schema is not yet supported')
  return data_bag.DataBag.empty().obj(py_obj)
