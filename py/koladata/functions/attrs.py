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

"""Koda functions for modifying Object / Entity attributes."""

from typing import Any

from koladata.types import data_item as _  # pylint: disable=unused-import
from koladata.types import data_slice


def embed_schema(ds: data_slice.DataSlice) -> data_slice.DataSlice:
  """Returns a DataSlice with OBJECT schema.

  * For primitives no data change is done.
  * For Entities schema is stored as '__schema__' attribute.
  * Embedding Entities requires a DataSlice to be associated with a DataBag.

  Args:
    ds: (DataSlice) whose schema is embedded.
  """
  return ds.embed_schema()


def set_attr(
    ds: data_slice.DataSlice,
    attr_name: str,
    value: Any,
    update_schema: bool = False,
):
  """Sets an attribute `attr_name` to `value`.

  If `update_schema` is True and `ds` is either an Entity with explicit schema
  or an Object where some items are entities with explicit schema, it will get
  updated with `value`'s schema first.

  Args:
    ds: a DataSlice on which to set the attribute. Must have DataBag attached.
    attr_name: attribute name
    value: a DataSlice or convertible to a DataSlice that will be assigned as an
      attribute.
    update_schema: whether to update the schema before setting an attribute.
  """
  ds.set_attr(attr_name, value, update_schema=update_schema)


def set_attrs(
    ds: data_slice.DataSlice,
    *,
    update_schema: bool = False,
    **attrs: Any
):
  """Sets multiple attributes on an object / entity.

  Args:
    ds: a DataSlice on which attributes are set. Must have DataBag attached.
    update_schema: (bool) overwrite schema if attribute schema is missing or
      incompatible.
    **attrs: attribute values that are converted to DataSlices with DataBag
      adoption.
  """
  ds.set_attrs(**attrs, update_schema=update_schema)
