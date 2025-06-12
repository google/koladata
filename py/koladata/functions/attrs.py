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

"""Koda functions for modifying Object / Entity attributes."""

from typing import Any

from koladata.types import data_item as _  # pylint: disable=unused-import
from koladata.types import data_slice
from koladata.types import schema_constants


def embed_schema(x: data_slice.DataSlice) -> data_slice.DataSlice:
  """Returns a DataSlice with OBJECT schema.

  * For primitives no data change is done.
  * For Entities schema is stored as '__schema__' attribute.
  * Embedding Entities requires a DataSlice to be associated with a DataBag.

  Args:
    x: (DataSlice) whose schema is embedded.
  """
  return x.embed_schema()


def update_schema_fn(
    obj: data_slice.DataSlice, **attr_schemas: Any
) -> data_slice.DataSlice:
  """Updates the schema of `obj` DataSlice using given schemas for attrs."""
  schema = obj.get_schema()
  if schema == schema_constants.OBJECT:
    schema = obj.get_obj_schema()
  schema.set_attrs(**attr_schemas)
  return obj


def set_schema(
    x: data_slice.DataSlice, schema: data_slice.DataSlice
) -> data_slice.DataSlice:
  """Returns a copy of `x` with the provided `schema`.

  If `schema` is an Entity schema and has a different DataBag than `x`, it is
  merged into the DataBag of `x`.

  It only changes the schemas of `x` and does not change the items in `x`. To
  change the items in `x`, use `kd.cast_to` instead. For example,

    kd.set_schema(kd.ds([1, 2, 3]), kd.FLOAT32) -> fails because the items in
        `x` are not compatible with FLOAT32.
    kd.cast_to(kd.ds([1, 2, 3]), kd.FLOAT32) -> kd.ds([1.0, 2.0, 3.0])

  When items in `x` are primitives or `schemas` is a primitive schema, it checks
  items and schema are compatible. When items are ItemIds and `schema` is a
  non-primitive schema, it does not check the underlying data matches the
  schema. For example,

    kd.set_schema(kd.ds([1, 2, 3], schema=kd.OBJECT), kd.INT32)
      -> kd.ds([1, 2, 3])
    kd.set_schema(kd.ds([1, 2, 3]), kd.INT64) -> fail
    kd.set_schema(kd.ds(1).with_bag(kd.bag()), kd.schema.new_schema(x=kd.INT32))
    ->
    fail
    kd.set_schema(kd.new(x=1), kd.INT32) -> fail
    kd.set_schema(kd.new(x=1), kd.schema.new_schema(x=kd.INT64)) -> work

  Args:
    x: DataSlice to change the schema of.
    schema: DataSlice containing the new schema.

  Returns:
    DataSlice with the new schema.
  """
  return x.set_schema(schema)


def set_attr(
    x: data_slice.DataSlice,
    attr_name: str,
    value: Any,
    overwrite_schema: bool = False,
):
  """Sets an attribute `attr_name` to `value`.

  If `overwrite_schema` is True and `x` is either an Entity with explicit schema
  or an Object where some items are entities with explicit schema, it will get
  updated with `value`'s schema first.

  Args:
    x: a DataSlice on which to set the attribute. Must have DataBag attached.
    attr_name: attribute name
    value: a DataSlice or convertible to a DataSlice that will be assigned as an
      attribute.
    overwrite_schema: whether to overwrite the schema before setting an
      attribute.
  """
  x.set_attr(attr_name, value, overwrite_schema=overwrite_schema)


def set_attrs(
    x: data_slice.DataSlice,
    *,
    overwrite_schema: bool = False,
    **attrs: Any
):
  """Sets multiple attributes on an object / entity.

  Args:
    x: a DataSlice on which attributes are set. Must have DataBag attached.
    overwrite_schema: whether to overwrite the schema before setting an
      attribute.
    **attrs: attribute values that are converted to DataSlices with DataBag
      adoption.
  """
  x.set_attrs(**attrs, overwrite_schema=overwrite_schema)


def del_attr(
    x: data_slice.DataSlice,
    attr_name: str,
):
  """Deletes an attribute `attr_name` from `x`."""
  x.__delattr__(attr_name)


def get_attr_names(x: data_slice.DataSlice, *, intersection: bool) -> list[str]:
  """Returns a sorted list of unique attribute names of the given DataSlice.

  In case of OBJECT schema, attribute names are fetched from the `__schema__`
  attribute. In case of Entity schema, the attribute names are fetched from the
  schema. In case of primitives, an empty list is returned.

  Args:
    x: A DataSlice.
    intersection: If True, the intersection of all object attributes is
      returned. Otherwise, the union is returned.

  Returns:
    A list of unique attributes sorted by alphabetical order.
  """
  return x.get_attr_names(intersection=intersection)


def dir_(x: data_slice.DataSlice) -> list[str]:
  """Returns a sorted list of unique attribute names of the given DataSlice.

  This is equivalent to `kd.get_attr_names(ds, intersection=True)`. For more
  finegrained control, use `kd.get_attr_names` directly instead.

  In case of OBJECT schema, attribute names are fetched from the `__schema__`
  attribute. In case of Entity schema, the attribute names are fetched from the
  schema. In case of primitives, an empty list is returned.

  Args:
    x: A DataSlice.

  Returns:
    A list of unique attributes sorted by alphabetical order.
  """
  return x.get_attr_names(intersection=True)


dir = dir_  # pylint: disable=redefined-builtin
