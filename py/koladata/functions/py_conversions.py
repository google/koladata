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

import dataclasses
from typing import Any

from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import dict_item  # pylint: disable=unused-import
from koladata.types import list_item  # pylint: disable=unused-import
from koladata.types import schema_constants


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
  return data_slice.DataSlice.from_vals(str(x))


def to_repr(x: data_slice.DataSlice) -> data_slice.DataSlice:
  """Converts given DataSlice to string using repr(x)."""
  return data_slice.DataSlice.from_vals(repr(x))


def to_pylist(x: data_slice.DataSlice) -> list[Any]:
  """Expands the outermost DataSlice dimension into a list of DataSlices."""
  return list(x.L)


@data_slice.DataSlice.add_method('to_py')
def to_py(
    ds: data_slice.DataSlice,
    max_depth: int = 2,
    obj_as_dict: bool = False,
    include_missing_attrs: bool = True,
) -> Any:
  """Returns a readable python object from a DataSlice.

  Attributes, lists, and dicts are recursively converted to Python objects.

  Args:
    ds: A DataSlice
    max_depth: Maximum depth for recursive printing. Each attribute, list, and
      dict increments the depth by 1. Use -1 for unlimited depth.
    obj_as_dict: Whether to convert objects to python dicts. By default objects
      are converted to automatically constructed 'Obj' dataclass instances.
    include_missing_attrs: whether to include attributes with None value in
      objects.
  """
  if max_depth >= 0:
    max_depth += ds.get_ndim()

  if ds.db is not None:
    ds = ds.fork_db()
    db = ds.db
  else:
    db = data_bag.DataBag.empty()
  ds = db.implode(ds, -1)
  return _to_py_impl(ds, {}, 0, max_depth, obj_as_dict, include_missing_attrs)


@data_slice.DataSlice.add_method('to_pytree')
def to_pytree(
    ds: data_slice.DataSlice,
    max_depth: int = 2,
    include_missing_attrs: bool = True) -> Any:
  """Returns a readable python object from a DataSlice.

  Attributes, lists, and dicts are recursively converted to Python objects.
  Objects are converted to Python dicts.

  Same as kd.to_py(..., obj_as_dict=True)

  Args:
    ds: A DataSlice
    max_depth: Maximum depth for recursive printing. Each attribute, list, and
      dict increments the depth by 1. Use -1 for unlimited depth.
    include_missing_attrs: whether to include attributes with None value in
      objects.
  """
  return to_py(
      ds, max_depth=max_depth, obj_as_dict=True,
      include_missing_attrs=include_missing_attrs)


def _to_py_impl(
    ds: data_item.DataItem,
    obj_id_to_python_obj: dict[data_item.DataItem, Any],
    depth: int,
    max_depth: int,
    obj_as_dict: bool,
    include_missing_attrs: bool,
) -> Any:
  """Recursively converts a DataItem to a Python object."""
  assert isinstance(ds, data_item.DataItem)

  existing = obj_id_to_python_obj.get(ds)
  if existing is not None:
    return existing

  if ds.is_primitive():
    return ds.internal_as_py()

  if ds.db is None:
    return {}

  schema = ds.get_schema()

  if schema == schema_constants.ANY:
    raise ValueError(
        f'cannot convert a DataSlice with schema ANY to Python: {ds}'
    )

  if (
      max_depth >= 0 and depth >= max_depth
  ) or schema == schema_constants.ITEMID:
    return ds

  is_list = schema.is_list_schema()
  is_dict = schema.is_dict_schema()

  # Remove special attributes
  attr_names = list(set(dir(ds)) - set(['__items__', '__keys__', '__values__']))
  assert not (attr_names and (is_list or is_dict))

  if attr_names and not obj_as_dict:
    obj_class = dataclasses.make_dataclass(
        'Obj',
        [
            (attr_name, Any, dataclasses.field(default=None))
            for attr_name in attr_names
        ],
        eq=False,
    )

    def eq(x, y):
      """Checks whether two dataclasses are equal ignoring types."""
      return dataclasses.is_dataclass(y) and dataclasses.asdict(
          x
      ) == dataclasses.asdict(y)

    obj_class.__eq__ = eq

    py_obj = obj_class()
  elif is_list:
    py_obj = []
  else:
    py_obj = {}

  obj_id_to_python_obj[ds] = py_obj

  attrs = {}
  next_depth = depth + 1
  for attr_name in attr_names:
    attr_ds = ds.get_attr(attr_name)
    attr_value = _to_py_impl(
        attr_ds, obj_id_to_python_obj, next_depth, max_depth, obj_as_dict,
        include_missing_attrs)
    if include_missing_attrs or attr_value is not None:
      attrs[attr_name] = attr_value

  if dataclasses.is_dataclass(py_obj):
    for name, value in attrs.items():
      setattr(py_obj, name, value)
  elif attrs and obj_as_dict and not is_dict and not is_list:
    py_obj.update(attrs)  # pytype: disable=attribute-error

  if is_list:
    list_values = py_obj
    assert isinstance(list_values, list)
    for child_ds in ds:
      list_values.append(
          _to_py_impl(
              child_ds, obj_id_to_python_obj, next_depth, max_depth,
              obj_as_dict, include_missing_attrs)
      )

  if is_dict:
    dict_values = py_obj
    for key in ds:
      value_ds = ds[key]
      dict_values[key.no_db().internal_as_py()] = _to_py_impl(
          value_ds, obj_id_to_python_obj, next_depth, max_depth, obj_as_dict,
          include_missing_attrs)

  return py_obj
