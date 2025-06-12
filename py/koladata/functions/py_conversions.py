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

"""Koda functions for converting to and from Python structures."""

from typing import Any

from arolla import arolla
from koladata.base.py_conversions import clib as base_clib
from koladata.types import data_slice
from koladata.types import dict_item  # pylint: disable=unused-import
from koladata.types import list_item  # pylint: disable=unused-import
from koladata.types import py_boxing
from koladata.types import schema_constants


def from_py(
    py_obj: Any,
    *,
    dict_as_obj: bool = False,
    itemid: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice | None = None,
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
  return data_slice.DataSlice._from_py_impl(  # pylint: disable=protected-access
      py_obj, dict_as_obj, itemid, schema, from_dim
  )


def _from_py_v2(
    py_obj: Any,
    *,
    dict_as_obj: bool = False,
    itemid: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice | None = None,
    from_dim: int = 0,
) -> data_slice.DataSlice:
  """New version of from_py; it will eventually replace the old one."""
  del dict_as_obj, itemid
  return base_clib._from_py_v2(py_obj, schema, from_dim)  # pylint: disable=protected-access


def to_pylist(x: data_slice.DataSlice) -> list[Any]:
  """Expands the outermost DataSlice dimension into a list of DataSlices."""
  return list(x.L)


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
    max_depth: Maximum depth for recursive conversion. Each attribute, list item
      and dict keys / values access represent 1 depth increment. Use -1 for
      unlimited depth.
    obj_as_dict: Whether to convert objects to python dicts. By default objects
      are converted to automatically constructed 'Obj' dataclass instances.
    include_missing_attrs: whether to include attributes with None value in
      objects.
  """
  return ds._to_py_impl(  # pylint: disable=protected-access
      max_depth, obj_as_dict, include_missing_attrs
  )


def to_pytree(
    ds: data_slice.DataSlice,
    max_depth: int = 2,
    include_missing_attrs: bool = True,
) -> Any:
  return ds.to_pytree(
      max_depth=max_depth, include_missing_attrs=include_missing_attrs
  )


def py_reference(obj: Any) -> arolla.types.PyObject:
  """Wraps into a Arolla QValue using reference for serialization.

  py_reference can be used to pass arbitrary python objects through
  kd.apply_py/kd.py_fn.

  Note that using reference for serialization means that the resulting
  QValue (and Exprs created using it) will only be valid within the
  same process. Trying to deserialize it in a different process
  will result in an exception.

  Args:
    obj: the python object to wrap.
  Returns:
    The wrapped python object as Arolla QValue.
  """
  return arolla.types.PyObject(obj, codec=py_boxing.REF_CODEC)


def int32(x: Any) -> data_slice.DataSlice:
  """Returns kd.slice(x, kd.INT32)."""
  return data_slice.DataSlice.from_vals(x, schema_constants.INT32)


def int64(x: Any) -> data_slice.DataSlice:
  """Returns kd.slice(x, kd.INT64)."""
  return data_slice.DataSlice.from_vals(x, schema_constants.INT64)


def float32(x: Any) -> data_slice.DataSlice:
  """Returns kd.slice(x, kd.FLOAT32)."""
  return data_slice.DataSlice.from_vals(x, schema_constants.FLOAT32)


def float64(x: Any) -> data_slice.DataSlice:
  """Returns kd.slice(x, kd.FLOAT64)."""
  return data_slice.DataSlice.from_vals(x, schema_constants.FLOAT64)


def str_(x: Any) -> data_slice.DataSlice:
  """Returns kd.slice(x, kd.STRING)."""
  return data_slice.DataSlice.from_vals(x, schema_constants.STRING)


def bytes_(x: Any) -> data_slice.DataSlice:
  """Returns kd.slice(x, kd.BYTES)."""
  return data_slice.DataSlice.from_vals(x, schema_constants.BYTES)


def bool_(x: Any) -> data_slice.DataSlice:
  """Returns kd.slice(x, kd.BOOLEAN)."""
  return data_slice.DataSlice.from_vals(x, schema_constants.BOOLEAN)


def mask(x: Any) -> data_slice.DataSlice:
  """Returns kd.slice(x, kd.MASK)."""
  return data_slice.DataSlice.from_vals(x, schema_constants.MASK)


def expr_quote(x: Any) -> data_slice.DataSlice:
  """Returns kd.slice(x, kd.EXPR)."""
  return data_slice.DataSlice.from_vals(x, schema_constants.EXPR)
