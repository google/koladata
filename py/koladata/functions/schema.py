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

"""Koda functions for creating schemas."""

from collections.abc import MutableMapping
import dataclasses
import enum
import types as py_types
import typing
from typing import Any, Optional, Union
from koladata.functions import attrs
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants
from koladata.types import schema_item


def schema_from_py(tpe: type[Any]) -> schema_item.SchemaItem:
  """Creates a Koda entity schema corresponding to the given Python type.

  This method supports the following Python types / type annotations
  recursively:
  - Primitive types: int, float, bool, str, bytes.
  - Collections: list[...], dict[...], Sequence[...], Mapping[...], ect.
  - Unions: only "smth | None" or "Optional[smth]" is supported.
  - Dataclasses.

  This can be used in conjunction with kd.from_py to convert lists of Python
  objects to efficient Koda DataSlices. Because of the 'efficient' goal, we
  create an entity schema and do not use kd.OBJECT inside, which also results
  in strict type checking. If you do not care
  about efficiency or type safety, you can use kd.from_py(..., schema=kd.OBJECT)
  directly.

  Args:
    tpe: The Python type to create a schema for.

  Returns:
    A Koda entity schema corresponding to the given Python type. The returned
    schema is a uu-schema, in other words we always return the same output for
    the same input. For dataclasses, we use the module name and the class name
    to derive the itemid for the uu-schema.
  """
  # Cache to handle recursive types. _Not_ guaranteed to contain the result of
  # `tpe` after `schema_from_py_impl(tpe)` has been called.
  schema_from_tpe: dict[type[Any], data_slice.DataSlice] = {}
  db = data_bag.DataBag.empty_mutable()

  def schema_from_py_impl(tpe: type[Any]) -> data_slice.DataSlice:
    if (res := schema_from_tpe.get(tpe)) is not None:
      return res
    if origin_tpe := typing.get_origin(tpe):
      if isinstance(origin_tpe, type) and issubclass(
          origin_tpe, typing.Sequence
      ):
        (item_tpe,) = typing.get_args(tpe)
        return db.list_schema(schema_from_py_impl(item_tpe))
      if isinstance(origin_tpe, type) and issubclass(
          origin_tpe, typing.Mapping
      ):
        key_tpe, value_tpe = typing.get_args(tpe)
        return db.dict_schema(
            schema_from_py_impl(key_tpe), schema_from_py_impl(value_tpe)
        )
      if origin_tpe is typing.Annotated:
        return schema_from_py_impl(typing.get_args(tpe)[0])
      if origin_tpe == py_types.UnionType or origin_tpe == typing.Union:
        options = typing.get_args(tpe)
        if len(options) != 2 or (options[1] != py_types.NoneType):
          raise TypeError(
              f'unsupported union type: {tpe}. kd.schema_from_py only'
              ' supports "smth | None" or "Optional[smth]".'
          )
        return schema_from_py_impl(options[0])
      raise TypeError(
          f'unsupported generic field type in kd.schema_from_py: {origin_tpe}.'
      )
    if not isinstance(tpe, type):
      raise TypeError(f'kd.schema_from_py expects a Python type, got {tpe}.')
    if dataclasses.is_dataclass(tpe):
      s = db.named_schema(
          f'__schema_from_py__{tpe.__module__}.{tpe.__qualname__}'
      )
      schema_from_tpe[tpe] = s
      # Required to support `from __future__ import annotations`.
      field_types = typing.get_type_hints(tpe)
      s.set_attrs(**{
          field.name: schema_from_py_impl(field_types[field.name])
          for field in dataclasses.fields(tpe)
      })
      return s
    if tpe == str or issubclass(tpe, enum.StrEnum):
      return schema_constants.STRING
    if tpe == bytes:
      return schema_constants.BYTES
    if tpe == int or issubclass(tpe, enum.IntEnum):
      # kd.from_py can return either INT32 or INT64 for integers, so we return
      # INT64 to be on the safe side.
      return schema_constants.INT64
    if tpe == float:
      # kd.from_py always returns FLOAT32 for floats, so we do the same for
      # consistency.
      return schema_constants.FLOAT32
    if tpe == Any:
      return schema_constants.OBJECT
    if tpe == bool:
      return schema_constants.BOOLEAN
    raise TypeError(f'unsupported type in kd.schema_from_py: {tpe}.')

  return typing.cast(
      schema_item.SchemaItem,
      schema_from_py_impl(tpe).freeze_bag(),
  )

_koda_to_py_type_map = {
    schema_constants.INT64: int,
    schema_constants.INT32: int,
    schema_constants.FLOAT32: float,
    schema_constants.FLOAT64: float,
    schema_constants.BOOLEAN: bool,
    schema_constants.STRING: str,
    schema_constants.BYTES: bytes,
}
_PRIMITIVETYPE = type[Union[bool, bytes, float, int, str]]


def _primitive_schema_to_py(
    schema: schema_item.SchemaItem,
) -> _PRIMITIVETYPE | None:
  """Returns the Python type corresponding to the given Koda primitive schema."""
  if schema == schema_constants.OBJECT:
    return Any
  if schema not in _koda_to_py_type_map:
    raise TypeError(f'unsupported primitive schema: {schema}.')
  return Optional[_koda_to_py_type_map[schema]]


def _get_dataclass_name(schema: schema_item.SchemaItem) -> str:
  """Returns the Python dataclass name corresponding to the given Koda schema.

  Args:
    schema: The Koda schema to get the dataclass name for.

  Returns:
    If the `__schema_name__` attribute is present, it returns the last part of
    the name. Otherwise (e.g., if the schema was created via `kd.new_schema`
    without an explicit name), it returns 'Entity'.
  """
  # TODO(b/510247584) Once the public API for schema_name is implemented, use
  # that instead.
  dataclass_name = schema.get_attr('__schema_name__', 'Entity').to_py()
  return dataclass_name.split('.')[-1]


def _internal_schema_to_py(
    schema: schema_item.SchemaItem,
    visited: MutableMapping[Any, type[Any] | None],
) -> type[Any] | None:
  """Implementation of `schema_to_py`; handles recursive types via the `visited` dict."""

  fp = schema.fingerprint
  if fp in visited:
    return visited[fp]
  if schema.is_primitive():
    return _primitive_schema_to_py(schema)
  if schema.is_list_schema():
    res = list[_internal_schema_to_py(schema.get_item_schema(), visited)] | None
  elif schema.is_dict_schema():
    res = (
        dict[
            _internal_schema_to_py(schema.get_key_schema(), visited),
            _internal_schema_to_py(schema.get_value_schema(), visited),
        ]
        | None
    )
  elif schema.is_entity_schema():
    dc_type = dataclasses.make_dataclass(
        _get_dataclass_name(schema),
        [
            (name, Any, dataclasses.field(default=None))
            for name in attrs.dir(schema)
        ],
    )
    res = dc_type | None
    visited[fp] = res
    fields = dataclasses.fields(dc_type)

    for field in fields:
      name = field.name
      correct_type = _internal_schema_to_py(schema.get_attr(name), visited)
      dc_type.__annotations__[name] = (
          correct_type  # This is needed for `typing.get_type_hints`.
      )
      field.type = correct_type

  else:
    raise TypeError(f'unsupported schema: {schema!r}.')

  if fp in visited:
    res = visited[fp]
  else:
    visited[fp] = res
  return res


def schema_to_py(schema: schema_item.SchemaItem) -> type[Any] | None:
  """Creates a Python type corresponding to the given Koda schema.

  Primitive Koda schemas are converted to the corresponding Python types.
  List schemas are converted to lists, dict schemas to dicts.
  Entity schemas are converted to dynamically created dataclasses.
  kd.OBJECT is not supported at any level, since there is no corresponding
  Python type.

  Please note that all Koda types become optional in Python.

  Args:
    schema: The Koda schema to convert to a Python type.

  Returns:
    The Python type corresponding to the given Koda schema.

  Raises:
    TypeError: If the given schema is not supported.
  """
  return _internal_schema_to_py(schema, visited={})
