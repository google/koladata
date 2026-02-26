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

"""Module for creating dataclasses from the list of attribute names."""

import dataclasses
import types
import typing
from typing import Any, Union

_Type = type[Any] | types.UnionType | types.GenericAlias | typing.Optional[Any]


def _eq(x, y):
  """Checks whether two dataclasses are equal ignoring types."""
  return dataclasses.is_dataclass(y) and dataclasses.asdict(
      x
  ) == dataclasses.asdict(y)


def make_dataclass(attr_names):
  obj_class = dataclasses.make_dataclass(
      'Obj',
      [
          (attr_name, Any, dataclasses.field(default=None))
          for attr_name in attr_names
      ],
      eq=False,
  )
  obj_class.__eq__ = _eq
  return obj_class


def fields_names_and_values(py_obj):
  """Returns list of (attribute name, value) for a dataclass/SimpleNamespace."""
  if dataclasses.is_dataclass(py_obj):
    return [
        (field.name, getattr(py_obj, field.name))
        for field in dataclasses.fields(py_obj)
    ]
  if isinstance(py_obj, types.SimpleNamespace):
    return list(py_obj.__dict__.items())
  return None


def _get_underlying_optional_type(
    t: _Type,
) -> type[Any] | None:
  """If t is Optional[T] or T | None, returns the underlying type T.

  Args:
    t: The type to inspect.

  Returns:
    The underlying type T if t is Optional[T] or T | None, otherwise None.
  """

  if typing.get_origin(t) is Union or isinstance(t, types.UnionType):
    args = typing.get_args(t)
    if len(args) != 2 or args[1] is not types.NoneType:
      raise ValueError(
          f'only unions `SomeType | None` are supported ; got instead: {t}'
      )
    return args[0]
  return None


def _get_field_type_annotation(
    py_obj: _Type,
    attr_name: str,
    type_hints_cache: dict[_Type, dict[str, _Type]],
) -> _Type | None:
  """Returns the type annotation of the field `attr_name` of py_obj."""
  if (res := type_hints_cache.get(py_obj)) is None:
    res = typing.get_type_hints(py_obj)
    type_hints_cache[py_obj] = res
  return res.get(attr_name)


def get_class_field_type(
    py_obj: _Type,
    attr_name: str,
    for_primitive: bool,
    type_hints_cache: dict[_Type, dict[str, _Type]],
) -> _Type | None:
  """Returns the type of the given field of py_obj.

  If not found, returns None.

  Args:
    py_obj: The class to inspect.
    attr_name: The name of the attribute to inspect.
    for_primitive: Whether the caller's intention is to use it for a primitive
      type.
    type_hints_cache: A cache of type hints for dataclasses.

  Returns:
    The type of the given field of py_obj.
  """

  if py_obj is types.SimpleNamespace:
    if for_primitive:
      return None
    else:
      return types.SimpleNamespace

  if dataclasses.is_dataclass(py_obj):

    t = _get_field_type_annotation(py_obj, attr_name, type_hints_cache)
    if t is None:
      return None
    if dataclasses.is_dataclass(t):
      return t
    # x: Optional[Obj] or x: Obj | None
    underlying_optional_type = _get_underlying_optional_type(t)
    if underlying_optional_type is not None:
      return underlying_optional_type
    # x: list[Obj] or x: dict[Obj, int]
    if isinstance(t, types.GenericAlias):
      return t
    if t is types.SimpleNamespace:
      return t
    if for_primitive:
      return t
    raise ValueError(f"field '{attr_name}' has unsupported type: {t}")

  if origin_type := typing.get_origin(py_obj):
    # `list[Obj]`
    if (
        attr_name == '__items__'
        and isinstance(origin_type, type)
        and issubclass(origin_type, typing.Sequence)
    ):
      return typing.get_args(py_obj)[0]

    if isinstance(origin_type, type) and issubclass(
        origin_type, typing.Mapping
    ):
      # `dict[Obj, int]`
      if attr_name == '__keys__':
        return typing.get_args(py_obj)[0]
      # `dict[int, Obj]`
      if attr_name == '__values__':
        args = typing.get_args(py_obj)
        if len(args) != 2:
          raise ValueError(
              f'Expected dict; got instead: {typing.get_origin(py_obj)}'
          )
        return args[1]
    raise ValueError(
        f'unsupported GenericAlias {py_obj} for attribute {attr_name}'
    )

  raise ValueError(
      'only dataclasses or SimpleNamespace are supported; got instead:'
      f' {py_obj}'
  )


def has_optional_field(
    py_class: _Type,
    attr_name: str,
    type_hints_cache: dict[_Type, dict[str, _Type]],
) -> bool:
  """Returns whether the given attribute exists and is optional."""
  if not dataclasses.is_dataclass(py_class):
    return False
  field = _get_field_type_annotation(py_class, attr_name, type_hints_cache)
  return _get_underlying_optional_type(field) is not None


_simple_namespace_class = types.SimpleNamespace
