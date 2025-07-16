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

"""The definition of data slice paths and utilities for working with them.

A data slice path is a sequence of actions to perform on a DataSlice to obtain
a subslice. The set of allowed actions is small and simple:
* Getting the keys of a dict.
* Getting the values of a dict.
* Exploding a list.
* Getting an attribute of an entity.

The utility of a data slice path is that it specifies how to obtain a subslice
without doing it immediately. Thus, we have the possibility to gather more
information about the context of the subslice, for example, the schemas of its
ancestor data slices.

Not every data slice path is valid for every DataSlice. For example, we cannot
execute an action of exploding a list if we have a DICT DataSlice.

A Koda schema induces a set of data slice paths, namely those that are valid for
any DataSlice with that schema.
"""

from __future__ import annotations

import dataclasses
from typing import Generator

from koladata import kd


class ActionParsingError(Exception):
  """Raised when a data slice path cannot be parsed into an action + remainder."""


class IncompatibleSchemaError(Exception):
  """Raised when a data slice action is not compatible with a schema."""


class DataSliceAction:
  """An action to perform on a data slice. All instances are immutable."""

  def evaluate(self, data_slice: kd.types.DataSlice) -> kd.types.DataSlice:
    """Evaluates the action on the given data slice."""
    raise NotImplementedError(type(self))

  @classmethod
  def parse_from_data_slice_path_prefix(
      cls, data_slice_path: str
  ) -> tuple[DataSliceAction, str] | None:
    """Parses the given path to return an action and the remaining path.

    Args:
      data_slice_path: the data slice path from which to parse a prefix.

    Returns:
      A tuple of the parsed action and the remaining part of the data slice
      path. If the action does not apply, then None is returned. If the action
      is applicable but cannot be parsed, then an ActionParsingError is raised.

    Raises:
      ActionParsingError: if the action is applicable but it cannot be parsed.
    """
    raise NotImplementedError(cls)

  def __str__(self) -> str:
    """Returns the canonical string representation of the action."""
    raise NotImplementedError(type(self))

  def debug_string(self) -> str:
    return f'{self.__class__.__name__}()'

  def get_subschema(self, schema: kd.types.DataItem) -> kd.types.DataItem:
    """Returns the schema after applying the action on a DataSlice with `schema`.

    Args:
      schema: the schema of a hypothetical DataSlice.

    Returns:
      The schema after applying the action on an arbitrary DataSlice with schema
      `schema`.

    Raises:
      IncompatibleSchemaError: if the action is not compatible with the given
        schema.
    """
    raise NotImplementedError(type(self))

  def get_subschema_operation(self) -> str:
    """Returns the operation to obtain the subschema in get_subschema.

    This is decoupled from str(self), because conceptually the operation on the
    schema has a coarser granularity. For example, "[:4]" is a conceptually
    valid action on a data slice, but its corresponding schema operation is
    the same as that of "[:]", namely ".get_item_schema()".
    """
    raise NotImplementedError(type(self))


@dataclasses.dataclass(frozen=True)
class DictGetKeys(DataSliceAction):
  """Action to get the keys of a Koda DICT data slice."""

  def evaluate(self, data_slice: kd.types.DataSlice) -> kd.types.DataSlice:
    return data_slice.get_keys()

  @classmethod
  def parse_from_data_slice_path_prefix(
      cls, data_slice_path: str
  ) -> tuple[DataSliceAction, str] | None:
    if data_slice_path.startswith('.get_keys()'):
      return cls(), data_slice_path.removeprefix('.get_keys()')
    return None

  def __str__(self) -> str:
    return '.get_keys()'

  def get_subschema(self, schema: kd.types.DataItem) -> kd.types.DataItem:
    if not schema.is_dict_schema():
      raise IncompatibleSchemaError('getting keys requires a DICT schema')
    return schema.get_key_schema()

  def get_subschema_operation(self) -> str:
    return '.get_key_schema()'


@dataclasses.dataclass(frozen=True)
class DictGetValues(DataSliceAction):
  """Action to get the values of a Koda DICT data slice."""

  def evaluate(self, data_slice: kd.types.DataSlice) -> kd.types.DataSlice:
    return data_slice.get_values()

  @classmethod
  def parse_from_data_slice_path_prefix(
      cls, data_slice_path: str
  ) -> tuple[DataSliceAction, str] | None:
    if data_slice_path.startswith('.get_values()'):
      return cls(), data_slice_path.removeprefix('.get_values()')
    return None

  def __str__(self) -> str:
    return '.get_values()'

  def get_subschema(self, schema: kd.types.DataItem) -> kd.types.DataItem:
    if not schema.is_dict_schema():
      raise IncompatibleSchemaError('getting values requires a DICT schema')
    return schema.get_value_schema()

  def get_subschema_operation(self) -> str:
    return '.get_value_schema()'


@dataclasses.dataclass(frozen=True)
class ListExplode(DataSliceAction):
  """Action to explode a Koda LIST data slice."""

  def evaluate(self, data_slice: kd.types.DataSlice) -> kd.types.DataSlice:
    return data_slice.explode()

  @classmethod
  def parse_from_data_slice_path_prefix(
      cls, data_slice_path: str
  ) -> tuple[DataSliceAction, str] | None:
    if data_slice_path.startswith('[:]'):
      return cls(), data_slice_path.removeprefix('[:]')
    return None

  def __str__(self) -> str:
    return '[:]'

  def get_subschema(self, schema: kd.types.DataItem) -> kd.types.DataItem:
    if not schema.is_list_schema():
      raise IncompatibleSchemaError('exploding requires a LIST schema')
    return schema.get_item_schema()

  def get_subschema_operation(self) -> str:
    return '.get_item_schema()'


def can_be_used_with_dot_syntax_in_data_slice_path_string(
    attr_name: str,
) -> bool:
  """Returns True iff attr_name is a valid Python identifier."""
  return attr_name.isidentifier()


def base64_encoded_attr_name(attr_name: str) -> str:
  """Returns the base64-encoded version of the given attr_name."""
  return kd.strings.encode_base64(kd.strings.encode(attr_name)).to_py()


def decode_base64_encoded_attr_name(b64_encoded_attr_name: str) -> str:
  """Returns the decoded version of the given base64-encoded attr_name."""
  return kd.strings.decode(
      kd.strings.decode_base64(b64_encoded_attr_name)
  ).to_py()


@dataclasses.dataclass(frozen=True)
class GetAttr(DataSliceAction):
  """Action to get an attribute of a Koda entity data slice."""

  attr_name: str

  def evaluate(self, data_slice: kd.types.DataSlice) -> kd.types.DataSlice:
    return data_slice.get_attr(self.attr_name)

  @classmethod
  def parse_from_data_slice_path_prefix(
      cls, data_slice_path: str
  ) -> tuple[DataSliceAction, str] | None:
    if data_slice_path.startswith('.get_attr('):
      remainder = data_slice_path.removeprefix('.get_attr(')
      if not remainder:
        raise ActionParsingError('it does not mention an attribute name')
      quote_char = remainder[0]
      if quote_char not in '"\'':
        raise ActionParsingError(
            'the attribute name should be quoted, but it is not'
        )
      remainder = remainder.removeprefix(quote_char)
      b64_encoded_attr_name = _get_prefix(of=remainder, before=quote_char)
      try:
        attr_name = decode_base64_encoded_attr_name(b64_encoded_attr_name)
      except ValueError as e:
        raise ActionParsingError(
            'the attribute name should be a base64-encoded string, but it is'
            f' not. Got: {b64_encoded_attr_name}',
        ) from e
      remainder = remainder.removeprefix(b64_encoded_attr_name)
      close_quote_and_parenthesis = f'{quote_char})'
      if not remainder.startswith(close_quote_and_parenthesis):
        raise ActionParsingError(
            'the attribute name should be followed by'
            f' {close_quote_and_parenthesis}, but it is not',
        )
      remainder = remainder.removeprefix(close_quote_and_parenthesis)
      return cls(attr_name), remainder

    if not data_slice_path.startswith('.'):
      return None
    without_leading_dot = data_slice_path[1:]
    up_to_explode = _get_prefix(of=without_leading_dot, before='[:]')
    up_to_dot = _get_prefix(of=without_leading_dot, before='.')
    if len(up_to_explode) < len(up_to_dot):
      attr_name = up_to_explode
    else:
      attr_name = up_to_dot
    if not attr_name:
      raise ActionParsingError(
          'it does not contain an attribute name. If you truly have an '
          'empty attribute name, then specify it in a data slice path with '
          'the .get_attr("") syntax',
      )
    if not can_be_used_with_dot_syntax_in_data_slice_path_string(attr_name):
      raise ActionParsingError(
          f'the tentative attribute name "{attr_name}" is invalid after a'
          ' dot in a data slice path. If it is really the name of an'
          ' attribute, then please specify it in a data slice path with'
          ' the .get_attr("base64_encoded_attribute_name") syntax',
      )
    return cls(attr_name), data_slice_path.removeprefix(f'.{attr_name}')

  def __str__(self) -> str:
    if can_be_used_with_dot_syntax_in_data_slice_path_string(self.attr_name):
      return f'.{self.attr_name}'
    return f'.get_attr("{base64_encoded_attr_name(self.attr_name)}")'

  def debug_string(self) -> str:
    return f'{self.__class__.__name__}("{self.attr_name}")'

  def get_subschema(self, schema: kd.types.DataItem) -> kd.types.DataItem:
    if not schema.is_entity_schema():
      raise IncompatibleSchemaError(
          'getting attributes requires an ENTITY schema'
      )
    if not kd.any(kd.has_attr(schema, self.attr_name)):
      raise IncompatibleSchemaError(
          f'the schema does not have an attribute named "{self.attr_name}"'
      )
    return schema.get_attr(self.attr_name)

  def get_subschema_operation(self) -> str:
    return str(self)


def _get_prefix(*, of: str, before: str) -> str:
  """Returns the prefix of `of` that ends before `before`.

  If `of` does not include the substring passed in `before`, then the entire
  `of` is returned. The result is guaranteed to be a prefix of `of`.

  Args:
    of: the string we want a prefix of.
    before: the string that must be searched for in `of`.

  Returns:
    The prefix of `of` that ends before `before`.
  """
  return of.split(before)[0]


@dataclasses.dataclass(frozen=True)
class DataSlicePath:
  """A data slice path."""

  actions: tuple[DataSliceAction, ...]

  @classmethod
  def parse_from_string(cls, data_slice_path: str) -> DataSlicePath:
    r"""Parses the given string to return a data slice path.

    The empty string is a valid data slice path. Given a valid data slice path,
    we can obtain another valid data slice path by appending further actions:
    * append '.get_keys()' to get the keys of a dict.
    * append '.get_values()' to get the values of a dict.
    * append '[:]' to explode a list.
    * append '.attribute_name' to get an attribute of an entity. The attribute
      name must be a valid Python identifier to be used with this dot syntax.
      You can test for that by using some_string.isidentifier().
    * append '.get_attr("base64_encoded_attribute_name")' to get an attribute of
      an entity. The base64-encoded attribute name can be enclosed in single or
      double quotes. This syntax is useful when the attribute name contains
      special characters such as dots or quotes.

    Args:
      data_slice_path: the data slice path to parse. It must be valid according
        to the rules above.

    Returns:
      The parsed data slice path.

    Raises:
      ValueError: if the data_slice_path is not valid.
    """
    original_data_slice_path = data_slice_path
    actions = []

    def create_error(
        remaining_data_slice_path: str, error: ActionParsingError
    ) -> ValueError:
      path_prefix = original_data_slice_path.removesuffix(
          remaining_data_slice_path
      )
      actions_debug = [a.debug_string() for a in actions]
      return ValueError(
          f"invalid data slice path '{original_data_slice_path}'. After parsing"
          f" the prefix '{path_prefix}' as {actions_debug}, we cannot "
          f"process the remaining part '{remaining_data_slice_path}' because "
          f'{error}'
      )

    while data_slice_path:
      action_parsed = False
      for action_cls in [
          DictGetKeys,
          DictGetValues,
          ListExplode,
          GetAttr,
      ]:
        try:
          parsing_result = action_cls.parse_from_data_slice_path_prefix(
              data_slice_path
          )
        except ActionParsingError as e:
          raise create_error(data_slice_path, e) from e
        if parsing_result is None:
          continue
        action_parsed = True
        action, data_slice_path = parsing_result
        actions.append(action)
        break
      if not action_parsed:
        raise create_error(
            data_slice_path,
            ActionParsingError('it does not start with a valid prefix'),
        )
    return cls.from_actions(actions)

  @classmethod
  def from_actions(cls, actions: list[DataSliceAction]) -> DataSlicePath:
    return cls(tuple(actions))

  def concat(self, other: DataSlicePath) -> DataSlicePath:
    """Returns a new data slice path with the given data slice path appended."""
    return DataSlicePath(self.actions + other.actions)

  def extended_with_action(self, action: DataSliceAction) -> DataSlicePath:
    """Returns a new data slice path with the given action appended."""
    return DataSlicePath(self.actions + (action,))

  def to_string(self) -> str:
    """Returns the data slice path as a string."""
    return ''.join([str(action) for action in self.actions])

  def __str__(self) -> str:
    return self.to_string()


def get_subslice(
    data_slice: kd.types.DataSlice, data_slice_path: DataSlicePath
) -> kd.types.DataSlice:
  """Returns the subslice of `data_slice` at `data_slice_path`.

  Args:
    data_slice: the DataSlice to extract the subslice from.
    data_slice_path: the data slice path of the subslice to extract. It must be
      a valid data slice path for the schema of data_slice.

  Returns:
    The subslice of `data_slice` at `data_slice_path`.

  Raises:
    ValueError: if the data_slice_path is not valid for `data_slice`.
  """
  result = data_slice
  for action in data_slice_path.actions:
    result = action.evaluate(result)
  return result


def generate_data_slice_paths_for_arbitrary_data_slice_with_schema(
    schema: kd.types.DataItem, max_depth: int
) -> Generator[DataSlicePath, None, None]:
  """Yields all data slice paths for the given schema up to a maximum depth.

  This function answers the following question:
  For an arbitrary DataSlice ds with schema `schema`, what are the valid data
  slice paths with at most `max_depth` actions?

  This is a generator because the number of data slice paths can be very large.
  The maximum depth value is used to limit the number of data slice paths that
  are generated. Without a maximum depth, the number of data slice paths is
  infinite for recursive schemas.

  Args:
    schema: the Koda schema that induces the data slice paths.
    max_depth: the maximum depth of the data slice paths to generate.
  """

  def expand_one_step(
      data_slice_path: DataSlicePath,
      schema_item: kd.types.DataSlice,
  ) -> list[tuple[DataSlicePath, kd.types.DataSlice]]:
    try:
      schema_item.get_itemid().no_bag()
    except ValueError:  # The schema item does not have an item id.
      return []
    if schema_item.is_list_schema():
      return [(
          data_slice_path.extended_with_action(ListExplode()),
          schema_item.get_item_schema(),
      )]
    if schema_item.is_dict_schema():
      return [
          (
              data_slice_path.extended_with_action(DictGetKeys()),
              schema_item.get_key_schema(),
          ),
          (
              data_slice_path.extended_with_action(DictGetValues()),
              schema_item.get_value_schema(),
          ),
      ]
    # This is deterministic because kd.dir() returns sorted attribute names:
    return [
        (
            data_slice_path.extended_with_action(GetAttr(attr)),
            schema_item.get_attr(attr),
        )
        for attr in kd.dir(schema_item)
    ]

  level = [(DataSlicePath(tuple()), schema)]
  level_depth = 0
  while level and level_depth <= max_depth:
    level_depth += 1
    next_level = []
    for data_slice_path, schema_item in level:
      yield data_slice_path
      if level_depth <= max_depth:
        next_level.extend(expand_one_step(data_slice_path, schema_item))
    level = next_level
