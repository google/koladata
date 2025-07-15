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

"""Koda functions for converting to and from protocol buffers."""

from typing import Type

from google.protobuf import message
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice


def from_proto(
    messages: message.Message | None | list[message.Message | None],
    /,
    *,
    extensions: list[str] | None = None,
    itemid: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice | None = None,
) -> data_slice.DataSlice:
  """Returns a DataSlice representing proto data.

  Messages, primitive fields, repeated fields, and maps are converted to
  equivalent Koda structures: objects/entities, primitives, lists, and dicts,
  respectively. Enums are converted to INT32. The attribute names on the Koda
  objects match the field names in the proto definition. See below for methods
  to convert proto extensions to attributes alongside regular fields.

  Messages, primitive fields, repeated fields, and maps are converted to
  equivalent Koda structures. Enums are converted to ints.

  Only present values in `messages` are added. Default and missing values are
  not used.

  Proto extensions are ignored by default unless `extensions` is specified (or
  if an explicit entity schema with parenthesized attrs is used).
  The format of each extension specified in `extensions` is a dot-separated
  sequence of field names and/or extension names, where extension names are
  fully-qualified extension paths surrounded by parentheses. This sequence of
  fields and extensions is traversed during conversion, in addition to the
  default behavior of traversing all fields. For example:

    "path.to.field.(package_name.some_extension)"
    "path.to.repeated_field.(package_name.some_extension)"
    "path.to.map_field.values.(package_name.some_extension)"
    "path.(package_name.some_extension).(package_name2.nested_extension)"

  Extensions are looked up using the C++ generated descriptor pool, using
  `DescriptorPool::FindExtensionByName`, which requires that all extensions are
  compiled in as C++ protos. The Koda attribute names for the extension fields
  are parenthesized fully-qualified extension paths (e.g.
  "(package_name.some_extension)" or
  "(package_name.SomeMessage.some_extension)".) As the names contain '()' and
  '.' characters, they cannot be directly accessed using '.name' syntax but can
  be accessed using `.get_attr(name)'. For example,

    ds.get_attr('(package_name.AbcExtension.abc_extension)')
    ds.optional_field.get_attr('(package_name.DefExtension.def_extension)')

  If `messages` is a single proto Message, the result is a DataItem. If it is a
  list of proto Messages, the result is an 1D DataSlice.

  Args:
    messages: Message or list of Message of the same type. Any of the messages
      may be None, which will produce missing items in the result.
    extensions: List of proto extension paths.
    itemid: The ItemId(s) to use for the root object(s). If not specified, will
      allocate new id(s). If specified, will also infer the ItemIds for all
      child items such as List items from this id, so that repeated calls to
      this method on the same input will produce the same id(s) for everything.
      Use this with care to avoid unexpected collisions.
    schema: The schema to use for the return value. Can be set to kd.OBJECT to
      (recursively) create an object schema. Can be set to None (default) to
      create an uuschema based on the proto descriptor. When set to an entity
      schema, some fields may be set to kd.OBJECT to create objects from that
      point.

  Returns:
    A DataSlice representing the proto data.
  """
  if isinstance(messages, (list, tuple)):
    messages = list(messages)
    if not all(m is None or isinstance(m, message.Message) for m in messages):
      raise ValueError(
          f'messages must be Message or list of Message, got {messages}'
      )
    result_is_item = False
  elif messages is None or isinstance(messages, message.Message):
    messages = [messages]
    result_is_item = True
  else:
    raise ValueError(
        f'messages must be Message or list of Message, got {messages}'
    )

  if itemid is not None:
    if result_is_item:
      if itemid.get_ndim() != 0:
        raise ValueError(
            'itemid must be a DataItem if messages is a single message'
        )
      itemid = itemid.flatten(0, 0)  # itemid -> slice([itemid])
    else:
      if itemid.get_ndim() != 1:
        raise ValueError(
            'itemid must be a 1-D DataSlice if messages is a list of messages'
        )

  if schema is not None and schema.get_bag() is not None:
    # Avoid schema adoption.
    schema_bag = schema.get_bag()
    try:
      bag = schema_bag.fork()
    except ValueError:
      # Fork may fail with ValueError if the schema bag has fallbacks.
      bag = data_bag.DataBag.empty()
    else:
      schema = schema.with_bag(bag)
  else:
    bag = data_bag.DataBag.empty()
  result = bag._from_proto(  # pylint: disable=protected-access
      messages, extensions, itemid, schema
  )

  if result_is_item:
    result = result.S[0]
  return result.freeze_bag()


def schema_from_proto(
    message_class: Type[message.Message],
    /,
    *,
    extensions: list[str] | None = None,
) -> data_item.DataItem:
  """Returns a Koda schema representing a proto message class.

  This is similar to `from_proto(x).get_schema()` when `x` is an instance of
  `message_class`, except that it eagerly adds all non-extension fields to the
  schema instead of only adding fields that have data populated in `x`.

  The returned schema is a uuschema whose itemid is a function of the proto
  message class' fully qualified name, and any child message classes' schemas
  are also uuschemas derived in the same way. The returned schema has the same
  itemid as `from_proto(message_class()).get_schema()`.

  The format of each extension specified in `extensions` is a dot-separated
  sequence of field names and/or extension names, where extension names are
  fully-qualified extension paths surrounded by parentheses. For example:

    "path.to.field.(package_name.some_extension)"
    "path.to.repeated_field.(package_name.some_extension)"
    "path.to.map_field.values.(package_name.some_extension)"
    "path.(package_name.some_extension).(package_name2.nested_extension)"

  Args:
    message_class: A proto message class to convert.
    extensions: List of proto extension paths.

  Returns:
    A DataItem containing the converted schema.
  """
  result = data_bag.DataBag.empty()._schema_from_proto(  # pylint: disable=protected-access
      message_class(), extensions
  )
  return result.freeze_bag()


def to_proto(
    x: data_slice.DataSlice,
    /,
    message_class: Type[message.Message]
) -> list[message.Message | None] | message.Message | None:
  """Converts a DataSlice or DataItem to one or more proto messages.

  If `x` is a DataItem, this returns a single proto message object. Otherwise,
  `x` must be a 1-D DataSlice, and this returns a list of proto message objects
  with the same size as the input. Missing items in the input are returned as
  python None in place of a message.

  Koda data structures are converted to equivalent proto messages, primitive
  fields, repeated fields, maps, and enums, based on the proto schema. Koda
  entity attributes are converted to message fields with the same name, if
  those fields exist, otherwise they are ignored.

  Koda slices with mixed underlying dtypes are tolerated wherever the proto
  conversion is defined for all dtypes, regardless of schema.

  Koda entity attributes that are parenthesized fully-qualified extension
  paths (e.g. "(package_name.some_extension)") are converted to extensions,
  if those extensions exist in the descriptor pool of the messages' common
  descriptor, otherwise they are ignored.

  Args:
    x: DataSlice to convert.
    message_class: A proto message class.

  Returns:
    A converted proto message or list of converted proto messages.
  """
  return x._to_proto(message_class)  # pylint: disable=protected-access
