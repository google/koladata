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

"""Koda functions for converting to and from protocol buffers."""

from google.protobuf import message
from koladata.types import data_bag
from koladata.types import data_item as _  # pylint: disable=unused-import
from koladata.types import data_slice


def from_proto(
    messages: message.Message | list[message.Message],
    /,
    *,
    extensions: list[str] | None = None,
    itemid: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice | None = None,
    db: data_bag.DataBag | None = None,
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
    messages: Message or list of Message of the same type.
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
    db: The DataBag to use for the result, or None to use a new DataBag.

  Returns:
    A DataSlice representing the proto data.
  """
  if isinstance(messages, (list, tuple)):
    messages = list(messages)
    if not all(isinstance(m, message.Message) for m in messages):
      raise ValueError(
          f'messages must be Message or list of Message, got {messages}'
      )
    result_is_item = False
  elif isinstance(messages, message.Message):
    messages = [messages]
    result_is_item = True
  else:
    raise ValueError(
        f'messages must be Message or list of Message, got {messages}'
    )
  if extensions is None:
    extensions = []

  # Convert to utf-8 bytes to simplify C++ use.
  extensions = [ext.encode('utf-8') for ext in extensions]

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

  if db is None:
    db = data_bag.DataBag.empty()
  result = db._from_proto(messages, extensions, itemid, schema)  # pylint: disable=protected-access

  if result_is_item:
    result = result.S[0]
  return result
