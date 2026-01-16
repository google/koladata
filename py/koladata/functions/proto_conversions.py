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

from collections.abc import Iterator
from typing import Any, Type, TypeAlias, TypeVar, cast

from google.protobuf import any as protobuf_any
from google.protobuf import descriptor_pool as protobuf_descriptor_pool
from google.protobuf import message
from google.protobuf import message_factory as protobuf_message_factory
from koladata.expr import expr_eval
from koladata.operators import kde_operators
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import schema_constants
from koladata.types import schema_item

from google.protobuf import any_pb2


_T = TypeVar('_T')

# Note: these are for nested lists/sequences of uniform depth (intentionally).
# They can also be replaced with PEP 695 `type X[T] = ...` syntax once support
# is more widely available in type checkers.
_NestedNoneList: TypeAlias = list['_NestedNoneList'] | None
_NestedMessageList: TypeAlias = (
    message.Message | list['_NestedMessageList'] | None
)
_NestedMessageContainer: TypeAlias = (
    message.Message
    | list['_NestedMessageContainer']
    | tuple['_NestedMessageContainer', ...]
    | None
)
_NestedAnyMessageList: TypeAlias = (
    any_pb2.Any | list['_NestedAnyMessageList'] | None
)
_NestedAnyMessageContainer: TypeAlias = (
    any_pb2.Any
    | list['_NestedAnyMessageContainer']
    | tuple['_NestedAnyMessageContainer', ...]
    | None
)


def _to_nested_list_of_none(x: _NestedMessageContainer) -> _NestedNoneList:
  """Gets the "shape" of a nested container as a nested list with None leaves."""
  if not isinstance(x, (list, tuple)):
    return None
  return [_to_nested_list_of_none(item) for item in x]


def _flatten(x: _NestedMessageContainer) -> Iterator[message.Message | None]:
  """Iterates over the leaves of a nested container."""
  if isinstance(x, (list, tuple)):
    for item in x:
      yield from _flatten(item)
  else:
    yield x


# Note: could use `tree.unflatten_as`, but it's not worth adding an additional
# third-party dependency just for this.
def _unflatten(shape: _NestedNoneList, it: Iterator[Any]) -> Any:
  """Unflattens an iterator into a shape given by a nested list with None leaves."""
  return [_unflatten(x, it) for x in shape] if shape is not None else next(it)


def from_proto(
    messages: _NestedMessageContainer,
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

  If schema is not specified or schema is kd.OBJECT, only present fields in
  `messages` are loaded and included in the converted schema. To get a schema
  that contains all fields independent of the data, use `kd.schema_from_proto`.

  Proto extensions are ignored by default unless `extensions` is specified, or
  if an explicit entity schema with parenthesized attr names is specified. If
  both are specified, we use the union of the two extension sets.

  The format of each extension specified in `extensions` is a dot-separated
  sequence of field names and/or extension names, where extension names are
  fully-qualified extension paths surrounded by parentheses. This sequence of
  fields and extensions is traversed during conversion, in addition to the
  default behavior of traversing all fields. For example:

    "path.to.field.(package_name.some_extension)"
    "path.to.repeated_field.(package_name.some_extension)"
    "path.to.map_field.values.(package_name.some_extension)"
    "path.(package_name.some_extension).(package_name2.nested_extension)"

  If an explicit entity schema attr name starts with "(" and ends with ")" it is
  also interpreted as an extension name.

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
  nested list of proto Messages, the result is a DataSlice with the same number
  of dimensions as the nesting level.

  Args:
    messages: Message or nested list/tuple of Message of the same type. Any of
      the messages may be None, which will produce missing items in the result.
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
  messages_shape = data_slice.DataSlice.from_vals(
      _to_nested_list_of_none(messages)
  ).get_shape()
  messages_list = list(_flatten(messages))

  def _ellipsize_repr(x, max_length: int = 200) -> str:
    s = repr(x)
    if len(s) < max_length:
      return s
    return f'{s[:max_length//2]}...{s[-max_length//2+3:]}'

  for i, m in enumerate(messages_list):
    if m is not None and not isinstance(m, message.Message):
      if messages_shape.rank() == 0:
        raise ValueError(
            'messages must be Message or nested list of Message, got'
            f' type {type(m)} with value {_ellipsize_repr(m)}'
        )
      elif messages_shape.rank() == 1:
        raise ValueError(
            'messages must be Message or nested list of Message, got list'
            f' containing type {type(m)} at index {i} with value'
            f' {_ellipsize_repr(m)}'
        )
      else:
        raise ValueError(
            'messages must be Message or nested list of Message, got nested'
            f' list containing type {type(m)} at leaf index {i} with value'
            f' {_ellipsize_repr(m)}'
        )

  if itemid is not None:
    if itemid.get_shape() != messages_shape:
      raise ValueError(
          'itemid must match the shape of messages, got'
          f' {itemid.get_shape()} != {messages_shape}'
      )
    itemid = itemid.flatten()

  if schema is not None and schema.has_bag():
    # Avoid schema adoption.
    schema_bag = schema.get_bag()
    try:
      bag = schema_bag.fork()
    except ValueError:
      # Fork may fail with ValueError if the schema bag has fallbacks.
      bag = data_bag.DataBag.empty_mutable()
    else:
      schema = schema.with_bag(bag)
  else:
    bag = data_bag.DataBag.empty_mutable()
  result = bag._from_proto(  # pylint: disable=protected-access
      messages_list, extensions, itemid, schema
  )

  result = result.reshape(messages_shape)
  return result.freeze_bag()


def from_proto_any(
    messages: _NestedAnyMessageContainer,
    /,
    *,
    extensions: list[str] | None = None,
    itemid: data_slice.DataSlice | None = None,
    schema: data_slice.DataSlice | None = None,
    message_type: type[message.Message] | None = None,
    descriptor_pool: protobuf_descriptor_pool.DescriptorPool | None = None,
) -> data_slice.DataSlice:
  """Returns a DataSlice converted from a nested list of proto Any messages.

  This function is similar to `from_proto`, but it first unpacks the Any
  messages before converting them. Only the top-level Any message is unpacked:
  if there are Any fields inside of the unpacked message, they are treated the
  same as in `from_proto`.

  If `message_type` is provided, all Any messages are unpacked into this type.
  Otherwise, the type is inferred from the type URL in each Any message, and
  the messages are looked up in the `descriptor_pool`. If `descriptor_pool` is
  not provided, the default descriptor pool is used.

  If `schema` is not explicitly provided, the resulting DataSlice will have an
  OBJECT schema so that inputs with differing message types can be represented.

  Args:
    messages: google.protobuf.Any message or nested list/tuple of
      google.protobuf.Any messages. Any of the messages may be None, which will
      produce missing items in the result.
    extensions: See `from_proto` for more details.
    itemid: See `from_proto` for more details.
    schema: See `from_proto` for more details.
    message_type: The type to unpack the Any messages into. If None, the type is
      inferred from the Any messages.
    descriptor_pool: The descriptor pool to use for looking up message types. If
      None, the default descriptor pool is used.

  Returns:
    A DataSlice representing the unpacked and converted proto data.
  """
  if descriptor_pool is None and message_type is None:
    descriptor_pool = protobuf_descriptor_pool.Default()

  messages_list = _flatten(messages)
  shape = data_slice.DataSlice.from_vals(
      _to_nested_list_of_none(messages)
  ).get_shape()

  unpacked_message_items: list[data_slice.DataSlice | None] = []
  for m in messages_list:
    if isinstance(m, any_pb2.Any):
      if message_type is None:
        full_name = protobuf_any.type_name(m).split('/')[-1]
        descriptor = descriptor_pool.FindMessageTypeByName(full_name)
        message_cls = protobuf_message_factory.GetMessageClass(descriptor)
        unpacked_message = message_cls()
      else:
        unpacked_message = message_type()
      if not protobuf_any.unpack(m, unpacked_message):
        raise ValueError(f'failed to unpack into message type {message_type}')
      item = from_proto(
          unpacked_message,
          extensions=extensions,
          itemid=itemid,
          schema=schema,
      )
      if schema is None:
        item = expr_eval.eval(kde_operators.kde.obj(item))
      unpacked_message_items.append(item)
    elif m is None:
      item = None
      if schema is None:
        item = expr_eval.eval(kde_operators.kde.obj(item))
      unpacked_message_items.append(item)
    else:
      raise ValueError(
          'from_proto_any expects a nested container of google.protobuf.Any'
          f' messages, got {repr(type(m))}'
      )

  if not unpacked_message_items:
    return expr_eval.eval(
        kde_operators.kde.empty_shaped(
            shape,
            schema=schema if schema is not None else schema_constants.OBJECT,
        )
    )

  return expr_eval.eval(
      kde_operators.kde.reshape(
          kde_operators.kde.stack(*unpacked_message_items), shape
      )
  )


def schema_from_proto(
    message_class: Type[message.Message],
    /,
    *,
    extensions: list[str] | None = None,
) -> schema_item.SchemaItem:
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
    A SchemaItem containing the converted schema.
  """
  result = data_bag.DataBag.empty_mutable()._schema_from_proto(  # pylint: disable=protected-access
      message_class(), extensions
  )
  return cast(schema_item.SchemaItem, result.freeze_bag())


def to_proto(
    x: data_slice.DataSlice, /, message_class: Type[message.Message]
) -> _NestedMessageList:
  """Converts a DataSlice or DataItem to one or more proto messages.

  If `x` is a DataItem, this returns a single proto message object. Otherwise,
  this returns a nested list of proto message objects with the same size and
  shape as the input. Missing items in the input are returned as python None in
  place of a message.

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
  x_shape = (x & mask_constants.missing).to_py()
  results_flat = x.flatten()._to_proto(message_class)  # pylint: disable=protected-access
  return _unflatten(x_shape, iter(results_flat))


def to_proto_any(
    x: data_slice.DataSlice,
    *,
    descriptor_pool: protobuf_descriptor_pool.DescriptorPool | None = None,
    deterministic: bool = False,
) -> _NestedAnyMessageList:
  """Converts a DataSlice or DataItem to proto Any messages.

  The schemas of all present values in `x` must have been derived from a proto
  schema using `from_proto` or `schema_from_proto`, so that the original names
  of the message types are embedded in the schema. Otherwise, this will fail.

  Args:
    x: DataSlice to convert.
    descriptor_pool: Overrides the descriptor pool used to look up python proto
      message classes based on proto message type full name. If None, the
      default descriptor pool is used.
    deterministic: Passed to Any.Pack.

  Returns:
    A proto Any message or nested list of proto Any messages with the same
    shape as the input. Missing elements in the input are None in the output.
  """
  if descriptor_pool is None:
    descriptor_pool = protobuf_descriptor_pool.Default()

  x_shape = (x & mask_constants.missing).to_py()
  x_flat = x.flatten()
  full_names = expr_eval.eval(
      kde_operators.kde.proto.get_proto_full_name(x_flat)
  ).to_py()
  results = []
  for i, full_name in enumerate(full_names):
    x_item = x_flat.S[i]
    if full_name is None:
      if not x_item.is_empty():
        raise ValueError(
            'to_proto_any expects entities converted from proto messages, got'
            f' {x_item}'
        )
      else:
        results.append(None)
    else:
      message_descriptor = descriptor_pool.FindMessageTypeByName(full_name)
      message_type = protobuf_message_factory.GetMessageClass(
          message_descriptor
      )
      m = to_proto(x_item, message_type)
      result = any_pb2.Any()
      result.Pack(m, deterministic=deterministic)
      results.append(result)

  return _unflatten(x_shape, iter(results))
