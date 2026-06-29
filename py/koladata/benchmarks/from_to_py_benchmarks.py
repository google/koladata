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

"""Koladata benchmarks related to from/to_py."""

import copy
import dataclasses
from typing import Any

import google_benchmark
from koladata import kd
from koladata.functions.tests import test_pb2


I = kd.I
V = kd.V
S = kd.S
kde = kd.lazy
kdi = kd.eager
kdf = kd.functor


# pylint: disable=missing-function-docstring
@google_benchmark.register
def internal_as_py(state):
  x = kd.slice([12] * 1000 + ['abc'] * 2000 + [b'abc'] * 3000 + [3.14] * 4000)
  while state:
    _ = x.internal_as_py()


@google_benchmark.register
def internal_as_py_deep_nesting(state):
  lst = [3.14] * 4096  # 2 ^ 12 elements
  for _ in range(11):  # 12 levels-deep
    new_lst = []
    for i in range(0, len(lst), 2):
      new_lst.append([lst[i], lst[i + 1]])
    lst = new_lst
  x = kd.slice(lst)
  while state:
    _ = x.internal_as_py()


@google_benchmark.register
def to_py_1k_single_int32(state):
  schema = kd.schema.new_schema(int32_field=kd.INT32)
  x = kd.slice([kd.new(int32_field=i, schema=schema) for i in range(1000)])
  while state:
    _ = kd.to_py(x)


@google_benchmark.register
def to_py_1k_single_int32_with_output_class(state):

  @dataclasses.dataclass
  class ObjWithInt32Field:
    int32_field: int

  schema = kd.schema.new_schema(int32_field=kd.INT32)
  x = kd.slice([kd.new(int32_field=i, schema=schema) for i in range(1000)])
  while state:
    _ = kd.to_py(x, output_class=ObjWithInt32Field)


@google_benchmark.register
def to_py_100_of_100_single_int32(state):
  schema = kd.schema.new_schema(repeated_int32_field=kd.list_schema(kd.INT32))
  x = kd.slice([
      kd.new(repeated_int32_field=[i] * 100, schema=schema) for i in range(100)
  ])
  while state:
    _ = kd.to_py(x)


@google_benchmark.register
def to_py_100_of_100_single_int32_with_output_class(state):
  schema = kd.schema.new_schema(repeated_int32_field=kd.list_schema(kd.INT32))

  @dataclasses.dataclass
  class ObjWithRepeatedInt32Field:
    repeated_int32_field: list[int]

  x = kd.slice([
      kd.new(repeated_int32_field=[i] * 100, schema=schema) for i in range(100)
  ])
  while state:
    _ = kd.to_py(x, output_class=ObjWithRepeatedInt32Field)


@dataclasses.dataclass
class EmptyObj:
  pass


@dataclasses.dataclass
class ComplexObj:
  message_field: EmptyObj
  int32_field: int
  bytes_field: bytes
  repeated_message_field: list[EmptyObj]
  repeated_int32_field: list[int]
  repeated_bytes_field: list[bytes]
  map_int32_int32_field: dict[int, int]
  map_int32_message_field: dict[int, EmptyObj]


@google_benchmark.register
def to_py_1k_mixed_primitive_fields(state):
  schema = kd.schema.new_schema()
  x = kd.slice([
      kd.new(
          message_field=kd.new(schema=schema),
          int32_field=1,
          bytes_field=b'a',
          repeated_message_field=[kd.new(schema=schema)],
          repeated_int32_field=[1, 2, 3],
          repeated_bytes_field=[b'a', b'b', b'c'],
          map_int32_int32_field={1: 2, 3: 4},
          map_int32_message_field={1: kd.new(schema=schema)},
          schema=schema,
      )
      for _ in range(1000)
  ])
  while state:
    _ = kd.to_py(x, max_depth=-1)


@google_benchmark.register
def to_py_1k_mixed_primitive_fields_with_output_class(state):
  schema = kd.schema.new_schema()
  x = kd.slice([
      kd.new(
          message_field=kd.new(schema=schema),
          int32_field=1,
          bytes_field=b'a',
          repeated_message_field=[kd.new(schema=schema)],
          repeated_int32_field=[1, 2, 3],
          repeated_bytes_field=[b'a', b'b', b'c'],
          map_int32_int32_field={1: 2, 3: 4},
          map_int32_message_field={1: kd.new(schema=schema)},
          schema=schema,
      )
      for _ in range(1000)
  ])
  while state:
    _ = kd.to_py(x, output_class=ComplexObj, max_depth=-1)


@dataclasses.dataclass
class SimpleObj:
  int32_field: int


@google_benchmark.register
def to_py_100_of_100_deep(state):
  schema = kd.schema.new_schema(int32_field=kd.INT32)
  schema = schema.with_attrs(message_field=schema)
  bag = kd.mutable_bag()
  x = kd.slice([bag.new(int32_field=i, schema=schema) for i in range(100)])

  x_leaf = x
  for _ in range(100):
    x_leaf.int32_field = kd.int32(kd.range(100))
    x_leaf.message_field = kd.slice(
        [kd.new(int32_field=j, schema=schema) for j in range(100)]
    )
    x_leaf = x_leaf.message_field

  while state:
    _ = kd.to_py(x, max_depth=-1)


@google_benchmark.register
def to_py_100_of_100_deep_with_output_class(state):
  schema = kd.schema.new_schema(int32_field=kd.INT32)
  schema = schema.with_attrs(message_field=schema)
  bag = kd.mutable_bag()
  x = kd.slice([bag.new(int32_field=i, schema=schema) for i in range(100)])

  obj = SimpleObj

  x_leaf = x
  for i in range(100):
    x_leaf.int32_field = kd.int32(kd.range(100))
    x_leaf.message_field = kd.slice(
        [kd.new(int32_field=j, schema=schema) for j in range(100)]
    )
    x_leaf = x_leaf.message_field

    obj = dataclasses.make_dataclass(
        f'Obj{i}', [('int32_field', int), ('message_field', obj)]
    )

  while state:
    _ = kd.to_py(x, output_class=obj, max_depth=-1)


@google_benchmark.register
def to_py_leaves_are_ignored(state):
  @dataclasses.dataclass
  class SmallObj:
    x: int

  root = kd.new(
      y=kd.new(
          x=kd.slice(list(range(1000))),
          # This will be cut entirely.
          w1=kd.slice(['a' * 10_000] * 1000),
          w2=kd.slice(['a' * 10_000] * 1000),
          w3=kd.slice(['a' * 10_000] * 1000),
          w4=kd.slice(['a' * 10_000] * 1000),
      )
  )
  while state:
    _ = kd.to_py(root.y, output_class=SmallObj)


_SCHEMA_ARG = {
    0: kd.OBJECT,
    1: kd.INT32,
    2: kd.FLOAT32,
    3: None,
}


def _not_none_not_object(schema):
  return schema is not None and schema != kd.OBJECT


@google_benchmark.register
@google_benchmark.option.arg_names(['schema', 'from_dim'])
@google_benchmark.option.args_product([[0, 1, 2, 3], [0, 1]])
def universal_converter_no_caching_flat(state):
  # NOTE: There is no caching of Python objects, as all of them are unique.
  schema = _SCHEMA_ARG[state.range(0)]
  from_dim = state.range(1)
  if _not_none_not_object(schema) and from_dim == 0:
    schema = kd.list_schema(schema)
  l = list(range(10000))
  while state:
    _ = kd.from_py(l, from_dim=from_dim, schema=schema)


@google_benchmark.register
@google_benchmark.option.arg_names(['schema', 'from_dim'])
@google_benchmark.option.args_product([[0, 1, 2, 3], [0, 1]])
def universal_converter_no_caching_nested(state):
  # NOTE: There is no caching of Python objects, as all of them are unique.
  schema = _SCHEMA_ARG[state.range(0)]
  from_dim = state.range(1)
  if _not_none_not_object(schema):
    schema = kd.list_schema(schema)
    if from_dim == 0:
      schema = kd.list_schema(schema)
  nested_l = [[x, x + 1] for x in range(5000)]
  while (state):
    _ = kd.from_py(nested_l, from_dim=from_dim, schema=schema)


@google_benchmark.register
@google_benchmark.option.arg_names(['schema', 'from_dim'])
@google_benchmark.option.args_product([[0, 1, 2, 3], [0, 12]])
def universal_converter_no_caching_deep_nesting(state):
  # NOTE: There is no caching of Python objects, as all of them are unique.
  schema = _SCHEMA_ARG[state.range(0)]
  from_dim = state.range(1)
  use_list_schema = _not_none_not_object(schema) and from_dim == 0
  if use_list_schema:
    schema = kd.list_schema(schema)
  lst = list(range(4096))  # 2 ^ 12 elements
  for _ in range(11):  # 12 levels-deep
    new_lst = []
    if use_list_schema:
      schema = kd.list_schema(schema)
    for i in range(0, len(lst), 2):
      new_lst.append([lst[i], lst[i + 1]])
    lst = new_lst
  while state:
    _ = kd.from_py(lst, from_dim=from_dim, schema=schema)


@google_benchmark.register
@google_benchmark.option.arg_names(['schema'])
@google_benchmark.option.args_product([[0, 3]])  # OBJECT and None
def deep_universal_converter(state):
  schema = _SCHEMA_ARG[state.range(0)]
  d = {'abc': 42}
  for _ in range(1000):
    d = {12: d.copy()}
  while state:
    _ = kd.from_py(d, schema=schema)


@google_benchmark.register
@google_benchmark.option.arg_names(['schema'])
@google_benchmark.option.args_product([[0, 3]])  # OBJECT and None
def deep_universal_converter_dict_as_obj(state):
  schema = _SCHEMA_ARG[state.range(0)]
  d = {'abc': 42}
  for _ in range(1000):
    d = {'x': d.copy()}
  while state:
    _ = kd.from_py(d, schema=schema, dict_as_obj=True)


@google_benchmark.register
@google_benchmark.option.arg_names(['schema'])
@google_benchmark.option.args_product([[0, 3]])  # OBJECT and None
def universal_converter_object(state):
  schema = _SCHEMA_ARG[state.range(0)]

  @dataclasses.dataclass
  class A:
    x: Any | int = 123
    y: Any | int = 456
    z: Any | int = 789

  def create_obj():
    obj = A()
    for _ in range(5):
      obj = A(x=obj, y=obj, z=obj)
    return obj

  obj = [create_obj() for _ in range(1000)]

  while state:
    _ = kd.from_py(obj, schema=schema)


@google_benchmark.register
def universal_converter_entity(state):
  schema = kd.schema.new_schema(x=kd.INT32, y=kd.INT32, z=kd.INT32)

  @dataclasses.dataclass
  class A:
    x: Any | int = 123
    y: Any | int = 456
    z: Any | int = 789

  def create_obj():
    obj = A()
    for _ in range(5):
      obj = A(x=obj, y=obj, z=obj)
    return obj

  def create_schema(leaf_schema):
    schema = leaf_schema
    for _ in range(5):
      schema = kd.schema.new_schema(x=schema, y=schema, z=schema)
    schema = kd.schema.list_schema(schema)
    return schema

  obj = [create_obj() for _ in range(1000)]

  while state:
    _ = kd.from_py(obj, schema=create_schema(schema))


@google_benchmark.register
@google_benchmark.option.arg_names(['schema'])
@google_benchmark.option.args_product([[0, 3]])
def universal_converter_list(state):
  schema = _SCHEMA_ARG[state.range(0)]
  l = [1, 2, 3]
  for _ in range(10):
    l = [copy.deepcopy(l), copy.deepcopy(l), copy.deepcopy(l)]
  while (state):
    _ = kd.from_py(l, schema=schema)


@google_benchmark.register
@google_benchmark.option.arg_names(['schema'])
@google_benchmark.option.args_product([[0, 3, 6]])
def universal_converter_list_of_obj_primitives(state):
  l = [42] * 10000
  if state.range(0) == 6:
    schema = kd.list_schema(kd.OBJECT)
  else:
    schema = _SCHEMA_ARG[state.range(0)]

  while (state):
    _ = kd.from_py(l, schema=schema)


@google_benchmark.register
@google_benchmark.option.arg_names(['schema'])
@google_benchmark.option.args_product([[0, 3]])
def universal_converter_itemid(state):
  schema = _SCHEMA_ARG[state.range(0)]
  l = [1, 2, 3]
  for _ in range(10):
    l = [copy.deepcopy(l), copy.deepcopy(l), copy.deepcopy(l)]
  while (state):
    _ = kd.from_py(l, itemid=kd.uuid_for_list('itemid'), schema=schema)


# -- Proto benchmarks via kd.from_py --
# These benchmark the ConvertProto path in from_py, which converts proto
# messages to Koda objects via from_py (as opposed to kd.from_proto).

MESSAGE_C_SCHEMA = kd.schema_from_proto(test_pb2.MessageC)
MESSAGE_D_SCHEMA = kd.schema_from_proto(test_pb2.MessageD)


def _make_message_c_mixed():
  """Creates a MessageC with all field types populated."""
  return test_pb2.MessageC(
      message_field=test_pb2.MessageC(),
      int32_field=1,
      bytes_field=b'a',
      bool_field=True,
      float_field=1.5,
      double_field=2.5,
      repeated_message_field=[test_pb2.MessageC()],
      repeated_int32_field=[1, 2, 3],
      repeated_bytes_field=[b'a', b'b', b'c'],
      map_int32_int32_field={1: 2, 3: 4},
      map_int32_message_field={1: test_pb2.MessageC()},
  )


def _make_message_d_full():
  """Creates a MessageD with all 100 int32 fields populated."""
  return test_pb2.MessageD(**{f'field{i}': i for i in range(1, 101)})


def _make_deep_messages(count, depth):
  """Creates `count` MessageC protos nested to `depth` levels."""
  messages = [test_pb2.MessageC() for _ in range(count)]
  leaf_messages = list(messages)
  for _ in range(depth):
    for i in range(count):
      leaf_messages[i] = leaf_messages[i].message_field
      leaf_messages[i].int32_field = i
  return messages


# --- Single proto via from_py ---


@google_benchmark.register
def from_py_proto_single_mixed_fields(state):
  message = _make_message_c_mixed()
  while state:
    _ = kd.from_py(message)


@google_benchmark.register
def from_py_proto_single_mixed_fields_with_schema(state):
  message = _make_message_c_mixed()
  while state:
    _ = kd.from_py(message, schema=MESSAGE_C_SCHEMA)


# --- 1k protos with mixed fields via from_py ---


@google_benchmark.register
def from_py_proto_1k_mixed_fields(state):
  messages = [_make_message_c_mixed() for _ in range(1000)]
  while state:
    _ = kd.from_py(messages)


@google_benchmark.register
def from_py_proto_1k_mixed_fields_with_schema(state):
  messages = [_make_message_c_mixed() for _ in range(1000)]
  schema = kd.list_schema(MESSAGE_C_SCHEMA)
  while state:
    _ = kd.from_py(messages, schema=schema)


# --- 100-field MessageD via from_py ---


@google_benchmark.register
def from_py_proto_100fields_1k(state):
  messages = [_make_message_d_full() for _ in range(1000)]
  while state:
    _ = kd.from_py(messages)


@google_benchmark.register
def from_py_proto_100fields_1k_with_schema(state):
  messages = [_make_message_d_full() for _ in range(1000)]
  schema = kd.list_schema(MESSAGE_D_SCHEMA)
  while state:
    _ = kd.from_py(messages, schema=schema)


# --- Deeply nested protos via from_py ---


@google_benchmark.register
def from_py_proto_100_of_100_deep(state):
  messages = _make_deep_messages(count=100, depth=100)
  while state:
    _ = kd.from_py(messages)


@google_benchmark.register
def from_py_proto_100_of_100_deep_with_schema(state):
  messages = _make_deep_messages(count=100, depth=100)
  schema = kd.list_schema(MESSAGE_C_SCHEMA)
  while state:
    _ = kd.from_py(messages, schema=schema)


# --- Mixed: protos alongside Python dicts/lists ---


@google_benchmark.register
def from_py_proto_mixed_with_dicts(state):
  """Protos interleaved with dicts in a list — exercises mixed conversion."""
  items = []
  for i in range(500):
    items.append(test_pb2.MessageC(int32_field=i))
    items.append({'int32_field': i})
  while state:
    _ = kd.from_py(items)


# --- Comparison: from_py(from_dim=1) vs from_proto ---


@google_benchmark.register
def from_py_proto_1k_mixed_fields_with_schema_from_dim1(state):
  messages = [_make_message_c_mixed() for _ in range(1000)]
  schema = MESSAGE_C_SCHEMA
  while state:
    # Use from_dim=1 to return a DataSlice of shape [1000] instead of a List,
    # matching what from_proto does.
    _ = kd.from_py(messages, schema=schema, from_dim=1)


@google_benchmark.register
def from_proto_1k_mixed_fields_with_schema_direct(state):
  messages = [_make_message_c_mixed() for _ in range(1000)]
  schema = MESSAGE_C_SCHEMA
  while state:
    _ = kd.from_proto(messages, schema=schema)


@google_benchmark.register
def from_py_proto_100fields_1k_with_schema_from_dim1(state):
  messages = [_make_message_d_full() for _ in range(1000)]
  schema = MESSAGE_D_SCHEMA
  while state:
    _ = kd.from_py(messages, schema=schema, from_dim=1)


@google_benchmark.register
def from_proto_100fields_1k_with_schema_direct(state):
  messages = [_make_message_d_full() for _ in range(1000)]
  schema = MESSAGE_D_SCHEMA
  while state:
    _ = kd.from_proto(messages, schema=schema)


@google_benchmark.register
def from_py_proto_nested_100x10_with_schema_from_dim2(state):
  nested_messages = [
      [_make_message_c_mixed() for _ in range(10)] for _ in range(100)
  ]
  schema = MESSAGE_C_SCHEMA
  while state:
    _ = kd.from_py(nested_messages, schema=schema, from_dim=2)


@google_benchmark.register
def from_proto_nested_100x10_with_schema_direct(state):
  nested_messages = [
      [_make_message_c_mixed() for _ in range(10)] for _ in range(100)
  ]
  schema = MESSAGE_C_SCHEMA
  while state:
    _ = kd.from_proto(nested_messages, schema=schema)


@google_benchmark.register
def from_py_proto_100_of_100_deep_with_schema_from_dim1(state):
  messages = _make_deep_messages(count=100, depth=100)
  schema = MESSAGE_C_SCHEMA
  while state:
    _ = kd.from_py(messages, schema=schema, from_dim=1)


@google_benchmark.register
def from_proto_100_of_100_deep_with_schema_direct(state):
  messages = _make_deep_messages(count=100, depth=100)
  schema = MESSAGE_C_SCHEMA
  while state:
    _ = kd.from_proto(messages, schema=schema)


@google_benchmark.register
def from_py_proto_100_of_100_deep_from_dim1(state):
  messages = _make_deep_messages(count=100, depth=100)
  while state:
    _ = kd.from_py(messages, schema=None, from_dim=1)


@google_benchmark.register
def from_proto_100_of_100_deep_direct(state):
  messages = _make_deep_messages(count=100, depth=100)
  while state:
    _ = kd.from_proto(messages)


if __name__ == '__main__':
  google_benchmark.main()
