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

"""Koladata benchmarks related to proto conversion."""

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
def from_proto_0fields_single_empty(state):
  message = test_pb2.EmptyMessage()
  while state:
    _ = kd.from_proto(message)


EMPTY_MESSAGE_SCHEMA = kd.schema_from_proto(test_pb2.EmptyMessage)
MESSAGE_C_SCHEMA = kd.schema_from_proto(test_pb2.MessageC)
MESSAGE_D_SCHEMA = kd.schema_from_proto(test_pb2.MessageD)


@google_benchmark.register
def from_proto_100fields_single_empty(state):
  message = test_pb2.MessageD()
  while state:
    _ = kd.from_proto(message)


@google_benchmark.register
def from_proto_0fields_single_empty_full_schema(state):
  message = test_pb2.EmptyMessage()
  while state:
    _ = kd.from_proto(message, schema=EMPTY_MESSAGE_SCHEMA)


@google_benchmark.register
def from_proto_100fields_1k_empty(state):
  messages = [test_pb2.MessageD() for _ in range(1000)]
  while state:
    _ = kd.from_proto(messages)


@google_benchmark.register
def from_proto_100fields_1k_empty_full_schema(state):
  messages = [test_pb2.MessageD() for _ in range(1000)]
  while state:
    _ = kd.from_proto(messages, schema=MESSAGE_D_SCHEMA)


@google_benchmark.register
def from_proto_1k_single_int32(state):
  messages = [test_pb2.MessageC(int32_field=i) for i in range(1000)]
  while state:
    _ = kd.from_proto(messages)


@google_benchmark.register
def from_proto_1k_single_int32_full_schema(state):
  messages = [test_pb2.MessageC(int32_field=i) for i in range(1000)]
  while state:
    _ = kd.from_proto(messages, schema=MESSAGE_C_SCHEMA)


@google_benchmark.register
def from_proto_100_of_100_single_int32(state):
  messages = [
      test_pb2.MessageC(repeated_int32_field=[i] * 100) for i in range(100)
  ]
  while state:
    _ = kd.from_proto(messages)


@google_benchmark.register
def from_proto_100_of_100_single_int32_full_schema(state):
  messages = [
      test_pb2.MessageC(repeated_int32_field=[i] * 100) for i in range(100)
  ]
  while state:
    _ = kd.from_proto(messages, schema=MESSAGE_C_SCHEMA)


@google_benchmark.register
def from_proto_1k_single_bytes(state):
  messages = [test_pb2.MessageC(bytes_field=b'a') for _ in range(1000)]
  while state:
    _ = kd.from_proto(messages)


@google_benchmark.register
def from_proto_1k_single_bytes_full_schema(state):
  messages = [test_pb2.MessageC(bytes_field=b'a') for _ in range(1000)]
  while state:
    _ = kd.from_proto(messages, schema=MESSAGE_C_SCHEMA)


@google_benchmark.register
def from_proto_100_single_10k_bytes(state):
  messages = [
      test_pb2.MessageC(bytes_field=b'a' * 10000) for _ in range(100)
  ]
  while state:
    _ = kd.from_proto(messages)


@google_benchmark.register
def from_proto_100_single_10k_bytes_full_schema(state):
  messages = [
      test_pb2.MessageC(bytes_field=b'a' * 10000) for _ in range(100)
  ]
  while state:
    _ = kd.from_proto(messages, schema=MESSAGE_C_SCHEMA)


@google_benchmark.register
def from_proto_single_mixed_primitive_fields(state):
  message = test_pb2.MessageC(
      message_field=test_pb2.MessageC(),
      int32_field=1,
      bytes_field=b'a',
      repeated_message_field=[test_pb2.MessageC()],
      repeated_int32_field=[1, 2, 3],
      repeated_bytes_field=[b'a', b'b', b'c'],
      map_int32_int32_field={1: 2, 3: 4},
      map_int32_message_field={1: test_pb2.MessageC()},
  )
  while state:
    _ = kd.from_proto(message)


@google_benchmark.register
def from_proto_single_mixed_primitive_fields_full_schema(state):
  message = test_pb2.MessageC(
      message_field=test_pb2.MessageC(),
      int32_field=1,
      bytes_field=b'a',
      repeated_message_field=[test_pb2.MessageC()],
      repeated_int32_field=[1, 2, 3],
      repeated_bytes_field=[b'a', b'b', b'c'],
      map_int32_int32_field={1: 2, 3: 4},
      map_int32_message_field={1: test_pb2.MessageC()},
  )
  while state:
    _ = kd.from_proto(message, schema=MESSAGE_C_SCHEMA)


@google_benchmark.register
def from_proto_1k_mixed_primitive_fields(state):
  messages = [
      test_pb2.MessageC(
          message_field=test_pb2.MessageC(),
          int32_field=1,
          bytes_field=b'a',
          repeated_message_field=[test_pb2.MessageC()],
          repeated_int32_field=[1, 2, 3],
          repeated_bytes_field=[b'a', b'b', b'c'],
          map_int32_int32_field={1: 2, 3: 4},
          map_int32_message_field={1: test_pb2.MessageC()}
      ) for _ in range(1000)
  ]
  while state:
    _ = kd.from_proto(messages)


@google_benchmark.register
def from_proto_1k_mixed_primitive_fields_full_schema(state):
  messages = [
      test_pb2.MessageC(
          message_field=test_pb2.MessageC(),
          int32_field=1,
          bytes_field=b'a',
          repeated_message_field=[test_pb2.MessageC()],
          repeated_int32_field=[1, 2, 3],
          repeated_bytes_field=[b'a', b'b', b'c'],
          map_int32_int32_field={1: 2, 3: 4},
          map_int32_message_field={1: test_pb2.MessageC()}
      ) for _ in range(1000)
  ]
  while state:
    _ = kd.from_proto(messages, schema=MESSAGE_C_SCHEMA)


@google_benchmark.register
def from_proto_100_of_100_deep(state):
  messages = [test_pb2.MessageC() for _ in range(100)]

  leaf_messages = list(messages)
  for _ in range(100):
    for i in range(100):
      leaf_messages[i] = leaf_messages[i].message_field
      leaf_messages[i].int32_field = i

  while state:
    _ = kd.from_proto(messages)


@google_benchmark.register
def from_proto_100_of_100_deep_full_schema(state):
  messages = [test_pb2.MessageC() for _ in range(100)]

  leaf_messages = list(messages)
  for _ in range(100):
    for i in range(100):
      leaf_messages[i] = leaf_messages[i].message_field
      leaf_messages[i].int32_field = i

  while state:
    _ = kd.from_proto(messages, schema=MESSAGE_C_SCHEMA)


@google_benchmark.register
def from_proto_100_of_100_deep_mixed_depth(state):
  messages = [test_pb2.MessageC() for _ in range(100)]

  leaf_messages = list(messages)
  for depth in range(100):
    for i in range(100):
      if i > depth:
        leaf_messages[i] = leaf_messages[i].message_field
        leaf_messages[i].int32_field = i

  while state:
    _ = kd.from_proto(messages)


@google_benchmark.register
def from_proto_100_of_100_deep_mixed_depth_full_schema(state):
  messages = [test_pb2.MessageC() for _ in range(100)]

  leaf_messages = list(messages)
  for depth in range(100):
    for i in range(100):
      if i > depth:
        leaf_messages[i] = leaf_messages[i].message_field
        leaf_messages[i].int32_field = i

  while state:
    _ = kd.from_proto(messages, schema=MESSAGE_C_SCHEMA)


@google_benchmark.register
def to_proto_0fields_single_empty(state):
  x = kd.new()
  while state:
    _ = kd.to_proto(x, test_pb2.EmptyMessage)


@google_benchmark.register
def to_proto_100fields_single_empty(state):
  x = kd.new()
  while state:
    _ = kd.to_proto(x, test_pb2.MessageD)


@google_benchmark.register
def to_proto_100fields_1k_empty(state):
  empty_schema = kd.schema.new_schema()
  x = kd.slice([kd.new(schema=empty_schema) for _ in range(1000)])
  while state:
    _ = kd.to_proto(x, test_pb2.MessageD)


@google_benchmark.register
def to_proto_1k_single_int32(state):
  schema = kd.schema.new_schema(int32_field=kd.INT32)
  x = kd.slice([kd.new(int32_field=i, schema=schema) for i in range(1000)])
  while state:
    _ = kd.to_proto(x, test_pb2.MessageC)


@google_benchmark.register
def to_proto_100_of_100_single_int32(state):
  schema = kd.schema.new_schema(repeated_int32_field=kd.list_schema(kd.INT32))
  x = kd.slice([
      kd.new(repeated_int32_field=[i] * 100, schema=schema) for i in range(100)
  ])
  while state:
    _ = kd.to_proto(x, test_pb2.MessageC)


@google_benchmark.register
def to_proto_1k_single_bytes(state):
  schema = kd.schema.new_schema(bytes_field=kd.BYTES)
  x = kd.slice([kd.new(bytes_field=b'a', schema=schema) for _ in range(1000)])
  while state:
    _ = kd.to_proto(x, test_pb2.MessageC)


@google_benchmark.register
def to_proto_100_single_10k_bytes(state):
  schema = kd.schema.new_schema(bytes_field=kd.BYTES)
  x = kd.slice(
      [kd.new(bytes_field=b'a' * 10000, schema=schema) for _ in range(100)]
  )
  while state:
    _ = kd.to_proto(x, test_pb2.MessageC)


@google_benchmark.register
def to_proto_single_mixed_primitive_fields(state):
  schema = kd.schema.new_schema()
  x = kd.new(
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
  while state:
    _ = kd.to_proto(x, test_pb2.MessageC)


@google_benchmark.register
def to_proto_1k_mixed_primitive_fields(state):
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
    _ = kd.to_proto(x, test_pb2.MessageC)


@google_benchmark.register
def to_proto_100_of_100_deep(state):
  schema = kd.schema.new_schema(int32_field=kd.INT32)
  schema = schema.with_attrs(message_field=schema)
  bag = kd.mutable_bag()
  x = kd.slice([bag.new(schema=schema) for _ in range(100)])

  x_leaf = x
  for _ in range(100):
    x_leaf.int32_field = kd.int32(kd.range(100))
    x_leaf.message_field = kd.slice([kd.new(schema=schema) for _ in range(100)])
    x_leaf = x_leaf.message_field

  while state:
    _ = kd.to_proto(x, test_pb2.MessageC)


@google_benchmark.register
def to_proto_100_of_100_deep_mixed_depth(state):
  schema = kd.schema.new_schema(int32_field=kd.INT32)
  schema = schema.with_attrs(message_field=schema)
  bag = kd.mutable_bag()
  x = kd.slice([bag.new(schema=schema) for _ in range(100)])

  x_leaf = x
  for i in range(100):
    x_leaf.int32_field = kd.int32(kd.range(100))
    x_leaf.message_field = kd.slice([kd.new(schema=schema) for _ in range(100)])
    x_leaf = x_leaf.message_field & (kd.index(x_leaf) > i)

  while state:
    _ = kd.to_proto(x, test_pb2.MessageC)


if __name__ == '__main__':
  google_benchmark.main()
