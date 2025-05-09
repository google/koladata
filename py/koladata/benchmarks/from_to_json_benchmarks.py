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

"""Koladata benchmarks related to JSON conversion."""

import json

import google_benchmark
from koladata import kd


I = kd.I
V = kd.V
S = kd.S
kde = kd.lazy
kdi = kd.eager
kdf = kd.functor


# pylint: disable=missing-function-docstring
@google_benchmark.register
def from_json_simple_from_py_baseline(state):
  x = '{"a": 1, "b": "2", "c": 3.0, "d": [true, false, null]}'
  while state:
    _ = kd.from_py(json.loads(x))


@google_benchmark.register
def from_json_simple(state):
  x = kd.item('{"a": 1, "b": "2", "c": 3.0, "d": [true, false, null]}')
  while state:
    _ = kd.from_json(x)


@google_benchmark.register
def from_json_simple_with_schema_from_py_baseline(state):
  schema = kd.schema.new_schema(
      a=kd.INT32, b=kd.STRING, c=kd.FLOAT32, d=kd.list_schema(kd.BOOLEAN)
  )
  while state:
    _ = kd.from_py(
        json.loads('{"a": 1, "b": "2", "c": 3.0, "d": [true, false, null]}'),
        schema=schema,
    )


@google_benchmark.register
def from_json_simple_with_schema(state):
  schema = kd.schema.new_schema(
      a=kd.INT32, b=kd.STRING, c=kd.FLOAT32, d=kd.list_schema(kd.BOOLEAN)
  )
  while state:
    _ = kd.from_json(
        '{"a": 1, "b": "2", "c": 3.0, "d": [true, false, null]}',
        schema=schema,
    )


@google_benchmark.register
def from_json_100_nested_array_from_py_baseline(state):
  x = '[' * 100 + ']' * 100
  while state:
    _ = kd.from_py(json.loads(x))


@google_benchmark.register
def from_json_100_nested_array(state):
  x = kd.item('[' * 100 + ']' * 100)
  while state:
    _ = kd.from_json(x)


@google_benchmark.register
def from_json_100_nested_object_from_py_baseline(state):
  x = '{"x":' * 100 + 'null' + '}' * 100
  while state:
    _ = kd.from_py(json.loads(x))


@google_benchmark.register
def from_json_100_nested_object_no_keys_attr(state):
  x = kd.item('{"x":' * 100 + 'null' + '}' * 100)
  while state:
    _ = kd.from_json(x, keys_attr=None, values_attr=None)


@google_benchmark.register
def from_json_100_nested_object(state):
  x = kd.item('{"x":' * 100 + 'null' + '}' * 100)
  while state:
    _ = kd.from_json(x)


@google_benchmark.register
def to_json_simple_to_pytree_baseline(state):
  x = kd.new(a=1, b='2', c=3.0, d=kd.list([True, False, None]))
  while state:
    _ = json.dumps(kd.to_pytree(x, max_depth=-1))


@google_benchmark.register
def to_json_simple(state):
  x = kd.new(a=1, b='2', c=3.0, d=kd.list([True, False, None]))
  while state:
    _ = kd.to_json(x)


@google_benchmark.register
def to_json_100_nested_array_to_pytree_baseline(state):
  x = kd.item(None)
  for _ in range(100):
    x = kd.list([x])
  while state:
    _ = json.dumps(kd.to_pytree(x, max_depth=-1))


@google_benchmark.register
def to_json_100_nested_array(state):
  x = kd.item(None)
  for _ in range(100):
    x = kd.list([x])
  while state:
    _ = kd.to_json(x)


@google_benchmark.register
def to_json_100_nested_object_to_pytree_baseline(state):
  x = kd.item(None)
  for _ in range(100):
    x = kd.new(x=x)
  while state:
    _ = json.dumps(kd.to_pytree(x, max_depth=-1))


@google_benchmark.register
def to_json_100_nested_object(state):
  x = kd.item(None)
  for _ in range(100):
    x = kd.new(x=x)
  while state:
    _ = kd.to_json(x)


if __name__ == '__main__':
  google_benchmark.main()
