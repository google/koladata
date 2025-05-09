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

"""Koladata benchmarks related to from/to_py."""

import copy

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
def to_py_100_of_100_single_int32(state):
  schema = kd.schema.new_schema(repeated_int32_field=kd.list_schema(kd.INT32))
  x = kd.slice([
      kd.new(repeated_int32_field=[i] * 100, schema=schema) for i in range(100)
  ])
  while state:
    _ = kd.to_py(x)


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
    _ = kd.to_py(x)


@google_benchmark.register
def to_py_100_of_100_deep(state):
  schema = kd.schema.new_schema(int32_field=kd.INT32)
  schema = schema.with_attrs(message_field=schema)
  bag = kd.bag()
  x = kd.slice([bag.new(schema=schema) for _ in range(100)])

  x_leaf = x
  for _ in range(100):
    x_leaf.int32_field = kd.int32(kd.range(100))
    x_leaf.message_field = kd.slice([kd.new(schema=schema) for _ in range(100)])
    x_leaf = x_leaf.message_field

  while state:
    _ = kd.to_py(x)


_SCHEMA_ARG = {
    0: kd.OBJECT,
    1: kd.INT32,
    2: kd.FLOAT32,
}


@google_benchmark.register
@google_benchmark.option.arg_names(['schema', 'from_dim'])
@google_benchmark.option.args([0, 0])
@google_benchmark.option.args([0, 1])
@google_benchmark.option.args([1, 0])
@google_benchmark.option.args([1, 1])
@google_benchmark.option.args([2, 0])
@google_benchmark.option.args([2, 1])
def universal_converter_no_caching_flat(state):
  # NOTE: There is no caching of Python objects, as all of them are unique.
  schema = _SCHEMA_ARG[state.range(0)]
  from_dim = state.range(1)
  if schema != kd.OBJECT and from_dim == 0:
    schema = kd.list_schema(schema)
  l = list(range(10000))
  while (state):
    _ = kd.from_py(l, from_dim=from_dim, schema=schema)


@google_benchmark.register
@google_benchmark.option.arg_names(['schema', 'from_dim'])
@google_benchmark.option.args([0, 0])
@google_benchmark.option.args([0, 1])
@google_benchmark.option.args([1, 0])
@google_benchmark.option.args([1, 1])
@google_benchmark.option.args([2, 0])
@google_benchmark.option.args([2, 1])
def universal_converter_no_caching_nested(state):
  # NOTE: There is no caching of Python objects, as all of them are unique.
  schema = _SCHEMA_ARG[state.range(0)]
  from_dim = state.range(1)
  if schema != kd.OBJECT:
    schema = kd.list_schema(schema)
  if schema != kd.OBJECT and from_dim == 0:
    schema = kd.list_schema(schema)
  nested_l = [[x, x + 1] for x in range(5000)]
  while (state):
    _ = kd.from_py(nested_l, from_dim=from_dim, schema=schema)


@google_benchmark.register
@google_benchmark.option.arg_names(['schema', 'from_dim'])
@google_benchmark.option.args([0, 0])
@google_benchmark.option.args([0, 12])
@google_benchmark.option.args([1, 0])
@google_benchmark.option.args([1, 12])
@google_benchmark.option.args([2, 0])
@google_benchmark.option.args([2, 12])
def universal_converter_no_caching_deep_nesting(state):
  # NOTE: There is no caching of Python objects, as all of them are unique.
  schema = _SCHEMA_ARG[state.range(0)]
  from_dim = state.range(1)
  if schema != kd.OBJECT and from_dim == 0:
    schema = kd.list_schema(schema)
  lst = list(range(4096))  # 2 ^ 12 elements
  for _ in range(11):  # 12 levels-deep
    new_lst = []
    if schema != kd.OBJECT and from_dim == 0:
      schema = kd.list_schema(schema)
    for i in range(0, len(lst), 2):
      new_lst.append([lst[i], lst[i + 1]])
    lst = new_lst
  while state:
    _ = kd.from_py(lst, from_dim=from_dim, schema=schema)


@google_benchmark.register
def deep_universal_converter(state):
  d = {'abc': 42}
  for _ in range(1000):
    d = {12: d.copy()}
  while (state):
    _ = kd.from_py(d)


@google_benchmark.register
def universal_converter_list(state):
  l = [1, 2, 3]
  for _ in range(10):
    l = [copy.deepcopy(l), copy.deepcopy(l), copy.deepcopy(l)]
  while (state):
    _ = kd.from_py(l)


@google_benchmark.register
def universal_converter_list_of_obj_primitives(state):
  l = [42] * 10000
  s = kd.list_schema(kd.OBJECT)
  while (state):
    _ = kd.from_py(l, schema=s)


@google_benchmark.register
def universal_converter_itemid(state):
  l = [1, 2, 3]
  for _ in range(10):
    l = [copy.deepcopy(l), copy.deepcopy(l), copy.deepcopy(l)]
  while (state):
    _ = kd.from_py(l, itemid=kd.uuid_for_list('itemid'))


if __name__ == '__main__':
  google_benchmark.main()
