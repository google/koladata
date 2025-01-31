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

"""Benchmarks for Kola Data library."""

import copy

from arolla import arolla
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
def create_empty_data_bag(state):
  while state:
    _ = kd.bag()


@google_benchmark.register
def create_empty_data_bag_via_kdi(state):
  while state:
    _ = kdi.bag()


@google_benchmark.register
def create_entity(state):
  val_1 = kd.slice(12)
  val_2 = kd.slice(13)
  val_3 = kd.slice(14)
  while state:
    db = kd.bag()
    _ = db.new(val_1=val_1, val_2=val_2, val_3=val_3)


@google_benchmark.register
@google_benchmark.option.arg(1)
@google_benchmark.option.arg(10)
@google_benchmark.option.arg(1000)
@google_benchmark.option.arg(10**6)
def create_entity_data_slice(state):
  size = state.range(0)
  val_1 = kd.slice([12] * size)
  val_2 = kd.slice([13] * size)
  val_3 = kd.slice([14] * size)
  while state:
    db = kd.bag()
    _ = db.new(val_1=val_1, val_2=val_2, val_3=val_3)


@google_benchmark.register
def create_obj(state):
  val_1 = kd.slice(12)
  val_2 = kd.slice(13)
  val_3 = kd.slice(14)
  while state:
    db = kd.bag()
    _ = db.obj(val_1=val_1, val_2=val_2, val_3=val_3)


@google_benchmark.register
@google_benchmark.option.arg(1)
@google_benchmark.option.arg(10)
@google_benchmark.option.arg(1000)
@google_benchmark.option.arg(10**6)
def create_obj_data_slice(state):
  size = state.range(0)
  val_1 = kd.slice([12] * size)
  val_2 = kd.slice([13] * size)
  val_3 = kd.slice([14] * size)
  while state:
    db = kd.bag()
    _ = db.obj(val_1=val_1, val_2=val_2, val_3=val_3)


@google_benchmark.register
@google_benchmark.option.arg(1)
@google_benchmark.option.arg(10)
@google_benchmark.option.arg(1000)
@google_benchmark.option.arg(10**6)
def create_obj_shaped(state):
  size = state.range(0)
  val_1 = kd.slice([12] * size)
  val_2 = kd.slice([13] * size)
  val_3 = kd.slice([14] * size)
  shape = val_1.get_shape()
  while state:
    db = kd.bag()
    _ = db.obj_shaped(shape, val_1=val_1, val_2=val_2, val_3=val_3)


@google_benchmark.register
def get_attr_names_10000_obj(state):
  size = 10000
  val_1 = kd.slice([12] * size)
  val_2 = kd.slice([13] * size)
  val_3 = kd.slice([14] * size)
  db = kd.bag()
  ds = db.obj(val_1=val_1, val_2=val_2, val_3=val_3)
  while state:
    _ = ds.get_attr_names(intersection=True)


@google_benchmark.register
def set_get_attr_entity_with_merging(state):
  ds = kd.bag().new()
  val = kd.new(a=1)  # Non-empty DataBag.
  ds.get_schema().abc = val.get_schema().no_bag()
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    ds.with_bag(kd.bag()).set_attr('abc', val)


@google_benchmark.register
def set_get_attr_entity(state):
  ds = kd.bag().new()
  ds.get_schema().abc = kd.INT32
  val = kd.slice(12)
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    ds.abc = val
    _ = ds.abc


@google_benchmark.register
def set_get_attr_data_slice_single_entity(state):
  ds = kd.bag().new_shaped(kd.shapes.new([1]))
  ds.get_schema().abc = kd.INT32
  val = kd.slice([12])
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    ds.abc = val
    _ = ds.abc


@google_benchmark.register
def set_get_attr_10000_entity(state):
  ds = kd.bag().new_shaped(kd.shapes.new([10000]))
  ds.get_schema().abc = kd.INT32
  val = kd.slice([12] * 10000)
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    ds.abc = val
    _ = ds.abc


@google_benchmark.register
def set_get_attr_object(state):
  ds = kd.bag().obj()
  val = kd.slice(12)
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    ds.abc = val
    _ = ds.abc


@google_benchmark.register
def set_get_attr_data_slice_single_object(state):
  ds = kd.bag().obj_shaped(kd.shapes.new([1]))
  val = kd.slice([12])
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    ds.abc = val
    _ = ds.abc


@google_benchmark.register
def set_get_attr_10000_object(state):
  ds = kd.bag().obj_shaped(kd.shapes.new([10000]))
  val = kd.slice([12] * 10000)
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    ds.abc = val
    _ = ds.abc


@google_benchmark.register
def get_attr_expr(state):
  ds = kd.new(abc=kd.slice(12))
  expr = kde.get_attr(arolla.L.ds, arolla.L.attr_name)
  attr_abc = kd.item('abc')
  try:
    # To initialize all lazy initializers and reduce variance.
    _ = arolla.eval(expr, ds=ds, attr_name=kd.item('missing'))
  except ValueError:
    pass
  while state:
    _ = arolla.eval(expr, ds=ds, attr_name=attr_abc)


@google_benchmark.register
def get_attr_eager_op(state):
  ds = kd.new(abc=kd.slice(12))
  attr_abc = kd.item('abc')
  try:
    # To initialize all lazy initializers and reduce variance.
    kd.get_attr(ds, 'missing')
  except ValueError:
    pass
  while state:
    _ = kd.get_attr(ds, attr_abc)


@google_benchmark.register
def get_attr_eager(state):
  ds = kd.new(abc=kd.slice(12))
  try:
    # To initialize all lazy initializers and reduce variance.
    ds.missing
  except ValueError:
    pass
  while state:
    _ = ds.get_attr('abc')


@google_benchmark.register
def get_attr_native(state):
  ds = kd.new(abc=kd.slice(12))
  try:
    # To initialize all lazy initializers and reduce variance.
    ds.missing
  except ValueError:
    pass
  while state:
    _ = ds.abc


@google_benchmark.register
@google_benchmark.option.arg_names(['size'])
@google_benchmark.option.range(100, 10000)
def one_obj_per_big_alloc(state):
  size = state.range(0)
  alloc_size = 17
  objs = []
  schema = kd.uu_schema(a=kd.FLOAT32)
  for i in range(size):
    obj = schema(a=kd.slice([i] * alloc_size))
    objs.append(obj.S[0])
  ds = kd.slice(objs).fork_bag()
  while state:
    ds.a = ds.a + 1
    # Extraction of the half of the slice.
    new_obj = kd.obj(x=ds.S[size // 2:])
    del new_obj


@google_benchmark.register
def uuid_item(state):
  a = kd.slice(42)
  b = kd.slice('abc')
  c = kd.slice(3.14)
  while state:
    _ = kd.uuid(a=a, b=b, c=c)


@google_benchmark.register
def uuid_slice(state):
  a = kd.slice([42] * 10000)
  b = kd.slice(['abc'] * 10000)
  c = kd.slice([3.14] * 10000)
  while state:
    _ = kd.uuid(a=a, b=b, c=c)


@google_benchmark.register
def create_list(state):
  data = list(range(1000))
  while (state):
    _ = kd.list(data)


@google_benchmark.register
def create_list_shaped(state):
  data = [list(range(100))] * 10
  shape = kd.shapes.new([10])
  while (state):
    _ = kd.list_shaped(shape, data)


@google_benchmark.register
def create_dict(state):
  d = {'a': 42, 'b': 37, 'c': 12}
  while (state):
    _ = kd.dict(d)


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
def universal_converter_caching(state):
  d = {'abc': 42}
  for _ in range(10):
    # NOTE: Here we use the same instance `d` to make sure we re-use them from
    # cache and avoid duplicate computation on each level.
    d = {12: d, 42: d}
  while (state):
    _ = kd.from_py(d)


@google_benchmark.register
def universal_converter_caching_dict_as_obj(state):
  d = {'abc': 42}
  for _ in range(10):
    # NOTE: Here we use the same instance `d` to make sure we re-use them from
    # cache and avoid duplicate computation on each level.
    d = {'x': d, 'y': d}
  while (state):
    _ = kd.from_py(d, dict_as_obj=True)


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


@google_benchmark.register
def create_item(state):
  while state:
    _ = kd.item(42)


@google_benchmark.register
def create_item_through_slice(state):
  while state:
    _ = kd.slice(42)


@google_benchmark.register
def from_py(state):
  lst = [12] * 1000 + ['abc'] * 2000 + [b'abc'] * 3000 + [3.14] * 4000
  while state:
    _ = kd.slice(lst)


@google_benchmark.register
@google_benchmark.option.range(1000, 50000)
def from_py_big_text(state):
  text_size = state.range(0)
  lst = [str(i % 10) * text_size for i in range(10)]
  while state:
    _ = kd.slice(lst)


@google_benchmark.register
@google_benchmark.option.range(1000, 50000)
def from_py_big_text_and_big_slice(state):
  text_size = state.range(0)
  lst = [str(i % 10) * text_size for i in range(1000)]
  while state:
    _ = kd.slice(lst)


@google_benchmark.register
def from_py_with_casting(state):
  lst = [12] * 5000 + [3.14] * 5000
  while state:
    _ = kd.slice(lst)


@google_benchmark.register
def from_py_dtype(state):
  lst = [42] * 10000
  while state:
    _ = kd.slice(lst, kd.INT32)


@google_benchmark.register
def from_py_dtype_with_casting(state):
  lst = [42] * 10000
  while state:
    _ = kd.slice(lst, kd.FLOAT32)


@google_benchmark.register
def from_py_deep_nesting(state):
  lst = [3.14] * 4096  # 2 ^ 12 elements
  for _ in range(11):  # 12 levels-deep
    new_lst = []
    for i in range(0, len(lst), 2):
      new_lst.append([lst[i], lst[i + 1]])
    lst = new_lst
  while state:
    _ = kd.slice(lst)


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
def from_proto_0fields_single_empty(state):
  message = test_pb2.EmptyMessage()
  while state:
    _ = kd.from_proto(message)


@google_benchmark.register
def from_proto_100fields_single_empty(state):
  message = test_pb2.MessageD()
  while state:
    _ = kd.from_proto(message)


@google_benchmark.register
def from_proto_100fields_1k_empty(state):
  messages = [test_pb2.MessageD() for _ in range(1000)]
  while state:
    _ = kd.from_proto(messages)


@google_benchmark.register
def from_proto_1k_single_int32(state):
  messages = [test_pb2.MessageC(int32_field=i) for i in range(1000)]
  while state:
    _ = kd.from_proto(messages)


@google_benchmark.register
def from_proto_100_of_100_single_int32(state):
  messages = [
      test_pb2.MessageC(repeated_int32_field=[i] * 100) for i in range(100)
  ]
  while state:
    _ = kd.from_proto(messages)


@google_benchmark.register
def from_proto_1k_single_bytes(state):
  messages = [test_pb2.MessageC(bytes_field=b'a') for _ in range(1000)]
  while state:
    _ = kd.from_proto(messages)


@google_benchmark.register
def from_proto_100_single_10k_bytes(state):
  messages = [
      test_pb2.MessageC(bytes_field=b'a' * 10000) for _ in range(100)
  ]
  while state:
    _ = kd.from_proto(messages)


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
  schema = kd.schema.new_schema(int32_field=kd.INT32, db=kd.bag())
  schema.message_field = schema
  bag = kd.bag()
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
  schema = kd.schema.new_schema(int32_field=kd.INT32, db=kd.bag())
  schema.message_field = schema
  bag = kd.bag()
  x = kd.slice([bag.new(schema=schema) for _ in range(100)])

  x_leaf = x
  for i in range(100):
    x_leaf.int32_field = kd.int32(kd.range(100))
    x_leaf.message_field = kd.slice([kd.new(schema=schema) for _ in range(100)])
    x_leaf = x_leaf.message_field & (kd.index(x_leaf) > i)

  while state:
    _ = kd.to_proto(x, test_pb2.MessageC)


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
  schema = kd.schema.new_schema(int32_field=kd.INT32, db=kd.bag())
  schema.message_field = schema
  bag = kd.bag()
  x = kd.slice([bag.new(schema=schema) for _ in range(100)])

  x_leaf = x
  for _ in range(100):
    x_leaf.int32_field = kd.int32(kd.range(100))
    x_leaf.message_field = kd.slice([kd.new(schema=schema) for _ in range(100)])
    x_leaf = x_leaf.message_field

  while state:
    _ = kd.to_py(x)


@google_benchmark.register
def add_eager(state):
  x = kd.slice(1)
  y = kd.slice(2)
  # To initialize all lazy initializers and reduce variance.
  _ = kd.add(x, y)
  while state:
    kd.add(x, y)


@google_benchmark.register
def add_magic_method(state):
  x = kd.slice(1)
  y = kd.slice(2)
  # To initialize all lazy initializers and reduce variance.
  _ = x + y
  while state:
    _ = x + y


@google_benchmark.register
def add(state):
  x = kd.slice(1)
  y = kd.slice(2)
  # To initialize all lazy initializers and reduce variance.
  expr = kde.add(arolla.L.x, arolla.L.y)
  _ = arolla.eval(expr, x=x, y=y)
  while state:
    arolla.eval(expr, x=x, y=y)


@google_benchmark.register
def add_10000(state):
  x = kd.slice([1] * 10000)
  y = kd.slice([2] * 10000)
  # To initialize all lazy initializers and reduce variance.
  expr = kde.add(arolla.L.x, arolla.L.y)
  _ = arolla.eval(expr, x=x, y=y)
  while state:
    arolla.eval(expr, x=x, y=y)


@google_benchmark.register
def add_kd_eval(state):
  x = kd.slice([1])
  y = kd.slice([2])
  # To initialize all lazy initializers and reduce variance.
  expr = kde.add(I.x, I.y)
  _ = kd.eval(expr, x=x, y=y)
  while state:
    kd.eval(expr, x=x, y=y)


@google_benchmark.register
def add_10000_kd_eval(state):
  x = kd.slice([1] * 10000)
  y = kd.slice([2] * 10000)
  # To initialize all lazy initializers and reduce variance.
  expr = kde.add(I.x, I.y)
  _ = kd.eval(expr, x=x, y=y)
  while state:
    kd.eval(expr, x=x, y=y)


@google_benchmark.register
def add_kd_call(state):
  x = kd.slice([1])
  y = kd.slice([2])
  # To initialize all lazy initializers and reduce variance.
  fn = kdf.expr_fn(kde.add(I.x, I.y))
  _ = kd.call(fn, x=x, y=y)
  while state:
    kd.call(fn, x=x, y=y)


@google_benchmark.register
def add_10000_kd_call(state):
  x = kd.slice([1] * 10000)
  y = kd.slice([2] * 10000)
  # To initialize all lazy initializers and reduce variance.
  fn = kdf.expr_fn(kde.add(I.x, I.y))
  _ = kd.call(fn, x=x, y=y)
  while state:
    kd.call(fn, x=x, y=y)


@google_benchmark.register
def add_10000_mixed_rank(state):
  x = kd.slice([1] * 10000)
  y = kd.slice(2)
  # To initialize all lazy initializers and reduce variance.
  expr = kde.add(arolla.L.x, arolla.L.y)
  _ = arolla.eval(expr, x=x, y=y)
  while state:
    arolla.eval(expr, x=x, y=y)


@google_benchmark.register
def add_10000_rank_1_and_rank_2(state):
  x = kd.slice([1] * 1000)
  y = kd.slice([[1] * 10] * 1000)
  # To initialize all lazy initializers and reduce variance.
  expr = kde.add(arolla.L.x, arolla.L.y)
  _ = arolla.eval(expr, x=x, y=y)
  while state:
    arolla.eval(expr, x=x, y=y)


@google_benchmark.register
def kd_literal_creation(state):
  x = arolla.int32(1)
  # To initialize all lazy initializers and reduce variance.
  _ = kd.expr.literal(x)
  while state:
    kd.expr.literal(x)


@google_benchmark.register
def rl_literal_creation(state):
  x = arolla.int32(1)
  # To initialize all lazy initializers and reduce variance.
  _ = arolla.literal(x)
  while state:
    arolla.literal(x)


@google_benchmark.register
@google_benchmark.option.arg_names(['nrows', 'ncols', 'ndim'])
@google_benchmark.option.args([10, 10, -1])
@google_benchmark.option.args([10, 1000, -1])
@google_benchmark.option.args([1000, 10, -1])
@google_benchmark.option.args([1000, 1000, -1])
@google_benchmark.option.args([10, 10, 2])
@google_benchmark.option.args([10, 1000, 2])
@google_benchmark.option.args([1000, 10, 2])
@google_benchmark.option.args([1000, 1000, 2])
def agg_count(state):
  x = kd.slice([[1] * state.range(1)] * state.range(0))
  ndim = (
      arolla.unspecified() if state.range(2) == -1 else kd.slice(state.range(2))
  )
  # To initialize all lazy initializers and reduce variance.
  expr = kde.agg_count(arolla.L.x, arolla.L.ndim)
  _ = arolla.eval(expr, x=x, ndim=ndim)
  while state:
    arolla.eval(expr, x=x, ndim=ndim)


@google_benchmark.register
@google_benchmark.option.arg_names(['nrows', 'ncols', 'ndim'])
@google_benchmark.option.args([10, 10, -1])
@google_benchmark.option.args([10, 1000, -1])
@google_benchmark.option.args([1000, 10, -1])
@google_benchmark.option.args([1000, 1000, -1])
@google_benchmark.option.args([10, 10, 2])
@google_benchmark.option.args([10, 1000, 2])
@google_benchmark.option.args([1000, 10, 2])
@google_benchmark.option.args([1000, 1000, 2])
def agg_sum(state):
  x = kd.slice([[1] * state.range(1)] * state.range(0))
  ndim = (
      arolla.unspecified() if state.range(2) == -1 else kd.slice(state.range(2))
  )
  # To initialize all lazy initializers and reduce variance.
  expr = kde.agg_sum(arolla.L.x, arolla.L.ndim)
  _ = arolla.eval(expr, x=x, ndim=ndim)
  while state:
    arolla.eval(expr, x=x, ndim=ndim)


@google_benchmark.register
def translate(state):
  ds = kd.slice([[list(range(100))] * 10] * 10)
  keys_from = kd.bag().uuobj(x=ds)
  values_from = ds
  keys_to = kd.bag().uuobj(x=kd.slice([[list(range(0, 100, 2))] * 10] * 10))
  while state:
    _ = kd.translate(keys_to, keys_from, values_from)


@google_benchmark.register
def translate_group(state):
  keys_from = kd.bag().uuobj(x=kd.slice([[list(range(100)) * 3] * 10] * 10))
  values_from = kd.slice([[list(range(300))] * 10] * 10)
  keys_to = kd.bag().uuobj(x=kd.slice([[list(range(0, 100, 2))] * 10] * 10))
  while state:
    _ = kd.translate_group(keys_to, keys_from, values_from)


@google_benchmark.register
def extract_entity(state):
  db = kd.bag()
  ds = db.new(a=db.new(b=34), b=12)
  _ = kd.extract(ds)  # To initialize all lazy initializers and reduce variance.
  while state:
    _ = kd.extract(ds)


@google_benchmark.register
def extract_object(state):
  db = kd.bag()
  ds = db.obj(a=db.obj(b=12), b=34)
  _ = kd.extract(ds)  # To initialize all lazy initializers and reduce variance.
  while state:
    _ = kd.extract(ds)


@google_benchmark.register
def extract_10000_entity(state):
  db = kd.bag()
  ds = db.new(a=db.new(b=kd.slice([34] * 10000)), b=kd.slice([12] * 10000))
  _ = kd.extract(ds)  # To initialize all lazy initializers and reduce variance.
  while state:
    _ = kd.extract(ds)


@google_benchmark.register
def extract_10000_object(state):
  db = kd.bag()
  ds = db.obj(a=db.obj(b=kd.slice([34] * 10000)), b=kd.slice([12] * 10000))
  _ = kd.extract(ds)  # To initialize all lazy initializers and reduce variance.
  while state:
    _ = kd.extract(ds)
# pylint: enable=missing-function-docstring


@google_benchmark.register
def extract_depth_100_object(state):
  db = kd.bag()
  ds = kd.slice(1)
  for idx in range(100):
    ds = db.obj(id=kd.slice(idx), next=ds)
  _ = kd.extract(ds)  # To initialize all lazy initializers and reduce variance.
  while state:
    _ = kd.extract(ds)


@google_benchmark.register
@google_benchmark.option.arg_names(['nattrs', 'same_name'])
@google_benchmark.option.args([1, False])
@google_benchmark.option.args([10, False])
@google_benchmark.option.args([100, False])
@google_benchmark.option.args([1000, False])
@google_benchmark.option.args([1, True])
@google_benchmark.option.args([10, True])
@google_benchmark.option.args([100, True])
@google_benchmark.option.args([1000, True])
def set_get_multiple_attrs_entity(state):
  """Benchmark for setting and getting multiple attributes."""
  ds = kd.bag().new()
  ds.get_schema().abc = kd.INT32
  val = kd.slice(12)
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_bag(kd.bag())
    for i in range(state.range(0)):
      arg_name = 'abc' if state.range(1) else f'abc{i}'
      merged_ds.set_attr(arg_name, val)
      _ = getattr(merged_ds, arg_name)


@google_benchmark.register
@google_benchmark.option.arg_names(['nattrs', 'same_name'])
@google_benchmark.option.args([1, False])
@google_benchmark.option.args([10, False])
@google_benchmark.option.args([100, False])
@google_benchmark.option.args([1000, False])
@google_benchmark.option.args([1, True])
@google_benchmark.option.args([10, True])
@google_benchmark.option.args([100, True])
@google_benchmark.option.args([1000, True])
def set_get_multiple_attrs_entity_with_merging(state):
  """Benchmark for setting and getting multiple attributes with merging."""
  ds = kd.bag().new()
  val = kd.new(a=1)  # Non-empty DataBag.
  ds.get_schema().abc = val.get_schema().no_bag()
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_bag(kd.bag())
    for i in range(state.range(0)):
      arg_name = 'abc' if state.range(1) else f'abc{i}'
      merged_ds.set_attr(arg_name, val)
      _ = getattr(merged_ds, arg_name)


@google_benchmark.register
@google_benchmark.option.arg_names(['nattrs', 'same_name'])
@google_benchmark.option.args([1, False])
@google_benchmark.option.args([10, False])
@google_benchmark.option.args([100, False])
@google_benchmark.option.args([1000, False])
@google_benchmark.option.args([1, True])
@google_benchmark.option.args([10, True])
@google_benchmark.option.args([100, True])
@google_benchmark.option.args([1000, True])
def set_get_multiple_attrs_entity_via_extract_plus_merge(state):
  """Benchmark for setting and getting multiple attributes via extract+merge."""
  ds = kd.bag().new()
  ds.get_schema().abc = kd.INT32
  val = kd.slice(12)
  updates = []
  for i in range(state.range(0)):
    update_ds = ds.with_bag(kd.bag())
    arg_name = 'abc' if state.range(1) else f'abc{i}'
    update_ds.set_attr(arg_name, val)
    updates.append((arg_name, update_ds))
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_bag(kd.bag())
    for arg_name, update_ds in updates:
      merged_ds.get_bag().merge_inplace(kd.extract(update_ds).get_bag())
      _ = getattr(merged_ds, arg_name)


@google_benchmark.register
@google_benchmark.option.arg_names(['nattrs', 'same_name'])
@google_benchmark.option.args([1, False])
@google_benchmark.option.args([10, False])
@google_benchmark.option.args([100, False])
@google_benchmark.option.args([1000, False])
@google_benchmark.option.args([1, True])
@google_benchmark.option.args([10, True])
@google_benchmark.option.args([100, True])
@google_benchmark.option.args([1000, True])
def set_get_multiple_attrs_entity_via_fallback(state):
  """Benchmark for setting and getting multiple attributes via fallback."""
  ds = kd.bag().new()
  ds.get_schema().abc = kd.INT32
  val = kd.slice(12)
  updates = []
  for i in range(state.range(0)):
    update_ds = ds.with_bag(kd.bag())
    arg_name = 'abc' if state.range(1) else f'abc{i}'
    update_ds.set_attr(arg_name, val)
    updates.append((arg_name, update_ds))
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_bag(kd.bag())
    for arg_name, update_ds in updates:
      merged_ds = update_ds.enriched(merged_ds.get_bag())
      _ = getattr(merged_ds, arg_name)


@google_benchmark.register
@google_benchmark.option.arg_names(['nattrs', 'same_name'])
@google_benchmark.option.args([1, False])
@google_benchmark.option.args([10, False])
@google_benchmark.option.args([100, False])
@google_benchmark.option.args([1000, False])
@google_benchmark.option.args([1, True])
@google_benchmark.option.args([10, True])
@google_benchmark.option.args([100, True])
@google_benchmark.option.args([1000, True])
def set_get_multiple_attrs_10000_entity(state):
  """Benchmark for setting and getting multiple attributes."""
  ds = kd.bag().new_shaped(kd.shapes.new([10000]))
  ds.get_schema().abc = kd.INT32
  val = kd.slice([12] * 10000)
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_bag(kd.bag())
    for i in range(state.range(0)):
      arg_name = 'abc' if state.range(1) else f'abc{i}'
      merged_ds.set_attr(arg_name, val)
      _ = getattr(merged_ds, arg_name)


@google_benchmark.register
@google_benchmark.option.arg_names(['nattrs', 'same_name'])
@google_benchmark.option.args([1, False])
@google_benchmark.option.args([10, False])
@google_benchmark.option.args([100, False])
@google_benchmark.option.args([1000, False])
@google_benchmark.option.args([1, True])
@google_benchmark.option.args([10, True])
@google_benchmark.option.args([100, True])
@google_benchmark.option.args([1000, True])
def set_get_multiple_attrs_10000_entity_with_merging(state):
  """Benchmark for setting and getting multiple attributes with merging."""
  ds = kd.bag().new_shaped(kd.shapes.new([10000]))
  val = kd.bag().new_shaped(kd.shapes.new([10000]))
  val.set_attr('a', 1)
  ds.get_schema().abc = val.get_schema().no_bag()
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_bag(kd.bag())
    for i in range(state.range(0)):
      arg_name = 'abc' if state.range(1) else f'abc{i}'
      merged_ds.set_attr(arg_name, val)
      _ = getattr(merged_ds, arg_name)


@google_benchmark.register
@google_benchmark.option.arg_names(['nattrs', 'same_name'])
@google_benchmark.option.args([1, False])
@google_benchmark.option.args([10, False])
@google_benchmark.option.args([100, False])
@google_benchmark.option.args([1000, False])
@google_benchmark.option.args([1, True])
@google_benchmark.option.args([10, True])
@google_benchmark.option.args([100, True])
@google_benchmark.option.args([1000, True])
def set_get_multiple_attrs_10000_entity_via_extract_plus_merge(state):
  """Benchmark for setting and getting multiple attributes via extract+merge."""
  ds = kd.bag().new_shaped(kd.shapes.new([10000]))
  ds.get_schema().abc = kd.INT32
  val = kd.slice([12] * 10000)
  updates = []
  for i in range(state.range(0)):
    update_ds = ds.with_bag(kd.bag())
    arg_name = 'abc' if state.range(1) else f'abc{i}'
    update_ds.set_attr(arg_name, val)
    updates.append((arg_name, update_ds))
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_bag(kd.bag())
    for arg_name, update_ds in updates:
      merged_ds.get_bag().merge_inplace(kd.extract(update_ds).get_bag())
      _ = getattr(merged_ds, arg_name)


@google_benchmark.register
@google_benchmark.option.arg_names(['nattrs', 'same_name'])
@google_benchmark.option.args([1, False])
@google_benchmark.option.args([10, False])
@google_benchmark.option.args([100, False])
@google_benchmark.option.args([1000, False])
@google_benchmark.option.args([1, True])
@google_benchmark.option.args([10, True])
@google_benchmark.option.args([100, True])
@google_benchmark.option.args([1000, True])
def set_get_multiple_attrs_10000_entity_via_fallback(state):
  """Benchmark for setting and getting multiple attributes via fallback."""
  ds = kd.bag().new_shaped(kd.shapes.new([10000]))
  ds.get_schema().abc = kd.INT32
  val = kd.slice([12] * 10000)
  updates = []
  for i in range(state.range(0)):
    update_ds = ds.with_bag(kd.bag())
    arg_name = 'abc' if state.range(1) else f'abc{i}'
    update_ds.set_attr(arg_name, val)
    updates.append((arg_name, update_ds))
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_bag(kd.bag())
    for arg_name, update_ds in updates:
      merged_ds = update_ds.enriched(merged_ds.get_bag())
      _ = getattr(merged_ds, arg_name)


@google_benchmark.register
@google_benchmark.option.arg_names(['nvars'])
@google_benchmark.option.args([1])
@google_benchmark.option.args([10])
@google_benchmark.option.args([100])
@google_benchmark.option.args([1000])
def kd_call_with_many_variables(state):
  """Benchmark for a functor with a long variable chain."""
  nvars = state.range(0)
  vars_ = {f'v{i}': V[f'v{i+1}'] for i in range(nvars - 1)}
  vars_[f'v{nvars-1}'] = S
  fn = kdf.expr_fn(V.v0, **vars_)
  # To initialize all lazy initializers and reduce variance.
  _ = kd.call(fn, 57)
  while state:
    kd.call(fn, 57)


def _create_large_expr():
  expr = I.x
  for i in range(100):
    for _ in range(100):
      expr += kd.slice([1, 2, 3])
    expr = arolla.M.annotation.name(expr, f'step{i}')
  return expr


@google_benchmark.register
def create_large_expr(state):
  # To initialize all lazy initializers and reduce variance.
  _create_large_expr()
  while state:
    _create_large_expr()


@google_benchmark.register
def create_large_functor(state):
  # To initialize all lazy initializers and reduce variance.
  expr = _create_large_expr()
  _ = kdf.expr_fn(expr)
  while state:
    expr = _create_large_expr()
    _ = kdf.expr_fn(expr)


@google_benchmark.register
def create_large_functor_auto_variables(state):
  # To initialize all lazy initializers and reduce variance.
  expr = _create_large_expr()
  _ = kdf.expr_fn(expr, auto_variables=True)
  while state:
    expr = _create_large_expr()
    _ = kdf.expr_fn(expr, auto_variables=True)


@google_benchmark.register
@google_benchmark.option.arg_names(['fallback_count', 'same_attr_name'])
@google_benchmark.option.args([1, False])
@google_benchmark.option.args([10, False])
@google_benchmark.option.args([100, False])
@google_benchmark.option.args([1000, False])
@google_benchmark.option.args([1, True])
@google_benchmark.option.args([10, True])
@google_benchmark.option.args([100, True])
@google_benchmark.option.args([1000, True])
def databag_repr_fallbacks(state):
  """Benchmark repr for a DataBag with many fallbacks."""
  fallback_count = state.range(0)
  e = kd.new_like(kd.slice([1, 2, 3, 4, 5, 6, 7]))
  for i in range(fallback_count):
    attr_name = 'abc' if state.range(1) else f'abc{i}'
    e = kd.with_attr(e, attr_name, kd.slice([1, 2, 3, 4, 5, 6, 7]))
  while state:
    _ = repr(e.get_bag())


@google_benchmark.register
@google_benchmark.option.arg_names(
    ['sorted_indices', 'use_slices', 'num_dims', 'dims_to_slice']
)
@google_benchmark.option.args([True, False, 5, 5])
@google_benchmark.option.args([False, False, 5, 5])
@google_benchmark.option.args([True, True, 5, 5])
@google_benchmark.option.args([False, False, 5, 1])
@google_benchmark.option.args([False, False, 1, 1])
def subslice(state):
  """Benchmark ds.S."""
  sorted_indices = state.range(0)
  use_slices = state.range(1)
  num_dims = state.range(2)
  dims_to_slice = state.range(3)
  if num_dims == 1:
    min_size = 10**5
    max_size = min_size
  else:
    min_size = 10
    max_size = 20
  ds = kd.present
  # Create random ragged data.
  for step in range(num_dims):
    ds = ds.repeat(kd.randint_like(ds, min_size, max_size + 1, seed=step))
  # Replace repeated present with random values.
  ds = kd.randint_like(ds, 1, 100, seed=57)
  # Choose random indices to take.
  indices = []
  chosen = kd.implode(ds, ndim=dims_to_slice)
  for step in range(dims_to_slice):
    if use_slices:
      take = slice(1, -1)
    else:
      available = kd.range(kd.list_size(chosen))
      take = kd.select(
          available, kd.randint_like(available, 0, 2, seed=100 + step) == 0
      )
      if not sorted_indices:
        # Shuffle.
        take = kd.sort(take, kd.randint_like(take, 1, 1000, seed=200 + step))
    indices.append(take)
    chosen = chosen[take]
  while state:
    _ = ds.S[*indices]


@google_benchmark.register
@google_benchmark.option.arg_names(['ndim'])
@google_benchmark.option.args([0])
@google_benchmark.option.args([1])
@google_benchmark.option.args([2])
@google_benchmark.option.args([3])
def expand_to(state):
  """Benchmark for kd.expand_to."""
  ndim = state.range(0)
  ds = kd.present
  min_size = 5
  max_size = 10
  for step in range(3):
    ds = ds.repeat(kd.randint_like(ds, min_size, max_size + 1, seed=step))
  child_ds = ds
  for step in range(2):
    child_ds = child_ds.repeat(
        kd.randint_like(ds, min_size, max_size + 1, seed=10 + step)
    )
  # Replace repeated present with random values.
  ds = kd.randint_like(ds, 1, 100, seed=57)
  while state:
    _ = ds.expand_to(child_ds, ndim=ndim)


@google_benchmark.register
def map_py(state):
  """Benchmark for kd.map_py."""
  x = kd.range(int(1e3))
  fn = lambda x: x + 1
  while state:
    _ = kd.map_py(fn, x)


@google_benchmark.register
def map_with_traced_fn(state):
  """Benchmark for kd.map with a traced fn inside."""
  x = kd.range(int(1e3))
  fn = kd.fn(lambda x: x + 1)
  while state:
    _ = kd.map(fn, x)


@google_benchmark.register
def map_with_py_fn(state):
  """Benchmark for kd.map with a py_fn inside."""
  x = kd.range(int(1e3))
  fn = kd.py_fn(lambda x: x + 1)
  while state:
    _ = kd.map(fn, x)


if __name__ == '__main__':
  google_benchmark.main()
