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

"""Koladata benchmarks related to object creation."""

import random

from arolla import arolla
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
def create_list(state):
  data = list(range(1000))
  while (state):
    _ = kd.list(data)


@google_benchmark.register
def create_dict(state):
  d = {'a': 42, 'b': 37, 'c': 12}
  while (state):
    _ = kd.dict(d)


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
    db = kd.mutable_bag()
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
    db = kd.mutable_bag()
    _ = db.new(val_1=val_1, val_2=val_2, val_3=val_3)


@google_benchmark.register
def create_obj(state):
  val_1 = kd.slice(12)
  val_2 = kd.slice(13)
  val_3 = kd.slice(14)
  while state:
    db = kd.mutable_bag()
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
    db = kd.mutable_bag()
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
    db = kd.mutable_bag()
    _ = db.obj_shaped(shape, val_1=val_1, val_2=val_2, val_3=val_3)


@google_benchmark.register
@google_benchmark.option.arg_names(['size'])
@google_benchmark.option.range(100, 10000)
def one_obj_per_big_alloc(state):
  size = state.range(0)
  alloc_size = 17
  objs = []
  schema = kd.uu_schema(a=kd.FLOAT32)
  for i in range(size):
    obj = kd.new(a=kd.slice([i] * alloc_size), schema=schema)
    objs.append(obj.S[0])
  random.shuffle(objs)
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
def create_list_shaped(state):
  data = kd.slice([list(range(100))] * 10)
  shape = kd.shapes.new([10])
  while (state):
    _ = kd.list_shaped(shape, data)


@google_benchmark.register
@google_benchmark.option.arg_names(['nattrs'])
@google_benchmark.option.args([1])
@google_benchmark.option.args([10])
@google_benchmark.option.args([100])
@google_benchmark.option.args([1000])
def kd_new_10000_with_whole_value(state):
  """Benchmark for kd.new that has whole value."""
  size = 10000
  attr_count = state.range(0)
  ds = kd.new(**{
      f'abc{i}': kd.slice([i] * size)
      for i in range(attr_count)
  })
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except AttributeError:
    pass
  while state:
    new_ds = kd.new(x=ds)
    del new_ds


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

if __name__ == '__main__':
  google_benchmark.main()
