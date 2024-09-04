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

I = kd.I
V = kd.V
S = kd.S
kde = kd.kde
kdf = kd.kdf


# pylint: disable=missing-function-docstring
@google_benchmark.register
def create_empty_data_bag(state):
  while state:
    _ = kd.bag()


@google_benchmark.register
def create_entity(state):
  val_1 = kd.slice(12)
  val_2 = kd.slice(13)
  val_3 = kd.slice(14)
  db = kd.bag()
  while state:
    _ = db.new(val_1=val_1, val_2=val_2, val_3=val_3)


@google_benchmark.register
def set_get_attr_entity_with_merging(state):
  ds = kd.new()
  val = kd.new(a=1)  # Non-empty DataBag.
  ds.get_schema().abc = val.get_schema().with_db(None)
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    ds.with_db(kd.bag()).set_attr('abc', val, update_schema=True)


@google_benchmark.register
def set_get_attr_entity(state):
  ds = kd.new()
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
  ds = kd.bag().new_shaped(kd.shapes.create([1]))
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
  ds = kd.bag().new_shaped(kd.shapes.create([10000]))
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
  ds = kd.obj()
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
  ds = kd.bag().obj_shaped(kd.shapes.create([1]))
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
  ds = kd.bag().obj_shaped(kd.shapes.create([10000]))
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
  shape = kd.shapes.create([10])
  while (state):
    _ = kd.list_shaped(shape, data)


@google_benchmark.register
def create_dict(state):
  d = {'a': 42, 'b': 37, 'c': 12}
  while (state):
    _ = kd.dict(d)


@google_benchmark.register
def universal_converter(state):
  d = {'abc': 42}
  for _ in range(10):
    d = {12: d, 42: d}
  obj = kd.obj()
  while (state):
    obj.d = d


@google_benchmark.register
def deep_universal_converter(state):
  d = {'abc': 42}
  for _ in range(1000):
    d = {12: d.copy()}
  obj = kd.obj()
  while (state):
    obj.d = d


@google_benchmark.register
def universal_converter_list(state):
  l = [1, 2, 3]
  for _ in range(10):
    l = [copy.deepcopy(l), copy.deepcopy(l), copy.deepcopy(l)]
  obj = kd.obj()
  while (state):
    obj.l = l


@google_benchmark.register
def from_py(state):
  lst = [12] * 1000 + ['abc'] * 2000 + [b'abc'] * 3000 + [3.14] * 4000
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
  fn = kdf.fn(kde.add(I.x, I.y))
  _ = kd.call(fn, x=x, y=y)
  while state:
    kd.call(fn, x=x, y=y)


@google_benchmark.register
def add_10000_kd_call(state):
  x = kd.slice([1] * 10000)
  y = kd.slice([2] * 10000)
  # To initialize all lazy initializers and reduce variance.
  fn = kdf.fn(kde.add(I.x, I.y))
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
  _ = kd.literal(x)
  while state:
    kd.literal(x)


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
  ds = kd.new()
  ds.get_schema().abc = kd.INT32
  val = kd.slice(12)
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_db(kd.bag())
    for i in range(state.range(0)):
      arg_name = 'abc' if state.range(1) else f'abc{i}'
      merged_ds.set_attr(arg_name, val, update_schema=True)
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
  ds = kd.new()
  val = kd.new(a=1)  # Non-empty DataBag.
  ds.get_schema().abc = val.get_schema().with_db(None)
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_db(kd.bag())
    for i in range(state.range(0)):
      arg_name = 'abc' if state.range(1) else f'abc{i}'
      merged_ds.set_attr(arg_name, val, update_schema=True)
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
    update_ds = ds.with_db(kd.bag())
    arg_name = 'abc' if state.range(1) else f'abc{i}'
    update_ds.set_attr(arg_name, val, update_schema=True)
    updates.append((arg_name, update_ds))
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_db(kd.bag())
    for arg_name, update_ds in updates:
      merged_ds.db.merge_inplace(kd.extract(update_ds).db)
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
    update_ds = ds.with_db(kd.bag())
    arg_name = 'abc' if state.range(1) else f'abc{i}'
    update_ds.set_attr(arg_name, val, update_schema=True)
    updates.append((arg_name, update_ds))
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_db(kd.bag())
    for arg_name, update_ds in updates:
      merged_ds = update_ds.with_fallback(merged_ds.db)
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
  ds = kd.bag().new_shaped(kd.shapes.create([10000]))
  ds.get_schema().abc = kd.INT32
  val = kd.slice([12] * 10000)
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_db(kd.bag())
    for i in range(state.range(0)):
      arg_name = 'abc' if state.range(1) else f'abc{i}'
      merged_ds.set_attr(arg_name, val, update_schema=True)
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
  ds = kd.bag().new_shaped(kd.shapes.create([10000]))
  val = kd.bag().new_shaped(kd.shapes.create([10000]))
  val.set_attr('a', 1, update_schema=True)
  ds.get_schema().abc = val.get_schema().with_db(None)
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_db(kd.bag())
    for i in range(state.range(0)):
      arg_name = 'abc' if state.range(1) else f'abc{i}'
      merged_ds.set_attr(arg_name, val, update_schema=True)
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
  ds = kd.bag().new_shaped(kd.shapes.create([10000]))
  ds.get_schema().abc = kd.INT32
  val = kd.slice([12] * 10000)
  updates = []
  for i in range(state.range(0)):
    update_ds = ds.with_db(kd.bag())
    arg_name = 'abc' if state.range(1) else f'abc{i}'
    update_ds.set_attr(arg_name, val, update_schema=True)
    updates.append((arg_name, update_ds))
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_db(kd.bag())
    for arg_name, update_ds in updates:
      merged_ds.db.merge_inplace(kd.extract(update_ds).db)
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
  ds = kd.bag().new_shaped(kd.shapes.create([10000]))
  ds.get_schema().abc = kd.INT32
  val = kd.slice([12] * 10000)
  updates = []
  for i in range(state.range(0)):
    update_ds = ds.with_db(kd.bag())
    arg_name = 'abc' if state.range(1) else f'abc{i}'
    update_ds.set_attr(arg_name, val, update_schema=True)
    updates.append((arg_name, update_ds))
  try:
    _ = ds.missing  # To initialize all lazy initializers and reduce variance.
  except ValueError:
    pass
  while state:
    merged_ds = ds.with_db(kd.bag())
    for arg_name, update_ds in updates:
      merged_ds = update_ds.with_fallback(merged_ds.db)
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
  fn = kdf.fn(V.v0, **vars_)
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
  _ = kdf.fn(expr)
  while state:
    expr = _create_large_expr()
    _ = kdf.fn(expr)


@google_benchmark.register
def create_large_functor_auto_variables(state):
  # To initialize all lazy initializers and reduce variance.
  expr = _create_large_expr()
  _ = kdf.fn(expr, auto_variables=True)
  while state:
    expr = _create_large_expr()
    _ = kdf.fn(expr, auto_variables=True)


if __name__ == '__main__':
  google_benchmark.main()
