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

"""Koladata benchmarks related to setting and getting attributes."""

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


if __name__ == '__main__':
  google_benchmark.main()
