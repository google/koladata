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

"""Misc Koladata benchmarks."""

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
@google_benchmark.option.arg_names(['size', 'fallback_count'])
@google_benchmark.option.ranges([(1, 10000), (0, 16)])
def get_from_dict(state):
  """Benchmark for setting and getting dict key with fallback."""
  size = state.range(0)
  fallback_count = state.range(1)
  ds = kd.dict_shaped(kd.shapes.new([size]))
  for fb_id in range(fallback_count):
    ds = ds.with_dict_update(
        kd.slice([
            'abc' if i % fallback_count == fb_id else None for i in range(size)
        ]),
        kd.slice([fb_id] * size),
    )
  while state:
    _ = ds['abc']


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


@google_benchmark.register
def kd_expr_fn(state):
  while state:
    fn = kdf.expr_fn(
        V.x + V.y + V.z,
        x=V.y * 2,
        y=kdi.float32(2.0),
        z=V.y * V.x,
        auto_variables=True,
    )
    del fn


@kd.trace_as_fn()
def kd_is_prime(n):
  k = kd.while_(
      lambda n, returns: (n % returns != 0) & (returns * returns <= n),
      lambda n, returns: kd.namedtuple(
          returns=returns + 2
      ),
      n=n,
      returns=3
    )
  return (n >= 2) & ((n == 2) | (n % 2 != 0)) & (k * k > n)


@google_benchmark.register
def kd_functor_is_prime(state):
  while state:
    kd_is_prime(998244353)


if __name__ == '__main__':
  google_benchmark.main()
