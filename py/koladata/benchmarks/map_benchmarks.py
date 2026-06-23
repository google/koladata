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

"""Benchmarks comparing kd.map vs kd.map_reduce_update for update operations.

The key scenario: entities with many attributes (big schema). kd.map returns
DataSlices containing updated entities (each with its own DataBag), and stacking
them involves quadratic merging. kd.map_reduce_update returns DataBags directly
and
merges them incrementally, avoiding the quadratic cost.
"""

import google_benchmark
from koladata import kd

kdf = kd.functor


def _create_entities(n_items, n_attrs):
  """Creates entities with n_attrs attributes (immutable bag)."""
  kwargs = {f'attr_{i}': kd.slice([i] * n_items) for i in range(n_attrs)}
  return kd.new(**kwargs)


def _make_map_reduce_update_fn():
  """Creates a functor that sets one attribute and returns a DataBag."""
  return kdf.fn(
      lambda x: kd.attrs(x, new_attr=x.attr_0 + 1),
      use_tracing=False,
      return_type_as=kd.types.DataBag,
  )


def _make_map_fn():
  """Creates a functor that sets one attribute and returns the entity."""
  return kdf.fn(
      lambda x: kd.with_attrs(x, new_attr=x.attr_0 + 1),
      use_tracing=False,
  )


# ---- Varying n_items with a fixed schema (100 attrs) ----


@google_benchmark.register
@google_benchmark.option.arg_names(['n_items'])
@google_benchmark.option.args([10])
@google_benchmark.option.args([100])
@google_benchmark.option.args([1000])
def map_reduce_update_varying_items(state):
  """kd.map_reduce_update with 100 attrs, varying item count."""
  n_items = state.range(0)
  x = _create_entities(n_items, n_attrs=100)
  fn = _make_map_reduce_update_fn()
  _ = x.updated(kd.map_reduce_update(fn, x=x))  # Warm up.
  while state:
    _ = x.updated(kd.map_reduce_update(fn, x=x))


@google_benchmark.register
@google_benchmark.option.arg_names(['n_items'])
@google_benchmark.option.args([10])
@google_benchmark.option.args([100])
@google_benchmark.option.args([1000])
def map_with_attr_varying_items(state):
  """kd.map returning with_attrs entity with 100 attrs, varying item count."""
  n_items = state.range(0)
  x = _create_entities(n_items, n_attrs=100)
  fn = _make_map_fn()
  _ = kd.map(fn, x=x)  # Warm up.
  while state:
    _ = kd.map(fn, x=x)


# ---- Varying n_attrs (schema size) with a fixed item count (100) ----


@google_benchmark.register
@google_benchmark.option.arg_names(['n_attrs'])
@google_benchmark.option.args([10])
@google_benchmark.option.args([100])
@google_benchmark.option.args([1000])
def map_reduce_update_varying_attrs(state):
  """kd.map_reduce_update with 100 items, varying schema size."""
  n_attrs = state.range(0)
  x = _create_entities(100, n_attrs)
  fn = _make_map_reduce_update_fn()
  _ = x.updated(kd.map_reduce_update(fn, x=x))  # Warm up.
  while state:
    _ = x.updated(kd.map_reduce_update(fn, x=x))


@google_benchmark.register
@google_benchmark.option.arg_names(['n_attrs'])
@google_benchmark.option.args([10])
@google_benchmark.option.args([100])
@google_benchmark.option.args([1000])
def map_with_attr_varying_attrs(state):
  """kd.map returning with_attrs entity with 100 items, varying schema size."""
  n_attrs = state.range(0)
  x = _create_entities(100, n_attrs)
  fn = _make_map_fn()
  _ = kd.map(fn, x=x)  # Warm up.
  while state:
    _ = kd.map(fn, x=x)


if __name__ == '__main__':
  google_benchmark.main()
