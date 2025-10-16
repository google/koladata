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

r"""Benchmarks for Konstructs.

To profile locally:

blaze run --config=benchmark --copt=-DMAX_PROFILE_STACK_DEPTH_SIZE=1024 \
    //py/koladata/ext/konstructs:benchmarks -- \
    --benchmark_filter=example_computation_ks/dataset:0 \
    --benchmark_min_time=3s --cpu_profile=/tmp/prof && pprof --flame /tmp/prof

To compare against checked-in state:

benchy --reference=srcfs //py/koladata/ext/konstructs:benchmarks
"""

import dataclasses
import google_benchmark
from koladata import kd_ext

ks = kd_ext.konstructs


@dataclasses.dataclass
class Foo:
  x: int


@dataclasses.dataclass
class Bar:
  z: list[Foo]


def _example_computation_py(os: list[Bar]) -> list[list[int]]:
  return [[f.x * f.x for f in o.z] for o in os]


def _example_computation_py_stepwise(os: list[Bar]) -> list[list[int]]:
  # This mirrors the operations we'd likely have to do if we had a Python-only
  # implementation of Lens.
  os = list(os)
  zs = [o.z for o in os]
  fs = [list(z) for z in zs]
  xs = [[f.x for f in fl] for fl in fs]
  res = [[x * x for x in xl] for xl in xs]
  return res


def _example_computation_ks(os: list[Bar]) -> list[list[int]]:
  l = ks.lens(os)
  o = l[:].z[:].x
  return ks.map(lambda x: x * x, o).get()


def _very_small_dataset() -> list[Bar]:
  return [Bar(z=[Foo(x=57)])]


def _small_dataset() -> list[Bar]:
  return [Bar(z=[Foo(x=i + j) for i in range(3)]) for j in range(4)]


def _medium_dataset() -> list[Bar]:
  return [Bar(z=[Foo(x=i + j) for i in range(100)]) for j in range(100)]


_DATASET_CHOICES = (_very_small_dataset, _small_dataset, _medium_dataset)


@google_benchmark.register
@google_benchmark.option.arg_names(['dataset'])
@google_benchmark.option.dense_range(0, len(_DATASET_CHOICES) - 1)
def example_computation_py(state):
  """An example simple computation."""
  dataset_fn = _DATASET_CHOICES[state.range(0)]
  ds = dataset_fn()
  # Sanity check
  assert _example_computation_py(ds) == _example_computation_ks(ds)
  while state:
    _ = _example_computation_py(ds)


@google_benchmark.register
@google_benchmark.option.arg_names(['dataset'])
@google_benchmark.option.dense_range(0, len(_DATASET_CHOICES) - 1)
def example_computation_py_stepwise(state):
  """An example simple computation."""
  dataset_fn = _DATASET_CHOICES[state.range(0)]
  ds = dataset_fn()
  # Sanity check
  assert _example_computation_py_stepwise(ds) == _example_computation_ks(ds)
  while state:
    _ = _example_computation_py_stepwise(ds)


@google_benchmark.register
@google_benchmark.option.arg_names(['dataset'])
@google_benchmark.option.dense_range(0, len(_DATASET_CHOICES) - 1)
def example_computation_ks(state):
  """An example simple computation."""
  dataset_fn = _DATASET_CHOICES[state.range(0)]
  ds = dataset_fn()
  # Sanity check
  assert _example_computation_py(ds) == _example_computation_ks(ds)
  while state:
    _ = _example_computation_ks(ds)


if __name__ == '__main__':
  google_benchmark.main()
