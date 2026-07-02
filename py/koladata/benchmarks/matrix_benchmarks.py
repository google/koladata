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

r"""Benchmarks comparing NumPy vs Koda for kd.matrix.* operators.

NumPy cannot natively perform vectorized batch operations on matrices of
different sizes, so the NumPy baseline uses Python loops over the batch.
Koda handles jagged batches natively via kd.matrix operators.

To run locally:

  blaze run --config=benchmark \
      //py/koladata/benchmarks:matrix_benchmarks -- \
      --benchmark_filter=<filter>

To compare against checked-in state:

  benchy --reference=srcfs \
      //py/koladata/benchmarks:matrix_benchmarks
"""

import random

import google_benchmark
from koladata import kd
import numpy as np

BATCH_SIZE = 100
UNIFORM_SIZE = 50
JAGGED_SIZES = [10, 20, 30, 50, 80]

_BATCH_MODE_NAMES = ['uniform', 'jagged']


def _seed_random_number_generators():
  random.seed(42)
  np.random.seed(42)


def _make_jagged_sizes():
  return [random.choice(JAGGED_SIZES) for _ in range(BATCH_SIZE)]


def _make_uniform_matrices_np():
  return np.random.randn(BATCH_SIZE, UNIFORM_SIZE, UNIFORM_SIZE)


def _make_jagged_matrices_np(sizes):
  return [np.random.randn(s, s) for s in sizes]


def _np_to_kd_matrices(mats_np):
  if isinstance(mats_np, np.ndarray):
    return kd.slice(mats_np.tolist())
  return kd.slice([m.tolist() for m in mats_np])


# Switch off docstring lint checks for the benchmark functions below.
# pylint: disable=missing-function-docstring

# ---- transpose ----


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def numpy_transpose(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    a_np = _make_uniform_matrices_np()
  else:
    sizes = _make_jagged_sizes()
    a_np = _make_jagged_matrices_np(sizes)
  while state:
    for i in range(BATCH_SIZE):
      a_np[i].T.copy()


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def koda_transpose(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    a_np = _make_uniform_matrices_np()
  else:
    sizes = _make_jagged_sizes()
    a_np = _make_jagged_matrices_np(sizes)
  a_kd = _np_to_kd_matrices(a_np)
  while state:
    kd.matrix.transpose(a_kd)


# At the time of writing, implementing kd.matrix.transpose via a lambda
# with kd.group_by and kd.index as shown below is 6.3x slower on uniform and
# 6.6x slower on jagged compared to the C++ implementation of
# kd.matrix.transpose. Should these numbers change significantly in the future,
# we might consider making kd.matrix.transpose a lambda operator, but for the
# time being we keep it as a C++ backend operator for performance reasons.
@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def koda_transpose_lambda(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    a_np = _make_uniform_matrices_np()
  else:
    sizes = _make_jagged_sizes()
    a_np = _make_jagged_matrices_np(sizes)
  a_kd = _np_to_kd_matrices(a_np)
  while state:
    kd.group_by(
        a_kd.flatten(-2),
        kd.index(kd.present_shaped_as(a_kd)).flatten(-2),
    )


if __name__ == '__main__':
  google_benchmark.main()
