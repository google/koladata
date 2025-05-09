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

"""Koladata benchmarks related to operator evaluation."""

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

if __name__ == '__main__':
  google_benchmark.main()
