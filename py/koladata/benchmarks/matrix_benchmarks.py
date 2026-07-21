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
from koladata import kd_ext
import numpy as np
from numpy import ma

BATCH_SIZE = 100
UNIFORM_SIZE = 50
JAGGED_SIZES = [10, 20, 30, 50, 80]

_BATCH_MODE_NAMES = ['uniform', 'jagged']


def _seed_random_number_generators():
  random.seed(42)
  np.random.seed(42)


def _make_jagged_sizes():
  return [random.choice(JAGGED_SIZES) for _ in range(BATCH_SIZE)]


def _make_masked_vector_np(size):
  data = np.random.randn(size)
  mask = np.random.random(size) < 0.1  # ~10% missing
  return np.ma.array(data, mask=mask)


def _make_uniform_vectors_np():
  return [_make_masked_vector_np(UNIFORM_SIZE) for _ in range(BATCH_SIZE)]


def _make_masked_matrix_np(rows, cols):
  data = np.random.randn(rows, cols)
  mask = np.random.random((rows, cols)) < 0.1  # ~10% missing
  return np.ma.array(data, mask=mask)


def _make_uniform_matrices_np():
  return [
      _make_masked_matrix_np(UNIFORM_SIZE, UNIFORM_SIZE)
      for _ in range(BATCH_SIZE)
  ]


def _make_jagged_vectors_np(sizes):
  return [_make_masked_vector_np(s) for s in sizes]


def _make_jagged_matrices_np(sizes):
  return [_make_masked_matrix_np(s, s) for s in sizes]


def _np_to_kd_vectors(vecs_np):
  if len(vecs_np) == 1:
    return kd_ext.npkd.from_array(vecs_np[0])
  return kd.stack(*[kd_ext.npkd.from_array(v) for v in vecs_np], ndim=1)


def _np_to_kd_matrices(mats_np):
  if len(mats_np) == 1:
    return kd_ext.npkd.from_array(mats_np[0])
  return kd.stack(*[kd_ext.npkd.from_array(m) for m in mats_np], ndim=2)


@kd.optools.as_lambda_operator(name='transpose_lambda')
def transpose_lambda(a):
  last_size = kd.agg_size(a)
  m = kd.agg_size(last_size)
  n = kd.collapse(last_size)
  return kd.subslice(a, kd.range(m.repeat(n)), kd.range(n))


@kd.optools.as_lambda_operator(name='outer_lambda_via_matmul')
def outer_lambda_via_matmul(x, y):
  # outer(x, y) = x_col @ y_row where:
  #   x_col has shape (..., m, 1) — column vector
  #   y_row has shape (..., 1, n) — row vector
  # matmul gives (..., m, 1) @ (..., 1, n) -> (..., m, n)
  x_col = kd.repeat(x, 1)  # (..., m) -> (..., m, 1)
  y_row = kd.matrix.transpose(
      kd.repeat(y, 1)
  )  # (..., n) -> (..., n, 1) -> (..., 1, n)
  return kd.matrix.matmul(x_col, y_row)


@kd.optools.as_lambda_operator(name='outer_lambda_via_multiply')
def outer_lambda_via_multiply(x, y):
  """Compute outer(x, y) using list operations and pointwise multiply."""
  # outer(x, y)[i, j] = x[i] * y[j]
  # 1) Align batch shapes by imploding (chop last dim), aligning, exploding.
  x_lists = kd.implode(x)
  y_lists = kd.implode(y)
  x_lists, y_lists = kd.align(x_lists, y_lists)
  x = kd.explode(x_lists)
  y = kd.explode(y_lists)
  # 2) Build x_col: repeat each x[i] by n times -> (..., m, n).
  n = kd.agg_size(y)
  x_col = kd.repeat(x, n)
  # 3) Build y_row: repeat each y list m times, then explode -> (..., m, n)
  #    where each row is the full y vector.
  m = kd.agg_size(x)
  y_row = kd.explode(kd.repeat(y_lists, m))
  return (x_col * y_row) | 0


eager_transpose_lambda = kd.optools.eager.EagerOperator(transpose_lambda)
eager_outer_lambda_via_matmul = kd.optools.eager.EagerOperator(
    outer_lambda_via_matmul
)
eager_outer_lambda_via_multiply = kd.optools.eager.EagerOperator(
    outer_lambda_via_multiply
)


# Switch off docstring lint checks for the benchmark functions below.
# pylint: disable=missing-function-docstring

# ---- dot ----


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def numpy_dot(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    x_np = _make_uniform_vectors_np()
    y_np = _make_uniform_vectors_np()
  else:
    sizes = _make_jagged_sizes()
    x_np = _make_jagged_vectors_np(sizes)
    y_np = _make_jagged_vectors_np(sizes)
  while state:
    _ = [np.dot(x_np[i], y_np[i]) for i in range(BATCH_SIZE)]


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def koda_dot(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    x_np = _make_uniform_vectors_np()
    y_np = _make_uniform_vectors_np()
  else:
    sizes = _make_jagged_sizes()
    x_np = _make_jagged_vectors_np(sizes)
    y_np = _make_jagged_vectors_np(sizes)
  x_kd = _np_to_kd_vectors(x_np)
  y_kd = _np_to_kd_vectors(y_np)
  # Make sure that Koda and NumPy implementations agree on a functional level.
  # Koda treats missing values as 0, so we compare against filled(0).
  kd.testing.assert_allclose(
      kd.matrix.dot(x_kd, y_kd),
      kd.slice(
          [np.dot(x_np[i].filled(0), y_np[i].filled(0))
           for i in range(BATCH_SIZE)],
          kd.FLOAT64,
      ),
      rtol=1e-12,
  )
  while state:
    _ = kd.matrix.dot(x_kd, y_kd)


# ---- matmul ----


def _np_matmul_impl(a_np, b_np):
  # In the general case of multiplying non-square matrices that contain
  # missing values, one has to fill in the missing values with 0 first.
  # Otherwise the @ operator will fail with a ValueError it tries to combine
  # the masks, which have different shapes.
  # Another option is to use ma.dot (see below), but that is much slower.
  return [a_np[i].filled(0) @ b_np[i].filled(0) for i in range(BATCH_SIZE)]


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def numpy_matmul_fill_zeros(state):
  """NumPy matmul with missing values filled in with zeros (baseline)."""
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    a_np = _make_uniform_matrices_np()
    b_np = _make_uniform_matrices_np()
  else:
    sizes = _make_jagged_sizes()
    a_np = _make_jagged_matrices_np(sizes)
    b_np = _make_jagged_matrices_np(sizes)
  while state:
    _ = _np_matmul_impl(a_np, b_np)


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def numpy_matmul_ma_dot(state):
  """NumPy matmul using ma.dot (baseline)."""
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    a_np = _make_uniform_matrices_np()
    b_np = _make_uniform_matrices_np()
  else:
    sizes = _make_jagged_sizes()
    a_np = _make_jagged_matrices_np(sizes)
    b_np = _make_jagged_matrices_np(sizes)
  while state:
    _ = [ma.dot(a_np[i], b_np[i]) for i in range(BATCH_SIZE)]


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def koda_matmul(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    a_np = _make_uniform_matrices_np()
    b_np = _make_uniform_matrices_np()
  else:
    sizes = _make_jagged_sizes()
    a_np = _make_jagged_matrices_np(sizes)
    b_np = _make_jagged_matrices_np(sizes)
  a_kd = _np_to_kd_matrices(a_np)
  b_kd = _np_to_kd_matrices(b_np)
  # Sanity check: from_array preserves numpy masks as Koda missing values,
  # and matmul treats missing values as 0 (matching NumPy's filled(0) behavior).
  kd.testing.assert_allclose(
      kd.matrix.matmul(a_kd, b_kd),
      _np_to_kd_matrices(_np_matmul_impl(a_np, b_np)),
  )
  while state:
    _ = kd.matrix.matmul(a_kd, b_kd)


# ---- outer ----


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def numpy_outer(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    x_np = _make_uniform_vectors_np()
    y_np = _make_uniform_vectors_np()
  else:
    sizes = _make_jagged_sizes()
    x_np = _make_jagged_vectors_np(sizes)
    y_np = _make_jagged_vectors_np(sizes)
  while state:
    _ = [np.outer(x_np[i], y_np[i]) for i in range(BATCH_SIZE)]


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def koda_outer(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    x_np = _make_uniform_vectors_np()
    y_np = _make_uniform_vectors_np()
  else:
    sizes = _make_jagged_sizes()
    x_np = _make_jagged_vectors_np(sizes)
    y_np = _make_jagged_vectors_np(sizes)
  x_kd = _np_to_kd_vectors(x_np)
  y_kd = _np_to_kd_vectors(y_np)
  # Make sure that Koda and NumPy implementations agree on a functional level.
  # Koda treats missing values as 0, so we compare against filled(0).
  kd.testing.assert_allclose(
      kd.matrix.outer(x_kd, y_kd),
      _np_to_kd_matrices(
          [np.outer(x_np[i].filled(0), y_np[i].filled(0))
           for i in range(BATCH_SIZE)]
      ),
      rtol=1e-12,
  )
  while state:
    _ = kd.matrix.outer(x_kd, y_kd)


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def koda_outer_lambda_via_matmul(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    x_np = _make_uniform_vectors_np()
    y_np = _make_uniform_vectors_np()
  else:
    sizes = _make_jagged_sizes()
    x_np = _make_jagged_vectors_np(sizes)
    y_np = _make_jagged_vectors_np(sizes)
  x_kd = _np_to_kd_vectors(x_np)
  y_kd = _np_to_kd_vectors(y_np)
  # Make sure that the lambda implementation agrees with the C++ implementation.
  kd.testing.assert_allclose(
      eager_outer_lambda_via_matmul(x_kd, y_kd),
      kd.matrix.outer(x_kd, y_kd),
      rtol=1e-12,
  )
  while state:
    _ = eager_outer_lambda_via_matmul(x_kd, y_kd)


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def koda_outer_lambda_via_multiply(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    x_np = _make_uniform_vectors_np()
    y_np = _make_uniform_vectors_np()
  else:
    sizes = _make_jagged_sizes()
    x_np = _make_jagged_vectors_np(sizes)
    y_np = _make_jagged_vectors_np(sizes)
  x_kd = _np_to_kd_vectors(x_np)
  y_kd = _np_to_kd_vectors(y_np)
  # Make sure that the lambda implementation agrees with the C++ implementation.
  kd.testing.assert_allclose(
      eager_outer_lambda_via_multiply(x_kd, y_kd),
      kd.matrix.outer(x_kd, y_kd),
      rtol=1e-12,
  )
  while state:
    _ = eager_outer_lambda_via_multiply(x_kd, y_kd)


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
    _ = [a.swapaxes(-1, -2).copy() for a in a_np]


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
  # Check that Koda's result is the same as the NumPy result:
  kd.testing.assert_equal(
      kd.matrix.transpose(a_kd),
      _np_to_kd_matrices([a.swapaxes(-1, -2).copy() for a in a_np]),
  )
  while state:
    _ = kd.matrix.transpose(a_kd)


# At the time of writing, implementing kd.matrix.transpose via a lambda
# with kd.group_by and kd.index as shown below is 7x slower on uniform and
# 6x slower on jagged compared to the C++ implementation of
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

  # Sanity check the lambda implementation.
  kd.testing.assert_equal(
      kd.matrix.transpose(a_kd), eager_transpose_lambda(a_kd)
  )

  while state:
    _ = eager_transpose_lambda(a_kd)


# ---- diag_matrix ----


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def numpy_diag_matrix(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    x_np = _make_uniform_vectors_np()
  else:
    sizes = _make_jagged_sizes()
    x_np = _make_jagged_vectors_np(sizes)
  while state:
    _ = [np.diag(x_np[i]) for i in range(BATCH_SIZE)]


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def koda_diag_matrix(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    x_np = _make_uniform_vectors_np()
  else:
    sizes = _make_jagged_sizes()
    x_np = _make_jagged_vectors_np(sizes)
  x_kd = _np_to_kd_vectors(x_np)
  # Make sure that Koda and NumPy implementations agree on a functional level.
  # Koda's diag_matrix produces a sparse diagonal matrix (missing off-diag),
  # while np.diag produces a dense matrix (zeros off-diag). We compare with
  # off-diagonal filled to zero on the Koda side. np.diag doesn't preserve
  # masks, so we reconstruct the masked diagonal matrix explicitly.
  off_diag_zeros = 0 & (
      ~kd.matrix.diag_matrix(kd.val_shaped_as(x_kd, kd.present))
  )
  kd.testing.assert_allclose(
      kd.matrix.diag_matrix(x_kd) | off_diag_zeros,
      _np_to_kd_matrices([
          np.ma.array(
              np.diag(x_np[i].data), mask=np.diag(np.ma.getmaskarray(x_np[i]))
          )
          for i in range(BATCH_SIZE)
      ]),
      rtol=1e-10,
  )
  while state:
    _ = kd.matrix.diag_matrix(x_kd)


# ---- diag_vector ----


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def numpy_diag_vector(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    a_np = _make_uniform_matrices_np()
  else:
    sizes = _make_jagged_sizes()
    a_np = _make_jagged_matrices_np(sizes)
  while state:
    _ = [np.diag(a_np[i]) for i in range(BATCH_SIZE)]


@google_benchmark.register
@google_benchmark.option.arg_names(['batch_mode'])
@google_benchmark.option.dense_range(0, 1)
def koda_diag_vector(state):
  _seed_random_number_generators()
  batch_mode = _BATCH_MODE_NAMES[state.range(0)]
  if batch_mode == 'uniform':
    a_np = _make_uniform_matrices_np()
  else:
    sizes = _make_jagged_sizes()
    a_np = _make_jagged_matrices_np(sizes)
  a_kd = _np_to_kd_matrices(a_np)
  # Make sure that Koda and NumPy implementations agree on a functional level.
  kd.testing.assert_equal(
      kd.matrix.diag_vector(a_kd),
      _np_to_kd_vectors([np.diag(a_np[i]) for i in range(BATCH_SIZE)]),
  )
  while state:
    _ = kd.matrix.diag_vector(a_kd)


if __name__ == '__main__':
  google_benchmark.main()
