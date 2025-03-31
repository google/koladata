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

"""Random operators."""

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.operators import allocation
from koladata.operators import arolla_bridge
from koladata.operators import assertion
from koladata.operators import ids
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import masking
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import schema
from koladata.operators import slices
from koladata.types import data_slice


M = arolla.OperatorsContainer(jagged_shape)
P = arolla.P
constraints = arolla.optools.constraints


@optools.as_lambda_operator(
    'koda_internal.assert_key_for_sample',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.key),
    ],
)
def _assert_key_for_sample(x, key):
  """(Internal) Assert key has the same shape as x if specified or return x."""
  assert_same_shape = arolla.types.LambdaOperator(
      'x, key',
      assertion.with_assertion(
          P.key,
          M.jagged.equal(
              jagged_shape_ops.get_shape(P.x),
              jagged_shape_ops.get_shape(P.key),
          ),
          "'x' and 'key' must have the same shape.",
      ),
  )
  return arolla.types.DispatchOperator(
      'x, key',
      unspecified_cast=arolla.types.DispatchCase(
          P.key, condition=P.key == arolla.UNSPECIFIED
      ),
      default=assert_same_shape(P.x, P.key),
  )(x, key)


@optools.as_lambda_operator(
    'koda_internal.to_dense_array_text',
    qtype_constraints=[qtype_utils.expect_data_slice_or_unspecified(P.x)],
)
def _to_dense_array_text_or_unspecified(x):
  """(Internal) Casts `x` to DENSE_ARRAY_TEXT if specified."""
  return arolla.types.DispatchOperator(
      'x',
      unspecified_cast=arolla.types.DispatchCase(
          P.x, condition=P.x == arolla.UNSPECIFIED
      ),
      default=arolla_bridge.to_arolla_dense_array_text(schema.to_str(P.x)),
  )(x)


@optools.add_to_registry(aliases=['kd.sample'])
@optools.as_lambda_operator(
    'kd.random.sample',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.ratio),
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_or_unspecified(P.key),
    ],
)
def sample(
    x,
    ratio,
    seed,
    key=arolla.unspecified(),
):
  """Randomly sample items in `x` based on ratio.

  The sampling is performed on flatten `x` rather than on the last dimension.

  All items including missing items in `x` are eligible for sampling.

  The sampling is stable given the same inputs. Optional `key` can be used to
  provide additional stability. That is, `key` is used for sampling if set and
  items corresponding to empty keys are never sampled. Otherwise, the indices of
  `x` is used.

  Note that the sampling is performed as follows:
    hash(key, seed) < ratio * 2^63
  Therefore, exact sampled count is not guaranteed. E,g, result of sampling an
  array of 1000 items with 0.1 ratio has present items close to 100 (e.g. 98)
  rather than exact 100 items. However this provides per-item stability that
  the sampling result for an item is deterministic given the same key regardless
  other keys are provided.

  Examples:
    # Select 50% from last dimension.
    ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
    kd.sample(ds, 0.5, 123) -> kd.slice([[None, 4], [None, 8]])

    # Use 'key' for stability
    ds_1 = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
    key_1 = kd.slice([['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd']])
    kd.sample(ds_1, 0.5, 123, key_1) -> kd.slice([[None, 2], [None, None]])

    ds_2 = kd.slice([[4, 3, 2, 1], [5, 6, 7, 8]])
    key_2 = kd.slice([['c', 'a', 'b', 'd'], ['a', 'b', 'c', 'd']])
    kd.sample(ds_2, 0.5, 123, key_2) -> kd.slice([[4, 2], [6, 7]])

  Args:
    x: DataSlice to sample.
    ratio: float number between [0, 1].
    seed: seed from random sampling.
    key: keys used to generate random numbers. The same key generates the same
      random number.

  Returns:
    Sampled DataSlice.
  """
  key = _assert_key_for_sample(x, key)
  x_shape = assertion.with_assertion(
      jagged_shape_ops.get_shape(x),
      slices.get_ndim(x) > 0,
      'expected rank(x) > 0',
  )
  flat_mask = M.random.sample(
      M.array.make_dense_array_shape(M.jagged.size(x_shape)),
      # TODO: allow the name to be specified in to_arolla_float64.
      arolla_bridge.to_arolla_float64(ratio),
      arolla_bridge.to_arolla_int64(seed),
      _to_dense_array_text_or_unspecified(key),
  )
  ds_mask = arolla_bridge.to_data_slice(flat_mask, x_shape)
  return slices.internal_select_by_slice(x, ds_mask)


@optools.add_to_registry(aliases=['kd.sample_n'])
@optools.as_lambda_operator(
    'kd.random.sample_n',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.n),
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_or_unspecified(P.key),
    ],
)
def sample_n(
    x,
    n,
    seed,
    key=arolla.unspecified(),
):
  """Randomly sample n items in `x` from the last dimension.

  The sampling is performed over the last dimension rather than on flatten `x`.

  `n` can either can be a scalar integer or DataSlice. If it is a DataSlice, it
  must have compatible shape with `x.get_shape()[:-1]`. All items including
  missing items in `x` are eligible for sampling.

  The sampling is stable given the same inputs. Optional `key` can be used to
  provide additional stability. That is, `key` is used for sampling if set.
  Otherwise, the indices of `x` are used.

  Examples:
    # Select 2 items from last dimension.
    ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
    kd.sample_n(ds, 2, 123) -> kd.slice([[2, 4], [None, 8]])

    # Select 1 item from the first and 2 items from the second.
    ds = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
    kd.sample_n(ds, [1, 2], 123) -> kd.slice([[4], [None, 5]])

    # Use 'key' for stability
    ds_1 = kd.slice([[1, 2, None, 4], [5, None, None, 8]])
    key_1 = kd.slice([['a', 'b', 'c', 'd'], ['a', 'b', 'c', 'd']])
    kd.sample_n(ds_1, 2, 123, key_1) -> kd.slice([[None, 2], [None, None]])

    ds_2 = kd.slice([[4, 3, 2, 1], [5, 6, 7, 8]])
    key_2 = kd.slice([['c', 'a', 'b', 'd'], ['a', 'b', 'c', 'd']])
    kd.sample_n(ds_2, 2, 123, key_2) -> kd.slice([[4, 2], [6, 7]])

  Args:
    x: DataSlice to sample.
    n: number of items to sample. Either an integer or a DataSlice.
    seed: seed from random sampling.
    key: keys used to generate random numbers. The same key generates the same
      random number.

  Returns:
    Sampled DataSlice.
  """
  key = _assert_key_for_sample(x, key)
  x_shape = assertion.with_assertion(
      jagged_shape_ops.get_shape(x),
      slices.get_ndim(x) > 0,
      'expected rank(x) > 0',
  )
  x_rank = M.jagged.rank(x_shape)
  n = assertion.with_assertion(
      n,
      M.jagged.rank(jagged_shape_ops.get_shape(n)) < x_rank,
      "the rank of 'n' must be smaller than rank of 'x'.",
  )
  n = jagged_shape_ops.expand_to_shape(
      n, jagged_shape_ops.remove_last_ndim(x_shape, 1)
  )
  flat_mask = M.random.sample_n(
      M.array.make_dense_array_shape(M.jagged.size(x_shape)),
      arolla_bridge.to_arolla_dense_array_int64(n),
      arolla_bridge.to_arolla_int64(seed),
      _to_dense_array_text_or_unspecified(key),
      M.jagged.edge_at(x_shape, -1),
  )
  ds_mask = arolla_bridge.to_data_slice(flat_mask, x_shape)
  return slices.internal_select_by_slice(x, ds_mask)


@optools.add_to_registry(
    aliases=['kd.randint_shaped']
)
@optools.as_lambda_operator(
    'kd.random.randint_shaped',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.shape),
        qtype_utils.expect_data_slice_or_unspecified(P.low),
        qtype_utils.expect_data_slice_or_unspecified(P.high),
        qtype_utils.expect_data_slice_or_unspecified(P.seed),
    ],
)
def randint_shaped(
    shape,
    low=arolla.unspecified(),
    high=arolla.unspecified(),
    seed=arolla.unspecified(),
):
  """Returns a DataSlice of random INT64 numbers with the given shape.

  When `seed` is not specified, the results are different across multiple
  invocations given the same input.

  Args:
    shape: used for the shape of the resulting DataSlice.
    low: Lowest (signed) integers to be drawn (unless high=None, in which case
      this parameter is 0 and this value is used for high), inclusive.
    high: If provided, the largest integer to be drawn (see above behavior if
      high=None), exclusive.
    seed: Seed for the random number generator. The same input with the same
      seed generates the same random numbers.

  Returns:
    A DataSlice of random numbers.
  """
  new_low = arolla.types.DispatchOperator(
      'low, high',
      unspecified_case=arolla.types.DispatchCase(
          arolla.int64(0),
          condition=(
              (P.low == arolla.UNSPECIFIED) | (P.high == arolla.UNSPECIFIED)
          ),
      ),
      default=arolla_bridge.to_arolla_int64(P.low),
  )(low, high)
  new_high = arolla_bridge.to_arolla_int64(
      M.core.default_if_unspecified(
          high,
          M.core.default_if_unspecified(
              low, data_slice.DataSlice.from_vals(2**63 - 1)
          ),
      )
  )
  seed = M.core.default_if_unspecified(
      seed, ids.hash_itemid(allocation.new_itemid())
  )

  flat_res = M.array.randint_with_shape(
      M.array.make_dense_array_shape(M.jagged.size(shape)),
      new_low,
      new_high,
      arolla_bridge.to_arolla_int64(seed),
  )
  return arolla_bridge.to_data_slice(flat_res, shape)


@optools.add_to_registry(
    aliases=['kd.randint_shaped_as']
)
@optools.as_lambda_operator(
    'kd.random.randint_shaped_as',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.low),
        qtype_utils.expect_data_slice_or_unspecified(P.high),
        qtype_utils.expect_data_slice_or_unspecified(P.seed),
    ],
)
def randint_shaped_as(
    x,
    low=arolla.unspecified(),
    high=arolla.unspecified(),
    seed=arolla.unspecified(),
):
  """Returns a DataSlice of random INT64 numbers with the same shape as `x`.

  When `seed` is not specified, the results are different across multiple
  invocations given the same input.

  Args:
    x: used to determine the shape of the resulting DataSlice.
    low: Lowest (signed) integers to be drawn (unless high=None, in which case
      this parameter is 0 and this value is used for high), inclusive.
    high: If provided, the largest integer to be drawn (see above behavior if
      high=None), exclusive.
    seed: Seed for the random number generator. The same input with the same
      seed generates the same random numbers.

  Returns:
    A DataSlice of random numbers.
  """
  return randint_shaped(jagged_shape_ops.get_shape(x), low, high, seed)


@optools.add_to_registry(
    aliases=['kd.randint_like']
)
@optools.as_lambda_operator(
    'kd.random.randint_like',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.low),
        qtype_utils.expect_data_slice_or_unspecified(P.high),
        qtype_utils.expect_data_slice_or_unspecified(P.seed),
    ],
)
def randint_like(
    x,
    low=arolla.unspecified(),
    high=arolla.unspecified(),
    seed=arolla.unspecified(),
):
  """Returns a DataSlice of random INT64 numbers with the same sparsity as `x`.

  When `seed` is not specified, the results are different across multiple
  invocations given the same input.

  Args:
    x: used to determine the shape and sparsity of the resulting DataSlice.
    low: Lowest (signed) integers to be drawn (unless high=None, in which case
      this parameter is 0 and this value is used for high), inclusive.
    high: If provided, the largest integer to be drawn (see above behavior if
      high=None), exclusive.
    seed: Seed for the random number generator. The same input with the same
      seed generates the same random numbers.

  Returns:
    A DataSlice of random numbers.
  """
  return randint_shaped_as(x, low, high, seed) & masking.has(x)
