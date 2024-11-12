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

"""Arithmetic Koda operators."""

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import op_repr
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import data_slice
from koladata.types import qtypes

M = arolla.OperatorsContainer(jagged_shape)
P = arolla.P


@optools.add_to_registry(repr_fn=op_repr.subtract_repr)
@optools.as_backend_operator(
    'kde.math.subtract',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def subtract(x, y):  # pylint: disable=unused-argument
  """Computes pointwise x - y."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(repr_fn=op_repr.multiply_repr)
@optools.as_backend_operator(
    'kde.math.multiply',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def multiply(x, y):  # pylint: disable=unused-argument
  """Computes pointwise x * y."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math.log',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def log(x):  # pylint: disable=unused-argument
  """Computes pointwise natural logarithm of the input."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math.log10',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def log10(x):  # pylint: disable=unused-argument
  """Computes pointwise logarithm in base 10 of the input."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math.exp',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def exp(x):  # pylint: disable=unused-argument
  """Computes pointwise exponential of the input."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math.abs',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def abs(x):  # pylint: disable=unused-argument,redefined-builtin
  """Computes pointwise absolute value of the input."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(repr_fn=op_repr.neg_repr)
@optools.as_backend_operator(
    'kde.math.neg',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def neg(x):  # pylint: disable=unused-argument,redefined-builtin
  """Computes pointwise negation of the input, i.e. -x."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(repr_fn=op_repr.pos_repr)
@optools.as_backend_operator(
    'kde.math.pos',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def pos(x):  # pylint: disable=unused-argument,redefined-builtin
  """Computes pointwise positive of the input, i.e. +x."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math.ceil',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def ceil(x):  # pylint: disable=unused-argument,g-doc-args
  """Computes pointwise ceiling of the input, e.g.

  rounding up: returns the smallest integer value that is not less than the
  input.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math.floor',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def floor(x):  # pylint: disable=unused-argument,g-doc-args
  """Computes pointwise floor of the input, e.g.

  rounding down: returns the largest integer value that is not greater than the
  input.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math.round',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def round(x):  # pylint: disable=unused-argument,redefined-builtin,g-doc-args
  """Computes pointwise rounding of the input.

  Please note that this is NOT bankers rounding, unlike Python built-in or
  Tensorflow round(). If the first decimal is exactly  0.5, the result is
  rounded to the number with a higher absolute value:
  round(1.4) == 1.0
  round(1.5) == 2.0
  round(1.6) == 2.0
  round(2.5) == 3.0 # not 2.0
  round(-1.4) == -1.0
  round(-1.5) == -2.0
  round(-1.6) == -2.0
  round(-2.5) == -3.0 # not -2.0
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(repr_fn=op_repr.divide_repr)
@optools.as_backend_operator(
    'kde.math.divide',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def divide(x, y):  # pylint: disable=unused-argument
  """Computes pointwise x / y."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(repr_fn=op_repr.pow_repr)
@optools.as_backend_operator(
    'kde.math.pow',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _pow(x, y):  # pylint: disable=unused-argument
  """Computes pointwise x ** y."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(repr_fn=op_repr.floordiv_repr)
@optools.as_backend_operator(
    'kde.math.floordiv',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def floordiv(x, y):  # pylint: disable=unused-argument
  """Computes pointwise x // y."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(repr_fn=op_repr.mod_repr)
@optools.as_backend_operator(
    'kde.math.mod',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def mod(x, y):  # pylint: disable=unused-argument
  """Computes pointwise x % y."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math._agg_sum',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _agg_sum(x):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.agg_sum'])
@optools.as_lambda_operator(
    'kde.math.agg_sum',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def agg_sum(x, ndim=arolla.unspecified()):
  """Returns the sums along the last ndim dimensions.

  The resulting slice has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.

  Example:
    ds = kd.slice([[1, None, 1], [3, 4], [None, None]])
    kd.agg_sum(ds)  # -> kd.slice([2, 7, None])
    kd.agg_sum(ds, ndim=1)  # -> kd.slice([2, 7, None])
    kd.agg_sum(ds, ndim=2)  # -> kd.slice(9)

  Args:
    x: A DataSlice of numbers.
    ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
      <= get_ndim(x).
  """
  return _agg_sum(jagged_shape_ops.flatten_last_ndim(x, ndim))


@optools.add_to_registry(aliases=['kde.sum'])
@optools.as_lambda_operator('kde.math.sum')
def sum(x):
  """Returns the sum of elements over all dimensions.

  The result is a zero-dimensional DataItem.

  Args:
    x: A DataSlice of numbers.
  """
  return agg_sum(jagged_shape_ops.flatten(x))


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math._agg_mean',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _agg_mean(x):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kde.math.agg_mean',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def agg_mean(x, ndim=arolla.unspecified()):
  """Returns the means along the last ndim dimensions.

  The resulting slice has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.

  Example:
    ds = kd.slice([[1, None, None], [3, 4], [None, None]])
    kd.agg_mean(ds)  # -> kd.slice([1, 3.5, None])
    kd.agg_mean(ds, ndim=1)  # -> kd.slice([1, 3.5, None])
    kd.agg_mean(ds, ndim=2)  # -> kd.slice(2.6666666666666) # (1 + 3 + 4) / 3)

  Args:
    x: A DataSlice of numbers.
    ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
      <= get_ndim(x).
  """
  return _agg_mean(jagged_shape_ops.flatten_last_ndim(x, ndim))


@optools.add_to_registry()
@optools.as_lambda_operator('kde.math.mean')
def mean(x):
  """Returns the mean of elements over all dimensions.

  The result is a zero-dimensional DataItem.

  Args:
    x: A DataSlice of numbers.
  """
  return agg_mean(jagged_shape_ops.flatten(x))


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math._agg_median',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _agg_median(x):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kde.math.agg_median',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def agg_median(x, ndim=arolla.unspecified()):
  """Returns the medians along the last ndim dimensions.

  The resulting slice has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.

  Please note that for even number of elements, the median is the next value
  down from the middle, p.ex.: median([1, 2]) == 1.
  That is made by design to fulfill the following property:
  1. type of median(x) == type of elements of x;
  2. median(x) ∈ x.

  Args:
    x: A DataSlice of numbers.
    ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
      <= get_ndim(x).
  """
  return _agg_median(jagged_shape_ops.flatten_last_ndim(x, ndim))


@optools.add_to_registry()
@optools.as_lambda_operator('kde.math.median')
def median(x):
  """Returns the median of elements over all dimensions.

  The result is a zero-dimensional DataItem.

  Please note that for even number of elements, the median is the next value
  down from the middle, p.ex.: median([1, 2]) == 1.
  That is made by design to fulfill the following property:
  1. type of median(x) == type of elements of x;
  2. median(x) ∈ x.

  Args:
    x: A DataSlice of numbers.
  """
  return agg_median(jagged_shape_ops.flatten(x))


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math._agg_std',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.unbiased),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _agg_std(x, unbiased=data_slice.DataSlice.from_vals(True)):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kde.math.agg_std',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
        qtype_utils.expect_data_slice(P.unbiased),
    ],
)
def agg_std(
    x, unbiased=data_slice.DataSlice.from_vals(True), ndim=arolla.unspecified()
):
  """Returns the standard deviation along the last ndim dimensions.

  The resulting slice has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.

  Example:
    ds = kd.slice([10, 9, 11])
    kd.agg_std(ds)  # -> kd.slice(1.0)
    kd.agg_std(ds, unbiased=False)  # -> kd.slice(0.8164966)

  Args:
    x: A DataSlice of numbers.
    unbiased: A boolean flag indicating whether to substract 1 from the number
      of elements in the denominator.
    ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
      <= get_ndim(x).
  """
  return _agg_std(
      jagged_shape_ops.flatten_last_ndim(x, ndim),
      unbiased,
  )


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math._agg_var',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.unbiased),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _agg_var(x, unbiased=data_slice.DataSlice.from_vals(True)):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kde.math.agg_var',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
        qtype_utils.expect_data_slice(P.unbiased),
    ],
)
def agg_var(
    x, unbiased=data_slice.DataSlice.from_vals(True), ndim=arolla.unspecified()
):
  """Returns the variance along the last ndim dimensions.

  The resulting slice has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.

  Example:
    ds = kd.slice([10, 9, 11])
    kd.agg_var(ds)  # -> kd.slice(1.0)
    kd.agg_var(ds, unbiased=False)  # -> kd.slice([0.6666667])

  Args:
    x: A DataSlice of numbers.
    unbiased: A boolean flag indicating whether to substract 1 from the number
      of elements in the denominator.
    ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
      <= get_ndim(x).
  """
  return _agg_var(
      jagged_shape_ops.flatten_last_ndim(x, ndim),
      unbiased,
  )


@optools.add_to_registry(aliases=['kde.maximum'])
@optools.as_backend_operator(
    'kde.math.maximum',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def maximum(x, y):  # pylint: disable=unused-argument
  """Computes pointwise max(x, y)."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.minimum'])
@optools.as_backend_operator(
    'kde.math.minimum',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def minimum(x, y):  # pylint: disable=unused-argument
  """Computes pointwise min(x, y)."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math._agg_max',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _agg_max(x):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.agg_max'])
@optools.as_lambda_operator(
    'kde.math.agg_max',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def agg_max(x, ndim=arolla.unspecified()):
  """Returns the maximum of items along the last ndim dimensions.

  The resulting slice has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.

  Example:
    ds = kd.slice([[2, None, 1], [3, 4], [None, None]])
    kd.agg_max(ds)  # -> kd.slice([2, 4, None])
    kd.agg_max(ds, ndim=1)  # -> kd.slice([2, 4, None])
    kd.agg_max(ds, ndim=2)  # -> kd.slice(4)

  Args:
    x: A DataSlice of numbers.
    ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
      <= get_ndim(x).
  """
  return _agg_max(jagged_shape_ops.flatten_last_ndim(x, ndim))


@optools.add_to_registry(aliases=['kde.max'])
@optools.as_lambda_operator('kde.math.max')
def _max(x):
  """Returns the maximum of items over all dimensions.

  The result is a zero-dimensional DataItem.

  Args:
    x: A DataSlice of numbers.
  """
  return agg_max(jagged_shape_ops.flatten(x))


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math._cum_max',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _cum_max(x):  # pylint: disable=unused-argument
  """Returns the cumulative max of items."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kde.math.cum_max',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def cum_max(x, ndim=arolla.unspecified()):
  """Returns the cumulative max of items along the last ndim dimensions."""
  res = _cum_max(jagged_shape_ops.flatten_last_ndim(x, ndim))
  return jagged_shape_ops.reshape(res, jagged_shape_ops.get_shape(x))


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math._agg_min',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _agg_min(x):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.agg_min'])
@optools.as_lambda_operator(
    'kde.math.agg_min',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def agg_min(x, ndim=arolla.unspecified()):
  """Returns the minimum of items along the last ndim dimensions.

  The resulting slice has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.

  Example:
    ds = kd.slice([[2, None, 1], [3, 4], [None, None]])
    kd.agg_min(ds)  # -> kd.slice([1, 3, None])
    kd.agg_min(ds, ndim=1)  # -> kd.slice([1, 3, None])
    kd.agg_min(ds, ndim=2)  # -> kd.slice(1)

  Args:
    x: A DataSlice of numbers.
    ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
      <= get_ndim(x).
  """
  return _agg_min(jagged_shape_ops.flatten_last_ndim(x, ndim))


@optools.add_to_registry(aliases=['kde.min'])
@optools.as_lambda_operator('kde.math.min')
def _min(x):
  """Returns the minimum of items over all dimensions.

  The result is a zero-dimensional DataItem.

  Args:
    x: A DataSlice of numbers.
  """
  return agg_min(jagged_shape_ops.flatten(x))


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math._cum_min',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _cum_min(x):  # pylint: disable=unused-argument
  """Returns the cumulative minimum of items."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kde.math.cum_min',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def cum_min(x, ndim=arolla.unspecified()):
  """Returns the cumulative minimum of items along the last ndim dimensions."""
  res = _cum_min(jagged_shape_ops.flatten_last_ndim(x, ndim))
  return jagged_shape_ops.reshape(res, jagged_shape_ops.get_shape(x))


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math._cum_sum',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _cum_sum(x):  # pylint: disable=unused-argument
  """Returns the cumulative sum of items."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kde.math.cum_sum',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def cum_sum(x, ndim=arolla.unspecified()):
  """Returns the cumulative sum of items along the last ndim dimensions."""
  res = _cum_sum(jagged_shape_ops.flatten_last_ndim(x, ndim))
  return jagged_shape_ops.reshape(res, jagged_shape_ops.get_shape(x))


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.math._cdf',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.weights),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _cdf(x, weights):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kde.math.cdf',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.weights),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def cdf(x, weights=arolla.unspecified(), ndim=arolla.unspecified()):
  """Returns the CDF of x in the last ndim dimensions of x element-wise.

  The CDF is an array of floating-point values of the same shape as x and
  weights, where each element represents which percentile the corresponding
  element in x is situated at in its sorted group, i.e. the percentage of values
  in the group that are smaller than or equal to it.

  Args:
    x: a DataSlice of numbers.
    weights: if provided, will compute weighted CDF: each output value will
      correspond to the weight percentage of values smaller than or equal to x.
    ndim: The number of dimensions to compute CDF over.
  """

  weights = M.core.default_if_unspecified(
      weights,
      data_slice.DataSlice.from_vals(1.0),
  )

  expanded_weights = jagged_shape_ops.expand_to_shape(
      weights,
      jagged_shape_ops.get_shape(x),
  )
  x_flattened = jagged_shape_ops.flatten_last_ndim(x, ndim)
  weights_flattened = jagged_shape_ops.flatten_last_ndim(expanded_weights, ndim)

  res = _cdf(x_flattened, weights_flattened)
  return jagged_shape_ops.reshape(res, jagged_shape_ops.get_shape(x))
