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

"""Logical DataSlice operators."""

from arolla import arolla
from koladata.operators import assertion
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import op_repr
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import schema
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

M = arolla.M
P = arolla.P

# Implemented in schema.py to avoid a dependency cycle.
has = schema._has  # pylint: disable=protected-access


@optools.add_to_registry(
    aliases=['kde.apply_mask'], repr_fn=op_repr.apply_mask_repr
)
@optools.as_backend_operator(
    'kde.logical.apply_mask',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def apply_mask(x, y):  # pylint: disable=unused-argument
  """Filters `x` to items where `y` is present.

  Pointwise masking operator that replaces items in DataSlice `x` by None
  if corresponding items in DataSlice `y` of MASK dtype is `kd.missing`.

  Args:
    x: DataSlice.
    y: DataSlice.

  Returns:
    Masked DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(
    aliases=['kde.coalesce'], repr_fn=op_repr.coalesce_repr
)
@optools.as_backend_operator(
    'kde.logical.coalesce',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def coalesce(x, y):  # pylint: disable=unused-argument
  """Fills in missing values of `x` with values of `y`.

  Pointwise masking operator that replaces missing items (i.e. None) in
  DataSlice `x` by corresponding items in DataSlice y`.
  `x` and `y` do not need to have the same type.

  Args:
    x: DataSlice.
    y: DataSlice used to fill missing items in `x`.

  Returns:
    Coalesced DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.logical._has_not',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _has_not(x):  # pylint: disable=unused-argument
  """Returns present iff `x` is missing element-wise."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.has_not'], repr_fn=op_repr.not_repr)
@optools.as_lambda_operator(
    'kde.logical.has_not',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def has_not(x):
  """Returns present iff `x` is missing element-wise.

  Pointwise operator which take a DataSlice and return a MASK indicating
  iff `x` is missing element-wise. Returns `kd.present` for missing
  items and `kd.missing` for present items.

  Args:
    x: DataSlice.

  Returns:
    DataSlice representing the non-presence of `x`.
  """
  return _has_not(has(x))


@optools.add_to_registry(aliases=['kde.cond'])
@optools.as_lambda_operator(
    'kde.logical.cond',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.condition),
        qtype_utils.expect_data_slice(P.yes),
        qtype_utils.expect_data_slice(P.no),
    ],
)
def cond(condition, yes, no=data_slice.DataSlice.from_vals(None)):
  """Returns `yes` where `condition` is present, otherwise `no`.

  Pointwise operator selects items in `yes` if corresponding items are
  `kd.present` or items in `no` otherwise. `condition` must have MASK dtype.

  If `no` is unspecified corresponding items in result are missing.

  Args:
    condition: DataSlice.
    yes: DataSlice.
    no: DataSlice or unspecified.

  Returns:
    DataSlice of items from `yes` and `no` based on `condition`.
  """
  # TODO: return `yes` or `no` in case condition is 0-dim.
  return (yes & condition) | (no & ~condition)


@optools.add_to_registry(aliases=['kde.mask_and'])
@optools.as_lambda_operator(
    'kde.logical.mask_and',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
)
def mask_and(x, y):
  """Applies pointwise MASK_AND operation on `x` and `y`.

  Both `x` and `y` must have MASK dtype. MASK_AND operation is defined as:
    kd.mask_and(kd.present, kd.present) -> kd.present
    kd.mask_and(kd.present, kd.missing) -> kd.missing
    kd.mask_and(kd.missing, kd.present) -> kd.missing
    kd.mask_and(kd.missing, kd.missing) -> kd.missing

  It is equivalent to `x & y`.

  Args:
    x: DataSlice.
    y: DataSlice.

  Returns:
    DataSlice.
  """
  x = assertion.assert_ds_has_primitives_of(
      x,
      schema_constants.MASK,
      'kde.logical.mask_and: argument `x` must have kd.MASK dtype',
  )
  y = assertion.assert_ds_has_primitives_of(
      y,
      schema_constants.MASK,
      'kde.logical.mask_and: argument `y` must have kd.MASK dtype',
  )
  return schema.with_schema(x & y, schema_constants.MASK)


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kde.logical.mask_or',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
)
def mask_or(x, y):
  """Applies pointwise MASK_OR operation on `x` and `y`.

  Both `x` and `y` must have MASK dtype. MASK_OR operation is defined as:
    kd.mask_or(kd.present, kd.present) -> kd.present
    kd.mask_or(kd.present, kd.missing) -> kd.present
    kd.mask_or(kd.missing, kd.present) -> kd.present
    kd.mask_or(kd.missing, kd.missing) -> kd.missing

  It is equivalent to `x | y`.

  Args:
    x: DataSlice.
    y: DataSlice.

  Returns:
    DataSlice.
  """
  x = assertion.assert_ds_has_primitives_of(
      x,
      schema_constants.MASK,
      'kde.logical.mask_or: argument `x` must have kd.MASK dtype',
  )
  y = assertion.assert_ds_has_primitives_of(
      y,
      schema_constants.MASK,
      'kde.logical.mask_or: argument `y` must have kd.MASK dtype',
  )
  return schema.with_schema(x | y, schema_constants.MASK)


@optools.add_to_registry(aliases=['kde.mask_equal'])
@optools.as_lambda_operator(
    'kde.logical.mask_equal',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
)
def mask_equal(x, y):
  """Applies pointwise MASK_EQUAL operation on `x` and `y`.

  Both `x` and `y` must have MASK dtype. MASK_EQUAL operation is defined as:
    kd.mask_equal(kd.present, kd.present) -> kd.present
    kd.mask_equal(kd.present, kd.missing) -> kd.missing
    kd.mask_equal(kd.missing, kd.present) -> kd.missing
    kd.mask_equal(kd.missing, kd.missing) -> kd.present

  Note that this is different from `x == y`. For example,
    kd.missing == kd.missing -> kd.missing

  Args:
    x: DataSlice.
    y: DataSlice.

  Returns:
    DataSlice.
  """
  x = assertion.assert_ds_has_primitives_of(
      x,
      schema_constants.MASK,
      'kde.logical.mask_equal: argument `x` must have kd.MASK dtype',
  )
  y = assertion.assert_ds_has_primitives_of(
      y,
      schema_constants.MASK,
      'kde.logical.mask_equal: argument `y` must have kd.MASK dtype',
  )
  return schema.with_schema((x & y) | (~x & ~y), schema_constants.MASK)


@optools.add_to_registry(aliases=['kde.mask_not_equal'])
@optools.as_lambda_operator(
    'kde.logical.mask_not_equal',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
)
def mask_not_equal(x, y):
  """Applies pointwise MASK_NOT_EQUAL operation on `x` and `y`.

  Both `x` and `y` must have MASK dtype. MASK_NOT_EQUAL operation is defined as:
    kd.mask_not_equal(kd.present, kd.present) -> kd.missing
    kd.mask_not_equal(kd.present, kd.missing) -> kd.present
    kd.mask_not_equal(kd.missing, kd.present) -> kd.present
    kd.mask_not_equal(kd.missing, kd.missing) -> kd.missing

  Note that this is different from `x != y`. For example,
    kd.present != kd.missing -> kd.missing
    kd.missing != kd.present -> kd.missing

  Args:
    x: DataSlice.
    y: DataSlice.

  Returns:
    DataSlice.
  """
  return ~mask_equal(x, y)


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.logical._agg_any',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _agg_any(x):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.agg_any'])
@optools.as_lambda_operator(
    'kde.logical.agg_any',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def agg_any(x, ndim=arolla.unspecified()):
  """Returns present if any element is present along the last ndim dimensions.

  `x` must have MASK dtype.

  The resulting slice has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.

  Args:
    x: A DataSlice.
    ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
      <= get_ndim(x).
  """
  return _agg_any(jagged_shape_ops.flatten_last_ndim(x, ndim))


@optools.add_to_registry(aliases=['kde.any'])
@optools.as_lambda_operator('kde.logical.any')
def any_(x):
  """Returns present iff any element is present over all dimensions.

  `x` must have MASK dtype.

  The result is a zero-dimensional DataItem.

  Args:
    x: A DataSlice.
  """
  return agg_any(jagged_shape_ops.flatten(x))


@optools.add_to_registry(aliases=['kde.agg_has'])
@optools.as_lambda_operator('kde.logical.agg_has')
def agg_has(x, ndim=arolla.unspecified()):
  """Returns present iff any element is present along the last ndim dimensions.

  The resulting slice has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.

  It is equivalent to `kd.agg_any(kd.has(x))`.

  Args:
    x: A DataSlice.
    ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
      <= get_ndim(x).
  """
  return agg_any(has(x), ndim=ndim)


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.logical._agg_all',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _agg_all(x):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.agg_all'])
@optools.as_lambda_operator(
    'kde.logical.agg_all',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def agg_all(x, ndim=arolla.unspecified()):
  """Returns present if all elements are present along the last ndim dimensions.

  `x` must have MASK dtype.

  The resulting slice has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.

  Args:
    x: A DataSlice.
    ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
      <= get_ndim(x).
  """
  return _agg_all(jagged_shape_ops.flatten_last_ndim(x, ndim))


@optools.add_to_registry(aliases=['kde.all'])
@optools.as_lambda_operator('kde.logical.all')
def all_(x):
  """Returns present iff all elements are present over all dimensions.

  `x` must have MASK dtype.

  The result is a zero-dimensional DataItem.

  Args:
    x: A DataSlice.
  """
  return agg_all(jagged_shape_ops.flatten(x))


@optools.add_to_registry(aliases=['kde.disjoint_coalesce'])
@optools.as_lambda_operator(
    'kde.logical.disjoint_coalesce',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
)
def disjoint_coalesce(x, y):
  """Fills in missing values of `x` with values of `y`.

  Raises if `x` and `y` intersect. It is equivalent to `x | y` with additional
  assertion that `x` and `y` are disjoint.

  Args:
    x: DataSlice.
    y: DataSlice used to fill missing items in `x`.

  Returns:
    Coalesced DataSlice.
  """
  x = assertion.with_assertion(
      x,
      ~any_(has(x) & has(y)),
      'kde.logical.disjoint_coalesce: `x` and `y` cannot intersect',
  )
  return coalesce(x, y)
