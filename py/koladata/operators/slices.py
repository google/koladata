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

"""Operators that work on DataSlices."""

import types as py_types

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.operators import arolla_bridge
from koladata.operators import assertion
from koladata.operators import functor
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import masking
from koladata.operators import math
from koladata.operators import op_repr
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import schema as schema_ops
from koladata.operators import view_overloads as _
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants


M = arolla.OperatorsContainer(jagged_shape)
P = arolla.P
MASK = schema_constants.MASK
constraints = arolla.optools.constraints


@optools.add_to_registry(aliases=['kde.val_shaped'])
@optools.as_lambda_operator(
    'kde.slices.val_shaped',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.shape),
        qtype_utils.expect_data_slice(P.val),
    ],
)
def val_shaped(shape, val):
  """Creates a DataSlice with `val` expanded to the given shape.

  Example:
    shape = kd.shapes.create_shape([2], [1, 2])
    kd.slices.val_shaped(shape, 1) -> kd.slice([[1], [1, 1]])
    kd.slices.val_shaped(shape, kd.slice([None, 2])) -> kd.slice([[None], [2,
    2]])

  Args:
    shape: shape to expand to.
    val: value to expand.

  Returns:
    A DataSlice with the same shape as `shape`.
  """
  return jagged_shape_ops.expand_to_shape(val, shape)


@optools.add_to_registry(aliases=['kde.val_shaped_as'])
@optools.as_lambda_operator(
    'kde.slices.val_shaped_as',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.val),
    ],
)
def val_shaped_as(x, val):
  """Creates a DataSlice with `val` expanded to the shape of `x`.

  Example:
    x = kd.slice([0], [0, 0])
    kd.slices.val_shaped_as(x, 1) -> kd.slice([[1], [1, 1]])
    kd.slices.val_shaped_as(x, kd.slice([None, 2])) -> kd.slice([[None], [2,
    2]])

  Args:
    x: DataSlice to match the shape of.
    val: DataSlice to expand.

  Returns:
    A DataSlice with the same shape as `x`.
  """
  return val_shaped(jagged_shape_ops.get_shape(x), val)


@optools.add_to_registry(aliases=['kde.val_like'])
@optools.as_lambda_operator(
    'kde.slices.val_like',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.val),
    ],
)
def val_like(x, val):
  """Creates a DataSlice with `val` masked and expanded to the shape of `x`.

  Example:
    x = kd.slice([0], [0, None])
    kd.slices.val_like(x, 1) -> kd.slice([[1], [1, None]])
    kd.slices.val_like(x, kd.slice([1, 2])) -> kd.slice([[1], [2, None]])
    kd.slices.val_like(x, kd.slice([None, 2])) -> kd.slice([[None], [2, None]])

  Args:
    x: DataSlice to match the shape and sparsity of.
    val: DataSlice to expand.

  Returns:
    A DataSlice with the same shape as `x` and masked by `x`.
  """
  return val_shaped_as(x, val) & masking.has(x)


@optools.add_to_registry(aliases=['kde.present_shaped'])
@optools.as_lambda_operator(
    'kde.slices.present_shaped',
    qtype_constraints=[qtype_utils.expect_jagged_shape(P.shape)],
)
def present_shaped(shape):
  """Creates a DataSlice of present masks with the given shape.

  Example:
    shape = kd.shapes.create_shape([2], [1, 2])
    kd.slices.present_shaped(shape) -> kd.slice([[present], [present, present]])

  Args:
    shape: shape to expand to.

  Returns:
    A DataSlice with the same shape as `shape`.
  """
  return val_shaped(shape, data_slice.DataSlice.from_vals(arolla.present()))


@optools.add_to_registry(aliases=['kde.present_shaped_as'])
@optools.as_lambda_operator(
    'kde.slices.present_shaped_as',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def present_shaped_as(x):
  """Creates a DataSlice of present masks with the shape of `x`.

  Example:
    x = kd.slice([0], [0, 0])
    kd.slices.present_shaped_as(x) -> kd.slice([[present], [present, present]])

  Args:
    x: DataSlice to match the shape of.

  Returns:
    A DataSlice with the same shape as `x`.
  """
  return val_shaped_as(x, data_slice.DataSlice.from_vals(arolla.present()))


@optools.add_to_registry(aliases=['kde.present_like'])
@optools.as_lambda_operator(
    'kde.slices.present_like',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def present_like(x):
  """Creates a DataSlice of present masks with the shape and sparsity of `x`.

  Example:
    x = kd.slice([0], [0, None])
    kd.present_like(x) -> kd.slice([[present], [present, None]])

  Args:
    x: DataSlice to match the shape and sparsity of.

  Returns:
    A DataSlice with the same shape and sparsity as `x`.
  """
  return val_like(x, data_slice.DataSlice.from_vals(arolla.present()))


@optools.add_to_registry(aliases=['kde.agg_count'])
@optools.as_lambda_operator(
    'kde.slices.agg_count',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def agg_count(x, ndim=arolla.unspecified()):
  """Returns counts of present items over the last ndim dimensions.

  The resulting slice has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.

  Example:
    ds = kd.slice([[1, None, 1], [3, 4, 5], [None, None]])
    kd.agg_count(ds)  # -> kd.slice([2, 3, 0])
    kd.agg_count(ds, ndim=1)  # -> kd.slice([2, 3, 0])
    kd.agg_count(ds, ndim=2)  # -> kd.slice(5)

  Args:
    x: A DataSlice.
    ndim: The number of dimensions to aggregate over. Requires 0 <= ndim <=
      get_ndim(x).
  """
  x = jagged_shape_ops.flatten_last_ndim(x, ndim)
  flat_units = arolla_bridge.to_arolla_dense_array_unit(masking.has(x))
  shape = jagged_shape_ops.get_shape(x)
  flat_res = M.array.count(flat_units, into=M.jagged.edge_at(shape, -1))
  return arolla_bridge.to_data_slice(
      flat_res, M.jagged.remove_dims(shape, from_dim=-1)
  )


@optools.add_to_registry(aliases=['kde.cum_count'])
@optools.as_lambda_operator(
    'kde.slices.cum_count',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def cum_count(x, ndim=arolla.unspecified()):
  """Computes a partial count of present items over the last `ndim` dimensions.

  If `ndim` isn't specified, it defaults to 1 (count over the last dimension).

  Example:
    x = kd.slice([[1, None, 1, 1], [3, 4, 5]])
    kd.cum_count(x, ndim=1)  # -> kd.slice([[1, None, 2, 3], [1, 2, 3]])
    kd.cum_count(x, ndim=2)  # -> kd.slice([[1, None, 2, 3], [4, 5, 6]])

  Args:
    x: A DataSlice.
    ndim: The number of trailing dimensions to count within. Requires 0 <= ndim
      <= get_ndim(x).

  Returns:
    A DataSlice of INT64 with the same shape and sparsity as `x`.
  """
  x_shape = jagged_shape_ops.get_shape(x)
  flat_units = arolla_bridge.to_arolla_dense_array_unit(masking.has(x))
  flat_res = M.array.cum_count(
      flat_units,
      over=M.jagged.edge_at(
          jagged_shape_ops.flatten_last_ndim(x_shape, ndim), -1
      ),
  )
  return arolla_bridge.to_data_slice(flat_res, x_shape)


@optools.add_to_registry(aliases=['kde.count'])
@optools.as_lambda_operator('kde.slices.count')
def count(x):
  """Returns the count of present items over all dimensions.

  The result is a zero-dimensional DataItem.

  Args:
    x: A DataSlice of numbers.
  """
  return agg_count(jagged_shape_ops.flatten(x))


@optools.add_to_registry(aliases=['kde.agg_size'])
@optools.as_lambda_operator(
    'kde.slices.agg_size',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def agg_size(x, ndim=arolla.unspecified()):
  """Returns number of items in `x` over the last ndim dimensions.

  Note that it counts missing items, which is different from `kd.count`.

  The resulting DataSlice has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.

  Example:
    ds = kd.slice([[1, None, 1], [3, 4, 5], [None, None]])
    kd.agg_size(ds)  # -> kd.slice([3, 3, 2])
    kd.agg_size(ds, ndim=1)  # -> kd.slice([3, 3, 2])
    kd.agg_size(ds, ndim=2)  # -> kd.slice(8)

  Args:
    x: A DataSlice.
    ndim: The number of dimensions to aggregate over. Requires 0 <= ndim <=
      get_ndim(x).

  Returns:
    A DataSlice of number of items in `x` over the last `ndim` dimensions.
  """
  return agg_count(present_shaped_as(x), ndim)


@optools.add_to_registry(aliases=['kde.size'])
@optools.as_lambda_operator('kde.slices.size')
def size(x):
  """Returns the number of items in `x`, including missing items.

  Args:
    x: A DataSlice.

  Returns:
    The size of `x`.
  """
  return jagged_shape_ops.size(jagged_shape_ops.get_shape(x))


@optools.add_to_registry(aliases=['kde.align'])
@arolla.optools.as_backend_operator(
    'kde.slices.align',
    qtype_constraints=[
        qtype_utils.expect_data_slice_args(P.args),
    ],
    qtype_inference_expr=P.args,
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
)
def align(*args):  # pylint: disable=unused-argument
  """Expands all of the DataSlices in `args` to the same common shape.

  All DataSlices must be expandable to the shape of the DataSlice with the
  largest number of dimensions.

  Example:
    kd.align(kd.slice([[1, 2, 3], [4, 5]]), kd.slice('a'), kd.slice([1, 2]))
    # Returns:
    # (
    #   kd.slice([[1, 2, 3], [4, 5]]),
    #   kd.slice([['a', 'a', 'a'], ['a', 'a']]),
    #   kd.slice([[1, 1, 1], [2, 2]]),
    # )

  Args:
    *args: DataSlices to align.

  Returns:
    A tuple of aligned DataSlices, matching `args`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.as_backend_operator(
    'kde.slices._collapse', qtype_inference_expr=qtypes.DATA_SLICE
)
def _collapse(ds):  # pylint: disable=unused-argument
  """Creates a new DataSlice by collapsing 'ds' over its last dimension.

  Args:
    ds: DataSlice to be collapsed

  Returns:
    Collapsed DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.collapse'])
@optools.as_lambda_operator(
    'kde.slices.collapse',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def collapse(x, ndim=arolla.unspecified()):
  """Collapses the same items over the last ndim dimensions.

  Missing items are ignored. For each collapse aggregation, the result is
  present if and only if there is at least one present item and all present
  items are the same.

  The resulting slice has `rank = rank - ndim` and shape: `shape =
  shape[:-ndim]`.

  Example:
    ds = kd.slice([[1, None, 1], [3, 4, 5], [None, None]])
    kd.collapse(ds)  # -> kd.slice([1, None, None])
    kd.collapse(ds, ndim=1)  # -> kd.slice([1, None, None])
    kd.collapse(ds, ndim=2)  # -> kd.slice(None)

  Args:
    x: A DataSlice.
    ndim: The number of dimensions to collapse into. Requires 0 <= ndim <=
      get_ndim(x).

  Returns:
    Collapsed DataSlice.
  """
  return _collapse(jagged_shape_ops.flatten_last_ndim(x, ndim))


@arolla.optools.as_backend_operator(
    'kde.slices._concat_or_stack', qtype_inference_expr=qtypes.DATA_SLICE
)
def _concat_or_stack(stack, ndim, *args):  # pylint: disable=unused-argument,redefined-outer-name
  """Concatenates or stacks a tuple of DataSlices on dimension `rank-ndim`.

  Each of `args` must be a DataSlice with the same rank, and that rank must
  be at least `ndim`.

  `ndim` must be positive if `stack` is False, and non-negative if `stack` is
  True.

  If `stack` is True, a new unit dimension is inserted as the concatenation
  dimension, i.e. inserted so that it is just before the dimension that would
  have otherwise been concatenated. This is consistent with the relationship
  between `np.stack` and `np.concat`.

  This combined concat-or-stack is implemented as a single backend operator
  because of the symmetry between the concat and stack operators themselves,
  and because `kd.concat(..., ndim=0)` is actually a stack operation and needs
  to be dispatched uniformly.

  Args:
    stack: Whether to stack instead of concatenating.
    ndim: The rank-relative dimension to concatenate along.
    *args: The DataSlices to concatenate.

  Returns:
    The concatenated DataSlice. If inputs all use the same DataBag, the result
    will have that DataBag, If inputs use different DataBags, the result will
    use a new immutable DataBag merged from the extraction of all inputs.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.concat'])
@optools.as_lambda_operator(
    'kde.slices.concat',
    qtype_constraints=[
        qtype_utils.expect_data_slice_args(P.args),
        [
            M.qtype.get_field_count(P.args) > 0,
            'expected a nonzero number of args',
        ],
        qtype_utils.expect_data_slice(P.ndim),
    ],
)
def concat(*args, ndim=1):
  """Returns the concatenation of the given DataSlices on dimension `rank-ndim`.

  All given DataSlices must have the same rank, and the shapes of the first
  `rank-ndim` dimensions must match. If they have incompatible shapes, consider
  using `kd.align(*args)`, `arg.repeat(...)`, or `arg.expand_to(other_arg, ...)`
  to bring them to compatible shapes first.

  The shape of the concatenated result is the following:
    1) the shape of the first `rank-ndim` dimensions remains the same
    2) the shape of the concatenation dimension is the element-wise sum of the
      shapes of the arguments' concatenation dimensions
    3) the shapes of the last `ndim-1` dimensions are interleaved within the
      groups implied by the concatenation dimension

  Alteratively, if we think of each input DataSlice as a nested Python list,
  this operator simultaneously iterates over the inputs at depth `rank-ndim`,
  concatenating the root lists of the corresponding nested sub-lists from each
  input.

  For example,
  a = kd.slice([[[1, 2], [3]], [[5], [7, 8]]])
  b = kd.slice([[[1], [2]], [[3], [4]]])

  kd.concat(a, b, ndim=1) -> [[[1, 2, 1], [3, 2]], [[5, 3], [7, 8, 4]]]
  kd.concat(a, b, ndim=2) -> [[[1, 2], [3], [1], [2]], [[5], [7, 8], [3], [4]]]
  kd.concat(a, b, ndim=3) -> [[[1, 2], [3]], [[5], [7, 8]],
                              [[1], [2]], [[3], [4]]]
  kd.concat(a, b, ndim=4) -> raise an exception
  kd.concat(a, b) -> the same as kd.concat(a, b, ndim=1)

  The reason auto-broadcasting is not supported is that such behavior can be
  confusing and often not what users want. For example,

  a = kd.slice([[[1, 2], [3]], [[5], [7, 8]]])
  b = kd.slice([[1, 2], [3, 4]])
  kd.concat(a, b) -> should it be which of the following?
    [[[1, 2, 1, 2], [3, 1, 2]], [[5, 3, 4], [7, 8, 3, 4]]]
    [[[1, 2, 1, 1], [3, 2]], [[5, 3], [7, 8, 4, 4]]]
    [[[1, 2, 1], [3, 2]], [[5, 3], [7, 8, 4]]]

  Args:
    *args: The DataSlices to concatenate.
    ndim: The number of last dimensions to concatenate (default 1).

  Returns:
    The contatenation of the input DataSlices on dimension `rank-ndim`. In case
    the input DataSlices come from different DataBags, this will refer to a
    new merged immutable DataBag.
  """
  args = arolla.optools.fix_trace_args(args)
  return M.core.apply_varargs(
      _concat_or_stack, data_slice.DataSlice.from_vals(False), ndim, args
  )


@arolla.optools.as_backend_operator(
    'kde.slices._dense_rank', qtype_inference_expr=qtypes.DATA_SLICE
)
def _dense_rank(x, descending):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.dense_rank'])
@optools.as_lambda_operator(
    'kde.slices.dense_rank',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.descending),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def dense_rank(x, descending=False, ndim=arolla.unspecified()):
  """Returns dense ranks of items in `x` over the last `ndim` dimensions.

  Items are grouped over the last `ndim` dimensions and ranked within the group.
  `ndim` is set to 1 by default if unspecified. Ranks are integers starting from
  0, assigned to values in ascending order by default.

  By dense ranking ("1 2 2 3" ranking), equal items are assigned to the same
  rank and the next items are assigned to that rank plus one (i.e. no gap
  between the rank numbers).

  NaN values are ranked lowest regardless of the order of ranking. Ranks of
  missing items are missing in the result.

  Example:

    ds = kd.slice([[4, 3, None, 3], [3, None, 2, 1]])
    kd.dense_rank(x) -> kd.slice([[1, 0, None, 0], [2, None, 1, 0]])

    kd.dense_rank(x, descending=True) ->
        kd.slice([[0, 1, None, 1], [0, None, 1, 2]])

    kd.dense_rank(x, ndim=0) -> kd.slice([[0, 0, None, 0], [0, None, 0, 0]])

    kd.dense_rank(x, ndim=2) -> kd.slice([[3, 2, None, 2], [2, None, 1, 0]])

  Args:
    x: DataSlice to rank.
    descending: If true, items are compared in descending order.
    ndim: The number of dimensions to rank over. Requires 0 <= ndim <=
      get_ndim(x).

  Returns:
    A DataSlice of dense ranks.
  """
  res = _dense_rank(
      jagged_shape_ops.flatten_last_ndim(x, ndim),
      descending,
  )
  # Need to reshape back to the original shape.
  return jagged_shape_ops.reshape(res, jagged_shape_ops.get_shape(x))


@optools.add_to_registry(aliases=['kde.expand_to'])
@optools.as_lambda_operator(
    'kde.slices.expand_to',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.target),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def expand_to(x, target, ndim=arolla.unspecified()):
  """Expands `x` based on the shape of `target`.

  When `ndim` is not set, expands `x` to the shape of
  `target`. The dimensions of `x` must be the same as the first N
  dimensions of `target` where N is the number of dimensions of `x`. For
  example,

  Example 1:
    x: kd.slice([[1, 2], [3]])
    target: kd.slice([[[0], [0, 0]], [[0, 0, 0]]])
    result: kd.slice([[[1], [2, 2]], [[3, 3, 3]]])

  Example 2:
    x: kd.slice([[1, 2], [3]])
    target: kd.slice([[[0]], [[0, 0, 0]]])
    result: incompatible shapes

  Example 3:
    x: kd.slice([[1, 2], [3]])
    target: kd.slice([0, 0])
    result: incompatible shapes

  When `ndim` is set, the expansion is performed in 3 steps:
    1) the last N dimensions of `x` are first imploded into lists
    2) the expansion operation is performed on the DataSlice of lists
    3) the lists in the expanded DataSlice are exploded

  The result will have M + ndim dimensions where M is the number
  of dimensions of `target`.

  For example,

  Example 4:
    x: kd.slice([[1, 2], [3]])
    target: kd.slice([[1], [2, 3]])
    ndim: 1
    result: kd.slice([[[1, 2]], [[3], [3]]])

  Example 5:
    x: kd.slice([[1, 2], [3]])
    target: kd.slice([[1], [2, 3]])
    ndim: 2
    result: kd.slice([[[[1, 2], [3]]], [[[1, 2], [3]], [[1, 2], [3]]]])

  Args:
    x: DataSlice to expand.
    target: target DataSlice.
    ndim: the number of dimensions to implode during expansion.

  Returns:
    Expanded DataSlice
  """
  return jagged_shape_ops.expand_to_shape(
      x, jagged_shape_ops.get_shape(target), ndim
  )


@optools.add_to_registry(aliases=['kde.is_expandable_to'])
@optools.as_lambda_operator(
    'kde.slices.is_expandable_to',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.target),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def is_expandable_to(x, target, ndim=arolla.unspecified()):
  """Returns true if `x` is expandable to `target`.

  Args:
    x: DataSlice to expand.
    target: target DataSlice.
    ndim: the number of dimensions to implode before expansion.

  See `expand_to` for a detailed description of expansion.
  """
  return jagged_shape_ops.is_expandable_to_shape(
      x, jagged_shape_ops.get_shape(target), ndim
  )


@optools.add_to_registry(aliases=['kde.get_ndim'])
@optools.as_lambda_operator(
    'kde.slices.get_ndim',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def get_ndim(x):
  """Returns the number of dimensions of DataSlice `x`."""
  return jagged_shape_ops.rank(jagged_shape_ops.get_shape(x))


@optools.add_to_registry(aliases=['kde.take', 'kde.slices.at', 'kde.at'])
@optools.as_backend_operator(
    'kde.slices.take',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.indices),
    ],
)
def take(x, indices):  # pylint: disable=unused-argument
  """Returns a new DataSlice with items at provided indices.

  `indices` must have INT32 or INT64 dtype or OBJECT schema holding INT32 or
  INT64 items.

  Indices in the DataSlice `indices` are based on the last dimension of the
  DataSlice `x`. Negative indices are supported and out-of-bound indices result
  in missing items.

  If ndim(x) - 1 > ndim(indices), indices are broadcasted to shape(x)[:-1].
  If ndim(x) <= ndim(indices), indices are unchanged but shape(x)[:-1] must be
  broadcastable to shape(indices).

  Example:
    x = kd.slice([[1, None, 2], [3, 4]])
    kd.take(x, kd.item(1))  # -> kd.slice([[None, 4]])
    kd.take(x, kd.slice([0, 1]))  # -> kd.slice([1, 4])
    kd.take(x, kd.slice([[0, 1], [1]]))  # -> kd.slice([[1, None], [4]])
    kd.take(x, kd.slice([[[0, 1], []], [[1], [0]]]))
      # -> kd.slice([[[1, None]], []], [[4], [3]]])
    kd.take(x, kd.slice([3, -3]))  # -> kd.slice([None, None])
    kd.take(x, kd.slice([-1, -2]))  # -> kd.slice([2, 3])
    kd.take(x, kd.slice('1')) # -> dtype mismatch error
    kd.take(x, kd.slice([1, 2, 3])) -> incompatible shape

  Args:
    x: DataSlice to be indexed
    indices: indices used to select items

  Returns:
    A new DataSlice with items selected by indices.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.group_by_indices'])
@arolla.optools.as_backend_operator(
    'kde.slices.group_by_indices',
    qtype_constraints=[
        (
            M.qtype.get_field_count(P.args) > 0,
            'expected at least one argument',
        ),
        qtype_utils.expect_data_slice_args(P.args),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
)
def group_by_indices(*args):  # pylint: disable=unused-argument
  """Returns a indices DataSlice with injected grouped_by dimension.

  The resulting DataSlice has get_ndim() + 1. The first `get_ndim() - 1`
  dimensions are unchanged. The last two dimensions corresponds to the groups
  and the items within the groups.

  Values of the DataSlice are the indices of the items within the parent
  dimension. `kde.take(x, kde.group_by_indices(x))` would group the items in
  `x` by their values.

  Groups are ordered by the appearance of the first object in the group.

  Example 1:
    x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
    result: kd.slice([[0, 3, 6], [1, 5, 7], [2, 4]])

    We have three groups in order: 1, 3, 2. Each sublist contains the indices of
    the items in the original DataSlice.

  Example 2:
    x: kd.slice([[1, 2, 1, 3, 1, 3], [1, 3, 1]])
    result: kd.slice([[[0, 2, 4], [1], [3, 5]], [[0, 2], [1]]])

    We have three groups in the first sublist in order: 1, 2, 3 and two groups
    in the second sublist in order: 1, 3.
    Each sublist contains the indices of the items in the original sublist.

  Example 3:
    x: kd.slice([1, 3, 2, 1, None, 3, 1, None])
    result: kd.slice([[0, 3, 6], [1, 5], [2]])

    Missing values are not listed in the result.

  Example 4:
    x: kd.slice([1, 2, 3, 1, 2, 3, 1, 3]),
    y: kd.slice([7, 4, 0, 9, 4, 0, 7, 0]),
    result: kd.slice([[0, 6], [1, 4], [2, 5, 7], [3]])

    With several arguments keys is a tuple.
    In this example we have the following groups: (1, 7), (2, 4), (3, 0), (1, 9)

  Args:
    *args: DataSlices keys to group by. All data slices must have the same
      shape. Scalar DataSlices are not supported.

  Returns:
    INT64 DataSlice with indices and injected grouped_by dimension.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.group_by_indices_sorted'])
@arolla.optools.as_backend_operator(
    'kde.slices.group_by_indices_sorted',
    qtype_constraints=[
        (
            M.qtype.get_field_count(P.args) > 0,
            'expected at least one argument',
        ),
        qtype_utils.expect_data_slice_args(P.args),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
)
def group_by_indices_sorted(*args):  # pylint: disable=unused-argument
  """Similar to `group_by_indices` but groups are sorted by the value.

  Each argument must contain the values of one type.

  Mixed types are not supported.
  ExprQuote and DType are not supported.

  Example 1:
    x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
    result: kd.slice([[0, 3, 6], [2, 4], [1, 5, 7]])

    We have three groups in order: 1, 2, 3. Each sublist contains the indices of
    the items in the original DataSlice.

  Example 2:
    x: kd.slice([1, 2, 3, 1, 2, 3, 1, 3]),
    y: kd.slice([9, 4, 0, 3, 4, 0, 9, 0]),
    result: kd.slice([[3], [0, 6], [1, 4], [2, 5, 7]])

    With several arguments keys is a tuple.
    In this example we have the following groups: (1, 3), (1, 9), (2, 4), (3, 0)

  Args:
    *args: DataSlices keys to group by. All data slices must have the same
      shape. Scalar DataSlices are not supported.

  Returns:
    INT64 DataSlice with indices and injected grouped_by dimension.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.group_by'])
@optools.as_lambda_operator(
    'kde.slices.group_by',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_args(P.args),
    ],
)
def group_by(x, *args):
  """Returns permutation of `x` with injected grouped_by dimension.

  The resulting DataSlice has get_ndim() + 1. The first `get_ndim() - 1`
  dimensions are unchanged. The last two dimensions corresponds to the groups
  and the items within the groups.

  Values of the result is a permutation of `x`. `args` are used for the grouping
  keys. If length of `args` is greater than 1, the key is a tuple.
  If `args` is empty, the key is `x`.

  Groups are ordered by the appearance of the first item in the group.

  Example 1:
    x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
    result: kd.slice([[1, 1, 1], [3, 3, 3], [2, 2]])

  Example 2:
    x: kd.slice([[1, 2, 1, 3, 1, 3], [1, 3, 1]])
    result: kd.slice([[[1, 1, 1], [2], [3, 3]], [[1, 1], [3]]])

  Example 3:
    x: kd.slice([1, 3, 2, 1, None, 3, 1, None])
    result: kd.slice([[1, 1, 1], [3, 3], [2]])

    Missing values are not listed in the result.

  Example 4:
    x: kd.slice([1, 2, 3, 4, 5, 6, 7, 8]),
    y: kd.slice([7, 4, 0, 9, 4, 0, 7, 0]),
    result: kd.slice([[1, 7], [2, 5], [3, 6, 8], [4]])

    When *args is present, `x` is not used for the key.

  Example 5:
    x: kd.slice([1, 2, 3, 4, None, 6, 7, 8]),
    y: kd.slice([7, 4, 0, 9, 4,    0, 7, None]),
    result: kd.slice([[1, 7], [2, None], [3, 6], [4]])

    Items with missing key is not listed in the result.
    Missing `x` values are missing in the result.

  Example 6:
    x: kd.slice([1, 2, 3, 4, 5, 6, 7, 8]),
    y: kd.slice([7, 4, 0, 9, 4, 0, 7, 0]),
    z: kd.slice([A, D, B, A, D, C, A, B]),
    result: kd.slice([[1, 7], [2, 5], [3], [4], [6]])

    When *args has two or more values, the  key is a tuple.
    In this example we have the following groups:
    (7, A), (4, D), (0, B), (9, A), (0, C)

  Args:
    x: DataSlice to group.
    *args: DataSlices keys to group by. All data slices must have the same shape
      as x. Scalar DataSlices are not supported. If not present, `x` is used as
      the key.

  Returns:
    DataSlice with the same shape and schema as `x` with injected grouped
    by dimension.
  """
  args = arolla.optools.fix_trace_args(args)
  dispatch_op = arolla.types.DispatchOperator(
      'x, args',
      x_is_key_case=arolla.types.DispatchCase(
          take(P.x, group_by_indices(P.x)),
          condition=M.qtype.get_field_count(P.args) == 0,
      ),
      # TODO: add assertion: x has the same shape as other args.
      default=take(P.x, M.core.apply_varargs(group_by_indices, P.args)),
  )
  return dispatch_op(x, args)


@optools.add_to_registry(aliases=['kde.index'])
@optools.as_lambda_operator(
    'kde.slices.index',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.dim),
    ],
)
def index(x, dim=arolla.unspecified()):
  """Returns the indices of the elements computed over the last dim dimensions.

  The resulting slice has the same shape as the input.

  Example:
    ds = kd.slice([
        [
            ['a', None, 'c'],
            ['d', 'e']
        ],
        [
            [None, 'g'],
            ['h', 'i', 'j']
        ]
    ])
    kd.index(ds, dim=0)
      # -> kd.slice([[[0, None, 0], [0, 0]], [[None, 1], [1, 1, 1]]])
    kd.index(ds, dim=1)
      # -> kd.slice([[[0, None, 0], [1, 1]], [[None, 0], [1, 1, 1]]])
    kd.index(ds, dim=2)
      # -> kd.slice([[[0, None, 2], [0, 1]], [[None, 1], [0, 1, 2]]])

    kd.index(ds) -> kd.index(ds, dim=ds.get_ndim() - 1)

  Args:
    x: A DataSlice.
    dim: The dimension to compute indices over. Requires 0 <= dim < get_ndim(x).
      If unspecified, it is set to the last dimension of x.
  """
  x = assertion.with_assertion(
      x,
      get_ndim(x) != 0,
      'kde.slices.index: argument `x` must have non-zero rank',
  )

  dim = M.core.default_if_unspecified(dim, get_ndim(x) - 1)

  ndim = get_ndim(x) - dim - 1
  ndim = assertion.with_assertion(
      ndim,
      (ndim < get_ndim(x)) & (ndim >= 0),
      'kde.slices.index: expected 0 <= dim < rank',
  )

  aggregated = masking.agg_has(x, ndim)
  flat_units = arolla_bridge.to_arolla_dense_array_unit(masking.has(aggregated))
  shape = jagged_shape_ops.get_shape(aggregated)
  flat_res = M.array.agg_index(
      flat_units,
      over=M.jagged.edge_at(
          shape,
          -1,
      ),
  )
  shaped_res = arolla_bridge.to_data_slice(flat_res, shape)
  return expand_to(shaped_res, x)


@optools.as_backend_operator('kde.slices._inverse_mapping')
def _inverse_mapping(x):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.inverse_mapping'])
@optools.as_lambda_operator(
    'kde.slices.inverse_mapping',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def inverse_mapping(x, ndim=arolla.unspecified()):
  """Returns inverse permutations of indices over the last `ndim` dimension.

  It interprets `indices` over the last `ndim` dimension as a permutation and
  substitute with the corresponding inverse permutation. `ndim` is set to 1 by
  default if unspecified. It fails when `indices` is not a valid permutation.

  Example:
    indices = kd.slice([[1, 2, 0], [1, None]])
    kd.inverse_mapping(indices)  ->  kd.slice([[2, 0, 1], [None, 0]])

    Explanation:
      indices      = [[1, 2, 0], [1, None]]
      inverse_permutation[1, 2, 0] = [2, 0, 1]
      inverse_permutation[1, None] = [None, 0]

    kd.inverse_mapping(indices, ndim=1) -> raise

    indices = kd.slice([[1, 2, 0], [3, None]])
    kd.inverse_mapping(indices, ndim=2)  ->  kd.slice([[2, 0, 1], [3, None]])

  Args:
    x: A DataSlice of indices.
    ndim: The number of dimensions to compute inverse permutations over.
      Requires 0 <= ndim <= get_ndim(x).

  Returns:
    An inverse permutation of indices.
  """
  res = _inverse_mapping(jagged_shape_ops.flatten_last_ndim(x, ndim))
  # Need to reshape back to the original shape.
  return jagged_shape_ops.reshape(res, jagged_shape_ops.get_shape(x))


@optools.add_to_registry(
    aliases=[
        'kde.inverse_select',
        'kde.slices.reverse_select',
        'kde.reverse_select',
    ]
)
@optools.as_backend_operator(
    'kde.slices.inverse_select',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_slice(P.fltr),
    ],
)
def inverse_select(ds, fltr):  # pylint: disable=unused-argument
  """Creates a DataSlice by putting items in ds to present positions in fltr.

  The shape of `ds` and the shape of `fltr` must have the same rank and the same
  first N-1 dimensions. That is, only the last dimension can be different. The
  shape of `ds` must be the same as the shape of the DataSlice after applying
  `fltr` using kd.select. That is,
  ds.get_shape() == kd.select(fltr, fltr).get_shape().

  Example:
    ds = kd.slice([[1, None], [2]])
    fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
    kd.inverse_select(ds, fltr) -> [[None, 1, None], [2, None]]

    ds = kd.slice([1, None, 2])
    fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
    kd.inverse_select(ds, fltr) -> error due to different ranks

    ds = kd.slice([[1, None, 2]])
    fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
    kd.inverse_select(ds, fltr) -> error due to different N-1 dimensions

    ds = kd.slice([[1], [2]])
    fltr = kd.slice([[None, kd.present, kd.present], [kd.present, None]])
    kd.inverse_select(ds, fltr) -> error due to incompatible shapes

  Note, in most cases, kd.inverse_select is not a strict reverse operation of
  kd.select as kd.select operation is lossy and does not require `ds` and `fltr`
  to have the same rank. That is,
  kd.inverse_select(kd.select(ds, fltr), fltr) != ds.

  The most common use case of kd.inverse_select is to restore the shape of the
  original DataSlice after applying kd.select and performing some operations on
  the subset of items in the original DataSlice. E.g.
    filtered_ds = kd.select(ds, fltr)
    # do something on filtered_ds
    ds = kd.inverse_select(filtered_ds, fltr) | ds

  Args:
    ds: DataSlice to be reverse filtered
    fltr: filter DataSlice with dtype as kd.MASK.

  Returns:
    Reverse filtered DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.isin'])
@optools.as_lambda_operator(
    'kde.slices.isin',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
)
def isin(x, y):
  """Returns a DataItem indicating whether DataItem x is present in y."""
  x = assertion.with_assertion(
      x, get_ndim(x) == 0, 'kde.slices.isin: argument `x` must be a DataItem'
  )
  return masking.any_(x == y)


@optools.add_to_registry(aliases=['kde.is_empty'])
@optools.as_backend_operator(
    'kde.slices.is_empty',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def is_empty(x):  # pylint: disable=unused-argument
  """Returns kd.present if all items in the DataSlice are missing."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.is_shape_compatible'])
@optools.as_lambda_operator(
    'kde.slices.is_shape_compatible',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
)
def is_shape_compatible(x, y):
  """Returns present if the shapes of `x` and `y` are compatible.

  Two DataSlices have compatible shapes if dimensions of one DataSlice equal or
  are prefix of dimensions of another DataSlice.

  Args:
    x: DataSlice to check.
    y: DataSlice to check.

  Returns:
    A MASK DataItem indicating whether 'x' and 'y' are compatible.
  """
  return is_expandable_to(x, y) | is_expandable_to(y, x)


@arolla.optools.as_backend_operator(
    'kde.slices._ordinal_rank', qtype_inference_expr=qtypes.DATA_SLICE
)
def _ordinal_rank(x, tie_breaker, descending):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.ordinal_rank'])
@optools.as_lambda_operator(
    'kde.slices.ordinal_rank',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.tie_breaker),
        qtype_utils.expect_data_slice(P.descending),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def ordinal_rank(
    x,
    tie_breaker=arolla.unspecified(),
    descending=False,
    ndim=arolla.unspecified(),
):
  """Returns ordinal ranks of items in `x` over the last `ndim` dimensions.

  Items are grouped over the last `ndim` dimensions and ranked within the group.
  `ndim` is set to 1 by default if unspecified. Ranks are integers starting from
  0, assigned to values in ascending order by default.

  By ordinal ranking ("1 2 3 4" ranking), equal items receive distinct ranks.
  Items are compared by the triple (value, tie_breaker, position) to resolve
  ties. When descending=True, values are ranked in descending order but
  tie_breaker and position are ranked in ascending order.

  NaN values are ranked lowest regardless of the order of ranking. Ranks of
  missing items are missing in the result. If `tie_breaker` is specified, it
  cannot be more sparse than `x`.

  Example:

    ds = kd.slice([[0, 3, None, 6], [5, None, 2, 1]])
    kd.ordinal_rank(x) -> kd.slice([[0, 1, None, 2], [2, None, 1, 0]])

    kd.ordinal_rank(x, descending=True) ->
        kd.slice([[2, 1, None, 0], [0, None, 1, 2]])

    kd.ordinal_rank(x, ndim=0) -> kd.slice([[0, 0, None, 0], [0, None, 0, 0]])

    kd.ordinal_rank(x, ndim=2) -> kd.slice([[0, 3, None, 5], [4, None, 2, 1]])

  Args:
    x: DataSlice to rank.
    tie_breaker: If specified, used to break ties. If `tie_breaker` does not
      fully resolve all ties, then the remaining ties are resolved by their
      positions in the DataSlice.
    descending: If true, items are compared in descending order. Does not affect
      the order of tie breaker and position in tie-breaking compairson.
    ndim: The number of dimensions to rank over. Requires 0 <= ndim <=
      get_ndim(x).

  Returns:
    A DataSlice of ordinal ranks.
  """
  tie_breaker = M.core.default_if_unspecified(
      tie_breaker, data_slice.DataSlice.from_vals(0, schema_constants.INT64)
  )
  res = _ordinal_rank(
      jagged_shape_ops.flatten_last_ndim(x, ndim),
      jagged_shape_ops.flatten_last_ndim(expand_to(tie_breaker, x), ndim),
      descending,
  )
  # Need to reshape back to the original shape.
  return jagged_shape_ops.reshape(res, jagged_shape_ops.get_shape(x))


@optools.add_to_registry(
    aliases=['kde.add_dim', 'kde.slices.add_dim', 'kde.repeat']
)
@optools.as_lambda_operator(
    'kde.slices.repeat',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.sizes),
    ],
)
def repeat(x, sizes):
  """Returns `x` with values repeated according to `sizes`.

  The resulting DataSlice has `rank = rank + 1`. The input `sizes` are
  broadcasted to `x`, and each value is repeated the given number of times.

  Example:
    ds = kd.slice([[1, None], [3]])
    sizes = kd.slice([[1, 2], [3]])
    kd.repeat(ds, sizes)  # -> kd.slice([[[1], [None, None]], [[3, 3, 3]]])

    ds = kd.slice([[1, None], [3]])
    sizes = kd.slice([2, 3])
    kd.repeat(ds, sizes)  # -> kd.slice([[[1, 1], [None, None]], [[3, 3, 3]]])

    ds = kd.slice([[1, None], [3]])
    size = kd.item(2)
    kd.repeat(ds, size)  # -> kd.slice([[[1, 1], [None, None]], [[3, 3]]])

  Args:
    x: A DataSlice of data.
    sizes: A DataSlice of sizes that each value in `x` should be repeated for.
  """
  x_shape = jagged_shape_ops.get_shape(x)
  expanded_sizes = jagged_shape_ops.expand_to_shape(sizes, x_shape)
  edge = M.edge.from_sizes(
      arolla_bridge.to_arolla_dense_array_int64(expanded_sizes)
  )
  target_shape = M.jagged.add_dims(x_shape, edge)
  return jagged_shape_ops.expand_to_shape(x, target_shape)


@optools.add_to_registry(
    aliases=[
        'kde.add_dim_to_present',
        'kde.slices.add_dim_to_present',
        'kde.repeat_present',
    ]
)
@optools.as_lambda_operator(
    'kde.slices.repeat_present',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.sizes),
    ],
)
def repeat_present(x, sizes):
  """Returns `x` with present values repeated according to `sizes`.

  The resulting DataSlice has `rank = rank + 1`. The input `sizes` are
  broadcasted to `x`, and each value is repeated the given number of times.

  Example:
    ds = kd.slice([[1, None], [3]])
    sizes = kd.slice([[1, 2], [3]])
    kd.repeat_present(ds, sizes)  # -> kd.slice([[[1], []], [[3, 3, 3]]])

    ds = kd.slice([[1, None], [3]])
    sizes = kd.slice([2, 3])
    kd.repeat_present(ds, sizes)  # -> kd.slice([[[1, 1], []], [[3, 3, 3]]])

    ds = kd.slice([[1, None], [3]])
    size = kd.item(2)
    kd.repeat_present(ds, size)  # -> kd.slice([[[1, 1], []], [[3, 3]]])

  Args:
    x: A DataSlice of data.
    sizes: A DataSlice of sizes that each value in `x` should be repeated for.
  """
  expanded_sizes = jagged_shape_ops.expand_to_shape(
      sizes, jagged_shape_ops.get_shape(x)
  )
  return repeat(x, masking.cond(masking.has(x), expanded_sizes, 0))


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kde.slices._range',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.start),
        qtype_utils.expect_data_slice(P.end),
    ],
)
def internal_range(start, end):
  """(Internal) Returns a DataSlice with a range of integers."""
  start, end = align(start, end)
  return schema_ops.with_schema(
      (
          index(repeat(start, (math.maximum(math.subtract(end, start), 0) | 0)))
          + start
      ),
      schema_constants.INT64,
  )


@optools.add_to_registry(aliases=['kde.range'])
@optools.as_lambda_operator(
    'kde.slices.range',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.start),
        qtype_utils.expect_data_slice_or_unspecified(P.end),
    ],
)
def _range(start, end=arolla.unspecified()):
  """Returns a DataSlice of INT64s with range [start, end).

  `start` and `end` must be broadcastable to the same shape. The resulting
  DataSlice has one more dimension than the broadcasted shape.

  When `end` is unspecified, `start` is used as `end` and 0 is used as `start`.
  For example,

    kd.range(5) -> kd.slice([0, 1, 2, 3, 4])
    kd.range(2, 5) -> kd.slice([2, 3, 4])
    kd.range(5, 2) -> kd.slice([])  # empty range
    kd.range(kd.slice([2, 4])) -> kd.slice([[0, 1], [0, 1, 2, 3])
    kd.range(kd.slice([2, 4]), 6) -> kd.slice([[2, 3, 4, 5], [4, 5])

  Args:
    start: A DataSlice for start (inclusive) of intervals (unless `end` is
      unspecified, in which case this parameter is used as `end`).
    end: A DataSlice for end (exclusive) of intervals.

  Returns:
    A DataSlice of INT64s with range [start, end).
  """
  return arolla.types.DispatchOperator(
      'start, end',
      unspecified_case=arolla.types.DispatchCase(
          internal_range(data_slice.DataSlice.from_vals(0), P.start),
          condition=P.end == arolla.UNSPECIFIED,
      ),
      default=internal_range(P.start, P.end),
  )(start, end)


@optools.as_backend_operator('kde.slices._select')
def _select(ds, fltr, expand_filter):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.select'])
@arolla.optools.as_lambda_operator(
    'kde.slices.select',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_slice(P.fltr),
        qtype_utils.expect_data_slice(P.expand_filter),
    ],
    experimental_aux_policy='koladata_adhoc_binding_policy[kde.slices.select]',
)
def select(ds, fltr, expand_filter=data_slice.DataSlice.from_vals(True)):
  """Creates a new DataSlice by filtering out missing items in fltr.

  The dimensions of `fltr` needs to be compatible with the dimensions of `ds`.
  By default, `fltr` is expanded to 'ds' and items in `ds` corresponding
  missing items in `fltr` are removed. The last dimension of the resulting
  DataSlice is changed while the first N-1 dimensions are the same as those in
  `ds`.

  Example:
    val = kd.slice([[1, None, 4], [None], [2, 8]])
    kd.select(val, val > 3) -> [[4], [], [8]]

    fltr = kd.slice(
        [[None, kd.present, kd.present], [kd.present], [kd.present, None]])
    kd.select(val, fltr) -> [[None, 4], [None], [2]]

    fltr = kd.slice([kd.present, kd.present, None])
    kd.select(val, fltr) -> [[1, None, 4], [None], []]
    kd.select(val, fltr, expand_filter=False) -> [[1, None, 4], [None]]

  Args:
    ds: DataSlice to be filtered
    fltr: filter DataSlice with dtype as kd.MASK.
    expand_filter: flag indicating if the 'filter' should be expanded to 'ds'

  Returns:
    Filtered DataSlice.
  """
  return _select(
      ds=ds,
      fltr=functor._maybe_call(fltr, ds),  # pylint: disable=protected-access
      expand_filter=arolla_bridge.to_arolla_boolean(expand_filter),
  )


def _select_bind_args(
    ds, fltr, expand_filter=data_slice.DataSlice.from_vals(True)
):
  """Argument binding policy for the `kde.slices.select` operator."""
  if isinstance(fltr, py_types.FunctionType):
    fltr = fltr(ds)
  return (
      py_boxing.as_qvalue_or_expr(ds),
      py_boxing.as_qvalue_or_expr(fltr),
      py_boxing.as_qvalue_or_expr(expand_filter),
  )


arolla.abc.register_adhoc_aux_binding_policy(
    select, _select_bind_args, make_literal_fn=py_boxing.literal
)


@optools.add_to_registry(aliases=['kde.select_present'])
@optools.as_lambda_operator(
    'kde.slices.select_present',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
    ],
)
def select_present(ds):
  """Creates a new DataSlice by removing missing items."""
  return select(ds=ds, fltr=masking.has(ds))


@optools.add_to_registry(aliases=['kde.remove'])
@optools.as_lambda_operator(
    'kde.slices.remove',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_slice(P.fltr),
    ],
)
def remove(ds, fltr):
  """Creates a new DataSlice by filtering out present items in fltr.

  The dimensions of `fltr` needs to be compatible with the dimensions of `ds`.
  By default, `fltr` is expanded to 'ds' and items in `ds` corresponding
  present items in `fltr` are removed. The last dimension of the resulting
  DataSlice is changed while the first N-1 dimensions are the same as those in
  `ds`.

  Example:
    val = kd.slice([[1, None, 4], [None], [2, 8]])
    kd.remove(val, val > 3) -> [[1, None], [None], [2]]

    fltr = kd.slice(
        [[None, None, kd.present], [kd.present], [kd.present, None]])
    kd.remove(val, fltr) -> [[1, None], [None], [8]]

  Args:
    ds: DataSlice to be filtered
    fltr: filter DataSlice with dtype as kd.MASK.

  Returns:
    Filtered DataSlice.
  """
  fltr = assertion.assert_ds_has_primitives_of(
      fltr, MASK, 'kde.slices.remove: argument `fltr` must have kd.MASK dtype'
  )
  return select(ds=ds, fltr=~fltr)


@optools.add_to_registry(aliases=['kde.reverse'])
@optools.as_backend_operator(
    'kde.slices.reverse',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
    ],
)
def reverse(ds):  # pylint: disable=unused-argument
  """Returns a DataSlice with items reversed on the last dimension.

  Example:
    ds = kd.slice([[1, None], [2, 3, 4]])
    kd.reverse(ds) -> [[None, 1], [4, 3, 2]]

    ds = kd.slice([1, None, 2])
    kd.reverse(ds) -> [2, None, 1]

  Args:
    ds: DataSlice to be reversed.

  Returns:
    Reversed on the last dimension DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.sort'])
@optools.as_lambda_operator(
    'kde.slices.sort',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.sort_by),
        qtype_utils.expect_data_slice(P.descending),
    ],
)
def sort(x, sort_by=arolla.unspecified(), descending=False):
  """Sorts the items in `x` over the last dimension.

  When `sort_by` is specified, it is used to sort items in `x`. `sort_by` must
  have the same shape as `x` and cannot be more sparse than `x`. Otherwise,
  items in `x` are compared by their values. Missing items are put in the end of
  the sorted list regardless of the value of `descending`.

  Examples:
    ds = kd.slice([[[2, 1, None, 4], [4, 1]], [[5, 4, None]]])

    kd.sort(ds) -> kd.slice([[[1, 2, 4, None], [1, 4]], [[4, 5, None]]])

    kd.sort(ds, descending=True) ->
        kd.slice([[[4, 2, 1, None], [4, 1]], [[5, 4, None]]])

    sort_by = kd.slice([[[9, 2, 1, 3], [2, 3]], [[9, 7, 9]]])
    kd.sort(ds, sort_by) ->
        kd.slice([[[None, 1, 4, 2], [4, 1]], [[4, 5, None]]])

    kd.sort(kd.slice([1, 2, 3]), kd.slice([5, 4])) ->
        raise due to different shapes

    kd.sort(kd.slice([1, 2, 3]), kd.slice([5, 4, None])) ->
        raise as `sort_by` is more sparse than `x`

  Args:
    x: DataSlice to sort.
    sort_by: DataSlice used for comparisons.
    descending: whether to do descending sort.

  Returns:
    DataSlice with last dimension sorted.
  """
  assert_same_shape = arolla.types.LambdaOperator(
      'x, sort_by',
      assertion.with_assertion(
          P.x,
          M.jagged.equal(
              jagged_shape_ops.get_shape(P.x),
              jagged_shape_ops.get_shape(P.sort_by),
          ),
          'kde.slices.sort: arguments `x` and `sort_by` must have the same'
          ' shape',
      ),
  )
  assert_sparsity_less = arolla.types.LambdaOperator(
      'x, sort_by',
      assertion.with_assertion(
          P.sort_by,
          count(masking.has(P.x) & masking.has_not(P.sort_by)) == 0,
          'kde.slices.sort: trying to sort `x` by `sort_by` that is more'
          ' sparse',
      ),
  )
  sort_by = arolla.types.DispatchOperator(
      'x, sort_by',
      unspecified_cast=arolla.types.DispatchCase(
          P.x, condition=P.sort_by == arolla.UNSPECIFIED
      ),
      default=assert_sparsity_less(
          assert_same_shape(P.x, P.sort_by), P.sort_by
      ),
  )(x, sort_by)
  return take(x, inverse_mapping(ordinal_rank(sort_by, descending=descending)))


@optools.add_to_registry(aliases=['kde.stack'])
@optools.as_lambda_operator(
    'kde.slices.stack',
    qtype_constraints=[
        qtype_utils.expect_data_slice_args(P.args),
        [
            M.qtype.get_field_count(P.args) > 0,
            'expected a nonzero number of args',
        ],
        qtype_utils.expect_data_slice(P.ndim),
    ],
)
def stack(*args, ndim=0):
  """Stacks the given DataSlices, creating a new dimension at index `rank-ndim`.

  The given DataSlices must have the same rank, and the shapes of the first
  `rank-ndim` dimensions must match. If they have incompatible shapes, consider
  using `kd.align(*args)`, `arg.repeat(...)`, or `arg.expand_to(other_arg, ...)`
  to bring them to compatible shapes first.

  The result has the following shape:
    1) the shape of the first `rank-ndim` dimensions remains the same
    2) a new dimension is inserted at `rank-ndim` with uniform shape `len(args)`
    3) the shapes of the last `ndim` dimensions are interleaved within the
      groups implied by the newly-inserted dimension

  Alteratively, if we think of each input DataSlice as a nested Python list,
  this operator simultaneously iterates over the inputs at depth `rank-ndim`,
  wrapping the corresponding nested sub-lists from each input in new lists.

  For example,
  a = kd.slice([[1, None, 3], [4]])
  b = kd.slice([[7, 7, 7], [7]])

  kd.stack(a, b, ndim=0) -> [[[1, 7], [None, 7], [3, 7]], [[4, 7]]]
  kd.stack(a, b, ndim=1) -> [[[1, None, 3], [7, 7, 7]], [[4], [7]]]
  kd.stack(a, b, ndim=2) -> [[[1, None, 3], [4]], [[7, 7, 7], [7]]]
  kd.stack(a, b, ndim=4) -> raise an exception
  kd.stack(a, b) -> the same as kd.stack(a, b, ndim=0)

  Args:
    *args: The DataSlices to stack.
    ndim: The number of last dimensions to stack (default 0).

  Returns:
    The stacked DataSlice. If the input DataSlices come from different DataBags,
    this will refer to a merged immutable DataBag.
  """
  args = arolla.optools.fix_trace_args(args)
  return M.core.apply_varargs(
      _concat_or_stack, data_slice.DataSlice.from_vals(True), ndim, args
  )


def _expect_data_slices_or_slices_or_ellipsis(value):
  """Constrains `value` to be a tuple of DataSlices or Slices or Ellipsis."""
  is_data_slice_or_slice_or_ellipsis = arolla.LambdaOperator(
      'x',
      (P.x == qtypes.DATA_SLICE)
      | M.qtype.is_slice_qtype(P.x)
      | (P.x == qtypes.ELLIPSIS),
  )
  return (
      M.seq.all(
          M.seq.map(
              is_data_slice_or_slice_or_ellipsis,
              M.qtype.get_field_qtypes(value),
          )
      ),
      (
          'all arguments must be DataSlices or Slices or Ellipsis, got:'
          f' {constraints.variadic_name_type_msg(value)}'
      ),
  )


@optools.add_to_registry(
    aliases=['kde.subslice'], repr_fn=op_repr.subslice_repr
)
@arolla.optools.as_backend_operator(
    'kde.slices.subslice',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        _expect_data_slices_or_slices_or_ellipsis(P.slices),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
)
def subslice(x, *slices):  # pylint: disable=unused-argument
  """Slices `x` across all of its dimensions based on the provided `slices`.

  `slices` is a variadic argument for slicing arguments where individual
  slicing argument can be one of the following:

    1) INT32/INT64 DataItem or Python integer wrapped into INT32 DataItem. It is
       used to select a single item in one dimension. It reduces the number of
       dimensions in the resulting DataSlice by 1.
    2) INT32/INT64 DataSlice or Python nested list of integer wrapped into INT32
       DataSlice. It is used to select multiple items in one dimension.
    3) Python slice (e.g. slice(1), slice(1, 3), slice(2, -1)). It is used to
       select a slice of items in one dimension. 'step' is not supported and it
       results in no item if 'start' is larger than or equal to 'stop'.
    4) .../Ellipsis. It can appear at most once in `slices` and used to fill
       corresponding dimensions in `x` but missing in `slices`. It means
       selecting all items in these dimensions.

  If the Ellipsis is not provided, it is added to the **beginning** of `slices`
  by default, which is different from Numpy. Individual slicing argument is used
  to slice corresponding dimension in `x`.

  The slicing algorithm can be thought as:
    1) implode `x` recursively to a List DataItem
    2) explode the List DataItem recursively with the slicing arguments (i.e.
       imploded_x[slice])

  Example 1:
    x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
    kd.subslice(x, 0)
      => kd.slice([[1, 3], [4], [7, 8]])

  Example 2:
    x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
    kd.subslice(x, 0, 1, kd.item(0))
      => kd.item(3)

  Example 3:
    x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
    kd.subslice(x, slice(0, -1))
      => kd.slice([[[1], []], [[4, 5]], [[], [8]]])

  Example 4:
    x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
    kd.subslice(x, slice(0, -1), slice(0, 1), slice(1, None))
       => kd.slice([[[2], []], [[5, 6]]])

  Example 5 (also see Example 6/7 for using DataSlices for subslicing):
    x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
    kd.subslice(x, kd.slice([1, 2]), kd.slice([[0, 0], [1, 0]]), kd.slice(0))
      => kd.slice([[4, 4], [8, 7]])

  Example 6:
    x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
    kd.subslice(x, kd.slice([1, 2]), ...)
      => kd.slice([[[4, 5, 6]], [[7], [8, 9]]])

  Example 7:
    x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
    kd.subslice(x, kd.slice([1, 2]), kd.slice([[0, 0], [1, 0]]), ...)
      => kd.slice([[[4, 5, 6], [4, 5, 6]], [[8, 9], [7]]])

  Example 8:
    x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
    kd.subslice(x, ..., slice(1, None))
      => kd.slice([[[2], []], [[5, 6]], [[], [9]]])

  Example 9:
    x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
    kd.subslice(x, 2, ..., slice(1, None))
      => kd.slice([[], [9]])

  Example 10:
    x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
    kd.subslice(x, ..., 2, ...)
      => error as ellipsis can only appear once

  Example 11:
    x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
    kd.subslice(x, 1, 2, 3, 4)
      => error as at most 3 slicing arguments can be provided

  Note that there is a shortcut `ds.S[*slices] for this operator which is more
  commonly used and the Python slice can be written as [start:end] format. For
  example:
    kd.subslice(x, 0) == x.S[0]
    kd.subslice(x, 0, 1, kd.item(0)) == x.S[0, 1, kd.item(0)]
    kd.subslice(x, slice(0, -1)) == x.S[0:-1]
    kd.subslice(x, slice(0, -1), slice(0, 1), slice(1, None))
      == x.S[0:-1, 0:1, 1:]
    kd.subslice(x, ..., slice(1, None)) == x.S[..., 1:]
    kd.subslice(x, slice(1, None)) == x.S[1:]

  Args:
    x: DataSlice to slice.
    *slices: variadic slicing argument.

  Returns:
    A DataSlice with selected items
  """
  raise NotImplementedError('implemented in the backend')


optools.add_to_registry(
    'kde.slices._subslice_for_slicing_helper',
    repr_fn=op_repr.subslicehelper_repr,
)(subslice)


@optools.add_to_registry(aliases=['kde.translate'])
@optools.as_backend_operator(
    'kde.slices.translate',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.keys_to),
        qtype_utils.expect_data_slice(P.keys_from),
        qtype_utils.expect_data_slice(P.values_from),
    ],
)
def translate(keys_to, keys_from, values_from):  # pylint: disable=unused-argument
  """Translates `keys_to` based on `keys_from`->`values_from` mapping.

  The translation is done by matching keys from `keys_from` to `keys_to` over
  the last dimension of `keys_to`. `keys_from` cannot have duplicate keys within
  each group of the last dimension. Also see kd.translate_group.

  `values_from` is first broadcasted to `keys_from` and the first N-1 dimensions
  of `keys_from` and `keys_to` must be the same. The resulting DataSlice has the
  same shape as `keys_to` and the same DataBag as `values_from`.

  `keys_from` and `keys_to` must have the same schema.

  Missing items or items with no matching keys in `keys_from` result in missing
  items in the resulting DataSlice.

  For example:

  keys_to = kd.slice([['a', 'd'], ['c', None]])
  keys_from = kd.slice([['a', 'b'], ['c', None]])
  values_from = kd.slice([[1, 2], [3, 4]])
  kd.translate(keys_to, keys_from, values_from) ->
      kd.slice([[1, None], [3, None]])

  Args:
    keys_to: DataSlice of keys to be translated.
    keys_from: DataSlice of keys to be matched.
    values_from: DataSlice of values to be matched.

  Returns:
    A DataSlice of translated values.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.translate_group'])
@optools.as_lambda_operator(
    'kde.slices.translate_group',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.keys_to),
        qtype_utils.expect_data_slice(P.keys_from),
        qtype_utils.expect_data_slice(P.values_from),
    ],
)
def translate_group(keys_to, keys_from, values_from):
  """Translates `keys_to` based on `keys_from`->`values_from` mapping.

  The translation is done by creating an additional dimension under `keys_to`
  and putting items in `values_from` to this dimension by matching keys from
  `keys_from` to `keys_to` over the last dimension of `keys_to`.
  `keys_to` can have duplicate keys within each group of the last
  dimension.

  `values_from` and `keys_from` must have the same shape and the first N-1
  dimensions of `keys_from` and `keys_to` must be the same. The shape of
  resulting DataSlice is the combination of the shape of `keys_to` and an
  injected group_by dimension.

  `keys_from` and `keys_to` must have the same schema.

  Missing items or items with no matching keys in `keys_from` result in empty
  groups in the resulting DataSlice.

  For example:

  keys_to = kd.slice(['a', 'c', None, 'd', 'e'])
  keys_from = kd.slice(['a', 'c', 'b', 'c', 'a', 'e'])
  values_from = kd.slice([1, 2, 3, 4, 5, 6])
  kd.translate_group(keys_to, keys_from, values_from) ->
    kd.slice([[1, 5], [2, 4], [], [], [6]])

  Args:
    keys_to: DataSlice of keys to be translated.
    keys_from: DataSlice of keys to be matched.
    values_from: DataSlice of values to be matched.

  Returns:
    A DataSlice of translated values.
  """
  assert_same_shape = arolla.types.LambdaOperator(
      'keys_from, values_from',
      assertion.with_assertion(
          P.keys_from,
          M.jagged.equal(
              jagged_shape_ops.get_shape(P.keys_from),
              jagged_shape_ops.get_shape(P.values_from),
          ),
          'kde.slices.translate_group: `keys_from` and `values_from` must have'
          ' the same shape',
      ),
  )
  keys_from = assert_same_shape(keys_from, values_from)

  # Group keys_from and values_from by keys_from. The results have one more
  # group_by dimension.
  grouped_indices = group_by_indices(keys_from)
  unique_keys = collapse(take(keys_from, grouped_indices))
  grouped_values = take(values_from, grouped_indices)

  # Compute the translated_indices of keys_to in keys_from
  translated_indices = translate(keys_to, unique_keys, index(unique_keys))

  # Keep the first N-2 dimensions, select items in N-1 dimension by
  # translated_indices and keep the last group_by dimension.
  return subslice(grouped_values, ..., translated_indices, slice(None))


@optools.add_to_registry(aliases=['kde.unique'])
@optools.as_backend_operator(
    'kde.slices.unique',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.sort),
    ],
)
def unique(x, sort=False):  # pylint: disable=redefined-outer-name,unused-argument
  """Returns a DataSlice with unique values within each dimension.

  The resulting DataSlice has the same rank as `x`, but a different shape.
  The first `get_ndim(x) - 1` dimensions are unchanged. The last dimension
  contains the unique values.

  If `sort` is False elements are ordered by the appearance of the first item.

  If `sort` is True:
  1. Elements are ordered by the value.
  2. Mixed types are not supported.
  3. ExprQuote and DType are not supported.

  Example 1:
    x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
    sort: False
    result: kd.unique([1, 3, 2])

  Example 2:
    x: kd.slice([[1, 2, 1, 3, 1, 3], [3, 1, 1]])
    sort: False
    result: kd.slice([[1, 2, 3], [3, 1]])

  Example 3:
    x: kd.slice([1, 3, 2, 1, None, 3, 1, None])
    sort: False
    result: kd.slice([1, 3, 2])

    Missing values are ignored.

  Example 4:
    x: kd.slice([[1, 3, 2, 1, 3, 1, 3], [3, 1, 1]])
    sort: True
    result: kd.slice([[1, 2, 3], [1, 3]])

  Args:
    x: DataSlice to find unique values in.
    sort: whether elements must be ordered by the value.

  Returns:
    DataSlice with the same rank and schema as `x` with unique values in the
    last dimension.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.zip'])
@optools.as_lambda_operator(
    'kde.slices.zip',
    qtype_constraints=[
        qtype_utils.expect_data_slice_args(P.args),
        [
            M.qtype.get_field_count(P.args) > 0,
            'expected a nonzero number of args',
        ],
    ],
)
def _zip(*args):
  """Zips the given DataSlices into a new DataSlice with a new last dimension.

  Input DataSlices are automatically aligned. The result has the shape of the
  aligned inputs, plus a new last dimension with uniform shape `len(args)`
  containing the values from each input.

  For example,
  a = kd.slice([1, 2, 3, 4])
  b = kd.slice([5, 6, 7, 8])
  c = kd.slice(['a', 'b', 'c', 'd'])
  kd.zip(a, b, c) -> [[1, 5, 'a'], [2, 6, 'b'], [3, 7, 'c'], [4, 8, 'd']]

  a = kd.slice([[1, None, 3], [4]])
  b = kd.slice([7, None])
  kd.zip(a, b) ->  [[[1, 7], [None, 7], [3, 7]], [[4, None]]]

  Args:
    *args: The DataSlices to zip.

  Returns:
    The zipped DataSlice. If the input DataSlices come from different DataBags,
    this will refer to a merged immutable DataBag.
  """
  args = arolla.optools.fix_trace_args(args)
  return M.core.apply_varargs(
      _concat_or_stack,
      data_slice.DataSlice.from_vals(True),
      data_slice.DataSlice.from_vals(0),
      M.core.apply_varargs(align, args),
  )