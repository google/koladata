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

"""Operators that work on DataSlices."""

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.operators import arolla_bridge
from koladata.operators import assertion
from koladata.operators import comparison
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import masking
from koladata.operators import math
from koladata.operators import op_repr
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import schema as schema_ops
from koladata.operators import view_overloads as _
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import py_misc_py_ext
from koladata.types import qtypes
from koladata.types import schema_constants


M = arolla.M | jagged_shape.M
P = arolla.P
MASK = schema_constants.MASK
constraints = arolla.optools.constraints


def _is_unspecified(x):
  return isinstance(x, arolla.abc.Unspecified)


# Implemented in masking.py to avoid a dependency cycle.
val_shaped = masking._val_shaped  # pylint: disable=protected-access
val_shaped_as = masking._val_shaped_as  # pylint: disable=protected-access
val_like = masking._val_like  # pylint: disable=protected-access


@optools.add_to_registry(aliases=['kd.empty_shaped'])
@optools.as_backend_operator(
    'kd.slices.empty_shaped',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.shape),
        qtype_utils.expect_data_slice(P.schema),
    ],
)
def empty_shaped(shape, schema=schema_constants.MASK):  # pylint: disable=unused-argument
  """Returns a DataSlice of missing items with the given shape.

  If `schema` is a Struct schema, an empty Databag is created and attached to
  the resulting DataSlice and `schema` is adopted into that DataBag.

  Args:
    shape: Shape of the resulting DataSlice.
    schema: optional schema of the resulting DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.empty_shaped_as'])
@optools.as_lambda_operator(
    'kd.slices.empty_shaped_as',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_from),
        qtype_utils.expect_data_slice(P.schema),
    ],
)
def empty_shaped_as(shape_from, schema=schema_constants.MASK):  # pylint: disable=unused-argument
  """Returns a DataSlice of missing items with the shape of `shape_from`.

  If `schema` is a Struct schema, an empty Databag is created and attached to
  the resulting DataSlice and `schema` is adopted into that DataBag.

  Args:
    shape_from: used for the shape of the resulting DataSlice.
    schema: optional schema of the resulting DataSlice.
  """
  return empty_shaped(shape_from.get_shape(), schema)


@optools.add_to_registry(aliases=['kd.agg_count'])
@optools.as_lambda_operator(
    'kd.slices.agg_count',
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
  shape_upcast = arolla_bridge.to_arolla_jagged_shape(shape)
  flat_res = M.array.count(flat_units, into=M.jagged.edge_at(shape_upcast, -1))
  return arolla_bridge.to_data_slice(flat_res).reshape(
      arolla_bridge.from_arolla_jagged_shape(
          M.jagged.remove_dims(shape_upcast, from_dim=-1)
      )
  )


@optools.add_to_registry(aliases=['kd.cum_count'])
@optools.as_lambda_operator(
    'kd.slices.cum_count',
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
          arolla_bridge.to_arolla_jagged_shape(
              jagged_shape_ops.flatten_last_ndim(x_shape, ndim),
          ),
          -1,
      ),
  )
  return arolla_bridge.to_data_slice(flat_res).reshape(x_shape)


@optools.add_to_registry(aliases=['kd.count'])
@optools.as_lambda_operator('kd.slices.count')
def count(x):
  """Returns the count of present items over all dimensions.

  The result is a zero-dimensional DataItem.

  Args:
    x: A DataSlice of numbers.
  """
  return agg_count(jagged_shape_ops.flatten(x))


@optools.add_to_registry(aliases=['kd.agg_size'])
@optools.as_lambda_operator(
    'kd.slices.agg_size',
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
  return agg_count(masking.present_shaped_as(x), ndim)


@optools.add_to_registry(aliases=['kd.size'])
@optools.as_lambda_operator('kd.slices.size')
def size(x):
  """Returns the number of items in `x`, including missing items.

  Args:
    x: A DataSlice.

  Returns:
    The size of `x`.
  """
  return jagged_shape_ops.size(jagged_shape_ops.get_shape(x))


@optools.add_to_registry(aliases=['kd.align'])
@arolla.optools.as_backend_operator(
    'kd.slices.align',
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
    'kd.slices._collapse', qtype_inference_expr=qtypes.DATA_SLICE
)
def _collapse(ds):  # pylint: disable=unused-argument
  """Creates a new DataSlice by collapsing 'ds' over its last dimension.

  Args:
    ds: DataSlice to be collapsed

  Returns:
    Collapsed DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.collapse'])
@optools.as_lambda_operator(
    'kd.slices.collapse',
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
    'kd.slices._concat_or_stack', qtype_inference_expr=qtypes.DATA_SLICE
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


@optools.add_to_registry(aliases=['kd.concat'])
@optools.as_lambda_operator(
    'kd.slices.concat',
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
    'kd.slices._dense_rank', qtype_inference_expr=qtypes.DATA_SLICE
)
def _dense_rank(x, descending):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.dense_rank'])
@optools.as_lambda_operator(
    'kd.slices.dense_rank',
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


@optools.add_to_registry(aliases=['kd.expand_to'])
@optools.as_lambda_operator(
    'kd.slices.expand_to',
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


@optools.add_to_registry(aliases=['kd.is_expandable_to'])
@optools.as_lambda_operator(
    'kd.slices.is_expandable_to',
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


@optools.add_to_registry(aliases=['kd.get_ndim'])
@optools.as_lambda_operator(
    'kd.slices.get_ndim',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def get_ndim(x):
  """Returns the number of dimensions of DataSlice `x`."""
  return jagged_shape_ops.rank(jagged_shape_ops.get_shape(x))


@optools.add_to_registry(aliases=['kd.take', 'kd.slices.at', 'kd.at'])
@optools.as_backend_operator(
    'kd.slices.take',
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


@arolla.optools.as_backend_operator(
    'kd.slices._group_by_indices',
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _group_by_indices(*args):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.group_by_indices'])
@optools.as_lambda_operator(
    'kd.slices.group_by_indices',
    qtype_constraints=[
        (
            M.qtype.get_field_count(P.args) > 0,
            'expected at least one argument',
        ),
        qtype_utils.expect_data_slice_args(P.args),
        qtype_utils.expect_data_slice(P.sort),
    ],
)
def group_by_indices(*args, sort=False):  # pylint: disable=redefined-outer-name, unused-argument
  """Returns a indices DataSlice with injected grouped_by dimension.

  The resulting DataSlice has get_ndim() + 1. The first `get_ndim() - 1`
  dimensions are unchanged. The last two dimensions corresponds to the groups
  and the items within the groups.

  Values of the DataSlice are the indices of the items within the parent
  dimension. `kd.take(x, kd.group_by_indices(x))` would group the items in
  `x` by their values.

  If sort=True groups are ordered by value, otherwise groups are ordered by the
  appearance of the first object in the group.

  Example 1:
    x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
    result: kd.slice([[0, 3, 6], [1, 5, 7], [2, 4]])

    We have three groups in order: 1, 3, 2. Each sublist contains the indices of
    the items in the original DataSlice.

  Example 2:
    x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3], sort=True)
    result: kd.slice([[0, 3, 6], [2, 4], [1, 5, 7]])

    Groups are now ordered by value.

  Example 3:
    x: kd.slice([[1, 2, 1, 3, 1, 3], [1, 3, 1]])
    result: kd.slice([[[0, 2, 4], [1], [3, 5]], [[0, 2], [1]]])

    We have three groups in the first sublist in order: 1, 2, 3 and two groups
    in the second sublist in order: 1, 3.
    Each sublist contains the indices of the items in the original sublist.

  Example 4:
    x: kd.slice([1, 3, 2, 1, None, 3, 1, None])
    result: kd.slice([[0, 3, 6], [1, 5], [2]])

    Missing values are not listed in the result.

  Example 5:
    x: kd.slice([1, 2, 3, 1, 2, 3, 1, 3]),
    y: kd.slice([7, 4, 0, 9, 4, 0, 7, 0]),
    result: kd.slice([[0, 6], [1, 4], [2, 5, 7], [3]])

    With several arguments keys is a tuple.
    In this example we have the following groups: (1, 7), (2, 4), (3, 0), (1, 9)

  Args:
    *args: DataSlices keys to group by. All data slices must have the same
      shape. Scalar DataSlices are not supported.
    sort: Whether groups should be ordered by value.

  Returns:
    INT64 DataSlice with indices and injected grouped_by dimension.
  """
  args = arolla.optools.fix_trace_args(args)
  return M.core.apply_varargs(_group_by_indices, sort, args)


@optools.add_to_registry(aliases=['kd.group_by'])
@optools.as_lambda_operator(
    'kd.slices.group_by',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_args(P.args),
        qtype_utils.expect_data_slice(P.sort),
    ],
)
def group_by(x, *args, sort=False):  # pylint: disable=redefined-outer-name
  """Returns permutation of `x` with injected grouped_by dimension.

  The resulting DataSlice has get_ndim() + 1. The first `get_ndim() - 1`
  dimensions are unchanged. The last two dimensions corresponds to the groups
  and the items within the groups.

  Values of the result is a permutation of `x`. `args` are used for the grouping
  keys. If length of `args` is greater than 1, the key is a tuple.
  If `args` is empty, the key is `x`.

  If sort=True groups are ordered by value, otherwise groups are ordered by the
  appearance of the first object in the group.

  Example 1:
    x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3])
    result: kd.slice([[1, 1, 1], [3, 3, 3], [2, 2]])

  Example 2:
    x: kd.slice([1, 3, 2, 1, 2, 3, 1, 3], sort=True)
    result: kd.slice([[1, 1, 1], [2, 2], [3, 3, 3]])

  Example 3:
    x: kd.slice([[1, 2, 1, 3, 1, 3], [1, 3, 1]])
    result: kd.slice([[[1, 1, 1], [2], [3, 3]], [[1, 1], [3]]])

  Example 4:
    x: kd.slice([1, 3, 2, 1, None, 3, 1, None])
    result: kd.slice([[1, 1, 1], [3, 3], [2]])

    Missing values are not listed in the result.

  Example 5:
    x: kd.slice([1, 2, 3, 4, 5, 6, 7, 8]),
    y: kd.slice([7, 4, 0, 9, 4, 0, 7, 0]),
    result: kd.slice([[1, 7], [2, 5], [3, 6, 8], [4]])

    When *args is present, `x` is not used for the key.

  Example 6:
    x: kd.slice([1, 2, 3, 4, None, 6, 7, 8]),
    y: kd.slice([7, 4, 0, 9, 4,    0, 7, None]),
    result: kd.slice([[1, 7], [2, None], [3, 6], [4]])

    Items with missing key is not listed in the result.
    Missing `x` values are missing in the result.

  Example 7:
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
    sort: Whether groups should be ordered by value.

  Returns:
    DataSlice with the same shape and schema as `x` with injected grouped
    by dimension.
  """
  args = arolla.optools.fix_trace_args(args)
  dispatch_op = arolla.types.DispatchOperator(
      'x, args, sort',
      x_is_key_case=arolla.types.DispatchCase(
          take(P.x, _group_by_indices(P.sort, P.x)),
          condition=M.qtype.get_field_count(P.args) == 0,
      ),
      default=take(
          assertion.with_assertion(
              P.x,
              M.jagged.equal(
                  arolla_bridge.to_arolla_jagged_shape(
                      jagged_shape_ops.get_shape(P.x)
                  ),
                  arolla_bridge.to_arolla_jagged_shape(
                      jagged_shape_ops.get_shape(M.core.get_nth(P.args, 0)),
                  ),
              ),
              'First argument `x` must have the same shape as the other'
              ' arguments',
          ),
          M.core.apply_varargs(_group_by_indices, P.sort, P.args),
      ),
  )
  return dispatch_op(x, args, sort)


@optools.as_lambda_operator(
    'kd.slices._normalize_dim',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.dim),
    ],
)
def normalize_dim(x, dim):
  """Returns dim if dim >= 0, otherwise get_ndim(x) + dim."""
  # TODO: masking.cond can be slow, optimize
  return masking.cond(comparison.less(dim, 0), get_ndim(x) + dim, dim)


@optools.add_to_registry(aliases=['kd.index'])
@optools.as_lambda_operator(
    'kd.slices.index',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.dim),
    ],
)
def index(x, dim=-1):
  """Returns the indices of the elements computed over dimension `dim`.

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
    kd.index(ds, dim=2)  # (same as kd.index(ds, -1) or kd.index(ds))
      # -> kd.slice([[[0, None, 2], [0, 1]], [[None, 1], [0, 1, 2]]])

    kd.index(ds) -> kd.index(ds, dim=ds.get_ndim() - 1)

  Args:
    x: A DataSlice.
    dim: The dimension to compute indices over.
      Requires -get_ndim(x) <= dim < get_ndim(x).
      If dim < 0 then dim = get_ndim(x) + dim.
  """
  x = assertion.with_assertion(
      x,
      get_ndim(x) != 0,
      'kd.slices.index: argument `x` must have non-zero rank',
  )

  dim = normalize_dim(x, dim)
  ndim = get_ndim(x) - dim - 1
  ndim = assertion.with_assertion(
      ndim,
      (ndim < get_ndim(x)) & (ndim >= 0),
      'kd.slices.index: expected -get_ndim(x) <= dim < get_ndim(x)',
  )

  aggregated = masking.agg_has(x, ndim)
  flat_units = arolla_bridge.to_arolla_dense_array_unit(masking.has(aggregated))
  shape = jagged_shape_ops.get_shape(aggregated)
  flat_res = M.array.agg_index(
      flat_units,
      over=M.jagged.edge_at(
          arolla_bridge.to_arolla_jagged_shape(shape),
          -1,
      ),
  )
  shaped_res = arolla_bridge.to_data_slice(flat_res).reshape(shape)
  return expand_to(shaped_res, x)


@optools.as_backend_operator('kd.slices._inverse_mapping')
def _inverse_mapping(x):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.inverse_mapping'])
@optools.as_lambda_operator(
    'kd.slices.inverse_mapping',
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
        'kd.inverse_select',
        'kd.slices.reverse_select',
        'kd.reverse_select',
    ]
)
@optools.as_backend_operator(
    'kd.slices.inverse_select',
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


@optools.add_to_registry(aliases=['kd.isin'])
@optools.as_lambda_operator(
    'kd.slices.isin',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
)
def isin(x, y):
  """Returns a DataItem indicating whether DataItem x is present in y."""
  x = assertion.with_assertion(
      x, get_ndim(x) == 0, 'kd.slices.isin: argument `x` must be a DataItem'
  )
  return masking.any_(x == y)


@optools.add_to_registry(aliases=['kd.is_empty'])
@optools.as_backend_operator(
    'kd.slices.is_empty',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def is_empty(x):  # pylint: disable=unused-argument
  """Returns kd.present if all items in the DataSlice are missing."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.is_shape_compatible'])
@optools.as_lambda_operator(
    'kd.slices.is_shape_compatible',
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
    'kd.slices._ordinal_rank', qtype_inference_expr=qtypes.DATA_SLICE
)
def _ordinal_rank(x, tie_breaker, descending):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.ordinal_rank'])
@optools.as_lambda_operator(
    'kd.slices.ordinal_rank',
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


@optools.add_to_registry(aliases=['kd.repeat'])
@optools.as_lambda_operator(
    'kd.slices.repeat',
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
  x, expanded_sizes = align(x, sizes)
  x_shape = jagged_shape_ops.get_shape(x)
  edge = M.edge.from_sizes(
      arolla_bridge.to_arolla_dense_array_int64(expanded_sizes)
  )

  target_shape = arolla_bridge.from_arolla_jagged_shape(
      M.jagged.add_dims(
          arolla_bridge.to_arolla_jagged_shape(x_shape),
          edge,
      )
  )
  return jagged_shape_ops.expand_to_shape(x, target_shape)


@optools.add_to_registry(aliases=['kd.repeat_present'])
@optools.as_lambda_operator(
    'kd.slices.repeat_present',
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
  x, expanded_sizes = align(x, sizes)
  return repeat(x, masking.cond(masking.has(x), expanded_sizes, 0))


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.slices._range',
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


@optools.add_to_registry(aliases=['kd.range'])
@optools.as_lambda_operator(
    'kd.slices.range',
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


@optools.add_to_registry(aliases=['kd.tile'])
@optools.as_lambda_operator(
    'kd.slices.tile',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_jagged_shape(P.shape),
    ],
)
def tile(x, shape):
  """Nests the whole `x` under `shape`.

  Example 1:
    x: [1, 2]
    shape: JaggedShape([3])
    result: [[1, 2], [1, 2], [1, 2]]

  Example 2:
    x: [1, 2]
    shape: JaggedShape([2], [2, 1])
    result: [[[1, 2], [1, 2]], [[1, 2]]]

  Args:
    x: DataSlice to expand.
    shape: JaggedShape.

  Returns:
    Expanded DataSlice.
  """
  return jagged_shape_ops.expand_to_shape(x, shape, get_ndim(x))


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.slices.internal_select_by_slice',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_slice(P.fltr),
        qtype_utils.expect_data_slice(P.expand_filter),
    ],
)
def internal_select_by_slice(ds, fltr, expand_filter=True):  # pylint: disable=unused-argument
  """A version of kd.select that does not support lambdas/functors."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.select_present'])
@optools.as_lambda_operator(
    'kd.slices.select_present',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
    ],
)
def select_present(ds):
  """Creates a new DataSlice by removing missing items.

  It is not supported for DataItems because their sizes are always 1.

  Example:
    val = kd.slice([[1, None, 4], [None], [2, 8]])
    kd.select_present(val) -> [[1, 4], [], [2, 8]]

  Args:
    ds: DataSlice with ndim > 0 to be filtered.

  Returns:
    Filtered DataSlice.
  """
  return internal_select_by_slice(ds=ds, fltr=masking.has(ds))


@optools.add_to_registry(aliases=['kd.reverse'])
@optools.as_backend_operator(
    'kd.slices.reverse',
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


@optools.add_to_registry(aliases=['kd.sort'])
@optools.as_lambda_operator(
    'kd.slices.sort',
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
              arolla_bridge.to_arolla_jagged_shape(
                  jagged_shape_ops.get_shape(P.x)
              ),
              arolla_bridge.to_arolla_jagged_shape(
                  jagged_shape_ops.get_shape(P.sort_by)
              ),
          ),
          'kd.slices.sort: arguments `x` and `sort_by` must have the same'
          ' shape',
      ),
  )
  assert_sparsity_less = arolla.types.LambdaOperator(
      'x, sort_by',
      assertion.with_assertion(
          P.sort_by,
          count(masking.has(P.x) & masking.has_not(P.sort_by)) == 0,
          'kd.slices.sort: trying to sort `x` by `sort_by` that is more sparse',
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


@optools.add_to_registry(aliases=['kd.stack'])
@optools.as_lambda_operator(
    'kd.slices.stack',
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
      | (
          M.qtype.is_slice_qtype(P.x)
          & M.seq.all(
              M.seq.map(
                  arolla.LambdaOperator(
                      'arg',
                      (P.arg == qtypes.DATA_SLICE)
                      | (P.arg == arolla.UNSPECIFIED),
                  ),
                  M.qtype.get_field_qtypes(P.x),
              )
          )
      )
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
    aliases=['kd.subslice'],
    repr_fn=op_repr.subslice_repr,
)
@arolla.optools.as_backend_operator(
    'kd.slices.subslice',
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
    2) INT32/INT64 DataSlice. It is used to select multiple items in one
    dimension.
    3) Python slice (e.g. slice(1), slice(1, 3), slice(2, -1)). It is used to
       select a slice of items in one dimension. 'step' is not supported and it
       results in no item if 'start' is larger than or equal to 'stop'. 'start'
       and 'stop' can be either Python integers, DataItems or DataSlices, in
       the latter case we can select a different range for different items,
       or even select multiple ranges for the same item if the 'start'
       or 'stop' have more dimensions. If an item is missing either in 'start'
       or in 'stop', the corresponding slice is considered empty.
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

  Example 12:
    x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
    kd.subslice(x, slice(kd.slice([0, 1, 2]), None))
      => kd.slice([[[1, 2], [3]], [[5, 6]], [[], []]])

  Example 13:
    x = kd.slice([[[1, 2], [3]], [[4, 5, 6]], [[7], [8, 9]]])
    kd.subslice(x, slice(kd.slice([0, 1, 2]), kd.slice([2, 3, None])), ...)
      => kd.slice([[[[1, 2], [3]], [[4, 5, 6]]], [[[4, 5, 6]], [[7], [8, 9]]],
      []])

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
    'kd.slices._subslice_for_slicing_helper',
    repr_fn=op_repr.subslicehelper_repr,
)(subslice)


@optools.add_to_registry(aliases=['kd.translate'])
@optools.as_backend_operator(
    'kd.slices.translate',
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


@optools.add_to_registry(aliases=['kd.translate_group'])
@optools.as_lambda_operator(
    'kd.slices.translate_group',
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
              arolla_bridge.to_arolla_jagged_shape(
                  jagged_shape_ops.get_shape(P.keys_from)
              ),
              arolla_bridge.to_arolla_jagged_shape(
                  jagged_shape_ops.get_shape(P.values_from)
              ),
          ),
          'kd.slices.translate_group: `keys_from` and `values_from` must have'
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


@optools.add_to_registry(aliases=['kd.unique'])
@optools.as_backend_operator(
    'kd.slices.unique',
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


@optools.add_to_registry(aliases=['kd.zip'])
@optools.as_lambda_operator(
    'kd.slices.zip',
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


@optools.add_to_registry(aliases=['kd.item'])
@arolla.optools.as_lambda_operator(
    'kd.slices.item',
    experimental_aux_policy='koladata_adhoc_binding_policy[kd.slices.item]',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
    ],
)
def item(x, schema=arolla.unspecified()):
  """Returns a DataItem created from `x`.

  If `schema` is set, that schema is used, otherwise the schema is inferred from
  `x`. Python value must be convertible to Koda scalar and the result cannot
  be multidimensional DataSlice.

  Args:
    x: a Python value or a DataItem.
    schema: schema DataItem to set. If `x` is already a DataItem, this will cast
      it to the given schema.
  """
  # If `x` is already a DataSlice, we check that it's 0-dim in the binding
  # policy. This speeds up the evaluation and often allows to get the error
  # message closer to where the error is.
  return arolla.types.DispatchOperator(
      'x, schema',
      unspecified_case=arolla.types.DispatchCase(
          P.x,
          condition=P.schema == arolla.UNSPECIFIED,
      ),
      default=schema_ops.cast_to(P.x, P.schema),
  )(x, schema)


_ITEM_MULTIDIM_ERROR = 'kd.item: argument `x` cannot be a multi-dim DataSlice'
_SCHEMA_EXPR_ERROR = (
    '`schema` cannot be an expression when `x` is a Python value, since we need'
    ' to convert the Python value to a Koda value at binding time'
)


def _item_bind_args(x, schema=arolla.unspecified()):
  """Binding policy for kd.slices.item."""
  if isinstance(x, arolla.Expr):
    x = assertion.with_assertion(
        x,
        get_ndim(x) == 0,
        _ITEM_MULTIDIM_ERROR,
    )
    return (x, schema)
  elif isinstance(x, data_slice.DataSlice):
    if x.get_ndim() > 0:
      raise ValueError(_ITEM_MULTIDIM_ERROR)
    return (x, schema)
  else:
    if isinstance(schema, arolla.Expr):
      raise ValueError(f'kd.item: {_SCHEMA_EXPR_ERROR}')
    return (
        data_item.DataItem.from_vals(
            x, schema=None if _is_unspecified(schema) else schema
        ),
        arolla.unspecified(),
    )


arolla.abc.register_adhoc_aux_binding_policy(
    item, _item_bind_args, make_literal_fn=py_boxing.literal
)


@optools.add_to_registry(aliases=['kd.slice'])
@arolla.optools.as_lambda_operator(
    'kd.slices.slice',
    experimental_aux_policy='koladata_adhoc_binding_policy[kd.slices.slice]',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
    ],
)
def slice_(x, schema=arolla.unspecified()):
  """Returns a DataSlice created from `x`.

  If `schema` is set, that schema is used, otherwise the schema is inferred from
  `x`.

  Args:
    x: a Python value or a DataSlice. If it is a (nested) Python list or tuple,
      a multidimensional DataSlice is created.
    schema: schema DataItem to set. If `x` is already a DataSlice, this will
      cast it to the given schema.
  """
  return arolla.types.DispatchOperator(
      'x, schema',
      unspecified_case=arolla.types.DispatchCase(
          P.x,
          condition=P.schema == arolla.UNSPECIFIED,
      ),
      default=schema_ops.cast_to(P.x, P.schema),
  )(x, schema)


def _slice_bind_args(x, schema=arolla.unspecified()):
  """Binding policy for kd.slices.slice."""
  if isinstance(x, (arolla.Expr, data_slice.DataSlice)):
    return (x, schema)
  elif type(x) in (  # Checking exact type to match behavior in boxing.cc
      list,
      tuple,
  ):
    flat_val, shape = py_misc_py_ext.flatten_py_list(x)
    if not any(isinstance(x, arolla.Expr) for x in flat_val) and not isinstance(
        schema, arolla.Expr
    ):
      # This branch is not necessary for correctness, but it makes the resulting
      # expr smaller in the literal case.
      return (
          data_slice.DataSlice.from_vals(
              x, schema=None if _is_unspecified(schema) else schema
          ),
          arolla.unspecified(),
      )
    flat_items = [item(x, schema=schema) for x in flat_val]
    if flat_items:
      # stack requires at least one argument.
      flat_slice = stack(*flat_items)
    else:
      flat_slice = schema_ops.with_schema(
          data_slice.DataSlice.from_vals([]), schema
      )
    shaped_slice = jagged_shape_ops.reshape(flat_slice, shape)
    return (shaped_slice, arolla.unspecified())
  else:
    if isinstance(schema, arolla.Expr):
      raise ValueError(f'kd.slice: {_SCHEMA_EXPR_ERROR}')
    return (
        data_slice.DataSlice.from_vals(
            x, schema=None if _is_unspecified(schema) else schema
        ),
        arolla.unspecified(),
    )


arolla.abc.register_adhoc_aux_binding_policy(
    slice_, _slice_bind_args, make_literal_fn=py_boxing.literal
)


def _typed_slice_bind_args(x, schema):
  """Binding policy for typed slice operators."""
  bound_x, _ = _slice_bind_args(x, schema=schema)
  return (bound_x,)


@optools.add_to_registry(aliases=['kd.int32'])
@arolla.optools.as_lambda_operator(
    'kd.slices.int32',
    experimental_aux_policy='koladata_adhoc_binding_policy[kd.slices.int32]',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def int32(x):
  """Returns kd.slice(x, kd.INT32)."""
  return schema_ops.to_int32(x)


arolla.abc.register_adhoc_aux_binding_policy(
    int32,
    lambda x: _typed_slice_bind_args(x, schema_constants.INT32),
    make_literal_fn=py_boxing.literal,
)


@optools.add_to_registry(aliases=['kd.int64'])
@arolla.optools.as_lambda_operator(
    'kd.slices.int64',
    experimental_aux_policy='koladata_adhoc_binding_policy[kd.slices.int64]',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def int64(x):
  """Returns kd.slice(x, kd.INT64)."""
  return schema_ops.to_int64(x)


arolla.abc.register_adhoc_aux_binding_policy(
    int64,
    lambda x: _typed_slice_bind_args(x, schema_constants.INT64),
    make_literal_fn=py_boxing.literal,
)


@optools.add_to_registry(aliases=['kd.float32'])
@arolla.optools.as_lambda_operator(
    'kd.slices.float32',
    experimental_aux_policy='koladata_adhoc_binding_policy[kd.slices.float32]',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def float32(x):
  """Returns kd.slice(x, kd.FLOAT32)."""
  return schema_ops.to_float32(x)


arolla.abc.register_adhoc_aux_binding_policy(
    float32,
    lambda x: _typed_slice_bind_args(x, schema_constants.FLOAT32),
    make_literal_fn=py_boxing.literal,
)


@optools.add_to_registry(aliases=['kd.float64'])
@arolla.optools.as_lambda_operator(
    'kd.slices.float64',
    experimental_aux_policy='koladata_adhoc_binding_policy[kd.slices.float64]',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def float64(x):
  """Returns kd.slice(x, kd.FLOAT64)."""
  return schema_ops.to_float64(x)


arolla.abc.register_adhoc_aux_binding_policy(
    float64,
    lambda x: _typed_slice_bind_args(x, schema_constants.FLOAT64),
    make_literal_fn=py_boxing.literal,
)


@optools.add_to_registry(aliases=['kd.str'])
@arolla.optools.as_lambda_operator(
    'kd.slices.str',
    experimental_aux_policy='koladata_adhoc_binding_policy[kd.slices.str]',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def str_(x):
  """Returns kd.slice(x, kd.STRING)."""
  return schema_ops.to_str(x)


@optools.add_to_registry(aliases=['kd.get_repr'])
@optools.as_backend_operator(
    'kd.slices.get_repr',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def get_repr(x):
  """Returns a string representation of the DataSlice `x`."""
  raise NotImplementedError('implemented in the backend')


arolla.abc.register_adhoc_aux_binding_policy(
    str_,
    lambda x: _typed_slice_bind_args(x, schema_constants.STRING),
    make_literal_fn=py_boxing.literal,
)


@optools.add_to_registry(aliases=['kd.bytes'])
@arolla.optools.as_lambda_operator(
    'kd.slices.bytes',
    experimental_aux_policy='koladata_adhoc_binding_policy[kd.slices.bytes]',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def bytes_(x):
  """Returns kd.slice(x, kd.BYTES)."""
  return schema_ops.to_bytes(x)


arolla.abc.register_adhoc_aux_binding_policy(
    bytes_,
    lambda x: _typed_slice_bind_args(x, schema_constants.BYTES),
    make_literal_fn=py_boxing.literal,
)


@optools.add_to_registry(aliases=['kd.bool'])
@arolla.optools.as_lambda_operator(
    'kd.slices.bool',
    experimental_aux_policy='koladata_adhoc_binding_policy[kd.slices.bool]',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def bool_(x):
  """Returns kd.slice(x, kd.BOOLEAN)."""
  return schema_ops.to_bool(x)


arolla.abc.register_adhoc_aux_binding_policy(
    bool_,
    lambda x: _typed_slice_bind_args(x, schema_constants.BOOLEAN),
    make_literal_fn=py_boxing.literal,
)


@optools.add_to_registry(aliases=['kd.mask'])
@arolla.optools.as_lambda_operator(
    'kd.slices.mask',
    experimental_aux_policy='koladata_adhoc_binding_policy[kd.slices.mask]',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def mask(x):
  """Returns kd.slice(x, kd.MASK)."""
  return schema_ops.to_mask(x)


arolla.abc.register_adhoc_aux_binding_policy(
    mask,
    lambda x: _typed_slice_bind_args(x, schema_constants.MASK),
    make_literal_fn=py_boxing.literal,
)


@optools.add_to_registry(aliases=['kd.expr_quote'])
@arolla.optools.as_lambda_operator(
    'kd.slices.expr_quote',
    experimental_aux_policy=(
        'koladata_adhoc_binding_policy[kd.slices.expr_quote]'
    ),
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def expr_quote(x):
  """Returns kd.slice(x, kd.EXPR)."""
  return schema_ops.to_expr(x)


arolla.abc.register_adhoc_aux_binding_policy(
    expr_quote,
    lambda x: _typed_slice_bind_args(x, schema_constants.EXPR),
    make_literal_fn=py_boxing.literal,
)


# There is an alternative faster implementation by implementing _concat_or_stack
# on a sequence of DataSlices at QExpr level.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.shapes.get_sizes',
    qtype_constraints=[(
        (P.x == qtypes.JAGGED_SHAPE) | (P.x == qtypes.DATA_SLICE),
        (
            'expected a JaggedShape or a DataSlice, got'
            f' {constraints.name_type_msg(P.x)}'
        ),
    )],
)
def shape_sizes(x):
  """Returns a DataSlice of sizes of a given shape.

  Example:
    kd.shapes.get_sizes(kd.shapes.new([2], [2, 1])) -> kd.slice([[2], [2, 1]])
    kd.shapes.get_sizes(kd.slice([['a', 'b'], ['c']])) -> kd.slice([[2], [2,
    1]])

  Args:
    x: a shape or a DataSlice from which the shape will be taken.

  Returns:
    A 2-dimensional DataSlice where the first dimension's size corresponds to
    the shape's rank and the n-th subslice corresponds to the sizes of the n-th
    dimension of the original shape.
  """

  dispatch_get_shape = arolla.types.DispatchOperator(
      'x',
      x_is_shape=arolla.types.DispatchCase(
          P.x,
          condition=P.x == qtypes.JAGGED_SHAPE,
      ),
      default=jagged_shape_ops.get_shape(P.x),
  )

  @optools.as_lambda_operator('size_slice')
  def get_sizes(edge):
    return arolla_bridge.to_data_slice(M.edge.sizes(edge))

  @optools.as_lambda_operator('concat_sizes')
  def concat_sizes(x, y):
    return concat(x,
                  jagged_shape_ops.flatten(y, 0, 0),  # Add an outer dimension.
                  ndim=2)

  edges = M.jagged.edges(arolla_bridge.to_arolla_jagged_shape(
      dispatch_get_shape(x)))
  sizes_ = M.seq.map(get_sizes, edges)
  return M.seq.reduce(
      concat_sizes,
      sizes_,
      initial=data_slice.DataSlice.from_vals([], schema_constants.INT64).repeat(
          0
      ),
  )
