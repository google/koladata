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

"""Jagged shape operators."""

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.operators import arolla_bridge
from koladata.operators import assertion
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants


M = arolla.M | jagged_shape.M
P = arolla.P
INT64 = schema_constants.INT64
constraints = arolla.optools.constraints

_reshape = arolla_bridge._reshape  # pylint: disable=protected-access


def _expect_slices_or_edges(value):
  """Constrains `value` to be a tuple of DataSlices or Edges."""
  is_slice_or_edge = arolla.LambdaOperator(
      'x', (P.x == qtypes.DATA_SLICE) | (P.x == arolla.DENSE_ARRAY_EDGE)
  )
  return (
      M.seq.all(M.seq.map(is_slice_or_edge, M.qtype.get_field_qtypes(value))),
      (
          'all arguments must be DataSlices or Edges, got:'
          f' {constraints.variadic_name_type_msg(value)}'
      ),
  )


@optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'kd.shapes.new',
    qtype_constraints=[_expect_slices_or_edges(P.dimensions)],
    qtype_inference_expr=qtypes.JAGGED_SHAPE,
    experimental_aux_policy=py_boxing.LIST_TO_SLICE_BOXING_POLICY,
)
def new(*dimensions):  # pylint: disable=unused-argument
  """Returns a JaggedShape from the provided dimensions.

  Example:
    # Creates a scalar shape (i.e. no dimension).
    kd.shapes.new()  # -> JaggedShape()

    # Creates a 3-dimensional shape with all uniform dimensions.
    kd.shapes.new(2, 3, 1)  # -> JaggedShape(2, 3, 1)

    # Creates a 3-dimensional shape with 2 sub-values in the first dimension.
    #
    # The second dimension is jagged with 2 values. The first value in the
    # second dimension has 2 sub-values, and the second value has 1 sub-value.
    #
    # The third dimension is jagged with 3 values. The first value in the third
    # dimension has 1 sub-value, the second has 2 sub-values, and the third has
    # 3 sub-values.
    kd.shapes.new(2, [2, 1], [1, 2, 3])
        # -> JaggedShape(2, [2, 1], [1, 2, 3])

  Args:
    *dimensions: A combination of Edges and DataSlices representing the
      dimensions of the JaggedShape. Edges are used as is, while DataSlices are
      treated as sizes. DataItems (of ints) are interpreted as uniform
      dimensions which have the same child size for all parent elements.
      DataSlices (of ints) are interpreted as a list of sizes, where `ds[i]` is
      the child size of parent `i`. Only rank-0 or rank-1 int DataSlices are
      supported.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'kd.shapes._new_with_size',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.result_size),
        _expect_slices_or_edges(P.dimensions),
    ],
    qtype_inference_expr=qtypes.JAGGED_SHAPE,
    experimental_aux_policy=py_boxing.LIST_TO_SLICE_BOXING_POLICY,
)
def _new_with_size(result_size, *dimensions):  # pylint: disable=unused-argument
  """Returns a JaggedShape from the provided dimensions and size.

  It supports a single placeholder dimension argument denoted as `-1`, for which
  its true value is inferred from the provided `size` argument and remaining
  `dimensions`. The resulting dimension must be a uniform dimension, i.e. all
  parent elements must have the same child size.

  Args:
    result_size: The size of the resulting JaggedShape.
    *dimensions: A combination of Edges and DataSlices representing the
      dimensions of the JaggedShape. Edges are used as is, while DataSlices are
      treated as sizes. DataItems (of ints) are interpreted as uniform
      dimensions which have the same child size for all parent elements.
      DataSlices (of ints) are interpreted as a list of sizes, where `ds[i]` is
      the child size of parent `i`. Only rank-0 or rank-1 int DataSlices are
      supported.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.shapes.size',
    qtype_constraints=[qtype_utils.expect_jagged_shape(P.shape)],
)
def size(shape):
  """Returns the total number of elements the jagged shape represents."""
  return arolla_bridge.to_data_slice(
      M.jagged.size(arolla_bridge.to_arolla_jagged_shape(shape))
  )


@optools.add_to_registry(aliases=['kd.get_shape'])
@optools.as_backend_operator(
    'kd.shapes.get_shape',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.JAGGED_SHAPE,
)
def get_shape(x):  # pylint: disable=unused-argument
  """Returns the shape of `x`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.reshape'])
@optools.as_lambda_operator('kd.shapes.reshape')
def reshape(x, shape):
  """Returns a DataSlice with the provided shape.

  Examples:
    x = kd.slice([1, 2, 3, 4])

    # Using a shape.
    kd.reshape(x, kd.shapes.new(2, 2))  # -> kd.slice([[1, 2], [3, 4]])

    # Using a tuple of sizes.
    kd.reshape(x, kd.tuple(2, 2))  # -> kd.slice([[1, 2], [3, 4]])

    # Using a tuple of sizes and a placeholder dimension.
    kd.reshape(x, kd.tuple(-1, 2))  # -> kd.slice([[1, 2], [3, 4]])

    # Using a tuple of sizes and a placeholder dimension.
    kd.reshape(x, kd.tuple(-1, 2))  # -> kd.slice([[1, 2], [3, 4]])

    # Using a tuple of slices and a placeholder dimension.
    kd.reshape(x, kd.tuple(-1, kd.slice([3, 1])))
        # -> kd.slice([[1, 2, 3], [4]])

    # Reshaping a scalar.
    kd.reshape(1, kd.tuple(1, 1))  # -> kd.slice([[1]])

    # Reshaping an empty slice.
    kd.reshape(kd.slice([]), kd.tuple(2, 0))  # -> kd.slice([[], []])

  Args:
    x: a DataSlice.
    shape: a JaggedShape or a tuple of dimensions that forms a shape through
      `kd.shapes.new`, with additional support for a `-1` placeholder dimension.
  """
  to_shape = arolla.types.DispatchOperator(
      'x, value',
      shape_case=arolla.types.DispatchCase(
          P.value, condition=P.value == qtypes.JAGGED_SHAPE
      ),
      default=M.core.apply_varargs(
          _new_with_size, size(get_shape(P.x)), P.value
      ),
  )
  return _reshape(x, to_shape(x, shape))


@optools.add_to_registry(aliases=['kd.reshape_as'])
@optools.as_lambda_operator('kd.shapes.reshape_as')
def reshape_as(x, shape_from):
  """Returns a DataSlice x reshaped to the shape of DataSlice shape_from."""
  return reshape(x, get_shape(shape_from))


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal._flatten_last_ndim',
    qtype_constraints=[
        (
            (P.x == qtypes.JAGGED_SHAPE) | (P.x == qtypes.DATA_SLICE),
            (
                'expected a JaggedShape or a DataSlice, got:'
                f' {constraints.name_type_msg(P.x)}'
            ),
        ),
        (
            (P.ndim == qtypes.DATA_SLICE)
            | M.qtype.is_integral_scalar_qtype(P.ndim),
            (
                'expected a DataSlice or a scalar integer, got:'
                f' {constraints.name_type_msg(P.ndim)}'
            ),
        ),
    ],
)
def _flatten_last_ndim(x, ndim):
  """(internal) Flatten the last `ndim` dimensions of `x`."""
  to_int64 = arolla.types.DispatchOperator(
      'ndim',
      data_slice_case=arolla.types.DispatchCase(
          arolla_bridge.to_arolla_int64(P.ndim),
          condition=P.ndim == qtypes.DATA_SLICE,
      ),
      default=P.ndim,
  )

  @arolla.optools.as_lambda_operator('koda_internal.flatten_last_ndim.shape')
  def flatten_shape(x, ndim):
    x_upcast = arolla_bridge.to_arolla_jagged_shape(x)
    x_rank = M.jagged.rank(x_upcast)
    ndim = to_int64(ndim)
    ndim = assertion.with_assertion(
        ndim, (ndim >= 0) & (ndim <= x_rank), 'expected 0 <= ndim <= rank'
    )
    return arolla_bridge.from_arolla_jagged_shape(
        M.jagged.flatten(x_upcast, x_rank - ndim)
    )

  @arolla.optools.as_lambda_operator('koda_internal.flatten_last_ndim.slice')
  def flatten_slice(x, ndim):
    return reshape(x, flatten_shape(get_shape(x), ndim))

  return arolla.types.DispatchOperator(
      'x, ndim',
      data_slice_case=arolla.types.DispatchCase(
          flatten_shape(P.x, P.ndim),
          condition=P.x == qtypes.JAGGED_SHAPE,
      ),
      default=flatten_slice(P.x, P.ndim),
  )(x, ndim)


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.flatten_last_ndim',
    qtype_constraints=[(
        (P.x == qtypes.JAGGED_SHAPE) | (P.x == qtypes.DATA_SLICE),
        (
            'expected a JaggedShape or a DataSlice, got:'
            f' {constraints.name_type_msg(P.x)}'
        ),
    )],
)
def flatten_last_ndim(x, ndim):
  """Flattens the last `ndim` dimensions of `x`.

  Args:
    x: a JaggedShape or a DataSlice.
    ndim: the number of dimensions to flatten, or `unspecified`.

  Returns:
    `x` with the last `ndim` dimensions flattened. If `ndim` is `unspecified`,
    `x` is returned as-is.
  """
  flatten_if_specified = arolla.types.DispatchOperator(
      'x, ndim',
      unspecified_case=arolla.types.DispatchCase(
          P.x, condition=P.ndim == arolla.UNSPECIFIED
      ),
      default=_flatten_last_ndim,
  )
  return flatten_if_specified(x, ndim)


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal._remove_last_ndim',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.x),
        qtype_utils.expect_data_slice(P.ndim),
    ],
)
def _remove_last_ndim(x, ndim):
  """(internal) Remove the last `ndim` dimensions of shape `x`."""
  x_upcast = arolla_bridge.to_arolla_jagged_shape(x)
  x_rank = M.jagged.rank(x_upcast)
  ndim = arolla_bridge.to_arolla_int64(ndim)
  ndim = assertion.with_assertion(
      ndim, (ndim >= 0) & (ndim <= x_rank), 'expected 0 <= ndim <= rank'
  )
  return arolla_bridge.from_arolla_jagged_shape(
      M.jagged.remove_dims(x_upcast, x_rank - ndim)
  )


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.remove_last_ndim',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.x),
    ],
)
def remove_last_ndim(x, ndim):
  """Removes the last `ndim` dimensions of a shape `x`.

  Args:
    x: a JaggedShape.
    ndim: the number of dimensions to remove, 0 if unspecified.

  Returns:
    `x` with the last `ndim` dimensions removed.
  """
  remove_last_ndim_if_specified = arolla.types.DispatchOperator(
      'x, ndim',
      unspecified_cast=arolla.types.DispatchCase(
          P.x, condition=P.ndim == arolla.UNSPECIFIED
      ),
      default=_remove_last_ndim,
  )
  return remove_last_ndim_if_specified(x, ndim)


@optools.as_backend_operator('kd.shapes._expand_to_shape')
def _expand_to_shape(x, shape, ndim):  # pylint: disable=unused-argument
  """Broadcasts a DataSlice to the provided shape."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.expand_to_shape'])
@optools.as_lambda_operator(
    'kd.shapes.expand_to_shape',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_jagged_shape(P.shape),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def expand_to_shape(x, shape, ndim=arolla.unspecified()):
  """Expands `x` based on the provided `shape`.

  When `ndim` is not set, expands `x` to `shape`. The dimensions
  of `x` must be the same as the first N dimensions of `shape` where N is the
  number of dimensions of `x`. For example,

  Example 1:
    x: [[1, 2], [3]]
    shape: JaggedShape(3, [2, 1], [1, 2, 3])
    result: [[[1], [2, 2]], [[3, 3, 3]]]

  Example 2:
    x: [[1, 2], [3]]
    shape: JaggedShape(3, [1, 1], [1, 3])
    result: incompatible shapes

  Example 3:
    x: [[1, 2], [3]]
    shape: JaggedShape(2)
    result: incompatible shapes

  When `ndim` is set, the expansion is performed in 3 steps:
    1) the last N dimensions of `x` are first imploded into lists
    2) the expansion operation is performed on the DataSlice of lists
    3) the lists in the expanded DataSlice are exploded

  The result will have M + ndim dimensions where M is the number
  of dimensions of `shape`.

  For example,

  Example 4:
    x: [[1, 2], [3]]
    shape: JaggedShape(2, [1, 2])
    ndim: 1
    result: [[[1, 2]], [[3], [3]]]

  Example 5:
    x: [[1, 2], [3]]
    shape: JaggedShape(2, [1, 2])
    ndim: 2
    result: [[[[1, 2], [3]]], [[[1, 2], [3]], [[1, 2], [3]]]]

  Args:
    x: DataSlice to expand.
    shape: JaggedShape.
    ndim: the number of dimensions to implode during expansion.

  Returns:
    Expanded DataSlice
  """
  to_arolla_int64 = arolla.types.DispatchOperator(
      'ndim',
      unspecified_case=arolla.types.DispatchCase(
          arolla.int64(0), condition=P.ndim == arolla.UNSPECIFIED
      ),
      default=arolla_bridge.to_arolla_int64(P.ndim),
  )
  return _expand_to_shape(
      x,
      arolla_bridge.to_arolla_jagged_shape(shape),
      to_arolla_int64(ndim),
  )


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.shapes.is_expandable_to_shape',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_jagged_shape(P.target_shape),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def is_expandable_to_shape(x, target_shape, ndim=arolla.unspecified()):
  """Returns true if `x` is expandable to `target_shape`.

  See `expand_to_shape` for a detailed description of expansion.

  Args:
    x: DataSlice that would be expanded.
    target_shape: JaggedShape that would be expanded to.
    ndim: The number of dimensions to implode before expansion. If unset,
      defaults to 0.
  """
  shape = remove_last_ndim(get_shape(x), ndim)
  return arolla_bridge.to_data_slice(
      M.jagged.is_broadcastable_to(
          arolla_bridge.to_arolla_jagged_shape(shape),
          arolla_bridge.to_arolla_jagged_shape(target_shape),
      )
  )


@optools.add_to_registry(aliases=['kd.flatten'])
@optools.as_lambda_operator(
    'kd.shapes.flatten',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.from_dim),
        qtype_utils.expect_data_slice_or_unspecified(P.to_dim),
    ],
)
def flatten(
    x,
    from_dim=data_slice.DataSlice.from_vals(0, INT64),
    to_dim=arolla.unspecified(),
):
  """Returns `x` with dimensions `[from_dim:to_dim]` flattened.

  Indexing works as in python:
  * If `to_dim` is unspecified, `to_dim = rank()` is used.
  * If `to_dim < from_dim`, `to_dim = from_dim` is used.
  * If `to_dim < 0`, `max(0, to_dim + rank())` is used. The same goes for
    `from_dim`.
  * If `to_dim > rank()`, `rank()` is used. The same goes for `from_dim`.

  The above-mentioned adjustments places both `from_dim` and `to_dim` in the
  range `[0, rank()]`. After adjustments, the new DataSlice has `rank() ==
  old_rank - (to_dim - from_dim) + 1`. Note that if `from_dim == to_dim`, a
  "unit" dimension is inserted at `from_dim`.

  Example:
    # Flatten the last two dimensions into a single dimension, producing a
    # DataSlice with `rank = old_rank - 1`.
    kd.get_shape(x)  # -> JaggedShape(..., [2, 1], [7, 5, 3])
    flat_x = kd.flatten(x, -2)
    kd.get_shape(flat_x)  # -> JaggedShape(..., [12, 3])

    # Flatten all dimensions except the last, producing a DataSlice with
    # `rank = 2`.
    kd.get_shape(x)  # -> jaggedShape(..., [7, 5, 3])
    flat_x = kd.flatten(x, 0, -1)
    kd.get_shape(flat_x)  # -> JaggedShape([3], [7, 5, 3])

    # Flatten all dimensions.
    kd.get_shape(x)  # -> JaggedShape([3], [7, 5, 3])
    flat_x = kd.flatten(x)
    kd.get_shape(flat_x)  # -> JaggedShape([15])

  Args:
    x: a DataSlice.
    from_dim: start of dimensions to flatten. Defaults to `0` if unspecified.
    to_dim: end of dimensions to flatten. Defaults to `rank()` if unspecified.
  """
  to_int64 = arolla.types.DispatchOperator(
      'value',
      unspecified_case=arolla.types.DispatchCase(
          P.value, condition=P.value == arolla.UNSPECIFIED
      ),
      default=arolla_bridge.to_arolla_int64,
  )
  return reshape(
      x,
      arolla_bridge.from_arolla_jagged_shape(
          M.jagged.flatten(
              arolla_bridge.to_arolla_jagged_shape(get_shape(x)),
              to_int64(from_dim),
              to_int64(to_dim),
          ),
      ),
  )


@optools.add_to_registry(aliases=['kd.flatten_end'])
@optools.as_lambda_operator(
    'kd.shapes.flatten_end',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.n_times),
    ],
)
def flatten_end(x, n_times=data_slice.DataSlice.from_vals(1, INT64)):
  """Returns `x` with a shape flattened `n_times` from the end.

  The new shape has x.get_ndim() - n_times dimensions.

  Given that flattening happens from the end, only positive integers are
  allowed. For more control over flattening, please use `kd.flatten`, instead.

  Args:
    x: a DataSlice.
    n_times: number of dimensions to flatten from the end
      (0 <= n_times <= rank).
  """
  x_shape = arolla_bridge.to_arolla_jagged_shape(get_shape(x))
  x_ndim = M.jagged.rank(x_shape)
  x_ndim = assertion.with_assertion(
      x_ndim, x_ndim > 0, 'expected multidim DataSlice, got DataItem'
  )
  n_times = arolla_bridge.to_arolla_int64(n_times)
  n_times = assertion.with_assertion(
      n_times, (n_times >= 0) & (n_times <= x_ndim),
      'expected 0 <= n_times <= rank'
  )
  return reshape(
      x,
      arolla_bridge.from_arolla_jagged_shape(
          M.jagged.flatten(
              x_shape,
              arolla.M.math.neg(n_times) - 1,
              arolla.unspecified(),
          ),
      ),
  )


@optools.add_to_registry(aliases=['kd.shapes.ndim'])
@optools.as_lambda_operator(
    'kd.shapes.rank',
    qtype_constraints=[qtype_utils.expect_jagged_shape(P.shape)],
)
def rank(shape):
  """Returns the rank of the jagged shape."""
  return arolla_bridge.to_data_slice(
      M.jagged.rank(arolla_bridge.to_arolla_jagged_shape(shape))
  )


# TODO: Remove this operator once the shapes.get_sizes is ready.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.shapes.dim_sizes',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.shape),
        qtype_utils.expect_data_slice(P.dim),
    ],
)
def dim_sizes(shape, dim):
  """Returns the row sizes at the provided dimension in the given shape.

  Example:
    shape = kd.shapes.new([2], [2, 1])
    kd.shapes.dim_sizes(shape, 0)  # -> kd.slice([2])
    kd.shapes.dim_sizes(shape, 1)  # -> kd.slice([2, 1])

  Args:
    shape: a JaggedShape.
    dim: the dimension to get the sizes for.
  """
  dim_int64 = arolla_bridge.to_arolla_int64(dim)
  return arolla_bridge.to_data_slice(
      M.edge.sizes(
          M.jagged.edge_at(
              arolla_bridge.to_arolla_jagged_shape(shape), dim_int64
          )
      )
  )


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.shapes.dim_mapping',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.shape),
        qtype_utils.expect_data_slice(P.dim),
    ],
)
def dim_mapping(shape, dim):
  """Returns the parent-to-child mapping of the dimension in the given shape.

  Example:
    shape = kd.shapes.new([2], [3, 2], [1, 2, 0, 2, 1])
    kd.shapes.dim_mapping(shape, 0) # -> kd.slice([0, 0])
    kd.shapes.dim_mapping(shape, 1) # -> kd.slice([0, 0, 0, 1, 1])
    kd.shapes.dim_mapping(shape, 2) # -> kd.slice([0, 1, 1, 3, 3, 4])

  Args:
    shape: a JaggedShape.
    dim: the dimension to get the parent-to-child mapping for.
  """
  dim_int64 = arolla_bridge.to_arolla_int64(dim)
  return arolla_bridge.to_data_slice(
      M.edge.mapping(
          M.jagged.edge_at(
              arolla_bridge.to_arolla_jagged_shape(shape), dim_int64
          )
      )
  )
