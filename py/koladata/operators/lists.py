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

"""Operators that work on lists."""

import types as py_types

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.operators import arolla_bridge
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import slices as slice_ops
from koladata.operators import view_overloads as _
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import schema_constants


M = arolla.OperatorsContainer(jagged_shape)
P = arolla.P
MASK = schema_constants.MASK
constraints = arolla.optools.constraints


@optools.as_backend_operator('kde.lists._like', deterministic=False)
def _like(
    shape_and_mask_from, items, item_schema, schema, itemid  # pylint: disable=unused-argument
):
  """Implementation of `kde.lists.like`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.list_like'])
@optools.as_lambda_operator(
    'kde.lists.like',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_and_mask_from),
        qtype_utils.expect_data_slice_or_unspecified(P.items),
        qtype_utils.expect_data_slice_or_unspecified(P.item_schema),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
    ],
)
def like(
    shape_and_mask_from,
    /,
    items=arolla.unspecified(),
    *,
    item_schema=arolla.unspecified(),
    schema=arolla.unspecified(),
    itemid=arolla.unspecified(),
):
  """Creates new Koda lists with shape and sparsity of `shape_and_mask_from`.

  Args:
    shape_and_mask_from: a DataSlice with the shape and sparsity for the desired
      lists.
    items: optional items to assign to the newly created lists. If not given,
      the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

  Returns:
    A DataSlice with the lists.
  """
  items = M.core.default_if_unspecified(items, data_slice.unspecified())
  item_schema = M.core.default_if_unspecified(
      item_schema, data_slice.unspecified()
  )
  schema = M.core.default_if_unspecified(schema, data_slice.unspecified())
  itemid = M.core.default_if_unspecified(itemid, data_slice.unspecified())
  return _like(shape_and_mask_from, items, item_schema, schema, itemid)


@optools.as_backend_operator('kde.lists._shaped', deterministic=False)
def _shaped(shape, items, item_schema, schema, itemid):  # pylint: disable=unused-argument
  """Implementation of `kde.lists.shaped`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.list_shaped'])
@optools.as_lambda_operator(
    'kde.lists.shaped',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.shape),
        qtype_utils.expect_data_slice_or_unspecified(P.items),
        qtype_utils.expect_data_slice_or_unspecified(P.item_schema),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
    ],
)
def shaped(
    shape,
    /,
    items=arolla.unspecified(),
    *,
    item_schema=arolla.unspecified(),
    schema=arolla.unspecified(),
    itemid=arolla.unspecified(),
):
  """Creates new Koda lists with the given shape.

  Args:
    shape: the desired shape.
    items: optional items to assign to the newly created lists. If not given,
      the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

  Returns:
    A DataSlice with the lists.
  """
  items = M.core.default_if_unspecified(items, data_slice.unspecified())
  item_schema = M.core.default_if_unspecified(
      item_schema, data_slice.unspecified()
  )
  schema = M.core.default_if_unspecified(schema, data_slice.unspecified())
  itemid = M.core.default_if_unspecified(itemid, data_slice.unspecified())
  return _shaped(shape, items, item_schema, schema, itemid)


@optools.add_to_registry(aliases=['kde.list_shaped_as'])
@optools.as_lambda_operator(
    'kde.lists.shaped_as',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_from),
        qtype_utils.expect_data_slice_or_unspecified(P.items),
        qtype_utils.expect_data_slice_or_unspecified(P.item_schema),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
    ],
)
def shaped_as(
    shape_from,
    /,
    items=arolla.unspecified(),
    *,
    item_schema=arolla.unspecified(),
    schema=arolla.unspecified(),
    itemid=arolla.unspecified(),
):
  """Creates new Koda lists with the shape of the given DataSlice.

  Args:
    shape_from: DataSlice of the desired shape.
    items: optional items to assign to the newly created lists. If not given,
      the function returns empty lists.
    item_schema: the schema of the list items. If not specified, it will be
      deduced from `items` or defaulted to OBJECT.
    schema: The schema to use for the list. If specified, then item_schema must
      not be specified.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting lists.

  Returns:
    A DataSlice with the lists.
  """
  return shaped(
      jagged_shape_ops.get_shape(shape_from),
      items=items,
      item_schema=item_schema,
      schema=schema,
      itemid=itemid,
  )


@optools.as_backend_operator('kde.lists._explode')
def _explode(x, ndim):  # pylint: disable=unused-argument
  """Implementation of kde.lists.explode."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.explode'])
@optools.as_lambda_operator(
    'kde.lists.explode',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.ndim),
    ],
)
def explode(x, ndim=1):
  """Explodes a List DataSlice `x` a specified number of times.

  A single list "explosion" converts a rank-K DataSlice of LIST[T] to a
  rank-(K+1) DataSlice of T, by unpacking the items in the Lists in the original
  DataSlice as a new DataSlice dimension in the result. Missing values in the
  original DataSlice are treated as empty lists.

  A single list explosion can also be done with `x[:]`.

  If `ndim` is set to a non-negative integer, explodes recursively `ndim` times.
  An `ndim` of zero is a no-op.

  If `ndim` is set to a negative integer, explodes as many times as possible,
  until at least one of the items of the resulting DataSlice is not a List.

  Args:
    x: DataSlice of Lists to explode
    ndim: the number of explosion operations to perform, defaults to 1

  Returns:
    DataSlice
  """
  return _explode(x, arolla_bridge.to_arolla_int64(ndim))


@optools.as_backend_operator('kde.lists._implode', deterministic=False)
def _implode(x, ndim, itemid):  # pylint: disable=unused-argument
  """Implementation of kde.lists.implode."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.implode'])
@optools.as_lambda_operator(
    'kde.lists.implode',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.ndim),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
    ],
)
def implode(x, ndim=1, itemid=arolla.unspecified()):
  """Implodes a Dataslice `x` a specified number of times.

  A single list "implosion" converts a rank-(K+1) DataSlice of T to a rank-K
  DataSlice of LIST[T], by folding the items in the last dimension of the
  original DataSlice into newly-created Lists.

  A single list implosion is equivalent to `kd.list(x, db)`.

  If `ndim` is set to a non-negative integer, implodes recursively `ndim` times.

  If `ndim` is set to a negative integer, implodes as many times as possible,
  until the result is a DataItem (i.e. a rank-0 DataSlice) containing a single
  nested List.

  Args:
    x: the DataSlice to implode
    ndim: the number of implosion operations to perform
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting lists.

  Returns:
    DataSlice of nested Lists
  """
  itemid = M.core.default_if_unspecified(itemid, data_slice.unspecified())
  return _implode(x, arolla_bridge.to_arolla_int64(ndim), itemid)


@optools.add_to_registry(aliases=['kde.list_size'])
@optools.as_backend_operator(
    'kde.lists.size',
    qtype_constraints=[qtype_utils.expect_data_slice(P.list_slice)],
)
def size(list_slice):  # pylint: disable=unused-argument
  """Returns size of a List."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.has_list'])
@optools.as_backend_operator(
    'kde.lists.has_list',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def has_list(x):  # pylint: disable=unused-argument
  """Returns present for each item in `x` that is List.

  Note that this is a pointwise operation.

  Also see `kd.is_list` for checking if `x` is a List DataSlice. But note that
  `kd.all(kd.has_list(x))` is not always equivalent to `kd.is_list(x)`. For
  example,

    kd.is_list(kd.item(None, kd.OBJECT)) -> kd.present
    kd.all(kd.has_list(kd.item(None, kd.OBJECT))) -> invalid for kd.all
    kd.is_list(kd.item([None], kd.OBJECT)) -> kd.present
    kd.all(kd.has_list(kd.item([None], kd.OBJECT))) -> kd.missing

  Args:
    x: DataSlice to check.

  Returns:
    A MASK DataSlice with the same shape as `x`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.is_list'])
@optools.as_backend_operator(
    'kde.lists.is_list', qtype_constraints=[qtype_utils.expect_data_slice(P.x)]
)
def is_list(x):  # pylint: disable=unused-argument
  """Returns whether x is a List DataSlice.

  `x` is a List DataSlice if it meets one of the following conditions:
    1) it has a List schema
    2) it has OBJECT/ANY schema and only has List items

  Also see `kd.has_list` for a pointwise version. But note that
  `kd.all(kd.has_list(x))` is not always equivalent to `kd.is_list(x)`. For
  example,

    kd.is_list(kd.item(None, kd.OBJECT)) -> kd.present
    kd.all(kd.has_list(kd.item(None, kd.OBJECT))) -> invalid for kd.all
    kd.is_list(kd.item([None], kd.OBJECT)) -> kd.present
    kd.all(kd.has_list(kd.item([None], kd.OBJECT))) -> kd.missing

  Args:
    x: DataSlice to check.

  Returns:
    A MASK DataItem.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.select_items'])
@arolla.optools.as_lambda_operator(
    'kde.lists.select_items',
    qtype_constraints=[qtype_utils.expect_data_slice(P.ds)],
    experimental_aux_policy=(
        'koladata_adhoc_binding_policy[kde.lists.select_items]'
    ),
)
def select_items(ds, fltr):
  """Selects List items by filtering out missing items in fltr.

  Also see kd.select.

  Args:
    ds: List DataSlice to be filtered
    fltr: filter can be a DataSlice with dtype as kd.MASK. It can also be a Koda
      Functor or a Python function which can be evalauted to such DataSlice.

  Returns:
    Filtered DataSlice.
  """
  return slice_ops.select(ds=explode(ds), fltr=fltr)


def _select_items_bind_args(ds, fltr):
  """Argument binding policy for the `kde.lists.select_items` operator."""
  if isinstance(fltr, py_types.FunctionType):
    fltr = fltr(ds[:])
  return (py_boxing.as_qvalue_or_expr(ds), py_boxing.as_qvalue_or_expr(fltr))


arolla.abc.register_adhoc_aux_binding_policy(
    select_items, _select_items_bind_args, make_literal_fn=py_boxing.literal
)
