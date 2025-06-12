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

"""Operators that work on dicts."""

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.operators import arolla_bridge
from koladata.operators import core as core_ops
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import view_overloads as _
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


M = arolla.M | jagged_shape.M
P = arolla.P
MASK = schema_constants.MASK
constraints = arolla.optools.constraints


@optools.as_backend_operator('kd.dicts._shaped', deterministic=False)
def _shaped(
    shape, keys, values, key_schema, value_schema, schema, itemid
):  # pylint: disable=unused-argument
  """Implementation of `kd.dicts.shaped`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.dict'])
@optools.as_lambda_operator(
    'kd.dicts.new',
    qtype_constraints=[
        qtype_utils.expect_data_slice_or_unspecified(P.keys),
        qtype_utils.expect_data_slice_or_unspecified(P.values),
        qtype_utils.expect_data_slice_or_unspecified(P.key_schema),
        qtype_utils.expect_data_slice_or_unspecified(P.value_schema),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
    ],
)
def create(
    keys=arolla.unspecified(),
    values=arolla.unspecified(),
    *,
    key_schema=arolla.unspecified(),
    value_schema=arolla.unspecified(),
    schema=arolla.unspecified(),
    itemid=arolla.unspecified(),
):
  """Creates a Koda dict.

  Acceptable arguments are:
    1) no argument: a single empty dict
    2) two DataSlices/DataItems as keys and values: a DataSlice of dicts whose
       shape is the last N-1 dimensions of keys/values DataSlice

  Examples:
  dict() -> returns a single new dict
  dict(kd.slice([1, 2]), kd.slice([3, 4]))
    -> returns a dict ({1: 3, 2: 4})
  dict(kd.slice([[1], [2]]), kd.slice([3, 4]))
    -> returns a 1-D DataSlice that holds two dicts ({1: 3} and {2: 4})
  dict('key', 12) -> returns a single dict mapping 'key'->12

  Args:
    keys: a DataSlice with keys.
    values: a DataSlice with values.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: the schema to use for the newly created Dict. If specified, then
      key_schema and value_schema must not be specified.
    itemid: ITEMID DataSlice used as ItemIds of the resulting dicts.

  Returns:
    A DataSlice with the dict.
  """
  keys = M.core.default_if_unspecified(keys, data_slice.unspecified())
  values = M.core.default_if_unspecified(values, data_slice.unspecified())
  key_schema = M.core.default_if_unspecified(
      key_schema, data_slice.unspecified()
  )
  value_schema = M.core.default_if_unspecified(
      value_schema, data_slice.unspecified()
  )
  schema = M.core.default_if_unspecified(schema, data_slice.unspecified())

  shape = arolla.types.DispatchOperator(
      'itemid, keys',
      unspecified_case=arolla.types.DispatchCase(
          arolla_bridge.from_arolla_jagged_shape(
              M.jagged.remove_dims(
                  arolla_bridge.to_arolla_jagged_shape(
                      jagged_shape_ops.get_shape(P.keys)
                  ),
                  from_dim=-1,
              )
          ),
          condition=(P.itemid == arolla.UNSPECIFIED),
      ),
      default=jagged_shape_ops.get_shape(P.itemid),
  )(itemid, keys)

  itemid = M.core.default_if_unspecified(itemid, data_slice.unspecified())

  return _shaped(
      shape=shape,
      keys=keys,
      values=values,
      key_schema=key_schema,
      value_schema=value_schema,
      schema=schema,
      itemid=itemid,
  )


@optools.add_to_registry(aliases=['kd.dict_shaped'])
@optools.as_lambda_operator(
    'kd.dicts.shaped',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.shape),
        qtype_utils.expect_data_slice_or_unspecified(P.keys),
        qtype_utils.expect_data_slice_or_unspecified(P.values),
        qtype_utils.expect_data_slice_or_unspecified(P.key_schema),
        qtype_utils.expect_data_slice_or_unspecified(P.value_schema),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
    ],
)
def shaped(
    shape,
    /,
    keys=arolla.unspecified(),
    values=arolla.unspecified(),
    *,
    key_schema=arolla.unspecified(),
    value_schema=arolla.unspecified(),
    schema=arolla.unspecified(),
    itemid=arolla.unspecified(),
):
  """Creates new Koda dicts with the given shape.

  If keys and values are not provided, creates empty dicts. Otherwise,
  the function assigns the given keys and values to the newly created dicts. So
  the keys and values must be either broadcastable to `shape` or one dimension
  higher.

  Args:
    shape: the desired shape.
    keys: a DataSlice with keys.
    values: a DataSlice of values.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: the schema to use for the newly created Dict. If specified, then
      key_schema and value_schema must not be specified.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting dicts.

  Returns:
    A DataSlice with the dicts.
  """
  keys = M.core.default_if_unspecified(keys, data_slice.unspecified())
  values = M.core.default_if_unspecified(values, data_slice.unspecified())
  key_schema = M.core.default_if_unspecified(
      key_schema, data_slice.unspecified()
  )
  value_schema = M.core.default_if_unspecified(
      value_schema, data_slice.unspecified()
  )
  schema = M.core.default_if_unspecified(schema, data_slice.unspecified())
  itemid = M.core.default_if_unspecified(itemid, data_slice.unspecified())
  return _shaped(
      shape,
      keys=keys,
      values=values,
      key_schema=key_schema,
      value_schema=value_schema,
      schema=schema,
      itemid=itemid,
  )


@optools.add_to_registry(aliases=['kd.dict_shaped_as'])
@optools.as_lambda_operator(
    'kd.dicts.shaped_as',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_from),
        qtype_utils.expect_data_slice_or_unspecified(P.keys),
        qtype_utils.expect_data_slice_or_unspecified(P.values),
        qtype_utils.expect_data_slice_or_unspecified(P.key_schema),
        qtype_utils.expect_data_slice_or_unspecified(P.value_schema),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
    ],
)
def shaped_as(
    shape_from,
    /,
    keys=arolla.unspecified(),
    values=arolla.unspecified(),
    *,
    key_schema=arolla.unspecified(),
    value_schema=arolla.unspecified(),
    schema=arolla.unspecified(),
    itemid=arolla.unspecified(),
):
  """Creates new Koda dicts with shape of the given DataSlice.

  If keys and values are not provided, creates empty dicts. Otherwise,
  the function assigns the given keys and values to the newly created dicts. So
  the keys and values must be either broadcastable to `shape` or one dimension
  higher.

  Args:
    shape_from: a DataSlice, whose shape the returned DataSlice will have.
    keys: a DataSlice with keys.
    values: a DataSlice of values.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: the schema to use for the newly created Dict. If specified, then
      key_schema and value_schema must not be specified.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting dicts.

  Returns:
    A DataSlice with the dicts.
  """
  return arolla.abc.bind_op(
      shaped,
      shape=jagged_shape_ops.get_shape(shape_from),
      keys=keys,
      values=values,
      key_schema=key_schema,
      value_schema=value_schema,
      schema=schema,
      itemid=itemid,
      **optools.unified_non_deterministic_kwarg(),
  )


@optools.as_backend_operator('kd.dicts._like', deterministic=False)
def _like(
    shape_and_mask_from, keys, values, key_schema, value_schema, schema, itemid
):  # pylint: disable=unused-argument
  """Implementation of `kd.dicts.like`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.dict_like'])
@optools.as_lambda_operator(
    'kd.dicts.like',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_and_mask_from),
        qtype_utils.expect_data_slice_or_unspecified(P.keys),
        qtype_utils.expect_data_slice_or_unspecified(P.values),
        qtype_utils.expect_data_slice_or_unspecified(P.key_schema),
        qtype_utils.expect_data_slice_or_unspecified(P.value_schema),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
    ],
)
def like(
    shape_and_mask_from,
    /,
    keys=arolla.unspecified(),
    values=arolla.unspecified(),
    *,
    key_schema=arolla.unspecified(),
    value_schema=arolla.unspecified(),
    schema=arolla.unspecified(),
    itemid=arolla.unspecified(),
):
  """Creates new Koda dicts with shape and sparsity of `shape_and_mask_from`.

  If items_or_keys and values are not provided, creates empty dicts. Otherwise,
  the function assigns the given keys and values to the newly created dicts. So
  the keys and values must be either broadcastable to shape_and_mask_from
  shape, or one dimension higher.

  Args:
    shape_and_mask_from: a DataSlice with the shape and sparsity for the desired
      dicts.
    keys: a DataSlice with keys.
    values: a DataSlice of values.
    key_schema: the schema of the dict keys. If not specified, it will be
      deduced from keys or defaulted to OBJECT.
    value_schema: the schema of the dict values. If not specified, it will be
      deduced from values or defaulted to OBJECT.
    schema: the schema to use for the newly created Dict. If specified, then
      key_schema and value_schema must not be specified.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting dicts.

  Returns:
    A DataSlice with the dicts.
  """
  keys = M.core.default_if_unspecified(keys, data_slice.unspecified())
  values = M.core.default_if_unspecified(values, data_slice.unspecified())
  key_schema = M.core.default_if_unspecified(
      key_schema, data_slice.unspecified()
  )
  value_schema = M.core.default_if_unspecified(
      value_schema, data_slice.unspecified()
  )
  schema = M.core.default_if_unspecified(schema, data_slice.unspecified())
  itemid = M.core.default_if_unspecified(itemid, data_slice.unspecified())
  return _like(
      shape_and_mask_from,
      keys=keys,
      values=values,
      key_schema=key_schema,
      value_schema=value_schema,
      schema=schema,
      itemid=itemid,
  )


@optools.add_to_registry(aliases=['kd.dict_size'])
@optools.as_backend_operator(
    'kd.dicts.size',
    qtype_constraints=[qtype_utils.expect_data_slice(P.dict_slice)],
)
def size(dict_slice):  # pylint: disable=unused-argument
  """Returns size of a Dict."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.has_dict'])
@optools.as_backend_operator(
    'kd.dicts.has_dict',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def has_dict(x):  # pylint: disable=unused-argument
  """Returns present for each item in `x` that is Dict.

  Note that this is a pointwise operation.

  Also see `kd.is_dict` for checking if `x` is a Dict DataSlice. But note that
  `kd.all(kd.has_dict(x))` is not always equivalent to `kd.is_dict(x)`. For
  example,

    kd.is_dict(kd.item(None, kd.OBJECT)) -> kd.present
    kd.all(kd.has_dict(kd.item(None, kd.OBJECT))) -> invalid for kd.all
    kd.is_dict(kd.item([None], kd.OBJECT)) -> kd.present
    kd.all(kd.has_dict(kd.item([None], kd.OBJECT))) -> kd.missing

  Args:
    x: DataSlice to check.

  Returns:
    A MASK DataSlice with the same shape as `x`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.is_dict'])
@optools.as_backend_operator(
    'kd.dicts.is_dict', qtype_constraints=[qtype_utils.expect_data_slice(P.x)]
)
def is_dict(x):  # pylint: disable=unused-argument
  """Returns whether x is a Dict DataSlice.

  `x` is a Dict DataSlice if it meets one of the following conditions:
    1) it has a Dict schema
    2) it has OBJECT schema and only has Dict items

  Also see `kd.has_dict` for a pointwise version. But note that
  `kd.all(kd.has_dict(x))` is not always equivalent to `kd.is_dict(x)`. For
  example,

    kd.is_dict(kd.item(None, kd.OBJECT)) -> kd.present
    kd.all(kd.has_dict(kd.item(None, kd.OBJECT))) -> invalid for kd.all
    kd.is_dict(kd.item([None], kd.OBJECT)) -> kd.present
    kd.all(kd.has_dict(kd.item([None], kd.OBJECT))) -> kd.missing

  Args:
    x: DataSlice to check.

  Returns:
    A MASK DataItem.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.get_keys'])
@optools.as_backend_operator(
    'kd.dicts.get_keys',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.dict_ds),
    ],
)
def get_keys(dict_ds):  # pylint: disable=unused-argument
  """Returns keys of all Dicts in `dict_ds`.

  The result DataSlice has one more dimension used to represent keys in each
  dict than `dict_ds`. While the order of keys within a dict is arbitrary, it is
  the same as get_values().

  Args:
    dict_ds: DataSlice of Dicts.

  Returns:
    A DataSlice of keys.
  """
  raise NotImplementedError('implemented in the backend')


@optools.as_backend_operator('kd.dicts._get_values')
def _get_values(dict_ds):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.as_backend_operator('kd.dicts._get_values_by_keys')
def _get_values_by_keys(dict_ds, key_ds):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.get_values'])
@optools.as_lambda_operator(
    'kd.dicts.get_values',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.dict_ds),
        qtype_utils.expect_data_slice_or_unspecified(P.key_ds),
    ],
)
def get_values(dict_ds, key_ds=arolla.unspecified()):
  """Returns values corresponding to `key_ds` for dicts in `dict_ds`.

  When `key_ds` is specified, it is equivalent to dict_ds[key_ds].

  When `key_ds` is unspecified, it returns all values in `dict_ds`. The result
  DataSlice has one more dimension used to represent values in each dict than
  `dict_ds`. While the order of values within a dict is arbitrary, it is the
  same as get_keys().

  Args:
    dict_ds: DataSlice of Dicts.
    key_ds: DataSlice of keys or unspecified.

  Returns:
    A DataSlice of values.
  """
  return arolla.types.DispatchOperator(
      'dict_ds, key_ds',
      unspecified_case=arolla.types.DispatchCase(
          _get_values(P.dict_ds),
          condition=(P.key_ds == arolla.UNSPECIFIED),
      ),
      default=_get_values_by_keys(P.dict_ds, P.key_ds),
  )(dict_ds, key_ds)


@optools.as_backend_operator(
    'kd.dicts._dict_update', qtype_inference_expr=qtypes.DATA_BAG
)
def _dict_update(x, keys, values):  # pylint: disable=unused-argument
  """Backend operator for kd.dict_update(x, keys, values)."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.dict_update'])
@optools.as_lambda_operator(
    'kd.dicts.dict_update',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.keys),
        qtype_utils.expect_data_slice_or_unspecified(P.values),
    ],
)
def dict_update(x, keys, values=arolla.unspecified()):
  """Returns DataBag containing updates to a DataSlice of dicts.

  This operator has two forms:
    kd.dict_update(x, keys, values) where keys and values are slices
    kd.dict_update(x, dict_updates) where dict_updates is a DataSlice of dicts

  If both keys and values are specified, they must both be broadcastable to the
  shape of `x`. If only keys is specified (as dict_updates), it must be
  broadcastable to 'x'.

  Args:
    x: DataSlice of dicts to update.
    keys: A DataSlice of keys, or a DataSlice of dicts of updates.
    values: A DataSlice of values, or unspecified if `keys` contains dicts.
  """
  return arolla.types.DispatchOperator(
      'x, keys, values',
      unspecified_case=arolla.types.DispatchCase(
          # Note: relies on get_keys and get_values having the same order
          # (which is guaranteed, but not obvious).
          _dict_update(P.x, get_keys(P.keys), get_values(P.keys)),
          condition=(P.values == arolla.UNSPECIFIED),
      ),
      default=_dict_update(P.x, P.keys, P.values),
  )(x, keys, values)


@optools.add_to_registry(aliases=['kd.with_dict_update'])
@optools.as_lambda_operator(
    'kd.dicts.with_dict_update',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.keys),
        qtype_utils.expect_data_slice_or_unspecified(P.values),
    ],
)
def with_dict_update(x, keys, values=arolla.unspecified()):
  """Returns a DataSlice with a new DataBag containing updated dicts.

  This operator has two forms:
    kd.with_dict_update(x, keys, values) where keys and values are slices
    kd.with_dict_update(x, dict_updates) where dict_updates is a DataSlice of
      dicts

  If both keys and values are specified, they must both be broadcastable to the
  shape of `x`. If only keys is specified (as dict_updates), it must be
  broadcastable to 'x'.

  Args:
    x: DataSlice of dicts to update.
    keys: A DataSlice of keys, or a DataSlice of dicts of updates.
    values: A DataSlice of values, or unspecified if `keys` contains dicts.
  """
  return core_ops.updated(x, dict_update(x, keys, values))
