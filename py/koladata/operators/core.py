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

"""Core DataSlice operators."""

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.operators import arolla_bridge
from koladata.operators import assertion
from koladata.operators import masking
from koladata.operators import op_repr
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import schema as schema_ops
from koladata.operators import tuple as tuple_ops
from koladata.operators import view_overloads as _
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants

M = arolla.OperatorsContainer(jagged_shape)
P = arolla.P
MASK = schema_constants.MASK
constraints = arolla.optools.constraints


@optools.as_backend_operator('kd.core._get_attr')
def _get_attr(x, attr_name):  # pylint: disable=unused-argument
  """Gets an attribute from a DataSlice."""
  raise NotImplementedError('implemented in the backend')


@optools.as_backend_operator('kd.core._get_attr_with_default')
def _get_attr_with_default(x, attr_name, default):  # pylint: disable=unused-argument
  """Gets an attribute from a DataSlice replacing missing items from default."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.get_attr'], repr_fn=op_repr.getattr_repr)
@optools.as_lambda_operator(
    'kd.core.get_attr',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.attr_name),
        # None is boxed as an empty OBJECT DataSlice.
        qtype_utils.expect_data_slice_or_unspecified(P.default),
    ],
)
def get_attr(x, attr_name, default=arolla.unspecified()):
  """Resolves (ObjectId(s), attr_name) => (Value|ObjectId)s.

  In case attr points to Lists or Maps, the result is a DataSlice that
  contains "pointers" to the beginning of lists/dicts.

  For simple values ((entity, attr) => values), just returns
  DataSlice(primitive values)

  Args:
    x: DataSlice to get attribute from.
    attr_name: name of the attribute to access.
    default: default value to use when `x` does not have such attribute. In case
      default is specified, this will not warn/raise if the attribute does not
      exist in the schema, so one can use `default=None` to suppress the missing
      attribute warning/error. When `default=None` and the attribute is missing
      on all entities, this will return an empty slices with NONE schema.

  Returns:
    DataSlice
  """
  return arolla.types.DispatchOperator(
      'x, attr_name, default',
      unspecified_case=arolla.types.DispatchCase(
          _get_attr(P.x, P.attr_name),
          condition=P.default == arolla.UNSPECIFIED,
      ),
      default=_get_attr_with_default(P.x, P.attr_name, P.default),
  )(x, attr_name, default)


@optools.add_to_registry(aliases=['kd.maybe'])
@optools.as_lambda_operator(
    'kd.core.maybe',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.attr_name),
    ],
)
def maybe(x, attr_name):
  """A shortcut for kd.get_attr(x, attr_name, default=None)."""
  return _get_attr_with_default(x, attr_name, None)


@optools.add_to_registry(aliases=['kd.has_attr'])
@optools.as_lambda_operator(
    'kd.core.has_attr',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.attr_name),
    ],
)
def has_attr(x, attr_name):
  """Indicates whether the items in `x` DataSlice have the given attribute.

  This function checks for attributes based on data rather than "schema" and may
  be slow in some cases.

  Args:
    x: DataSlice
    attr_name: Name of the attribute to check.

  Returns:
    A MASK DataSlice with the same shape as `x` that contains present if the
    attribute exists for the corresponding item.
  """
  return masking.has(
      maybe(x & (x.get_schema() == schema_constants.SCHEMA), attr_name)
  ) | masking.has(
      maybe(
          (x & (x.get_schema() != schema_constants.SCHEMA)).as_any(),
          attr_name,
      )
  )


@optools.add_to_registry(aliases=['kd.has_primitive'])
@optools.as_backend_operator(
    'kd.core.has_primitive',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def has_primitive(x):  # pylint: disable=unused-argument
  """Returns present for each item in `x` that is primitive.

  Note that this is a pointwise operation.

  Also see `kd.is_primitive` for checking if `x` is a primitive DataSlice. But
  note that `kd.all(kd.has_primitive(x))` is not always equivalent to
  `kd.is_primitive(x)`. For example,

    kd.is_primitive(kd.int32(None)) -> kd.present
    kd.all(kd.has_primitive(kd.int32(None))) -> invalid for kd.all
    kd.is_primitive(kd.int32([None])) -> kd.present
    kd.all(kd.has_primitive(kd.int32([None]))) -> kd.missing

  Args:
    x: DataSlice to check.

  Returns:
    A MASK DataSlice with the same shape as `x`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.is_primitive'])
@optools.as_backend_operator(
    'kd.core.is_primitive',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def is_primitive(x):  # pylint: disable=unused-argument
  """Returns whether x is a primitive DataSlice.

  `x` is a primitive DataSlice if it meets one of the following conditions:
    1) it has a primitive schema
    2) it has OBJECT/ANY/SCHEMA schema and only has primitives

  Also see `kd.has_primitive` for a pointwise version. But note that
  `kd.all(kd.has_primitive(x))` is not always equivalent to
  `kd.is_primitive(x)`. For example,

    kd.is_primitive(kd.int32(None)) -> kd.present
    kd.all(kd.has_primitive(kd.int32(None))) -> invalid for kd.all
    kd.is_primitive(kd.int32([None])) -> kd.present
    kd.all(kd.has_primitive(kd.int32([None]))) -> kd.missing

  Args:
    x: DataSlice to check.

  Returns:
    A MASK DataItem.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.has_entity'])
@optools.as_backend_operator(
    'kd.core.has_entity',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def has_entity(x):  # pylint: disable=unused-argument
  """Returns present for each item in `x` that is an Entity.

  Note that this is a pointwise operation.

  Also see `kd.is_entity` for checking if `x` is an Entity DataSlice. But
  note that `kd.all(kd.has_entity(x))` is not always equivalent to
  `kd.is_entity(x)`. For example,

    kd.is_entity(kd.item(None, kd.OBJECT)) -> kd.present
    kd.all(kd.has_entity(kd.item(None, kd.OBJECT))) -> invalid for kd.all
    kd.is_entity(kd.item([None], kd.OBJECT)) -> kd.present
    kd.all(kd.has_entity(kd.item([None], kd.OBJECT))) -> kd.missing

  Args:
    x: DataSlice to check.

  Returns:
    A MASK DataSlice with the same shape as `x`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.is_entity'])
@optools.as_backend_operator(
    'kd.core.is_entity',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def is_entity(x):  # pylint: disable=unused-argument
  """Returns whether x is an Entity DataSlice.

  `x` is an Entity DataSlice if it meets one of the following conditions:
    1) it has an Entity schema
    2) it has OBJECT/ANY schema and only has Entity items

  Also see `kd.has_entity` for a pointwise version. But note that
  `kd.all(kd.has_entity(x))` is not always equivalent to
  `kd.is_entity(x)`. For example,

    kd.is_entity(kd.item(None, kd.OBJECT)) -> kd.present
    kd.all(kd.has_entity(kd.item(None, kd.OBJECT))) -> invalid for kd.all
    kd.is_entity(kd.item([None], kd.OBJECT)) -> kd.present
    kd.all(kd.has_entity(kd.item([None], kd.OBJECT))) -> kd.missing

  Args:
    x: DataSlice to check.

  Returns:
    A MASK DataItem.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.stub'])
@optools.as_backend_operator(
    'kd.core.stub',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.attrs),
    ],
)
def stub(x, attrs=data_slice.DataSlice.from_vals([])):  # pylint: disable=unused-argument
  """Copies a DataSlice's schema stub to a new DataBag.

  The "schema stub" of a DataSlice is a subset of its schema (including embedded
  schemas) that contains just enough information to support direct updates to
  that DataSlice.

  Optionally copies `attrs` schema attributes to the new DataBag as well.

  This method works for items, objects, and for lists and dicts stored as items
  or objects. The intended usage is to add new attributes to the object in the
  new bag, or new items to the dict in the new bag, and then to be able
  to merge the bags to obtain a union of attributes/values. For lists, we
  extract the list with stubs for list items, which also works recursively so
  nested lists are deep-extracted. Note that if you modify the list afterwards
  by appending or removing items, you will no longer be able to merge the result
  with the original bag.

  Args:
    x: DataSlice to extract the schema stub from.
    attrs: Optional list of additional schema attribute names to copy. The
      schemas for those attributes will be copied recursively (so including
      attributes of those attributes etc).

  Returns:
    DataSlice with the same schema stub in the new DataBag.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.attrs'])
@optools.as_backend_operator(
    'kd.core.attrs',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.update_schema),
        qtype_utils.expect_data_slice_kwargs(P.attrs),
    ],
    qtype_inference_expr=qtypes.DATA_BAG,
)
def _attrs(x, /, *, update_schema=False, **attrs):
  """Returns a new DataBag containing attribute updates for `x`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.attr'])
@optools.as_backend_operator(
    'kd.core.attr',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.attr_name),
        qtype_utils.expect_data_slice(P.value),
        qtype_utils.expect_data_slice(P.update_schema),
    ],
    qtype_inference_expr=qtypes.DATA_BAG,
)
def _attr(x, attr_name, value, update_schema=False):
  """Returns a new DataBag containing attribute `attr_name` update for `x`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.with_attrs'])
@optools.as_backend_operator(
    'kd.core.with_attrs',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.update_schema),
        qtype_utils.expect_data_slice_kwargs(P.attrs),
    ],
)
def with_attrs(x, /, *, update_schema=False, **attrs):
  """Returns a DataSlice with a new DataBag containing updated attributes."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.with_attr'])
@optools.as_backend_operator(
    'kd.core.with_attr',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.attr_name),
        qtype_utils.expect_data_slice(P.value),
        qtype_utils.expect_data_slice(P.update_schema),
    ],
)
def with_attr(x, attr_name, value, update_schema=False):
  """Returns a DataSlice with a new DataBag containing a single updated attribute."""
  raise NotImplementedError('implemented in the backend')


# TODO: Remove the *_db alias.
@optools.add_to_registry(
    aliases=[
        'kd.with_bag',
        'kd.with_db',
        'kd.core.with_db',
    ]
)
@optools.as_backend_operator(
    'kd.core.with_bag',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_bag(P.bag),
    ],
)
def with_bag(ds, bag):  # pylint: disable=unused-argument
  """Returns a DataSlice with the given DataBatg attached."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry_as_overload(
    overload_condition_expr=P.bag == qtypes.DATA_BAG
)
@optools.as_lambda_operator('koda_internal.view.get_item._bag')
def _get_item_bag(bag, ds):
  return with_bag(ds, bag)


@optools.as_backend_operator(
    'kd.core._get_list_item_by_range',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        arolla.optools.constraints.expect_scalar_integer(P.start),
        arolla.optools.constraints.expect_scalar_integer(P.stop),
    ],
)
def _get_list_item_by_range(ds, start, stop):  # pylint: disable=unused-argument
  """Gets an attribute from a DataSlice."""
  raise NotImplementedError('implemented in the backend')


@optools.as_lambda_operator(
    'kd.core._get_list_item_by_slice',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        (
            M.qtype.is_slice_qtype(P.s),
            'key_or_index must be Slice',
        ),
    ],
)
def _get_list_item_by_slice(x, s):
  """Get List items in `x` by Slice `s`."""
  normalize_slice_arg = arolla.types.DispatchOperator(
      'n, default',
      data_slice_case=arolla.types.DispatchCase(
          arolla_bridge.to_arolla_int64(P.n),
          condition=P.n == qtypes.DATA_SLICE,
      ),
      undefined_case=arolla.types.DispatchCase(
          P.default,
          condition=P.n == arolla.UNSPECIFIED,
      ),
  )
  start = normalize_slice_arg(tuple_ops.get_nth(s, 0), 0)
  stop = normalize_slice_arg(tuple_ops.get_nth(s, 1), arolla.int64(2**63 - 1))
  step = normalize_slice_arg(tuple_ops.get_nth(s, 2), 1)
  x = assertion.with_assertion(
      x, step == 1, 'kd.core.get_item: slice with step != 1 is not supported'
  )
  return _get_list_item_by_range(x, start, stop)


@optools.as_backend_operator('kd.core._get_item')
def _get_item(x, key_or_index):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry_as_overload(
    'koda_internal.view.get_item._slice',
    overload_condition_expr=P.x == qtypes.DATA_SLICE,
)
@optools.add_to_registry(
    'kd.core.get_item',
    aliases=[
        'kd.get_item',
        'kd.lists.get_item',
        'kd.dicts.get_item',
    ],
    repr_fn=op_repr.get_item_repr,
)
@optools.as_lambda_operator(
    'kd.core.get_item',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        (
            (P.key_or_index == qtypes.DATA_SLICE)
            | M.qtype.is_slice_qtype(P.key_or_index),
            'key_or_index must be DataSlice or Slice',
        ),
    ],
)
def get_item(x, key_or_index):
  """Get items from Lists or Dicts in `x` by `key_or_index`.

  Examples:
  l = kd.list([1, 2, 3])
  # Get List items by range slice from 1 to -1
  kd.get_item(l, slice(1, -1)) -> kd.slice([2, 3])
  # Get List items by indices
  kd.get_item(l, kd.slice([2, 5])) -> kd.slice([3, None])

  d = kd.dict({'a': 1, 'b': 2})
  # Get Dict values by keys
  kd.get_item(d, kd.slice(['a', 'c'])) -> kd.slice([1, None])

  Args:
    x: List or Dict DataSlice.
    key_or_index: DataSlice or Slice.

  Returns:
    Result DataSlice.
  """
  return arolla.types.DispatchOperator(
      'x, key_or_index',
      unspecified_case=arolla.types.DispatchCase(
          _get_item(P.x, P.key_or_index),
          condition=P.key_or_index == qtypes.DATA_SLICE,
      ),
      default=_get_list_item_by_slice(P.x, P.key_or_index),
  )(x, key_or_index)


@optools.as_backend_operator('kd.core._new_ids_like', deterministic=False)
def _new_ids_like(x):  # pylint: disable=unused-argument
  """Creates a DataSlice with new ItemIds of a similar kind."""
  raise NotImplementedError('implemented in the backend')


@optools.as_backend_operator('kd.core._extract')
def _extract(ds, schema):  # pylint: disable=unused-argument
  """Creates a DataSlice with a new DataBag containing only reachable attrs."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.extract'])
@optools.as_lambda_operator(
    'kd.core.extract',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
    ],
)
def extract(ds, schema=arolla.unspecified()):
  """Creates a DataSlice with a new DataBag containing only reachable attrs.

  Args:
    ds: DataSlice to extract.
    schema: schema of the extracted DataSlice.

  Returns:
    A DataSlice with a new immutable DataBag attached.
  """
  schema = M.core.default_if_unspecified(schema, schema_ops.get_schema(ds))
  return _extract(ds, schema)


@optools.as_backend_operator('kd.core._shallow_clone')
def _shallow_clone(x, itemid, schema, non_deterministic):  # pylint: disable=unused-argument
  """Creates a DataSlice with shallow clones of immediate attributes."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.shallow_clone'])
@optools.as_lambda_operator(
    'kd.core.shallow_clone',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_kwargs(P.overrides),
    ],
)
def shallow_clone(
    x,
    /,
    *,
    itemid=arolla.unspecified(),
    schema=arolla.unspecified(),
    **overrides,
):
  """Creates a DataSlice with shallow clones of immediate attributes.

  The entities themselves get new ItemIds and their top-level attributes are
  copied by reference.

  Also see kd.clone and kd.deep_clone.

  Note that unlike kd.deep_clone, if there are multiple references to the same
  entity, the returned DataSlice will have multiple clones of it rather than
  references to the same clone.

  Args:
    x: The DataSlice to copy.{SELF}
    itemid: The ItemId to assign to cloned entities. If not specified, will
      allocate new ItemIds.
    schema: The schema to resolve attributes, and also to assign the schema to
      the resulting DataSlice. If not specified, will use the schema of 'x'.
    **overrides: attribute overrides.

  Returns:
    A copy of the entities with new ItemIds where all top-level attributes are
    copied by reference.
  """
  overrides = arolla.optools.fix_trace_kwargs(overrides)
  itemid = M.core.default_if_unspecified(itemid, _new_ids_like(x))
  schema = M.core.default_if_unspecified(schema, schema_ops.get_schema(x))
  return arolla.types.DispatchOperator(
      'x, itemid, schema, overrides, non_deterministic',
      overrides_case=arolla.types.DispatchCase(
          arolla.abc.bind_op(
              with_attrs,
              _shallow_clone(P.x, P.itemid, P.schema, P.non_deterministic),
              update_schema=py_boxing.as_qvalue(False),
              attrs=P.overrides,
          ),
          condition=arolla.M.qtype.get_field_count(P.overrides) > 0,
      ),
      default=_shallow_clone(P.x, P.itemid, P.schema, P.non_deterministic),
  )(x, itemid, schema, overrides, optools.unified_non_deterministic_arg())


@optools.as_backend_operator('kd.core._clone')
def _clone(x, itemid, schema, non_deterministic):  # pylint: disable=unused-argument
  """Creates a DataSlice with clones of provided entities in a new DataBag."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.clone'])
@optools.as_lambda_operator(
    'kd.core.clone',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_kwargs(P.overrides),
    ],
)
def clone(
    x,
    /,
    *,
    itemid=arolla.unspecified(),
    schema=arolla.unspecified(),
    **overrides,
):
  """Creates a DataSlice with clones of provided entities in a new DataBag.

  The entities themselves and their top-level attributes are cloned (with new
  ItemIds) and non-top-level attributes are extracted (with the same ItemIds).

  Also see kd.shallow_clone and kd.deep_clone.

  Note that unlike kd.deep_clone, if there are multiple references to the same
  entity, the returned DataSlice will have multiple clones of it rather than
  references to the same clone.

  Args:
    x: The DataSlice to copy.
    itemid: The ItemId to assign to cloned entities. If not specified, new
      ItemIds will be allocated.
    schema: The schema to resolve attributes, and also to assign the schema to
      the resulting DataSlice. If not specified, will use the schema of `x`.
    **overrides: attribute overrides.

  Returns:
    A copy of the entities where all top-level attributes are cloned (new
    ItemIds) and all of the rest extracted.
  """
  overrides = arolla.optools.fix_trace_kwargs(overrides)
  itemid = M.core.default_if_unspecified(itemid, _new_ids_like(x))
  schema = M.core.default_if_unspecified(schema, schema_ops.get_schema(x))
  return arolla.types.DispatchOperator(
      'x, itemid, schema, overrides, non_deterministic',
      overrides_case=arolla.types.DispatchCase(
          arolla.abc.bind_op(
              with_attrs,
              _clone(P.x, P.itemid, P.schema, P.non_deterministic),
              update_schema=py_boxing.as_qvalue(False),
              attrs=P.overrides,
          ),
          condition=arolla.M.qtype.get_field_count(P.overrides) > 0,
      ),
      default=_clone(P.x, P.itemid, P.schema, P.non_deterministic),
  )(x, itemid, schema, overrides, optools.unified_non_deterministic_arg())


@optools.as_backend_operator('kd.core._deep_clone')
def _deep_clone(x, schema, non_deterministic):  # pylint: disable=unused-argument
  """Creates a DataSlice with a deep copy of `x`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.deep_clone'])
@optools.as_lambda_operator(
    'kd.core.deep_clone',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_kwargs(P.overrides),
    ],
)
def deep_clone(x, /, schema=arolla.unspecified(), **overrides):
  """Creates a slice with a (deep) copy of the given slice.

  The entities themselves and all their attributes including both top-level and
  non-top-level attributes are cloned (with new ItemIds).

  Also see kd.shallow_clone and kd.clone.

  Note that unlike kd.clone, if there are multiple references to the same entity
  in `x`, or multiple ways to reach one entity through attributes, there will be
  exactly one clone made per entity.

  Args:
    x: The slice to copy.
    schema: The schema to use to find attributes to clone, and also to assign
      the schema to the resulting DataSlice. If not specified, will use the
      schema of 'x'.
    **overrides: attribute overrides.

  Returns:
    A (deep) copy of the given DataSlice.
    All referenced entities will be copied with newly allocated ItemIds. Note
    that UUIDs will be copied as ItemIds.
  """
  overrides = arolla.optools.fix_trace_kwargs(overrides)
  schema = M.core.default_if_unspecified(schema, schema_ops.get_schema(x))
  return arolla.types.DispatchOperator(
      'x, schema, overrides, non_deterministic',
      overrides_case=arolla.types.DispatchCase(
          arolla.abc.bind_op(
              with_attrs,
              _deep_clone(P.x, P.schema, P.non_deterministic),
              update_schema=py_boxing.as_qvalue(False),
              attrs=P.overrides,
          ),
          condition=arolla.M.qtype.get_field_count(P.overrides) > 0,
      ),
      default=_deep_clone(P.x, P.schema, P.non_deterministic),
  )(x, schema, overrides, optools.unified_non_deterministic_arg())


@optools.add_to_registry(aliases=['kd.nofollow'])
@optools.as_backend_operator(
    'kd.core.nofollow',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def nofollow(x):  # pylint: disable=unused-argument
  """Returns a nofollow DataSlice targeting the given slice.

  When a slice is wrapped into a nofollow, it's attributes are not further
  traversed during extract, clone, deep_clone, etc.

  `nofollow` is reversible.

  Args:
    x: DataSlice to wrap.
  """
  raise NotImplementedError('implemented in the backend')


# TODO: Remove the *_db alias.
@optools.add_to_registry(aliases=['kd.no_bag', 'kd.no_db', 'kd.core.no_db'])
@optools.as_backend_operator(
    'kd.core.no_bag',
    qtype_constraints=[qtype_utils.expect_data_slice(P.ds)],
)
def no_bag(ds):  # pylint: disable=unused-argument
  """Returns DataSlice without any DataBag attached."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.ref'])
@optools.as_backend_operator(
    'kd.core.ref',
    qtype_constraints=[qtype_utils.expect_data_slice(P.ds)],
)
def ref(ds):  # pylint: disable=unused-argument
  """Returns `ds` with the DataBag removed.

  Unlike `no_bag`, `ds` is required to hold ItemIds and no primitives are
  allowed.

  The result DataSlice still has the original schema. If the schema is an Entity
  schema (including List/Dict schema), it is treated an ItemId after the DataBag
  is removed.

  Args:
    ds: DataSlice of ItemIds.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.follow'])
@optools.as_backend_operator(
    'kd.core.follow',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def follow(x):  # pylint: disable=unused-argument
  """Returns the original DataSlice from a NoFollow DataSlice.

  When a DataSlice is wrapped into a NoFollow DataSlice, it's attributes
  are not further traversed during extract, clone, deep_clone, etc.
  `kd.follow` operator inverses the DataSlice back to a traversable DataSlice.

  Inverse of `nofollow`.

  Args:
    x: DataSlice to unwrap, if nofollowed.
  """
  raise NotImplementedError('implemented in the backend')


@optools.as_backend_operator(
    'kd.core._databag_freeze', qtype_inference_expr=qtypes.DATA_BAG
)
def _databag_freeze(x):  # pylint: disable=unused-argument
  """Helper operator that freezes a DataBag."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.freeze_bag'])
@optools.as_backend_operator(
    'kd.core.freeze_bag',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def freeze_bag(x):  # pylint: disable=unused-argument
  """Returns a DataSlice with an immutable DataBag with the same data."""
  raise NotImplementedError('implemented in the backend')


# TODO: remove the DataSlice version from here, making it a
# simple DataBag only operator.
@optools.add_to_registry(aliases=['kd.freeze'])
@optools.as_lambda_operator(
    'kd.core.freeze',
    qtype_constraints=[
        (
            (P.x == qtypes.DATA_SLICE) | (P.x == qtypes.DATA_BAG),
            (
                'expected DATA_BAG or DATA_SLICE, got '
                f'{constraints.name_type_msg(P.x)}'
            ),
        ),
    ],
)
def freeze(x):  # pylint: disable=unused-argument
  """Returns a frozen version of `x`."""
  return arolla.types.DispatchOperator(
      'x',
      data_slice_case=arolla.types.DispatchCase(
          freeze_bag(P.x), condition=P.x == qtypes.DATA_SLICE
      ),
      default=_databag_freeze(P.x),
  )(x)


@optools.add_to_registry(aliases=['kd.get_bag'])
@optools.as_backend_operator(
    'kd.core.get_bag',
    qtype_constraints=[qtype_utils.expect_data_slice(P.ds)],
    qtype_inference_expr=qtypes.DATA_BAG,
)
def get_bag(ds):  # pylint: disable=unused-argument
  """Returns the attached DataBag.

  It raises an Error if there is no DataBag attached.

  Args:
    ds: DataSlice to get DataBag from.

  Returns:
    The attached DataBag.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.extract_bag'])
@optools.as_lambda_operator(
    'kd.core.extract_bag',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
    ],
)
def extract_bag(ds, schema=arolla.unspecified()):
  """Creates a new DataBag containing only reachable attrs from 'ds'.

  Args:
    ds: DataSlice to extract.
    schema: schema of the extracted DataSlice.

  Returns:
    A new immutable DataBag with only the reachable attrs from 'ds'.
  """
  return get_bag(extract(ds, schema))


@optools.add_to_registry(aliases=['kd.reify'])
@optools.as_lambda_operator(
    'kd.core.reify',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_slice(P.source),
    ],
)
def reify(ds, source):
  """Assigns a bag and schema from `source` to the slice `ds`."""
  ds = with_bag(ds, get_bag(source))
  return schema_ops.with_schema(ds, schema_ops.get_schema(source))


@optools.add_to_registry(aliases=['kd.with_merged_bag'])
@optools.as_backend_operator(
    'kd.core.with_merged_bag',
    qtype_constraints=[qtype_utils.expect_data_slice(P.ds)],
)
def with_merged_bag(ds):  # pylint: disable=unused-argument
  """Returns a DataSlice with the DataBag of `ds` merged with its fallbacks.

  Note that a DataBag has multiple fallback DataBags and fallback DataBags can
  have fallbacks as well. This operator merges all of them into a new immutable
  DataBag.

  If `ds` has no attached DataBag, it raises an exception. If the DataBag of
  `ds` does not have fallback DataBags, it is equivalent to `ds.freeze_bag()`.

  Args:
    ds: DataSlice to merge fallback DataBags of.

  Returns:
    A new DataSlice with an immutable DataBags.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.enriched'])
@arolla.optools.as_backend_operator(
    'kd.core.enriched',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_bag_args(P.bag),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
)
def enriched(ds, *bag):  # pylint: disable=unused-argument
  """Returns a copy of a DataSlice with a additional fallback DataBag(s).

  Values in the original DataBag of `ds` take precedence over the ones in
  `*bag`.

  The DataBag attached to the result is a new immutable DataBag that falls back
  to the DataBag of `ds` if present and then to `*bag`.

  `enriched(x, a, b)` is equivalent to `enriched(enriched(x, a), b)`, and so on
  for additional DataBag args.

  Args:
    ds: DataSlice.
    *bag: additional fallback DataBag(s).

  Returns:
    DataSlice with additional fallbacks.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.updated'])
@arolla.optools.as_backend_operator(
    'kd.core.updated',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_bag_args(P.bag),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
)
def updated(ds, *bag):  # pylint: disable=unused-argument
  """Returns a copy of a DataSlice with DataBag(s) of updates applied.

  Values in `*bag` take precedence over the ones in the original DataBag of
  `ds`.

  The DataBag attached to the result is a new immutable DataBag that falls back
  to the DataBag of `ds` if present and then to `*bag`.

  `updated(x, a, b)` is equivalent to `updated(updated(x, b), a)`, and so on
  for additional DataBag args.

  Args:
    ds: DataSlice.
    *bag: DataBag(s) of updates.

  Returns:
    DataSlice with additional fallbacks.
  """
  raise NotImplementedError('implemented in the backend')
