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

"""Schema operators."""

from arolla import arolla
from koladata.operators import assertion
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.core._collapse',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _collapse_impl(ds):  # pylint: disable=unused-argument
  """Creates a new DataSlice by collapsing 'ds' over its last dimension.

  Args:
    ds: DataSlice to be collapsed

  Returns:
    Collapsed DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


# NOTE: Implemented here to avoid a dependency cycle between core and schema.
@optools.add_to_registry(aliases=['kde.collapse'])
@optools.as_lambda_operator(
    'kde.core.collapse',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def _collapse(x, ndim=arolla.unspecified()):
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
      rank(x).

  Returns:
    Collapsed DataSlice.
  """
  return _collapse_impl(jagged_shape_ops.flatten_last_ndim(x, ndim))


# NOTE: Implemented here to avoid a dependency cycle between logical and schema.
@optools.add_to_registry(aliases=['kde.has'])
@optools.as_backend_operator(
    'kde.logical.has',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _has(x):  # pylint: disable=unused-argument
  """Returns presence of `x`.

  Pointwise operator which take a DataSlice and return a MASK indicating the
  presence of each item in `x`. Returns `kd.present` for present items and
  `kd.missing` for missing items.

  Args:
    x: DataSlice.

  Returns:
    DataSlice representing the presence of `x`.
  """
  raise NotImplementedError('implemented in the backend')


# TODO: Make this operator public again once non-determinism is
# designed and implemented.
@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.schema._new_schema',
    aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    qtype_constraints=[
        (
            M.qtype.is_namedtuple_qtype(P.kwargs),
            f'expected named tuple, got {constraints.name_type_msg(P.kwargs)}',
        ),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _new_schema(kwargs=py_boxing.var_keyword()):  # pylint: disable=unused-argument
  """Creates a new allocated schema.

  Args:
    kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
      values must be schemas themselves.

  Returns:
    (DataSlice) containing the schema id.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.uu_schema'])
@optools.as_backend_operator(
    'kde.schema.uu_schema',
    aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        (
            M.qtype.is_namedtuple_qtype(P.kwargs),
            f'expected named tuple, got {constraints.name_type_msg(P.kwargs)}',
        ),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def uu_schema(
    seed=py_boxing.positional_or_keyword(''), kwargs=py_boxing.var_keyword()  # pylint: disable=unused-argument
):
  """Creates a UUSchema, i.e. a schema keyed by a uuid.

  In order to create a different id from the same arguments, use
  `seed` argument with the desired value, e.g.

  kd.uu_schema(seed='type_1', x=kd.INT32, y=kd.FLOAT32)

  and

  kd.uu_schema(seed='type_2', x=kd.INT32, y=kd.FLOAT32)

  have different ids.

  Args:
    seed: text seed for the uuid computation.
    kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
      values must be schemas themselves.

  Returns:
    (DataSlice) containing the schema uuid.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.with_schema'])
@optools.as_backend_operator(
    'kde.schema.with_schema',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.schema),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def with_schema(x, schema):  # pylint: disable=unused-argument
  """Returns a copy of `x` with the provided `schema`.

  If `schema` is an Entity schema, it must have no DataBag or the same DataBag
  as `x`. To set schema with a different DataBag, use `kd.set_schema` instead.

  It only changes the schemas of `x` and does not change the items in `x`. To
  change the items in `x`, use `kd.cast_to` instead. For example,

    kd.with_schema(kd.ds([1, 2, 3]), kd.FLOAT32) -> fails because the items in
        `x` are not compatible with FLOAT32.
    kd.cast_to(kd.ds([1, 2, 3]), kd.FLOAT32) -> kd.ds([1.0, 2.0, 3.0])

  When items in `x` are primitives or `schemas` is a primitive schema, it checks
  items and schema are compatible. When items are ItemIds and `schema` is a
  non-primitive schema, it does not check the underlying data matches the
  schema. For example,

    kd.with_schema(kd.ds([1, 2, 3], schema=kd.ANY), kd.INT32) ->
        kd.ds([1, 2, 3])
    kd.with_schema(kd.ds([1, 2, 3]), kd.INT64) -> fail

    db = kd.bag()
    kd.with_schema(kd.ds(1).with_db(db), db.new_schema(x=kd.INT32)) -> fail due
        to incompatible schema
    kd.with_schema(db.new(x=1), kd.INT32) -> fail due to incompatible schema
    kd.with_schema(db.new(x=1), kd.new_schema(x=kd.INT32)) -> fail due to
        different DataBag
    kd.with_schema(db.new(x=1), kd.new_schema(x=kd.INT32).no_db()) -> work
    kd.with_schema(db.new(x=1), db.new_schema(x=kd.INT64)) -> work

  Args:
    x: DataSlice to change the schema of.
    schema: DataSlice containing the new schema.

  Returns:
    DataSlice with the new schema.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.schema.cast_to_implicit',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.schema),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def cast_to_implicit(x, schema):  # pylint: disable=unused-argument
  """Returns `x` casted to the provided `schema` using implicit casting rules.

  Note that `schema` must be the common schema of `schema` and `x.get_schema()`
  according to go/koda-type-promotion.

  Args:
    x: DataSlice to cast.
    schema: Schema to cast to. Must be a scalar.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.cast_to'])
@optools.as_backend_operator(
    'kde.schema.cast_to',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.schema),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def cast_to(x, schema):  # pylint: disable=unused-argument
  """Returns `x` casted to the provided `schema` using explicit casting rules.

  Dispatches to the relevant `kd.to_...` operator. Performs permissive casting,
  e.g. allowing FLOAT32 -> INT32 casting through `kd.cast_to(slice, INT32)`.

  Args:
    x: DataSlice to cast.
    schema: Schema to cast to. Must be a scalar.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.schema.cast_to_narrow',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.schema),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def cast_to_narrow(x, schema):  # pylint: disable=unused-argument
  """Returns `x` casted to the provided `schema`.

  Allows for schema narrowing, where OBJECT and ANY types can be casted to
  primitive schemas as long as the data is implicitly castable to the schema.
  Follows the casting rules of `kd.cast_to_implicit` for the narrowed schema.

  Args:
    x: DataSlice to cast.
    schema: Schema to cast to. Must be a scalar.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.list_schema'])
@optools.as_backend_operator(
    'kde.schema.list_schema',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.item_schema),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    aux_policy=py_boxing.DEFAULT_AROLLA_POLICY,
)
def list_schema(item_schema):  # pylint: disable=unused-argument
  """Returns a List schema with the provided `item_schema`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.dict_schema'])
@optools.as_backend_operator(
    'kde.schema.dict_schema',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.key_schema),
        qtype_utils.expect_data_slice(P.value_schema),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    aux_policy=py_boxing.DEFAULT_AROLLA_POLICY,
)
def dict_schema(key_schema, value_schema):  # pylint: disable=unused-argument
  """Returns a Dict schema with the provided `key_schema` and `value_schema`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.to_int32'])
@optools.as_lambda_operator('kde.schema.to_int32')
def to_int32(x):
  """Casts `x` to INT32 using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.INT32)


@optools.add_to_registry(aliases=['kde.to_int64'])
@optools.as_lambda_operator('kde.schema.to_int64')
def to_int64(x):
  """Casts `x` to INT64 using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.INT64)


@optools.add_to_registry(aliases=['kde.to_float32'])
@optools.as_lambda_operator('kde.schema.to_float32')
def to_float32(x):
  """Casts `x` to FLOAT32 using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.FLOAT32)


@optools.add_to_registry(aliases=['kde.to_float64'])
@optools.as_lambda_operator('kde.schema.to_float64')
def to_float64(x):
  """Casts `x` to FLOAT64 using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.FLOAT64)


@optools.add_to_registry(aliases=['kde.to_mask'])
@optools.as_lambda_operator('kde.schema.to_mask')
def to_mask(x):
  """Casts `x` to MASK using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.MASK)


@optools.add_to_registry(aliases=['kde.to_bool'])
@optools.as_lambda_operator('kde.schema.to_bool')
def to_bool(x):
  """Casts `x` to BOOLEAN using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.BOOLEAN)


@optools.add_to_registry(aliases=['kde.to_bytes'])
@optools.as_lambda_operator('kde.schema.to_bytes')
def to_bytes(x):
  """Casts `x` to BYTES using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.BYTES)


@optools.add_to_registry(aliases=['kde.to_text'])
@optools.as_lambda_operator('kde.schema.to_text')
def to_text(x):
  """Casts `x` to TEXT using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.TEXT)


@optools.add_to_registry(aliases=['kde.decode'])
@optools.as_backend_operator(
    'kde.schema.decode',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def decode(x):  # pylint: disable=unused-argument
  """Decodes `x` as TEXT using UTF-8 decoding."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.encode'])
@optools.as_backend_operator(
    'kde.schema.encode',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def encode(x):  # pylint: disable=unused-argument
  """Encodes `x` as BYTES using UTF-8 encoding."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.to_expr'])
@optools.as_lambda_operator('kde.schema.to_expr')
def to_expr(x):
  """Casts `x` to EXPR using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.EXPR)


@optools.add_to_registry(aliases=['kde.to_schema'])
@optools.as_lambda_operator('kde.schema.to_schema')
def to_schema(x):
  """Casts `x` to SCHEMA using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.SCHEMA)


@optools.add_to_registry(
    aliases=['kde.to_itemid', 'kde.schema.as_itemid', 'kde.as_itemid']
)
@optools.as_lambda_operator('kde.schema.to_itemid')
def to_itemid(x):
  """Casts `x` to ITEMID using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.ITEMID)


@optools.add_to_registry(aliases=['kde.to_object'])
@optools.as_lambda_operator('kde.schema.to_object')
def to_object(x):
  """Casts `x` to OBJECT using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.OBJECT)


@optools.add_to_registry(
    aliases=['kde.to_any', 'kde.schema.as_any', 'kde.as_any']
)
@optools.as_lambda_operator('kde.schema.to_any')
def to_any(x):
  """Casts `x` to ANY using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.ANY)


@optools.add_to_registry(aliases=['kde.to_none'])
@optools.as_lambda_operator('kde.schema.to_none')
def to_none(x):
  """Casts `x` to NONE using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.NONE)


@optools.add_to_registry(aliases=['kde.get_schema'])
@optools.as_backend_operator(
    'kde.schema.get_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def get_schema(x):  # pylint: disable=unused-argument
  """Returns the schema of `x`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(
    aliases=[
        'kde.get_primitive_schema',
        'kde.schema.get_dtype',
        'kde.get_dtype',
    ]
)
@optools.as_backend_operator(
    'kde.schema.get_primitive_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.ds)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def get_primitive_schema(ds):  # pylint: disable=unused-argument
  """Returns a primitive schema representing the underlying items' dtype.

  If `ds` has a primitive schema, this returns that primitive schema, even if
  all items in `ds` are missing. If `ds` has an OBJECT/ANY schema but contains
  primitive values of a single dtype, it returns the schema for that primitive
  dtype.

  In case of items in `ds` have non-primitive types or mixed dtypes, returns
  a missing schema (i.e. `kd.item(None, kd.SCHEMA)`).

  Examples:
    kd.get_primitive_schema(kd.slice([1, 2, 3])) -> kd.INT32
    kd.get_primitive_schema(kd.slice([None, None, None], kd.INT32)) -> kd.INT32
    kd.get_primitive_schema(kd.slice([1, 2, 3], kd.OBJECT)) -> kd.INT32
    kd.get_primitive_schema(kd.slice([1, 2, 3], kd.ANY)) -> kd.INT32
    kd.get_primitive_schema(kd.slice([1, 'a', 3], kd.OBJECT)) -> missing schema
    kd.get_primitive_schema(kd.obj())) -> missing schema

  Args:
    ds: DataSlice to get dtype from.

  Returns:
    a primitive schema DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.get_obj_schema'])
@optools.as_backend_operator(
    'kde.schema.get_obj_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
    # Use Arolla boxing to avoid boxing literals into DataSlices.
    aux_policy=py_boxing.DEFAULT_AROLLA_POLICY,
)
def get_obj_schema(x):  # pylint: disable=unused-argument
  """Returns a DataSlice of schemas for Objects and primitives in `x`.

  DataSlice `x` must have OBJECT schema.

  Examples:
    db = kd.bag()
    s = db.new_schema(a=kd.INT32)
    obj = s(a=1).embed_schema()
    kd.get_obj_schema(kd.slice([1, None, 2.0, obj]))
      -> kd.slice([kd.INT32, NONE, kd.FLOAT32, s])

  Args:
    x: OBJECT DataSlice

  Returns:
    A DataSlice of schemas.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.get_item_schema'])
@optools.as_backend_operator(
    'kde.schema.get_item_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.list_schema)],
    qtype_inference_expr=qtypes.DATA_SLICE,
    # Use Arolla boxing to avoid boxing literals into DataSlices.
    aux_policy=py_boxing.DEFAULT_AROLLA_POLICY,
)
def get_item_schema(list_schema):  # pylint: disable=unused-argument,redefined-outer-name
  """Returns the item schema of a List schema`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.get_key_schema'])
@optools.as_backend_operator(
    'kde.schema.get_key_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.dict_schema)],
    qtype_inference_expr=qtypes.DATA_SLICE,
    # Use Arolla boxing to avoid boxing literals into DataSlices.
    aux_policy=py_boxing.DEFAULT_AROLLA_POLICY,
)
def get_key_schema(dict_schema):  # pylint: disable=unused-argument,redefined-outer-name
  """Returns the key schema of a Dict schema`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.get_value_schema'])
@optools.as_backend_operator(
    'kde.schema.get_value_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.dict_schema)],
    qtype_inference_expr=qtypes.DATA_SLICE,
    # Use Arolla boxing to avoid boxing literals into DataSlices.
    aux_policy=py_boxing.DEFAULT_AROLLA_POLICY,
)
def get_value_schema(dict_schema):  # pylint: disable=unused-argument,redefined-outer-name
  """Returns the value schema of a Dict schema`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.with_schema_from_obj'])
@optools.as_lambda_operator(
    'kde.schema.with_schema_from_obj',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def with_schema_from_obj(x):
  """Returns `x` with its embedded schema set as the schema.

  * `x` must have OBJECT schema.
  * All items in `x` must have the same embedded schema.
  * At least one value in `x` must be present.

  Args:
    x: An OBJECT DataSlice.
  """
  embedded_schema = _collapse(jagged_shape_ops.flatten(get_obj_schema(x)))
  embedded_schema = assertion.with_assertion(
      embedded_schema,
      _has(embedded_schema),
      'objects or primitives in `x` do not have an uniform schema',
  )
  return with_schema(x, embedded_schema)
