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

"""Schema operators."""

from arolla import arolla
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import masking
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import qtypes
from koladata.types import schema_constants

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints

# Implemented in masking.py to avoid a dependency cycle.
with_schema = masking._with_schema  # pylint: disable=protected-access


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.schema.new_schema',
    qtype_constraints=[
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    deterministic=False,
)
def new_schema(**kwargs):  # pylint: disable=unused-argument
  """Creates a new allocated schema.

  Args:
    **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
      values must be schemas themselves.

  Returns:
    (DataSlice) containing the schema id.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.uu_schema'])
@optools.as_backend_operator(
    'kd.schema.uu_schema',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def uu_schema(seed='', **kwargs):  # pylint: disable=unused-argument
  """Creates a UUSchema, i.e. a schema keyed by a uuid.

  In order to create a different id from the same arguments, use
  `seed` argument with the desired value, e.g.

  kd.uu_schema(seed='type_1', x=kd.INT32, y=kd.FLOAT32)

  and

  kd.uu_schema(seed='type_2', x=kd.INT32, y=kd.FLOAT32)

  have different ids.

  Args:
    seed: string seed for the uuid computation.
    **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
      values must be schemas themselves.

  Returns:
    (DataSlice) containing the schema uuid.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.named_schema'])
@optools.as_backend_operator(
    'kd.schema.named_schema',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.name),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def named_schema(name, /, **kwargs):  # pylint: disable=unused-argument
  """Creates a named entity schema.

  A named schema will have its item id derived only from its name, which means
  that two named schemas with the same name will have the same item id, even in
  different DataBags, or with different kwargs passed to this method.

  Args:
    name: The name to use to derive the item id of the schema.
    **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
      values must be schemas themselves.

  Returns:
    data_slice.DataSlice with the item id of the required schema and kd.SCHEMA
    schema, with a new immutable DataBag attached containing the provided
    kwargs.
  """
  raise NotImplementedError('implemented in the backend')


@optools.as_backend_operator('kd.schema._internal_maybe_named_schema')
def _internal_maybe_named_schema(name_or_schema):
  """Internal implementation of kd.schema.internal_maybe_named_schema."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.schema.internal_maybe_named_schema',
    qtype_constraints=[
        qtype_utils.expect_data_slice_or_unspecified(P.name_or_schema),
    ],
)
def internal_maybe_named_schema(name_or_schema):
  """Converts a string to a named schema, passes through schema otherwise.

  The operator also passes through arolla.unspecified, and raises when
  it receives anything else except unspecified, string or schema DataItem.

  This operator exists to support kd.core.new* family of operators.

  Args:
    name_or_schema: The input name or schema.

  Returns:
    The schema unchanged, or a named schema with the given name.
  """
  process_if_specified = arolla.types.DispatchOperator(
      'name_or_schema',
      unspecified_case=arolla.types.DispatchCase(
          P.name_or_schema, condition=P.name_or_schema == arolla.UNSPECIFIED
      ),
      default=_internal_maybe_named_schema,
  )
  return process_if_specified(name_or_schema)


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.schema.cast_to_implicit',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.schema),
    ],
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


@optools.add_to_registry(aliases=['kd.cast_to'])
@optools.as_backend_operator(
    'kd.schema.cast_to',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.schema),
    ],
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
    'kd.schema.cast_to_narrow',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.schema),
    ],
)
def cast_to_narrow(x, schema):  # pylint: disable=unused-argument
  """Returns `x` casted to the provided `schema`.

  Allows for schema narrowing, where OBJECT types can be casted to primitive
  schemas as long as the data is implicitly castable to the schema. Follows the
  casting rules of `kd.cast_to_implicit` for the narrowed schema.

  Args:
    x: DataSlice to cast.
    schema: Schema to cast to. Must be a scalar.
  """
  raise NotImplementedError('implemented in the backend')


# IMPORTANT: Use Arolla boxing to avoid boxing literals into DataSlices.
@optools.add_to_registry(aliases=['kd.list_schema'])
@arolla.optools.as_backend_operator(
    'kd.schema.list_schema',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.item_schema),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def list_schema(item_schema):  # pylint: disable=unused-argument
  """Returns a List schema with the provided `item_schema`."""
  raise NotImplementedError('implemented in the backend')


# IMPORTANT: Use Arolla boxing to avoid boxing literals into DataSlices.
@optools.add_to_registry(aliases=['kd.dict_schema'])
@arolla.optools.as_backend_operator(
    'kd.schema.dict_schema',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.key_schema),
        qtype_utils.expect_data_slice(P.value_schema),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def dict_schema(key_schema, value_schema):  # pylint: disable=unused-argument
  """Returns a Dict schema with the provided `key_schema` and `value_schema`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator('kd.schema.to_int32')
def to_int32(x):
  """Casts `x` to INT32 using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.INT32)


@optools.add_to_registry()
@optools.as_lambda_operator('kd.schema.to_int64')
def to_int64(x):
  """Casts `x` to INT64 using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.INT64)


@optools.add_to_registry()
@optools.as_lambda_operator('kd.schema.to_float32')
def to_float32(x):
  """Casts `x` to FLOAT32 using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.FLOAT32)


@optools.add_to_registry()
@optools.as_lambda_operator('kd.schema.to_float64')
def to_float64(x):
  """Casts `x` to FLOAT64 using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.FLOAT64)


@optools.add_to_registry()
@optools.as_lambda_operator('kd.schema.to_mask')
def to_mask(x):
  """Casts `x` to MASK using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.MASK)


@optools.add_to_registry()
@optools.as_lambda_operator('kd.schema.to_bool')
def to_bool(x):
  """Casts `x` to BOOLEAN using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.BOOLEAN)


@optools.add_to_registry()
@optools.as_lambda_operator('kd.schema.to_bytes')
def to_bytes(x):
  """Casts `x` to BYTES using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.BYTES)


@optools.add_to_registry()
@optools.as_lambda_operator('kd.schema.to_str')
def to_str(x):
  """Casts `x` to STRING using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.STRING)


@optools.add_to_registry(aliases=['kd.to_expr'])
@optools.as_lambda_operator('kd.schema.to_expr')
def to_expr(x):
  """Casts `x` to EXPR using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.EXPR)


@optools.add_to_registry(aliases=['kd.to_schema'])
@optools.as_lambda_operator('kd.schema.to_schema')
def to_schema(x):
  """Casts `x` to SCHEMA using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.SCHEMA)


@optools.add_to_registry(
    aliases=[
        'kd.to_itemid',
        'kd.schema.get_itemid',
        'kd.get_itemid',
    ]
)
@optools.as_lambda_operator('kd.schema.to_itemid')
def to_itemid(x):
  """Casts `x` to ITEMID using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.ITEMID)


# pylint: enable=g-doc-args,g-doc-return-or-yield


@optools.add_to_registry(aliases=['kd.to_object'])
@optools.as_lambda_operator('kd.schema.to_object')
def to_object(x):
  """Casts `x` to OBJECT using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.OBJECT)


@optools.add_to_registry(aliases=['kd.to_none'])
@optools.as_lambda_operator('kd.schema.to_none')
def to_none(x):
  """Casts `x` to NONE using explicit (permissive) casting rules."""
  return cast_to(x, schema_constants.NONE)


@optools.add_to_registry(aliases=['kd.get_schema'])
@optools.as_backend_operator(
    'kd.schema.get_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def get_schema(x):  # pylint: disable=unused-argument
  """Returns the schema of `x`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(
    aliases=[
        'kd.get_primitive_schema',
        'kd.schema.get_dtype',
        'kd.get_dtype',
    ]
)
@optools.as_backend_operator(
    'kd.schema.get_primitive_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.ds)],
)
def get_primitive_schema(ds):  # pylint: disable=unused-argument
  """Returns a primitive schema representing the underlying items' dtype.

  If `ds` has a primitive schema, this returns that primitive schema, even if
  all items in `ds` are missing. If `ds` has an OBJECT schema but contains
  primitive values of a single dtype, it returns the schema for that primitive
  dtype.

  In case of items in `ds` have non-primitive types or mixed dtypes, returns
  a missing schema (i.e. `kd.item(None, kd.SCHEMA)`).

  Examples:
    kd.get_primitive_schema(kd.slice([1, 2, 3])) -> kd.INT32
    kd.get_primitive_schema(kd.slice([None, None, None], kd.INT32)) -> kd.INT32
    kd.get_primitive_schema(kd.slice([1, 2, 3], kd.OBJECT)) -> kd.INT32
    kd.get_primitive_schema(kd.slice([1, 'a', 3], kd.OBJECT)) -> missing schema
    kd.get_primitive_schema(kd.obj())) -> missing schema

  Args:
    ds: DataSlice to get dtype from.

  Returns:
    a primitive schema DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


# IMPORTANT: Use Arolla boxing to avoid boxing literals into DataSlices.
@optools.add_to_registry(aliases=['kd.get_obj_schema'])
@arolla.optools.as_backend_operator(
    'kd.schema.get_obj_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
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


# IMPORTANT: Use Arolla boxing to avoid boxing literals into DataSlices.
@optools.add_to_registry(aliases=['kd.get_item_schema'])
@arolla.optools.as_backend_operator(
    'kd.schema.get_item_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.list_schema)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def get_item_schema(list_schema):  # pylint: disable=unused-argument,redefined-outer-name
  """Returns the item schema of a List schema`."""
  raise NotImplementedError('implemented in the backend')


# IMPORTANT: Use Arolla boxing to avoid boxing literals into DataSlices.
@optools.add_to_registry(aliases=['kd.get_key_schema'])
@arolla.optools.as_backend_operator(
    'kd.schema.get_key_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.dict_schema)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def get_key_schema(dict_schema):  # pylint: disable=unused-argument,redefined-outer-name
  """Returns the key schema of a Dict schema`."""
  raise NotImplementedError('implemented in the backend')


# IMPORTANT: Use Arolla boxing to avoid boxing literals into DataSlices.
@optools.add_to_registry(aliases=['kd.get_value_schema'])
@arolla.optools.as_backend_operator(
    'kd.schema.get_value_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.dict_schema)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def get_value_schema(dict_schema):  # pylint: disable=unused-argument,redefined-outer-name
  """Returns the value schema of a Dict schema`."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.schema.is_dict_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def is_dict_schema(x):  # pylint: disable=unused-argument
  """Returns true iff `x` is a Dict schema DataItem."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.schema.is_entity_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def is_entity_schema(x):  # pylint: disable=unused-argument
  """Returns true iff `x` is an Entity schema DataItem."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.schema.is_struct_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def is_struct_schema(x):  # pylint: disable=unused-argument
  """Returns true iff `x` is a Struct schema DataItem."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.schema.is_list_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def is_list_schema(x):  # pylint: disable=unused-argument
  """Returns true iff `x` is a List schema DataItem."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.schema.is_primitive_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def is_primitive_schema(x):  # pylint: disable=unused-argument
  """Returns true iff `x` is a primitive schema DataItem."""
  raise NotImplementedError('implemented in the backend')


@optools.as_backend_operator('kd.schema._agg_common_schema')
def _agg_common_schema(x):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.schema.agg_common_schema',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def agg_common_schema(x, ndim=arolla.unspecified()):
  """Returns the common schema of `x` along the last `ndim` dimensions.

  The "common schema" is defined according to go/koda-type-promotion.

  Examples:
    kd.agg_common_schema(kd.slice([kd.INT32, None, kd.FLOAT32]))
      # -> kd.FLOAT32

    kd.agg_common_schema(kd.slice([[kd.INT32, None], [kd.FLOAT32, kd.FLOAT64]]))
      # -> kd.slice([kd.INT32, kd.FLOAT64])

    kd.agg_common_schema(
        kd.slice([[kd.INT32, None], [kd.FLOAT32, kd.FLOAT64]]), ndim=2)
      # -> kd.FLOAT64

  Args:
    x: DataSlice of schemas.
    ndim: The number of last dimensions to aggregate over.
  """
  return _agg_common_schema(jagged_shape_ops.flatten_last_ndim(x, ndim))


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.schema.common_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def common_schema(x):
  """Returns the common schema as a scalar DataItem of `x`.

  The "common schema" is defined according to go/koda-type-promotion.

  Args:
    x: DataSlice of schemas.
  """
  return agg_common_schema(jagged_shape_ops.flatten(x))


@optools.add_to_registry()
@optools.as_backend_operator('kd.schema._unsafe_cast_to')
def _unsafe_cast_to(x, schema):  # pylint: disable=unused-argument
  """Casts x to schema using explicit rules, allowing unvalidated entity casts."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.with_schema_from_obj'])
@optools.as_lambda_operator(
    'kd.schema.with_schema_from_obj',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def with_schema_from_obj(x):
  """Returns `x` with its embedded common schema set as the schema.

  * `x` must have OBJECT schema.
  * All items in `x` must have a common schema.
  * If `x` is empty, the schema is set to NONE.
  * If `x` contains mixed primitives without a common primitive type, the output
    will have OBJECT schema.

  Args:
    x: An OBJECT DataSlice.
  """
  schema = common_schema(get_obj_schema(x))
  schema = schema | schema_constants.NONE
  # Explicit casting is safe since the compatibility is guaranteed by
  # `get_obj_schema` (returning the schema of the data) and `common_schema`
  # (returning a safe common alternative).
  #
  # NOTE: kd.cast_to can be used, but this performs an additional check of the
  # __schema__ attributes, causing this operator to become ~43% slower.
  return _unsafe_cast_to(x, schema)


@optools.add_to_registry(aliases=['kd.nofollow_schema'])
@optools.as_backend_operator(
    'kd.schema.nofollow_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.schema)],
)
def nofollow_schema(schema):  # pylint: disable=unused-argument
  """Returns a NoFollow schema of the provided schema.

  `nofollow_schema` is reversible with `get_actual_schema`.

  `nofollow_schema` can only be called on implicit and explicit schemas and
  OBJECT. It raises an Error if called on primitive schemas, ITEMID, etc.

  Args:
    schema: Schema DataSlice to wrap.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.get_nofollowed_schema'])
@optools.as_backend_operator(
    'kd.schema.get_nofollowed_schema',
    qtype_constraints=[qtype_utils.expect_data_slice(P.schema)],
)
def get_nofollowed_schema(schema):  # pylint: disable=unused-argument
  """Returns the original schema from nofollow schema.

  Requires `nofollow_schema` to be a nofollow schema, i.e. that it wraps some
  other schema.

  Args:
    schema: nofollow schema DataSlice.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.schema.get_repr',
    qtype_constraints=[qtype_utils.expect_data_slice(P.schema)],
)
def get_repr(schema):  # pylint: disable=unused-argument
  """Returns a string representation of the schema.

  Named schemas are only represented by their name. Other schemas are
  represented by their content.

  Args:
    schema: A scalar schema DataSlice.
  Returns:
    A scalar string DataSlice. A repr of the given schema.
  """
  raise NotImplementedError('implemented in the backend')
