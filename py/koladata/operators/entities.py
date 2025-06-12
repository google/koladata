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

"""Operators working with entities."""

from arolla import arolla
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import schema as schema_ops

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@optools.as_backend_operator('kd.entities._new', deterministic=False)
def _new(arg, schema, overwrite_schema, itemid, attrs):  # pylint: disable=unused-argument
  """Internal implementation of kd.entities.new."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.new'])
@optools.as_lambda_operator(
    'kd.entities.new',
    qtype_constraints=[
        (
            (P.arg == arolla.UNSPECIFIED),
            'kd.new does not support converter use-case. For converting Python '
            + 'objects to Entities, please use eager only kd.eager.new',
        ),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice(P.overwrite_schema),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
        qtype_utils.expect_data_slice_kwargs(P.attrs),
    ],
)
def new(
    arg=arolla.unspecified(),
    /,
    *,
    schema=arolla.unspecified(),
    overwrite_schema=False,
    itemid=arolla.unspecified(),
    **attrs,
):
  """Creates Entities with given attrs.

  First argument `arg` is used for interface consistency with its eager version.
  Reports that eager version should be used for converting Python objects into
  Koda Entities.

  Args:
    arg: should keep the default arolla.unspecified() value.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
    overwrite_schema: if schema attribute is missing and the attribute is being
      set through `attrs`, schema is successfully updated.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting entities.
      itemid will only be set when the args is not a primitive or primitive
      DataSlice if args present.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  attrs = arolla.optools.fix_trace_kwargs(attrs)
  return _new(
      arg=arg,
      schema=schema_ops.internal_maybe_named_schema(schema),
      overwrite_schema=overwrite_schema,
      itemid=itemid,
      attrs=attrs,
  )


@optools.as_backend_operator('kd.entities._shaped', deterministic=False)
def _shaped(shape, schema, overwrite_schema, itemid, attrs):  # pylint: disable=unused-argument
  """Internal implementation of kd.entities.shaped."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.new_shaped'])
@optools.as_lambda_operator(
    'kd.entities.shaped',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.shape),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
        qtype_utils.expect_data_slice(P.overwrite_schema),
        qtype_utils.expect_data_slice_kwargs(P.attrs),
    ],
)
def shaped(
    shape,
    /,
    *,
    schema=arolla.unspecified(),
    overwrite_schema=False,
    itemid=arolla.unspecified(),
    **attrs,
):
  """Creates new Entities with the given shape.

  Args:
    shape: JaggedShape that the returned DataSlice will have.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
    overwrite_schema: if schema attribute is missing and the attribute is being
      set through `attrs`, schema is successfully updated.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting entities.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  attrs = arolla.optools.fix_trace_kwargs(attrs)
  return _shaped(
      shape=shape,
      schema=schema_ops.internal_maybe_named_schema(schema),
      overwrite_schema=overwrite_schema,
      itemid=itemid,
      attrs=attrs,
  )


@optools.add_to_registry(aliases=['kd.new_shaped_as'])
@optools.as_lambda_operator(
    'kd.entities.shaped_as',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_from),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
        qtype_utils.expect_data_slice(P.overwrite_schema),
        qtype_utils.expect_data_slice_kwargs(P.attrs),
    ],
)
def shaped_as(
    shape_from,
    /,
    *,
    schema=arolla.unspecified(),
    overwrite_schema=False,
    itemid=arolla.unspecified(),
    **attrs,
):
  """Creates new Koda entities with shape of the given DataSlice.

  Args:
    shape_from: DataSlice, whose shape the returned DataSlice will have.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
    overwrite_schema: if schema attribute is missing and the attribute is being
      set through `attrs`, schema is successfully updated.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting entities.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  attrs = arolla.optools.fix_trace_kwargs(attrs)
  return _shaped(
      shape=jagged_shape_ops.get_shape(shape_from),
      schema=schema_ops.internal_maybe_named_schema(schema),
      overwrite_schema=overwrite_schema,
      itemid=itemid,
      attrs=attrs,
  )


@optools.as_backend_operator('kd.entities._like', deterministic=False)
def _like(shape_and_mask_from, schema, overwrite_schema, itemid, attrs):  # pylint: disable=unused-argument
  """Internal implementation of kd.entities.like."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.new_like'])
@optools.as_lambda_operator(
    'kd.entities.like',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_and_mask_from),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
        qtype_utils.expect_data_slice(P.overwrite_schema),
        qtype_utils.expect_data_slice_kwargs(P.attrs),
    ],
)
def like(
    shape_and_mask_from,
    /,
    *,
    schema=arolla.unspecified(),
    overwrite_schema=False,
    itemid=arolla.unspecified(),
    **attrs,
):
  """Creates new Entities with the shape and sparsity from shape_and_mask_from.

  Args:
    shape_and_mask_from: DataSlice, whose shape and sparsity the returned
      DataSlice will have.
    schema: optional DataSlice schema. If not specified, a new explicit schema
      will be automatically created based on the schemas of the passed **attrs.
    overwrite_schema: if schema attribute is missing and the attribute is being
      set through `attrs`, schema is successfully updated.
    itemid: Optional ITEMID DataSlice used as ItemIds of the resulting entities.
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  attrs = arolla.optools.fix_trace_kwargs(attrs)
  return _like(
      shape_and_mask_from=shape_and_mask_from,
      schema=schema_ops.internal_maybe_named_schema(schema),
      overwrite_schema=overwrite_schema,
      itemid=itemid,
      attrs=attrs,
  )


@optools.as_backend_operator('kd.entities._uu')
def _uu(seed, schema, overwrite_schema, kwargs):  # pylint: disable=unused-argument
  """Internal implementation of kd.uu."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.uu'])
@optools.as_lambda_operator(
    'kd.entities.uu',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice(P.overwrite_schema),
        (
            M.qtype.is_namedtuple_qtype(P.kwargs),
            f'expected named tuple, got {constraints.name_type_msg(P.kwargs)}',
        ),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def uu(
    seed='', *, schema=arolla.unspecified(), overwrite_schema=False, **kwargs
):
  """Creates Entities whose ids are uuid(s) with the provided attributes.

  In order to create a different id from the same arguments, use
  `seed` argument with the desired value.

  Args:
    seed: text seed for the uuid computation.
    schema: shared schema of created entities. If not specified, a uu_schema
      based on the schemas of the passed **kwargs will be created. Can also be
      specified as a string, which is a shortcut for kd.named_schema(name).
    overwrite_schema: if True, overwrite the provided schema with the schema
      derived from the keyword values in the resulting Databag.
    **kwargs: DataSlice kwargs defining the attributes of the entities. The
      DataSlice values must be alignable.

  Returns:
    (DataSlice) of uuids. The provided attributes are also set in a newly
    created databag. The shape of this DataSlice is the result of aligning the
    shapes of the kwarg DataSlices.
  """
  kwargs = arolla.optools.fix_trace_kwargs(kwargs)
  return _uu(
      seed=seed,
      schema=schema_ops.internal_maybe_named_schema(schema),
      overwrite_schema=overwrite_schema,
      kwargs=kwargs,
  )
