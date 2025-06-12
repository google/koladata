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

"""Operators working with objects."""

from arolla import arolla
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import optools
from koladata.operators import qtype_utils

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@optools.add_to_registry(aliases=['kd.obj'])
@optools.as_backend_operator(
    'kd.objs.new',
    qtype_constraints=[
        qtype_utils.expect_data_slice_or_unspecified(P.arg),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
        qtype_utils.expect_data_slice_kwargs(P.attrs),
    ],
    deterministic=False,
)
def obj(arg=arolla.unspecified(), /, *, itemid=arolla.unspecified(), **attrs):  # pylint: disable=unused-argument
  """Creates new Objects with an implicit stored schema.

  Returned DataSlice has OBJECT schema.

  Args:
    arg: optional Python object to be converted to an Object.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
      ItemIds will only be set when the args is not a primitive or primitive
      DataSlice.
    **attrs: attrs to set on the returned object.

  Returns:
    data_slice.DataSlice with the given attrs and kd.OBJECT schema.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.obj_shaped'])
@optools.as_backend_operator(
    'kd.objs.shaped',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.shape),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
        qtype_utils.expect_data_slice_kwargs(P.attrs),
    ],
    deterministic=False,
)
def obj_shaped(shape, /, *, itemid=arolla.unspecified(), **attrs):  # pylint: disable=unused-argument
  """Creates Objects with the given shape.

  Returned DataSlice has OBJECT schema.

  Args:
    shape: JaggedShape that the returned DataSlice will have.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.obj_shaped_as'])
@optools.as_lambda_operator(
    'kd.objs.shaped_as',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_from),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
        qtype_utils.expect_data_slice_kwargs(P.attrs),
    ],
)
def obj_shaped_as(shape_from, /, *, itemid=arolla.unspecified(), **attrs):
  """Creates Objects with the shape of the given DataSlice.

  Returned DataSlice has OBJECT schema.

  Args:
    shape_from: DataSlice, whose shape the returned DataSlice will have.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    **attrs: attrs to set in the returned Entity.

  Returns:
    data_slice.DataSlice with the given attrs.
  """
  attrs = arolla.optools.fix_trace_kwargs(attrs)
  return arolla.abc.bind_op(
      obj_shaped,
      shape=jagged_shape_ops.get_shape(shape_from),
      itemid=itemid,
      attrs=attrs,
      **optools.unified_non_deterministic_kwarg(),
  )


@optools.add_to_registry(aliases=['kd.obj_like'])
@optools.as_backend_operator(
    'kd.objs.like',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.shape_and_mask_from),
        qtype_utils.expect_data_slice_or_unspecified(P.itemid),
        qtype_utils.expect_data_slice_kwargs(P.attrs),
    ],
    deterministic=False,
)
def obj_like(shape_and_mask_from, /, *, itemid=arolla.unspecified(), **attrs):  # pylint: disable=unused-argument
  """Returns a new DataSlice with object schema and the shape and mask of given DataSlice.

  Please note the difference to obj_shaped_as:

  x = kd.obj_like(ds([None, None]), a=42)
  kd.has(x) # => ds([None, None], schema_constants.MASK)
  x.a # => ds([None, None], schema_constants.OBJECT)

  Args:
    shape_and_mask_from: DataSlice to copy the shape and mask from.
    itemid: optional ITEMID DataSlice used as ItemIds of the resulting obj(s).
    **attrs: attrs to set on the returned object.

  Returns:
    data_slice.DataSlice with the given attrs and kd.OBJECT schema.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.uuobj'])
@optools.as_backend_operator(
    'kd.objs.uu',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def uuobj(seed='', **kwargs):  # pylint: disable=unused-argument
  """Creates Object(s) whose ids are uuid(s) with the provided attributes.

  In order to create a different id from the same arguments, use
  `seed` argument with the desired value, e.g.

  kd.uuobj(seed='type_1', x=[1, 2, 3], y=[4, 5, 6])

  and

  kd.uuobj(seed='type_2', x=[1, 2, 3], y=[4, 5, 6])

  have different ids.

  Args:
    seed: text seed for the uuid computation.
    **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
      values must be alignable.

  Returns:
    (DataSlice) of uuids. The provided attributes are also set in a newly
    created databag. The shape of this DataSlice is the result of aligning the
    shapes of the kwarg DataSlices.
  """
  raise NotImplementedError('implemented in the backend')
