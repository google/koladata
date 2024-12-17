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

"""Object factory operators."""

from arolla import arolla
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import schema as schema_ops
from koladata.types import qtypes

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@optools.add_to_registry(aliases=['kde.uuobj'])
@optools.as_unified_backend_operator(
    'kde.core.uuobj',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
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


@optools.as_backend_operator(
    'kde.core._uu', qtype_inference_expr=qtypes.DATA_SLICE
)
def _uu(seed, schema, update_schema, kwargs):
  """Internal implementation of kde.uu."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.uu'])
@optools.as_lambda_operator(
    'kde.core.uu',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice(P.update_schema),
        (
            M.qtype.is_namedtuple_qtype(P.kwargs),
            f'expected named tuple, got {constraints.name_type_msg(P.kwargs)}',
        ),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def uu(seed='', *, schema=arolla.unspecified(), update_schema=False, **kwargs):
  """Creates Entities whose ids are uuid(s) with the provided attributes.

  In order to create a different id from the same arguments, use
  `seed` argument with the desired value.

  Args:
    seed: text seed for the uuid computation.
    schema: shared schema of created entities. If not specified, a uu_schema
      based on the schemas of the passed **kwargs will be created. Can also be
      specified as a string, which is a shortcut for kd.named_schema(name).
    update_schema: if True, overwrite the provided schema with the schema
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
      update_schema=update_schema,
      kwargs=kwargs,
  )
