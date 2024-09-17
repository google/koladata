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

"""UUObj operators."""

from arolla import arolla
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import data_slice
from koladata.types import qtypes

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


# TODO: Make this operator public again once non-determinism is
# designed and implemented.
@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.core._uuobj',
    aux_policy='koladata_obj_kwargs',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        (
            M.qtype.is_namedtuple_qtype(P.kwargs),
            f'expected named tuple, got {constraints.name_type_msg(P.kwargs)}',
        ),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE
)
def _uuobj(seed=data_slice.DataSlice.from_vals(''), kwargs=arolla.namedtuple()):  # pylint: disable=unused-argument
  """Creates Object(s) whose ids are uuid(s) with the provided attributes.

  In order to create a different id from the same arguments, use
  `seed` argument with the desired value, e.g.

  kd.uuobj(seed='type_1', x=[1, 2, 3], y=[4, 5, 6])

  and

  kd.uuobj(seed='type_2', x=[1, 2, 3], y=[4, 5, 6])

  have different ids.

  Args:
    seed: text seed for the uuid computation.
    kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
      values must be alignable.

  Returns:
    (DataSlice) of uuids. The provided attributes are also set in a newly
    created databag.
  """
  raise NotImplementedError('implemented in the backend')
