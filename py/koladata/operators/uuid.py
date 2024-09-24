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

"""UUID operators."""

from arolla import arolla
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes


M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@optools.add_to_registry(aliases=['kde._uuid'])
@optools.as_backend_operator(
    'kde.core._uuid', qtype_inference_expr=qtypes.DATA_SLICE
)
def _uuid(seed, kwargs):  # pylint: disable=unused-argument
  """Expr proxy for backend operator."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.uuid'])
@optools.as_lambda_operator(
    'kde.core.uuid',
    aux_policy=py_boxing.KWARGS_POLICY,
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        (
            M.qtype.is_namedtuple_qtype(P.kwargs),
            f'expected named tuple, got {constraints.name_type_msg(P.kwargs)}',
        ),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def uuid(seed=data_slice.DataSlice.from_vals(''), kwargs=arolla.namedtuple()):
  """Creates a DataSlice whose items are Fingerprints identifying arguments.

  Args:
    seed: text seed for the uuid computation.
    kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
      values must be alignable.

  Returns:
    DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
    item from each kwarg value.
  """
  return _uuid(seed, kwargs)
