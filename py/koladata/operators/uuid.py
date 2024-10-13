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
from koladata.operators import op_repr
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import py_boxing
from koladata.types import qtypes


M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@optools.add_to_registry(
    aliases=['kde.uuid'], repr_fn=op_repr.full_signature_repr
)
@optools.as_backend_operator(
    'kde.core.uuid',
    aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def uuid(seed=py_boxing.keyword_only(''), kwargs=py_boxing.var_keyword()):  # pylint: disable=unused-argument
  """Creates a DataSlice whose items are Fingerprints identifying arguments.

  Args:
    seed: text seed for the uuid computation.
    kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
      values must be alignable.

  Returns:
    DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
    item from each kwarg value.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(
    aliases=['kde.uuid_for_list'], repr_fn=op_repr.full_signature_repr
)
@optools.as_backend_operator(
    'kde.core.uuid_for_list',
    aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def uuid_for_list(
    seed=py_boxing.keyword_only(''), kwargs=py_boxing.var_keyword()
):  # pylint: disable=unused-argument
  """Creates a DataSlice whose items are Fingerprints identifying arguments.

  To be used for keying list items.

  e.g.

  kd.list([1, 2, 3], itemid=kd.uuid_for_list(seed='seed', a=ds(1)))

  Args:
    seed: text seed for the uuid computation.
    kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
      values must be alignable.

  Returns:
    DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
    item from each kwarg value.
  """
  raise NotImplementedError('implemented in the backend')
