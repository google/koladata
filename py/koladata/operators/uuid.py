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
from koladata.types import qtypes


M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@optools.add_to_registry(aliases=['kde.uuid'])
@optools.as_unified_backend_operator(
    'kde.core.uuid',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def uuid(seed='', **kwargs):  # pylint: disable=unused-argument
  """Creates a DataSlice whose items are Fingerprints identifying arguments.

  Args:
    seed: text seed for the uuid computation.
    **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
      values must be alignable.

  Returns:
    DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
    item from each kwarg value.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.uuid_for_list'])
@optools.as_unified_backend_operator(
    'kde.core.uuid_for_list',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def uuid_for_list(seed='', **kwargs):  # pylint: disable=unused-argument
  """Creates a DataSlice whose items are Fingerprints identifying arguments.

  To be used for keying list items.

  e.g.

  kd.list([1, 2, 3], itemid=kd.uuid_for_list(seed='seed', a=ds(1)))

  Args:
    seed: text seed for the uuid computation.
    **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
      values must be alignable.

  Returns:
    DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
    item from each kwarg value.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.uuid_for_dict'])
@optools.as_unified_backend_operator(
    'kde.core.uuid_for_dict',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def uuid_for_dict(seed='', **kwargs):  # pylint: disable=unused-argument
  """Creates a DataSlice whose items are Fingerprints identifying arguments.

  To be used for keying dict items.

  e.g.

  kd.dict(['a', 'b'], [1, 2], itemid=kd.uuid_for_dict(seed='seed', a=ds(1)))

  Args:
    seed: text seed for the uuid computation.
    **kwargs: a named tuple mapping attribute names to DataSlices. The DataSlice
      values must be alignable.

  Returns:
    DataSlice of Uuids. The i-th uuid is computed by taking the i-th (aligned)
    item from each kwarg value.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.uuids_with_allocation_size'])
@optools.as_unified_backend_operator(
    'kde.core.uuids_with_allocation_size',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice(P.size),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def uuids_with_allocation_size(seed='', *, size):  # pylint: disable=unused-argument
  """Creates a DataSlice whose items are uuids.

  The uuids are allocated in a single allocation. They are all distinct.
  You can think of the result as a DataSlice created with:
  [fingerprint(seed, size, i) for i in range(size)]

  Args:
    seed: text seed for the uuid computation.
    size: the size of the allocation. It will also be used for the uuid
      computation.

  Returns:
    A 1-dimensional DataSlice with `size` distinct uuids.
  """
  raise NotImplementedError('implemented in the backend')
