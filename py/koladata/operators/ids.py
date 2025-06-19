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

"""UUID operators."""

from arolla import arolla
from koladata.operators import arolla_bridge
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import schema as schema_ops
from koladata.types import schema_constants


M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints

_AGG_UUID_MISSING_VALUE_REPLACEMENT = '__empty_input_to_uuid__'


@optools.add_to_registry(aliases=['kd.uuid'])
@optools.as_backend_operator(
    'kd.ids.uuid',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
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


@optools.add_to_registry(
    aliases=[
        'kd.uuid_for_list',
        'lists.uuid_for_list',
    ]
)
@optools.as_backend_operator(
    'kd.ids.uuid_for_list',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
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


@optools.add_to_registry(
    aliases=[
        'kd.uuid_for_dict',
        'dicts.uuid_for_dict',
    ]
)
@optools.as_backend_operator(
    'kd.ids.uuid_for_dict',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
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


@optools.add_to_registry(aliases=['kd.uuids_with_allocation_size'])
@optools.as_backend_operator(
    'kd.ids.uuids_with_allocation_size',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.seed),
        qtype_utils.expect_data_slice(P.size),
    ],
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


@optools.as_backend_operator('kd.ids._agg_uuid')
def _agg_uuid(x):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.agg_uuid'])
@optools.as_lambda_operator(
    'kd.ids.agg_uuid',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def agg_uuid(x, ndim=arolla.unspecified()):
  """Computes aggregated uuid of elements over the last `ndim` dimensions.

  Args:
    x: A DataSlice.
    ndim: The number of dimensions to aggregate over. Requires 0 <= ndim <=
      get_ndim(x).

  Returns:
    DataSlice with that has `rank = rank - ndim` and shape: `shape =
    shape[:-ndim]`.
  """
  x = jagged_shape_ops.flatten_last_ndim(x, ndim)
  x = (
      # OBJECT allows us to have mixed data. Only the raw data is used for the
      # _agg_uuid computation, so we're not required to embed Entity schemas.
      schema_ops.with_schema(x, schema_constants.OBJECT)
      | _AGG_UUID_MISSING_VALUE_REPLACEMENT
  )
  return _agg_uuid(x)


@optools.as_backend_operator('kd.ids._deep_uuid')
def _deep_uuid(x, schema, seed):  # pylint: disable=unused-argument
  """Creates a slice with a (deep) uuid of the given slice."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.deep_uuid'])
@optools.as_lambda_operator(
    'kd.ids.deep_uuid',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice(P.seed),
    ],
)
def deep_uuid(x, /, schema=arolla.unspecified(), *, seed=''):
  """Recursively computes uuid for x.

  Args:
    x: The slice to take uuid on.
    schema: The schema to use to resolve '*' and '**' tokens. If not specified,
      will use the schema of the 'x' DataSlice.
    seed: The seed to use for uuid computation.

  Returns:
    Result of recursive uuid application `x`.
  """
  schema = M.core.default_if_unspecified(schema, schema_ops.get_schema(x))
  return _deep_uuid(x, schema, seed)


@optools.add_to_registry(aliases=['kd.encode_itemid'])
@optools.as_backend_operator(
    'kd.ids.encode_itemid',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
    ],
)
def encode_itemid(ds):  # pylint: disable=unused-argument
  """Returns the base62 encoded ItemIds in `ds` as strings."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.decode_itemid'])
@optools.as_backend_operator(
    'kd.ids.decode_itemid',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
    ],
)
def decode_itemid(ds):  # pylint: disable=unused-argument
  """Returns ItemIds decoded from the base62 strings."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.hash_itemid'])
@optools.as_lambda_operator(
    'kd.ids.hash_itemid',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def hash_itemid(x):
  """Returns a INT64 DataSlice of hash values of `x`.

  The hash values are in the range of [0, 2**63-1].

  The hash algorithm is subject to change. It is not guaranteed to be stable in
  future releases.

  Args:
    x: DataSlice of ItemIds.

  Returns:
    A DataSlice of INT64 hash values.
  """
  hash_value = M.random.cityhash(
      arolla_bridge.to_arolla_dense_array_text(encode_itemid(x)),
      arolla.int64(85852539),
  )
  return arolla_bridge.to_data_slice(hash_value).reshape(
      jagged_shape_ops.get_shape(x)
  )
