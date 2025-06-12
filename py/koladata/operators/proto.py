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

"""Protocol buffer DataSlice operators."""

from arolla import arolla
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import view_overloads as _
from koladata.types import data_slice

P = arolla.P


@optools.as_backend_operator(
    'kd.proto._from_proto_bytes',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.proto_path),
        qtype_utils.expect_data_slice(P.extensions),
        qtype_utils.expect_data_slice(P.itemids),
        qtype_utils.expect_data_slice(P.schema),
        qtype_utils.expect_data_slice(P.on_invalid),
    ],
    deterministic=False,
)
def _from_proto_bytes(
    x,  # pylint: disable=unused-argument
    proto_path,  # pylint: disable=unused-argument
    /,
    *,
    extensions,  # pylint: disable=unused-argument
    itemids,  # pylint: disable=unused-argument
    schema,  # pylint: disable=unused-argument
    on_invalid,  # pylint: disable=unused-argument
):
  """Implementation of kd.proto.from_proto_bytes."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.from_proto_bytes'])
@optools.as_lambda_operator(
    'kd.proto.from_proto_bytes',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.proto_path),
        qtype_utils.expect_data_slice_or_unspecified(P.extensions),
        qtype_utils.expect_data_slice_or_unspecified(P.itemids),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_or_unspecified(P.on_invalid),
    ],
)
def from_proto_bytes(
    x,  # pylint: disable=unused-argument
    proto_path,  # pylint: disable=unused-argument
    /,
    *,
    extensions=arolla.unspecified(),  # pylint: disable=unused-argument
    itemids=arolla.unspecified(),  # pylint: disable=unused-argument
    schema=arolla.unspecified(),  # pylint: disable=unused-argument
    on_invalid=arolla.unspecified(),  # pylint: disable=unused-argument
):
  """Parses a DataSlice `x` of binary proto messages.

  This is equivalent to parsing `x.to_py()` as a binary proto message in Python,
  and then converting the parsed message to a DataSlice using `kd.from_proto`,
  but bypasses Python, is traceable, and supports any shape and sparsity, and
  can handle parse errors.

  `x` must be a DataSlice of BYTES. Missing elements of `x` will be missing in
  the result.

  `proto_path` must be a DataItem containing a STRING fully-qualified proto
  message name, which will be used to look up the message descriptor in the C++
  generated descriptor pool. For this to work, the C++ proto message needs to
  be compiled into the binary that executes this operator, which is not the
  same as the proto message being available in Python.

  See kd.from_proto for a detailed explanation of the `extensions`, `itemids`,
  and `schema` arguments.

  If `on_invalid` is unset, this operator will throw an error if any input
  fails to parse. If `on_invalid` is set, it must be broadcastable to `x`, and
  will be used in place of the result wherever the input fails to parse.

  Args:
    x: DataSlice of BYTES
    proto_path: DataItem containing STRING
    extensions: 1D DataSlice of STRING
    itemids: DataSlice of ITEMID with the same shape as `x` (optional)
    schema: DataItem containing SCHEMA (optional)
    on_invalid: DataSlice broacastable to the result (optional)

  Returns:
    A DataSlice representing the proto data.
  """
  extensions = arolla.M.core.default_if_unspecified(
      extensions, data_slice.unspecified()
  )
  itemids = arolla.M.core.default_if_unspecified(
      itemids, data_slice.unspecified()
  )
  schema = arolla.M.core.default_if_unspecified(
      schema, data_slice.unspecified()
  )
  on_invalid = arolla.M.core.default_if_unspecified(
      on_invalid, data_slice.unspecified()
  )
  return _from_proto_bytes(
      x,
      proto_path,
      extensions=extensions,
      itemids=itemids,
      schema=schema,
      on_invalid=on_invalid,
  )


@optools.as_backend_operator(
    'kd.proto._from_proto_json',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.proto_path),
        qtype_utils.expect_data_slice(P.extensions),
        qtype_utils.expect_data_slice(P.itemids),
        qtype_utils.expect_data_slice(P.schema),
        qtype_utils.expect_data_slice(P.on_invalid),
    ],
    deterministic=False,
)
def _from_proto_json(
    x,  # pylint: disable=unused-argument
    proto_path,  # pylint: disable=unused-argument
    /,
    *,
    extensions,  # pylint: disable=unused-argument
    itemids,  # pylint: disable=unused-argument
    schema,  # pylint: disable=unused-argument
    on_invalid,  # pylint: disable=unused-argument
):
  """Implementation of kd.proto.from_proto_json."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.from_proto_json'])
@optools.as_lambda_operator(
    'kd.proto.from_proto_json',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.proto_path),
        qtype_utils.expect_data_slice_or_unspecified(P.extensions),
        qtype_utils.expect_data_slice_or_unspecified(P.itemids),
        qtype_utils.expect_data_slice_or_unspecified(P.schema),
        qtype_utils.expect_data_slice_or_unspecified(P.on_invalid),
    ],
)
def from_proto_json(
    x,  # pylint: disable=unused-argument
    proto_path,  # pylint: disable=unused-argument
    /,
    *,
    extensions=arolla.unspecified(),  # pylint: disable=unused-argument
    itemids=arolla.unspecified(),  # pylint: disable=unused-argument
    schema=arolla.unspecified(),  # pylint: disable=unused-argument
    on_invalid=arolla.unspecified(),  # pylint: disable=unused-argument
):
  """Parses a DataSlice `x` of proto JSON-format strings.

  This is equivalent to parsing `x.to_py()` as a JSON-format proto message in
  Python, and then converting the parsed message to a DataSlice using
  `kd.from_proto`, but bypasses Python, is traceable, supports any shape and
  sparsity, and can handle parse errors.

  `x` must be a DataSlice of STRING. Missing elements of `x` will be missing in
  the result.

  `proto_path` must be a DataItem containing a STRING fully-qualified proto
  message name, which will be used to look up the message descriptor in the C++
  generated descriptor pool. For this to work, the C++ proto message needs to
  be compiled into the binary that executes this operator, which is not the
  same as the proto message being available in Python.

  See kd.from_proto for a detailed explanation of the `extensions`, `itemids`,
  and `schema` arguments.

  If `on_invalid` is unset, this operator will throw an error if any input
  fails to parse. If `on_invalid` is set, it must be broadcastable to `x`, and
  will be used in place of the result wherever the input fails to parse.

  Args:
    x: DataSlice of STRING
    proto_path: DataItem containing STRING
    extensions: 1D DataSlice of STRING
    itemids: DataSlice of ITEMID with the same shape as `x` (optional)
    schema: DataItem containing SCHEMA (optional)
    on_invalid: DataSlice broacastable to the result (optional)

  Returns:
    A DataSlice representing the proto data.
  """
  extensions = arolla.M.core.default_if_unspecified(
      extensions, data_slice.unspecified()
  )
  itemids = arolla.M.core.default_if_unspecified(
      itemids, data_slice.unspecified()
  )
  schema = arolla.M.core.default_if_unspecified(
      schema, data_slice.unspecified()
  )
  on_invalid = arolla.M.core.default_if_unspecified(
      on_invalid, data_slice.unspecified()
  )
  return _from_proto_json(
      x,
      proto_path,
      extensions=extensions,
      itemids=itemids,
      schema=schema,
      on_invalid=on_invalid,
  )


@optools.add_to_registry(aliases=['kd.schema_from_proto_path'])
@optools.as_backend_operator(
    'kd.proto.schema_from_proto_path',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.proto_path),
        qtype_utils.expect_data_slice(P.extensions),
    ],
)
def schema_from_proto_path(
    proto_path,  # pylint: disable=unused-argument
    /,
    *,
    extensions=data_slice.unspecified(),  # pylint: disable=unused-argument
):
  """Returns a Koda schema representing a proto message class.

  This is equivalent to `kd.schema_from_proto(message_cls)` if `message_cls` is
  the Python proto class with full name `proto_path`, but bypasses Python and
  is traceable.

  `proto_path` must be a DataItem containing a STRING fully-qualified proto
  message name, which will be used to look up the message descriptor in the C++
  generated descriptor pool. For this to work, the C++ proto message needs to
  be compiled into the binary that executes this operator, which is not the
  same as the proto message being available in Python.

  See `kd.schema_from_proto` for a detailed explanation of the `extensions`
  argument.

  Args:
    proto_path: DataItem containing STRING
    extensions: 1D DataSlice of STRING
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.to_proto_bytes'])
@optools.as_backend_operator(
    'kd.proto.to_proto_bytes',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.proto_path),
    ],
)
def to_proto_bytes(
    x,  # pylint: disable=unused-argument
    proto_path,  # pylint: disable=unused-argument
    /,
):
  """Serializes a DataSlice `x` as binary proto messages.

  This is equivalent to using `kd.to_proto` to serialize `x` as a proto message
  in Python, then serializing that message into a binary proto, but bypasses
  Python, is traceable, and supports any shape and sparsity.

  `x` must be serializable as the proto message with full name `proto_path`.
  Missing elements of `x` will be missing in the result.

  `proto_path` must be a DataItem containing a STRING fully-qualified proto
  message name, which will be used to look up the message descriptor in the C++
  generated descriptor pool. For this to work, the C++ proto message needs to
  be compiled into the binary that executes this operator, which is not the
  same as the proto message being available in Python.

  Args:
    x: DataSlice
    proto_path: DataItem containing STRING

  Returns:
    A DataSlice of BYTES with the same shape and sparsity as `x`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.to_proto_json'])
@optools.as_backend_operator(
    'kd.proto.to_proto_json',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.proto_path),
    ],
)
def to_proto_json(
    x,  # pylint: disable=unused-argument
    proto_path,  # pylint: disable=unused-argument
    /,
):
  """Serializes a DataSlice `x` as JSON-format proto messages.

  This is equivalent to using `kd.to_proto` to serialize `x` as a proto message
  in Python, then serializing that message into a JSON-format proto, but
  bypasses Python, is traceable, and supports any shape and sparsity.

  `x` must be serializable as the proto message with full name `proto_path`.
  Missing elements of `x` will be missing in the result.

  `proto_path` must be a DataItem containing a STRING fully-qualified proto
  message name, which will be used to look up the message descriptor in the C++
  generated descriptor pool. For this to work, the C++ proto message needs to
  be compiled into the binary that executes this operator, which is not the
  same as the proto message being available in Python.

  Args:
    x: DataSlice
    proto_path: DataItem containing STRING

  Returns:
    A DataSlice of STRING with the same shape and sparsity as `x`.
  """
  raise NotImplementedError('implemented in the backend')
