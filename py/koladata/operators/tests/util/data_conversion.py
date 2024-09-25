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

"""Conversion tools for Koda <-> Arolla data for use in testing.

These conversion tools are likely incomplete and should be expanded when needed
to support the relevant test. See http://shortn/_vqBXE4KOE6 for additional
examples and go/koda-operator-testing for more information.
"""

import typing
from arolla import arolla
# Register kde ops for e.g. jagged_shape.create_shape().
from koladata.operators import kde_operators as _
from koladata.types import data_slice
from koladata.types import jagged_shape

_EDGE_QTYPES = frozenset({
    arolla.types.ARRAY_EDGE,
    arolla.types.ARRAY_TO_SCALAR_EDGE,
    arolla.types.DENSE_ARRAY_EDGE,
    arolla.types.DENSE_ARRAY_TO_SCALAR_EDGE,
    arolla.types.SCALAR_TO_SCALAR_EDGE,
})

_PASSTHROUGH_QTYPES = frozenset({arolla.UNSPECIFIED})


def _get_jagged_shape_from_edge(
    edge: arolla.QValue,
) -> jagged_shape.JaggedShape:
  match edge.qtype:
    case arolla.ARRAY_EDGE | arolla.DENSE_ARRAY_EDGE:
      edge = arolla.abc.invoke_op('edge.as_dense_array_edge', (edge,))
      parent_edge = arolla.types.DenseArrayEdge.from_sizes([edge.parent_size])
      return jagged_shape.JaggedShape.from_edges(parent_edge, edge)
    case arolla.ARRAY_TO_SCALAR_EDGE | arolla.DENSE_ARRAY_TO_SCALAR_EDGE:
      return jagged_shape.create_shape([edge.child_size])  # pytype: disable=attribute-error
    case arolla.types.SCALAR_TO_SCALAR_EDGE:
      return jagged_shape.JaggedShape.from_edges()
    case _:
      assert False, 'unreachable'


def koda_from_arolla(qvalue: arolla.QValue) -> data_slice.DataSlice:
  """Converts arolla data to Koda."""
  qtype = qvalue.qtype
  if qtype in _PASSTHROUGH_QTYPES:
    return qvalue
  if qtype in _EDGE_QTYPES:
    return _get_jagged_shape_from_edge(qvalue)
  if (
      not arolla.types.is_array_qtype(qtype)
      and not arolla.types.is_scalar_qtype(qtype)
      and not arolla.types.is_optional_qtype(qtype)
  ):
    raise ValueError(f'unsupported qtype: {qtype}')
  return data_slice.DataSlice.from_vals(qvalue)


def _safe_cast_to_output_qtype(
    qvalue: arolla.QValue, *, output_qtype: arolla.QType
) -> arolla.QValue:
  """Attempts to safely cast `qvalue` to the `output_qtype`."""
  if output_qtype == qvalue.qtype:
    return qvalue
  if arolla.types.is_dense_array_qtype(output_qtype):
    converted_qvalue = arolla.abc.invoke_op('array.as_dense_array', (qvalue,))
  elif arolla.types.is_array_qtype(output_qtype):
    converted_qvalue = arolla.abc.invoke_op('array.as_array', (qvalue,))
  elif arolla.types.is_scalar_qtype(output_qtype):
    converted_qvalue = arolla.abc.invoke_op(
        'core.get_optional_value', (qvalue,)
    )
  elif arolla.types.is_optional_qtype(output_qtype):
    converted_qvalue = arolla.abc.invoke_op('core.to_optional', (qvalue,))
  else:
    raise ValueError(f'unsupported output_qtype: {output_qtype}')
  if converted_qvalue.qtype != output_qtype:
    raise ValueError(
        f'could not safely cast {converted_qvalue.qtype} to {output_qtype}'
    )
  return converted_qvalue


def arolla_from_koda(
    value: arolla.QValue, *, output_qtype: arolla.QType | None = None
) -> arolla.QValue:
  """Converts Koda to Arolla data."""
  if value.qtype in _PASSTHROUGH_QTYPES:
    return _safe_cast_to_output_qtype(value, output_qtype=value.qtype)
  if isinstance(value, jagged_shape.JaggedShape):
    return _safe_cast_to_output_qtype(value, output_qtype=value.qtype)
  if not isinstance(value, data_slice.DataSlice):
    raise ValueError(f'{value=} is not a DataSlice')
  value = typing.cast(data_slice.DataSlice, value)
  if (rank := value.get_shape().rank()) not in (0, 1):
    raise ValueError(f'{rank=} not in (0, 1)')
  arolla_value = value.as_arolla_value()
  if output_qtype is None:
    output_qtype = arolla_value.qtype
  return _safe_cast_to_output_qtype(arolla_value, output_qtype=output_qtype)
