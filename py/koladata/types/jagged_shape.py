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

"""JaggedShape type."""

from __future__ import annotations

from typing import Any

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.expr import py_expr_eval_py_ext
from koladata.types import data_slice_py_ext as _data_slice_py_ext
from koladata.types import py_misc_py_ext as _py_misc_py_ext


DataSlice = _data_slice_py_ext.DataSlice


def create_shape(*dimensions: Any) -> JaggedShape:
  """Returns a JaggedShape from sizes or edges."""
  return py_expr_eval_py_ext.eval_op('kd.shapes.new', *dimensions)

JAGGED_SHAPE = _py_misc_py_ext.get_jagged_shape_qtype()


class JaggedShape(
    arolla.abc.QValue,
    jagged_shape.JaggedShapeInterface[arolla.types.DenseArrayEdge],
):
  """QValue specialization for JAGGED_SHAPE qtypes."""

  @classmethod
  def from_edges(cls, *edges: arolla.types.DenseArrayEdge) -> JaggedShape:
    dense_array_shape = arolla.abc.invoke_op(
        'jagged.dense_array_shape_from_edges', edges
    )
    return arolla.abc.invoke_op(
        'koda_internal.from_arolla_jagged_shape', (dense_array_shape,)
    )

  def edges(self) -> list[arolla.types.DenseArrayEdge]:
    dense_array_shape = arolla.abc.invoke_op(
        'koda_internal.to_arolla_jagged_shape', (self,)
    )
    return dense_array_shape.edges()

  def rank(self) -> int:
    dense_array_shape = arolla.abc.invoke_op(
        'koda_internal.to_arolla_jagged_shape', (self,)
    )
    return dense_array_shape.rank()

  def get_sizes(self) -> DataSlice:
    return py_expr_eval_py_ext.eval_op('kd.shapes.get_sizes', self)

  def __getitem__(
      self, value: Any
  ) -> arolla.types.DenseArrayEdge | JaggedShape:
    dense_array_shape = arolla.abc.invoke_op(
        'koda_internal.to_arolla_jagged_shape', (self,)
    )
    result = dense_array_shape[value]
    if isinstance(result, jagged_shape.JaggedDenseArrayShape):
      return arolla.abc.invoke_op(
          'koda_internal.from_arolla_jagged_shape', (result,)
      )
    else:
      return result

  def __eq__(self, other: Any) -> bool:
    if not isinstance(other, type(self)):
      raise NotImplementedError
    return arolla.abc.invoke_op(
        'koda_internal.to_arolla_jagged_shape', (self,)
    ) == arolla.abc.invoke_op('koda_internal.to_arolla_jagged_shape', (other,))

  def __ne__(self, other: Any) -> bool:
    if not isinstance(other, type(self)):
      raise NotImplementedError
    return arolla.abc.invoke_op(
        'koda_internal.to_arolla_jagged_shape', (self,)
    ) != arolla.abc.invoke_op('koda_internal.to_arolla_jagged_shape', (other,))


arolla.abc.register_qvalue_specialization(JAGGED_SHAPE, JaggedShape)
