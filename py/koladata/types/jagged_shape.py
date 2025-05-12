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

"""JaggedShape type."""

from __future__ import annotations

from typing import Any

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.expr import py_expr_eval_py_ext
from koladata.types import py_misc_py_ext as _py_misc_py_ext

# Jagged shape alias for use in Kola.
# TODO: Replace this with KodaJaggedShape, keeping the JaggedShape
# name.
JaggedShape = jagged_shape.JaggedDenseArrayShape


def create_shape(*dimensions: Any) -> JaggedShape:
  """Returns a JaggedShape from sizes or edges."""
  return py_expr_eval_py_ext.eval_op('kd.shapes.new', *dimensions)

KODA_JAGGED_SHAPE = _py_misc_py_ext.get_jagged_shape_qtype()
# TODO: Replace this with KODA_JAGGED_SHAPE, keeping the
# JAGGED_SHAPE name.
JAGGED_SHAPE = jagged_shape.JAGGED_DENSE_ARRAY_SHAPE


class KodaJaggedShape(
    arolla.abc.QValue,
    jagged_shape.JaggedShapeInterface[arolla.types.DenseArrayEdge],
):
  """QValue specialization for JAGGED_SHAPE qtypes."""

  @classmethod
  def from_edges(cls, *edges: arolla.types.DenseArrayEdge) -> KodaJaggedShape:
    return arolla.eval(
        arolla.M.derived_qtype.downcast(
            KODA_JAGGED_SHAPE,
            arolla.abc.invoke_op('jagged.dense_array_shape_from_edges', edges)
        )
    )

  def edges(self) -> list[arolla.types.DenseArrayEdge]:
    raise NotImplementedError

  def rank(self) -> int:
    raise NotImplementedError

  def __getitem__(
      self, value: Any
  ) -> arolla.types.DenseArrayEdge | KodaJaggedShape:
    raise NotImplementedError

  def __eq__(self, other: Any) -> bool:
    raise NotImplementedError

  def __ne__(self, other: Any) -> bool:
    raise NotImplementedError


arolla.abc.register_qvalue_specialization(KODA_JAGGED_SHAPE, KodaJaggedShape)
