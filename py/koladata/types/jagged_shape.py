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

from typing import Any

from arolla.jagged_shape import jagged_shape
from koladata.expr import py_expr_eval_py_ext

# Jagged shape alias for use in Kola.
JaggedShape = jagged_shape.JaggedDenseArrayShape


def create_shape(*dimensions: Any) -> JaggedShape:
  """Returns a JaggedShape from sizes or edges."""
  return py_expr_eval_py_ext.eval_op('kde.shapes.new', *dimensions)
