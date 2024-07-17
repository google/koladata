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

"""Eval utility that handles Arolla expressions with Koda primitives."""

from typing import Any

from arolla import arolla
from koladata.expr import input_container
from koladata.expr import py_expr_eval_py_ext as _py_expr_eval_py_ext
from koladata.types import py_boxing

I = input_container.InputContainer('I')


def eval_(expr: Any, /, **input_values: Any) -> arolla.abc.AnyQValue:
  """Returns the expr evaluated on the given `input_values`.

  Only Koda Inputs from container `I` (e.g. `I.x`) can be evaluated. Other
  input types must be substituted before calling this function.

  Args:
    expr: Koda expression with inputs from container `I`.
    **input_values: Values to evaluate `expr` with. Note that all inputs in
      `expr` must be present in the input values. All input values should either
      be DataSlices or convertible to DataSlices.
  """
  data_slice_values = {
      k: py_boxing.as_qvalue(v) for k, v in input_values.items()
  }
  return _py_expr_eval_py_ext.eval_expr(
      py_boxing.as_expr(expr), **data_slice_values
  )


eval = eval_  # pylint: disable=redefined-builtin


# Subscribe eval caches for arolla cleanups.
arolla.abc.cache_clear_callbacks.add(_py_expr_eval_py_ext.clear_eval_cache)