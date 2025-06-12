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

"""Eval utility that handles Arolla expressions with Koda primitives."""

from typing import Any

from arolla import arolla
from koladata.expr import input_container
from koladata.expr import py_expr_eval_py_ext as _py_expr_eval_py_ext
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import py_boxing

I = input_container.InputContainer('I')
UNSPECIFIED_SELF_INPUT = _py_expr_eval_py_ext.unspecified_self_input()


def freeze_data_slice_or_databag(x: Any) -> Any:
  """Freezes object if it is a DataSlice or DataBag."""
  if isinstance(x, data_slice.DataSlice):
    return x.freeze_bag()
  elif isinstance(x, data_bag.DataBag):
    return x.freeze()
  else:
    return x


def eval_(
    expr: Any,
    self_input: Any = UNSPECIFIED_SELF_INPUT,
    /,
    **input_values: Any,
) -> arolla.abc.AnyQValue:
  """Returns the expr evaluated on the given `input_values`.

  Only Koda Inputs from container `I` (e.g. `I.x`) can be evaluated. Other
  input types must be substituted before calling this function.

  Args:
    expr: Koda expression with inputs from container `I`.
    self_input: The value for I.self input. When not provided, it will still
      have a default value that can be passed to a subroutine.
    **input_values: Values to evaluate `expr` with. Note that all inputs in
      `expr` must be present in the input values. All input values should either
      be DataSlices or convertible to DataSlices.
  """
  data_slice_values = {
      k: py_boxing.as_qvalue(freeze_data_slice_or_databag(v))
      for k, v in input_values.items()
  }
  if 'self' in data_slice_values:
    raise ValueError(
        'I.self must be passed as a positional argument to kd.eval()'
    )
  data_slice_values['self'] = py_boxing.as_qvalue(self_input)
  return _py_expr_eval_py_ext.eval_expr(
      py_boxing.as_expr(expr), **data_slice_values
  )


eval = eval_  # pylint: disable=redefined-builtin


# Subscribe eval caches for arolla cleanups.
arolla.abc.cache_clear_callbacks.add(_py_expr_eval_py_ext.clear_eval_cache)
arolla.abc.cache_clear_callbacks.add(_py_expr_eval_py_ext.clear_arolla_op_cache)
