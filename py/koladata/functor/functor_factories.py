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

"""Tools to create functors."""

from typing import Any

from arolla import arolla
from koladata.expr import introspection
from koladata.functor import py_functors_py_ext as _py_functors_py_ext
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import py_boxing


# We can move this wrapping inside the CPython layer if needed for performance.
def _maybe_wrap_expr(arg: Any) -> arolla.QValue:
  if isinstance(arg, arolla.Expr):
    arg = introspection.pack_expr(arg)
  return py_boxing.as_qvalue(arg)


def fn(
    returns: Any,
    *,
    signature: data_slice.DataSlice | None = None,
    **variables: Any
) -> data_slice.DataSlice:
  """Creates a functor.

  Args:
    returns: What should calling a functor return. Will typically be an Expr to
      be evaluated, but can also be a DataItem in which case calling will just
      return this DataItem, or a primitive that will be wrapped as a DataItem.
      When this is an Expr, it must evaluate to a DataSlice/DataItem.
    signature: The signature of the functor. Will be used to map from args/
      kwargs passed at calling time to I.smth inputs of the expressions. When
      None, the default signature will be created based on the inputs from the
      expressions involved.
    **variables: The variables of the functor. Each variable can either be an
      expression to be evaluated, or a DataItem, or a primitive that will be
      wrapped as a DataItem. The result of evaluating the variable can be
      accessed as V.smth in other expressions.

  Returns:
    A DataSlice with an item representing the functor.
  """
  return _py_functors_py_ext.create_functor(
      _maybe_wrap_expr(returns),
      signature,
      **{k: _maybe_wrap_expr(v) for k, v in variables.items()}
  )


def is_fn(obj: Any) -> data_slice.DataSlice:
  """Checks if `obj` represents a functor.

  Args:
    obj: The value to check.

  Returns:
    kd.present if `obj` is a DataSlice representing a functor, kd.missing
    otherwise (for example if fn has wrong type).
  """
  if isinstance(obj, data_slice.DataSlice) and _py_functors_py_ext.is_fn(obj):
    return mask_constants.present
  else:
    return mask_constants.missing
