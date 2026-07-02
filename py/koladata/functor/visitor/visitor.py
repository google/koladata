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

"""Visitor for non-recursive Koda Functor."""

from typing import Any, Callable

from koladata.functor import functor_factories
from koladata.functor import py_functors_py_ext
from koladata.types import data_item


class _FunctorVisitor:
  """Traverses non-recursive Koda functors bottom-up (subfunctors first).

  Visits the functor hierarchy only — it does not traverse variables or
  expressions within a functor.  For each functor it calls `callback_fn` with
  the functor and a dict mapping attribute names of subfunctor variables to
  their already-visited results.
  """

  def __init__(
      self,
      callback_fn: Callable[[data_item.DataItem, dict[str, Any]], Any],
  ):
    self._user_callback_fn = callback_fn
    self._visited_functors: dict[data_item.DataItem, Any] = {}
    self._on_stack: set[data_item.DataItem] = set()

  def visit(self, functor: data_item.DataItem) -> Any:
    if not functor_factories.is_fn(functor):
      raise ValueError(f'{functor} is not a functor')
    return self._visit_functor(functor)

  def _visit_functor(self, functor: data_item.DataItem) -> Any:
    """Visits a single functor, assuming it is not already visited."""
    itemid = functor.get_itemid()
    if itemid in self._on_stack:
      raise ValueError(f'{functor} is recursive')
    if itemid in self._visited_functors:
      return self._visited_functors[itemid]

    self._on_stack.add(itemid)
    subfunctors: dict[str, Any] = {}
    for var_name in py_functors_py_ext.get_variable_evaluation_order(functor):
      var = functor.get_attr(var_name)
      if functor_factories.is_fn(var):
        subfunctors[var_name] = self._visit_functor(var)  # pyrefly: ignore[bad-argument-type]
    self._on_stack.remove(itemid)

    result = self._user_callback_fn(functor, subfunctors)
    self._visited_functors[itemid] = result
    return result


def visit_functors(
    functor: data_item.DataItem,
    callback_fn: Callable[[data_item.DataItem, dict[str, Any]], Any],
) -> data_item.DataItem:
  """Recursively visits functors in the functor graph, bottom-up.

  Traverses the graph of nested functors.  For each functor, `callback_fn`
  is called with:

  * the functor itself, and
  * a dict mapping attribute names that hold subfunctors to their
    already-visited (transformed) results.

  To visit / transform functor variables (without recursing into subfunctors),
  use kd.functor.visit_variables.

  Example:
    fn = kd.fn(kd.V.g(kd.I.x, g=kd.fn(lambda x: x + 1)))

    def transform(fn, subfunctors):
      # subfunctors == {'g': <visited g>}
      return fn.with_attrs(**subfunctors)

    visitor.visit_functors(fn, transform)

  Args:
    functor: Root functor to traverse.
    callback_fn: Called on each functor with its visited subfunctors.

  Returns:
    Visit result.
  """
  return _FunctorVisitor(callback_fn).visit(functor)


def visit_variables(
    functor: data_item.DataItem,
    callback_fn: Callable[[str, data_item.DataItem, dict[str, Any]], Any],
) -> Any:
  """Visits variables of a single functor in topological order.

  This function does **not** recurse into subfunctors.  It iterates over the
  variables of `functor` in evaluation order (topological order) and calls
  `callback_fn` on each variable.

  `callback_fn` receives:

  * the variable name (str),
  * the variable value (DataItem - kd.EXPR which can be accessed via
        kd.expr.unpack_expr, while all other DataItems represent literals
        (even subfunctors)),
  * a dict mapping variable names to the results of all previously visited
        variables.

  Args:
    functor: The functor whose variables to visit.
    callback_fn: Called on each variable.

  Returns:
    A visit result for 'returns' variable.
  """
  if not functor_factories.is_fn(functor):
    raise ValueError(f'{functor} is not a functor')

  vars_: dict[str, Any] = {}
  for var_name in py_functors_py_ext.get_variable_evaluation_order(functor):
    vars_[var_name] = callback_fn(
        var_name, functor.get_attr(var_name), vars_  # pyrefly: ignore[bad-argument-type]
    )

  var_name, var = vars_.popitem()
  # The last variable is always 'returns'.
  assert var_name == 'returns'
  return var
