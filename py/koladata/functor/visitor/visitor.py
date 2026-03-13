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

from typing import Any, Callable, Generator

from arolla import arolla
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.functor import functor_factories
from koladata.types import data_item
from koladata.types import schema_constants


def _nested_expr_for_hash(expr, functor):
  """Returns an entry to track potential cycles in Expr(s)."""
  return functor.fingerprint, expr.fingerprint


def _get_variables(
    expr: arolla.Expr, functor: data_item.DataItem,
) -> Generator[tuple[str, data_item.DataItem], None, None]:
  """Iterates over variables in Expr stored in `functor`."""
  # NOTE: Using post-order explicitly, because introspection.get_input_names
  # sorts by variable names, which can change the order if names are changed and
  # is not a real post-order, which we rely on here.
  for node in arolla.abc.post_order(expr):
    if introspection.is_variable(node):
      var_name = introspection.get_input_names(
          node, container=input_container.InputContainer('V')
      )[0]
      var = functor.maybe(var_name)
      if not var.is_empty():
        yield var_name, var


# TODO: Make this traverser engine public and consider
# implementing an AbstractVisitor, instead of callback (or called from
# callback_fn) that can dispatch to different methods of the Visitor based on
# the types of items (sub-functors, lists, dicts, entities, etc.).
class _FunctorTraverser:
  """Stateful traverser of non-recursive Koda Functors.

  Traverses non-recursive Koda functors and their variables, calling
  `callback_fn` on each DataItem (in post-order fashion).

  Its main method `traverse` returns a new functor where each variable `v`
  (including the outermost functor) has been replaced by the result of
  `callback_fn` (or original value if `callback_fn` returns `None`).
  """

  def __init__(self, callback_fn: Callable[..., Any]):
    self._user_callback_fn = callback_fn
    self._visited_itemids = set()
    self._on_stack_hashes = set()

  def traverse(self, functor: data_item.DataItem) -> data_item.DataItem:
    """Traverses a top-level functor and invokes the callback bottom-up."""
    if not functor_factories.is_fn(functor):
      raise ValueError(f'{functor} is not a functor')
    return self._visit_functor(functor)

  def _callback_fn(
      self,
      var: data_item.DataItem,
      sub_vars: dict[str, data_item.DataItem],
  ) -> data_item.DataItem:
    new_var = self._user_callback_fn(var, sub_vars)
    if new_var is not None and not isinstance(new_var, data_item.DataItem):
      raise ValueError('`callback_fn` should return either None or DataItem')
    return new_var if new_var is not None else var

  def _visit_functor(self, functor: data_item.DataItem) -> data_item.DataItem:
    """Internal recursive traversal step for a single functor."""
    itemid = functor.get_itemid()
    if itemid in self._on_stack_hashes:
      raise ValueError(f'{functor} is recursive')
    if itemid in self._visited_itemids:
      return functor

    self._on_stack_hashes.add(itemid)
    try:
      vars_ = {}
      if functor.returns.get_schema() == schema_constants.EXPR:
        expr = introspection.unpack_expr(functor.returns)
        vars_ = self._visit_expr(expr, context=functor)
    finally:
      self._on_stack_hashes.remove(itemid)

    self._visited_itemids.add(itemid)

    return self._callback_fn(functor, vars_)

  def _visit_expr(
      self, expr: arolla.Expr, context: data_item.DataItem
  ) -> dict[str, data_item.DataItem]:
    """Processes an Arolla expression bound to a functor context.

    Args:
      expr: The Arolla expression.
      context: The Functor in which the expression is located.

    Returns:
      A dictionary mapping the names of the variables encountered in `expr`
      (and its sub-expressions accessed through variables in the same
      `context`) to their newly evaluated/transformed values (or original ones
      if `None` was returned by the callback).
    """
    for_hash = _nested_expr_for_hash(expr, context)
    if for_hash in self._on_stack_hashes:
      raise ValueError(f'{context} has dependency cycle in EXPRs')

    self._on_stack_hashes.add(for_hash)
    try:
      expr_vars = {}

      # TODO: Add support for visiting literals within Expr(s). It
      # can be a different state in _FunctorTraverser, either invoked for
      # Variables, either for Literals.
      for var_name, var in _get_variables(expr, context):
        # TODO: Add support for visiting more complex items, e.g.
        # Entity, which may have a nested functor that is invoked in this Expr
        # or list items, etc.
        if functor_factories.is_fn(var):
          # Most frequently set manually or programmatically with `with_attrs`
          # or similar after tracing (tracing happened likely with
          # kd.V.var_name).
          expr_vars[var_name] = self._visit_functor(functor=var)
        elif var.get_schema() == schema_constants.EXPR:
          # Most often happens when calling functions decorated with
          # kd.trace_as_fn. This generates code such as:
          # Functor[
          #   _sub_f_result=kd.call(V.sub_f, ...),
          #   sub_f=Functor[...],
          #   returns=V._sub_f_result + ...,
          # ]
          sub_var_vars = self._visit_expr(
              introspection.unpack_expr(var), context
          )
          expr_vars[var_name] = self._callback_fn(var, sub_var_vars)
          # Additional update is done here, because in the example above `sub_f`
          # occurs only as a variable of `_sub_f_result`, but not as a variable
          # of `returns`. `sub_f` is needed when calling `fn` on a `functor`.
          expr_vars.update(sub_var_vars)
        else:
          expr_vars[var_name] = self._callback_fn(var, {})
    finally:
      self._on_stack_hashes.remove(for_hash)

    return expr_vars


# NOTE: Not to be exposed as a public API for any of the Koda libraries (kd_g3,
# kd_g3_ext, etc.).
def visit_variables(
    functor: data_item.DataItem,
    callback_fn: Callable[
        [data_item.DataItem, dict[str, data_item.DataItem | None]],
        data_item.DataItem | None,
    ],
) -> data_item.DataItem | None:
  """Traverses the functor and calls callback_fn on each variable recursively.

  `callback_fn` is called on the top-level Functor, as well.

  The visit is done using the postorder strategy.

  The `callback_fn` provided by the caller is used to compute the new Koda
  Functor using the returned variable values in a bottom-up fashion. If
  `callback_fn` returns `None`, the original value will be passed up.

  Example:
    f = kd.fn(lambda x: kd.V.a * x + kd.V.b).with_attrs(a=42, b=37)

    def transform_fn(fn, sub_vars):
      if fn == f:
        return fn.with_attrs(a=12)
      return fn

    visitor.visit_variables(f, transform_fn)
    # Returns result equivalent to:
    # kd.fn(lambda x: kd.V.a * x + kd.V.b).with_attrs(a=12, b=37)

  To do a read-only traversal to only collect information, the `callback_fn`
  does not need to return a value or it can return the unmodified variable.

  Args:
    functor: Root functor to be traversed.
    callback_fn: callable that accepts a single Koda Functor and a dictionary of
      its variables on which `callback_fn` has already been applied.

  Returns:
    New Koda Functor or None
  """
  return _FunctorTraverser(callback_fn).traverse(functor)


# NOTE: Not to be exposed as a public API for any of the Koda libraries (kd_g3,
# kd_g3_ext, etc.).
def visit_subfunctors(
    functor: data_item.DataItem,
    callback_fn: Callable[[data_item.DataItem], None],
):
  """Traverses the functor and calls callback_fn on sub-functors recursively.

  The visit is done using the postorder strategy. Result of `callback_fn` is
  not used.

  Args:
    functor: Root functor to be traversed.
    callback_fn: callable that accepts a single Koda Functor provided by the
      caller.
  """
  def sub_functor_fn(
      var: data_item.DataItem, sub_vars: dict[str, data_item.DataItem]
  ):
    del sub_vars
    if functor_factories.is_fn(var):
      callback_fn(var)

  visit_variables(functor, sub_functor_fn)
