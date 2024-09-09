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

import inspect
import typing
from typing import Any, Callable

from arolla import arolla
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import tracing
from koladata.functions import functions as fns
from koladata.functor import py_functors_py_ext as _py_functors_py_ext
from koladata.functor import signature_utils
from koladata.operators import kde_operators
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import mask_constants
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
kde = kde_operators.kde


# We can move this wrapping inside the CPython layer if needed for performance.
def _maybe_wrap_expr(arg: Any) -> arolla.QValue:
  if isinstance(arg, arolla.Expr):
    arg = introspection.pack_expr(arg)
  return py_boxing.as_qvalue(arg)


def _extract_auto_variables(variables: dict[str, Any]) -> dict[str, Any]:
  """Creates additional variables for DataSlices and named nodes."""
  variables = dict(variables)
  aux_variable_fingerprints = set()
  prefix_to_counter = {}

  def create_unique_variable(prefix: str) -> str:
    if prefix not in prefix_to_counter:
      prefix_to_counter[prefix] = 0
    while True:
      var_name = f'{prefix}_{prefix_to_counter[prefix]}'
      prefix_to_counter[prefix] += 1
      if var_name not in variables:
        return var_name

  def transform_node(node: arolla.Expr) -> arolla.Expr:
    if node.is_literal or isinstance(node.op, literal_operator.LiteralOperator):
      val = typing.cast(arolla.QValue, node.qvalue)
      if val.qtype == qtypes.DATA_SLICE:
        val = typing.cast(data_slice.DataSlice, val)
        # Keep simple constants inlined, to avoid too many variables.
        if (
            val.db is None
            and val.get_ndim() == 0
            and val.get_schema().is_primitive_schema()
        ):
          return node
        # We need to implode the DataSlice into lists if it is not a DataItem.
        var_name = create_unique_variable('aux')
        var = V[var_name]
        ndim = val.get_ndim()
        if ndim:
          val = fns.implode(val, ndim=ndim)
          var = kde.explode(var, ndim=ndim)
        variables[var_name] = val
        aux_variable_fingerprints.add(var.fingerprint)
        return var

    if (expr_name := introspection.get_name(node)) is not None:
      name = (
          create_unique_variable(expr_name)
          if expr_name in variables
          else expr_name
      )
      child = introspection.unwrap_named(node)
      if child.fingerprint in aux_variable_fingerprints:
        # If a literal DataSlice was named, avoid creating a temporary name
        # and use the real name instead. The auxiliary variable might have
        # been wrapped with kde.explode(), so we do a sub_inputs instead of
        # just replacing with V[name].
        var_names = input_container.get_input_names(child, V)
        assert len(var_names) == 1
        variables[name] = variables.pop(var_names[0])
        return input_container.sub_inputs(child, V, **{var_names[0]: V[name]})
      variables[name] = introspection.pack_expr(child)
      return V[name]

    return node

  all_exprs = {}
  for k, v in variables.items():
    if v.get_schema() == schema_constants.EXPR:
      v = introspection.unpack_expr(v)
      all_exprs[k] = v
  combined = arolla.M.core.make_tuple(*all_exprs.values())
  # It is important to transform everything at once if there is some shared
  # named subtree.
  combined = arolla.abc.transform(combined, transform_node)
  assert len(combined.node_deps) == len(all_exprs)
  for k, v in zip(all_exprs, combined.node_deps):
    variables[k] = introspection.pack_expr(v)
  return variables


def fn(
    returns: Any,
    *,
    signature: data_slice.DataSlice | None = None,
    auto_variables: bool = False,
    **variables: Any,
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
    auto_variables: When true, we create additional variables automatically
      based on the provided expressions for 'returns' and user-provided
      variables. All non-scalar DataSlice literals become their own variables,
      and all named subexpressions become their own variables. This helps
      readability and manipulation of the resulting functor.
    **variables: The variables of the functor. Each variable can either be an
      expression to be evaluated, or a DataItem, or a primitive that will be
      wrapped as a DataItem. The result of evaluating the variable can be
      accessed as V.smth in other expressions.

  Returns:
    A DataItem representing the functor.
  """
  returns = _maybe_wrap_expr(returns)
  variables = {k: _maybe_wrap_expr(v) for k, v in variables.items()}
  if auto_variables:
    variables['returns'] = returns
    variables = _extract_auto_variables(variables)
    returns = variables.pop('returns')
  return _py_functors_py_ext.create_functor(returns, signature, **variables)


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


def trace_py_fn(
    f: Callable[..., Any], *, auto_variables: bool = True
) -> data_slice.DataSlice:
  """Returns a Koda functor created by tracing a given Python function.

  When 'f' has variadic positional (*args) or variadic keyword
  (**kwargs) arguments, their name must start with 'unused', and they
  must actually be unused inside 'f'.
  'f' must not use Python control flow operations such as if or for.

  Args:
    f: Python function.
    auto_variables: When true, we create additional variables automatically
      based on the traced expression. All DataSlice literals become their own
      variables, and all named subexpressions become their own variables. This
      helps readability and manipulation of the resulting functor. Note that
      this defaults to True here, while it defaults to False in kdf.fn.

  Returns:
    A DataItem representing the functor.
  """
  traced_expr = tracing.trace(f)
  signature = signature_utils.from_py_signature(inspect.signature(f))
  return fn(traced_expr, signature=signature, auto_variables=auto_variables)
