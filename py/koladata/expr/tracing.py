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

"""Tools to trace functions into Exprs."""

import inspect
import types as py_types
from typing import Any, Callable

from arolla import arolla
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import tracing_mode
from koladata.types import extension_types
from koladata.types import py_boxing
from koladata.util import kd_functools


I = input_container.InputContainer('I')


def _inspect_signature(fn: py_types.FunctionType) -> inspect.Signature:
  """Returns the signature of `fn` with resolved type annotations."""
  # See https://peps.python.org/pep-0563/#resolving-type-hints-at-runtime and
  # https://docs.python.org/3/library/inspect.html#inspect.signature for
  # details on `eval_str=True`.
  return inspect.signature(fn, eval_str=True)


@kd_functools.skip_from_functor_stack_trace
def trace(fn: Callable[..., Any]) -> arolla.Expr:
  """Converts the given Python function to an Expr by tracing it.

  Tracing means executing the function replacing argument "x" with an input I.x,
  and switching the container "kd." to tracing mode temporarily where it
  creates expressions instead of eagerly executing operators.

  This method supports all non-variadic Python parameters, but for variadic
  parameters there is limited support: the parameter name must start with
  'unused', and it must not be used in the function body. This stems from
  the fact that tracing cannot override the behavior of unpacking of
  tuples/dicts that would be necessary to meaningfully handle variadic
  arguments.

  You can call a parameter 'self' for it to become the
  positional argument when evaluating the resulting expression. The default
  values of the arguments of the function are ignored.

  Args:
    fn: The function to call.

  Returns:
    The expr produced by calling the function in lazy mode.
  """
  sig = _inspect_signature(fn)
  positional_params = []
  keyword_params = []
  for param in sig.parameters.values():
    # A sanity check for future Python versions.
    if param.kind not in [
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.VAR_POSITIONAL,
        inspect.Parameter.VAR_KEYWORD,
    ]:
      raise ValueError(f'Unexpected parameter kind: {param.kind}')
    if param.kind in [
        inspect.Parameter.VAR_POSITIONAL,
        inspect.Parameter.VAR_KEYWORD,
    ]:
      if not param.name.startswith('unused'):
        raise ValueError(
            f'Failed to trace the function {kd_functools.unwrap(fn)}. '
            'Variadic arguments are only supported in tracing when they are'
            ' actually not used in the function body, as a way to allow'
            ' arbitrary unknown arguments. If this is the case, please start'
            ' the parameter name with "unused" to acknowledge this. Current'
            f' parameter name: [{param.name}]'
        )
      # Otherwise ignore such parameters.
    else:
      if param.kind == inspect.Parameter.KEYWORD_ONLY:
        keyword_params.append(param)
      else:
        assert not keyword_params
        positional_params.append(param)

  def _get_arg_value(param: inspect.Parameter) -> Any:
    arg_value = I[param.name]
    if extension_types.is_koda_extension_type(param.annotation):
      arg_value = arolla.M.annotation.qtype(
          arg_value, extension_types.get_extension_qtype(param.annotation)
      )
    return arg_value

  try:
    with tracing_mode.enable_tracing():
      res = fn(
          *(_get_arg_value(p) for p in positional_params),
          **{p.name: _get_arg_value(p) for p in keyword_params},
      )
    expr = py_boxing.as_expr(res)
  except Exception as e:
    e.add_note(
        'Error occurred during tracing of the function'
        f' {kd_functools.unwrap(fn)}. If you only need Python evaluation, you'
        ' can use `kd.py_fn(fn)` instead.'
    )
    raise
  if diff := (
      frozenset(introspection.get_input_names(expr))
      - frozenset(p.name for p in positional_params)
      - frozenset(p.name for p in keyword_params)
  ):
    raise ValueError(
        f'unexpected inputs {sorted(diff)} found during tracing of function:'
        f' `{fn}`. Traced functions must not create new or capture external'
        ' inputs'
    )
  return expr
