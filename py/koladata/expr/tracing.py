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

"""Tools to trace functions into Exprs."""

import inspect
from typing import Any, Callable

from arolla import arolla
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import tracing_mode
from koladata.types import py_boxing

I = input_container.InputContainer('I')


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
  sig = inspect.signature(fn)
  positional_param_names = []
  keyword_param_names = []
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
            'Variadic arguments are only supported in tracing when they are'
            ' actually not used in the function body, as a way to allow'
            ' arbitrary unknown arguments. If this is the case, please start'
            ' the parameter name with "unused" to acknowledge this. Current'
            f' parameter name: [{param.name}]'
        )
      # Otherwise ignore such parameters.
    else:
      if param.kind == inspect.Parameter.KEYWORD_ONLY:
        keyword_param_names.append(param.name)
      else:
        assert not keyword_param_names
        positional_param_names.append(param.name)

  try:
    with tracing_mode.enable_tracing():
      res = fn(
          *[I[x] for x in positional_param_names],
          **{x: I[x] for x in keyword_param_names},
      )
    expr = py_boxing.as_expr(res)
  except Exception as e:
    raise ValueError(
        f'Failed to trace the function: `{fn}`. If you only need Python'
        ' evaluation, you can use `kd.py_fn(fn)` instead.'
    ) from e
  if diff := (
      frozenset(introspection.get_input_names(expr))
      - frozenset(positional_param_names)
      - frozenset(keyword_param_names)
  ):
    raise ValueError(
        f'unexpected inputs {sorted(diff)} found during tracing of function:'
        f' `{fn}`. Traced functions must not create new or capture external'
        ' inputs'
    )
  return expr
