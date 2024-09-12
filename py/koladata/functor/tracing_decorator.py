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

"""A decorator to customize the tracing behavior for a particular function."""

import functools
import types as py_types
from typing import Any

from koladata.expr import tracing_mode
from koladata.functor import functor_factories
from koladata.types import py_boxing


class TraceAsFnDecorator:
  """A decorator to customize the tracing behavior for a particular function.

  A function with this decorator is converted to an internally-stored functor.
  In traced expressions that call the function, that functor is invoked as a
  sub-functor via by 'kde.call', rather than the function being re-traced.
  Additionally, the result of 'kde.call' is also assigned a name, so that
  when auto_variables=True is used (which is the default in kdf.trace_py_fn),
  the functor for the decorated function will become an attribute of the
  functor for the outer function being traced.

  This can be used to avoid excessive re-tracing and recompilation of shared
  python functions, to quickly add structure to the functor produced by tracing
  for complex computations, or to conveniently embed a py_fn into a traced
  expression.

  This decorator is intended to be applied to standalone functions.

  When applying it to a lambda, consider specifying an explicit name, otherwise
  it will be called '<lambda>' or '<lambda>_0' etc, which is not very useful.

  When applying it to a class method, it is likely to fail in tracing mode
  because it will try to auto-box the class instance into an expr, which is
  likely not supported.
  """

  # TODO: Add support for py_cloudpcikle here.
  def __init__(
      self,
      *,
      name: str | None = None,
      py_fn: bool = False,
  ):
    """Initializes the decorator.

    Args:
      name: The name to assign to the sub-functor. If not provided, the name of
        the function being decorated is used.
      py_fn: Whether the function to trace should just be wrapped in kdf.py_fn
        and executed as Python code later instead of being traced to create the
        sub-functor. This is useful for functions that are not fully supported
        by the tracing infrastructure, and to add debug prints.
    """
    self._name = name
    self._py_fn = py_fn

  def __call__(self, fn: py_types.FunctionType) -> py_types.FunctionType:
    name = self._name if self._name is not None else fn.__name__
    if self._py_fn:
      to_call = functor_factories.py_fn(fn)
    else:
      to_call = functor_factories.trace_py_fn(fn)
    # It is important to create this expr once per function, so that its
    # fingerprint is stable and when we call it multiple times the functor
    # will only be extracted once by the auto-variables logic.
    to_call = py_boxing.as_expr(to_call).with_name(name)

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
      if tracing_mode.is_tracing_enabled():
        return to_call(*args, **kwargs)
      else:
        return fn(*args, **kwargs)

    return wrapper
