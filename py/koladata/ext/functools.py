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

"""High-level tools to work with functions and functors."""

import functools
import types as py_types
from typing import Any, Callable

from arolla import arolla
from koladata import kd

kdf = kd.functor


def MaybeEval(
    x: Any, root: kd.types.DataSlice | None = None
) -> kd.types.DataSlice:
  """Evaluates x on root/dh if it's evaluable otherwise returns x."""
  if isinstance(x, list):
    return [MaybeEval(y, root=root) for y in x]
  elif isinstance(x, tuple):
    return tuple(MaybeEval(y, root=root) for y in x)
  elif isinstance(x, dict):
    return {k: MaybeEval(v, root=root) for k, v in x.items()}
  elif kd.is_fn(x) or isinstance(x, py_types.FunctionType):
    return x(root)
  if isinstance(x, arolla.Expr):
    return x.eval(root)
  else:
    return x


def AcceptExprArgs(fn: Callable[..., Any]) -> Callable[..., Any]:
  """Makes fn accept either DataSlice or Expr/Functor arguments.

  If Exprs are passed, an argument named `root` must be also be passed. Its
  DataSlice will be used to evaluate the Exprs.

  The situation is similar when Functors are passed: they will be evaluated
  on the given root DataSlice.

  Args:
    fn: function to wrap.

  Returns:
    Wrapped function that evals expressions/functors under the hood.
  """
  @functools.wraps(fn)
  def WrapperFn(*args, **kwargs):
    root = None
    if 'root' in kwargs:
      root = kwargs.pop('root')
    args = MaybeEval(args, root=root)
    kwargs = MaybeEval(kwargs, root=root)
    return fn(*args, **kwargs)

  return WrapperFn
