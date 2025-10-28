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

"""Containers for Exprs."""

import types as py_types
from typing import Any
from koladata.expr import tracing_mode
from koladata.functions import predicates
from koladata.functor import tracing_decorator
from koladata.operators import kde_operators

kde = kde_operators.kde


class NamedContainer:
  """Container that automatically names Exprs.

  For non-expr inputs, in tracing mode it will be converted to an Expr,
  while in non-tracing mode it will be stored as is. This allows to use
  NamedContainer eager code that will later be traced.

  For example:
    c = kd.ext.expr_container.NamedContainer()
    c.x_plus_y = I.x + I.y
    c.x_plus_y  # Returns (I.x + I.y).with_name('x_plus_y')
    c.foo = 5
    c.foo  # Returns 5

  Functions and lambdas are automatically traced in tracing mode.

  For example:
    def foo(x):
      c = kd.ext.expr_container.NamedContainer()
      c.x = x
      c.update = lambda x: x + 1
      return c.update(c.x)

    fn = kd.fn(foo)
    fn(x=5)  # Returns 6
  """

  _HAS_DYNAMIC_ATTRIBUTES = True

  def __dir__(self):
    return self.__dict__.keys()

  def __setattr__(self, key: str, value: Any):
    if key.startswith('_') or key.endswith('_'):
      raise AttributeError(
          f'Attempt to set value with reserved key `container.{key}`.\nNames'
          ' that start or end with an underscore are reserved for class'
          ' methods.'
      )
    if predicates.is_expr(value):
      value = value.with_name(key)
    elif isinstance(value, py_types.FunctionType):
      value = tracing_decorator.TraceAsFnDecorator(name=key)(value)
    elif tracing_mode.is_tracing_enabled():
      value = kde.with_name(value, key)
    self.__dict__[key] = value
