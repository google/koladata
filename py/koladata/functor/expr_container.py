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
  """A container that automatically names expressions.

  In eager mode, non-expression inputs are stored as-is. In tracing mode,
  they are converted to expressions (functions and lambdas are automatically
  traced).

  Example:
    c = kd.named_container()

    # 1. Non-tracing mode
    # Storing a value:
    c.foo = 5
    c.foo       # Returns 5

    # Storing an expression:
    c.x_plus_y = I.x + I.y
    c.x_plus_y  # Returns (I.x + I.y).with_name('x_plus_y')

    # Listing stored items:
    vars(c)  # Returns {'foo': 5, 'x_plus_y': (I.x + I.y).with_name('x_plus_y')}

    # 2. Tracing mode
    def my_fn(x):
      c = kd.named_container()
      c.a = 2
      c.b = 1
      return c.a * x + c.b

    fn = kd.fn(my_fn)
    fn.a  # Returns 2 (accessible because it was named by the container)
    fn(x=5)  # Returns 11
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
