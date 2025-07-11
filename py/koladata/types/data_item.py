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

"""DataItem abstraction."""

from typing import Any, Callable

from arolla import arolla
from koladata.expr import py_expr_eval_py_ext as _py_expr_eval_py_ext
from koladata.types import data_item_py_ext as _data_item_py_ext
from koladata.types import data_slice
from koladata.util import kd_functools


_eval_op = _py_expr_eval_py_ext.eval_op

DataItem = _data_item_py_ext.DataItem


### Implementation of the DataItem's additional functionality.
@data_slice.add_method(DataItem, '__hash__')  # pylint: disable=protected-access
def _hash(self) -> int:
  return hash(self.fingerprint)


# Ideally we'd do this only for functors, but we don't have a fast way
# to check if a DataItem is a functor now. Note that SchemaItem overrides
# this behavior.
@kd_functools.skip_from_functor_stack_trace
@data_slice.add_method(DataItem, '__call__')  # pylint: disable=protected-access
def _call(
    self, *args: Any, return_type_as: Any = data_slice.DataSlice, **kwargs: Any
) -> data_slice.DataSlice:
  """Dispatches between eager and expr kd.call based on the arguments."""
  if any(isinstance(arg, arolla.Expr) for arg in args) or any(
      isinstance(arg, arolla.Expr) for arg in kwargs.values()
  ):
    return arolla.abc.aux_bind_op(
        'kd.call', self, *args, return_type_as=return_type_as, **kwargs
    )
  else:
    return _eval_op(
        'kd.call', self, *args, return_type_as=return_type_as, **kwargs
    )


_bind_method_impl: Callable[..., data_slice.DataSlice] = None


# As soon as this pattern is used >=3 times, please create a general
# registration mechanism for non-operator-based DataItem methods.
def register_bind_method_implementation(
    fn: Callable[..., data_slice.DataSlice],
):
  """Registers a method implementation for DataItem.bind."""
  global _bind_method_impl
  _bind_method_impl = fn


# Ideally we'd do this only for functors, but we don't have a fast way
# to check if a DataItem is a functor now.
@data_slice.add_method(DataItem, 'bind')  # pylint: disable=protected-access
def bind(
    self,
    *,
    return_type_as: Any = data_slice.DataSlice,
    **kwargs: Any,
) -> data_slice.DataSlice:
  """Returns a Koda functor that partially binds a function to `kwargs`.

  This function is intended to work the same as functools.partial in Python.
  More specifically, for every "k=something" argument that you pass to this
  function, whenever the resulting functor is called, if the user did not
  provide "k=something_else" at call time, we will add "k=something".

  Note that you can only provide defaults for the arguments passed as keyword
  arguments this way. Positional arguments must still be provided at call time.
  Moreover, if the user provides a value for a positional-or-keyword argument
  positionally, and it was previously bound using this method, an exception
  will occur.

  You can pass expressions with their own inputs as values in `kwargs`. Those
  inputs will become inputs of the resulting functor, will be used to compute
  those expressions, _and_ they will also be passed to the underying functor.
  Use kdf.call_fn for a more clear separation of those inputs.

  Example:
    f = kd.fn(I.x + I.y).bind(x=0)
    kd.call(f, y=1)  # 1

  Args:
    self: A Koda functor.
    return_type_as: The return type of the functor is expected to be the same as
      the type of this value. This needs to be specified if the functor does not
      return a DataSlice. kd.types.DataSlice, kd.types.DataBag and
      kd.types.JaggedShape can also be passed here.
    **kwargs: Partial parameter binding. The values in this map may be Koda
      expressions or DataItems. When they are expressions, they must evaluate to
      a DataSlice/DataItem or a primitive that will be automatically wrapped
      into a DataItem. This function creates auxiliary variables with names
      starting with '_aux_fn', so it is not recommended to pass variables with
      such names.

  Returns:
    A new Koda functor with some parameters bound.
  """
  # Since bind is not an operator, we cannot use the operator registration
  # mechanism, and have a custom registration mechanism instead.
  if _bind_method_impl is None:
    raise ValueError(
        'No implementation for DataItem.bind() was registered. If you are'
        ' importing the entire koladata, this should never happen. If you are'
        ' importing a subset of koladata modules, please import'
        ' functor_factories.'
    )
  return _bind_method_impl(self, return_type_as=return_type_as, **kwargs)
