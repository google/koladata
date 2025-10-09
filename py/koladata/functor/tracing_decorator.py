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

"""A decorator to customize the tracing behavior for a particular function."""

import functools
import inspect
import types as py_types
from typing import Any, Protocol

from arolla import arolla
from koladata.expr import tracing_mode
from koladata.functor import functor_factories
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.util import kd_functools


def _return_type_as(annotation: type[Any]) -> arolla.AnyQValue:
  """Returns a value with the Arolla type that should be used for 'annotation'."""
  if (value := py_boxing.get_dummy_qvalue(annotation)) is not None:
    return value
  else:
    # This will be incorrect if 'annotation' is anything except a DataSlice,
    # in which case we currently expect the user to specify return_type_as
    # explicitly.
    return data_slice.DataSlice.from_vals(None)


def _inspect_signature(fn: py_types.FunctionType) -> inspect.Signature:
  """Returns the signature of `fn` with resolved type annotations."""
  # See https://peps.python.org/pep-0563/#resolving-type-hints-at-runtime and
  # https://docs.python.org/3/library/inspect.html#inspect.signature for
  # details on `eval_str=True`.
  return inspect.signature(fn, eval_str=True)


def _wrap_with_from_and_to_kd(
    fn: py_types.FunctionType,
) -> py_types.FunctionType:
  """Adds conversion of the arguments from Koda and the return value to Koda."""
  sig = _inspect_signature(fn)
  wrapper_params = []
  for param in sig.parameters.values():
    if param.default is not inspect.Parameter.empty:
      param = param.replace(default=py_boxing.as_qvalue_or_expr(param.default))
    wrapper_params.append(param)
  wrapper_sig = sig.replace(parameters=wrapper_params)

  @functools.wraps(fn)
  def wrapper(*args: Any, **kwargs: Any) -> Any:
    __tracebackhide__ = True  # pylint: disable=invalid-name, unused-variable
    return py_boxing.as_qvalue_or_expr(fn(*args, **kwargs))

  wrapper.__signature__ = wrapper_sig

  return wrapper


class FunctorFactory(Protocol):
  """`functor_factory` argument protocol for `trace_as_fn`.

  Implements:
    (py_types.FunctionType, return_type_as: arolla.QValue) -> DataItem
  """

  def __call__(
      self, fn: py_types.FunctionType, return_type_as: arolla.AnyQValue
  ) -> data_item.DataItem:
    ...


class TraceAsFnDecorator:
  """A decorator to customize the tracing behavior for a particular function.

  A function with this decorator is converted to an internally-stored functor.
  In traced expressions that call the function, that functor is invoked as a
  sub-functor via by 'kde.call', rather than the function being re-traced.
  Additionally, the functor passed to 'kde.call' is assigned a name, so that
  when auto_variables=True is used (which is the default in kd.trace_py_fn),
  the functor for the decorated function will become an attribute of the
  functor for the outer function being traced.
  The result of 'kde.call' is also assigned a name with a '_result' suffix, so
  that it also becomes an separate variable in the outer function being traced.
  This is useful for debugging.

  This can be used to avoid excessive re-tracing and recompilation of shared
  python functions, to quickly add structure to the functor produced by tracing
  for complex computations, or to conveniently embed a py_fn into a traced
  expression.

  When using kd.parallel.call_multithreaded, using this decorator on
  sub-functors can improve parallelism, since all sub-functor calls
  are treated as separate tasks to be parallelized there.

  This decorator is intended to be applied to standalone functions.

  When applying it to a lambda, consider specifying an explicit name, otherwise
  it will be called '<lambda>' or '<lambda>_0' etc, which is not very useful.

  When applying it to a class method, it is likely to fail in tracing mode
  because it will try to auto-box the class instance into an expr, which is
  likely not supported.

  When executing the resulting function in eager mode, we will evaluate the
  underlying function directly instead of evaluating the functor, to have
  nicer stack traces in case of an exception. However, we will still apply
  the boxing rules on the returned value (for example, convert Python primitives
  to DataItems) to better emulate what will happen in tracing mode.
  """

  def __init__(
      self,
      *,
      name: str | None = None,
      return_type_as: Any = None,
      functor_factory: FunctorFactory | None = None,
  ):
    """Initializes the decorator.

    Args:
      name: The name to assign to the sub-functor. If not provided, the name of
        the function being decorated is used.
      return_type_as: The return type of the function is expected to be the same
        as the type of this value. This needs to be specified if the function
        does not return a DataSlice/DataItem or a primitive that would be
        auto-boxed into a DataItem. kd.types.DataSlice, kd.types.DataBag and
        kd.types.JaggedShape can also be passed here.
      functor_factory: The callable that produces the functor. Will be called
        with `(fn, return_type_as=return_type_as)` where `fn` is a modified
        version of the function to be converted into a functor, and
        `return_type_as` is a normalized version of the provided
        `return_type_as`. Should return a functor from the provided arguments.
        For example, pass `functor_factory=kd.py_fn` to wrap the function as raw
        Python code rather than being traced. Defaults to
        `lambda fn, return_type_as: kd.trace_py_fn(fn)`, which performs tracing.
    """
    self._name = name
    self._return_type_as = return_type_as
    if functor_factory is None:
      self._functor_factory = (
          lambda fn, return_type_as: functor_factories.trace_py_fn(fn)
      )
    else:
      self._functor_factory = functor_factory

  def __call__(self, fn: py_types.FunctionType) -> py_types.FunctionType:
    name = self._name if self._name is not None else fn.__name__
    sig = _inspect_signature(fn)
    kd_fn = _wrap_with_from_and_to_kd(fn)
    return_type_as = self._return_type_as
    if return_type_as is None:
      return_type_as = _return_type_as(sig.return_annotation)
    return_type_as = py_boxing.as_qvalue(return_type_as)
    to_call = self._functor_factory(kd_fn, return_type_as=return_type_as)
    # It is important to create this expr once per function, so that its
    # fingerprint is stable and when we call it multiple times the functor
    # will only be extracted once by the auto-variables logic.
    to_call = py_boxing.as_expr(to_call).with_name(name)

    @functools.wraps(fn)
    @kd_functools.skip_from_functor_stack_trace
    def wrapper(*args: Any, **kwargs: Any) -> Any:
      __tracebackhide__ = True  # pylint: disable=invalid-name,unused-variable

      # Converts the arguments to Koda before calling the function.
      try:
        bound = sig.bind(*args, **kwargs)
      except TypeError as ex:
        ex.__traceback__ = None  # Clear existing traceback
        raise  # Re-raise, excluding this function from the traceback

      # We don't call apply_defaults() here since the default values are
      # stored in the functor already, which allows the user to edit the functor
      # to change them if necessary.
      for param in sig.parameters.values():
        if param.name in bound.arguments:
          bound.arguments[param.name] = py_boxing.as_qvalue_or_expr(
              bound.arguments[param.name]
          )

      if tracing_mode.is_tracing_enabled():
        res = to_call(
            *bound.args,
            **bound.kwargs,
            return_type_as=return_type_as,
        ).with_name(f'_{name}_result')
      else:
        res = kd_fn(*bound.args, **bound.kwargs)
        if isinstance(res, arolla.Expr):
          raise ValueError(
              f'The function [{name}] annotated with @kd.trace_as_fn() was'
              ' expected to return a value in eager mode, but the computation'
              ' returned an Expr instead.'
          )
        if res.qtype != return_type_as.qtype:
          raise ValueError(
              f'The function [{name}] annotated with @kd.trace_as_fn() was'
              f' expected to return `{return_type_as.qtype}` as the'
              ' output type, but the computation resulted in type'
              f' `{res.qtype}` instead. Consider adding or updating'
              ' return_type_as= argument to @kd.trace_as_fn().'
          )
      return res

    return wrapper
