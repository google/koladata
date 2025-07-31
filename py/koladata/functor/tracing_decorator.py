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

import abc
import functools
import inspect
import types as py_types
from typing import Any, Callable

from arolla import arolla
from koladata.expr import tracing_mode
from koladata.functor import functor_factories
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import extension_types
from koladata.types import py_boxing
from koladata.util import kd_functools


# The design is loosely inspired by TensorFlow TraceType.
class TypeTracingConfig(abc.ABC):
  """Describes handling a given user Python type as input/output when tracing."""

  @abc.abstractmethod
  def return_type_as(self, annotation: type[Any]) -> Any:
    """Returns a value with the Koda type that should be used for 'annotation'.

    Args:
      annotation: The type annotation on the argument/return value.

    Returns:
      A Koda value or a value that can be automatically boxed into a Koda value
      with the Koda type that should be used for 'annotation'.
    """
    raise NotImplementedError()

  @abc.abstractmethod
  def to_kd(self, annotation: type[Any], value: Any) -> Any:
    """Converts a value annotated with 'annotation' to a Koda value or expr.

    This method will be invoked both in tracing mode and in eager mode (to
    reduce the probability of failure in tracing mode), so it
    needs to support both. Usually this means that the user type should be
    a composite type that can hold either Koda values or exprs inside.

    Args:
      annotation: The type annotation on the parameter/return value that we need
        to convert.
      value: The value of type 'annotation' to convert. Can either be a value of
        the user type created inside the function being traced, or a value
        returned from from_kd().

    Returns:
      A Koda value, a value that can be automatically boxed into a Koda value,
      or expr that represents the given value. After boxing, should have the
      same type as self.return_type_as(annotation). If the values of this type
      are going to be used as default values in the signature, it should
      be a DataItem or auto-boxable to DataItem.
    """
    raise NotImplementedError()

  @abc.abstractmethod
  def from_kd(self, annotation: type[Any], value: Any) -> Any:
    """Converts a Koda value or expr to a value of this type.

    This method will be invoked both in tracing mode and in eager mode (to
    reduce the probability of failure in tracing mode), so it
    needs to support both. Usually this means that the user type should be
    a composite type that can hold either Koda values or exprs inside.

    Args:
      annotation: The type annotation on the parameter/return value that we need
        to convert.
      value: The Koda value to convert, with the same type as
        self.return_type_as(annotation). Will always be a value returned from a
        previous call to to_kd().

    Returns:
      The value of type 'annotation' that represents the given Koda value or
      expression.
    """
    raise NotImplementedError()


class DefaultTypeTracingConfig(TypeTracingConfig):
  """Default type tracing config."""

  def return_type_as(self, annotation: type[Any]) -> Any:
    """Returns a value with the Arolla type that should be used for 'annotation'."""
    if annotation is data_bag.DataBag:
      return data_bag.DataBag.empty()
    elif extension_types.is_koda_extension_type(annotation):
      return extension_types.wrap(
          data_slice.DataSlice.from_vals(None),
          extension_types.get_extension_qtype(annotation),
      )
    else:
      # This will be incorrect if 'annotation' is anything except a DataSlice,
      # in which case we currently expect the user to specify return_type_as
      # explicitly.
      return data_slice.DataSlice.from_vals(None)

  def to_kd(self, annotation: type[Any], value: Any) -> Any:
    """Returns value as is."""
    return value

  def from_kd(self, annotation: type[Any], value: Any) -> Any:
    """Returns value as is."""
    return value


TYPE_TRACING_CONFIG_METHOD_NAME = '_koladata_type_tracing_config_'


def _get_type_tracing_config(annotation: type[Any]) -> TypeTracingConfig:
  """Returns the type tracing config for the given class."""
  try:
    config = getattr(annotation, TYPE_TRACING_CONFIG_METHOD_NAME)
  except AttributeError:
    config = DefaultTypeTracingConfig
  return config()


def _to_kd(annotation: type[Any], value: Any) -> Any:
  """Converts a value annotated with 'annotation' to a Koda value or expr."""
  return py_boxing.as_qvalue_or_expr(
      _get_type_tracing_config(annotation).to_kd(annotation, value)
  )


def _from_kd(annotation: type[Any], value: Any) -> Any:
  """Converts a Koda value or expr to a value of type annotated with 'annotation'."""
  return _get_type_tracing_config(annotation).from_kd(annotation, value)


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
  # This is a performance optimization to avoid iterating over all parameters in
  # the common case of no custom tracing config.
  params_with_custom_config = []
  for param in sig.parameters.values():
    if hasattr(param.annotation, TYPE_TRACING_CONFIG_METHOD_NAME):
      params_with_custom_config.append(param)
    if param.default is not inspect.Parameter.empty:
      param = param.replace(default=_to_kd(param.annotation, param.default))
    wrapper_params.append(param)
  wrapper_sig = sig.replace(parameters=wrapper_params)

  @functools.wraps(fn)
  def wrapper(*args: Any, **kwargs: Any) -> Any:
    __tracebackhide__ = True  # pylint: disable=invalid-name, unused-variable
    if params_with_custom_config:
      bound = wrapper_sig.bind(*args, **kwargs)
      bound.apply_defaults()
      for param in params_with_custom_config:
        bound.arguments[param.name] = _from_kd(
            param.annotation,
            bound.arguments[param.name],
        )
      res = fn(*bound.args, **bound.kwargs)
    else:
      res = fn(*args, **kwargs)
    return _to_kd(sig.return_annotation, res)

  wrapper.__signature__ = wrapper_sig

  return wrapper


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

  If the function accepts or returns a type that is not supported by Koda
  natively, the corresponding argument/return value must be annotated with a
  type that has a _koladata_type_tracing_config_() classmethod that returns an
  instance of TypeTracingConfig to describe how to convert the value to/from
  Koda.

  Note that for _koladata_type_tracing_config_ to work, type annotations must
  _not_ be forward declarations (which is possible when using `from __future__
  import annotations`) as these will fail to be resolved.

  When executing the resulting function in eager mode, we will evaluate the
  underlying function directly instead of evaluating the functor, to have
  nicer stack traces in case of an exception. However, we will still apply
  the boxing rules on the returned value (for example, convert Python primitives
  to DataItems), and the to/from Koda conversions defined by
  _koladata_type_tracing_config_, if any, to better emulate what will happen in
  tracing
  mode.
  """

  def __init__(
      self,
      *,
      name: str | None = None,
      py_fn: bool = False,
      return_type_as: Any = None,
      wrapper: Callable[[py_types.FunctionType], Any] | None = None,
  ):
    """Initializes the decorator.

    Args:
      name: The name to assign to the sub-functor. If not provided, the name of
        the function being decorated is used.
      py_fn: Whether the function to trace should just be wrapped in kd.py_fn
        and executed as Python code later instead of being traced to create the
        sub-functor. This is useful for functions that are not fully supported
        by the tracing infrastructure, and to add debug prints.
      return_type_as: The return type of the function is expected to be the same
        as the type of this value. This needs to be specified if the function
        does not return a DataSlice/DataItem or a primitive that would be
        auto-boxed into a DataItem. kd.types.DataSlice, kd.types.DataBag and
        kd.types.JaggedShape can also be passed here.
      wrapper: Extra wrapper to apply to the function before converting to a
        functor. I.e. can be a serialization wrapper (kd.py_reference or
        kd_ext.py_cloudpickle).
    """
    self._name = name
    self._py_fn = py_fn
    self._return_type_as = return_type_as
    self._wrapper = wrapper

  def __call__(self, fn: py_types.FunctionType) -> py_types.FunctionType:
    name = self._name if self._name is not None else fn.__name__
    sig = _inspect_signature(fn)
    kd_fn = _wrap_with_from_and_to_kd(fn)
    return_type_as = self._return_type_as
    if return_type_as is None:
      return_type_as = _get_type_tracing_config(
          sig.return_annotation
      ).return_type_as(sig.return_annotation)
    return_type_as = py_boxing.as_qvalue(return_type_as)
    if self._wrapper is not None:
      wrapped_fn = self._wrapper(kd_fn)
    else:
      wrapped_fn = kd_fn

    if self._py_fn:
      to_call = functor_factories.py_fn(
          wrapped_fn, return_type_as=return_type_as
      )
    else:
      to_call = functor_factories.trace_py_fn(wrapped_fn)
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
          bound.arguments[param.name] = _to_kd(
              param.annotation, bound.arguments[param.name]
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
      res = _from_kd(sig.return_annotation, res)
      return res

    return wrapper
