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

"""Tools to enable/disable the tracing mode."""

import contextlib
import contextvars
import types
from typing import Any, Iterator, TypeVar

_T = TypeVar('_T')
_TRACING_ENABLED: contextvars.ContextVar[bool] = contextvars.ContextVar(
    '_TRACING_ENABLED', default=False
)


@contextlib.contextmanager
def enable_tracing(enable: bool = True) -> Iterator[None]:
  """A context manager that can enable or disable the tracing mode."""
  token = _TRACING_ENABLED.set(enable)
  try:
    yield
  finally:
    _TRACING_ENABLED.reset(token)


def is_tracing_enabled() -> bool:
  """Returns the current tracing mode."""
  return _TRACING_ENABLED.get()


# A class to be able to use any object as key in dict by address.
# When defining the tracing configs, we would like to avoid duplicating
# the method names just for the tracing config, so we want to correspond
# the method that we found by name in globals() with the method that
# we have seen when the config was defined, using this wrapper.
class _HashAnythingWrapper:

  def __init__(self, obj: Any):
    self.obj = obj

  def __hash__(self):
    return hash(id(self.obj))

  def __eq__(self, other: Any) -> bool:
    if not isinstance(other, _HashAnythingWrapper):
      return False
    return self.obj is other.obj


def configure_tracing(
    configs: dict[Any, Any],
    eager: _T,
    tracing: Any,
) -> _T:
  """Configures the tracing behavior for the given object in the given dict.

  Args:
    configs: A mutable dict mapping from _HashAnythingWrapper of the eager
      version of an object to the tracing version of this object.
    eager: The eager version of the object.
    tracing: The tracing version of the object.

  Returns:
    The eager version of the object.
  """
  wrapper = _HashAnythingWrapper(eager)
  if wrapper in configs and configs[wrapper] is not tracing:
    raise ValueError(
        f'{eager} is already in the config dict with a different value:'
        f' {configs[wrapper]} vs {tracing}'
    )
  configs[wrapper] = tracing
  return eager


def same_when_tracing(configs: dict[Any, Any], obj: _T) -> _T:
  """Marks the given object as the same in eager and tracing mode."""
  return configure_tracing(configs, obj, obj)


def eager_only(configs: dict[Any, Any], obj: _T) -> _T:
  """Marks the given object as eager-only."""
  return configure_tracing(configs, obj, None)


class _TracingDescriptor:
  """A descriptor that returns the eager or tracing version of the object."""

  __slots__ = ('_name', '_eager', '_tracing')

  def __init__(self, name: Any, eager: Any, tracing: Any):
    self._name = name
    self._eager = eager
    self._tracing = tracing

  def __get__(self, obj: Any, objtype: Any = None) -> Any:
    # We do not use is_tracing_enabled() here to avoid the overhead of
    # a function call.
    if _TRACING_ENABLED.get():
      res = self._tracing
      if res is None:
        raise AttributeError(
            f"attribute '{self._name}' is not available in tracing mode on"
            f" '{objtype.__name__}'"
        )
      else:
        return res
    else:
      return self._eager

  # As recommended in
  # https://docs.python.org/3/howto/descriptor.html#descriptor-protocol,
  # adding a __set__ method makes it a data descriptor, which means that
  # Python will not look up in the object's __dict__ when accessing the
  # attribute, which speeds things up.
  def __set__(self, obj: Any, value: Any):
    raise AttributeError(
        'setting attributes on tracing objects is not supported'
    )


def prepare_module_for_tracing(
    mod: types.ModuleType,
    configs: dict[Any, Any],
) -> Any:
  """Prepares a module for tracing.

  This method should receive the mutable globals dict of a module, which
  must contain __all__ as the list of public APIs. Each of those public
  APIs must be registered in the configs dict.

  This will return an object to be placed into sys.modules instead of the
  original module, as suggested here:
  https://mail.python.org/pipermail/python-ideas/2012-May/014969.html

  Newer versions of Python support having a __getattr__ method on the module
  to avoid messing with sys.modules, however, using it comes with about 700ns
  penalty per module attribute access, which we would like to avoid.
  This approach results in only a 130ns penalty per module attribute access.

  The intended usage is to write this in the beginning of a module:

  _tracing_config = {}
  _dispatch = lambda eager, tracing: _tracing_mode.configure_tracing(
      _tracing_config, eager=eager, tracing=tracing
  )
  _eager_only = lambda obj: _tracing_mode.eager_only(_tracing_config, obj)
  _same_when_tracing = lambda obj: _tracing_mode.same_when_tracing(
      _tracing_config, obj
  )

  Then use _dispatch, _eager_only and _same_when_tracing to define the
  public APIs of the module, and then write this in the end of a module:

  if not _typing.TYPE_CHECKING:
    _sys.modules[__name__] = _tracing_mode.prepare_module_for_tracing(
        _sys.modules[__name__], _tracing_config
    )

  See koladata/kd.py for a usage example.

  Args:
    mod: The module to replace with the tracing-friendly version.
    configs: The tracing config dict.

  Returns:
    An object that should be put into sys.modules instead of the given module
    as the last line of the module.
  """
  name_to_eager = {}
  name_to_tracing = {}
  configs = dict(configs)
  for name in mod.__all__:  # pytype: disable=attribute-error
    obj = getattr(mod, name)
    wrapper = _HashAnythingWrapper(obj)
    if wrapper in configs:
      name_to_eager[name] = obj
      name_to_tracing[name] = configs[wrapper]
    else:
      raise ValueError(f"no tracing config for '{mod.__name__}.{name}'")

  # We need to create a new class to be able to use descriptors:
  # https://docs.python.org/3/howto/descriptor.html
  class _Dispatcher:
    """A class to dispatch calls to the eager or tracing version of the API."""

    def __init__(self, mod: types.ModuleType):
      # We need to keep a reference to the original module so that it's
      # not garbage collected, since we're going to replace it in the
      # sys.modules dict.
      self._ref_to_mod = mod
      self.__doc__ = mod.__doc__
      self.__all__ = mod.__all__  # pytype: disable=attribute-error

    def __dir__(self):
      return self.__all__

    # Pickle will store _ref_to_mod by module name, so on unpickling we'll
    # rerun the import and _Dispatcher will be recreated, therefore we
    # just pass "lambda x: x" as the constructing function.
    def __reduce__(self):
      return (lambda x: x, (self._ref_to_mod,))

  # For nicer error messages on missing attributes.
  _Dispatcher.__name__ = mod.__name__

  for name in name_to_eager:
    setattr(
        _Dispatcher,
        name,
        _TracingDescriptor(name, name_to_eager[name], name_to_tracing[name]),
    )

  return _Dispatcher(mod)
