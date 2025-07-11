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

"""User-facing module for Koda extensions."""

import sys as _sys
import types as _py_types
import typing as _typing

from koladata import kd as _kd
from koladata.expr import source_location as _source_location
from koladata.expr import tracing_mode as _tracing_mode
from koladata.ext import nested_data as _nested_data
from koladata.ext import npkd as _npkd
from koladata.ext import pdkd as _pdkd
from koladata.ext import py_cloudpickle as _py_cloudpickle
from koladata.ext import vis as _vis
from koladata.ext.contrib import functions as _contrib_functions
from koladata.ext.operators import kde_operators as _kde_operators
from koladata.ext.persisted_data import persisted_data as _persisted_data
from koladata.operators import eager_op_utils as _eager_op_utils

_HAS_DYNAMIC_ATTRIBUTES = True

_tracing_config = {}
_dispatch = lambda eager, tracing: _tracing_mode.configure_tracing(
    _tracing_config, eager=eager, tracing=tracing
)
_eager_only = lambda obj: _tracing_mode.eager_only(_tracing_config, obj)
_same_when_tracing = lambda obj: _tracing_mode.same_when_tracing(
    _tracing_config, obj
)


# CamelCase versions in ext since we're still not sure what we will eventually
# recommend, and 'kd' is consistently lowercase for all operations.
Fn = _eager_only(_kd.fn)
PyFn = _eager_only(_kd.py_fn)

py_cloudpickle = _eager_only(_py_cloudpickle.py_cloudpickle)


def _init_ops_and_containers():
  kd_ext_ops = _eager_op_utils.operators_container(
      top_level_arolla_container=_kde_operators.kde_ext
  )
  for op_or_container_name in dir(kd_ext_ops):
    globals()[op_or_container_name] = _dispatch(
        eager=getattr(kd_ext_ops, op_or_container_name),
        tracing=_source_location.attaching_source_location(
            getattr(_kde_operators.kde_ext, op_or_container_name)
        ),
    )


_init_ops_and_containers()


def _load_functions(
    namespace: str, module: _py_types.ModuleType | _py_types.SimpleNamespace
):
  """Injects the functions from the provided module."""
  fake_module = _py_types.SimpleNamespace(
      **{attr: getattr(module, attr) for attr in module.__dir__()}
  )
  # If the name exists already, it means it is defined both in Expr
  # operator and function. E.g. kd.obj/dict/list. We need to override the
  # eager operator derived from Expr op with the function.
  if namespace in globals():
    globals()[namespace] = _dispatch(
        eager=_eager_op_utils.add_overrides(globals()[namespace], fake_module),
        tracing=_source_location.attaching_source_location(
            getattr(_kde_operators.kde_ext, namespace)
        ),
    )
  else:
    globals()[namespace] = _eager_only(fake_module)


_load_functions('npkd', _npkd)
_load_functions('pdkd', _pdkd)
_load_functions('nested_data', _nested_data)
_load_functions('persisted_data', _persisted_data)
_load_functions('vis', _vis)
_load_functions('contrib', _contrib_functions)


lazy = _eager_only(_kde_operators.kde_ext)
eager = _same_when_tracing(_py_types.ModuleType('eager'))


__all__ = [api for api in globals().keys() if not api.startswith('_')]


def __dir__():  # pylint: disable=invalid-name
  return __all__


# `eager` has eager versions of everything, available even in tracing mode.
def _set_up_eager():
  for name in __all__:
    if name != 'eager':
      setattr(eager, name, globals()[name])
  eager.__all__ = [x for x in __all__ if x != 'eager']
  eager.__dir__ = lambda: eager.__all__


_set_up_eager()

# Set up the tracing mode machinery. This must be the last thing in this file.
if not _typing.TYPE_CHECKING:
  _sys.modules[__name__] = _tracing_mode.prepare_module_for_tracing(
      _sys.modules[__name__], _tracing_config
  )
