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

"""Entry point to Koda API for users."""

import sys as _sys
import types as _py_types
import typing as _typing

from koladata.exceptions import exceptions as _exceptions
from koladata.expr import expr_eval as _expr_eval
from koladata.expr import input_container as _input_container
from koladata.expr import introspection as _introspection
from koladata.expr import tracing_mode as _tracing_mode
from koladata.fstring import fstring as _fstring
from koladata.functions import functions as _functions
from koladata.functor import kdf as _kdf
from koladata.functor import tracing_decorator as _tracing_decorator
from koladata.operators import eager_op_utils as _eager_op_utils
from koladata.operators import kde_operators as _kde_operators
from koladata.testing import testing as _testing
from koladata.types import data_bag as _data_bag
from koladata.types import data_item as _data_item
from koladata.types import data_slice as _data_slice
from koladata.types import dict_item as _dict_item
from koladata.types import general_eager_ops as _general_eager_ops
from koladata.types import list_item as _list_item
from koladata.types import literal_operator as _literal_operator
from koladata.types import mask_constants as _mask_constants
from koladata.types import py_boxing as _py_boxing
from koladata.types import schema_constants as _schema_constants
from koladata.types import schema_item as _schema_item


_HAS_DYNAMIC_ATTRIBUTES = True

# This module supports tracing, which means running a Python function
# in a special mode where "kd.smth" APIs create new expressions instead
# of executing operations eagerly. Therefore for every API in this file
# we need to decide how it should behave in tracing mode. Please pay
# attention to this when adding new APIs, and test their tracing behavior
# in case it is not _eager_only.
# If you are not sure, _eager_only is the safest default, but note that
# it might provide a bad surprise for the user if the API is in fact useful
# in tracing workflows.

# Utiltities to make the tracing declarations below look nicer.
# Each public API in this module must use one of these.
# _tracing_config is a global dict that accumulates the tracing configs
# and is passed to the prepare_module_for_tracing method in the end.
_tracing_config = {}
_dispatch = lambda eager, tracing: _tracing_mode.configure_tracing(
    _tracing_config, eager=eager, tracing=tracing
)
_eager_only = lambda obj: _tracing_mode.eager_only(_tracing_config, obj)
_same_when_tracing = lambda obj: _tracing_mode.same_when_tracing(
    _tracing_config, obj
)


### Used as type annotations in user's code.
types = _eager_only(_py_types.ModuleType('types'))
types.DataBag = _data_bag.DataBag
types.DataItem = _data_item.DataItem
types.DataSlice = _data_slice.DataSlice
types.ListItem = _list_item.ListItem
types.DictItem = _dict_item.DictItem
types.SchemaItem = _schema_item.SchemaItem

exceptions = _eager_only(_py_types.ModuleType('exceptions'))
exceptions.KodaError = _exceptions.KodaError


### Eager operators / functions from operators.
def _InitOpsAndContainers(op_container):
  kd_ops = _eager_op_utils.operators_container(op_container)
  for op_or_container_name in dir(kd_ops):
    globals()[op_or_container_name] = _dispatch(
        eager=getattr(kd_ops, op_or_container_name),
        tracing=getattr(_kde_operators.kde, op_or_container_name),
    )


_InitOpsAndContainers('kde')


### Public functions.

# Creating DataItem / DataSlice.
# TODO: Make this work in tracing mode.
item = _eager_only(_data_item.DataItem.from_vals)
# TODO: Make this work in tracing mode.
slice = _eager_only(_data_slice.DataSlice.from_vals)  # pylint: disable=redefined-builtin


# Impure functions (kd.bag, kd.list, kd.new, kd.set_attr, ...).
def _LoadImpureFunctions():
  for fn_name in dir(_functions):
    if not fn_name.startswith('_'):
      globals()[fn_name] = _eager_only(getattr(_functions, fn_name))


_LoadImpureFunctions()


### Expr-related functions, Input/Variable containers and operator container.
I = _eager_only(_input_container.InputContainer('I'))
V = _eager_only(_input_container.InputContainer('V'))
S = _eager_only(I.self)
eval = _eager_only(_expr_eval.eval)  # pylint: disable=redefined-builtin
kde = _eager_only(_kde_operators.kde)
literal = _eager_only(_literal_operator.literal)
get_name = _eager_only(_introspection.get_name)
unwrap_named = _eager_only(_introspection.unwrap_named)
as_expr = _eager_only(_py_boxing.as_expr)
pack_expr = _eager_only(_introspection.pack_expr)
unpack_expr = _eager_only(_introspection.unpack_expr)
is_packed_expr = _eager_only(_introspection.is_packed_expr)
sub_inputs = _eager_only(_introspection.sub_inputs)
sub_by_name = _eager_only(_introspection.sub_by_name)
sub = _eager_only(_introspection.sub)
get_input_names = _eager_only(_introspection.get_input_names)
# This overrides fstr for eager computation due to subtle differences.
fstr = _dispatch(
    eager=_fstring.fstr, tracing=_kde_operators.kde.fstr
)
# This overrides the eager_op_utils implementation which unfortunately
# fails because M.annotation.name requires a literal as second argument.
with_name = _dispatch(
    eager=_general_eager_ops.with_name, tracing=_kde_operators.kde.with_name
)
trace_as_fn = _eager_only(_tracing_decorator.TraceAsFnDecorator)


### Koda constants.

# Primitive schemas.
INT32 = _same_when_tracing(_schema_constants.INT32)
INT64 = _same_when_tracing(_schema_constants.INT64)
FLOAT32 = _same_when_tracing(_schema_constants.FLOAT32)
FLOAT64 = _same_when_tracing(_schema_constants.FLOAT64)
BOOLEAN = _same_when_tracing(_schema_constants.BOOLEAN)
MASK = _same_when_tracing(_schema_constants.MASK)
BYTES = _same_when_tracing(_schema_constants.BYTES)
TEXT = _same_when_tracing(_schema_constants.TEXT)
EXPR = _same_when_tracing(_schema_constants.EXPR)

# Special purpose schemas.
ANY = _same_when_tracing(_schema_constants.ANY)
ITEMID = _same_when_tracing(_schema_constants.ITEMID)
OBJECT = _same_when_tracing(_schema_constants.OBJECT)
SCHEMA = _same_when_tracing(_schema_constants.SCHEMA)
NONE = _same_when_tracing(_schema_constants.NONE)

# Mask constants.
missing = _same_when_tracing(_mask_constants.missing)
present = _same_when_tracing(_mask_constants.present)


### Public submodules.

kdf = _eager_only(_kdf)
testing = _eager_only(_testing)
kdi = _eager_only(_py_types.ModuleType('kdi'))

__all__ = [api for api in globals().keys() if not api.startswith('_')]


def __dir__():  # pylint: disable=invalid-name
  return __all__


# `kdi` has eager versions of everything, available even in tracing mode.
def _SetUpKdi():
  for name in __all__:
    if name != 'kdi':
      setattr(kdi, name, globals()[name])
  kdi.__all__ = [x for x in __all__ if x != 'kdi']
  kdi.__dir__ = lambda: kdi.__all__


_SetUpKdi()

# Set up the tracing mode machinery. This must be the last thing in this file.
if not _typing.TYPE_CHECKING:
  _sys.modules[__name__] = _tracing_mode.prepare_module_for_tracing(
      _sys.modules[__name__], _tracing_config
  )
