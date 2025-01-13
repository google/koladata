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

from arolla import arolla as _arolla
from koladata.exceptions import exceptions as _exceptions
from koladata.expr import expr_eval as _expr_eval
from koladata.expr import input_container as _input_container
from koladata.expr import introspection as _introspection
from koladata.expr import py_expr_eval_py_ext as _py_expr_eval_py_ext
from koladata.expr import tracing_mode as _tracing_mode
from koladata.functions import functions as _functions
from koladata.functor import functions as _functor_functions
from koladata.operators import eager_op_utils as _eager_op_utils
from koladata.operators import kde_operators as _kde_operators
from koladata.operators import optools as _optools
from koladata.operators import qtype_utils as _qtype_utils
from koladata.testing import testing as _testing
from koladata.types import data_bag as _data_bag
from koladata.types import data_item as _data_item
from koladata.types import data_slice as _data_slice
from koladata.types import dict_item as _dict_item
from koladata.types import jagged_shape as _jagged_shape
from koladata.types import list_item as _list_item
from koladata.types import literal_operator as _literal_operator
from koladata.types import mask_constants as _mask_constants
from koladata.types import py_boxing as _py_boxing
from koladata.types import qtypes as _qtypes
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
types.JaggedShape = _jagged_shape.JaggedShape
types.SchemaItem = _schema_item.SchemaItem
types.Expr = _arolla.Expr

### Koda QTypes.
qtypes = _same_when_tracing(_py_types.ModuleType('qtypes'))
qtypes.DATA_SLICE = _qtypes.DATA_SLICE
qtypes.DATA_BAG = _qtypes.DATA_BAG

### Tools for defining operators.
optools = _eager_only(_py_types.ModuleType('optools'))
optools.add_alias = _optools.add_alias
optools.add_to_registry = _optools.add_to_registry
optools.as_backend_operator = _optools.as_backend_operator
optools.as_lambda_operator = _optools.as_lambda_operator
optools.as_py_function_operator = _optools.as_py_function_operator
optools.equiv_to_op = _optools.equiv_to_op
optools.as_qvalue = _py_boxing.as_qvalue
optools.as_qvalue_or_expr = _py_boxing.as_qvalue_or_expr
optools.make_operators_container = _optools.make_operators_container

### Operator constraints.
optools.constraints = _py_types.ModuleType('constraints')
optools.constraints.expect_data_slice = _qtype_utils.expect_data_slice
optools.constraints.expect_data_slice_args = _qtype_utils.expect_data_slice_args
optools.constraints.expect_data_slice_kwargs = (
    _qtype_utils.expect_data_slice_kwargs
)
optools.constraints.expect_data_slice_or_unspecified = (
    _qtype_utils.expect_data_slice_or_unspecified
)
optools.constraints.expect_data_bag_args = _qtype_utils.expect_data_bag_args
optools.constraints.expect_jagged_shape = _qtype_utils.expect_jagged_shape
optools.constraints.expect_jagged_shape_or_unspecified = (
    _qtype_utils.expect_jagged_shape_or_unspecified
)

### Tools for eager operators.
optools.eager = _py_types.ModuleType('optools')
optools.eager.EagerOperator = _eager_op_utils.EagerOperator

exceptions = _eager_only(_py_types.ModuleType('exceptions'))
exceptions.KodaError = _exceptions.KodaError


### Eager operators / functions from operators.
def _InitOpsAndContainers():
  kd_ops = _eager_op_utils.operators_container('kde')
  # We cannot use dir() since it is overridden in this module.
  for op_or_container_name in kd_ops.__dir__():
    globals()[op_or_container_name] = _dispatch(
        eager=getattr(kd_ops, op_or_container_name),
        tracing=getattr(_kde_operators.kde, op_or_container_name),
    )


_InitOpsAndContainers()


### Public functions.


# Impure functions (kd.bag, kd.list, kd.new, kd.set_attr, ...).
def _LoadImpureFunctions(*modules: _py_types.ModuleType):
  """Injects the functions from functions.py into kd.py."""
  seen_names = set()
  for module in modules:
    # We cannot use dir() since it is overridden in this module.
    for fn_name in module.__dir__():
      if not fn_name.startswith('_'):
        if fn_name in seen_names:
          raise ValueError(
              'The same function is overridden in two different modules:'
              f' {fn_name}'
          )
        seen_names.add(fn_name)
        fn_val = getattr(module, fn_name)
        # If the name exists already, it means it is defined both in Expr
        # operator and function. E.g. kd.obj/dict/list. We need to override the
        # eager operator derived from Expr op with the function.
        if fn_name in globals():
          if isinstance(fn_val, _py_types.SimpleNamespace):
            # Override operator containers via function namespaces for now.
            fn_val = _eager_op_utils.add_overrides(globals()[fn_name], fn_val)
          globals()[fn_name] = _dispatch(
              eager=fn_val,
              tracing=getattr(_kde_operators.kde, fn_name),
          )
        else:
          globals()[fn_name] = _eager_only(fn_val)


_LoadImpureFunctions(_functions, _functor_functions)


### Expr-related functions, Input/Variable containers and operator container.
I = _eager_only(_input_container.InputContainer('I'))
V = _eager_only(_input_container.InputContainer('V'))
S = _eager_only(I.self)
eval = _eager_only(_expr_eval.eval)  # pylint: disable=redefined-builtin
clear_eval_cache = _eager_only(_py_expr_eval_py_ext.clear_eval_cache)
lazy = _eager_only(_kde_operators.kde)
# TODO: Remove this alias once the migration is done.
kde = _eager_only(_kde_operators.kde)

expr = _eager_only(_py_types.ModuleType('expr'))
expr.literal = _literal_operator.literal
expr.get_name = _introspection.get_name
expr.unwrap_named = _introspection.unwrap_named
expr.as_expr = _py_boxing.as_expr
expr.pack_expr = _introspection.pack_expr
expr.unpack_expr = _introspection.unpack_expr
expr.is_packed_expr = _introspection.is_packed_expr
expr.sub_inputs = _introspection.sub_inputs
expr.sub_by_name = _introspection.sub_by_name
expr.sub = _introspection.sub
expr.get_input_names = _introspection.get_input_names


### Koda constants.

# Primitive schemas.
INT32 = _same_when_tracing(_schema_constants.INT32)
INT64 = _same_when_tracing(_schema_constants.INT64)
FLOAT32 = _same_when_tracing(_schema_constants.FLOAT32)
FLOAT64 = _same_when_tracing(_schema_constants.FLOAT64)
BOOLEAN = _same_when_tracing(_schema_constants.BOOLEAN)
MASK = _same_when_tracing(_schema_constants.MASK)
BYTES = _same_when_tracing(_schema_constants.BYTES)
STRING = _same_when_tracing(_schema_constants.STRING)
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

testing = _eager_only(_testing)
eager = _same_when_tracing(_py_types.ModuleType('eager'))
# TODO: Remove this alias once the migration is done.
kdi = _same_when_tracing(eager)

__all__ = [api for api in globals().keys() if not api.startswith('_')]


def __dir__():  # pylint: disable=invalid-name
  return __all__


# `eager` has eager versions of everything, available even in tracing mode.
def _SetUpEager():
  for name in __all__:
    if name != 'eager' and name != 'kdi':
      setattr(eager, name, globals()[name])
  eager.__all__ = [x for x in __all__ if x != 'eager' and x != 'kdi']
  eager.__dir__ = lambda: eager.__all__


_SetUpEager()

# Set up the tracing mode machinery. This must be the last thing in this file.
if not _typing.TYPE_CHECKING:
  _sys.modules[__name__] = _tracing_mode.prepare_module_for_tracing(
      _sys.modules[__name__], _tracing_config
  )
