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

"""Entry point to Koda API for users."""

import sys as _sys
import types as _py_types
import typing as _typing

from koladata import kd_types as _kd_types
from koladata.base import init as _
from koladata.expr import expr_eval as _expr_eval
from koladata.expr import input_container as _input_container
from koladata.expr import introspection as _introspection
from koladata.expr import source_location as _source_location
from koladata.expr import tracing_mode as _tracing_mode
from koladata.extension_types import extension_types as _extension_types
from koladata.extension_types import functions as _extension_type_functions
from koladata.functions import attrs as _attrs
from koladata.functions import functions as _functions
from koladata.functions import object_factories as _object_factories
from koladata.functions import parallel as _parallel
from koladata.functions import predicates as _predicates
from koladata.functions import proto_conversions as _proto_conversions
from koladata.functions import py_conversions as _py_conversions
from koladata.functions import s11n as _s11n
from koladata.functions import schema as _schema
from koladata.functor import boxing as _
from koladata.functor import expr_container as _expr_container
from koladata.functor import functions as _functor_functions
from koladata.functor import functor_factories as _functor_factories
from koladata.functor import tracing_decorator as _tracing_decorator
from koladata.operators import eager_op_utils as _eager_op_utils
from koladata.operators import kde_operators as _kde_operators
from koladata.operators import optools as _optools
from koladata.operators import qtype_utils as _qtype_utils
from koladata.testing import testing as _testing
from koladata.type_checking import type_checking as _type_checking
from koladata.types import literal_operator as _literal_operator
from koladata.types import mask_constants as _mask_constants
from koladata.types import py_boxing as _py_boxing
from koladata.types import qtypes as _qtypes
from koladata.types import schema_constants as _schema_constants


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

### Used as type annotations in users' code.
types = _same_when_tracing(_kd_types)

### Koda QTypes.
qtypes = _same_when_tracing(_py_types.SimpleNamespace())
qtypes.DATA_SLICE = _qtypes.DATA_SLICE
qtypes.NON_DETERMINISTIC_TOKEN = _qtypes.NON_DETERMINISTIC_TOKEN
qtypes.DATA_BAG = _qtypes.DATA_BAG
qtypes.EXECUTOR = _qtypes.EXECUTOR

### Tools for defining operators.
optools = _eager_only(_py_types.SimpleNamespace())
optools.add_alias = _optools.add_alias
optools.add_to_registry = _optools.add_to_registry
optools.as_backend_operator = _optools.as_backend_operator
optools.as_lambda_operator = _optools.as_lambda_operator
optools.as_py_function_operator = _optools.as_py_function_operator
optools.add_to_registry_as_overload = _optools.add_to_registry_as_overload
optools.add_to_registry_as_overloadable = (
    _optools.add_to_registry_as_overloadable
)
optools.equiv_to_op = _optools.equiv_to_op
optools.as_qvalue = _py_boxing.as_qvalue
optools.as_qvalue_or_expr = _py_boxing.as_qvalue_or_expr
optools.fix_non_deterministic_tokens = _optools.fix_non_deterministic_tokens
optools.make_operators_container = _optools.make_operators_container
optools.unified_non_deterministic_arg = _optools.unified_non_deterministic_arg
optools.unified_non_deterministic_kwarg = (
    _optools.unified_non_deterministic_kwarg
)
optools.WITH_PY_FUNCTION_TO_PY_OBJECT = _py_boxing.WITH_PY_FUNCTION_TO_PY_OBJECT

### Operator constraints.
optools.constraints = _py_types.SimpleNamespace()
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
optools.eager = _py_types.SimpleNamespace()
optools.eager.EagerOperator = _eager_op_utils.EagerOperator


### Public functions.

# go/keep-sorted start
del_attr = _eager_only(_attrs.del_attr)
dir = _eager_only(_attrs.dir)  # pylint: disable=redefined-builtin
dumps = _eager_only(_s11n.dumps)
embed_schema = _eager_only(_attrs.embed_schema)
experimental_safer_loads = _eager_only(_s11n.experimental_safer_loads)
extension_type = _eager_only(_extension_types.extension_type)
fn = _same_when_tracing(_functor_factories.fn)
from_proto = _eager_only(_proto_conversions.from_proto)
from_proto_any = _eager_only(_proto_conversions.from_proto_any)
from_py = _eager_only(_py_conversions.from_py)
from_pytree = _eager_only(_py_conversions.from_py)
is_expr = _eager_only(_predicates.is_expr)
is_item = _eager_only(_predicates.is_item)
is_slice = _eager_only(_predicates.is_slice)
loads = _eager_only(_s11n.loads)
mutable_bag = _eager_only(_object_factories.mutable_bag)
py_fn = _same_when_tracing(_functor_factories.py_fn)
py_reference = _eager_only(_py_conversions.py_reference)
register_py_fn = _eager_only(_functor_factories.register_py_fn)
schema_from_proto = _eager_only(_proto_conversions.schema_from_proto)
schema_from_py = _eager_only(_schema.schema_from_py)
set_attr = _eager_only(_attrs.set_attr)
set_attrs = _eager_only(_attrs.set_attrs)
set_schema = _eager_only(_attrs.set_schema)
to_py = _eager_only(_py_conversions.to_py)
to_pylist = _eager_only(_py_conversions.to_pylist)
to_pytree = _eager_only(_py_conversions.to_pytree)
trace_as_fn = _same_when_tracing(_tracing_decorator.TraceAsFnDecorator)
trace_py_fn = _same_when_tracing(_functor_factories.trace_py_fn)
update_schema = _eager_only(_attrs.update_schema_fn)
# go/keep-sorted end

parallel = _eager_only(
    _py_types.SimpleNamespace(
        call_multithreaded=_parallel.call_multithreaded,
        yield_multithreaded=_parallel.yield_multithreaded,
        transform=_parallel.transform,
    )
)


### Eager operators / functions from operators.
def _InitOpsAndContainers():
  kd_ops = _eager_op_utils.operators_container('kd')
  # We cannot use dir() since it is overridden in this module.
  for op_or_container_name in kd_ops.__dir__():
    assert op_or_container_name not in globals()
    globals()[op_or_container_name] = _dispatch(
        eager=getattr(kd_ops, op_or_container_name),
        tracing=_source_location.attaching_source_location(
            getattr(_kde_operators.kde, op_or_container_name)
        ),
    )


_InitOpsAndContainers()


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
              tracing=_source_location.attaching_source_location(
                  getattr(_kde_operators.kde, fn_name)
              ),
          )
        else:
          globals()[fn_name] = _eager_only(fn_val)


_LoadImpureFunctions(_functions, _functor_functions, _extension_type_functions)


### Expr-related functions, Input/Variable containers and operator container.
I = _same_when_tracing(_input_container.InputContainer('I'))
V = _same_when_tracing(_input_container.InputContainer('V'))
S = _same_when_tracing(I.self)
eval = _eager_only(_expr_eval.eval)  # pylint: disable=redefined-builtin
lazy = _eager_only(_kde_operators.kde)
named_container = _same_when_tracing(_expr_container.NamedContainer)
check_inputs = _same_when_tracing(_type_checking.check_inputs)
check_output = _same_when_tracing(_type_checking.check_output)
duck_type = _same_when_tracing(_type_checking.duck_type)
duck_list = _same_when_tracing(_type_checking.duck_list)
duck_dict = _same_when_tracing(_type_checking.duck_dict)
static_when_tracing = _same_when_tracing(_type_checking.static_when_tracing)

expr = _eager_only(_py_types.SimpleNamespace())
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
expr.is_input = _introspection.is_input
expr.is_variable = _introspection.is_variable
expr.is_literal = _introspection.is_literal


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
ITEMID = _same_when_tracing(_schema_constants.ITEMID)
OBJECT = _same_when_tracing(_schema_constants.OBJECT)
SCHEMA = _same_when_tracing(_schema_constants.SCHEMA)
NONE = _same_when_tracing(_schema_constants.NONE)

# Mask constants.
missing = _same_when_tracing(_mask_constants.missing)
present = _same_when_tracing(_mask_constants.present)

# Functor constants.

# "functor" namespace is created by _InitOpsAndContainers above.
functor.SWITCH_DEFAULT = _same_when_tracing(_kde_operators.SWITCH_DEFAULT)  # pylint: disable=undefined-variable
SWITCH_DEFAULT = functor.SWITCH_DEFAULT  # pylint: disable=undefined-variable

### Public submodules.

testing = _eager_only(_testing)
eager = _same_when_tracing(_py_types.ModuleType('eager'))
signature_utils = _eager_only(_functor_functions._signature_utils)  # pylint: disable=protected-access

__all__ = [api for api in globals().keys() if not api.startswith('_')]


def __dir__():  # pylint: disable=invalid-name
  return __all__


# `eager` has eager versions of everything, available even in tracing mode.
def _SetUpEager():
  for name in __all__:
    if name != 'eager':
      setattr(eager, name, globals()[name])
  eager_all = [x for x in __all__ if x != 'eager']
  eager.__all__ = eager_all
  eager.__dir__ = lambda: eager_all


_SetUpEager()

from koladata.kd_dynamic import *  # pylint: disable=g-import-not-at-top,g-bad-import-order,wildcard-import

# Set up the tracing mode machinery. This must be the last thing in this file.
if not _typing.TYPE_CHECKING:
  _sys.modules[__name__] = _tracing_mode.prepare_module_for_tracing(
      _sys.modules[__name__], _tracing_config
  )
