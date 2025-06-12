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

"""Utilities for advanced Python values conversion to Koda abstractions."""

import functools
import random
import types as py_types
from typing import Any, Callable

from arolla import arolla
from koladata.expr import py_expr_eval_py_ext
from koladata.types import data_bag
# NOTE: To allow Python scalar values to have DataItem Python type.
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import ellipsis
from koladata.types import jagged_shape
from koladata.types import literal_operator


literal = literal_operator.literal


# Binding policies should be implemented in such a way that all operators are
# bound in a same way and make sure behavior of binding does not alter between
# literal wrapping Python values and passing those Python values to kd.eval as a
# dict of values.


# Default boxing policy wraps various values (other than lists and tuples) into
# DataItems. QValues remain unchanged (useful for DataBags, JaggedShapes and
# already-created DataSlices).
DEFAULT_BOXING_POLICY = 'koladata_default_boxing'

# The same as DEFAULT_BOXING_POLICY, but also implicitly wraps list/tuples into
# DataSlices.
LIST_TO_SLICE_BOXING_POLICY = 'koladata_list_to_slice_boxing'

# NOTE: Recreating this object invalidates all existing references. Thus after
# reloading this module, any Exprs using this codec must be recreated.
# If this causes issues, we'll need to find a workaround.
_REF_CODEC_OBJECT = arolla.s11n.ReferencePyObjectCodec()
REF_CODEC = _REF_CODEC_OBJECT.name


NON_DETERMINISTIC_TOKEN_LEAF = arolla.abc.leaf(
    py_expr_eval_py_ext.NON_DETERMINISTIC_TOKEN_LEAF_KEY
)


# Isolated PRNG instance. Global to avoid overhead of repeated instantiation.
_RANDOM = random.Random()


def _random_int64() -> arolla.QValue:
  return arolla.int64(_RANDOM.randint(-(2**63), 2**63 - 1))


def new_non_deterministic_token() -> arolla.Expr:
  """Returns a new unique value for argument marked with non_deterministic marker."""
  return arolla.abc.bind_op(
      'koda_internal.non_deterministic',
      NON_DETERMINISTIC_TOKEN_LEAF,
      _random_int64(),
  )


def _no_py_function_boxing_registered(
    fn: py_types.FunctionType | functools.partial,
) -> arolla.QValue:
  del fn  # Unused.
  raise ValueError(
      'No implementation for Python function boxing was registered. If you are'
      ' importing the entire koladata, this should never happen. If you are'
      ' importing a subset of koladata modules, please do'
      ' `from koladata.functor import boxing`.'
  )


_py_function_boxing_fn: Callable[
    [py_types.FunctionType | functools.partial], arolla.QValue
] = _no_py_function_boxing_registered


def register_py_function_boxing_fn(
    fn: Callable[[py_types.FunctionType | functools.partial], arolla.QValue],
):
  """Registers an implementation for Python function boxing."""
  global _py_function_boxing_fn
  _py_function_boxing_fn = fn


# NOTE: This function should prefer to return QValues whenever possible to be as
# friendly to eager evaluation as possible.
def as_qvalue_or_expr(arg: Any) -> arolla.Expr | arolla.QValue:
  """Converts Python values into QValues or Exprs."""
  if isinstance(arg, (arolla.QValue, arolla.Expr)):
    return arg
  if isinstance(arg, slice):
    # None has a special meaning for slices defined by the Python language
    # (for example [2:] sets stop to None), so we box it to arolla.unspecified
    # instead of ds(None) here.
    def _wrap(x: Any) -> arolla.Expr | arolla.QValue:
      if x is None:
        return arolla.unspecified()
      return as_qvalue_or_expr(x)

    start = _wrap(arg.start)
    stop = _wrap(arg.stop)
    step = _wrap(arg.step)
    if arolla.Expr in (type(start), type(stop), type(step)):
      return arolla.abc.bind_op(
          'kd.tuples.slice', as_expr(start), as_expr(stop), as_expr(step)
      )
    else:
      return arolla.types.Slice(start, stop, step)
  if isinstance(arg, tuple):
    tpl = tuple(as_qvalue_or_expr(v) for v in arg)
    if arolla.Expr in (type(v) for v in tpl):
      return arolla.abc.make_operator_node(
          'kd.tuples.tuple', tuple(map(as_expr, tpl))
      )
    else:
      return arolla.tuple(*tpl)
  if isinstance(arg, py_types.EllipsisType):
    return ellipsis.ellipsis()
  if isinstance(arg, list):
    raise ValueError(
        'passing a Python list to a Koda operation is ambiguous. Please '
        'use kd.slice(...) to create a slice or a multi-dimensional slice, and '
        'kd.list(...) to create a single Koda list'
    )
  # TODO: Unify the handling of different function types.
  if isinstance(arg, (py_types.FunctionType, functools.partial)):
    return _py_function_boxing_fn(arg)
  if arg is data_slice.DataSlice:
    return data_item.DataItem.from_vals(None)
  if arg is data_bag.DataBag:
    return data_bag.DataBag.empty()
  if arg is jagged_shape.JaggedShape:
    return jagged_shape.create_shape()
  return data_item.DataItem.from_vals(arg)


def as_qvalue_or_expr_with_list_to_slice_support(
    arg: Any,
) -> arolla.Expr | arolla.QValue:
  if isinstance(arg, list):
    return data_slice.DataSlice.from_vals(arg)
  return as_qvalue_or_expr(arg)


def as_qvalue_or_expr_with_py_function_to_py_object_support(
    arg: Any,
) -> arolla.Expr | arolla.QValue:
  if isinstance(
      arg, (py_types.FunctionType, py_types.MethodType, functools.partial)
  ):
    return arolla.abc.PyObject(arg, codec=REF_CODEC)
  return as_qvalue_or_expr(arg)


_arolla_as_qvalue_or_expr = arolla.types.as_qvalue_or_expr


def as_qvalue(arg: Any) -> arolla.QValue:
  """Converts Python values into QValues."""
  qvalue_or_expr = as_qvalue_or_expr(arg)
  if isinstance(qvalue_or_expr, arolla.Expr):
    raise ValueError(
        'failed to construct a QValue from the provided input containing an'
        f' Expr: {arg}'
    )
  return qvalue_or_expr


def as_expr(arg: Any) -> arolla.Expr:
  """Converts Python values into Exprs."""
  qvalue_or_expr = as_qvalue_or_expr(arg)
  if isinstance(qvalue_or_expr, arolla.QValue):
    return literal(qvalue_or_expr)
  return qvalue_or_expr


##### Kola Data policy registrations #####

arolla.abc.register_classic_aux_binding_policy_with_custom_boxing(
    DEFAULT_BOXING_POLICY,
    as_qvalue_or_expr,
    make_literal_fn=literal,
)
arolla.abc.register_classic_aux_binding_policy_with_custom_boxing(
    LIST_TO_SLICE_BOXING_POLICY,
    as_qvalue_or_expr_with_list_to_slice_support,
    make_literal_fn=literal,
)

# Constants for custom boxing policies.
#
# IMPORTANT: These strings define attributes in the `py_boxing` module.
# If an operator uses the corresponding boxing policy, the attribute name
# will be stored with the operator during serialization.
#
# Modifying an attribute name must be done with caution, as it will invalidate
# previously serialised operators.
#
WITH_PY_FUNCTION_TO_PY_OBJECT = (
    'as_qvalue_or_expr_with_py_function_to_py_object_support'
)

WITH_AROLLA_BOXING = '_arolla_as_qvalue_or_expr'
