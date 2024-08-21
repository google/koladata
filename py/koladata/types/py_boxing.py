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

"""Utilities for advanced Python values conversion to Koda abstractions."""

import inspect
import types as py_types
from typing import Any

from arolla import arolla
from koladata.types import data_bag
# NOTE: To allow Python scalar values to have DataItem Python type.
from koladata.types import data_item as _
from koladata.types import data_slice
from koladata.types import ellipsis
from koladata.types import literal_operator


# Binding policies should be implemented in such a way that all operators are
# bound in a same way and make sure behavior of binding does not alter between
# literal wrapping Python values and passing those Python values to kd.eval as a
# dict of values.


# Default policy wraps arbitrarily nested Python lists into DataSlices and wraps
# scalars into DataItems. QValues remain unchanged (useful for DataBags,
# JaggedShapes and already created DataSlices).
DEFAULT_BOXING_POLICY = 'koladata_default_boxing'

# The same as DEFAULT_BOXING_POLICY, but also implicitly wraps list/tuples into
# DataSlices.
LIST_BOXING_POLICY = 'koladata_list_boxing'

KWARGS_POLICY = 'koladata_kwargs'
OBJ_KWARGS_POLICY = 'koladata_obj_kwargs'

# NOTE: Recreating this object invalidates all existing references. Thus after
# reloading this module, any Exprs using this codec must be recreated.
# If this causes issues, we'll need to find a workaround.
_REF_CODEC_OBJECT = arolla.types.PyObjectReferenceCodec()
REF_CODEC = _REF_CODEC_OBJECT.name


# NOTE: This function should prefer to return QValues whenever possible to be as
# friendly to eager evaluation as possible.
def as_qvalue_or_expr(arg: Any) -> arolla.Expr | arolla.QValue:
  """Converts Python values into QValues or Exprs."""
  if isinstance(arg, (arolla.QValue, arolla.Expr)):
    return arg
  if isinstance(arg, slice):
    # NOTE: Slices purposfully use default Arolla boxing and in practice only
    # support ints or Nones.
    if arolla.Expr in (type(arg.start), type(arg.stop), type(arg.step)):
      # TODO: use a koda-specific operator for pretty printing.
      return arolla.M.core.make_slice(arg.start, arg.stop, arg.step)
    else:
      return arolla.types.Slice(arg.start, arg.stop, arg.step)
  if isinstance(arg, py_types.EllipsisType):
    return ellipsis.ellipsis()
  if isinstance(arg, (list, tuple)):
    raise ValueError(
        'passing a Python list/tuple to a Koda operation is ambiguous. Please '
        'use kd.slice(...) to create a slice or a multi-dimensional slice, and '
        'kd.list(...) to create a single Koda list.'
    )
  if callable(arg):
    return arolla.abc.PyObject(arg, codec=REF_CODEC)
  return data_slice.DataSlice.from_vals(arg)


def as_qvalue_or_expr_with_list_support(
    arg: Any,
) -> arolla.Expr | arolla.QValue:
  if isinstance(arg, (list, tuple)):
    return data_slice.DataSlice.from_vals(arg)
  return as_qvalue_or_expr(arg)


def as_qvalue(arg: Any) -> arolla.QValue:
  """Converts Python values into QValues."""
  qvalue_or_expr = as_qvalue_or_expr(arg)
  if isinstance(qvalue_or_expr, arolla.Expr):
    raise ValueError('expected a QValue, got an Expr')
  return qvalue_or_expr


def as_expr(arg: Any) -> arolla.Expr:
  """Converts Python values into Exprs."""
  qvalue_or_expr = as_qvalue_or_expr(arg)
  if isinstance(qvalue_or_expr, arolla.QValue):
    return literal_operator.literal(qvalue_or_expr)
  return qvalue_or_expr


def _verify_kwargs_policy_signature(signature: arolla.abc.Signature) -> None:
  """Verifies that a KwargsBindingPolicy can be applied to an Expr signature."""
  assert all([
      parameter.kind == 'positional-or-keyword'
      for parameter in signature.parameters
  ]), (
      'only positional-or-keyword arguments are supported in the underlying'
      ' Expr signature'
  )
  assert (
      len(signature.parameters) >= 1
  ), 'underlying operator must have at least one parameter'
  last_param = signature.parameters[-1]
  if last_param.default is not None:
    assert (
        arolla.types.is_namedtuple_qtype(last_param.default.qtype)
        or last_param.default.qtype == arolla.UNSPECIFIED
    ), 'default argument for final param must be a NamedTuple'


def _determine_positional_args_and_kwargs(
    signature: arolla.abc.Signature, *args: Any, **kwargs: Any
) -> tuple[tuple[Any, ...], dict[str, Any]]:
  """Determines positional args and kwargs for a KwargsBindingPolicy.

  The returned args will match the positional args of the Arolla Signature
  (except for the last which is the kwarg named tuple).

  Args:
    signature: Arolla signature that determines the args and kwargs
    *args: Positional args received by the binding policy. Will always remain
      positional.
    **kwargs: Keyword args received by the binding policy. May be moved to args
      if they are determined to match a positional-or-keyword parameter of the
      Arolla signature.

  Returns:
    tuple of new args and kwargs
  """
  _verify_kwargs_policy_signature(signature)
  positional_params = signature.parameters[:-1]
  if len(args) > len(positional_params):
    raise TypeError(f'unexpected number of positional args: {len(args)}')
  i = 0
  positional_args = []
  while i < len(args):
    positional_args.append(args[i])
    i += 1
  while i < len(positional_params):
    if positional_params[i].name in kwargs:
      positional_args.append(kwargs.pop(positional_params[i].name))
    elif positional_params[i].default is not None:
      positional_args.append(positional_params[i].default)
    else:
      raise TypeError(f'missing required argument: {positional_params[i].name}')
    i += 1

  # All positional-or-kwargs arguments from kwargs have been matched in the
  # previous loop, and here we are left with only `true` kwargs.
  return tuple(positional_args), kwargs


class _KwargsBindingPolicy(arolla.abc.AuxBindingPolicy):
  """Argument binding policy for Koda operators that take arbitrary kwargs.

  This policy maps Python signatures to Expr operator signatures and vice versa.
  The underlying Expr operator is required to accept a NamedTuple as the last
  argument. Python arguments are first matched to positional arguments of
  the Expr parameters, and any unmatched keyword arguments are passed through
  the NamedTuple.

  Example:
    kd.uuid(x=1, y=2, seed="RandomSeed") ->
    kd.uuid("RandomSeed", arolla.namedtuple(x=1, y=2))
  """

  def make_python_signature(
      self, signature: arolla.abc.Signature
  ) -> inspect.Signature:
    # NOTE: This binding policy does not support operators with varargs.
    _verify_kwargs_policy_signature(signature)
    params = []
    for param in signature.parameters[:-1]:
      params.append(
          inspect.Parameter(
              param.name,
              inspect.Parameter.POSITIONAL_OR_KEYWORD,
              default=inspect.Parameter.empty
              if param.default is None
              else param.default,
          )
      )
    params.append(
        inspect.Parameter(
            signature.parameters[-1].name, inspect.Parameter.VAR_KEYWORD
        )
    )
    return inspect.Signature(params)

  def bind_arguments(
      self, signature: arolla.abc.Signature, *args: Any, **kwargs: Any
  ) -> tuple[arolla.QValue | arolla.Expr, ...]:
    args, kwargs = _determine_positional_args_and_kwargs(
        signature, *args, **kwargs
    )
    args = tuple(as_qvalue_or_expr(arg) for arg in args)
    kwarg_values = list(map(as_qvalue_or_expr, kwargs.values()))
    if all(isinstance(v, arolla.QValue) for v in kwarg_values):
      return args + (
          arolla.namedtuple(**dict(zip(kwargs.keys(), kwarg_values))),
      )
    else:
      return args + (
          arolla.M.namedtuple.make(**dict(zip(kwargs.keys(), kwarg_values))),
      )

  def make_literal(self, value: arolla.QValue) -> arolla.Expr:
    return literal_operator.literal(value)


# TODO: Support single arg for `kd.obj` and `kd.new` and
# positional-keyword args: 'seed' for `kd.uuobj`, 'schema', etc for `kd.new`.
# This might mean splitting this policy into multiple similar ones with shared
# low-level utilities.
class _ObjectKwargsPolicy(_KwargsBindingPolicy):
  """Binding policy for Koda operators that require DataBag to bind its args."""

  def bind_arguments(
      self, signature: arolla.abc.Signature, *args: Any, **kwargs: Any
  ) -> tuple[arolla.QValue | arolla.Expr, ...]:
    args, kwargs = _determine_positional_args_and_kwargs(
        signature, *args, **kwargs
    )
    args = tuple(as_qvalue_or_expr(arg) for arg in args)
    return args + (data_bag.DataBag.empty()._kwargs_to_namedtuple(**kwargs),)  # pylint: disable=protected-access


##### Kola Data policy registrations #####

arolla.abc.register_classic_aux_binding_policy_with_custom_boxing(
    DEFAULT_BOXING_POLICY,
    as_qvalue_or_expr,
    make_literal_fn=literal_operator.literal,
)
arolla.abc.register_classic_aux_binding_policy_with_custom_boxing(
    LIST_BOXING_POLICY,
    as_qvalue_or_expr_with_list_support,
    make_literal_fn=literal_operator.literal,
)
arolla.abc.register_aux_binding_policy(KWARGS_POLICY, _KwargsBindingPolicy())
arolla.abc.register_aux_binding_policy(OBJ_KWARGS_POLICY, _ObjectKwargsPolicy())
