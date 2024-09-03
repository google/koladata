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

import collections
import inspect
import random
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
FULL_SIGNATURE_POLICY = 'koladata_full_signature'
OBJ_KWARGS_POLICY = 'koladata_obj_kwargs'

# NOTE: Recreating this object invalidates all existing references. Thus after
# reloading this module, any Exprs using this codec must be recreated.
# If this causes issues, we'll need to find a workaround.
_REF_CODEC_OBJECT = arolla.types.PyObjectReferenceCodec()
REF_CODEC = _REF_CODEC_OBJECT.name


# Param markers used by FULL_SIGNATURE_POLICY. A param marker is an Arolla tuple
# of length 2 or 3 where the first element is `_PARAM_MARKER`, the second
# element is the marker type (`_*_MARKER_TYPE`), and the optional third element
# is the default value.
_PARAM_MARKER = arolla.text('__koladata_param_marker__')

_POSITIONAL_ONLY_MARKER_TYPE = arolla.text('positional_only')
_POSITIONAL_OR_KEYWORD_MARKER_TYPE = arolla.text('positional_or_keyword')
_VAR_POSITIONAL_MARKER_TYPE = arolla.text('var_positional')
_KEYWORD_ONLY_MARKER_TYPE = arolla.text('keyword_only')
_VAR_KEYWORD_MARKER_TYPE = arolla.text('var_keyword')
_HIDDEN_SEED_MARKER_TYPE = arolla.text('hidden_seed')

_NO_DEFAULT_VALUE = object()


def positional_only(default_value=_NO_DEFAULT_VALUE) -> arolla.abc.QValue:
  """Marks a parameter as positional-only (with optional default)."""
  if default_value is _NO_DEFAULT_VALUE:
    return arolla.tuple(_PARAM_MARKER, _POSITIONAL_ONLY_MARKER_TYPE)
  return arolla.tuple(
      _PARAM_MARKER, _POSITIONAL_ONLY_MARKER_TYPE, as_qvalue(default_value)
  )


def positional_or_keyword(default_value=_NO_DEFAULT_VALUE) -> arolla.abc.QValue:
  """Marks a parameter as positional-or-keyword (with optional default)."""
  if default_value is _NO_DEFAULT_VALUE:
    return arolla.tuple(_PARAM_MARKER, _POSITIONAL_OR_KEYWORD_MARKER_TYPE)
  return arolla.tuple(
      _PARAM_MARKER,
      _POSITIONAL_OR_KEYWORD_MARKER_TYPE,
      as_qvalue(default_value),
  )


def var_positional() -> arolla.abc.QValue:
  """Marks a parameter as variadic-positional."""
  return arolla.tuple(_PARAM_MARKER, _VAR_POSITIONAL_MARKER_TYPE)


def keyword_only(default_value=_NO_DEFAULT_VALUE) -> arolla.abc.QValue:
  """Marks a parameter as keyword-only (with optional default)."""
  if default_value is _NO_DEFAULT_VALUE:
    return arolla.tuple(_PARAM_MARKER, _KEYWORD_ONLY_MARKER_TYPE)
  return arolla.tuple(
      _PARAM_MARKER, _KEYWORD_ONLY_MARKER_TYPE, as_qvalue(default_value)
  )


def var_keyword() -> arolla.abc.QValue:
  """Marks a parameter as variadic-keyword."""
  return arolla.tuple(_PARAM_MARKER, _VAR_KEYWORD_MARKER_TYPE)


def hidden_seed() -> arolla.abc.QValue:
  """Marks a parameter as a hidden seed parameter."""
  return arolla.tuple(_PARAM_MARKER, _HIDDEN_SEED_MARKER_TYPE)


def _is_marker_param_default(param_default: arolla.abc.QValue) -> bool:
  if not arolla.is_tuple_qtype(param_default.qtype):
    return False
  if param_default.field_count not in (2, 3):  # pytype: disable=attribute-error
    return False
  if param_default[0].fingerprint != _PARAM_MARKER.fingerprint:  # pytype: disable=unsupported-operands
    return False
  return True


def _get_marker_type_and_default_value(
    param: arolla.abc.SignatureParameter,
) -> tuple[arolla.abc.QValue | None, arolla.abc.QValue | None]:
  """Unpacks an Arolla expr signature param into (marker_type, default_value).

  Args:
    param: Signature parameter.

  Returns:
    (marker, default_value)
    marker_type: If this is a marker, the marker type, else None.
    default_value: The default value for this param, regardless of whether it
      is a marker. If None, the param has no default value (so it is required).
  """
  param_default = param.default
  if param_default is None:
    return None, None
  if not _is_marker_param_default(param_default):
    return None, param_default
  if param_default.field_count == 2:  # pytype: disable=attribute-error
    return param_default[1], None  # pytype: disable=unsupported-operands
  return param_default[1], param_default[2]  # pytype: disable=unsupported-operands


def _is_positional_only(marker_type: arolla.abc.QValue | None) -> bool:
  return (
      marker_type is not None
      and marker_type.fingerprint == _POSITIONAL_ONLY_MARKER_TYPE.fingerprint
  )


def _is_positional_or_keyword(marker_type: arolla.abc.QValue | None) -> bool:
  return (
      marker_type is not None
      and marker_type.fingerprint
      == _POSITIONAL_OR_KEYWORD_MARKER_TYPE.fingerprint
  )


def _is_var_positional(marker_type: arolla.abc.QValue | None) -> bool:
  return (
      marker_type is not None
      and marker_type.fingerprint == _VAR_POSITIONAL_MARKER_TYPE.fingerprint
  )


def _is_keyword_only(marker_type: arolla.abc.QValue | None) -> bool:
  return (
      marker_type is not None
      and marker_type.fingerprint == _KEYWORD_ONLY_MARKER_TYPE.fingerprint
  )


def _is_var_keyword(marker_type: arolla.abc.QValue | None) -> bool:
  return (
      marker_type is not None
      and marker_type.fingerprint == _VAR_KEYWORD_MARKER_TYPE.fingerprint
  )


def _is_hidden_seed(marker_type: arolla.abc.QValue | None) -> bool:
  return (
      marker_type is not None
      and marker_type.fingerprint == _HIDDEN_SEED_MARKER_TYPE.fingerprint
  )


def find_hidden_seed_param(signature: inspect.Signature) -> int | None:
  """Returns the index of the hidden seed param, or None if there isn't one."""
  for i_param, param in enumerate(signature.parameters.values()):
    if (
        isinstance(param.default, arolla.QValue)
        and _is_marker_param_default(param.default)
        and _is_hidden_seed(param.default[1])
    ):
      return i_param
  return None


HIDDEN_SEED_PLACEHOLDER = arolla.abc.placeholder(
    '_koladata_hidden_seed_placeholder'
)


# Isolated PRNG instance. Global to avoid overhead of repeated instantiation.
_RANDOM = random.Random()


def _random_int64() -> arolla.QValue:
  return arolla.int64(_RANDOM.randint(-(2**63), 2**63 - 1))


def with_unique_hidden_seed(expr: arolla.Expr) -> arolla.Expr:
  return arolla.sub_by_fingerprint(
      expr,
      {HIDDEN_SEED_PLACEHOLDER.fingerprint: arolla.literal(_random_int64())},
  )


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


class _FullSignatureBindingPolicy(arolla.abc.AuxBindingPolicy):
  """Argument binding policy for Koda operators with an arbitrary signature.

  This policy maps Python signatures to Expr operator signatures and vice versa.
  The underlying Expr signature is expected to have all positional-or-keyword
  parametrers. Special default values are used to mark Expr parameters that
  should be converted to other kinds of parameters for the Python signature.

  For example, if you wanted a lambda operator to have the Python signature:

    def op(x, /, y, *args, z, **kwargs):
      ...

  then the lambda operator would be defined as:

    def op(
        x=positional_only(),
        y=positional_or_keyword(),
        args=var_positional(),
        z=keyword_only(),
        kwargs=var_keyword()):
      ...

  and we would convert the Python call `op(a, b, c, d, z=e, x=f, y=g)` to the
  Expr call `op(a, b, arolla.tuple(c, d), e, arolla.namedtuple(x=f, y=g))`.

  The following marker values are supported:
  - `var_positional()` marks an parameter as variadic-positional (like `*args`).
    The corresponding Expr parameter must accept an Arolla tuple.
  - `var_keyword()` marks an parameter as variadic-keyword (like `**kwargs`).
    The corresponding Expr parameter must accept an Arolla namedtuple.
  - `keyword_only()` marks a parameter as keyword-only, with an optional
    default value (which will be boxed if necessary).
  - `positional_only()` marks a parameter as positional-only, with an optional
    default value (which will be boxed if necessary).
  - `positional_or_keyword()` marks a parameter as positional-or-keyword, with
    an optional default value. Even though this is the default behavior, it may
    be necessary because earlier arguments in the signature have default values
    used for markers.
  - `hidden_seed()` marks a parameter as a hidden seed parameter, which will
    be removed from the Python signature, and will be populated with a random
    Arolla int64 value on every invocation.

  The implied Python signature must be a valid Python signature (`**kwargs`
  must be last, args after `*` must be keyword-only, etc.)

  This policy *also* applies default Koda value boxing, including to the values
  in the tuple/namedtuple passed to *args/**kwargs.
  """

  def make_python_signature(
      self, signature: arolla.abc.Signature,
  ) -> inspect.Signature:
    params = []
    visited_var_positional_param = False
    for param in signature.parameters:
      if param.kind != 'positional-or-keyword':
        raise ValueError(
            'only positional-or-keyword arguments are supported in the '
            'underlying Expr signature'
        )

      marker_type, default_value = _get_marker_type_and_default_value(param)
      python_param_default = (
          default_value
          if default_value is not None
          else inspect.Parameter.empty
      )

      if _is_positional_only(marker_type):
        python_param_kind = inspect.Parameter.POSITIONAL_ONLY
      elif _is_positional_or_keyword(marker_type):
        python_param_kind = inspect.Parameter.POSITIONAL_OR_KEYWORD
      elif _is_var_positional(marker_type):
        if visited_var_positional_param:
          # This is not enforced by inspect.Signature for some reason.
          raise ValueError('multiple variadic positional arguments')
        python_param_kind = inspect.Parameter.VAR_POSITIONAL
        visited_var_positional_param = True
      elif _is_keyword_only(marker_type):
        python_param_kind = inspect.Parameter.KEYWORD_ONLY
      elif _is_var_keyword(marker_type):
        python_param_kind = inspect.Parameter.VAR_KEYWORD
      elif marker_type is None:
        if visited_var_positional_param:
          # keyword-only (after var-positional)
          python_param_kind = inspect.Parameter.KEYWORD_ONLY
        else:
          # positional-or-keyword (before var-positional)
          python_param_kind = inspect.Parameter.POSITIONAL_OR_KEYWORD
      elif _is_hidden_seed(marker_type):
        # Strip hidden_seed params from the Python signature. These params will
        # be automatically populated by bind_arguments, and we don't want the
        # caller to accidentally pass in a value (which would be ignored).
        continue
      else:
        raise ValueError(f'unknown param marker type {marker_type}')

      params.append(
          inspect.Parameter(
              param.name, python_param_kind, default=python_param_default
          )
      )

    return inspect.Signature(params)  # Performs validation.

  def bind_arguments(
      self,
      signature: arolla.abc.Signature,
      *args: Any,
      **kwargs: Any,
  ) -> tuple[arolla.QValue | arolla.Expr, ...]:
    args_queue = collections.deque([as_qvalue_or_expr(arg) for arg in args])
    kwargs = {name: as_qvalue_or_expr(value) for name, value in kwargs.items()}

    bound_values: list[arolla.QValue | arolla.Expr] = []
    for param in signature.parameters:
      marker_type, default_value = _get_marker_type_and_default_value(param)

      if _is_positional_only(marker_type):
        if args_queue:
          bound_values.append(args_queue.popleft())
        elif default_value is not None:
          bound_values.append(default_value)
        else:
          raise TypeError(
              f"missing required positional argument: '{param.name}'"
          )
      elif _is_positional_or_keyword(marker_type) or marker_type is None:
        if args_queue:
          if param.name in kwargs:
            raise TypeError(f"got multiple values for argument '{param.name}'")
          bound_values.append(args_queue.popleft())
        elif param.name in kwargs:
          bound_values.append(kwargs.pop(param.name))
        elif default_value is not None:
          bound_values.append(default_value)
        else:
          raise TypeError(
              f"missing required positional argument: '{param.name}'"
          )
      elif _is_var_positional(marker_type):
        if all(isinstance(value, arolla.QValue) for value in args_queue):
          bound_values.append(arolla.tuple(*args_queue))
        else:
          bound_values.append(
              arolla.abc.bind_op('core.make_tuple', *map(as_expr, args_queue)))
        args_queue.clear()
      elif _is_keyword_only(marker_type):
        if param.name in kwargs:
          bound_values.append(kwargs.pop(param.name))
        elif default_value is not None:
          bound_values.append(default_value)
        else:
          raise TypeError(f"missing required keyword argument: '{param.name}'")
      elif _is_var_keyword(marker_type):
        if all(isinstance(value, arolla.QValue) for value in kwargs.values()):
          bound_values.append(arolla.namedtuple(**kwargs))
        else:
          bound_values.append(
              arolla.abc.bind_op(
                  'namedtuple.make',
                  arolla.text(','.join(kwargs.keys())),
                  *map(as_expr, kwargs.values()),
              )
          )
        kwargs.clear()
      elif _is_hidden_seed(marker_type):
        bound_values.append(
            arolla.M.math.add(HIDDEN_SEED_PLACEHOLDER, _random_int64())
        )
      else:
        raise TypeError(f'unknown param marker type {marker_type}')

    if args_queue:
      raise TypeError(
          f'expected {len(args) - len(args_queue)} positional arguments but '
          f'{len(args)} were given'
      )

    if kwargs:
      raise TypeError(
          f"got an unexpected keyword argument '{next(iter(kwargs.keys()))}'")

    return tuple(bound_values)

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
arolla.abc.register_aux_binding_policy(
    FULL_SIGNATURE_POLICY, _FullSignatureBindingPolicy()
)
arolla.abc.register_aux_binding_policy(OBJ_KWARGS_POLICY, _ObjectKwargsPolicy())
