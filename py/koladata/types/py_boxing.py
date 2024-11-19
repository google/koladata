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
import functools
import inspect
import random
import types as py_types
from typing import Any, Callable

from arolla import arolla
from koladata.expr import py_expr_eval_py_ext
from koladata.fstring import fstring as _fstring
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


# Default boxing policy wraps various values (other than lists and tuples) into
# DataItems. QValues remain unchanged (useful for DataBags, JaggedShapes and
# already-created DataSlices).
DEFAULT_BOXING_POLICY = 'koladata_default_boxing'

# Policy that does not wrap inputs to DataSlices.
DEFAULT_AROLLA_POLICY = ''

# The same as DEFAULT_BOXING_POLICY, but also implicitly wraps list/tuples into
# DataSlices.
LIST_TO_SLICE_BOXING_POLICY = 'koladata_list_to_slice_boxing'

FULL_SIGNATURE_POLICY = 'koladata_full_signature'
FSTR_POLICY = 'koladata_fstr'

# Select operators specific policies.
SELECT_POLICY = 'koladata_select'
SELECT_ITEMS_POLICY = 'koladata_select_items'
SELECT_KEYS_POLICY = 'koladata_select_keys'
SELECT_VALUES_POLICY = 'koladata_select_values'

# NOTE: Recreating this object invalidates all existing references. Thus after
# reloading this module, any Exprs using this codec must be recreated.
# If this causes issues, we'll need to find a workaround.
_REF_CODEC_OBJECT = arolla.types.PyObjectReferenceCodec()
REF_CODEC = _REF_CODEC_OBJECT.name


# Param markers used by FULL_SIGNATURE_POLICY. A param marker is an Arolla tuple
# of length 3 where the first element is `_PARAM_MARKER`, the second element is
# the marker type (`_*_MARKER_TYPE`), and the third element is a namedtuple of
# extra information, including the default value and boxing policy override.
_PARAM_MARKER = arolla.text('__koladata_param_marker__')

_POSITIONAL_ONLY_MARKER_TYPE = arolla.text('positional_only')
_POSITIONAL_OR_KEYWORD_MARKER_TYPE = arolla.text('positional_or_keyword')
_VAR_POSITIONAL_MARKER_TYPE = arolla.text('var_positional')
_KEYWORD_ONLY_MARKER_TYPE = arolla.text('keyword_only')
_VAR_KEYWORD_MARKER_TYPE = arolla.text('var_keyword')
_HIDDEN_SEED_MARKER_TYPE = arolla.text('hidden_seed')

_NO_DEFAULT_VALUE = object()


def _make_param_marker(
    marker_type: arolla.abc.QValue, **kwargs
) -> arolla.abc.QValue:
  """Builds a param marker tuple."""
  optional_fields = {}
  if 'boxing_policy' in kwargs:
    boxing_policy = kwargs.pop('boxing_policy')
    boxing_policy = arolla.text(boxing_policy)
    optional_fields['boxing_policy'] = boxing_policy
  if 'default_value' in kwargs:
    default_value = kwargs.pop('default_value')
    if default_value is not _NO_DEFAULT_VALUE:
      boxing_fn = _get_boxing_fn(optional_fields.get('boxing_policy'))
      optional_fields['default_value'] = boxing_fn(default_value)
  if kwargs:
    raise ValueError(f'unexpected param marker kwargs: {kwargs}')
  return arolla.tuple(
      _PARAM_MARKER, marker_type, arolla.namedtuple(**optional_fields)
  )


def is_param_marker(possible_param_marker: arolla.abc.QValue) -> bool:
  if not arolla.is_tuple_qtype(possible_param_marker.qtype):
    return False
  if possible_param_marker.field_count != 3:  # pytype: disable=attribute-error
    return False
  if possible_param_marker[0].fingerprint != _PARAM_MARKER.fingerprint:  # pytype: disable=unsupported-operands
    return False
  return True


def positional_only(
    default_value=_NO_DEFAULT_VALUE, *, boxing_policy=DEFAULT_BOXING_POLICY
) -> arolla.abc.QValue:
  """Marks a parameter as positional-only (with optional default)."""
  return _make_param_marker(
      _POSITIONAL_ONLY_MARKER_TYPE,
      default_value=default_value,
      boxing_policy=boxing_policy,
  )


def positional_or_keyword(
    default_value=_NO_DEFAULT_VALUE, *, boxing_policy=DEFAULT_BOXING_POLICY
) -> arolla.abc.QValue:
  """Marks a parameter as positional-or-keyword (with optional default)."""
  return _make_param_marker(
      _POSITIONAL_OR_KEYWORD_MARKER_TYPE,
      default_value=default_value,
      boxing_policy=boxing_policy,
  )


def var_positional(*, boxing_policy=DEFAULT_BOXING_POLICY) -> arolla.abc.QValue:
  """Marks a parameter as variadic-positional."""
  return _make_param_marker(
      _VAR_POSITIONAL_MARKER_TYPE, boxing_policy=boxing_policy
  )


def keyword_only(
    default_value=_NO_DEFAULT_VALUE, *, boxing_policy=DEFAULT_BOXING_POLICY
) -> arolla.abc.QValue:
  """Marks a parameter as keyword-only (with optional default)."""
  return _make_param_marker(
      _KEYWORD_ONLY_MARKER_TYPE,
      default_value=default_value,
      boxing_policy=boxing_policy,
  )


def var_keyword(*, boxing_policy=DEFAULT_BOXING_POLICY) -> arolla.abc.QValue:
  """Marks a parameter as variadic-keyword."""
  return _make_param_marker(
      _VAR_KEYWORD_MARKER_TYPE, boxing_policy=boxing_policy
  )


def hidden_seed() -> arolla.abc.QValue:
  """Marks a parameter as a hidden seed parameter."""
  return _make_param_marker(_HIDDEN_SEED_MARKER_TYPE)


def _unpack_param_marker(param: arolla.abc.SignatureParameter) -> tuple[
    arolla.abc.QValue | None,
    arolla.abc.QValue | None,
    collections.abc.Callable[[Any], arolla.abc.QValue | arolla.abc.Expr],
]:
  """Unpacks an Arolla expr signature param into a tuple of values (see below).

  Args:
    param: Signature parameter.

  Returns:
    (marker_type, default_value, boxing_fn)
    marker_type: If this is a marker, the marker type, else None.
    default_value: The default value for this param, regardless of whether it
      is a marker. If None, the param has no default value (so it is required).
    boxing_fn: A function that "boxes" an arbitrary python value passed to the
      python invocation of the operator into a QValue or QExpr to be passed to
      the bound operator.
  """
  param_default = param.default
  if param_default is None:
    return None, None, as_qvalue_or_expr
  if not is_param_marker(param_default):
    return None, param_default, as_qvalue_or_expr
  try:
    default_value = param_default[2]['default_value']  # pytype: disable=unsupported-operands
  except KeyError:
    default_value = None
  try:
    boxing_policy_name = param_default[2]['boxing_policy']  # pytype: disable=unsupported-operands
  except KeyError:
    boxing_policy_name = None
  boxing_fn = _get_boxing_fn(boxing_policy_name)
  return param_default[1], default_value, boxing_fn  # pytype: disable=unsupported-operands


def is_positional_only(marker_type: arolla.abc.QValue | None) -> bool:
  return (
      marker_type is not None
      and marker_type.fingerprint == _POSITIONAL_ONLY_MARKER_TYPE.fingerprint
  )


def is_positional_or_keyword(marker_type: arolla.abc.QValue | None) -> bool:
  return (
      marker_type is not None
      and marker_type.fingerprint
      == _POSITIONAL_OR_KEYWORD_MARKER_TYPE.fingerprint
  )


def is_var_positional(marker_type: arolla.abc.QValue | None) -> bool:
  return (
      marker_type is not None
      and marker_type.fingerprint == _VAR_POSITIONAL_MARKER_TYPE.fingerprint
  )


def is_keyword_only(marker_type: arolla.abc.QValue | None) -> bool:
  return (
      marker_type is not None
      and marker_type.fingerprint == _KEYWORD_ONLY_MARKER_TYPE.fingerprint
  )


def is_var_keyword(marker_type: arolla.abc.QValue | None) -> bool:
  return (
      marker_type is not None
      and marker_type.fingerprint == _VAR_KEYWORD_MARKER_TYPE.fingerprint
  )


def is_hidden_seed(marker_type: arolla.abc.QValue | None) -> bool:
  return (
      marker_type is not None
      and marker_type.fingerprint == _HIDDEN_SEED_MARKER_TYPE.fingerprint
  )


def find_hidden_seed_param(signature: inspect.Signature) -> int | None:
  """Returns the index of the hidden seed param, or None if there isn't one."""
  for i_param, param in enumerate(signature.parameters.values()):
    if (
        isinstance(param.default, arolla.QValue)
        and is_param_marker(param.default)
        and is_hidden_seed(param.default[1])
    ):
      return i_param
  return None


HIDDEN_SEED_LEAF = arolla.abc.leaf(py_expr_eval_py_ext.HIDDEN_SEED_LEAF_KEY)


# Isolated PRNG instance. Global to avoid overhead of repeated instantiation.
_RANDOM = random.Random()


def _random_int64() -> arolla.QValue:
  return arolla.int64(_RANDOM.randint(-(2**63), 2**63 - 1))


def with_unique_hidden_seed(expr: arolla.Expr) -> arolla.Expr:
  return arolla.sub_by_fingerprint(
      expr,
      {HIDDEN_SEED_LEAF.fingerprint: arolla.literal(_random_int64())},
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
  if isinstance(arg, tuple):
    tpl = tuple(as_qvalue_or_expr(v) for v in arg)
    if arolla.Expr in (type(v) for v in tpl):
      return arolla.abc.bind_op(
          'kde.tuple.make_tuple', *(as_expr(v) for v in tpl)
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
  if isinstance(arg, (py_types.FunctionType, functools.partial)):
    return arolla.abc.PyObject(arg, codec=REF_CODEC)
  if arg is data_slice.DataSlice:
    return data_slice.DataSlice.from_vals(None)
  if arg is data_bag.DataBag:
    return data_bag.DataBag.empty()
  return data_slice.DataSlice.from_vals(arg)


def as_qvalue_or_expr_with_list_to_slice_support(
    arg: Any,
) -> arolla.Expr | arolla.QValue:
  if isinstance(arg, list):
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


def is_non_deterministic_op(op: arolla.abc.RegisteredOperator) -> bool:
  """Returns True if the operator is non-deterministic."""
  sig = arolla.abc.get_operator_signature(op)
  for param in sig.parameters:
    marker_type, _, _ = _unpack_param_marker(param)
    if is_hidden_seed(marker_type):
      return True
  return False


_BOXING_FNS_BY_NAME_FINGERPRINT = {
    arolla.text(DEFAULT_BOXING_POLICY).fingerprint: as_qvalue_or_expr,
    arolla.text(DEFAULT_AROLLA_POLICY).fingerprint: lambda x: x,
    arolla.text(
        LIST_TO_SLICE_BOXING_POLICY
    ).fingerprint: as_qvalue_or_expr_with_list_to_slice_support,
}


def _get_boxing_fn(
    boxing_policy_name: arolla.QValue | None,
) -> collections.abc.Callable[[Any], arolla.abc.QValue | arolla.abc.Expr]:
  if boxing_policy_name is None:
    return as_qvalue_or_expr  # Default boxing policy.
  if boxing_policy_name.fingerprint in _BOXING_FNS_BY_NAME_FINGERPRINT:
    return _BOXING_FNS_BY_NAME_FINGERPRINT[boxing_policy_name.fingerprint]
  raise ValueError(f'unknown boxing policy: {boxing_policy_name}')


class BasicBindingPolicy(arolla.abc.AuxBindingPolicy):
  """A base class for binding policies with the Koladata method make_literal."""

  def make_literal(self, value: arolla.QValue) -> arolla.Expr:
    return literal_operator.literal(value)


class _FullSignatureBindingPolicy(BasicBindingPolicy):
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
    be removed from the Python signature, and will be populated with a unique
    Arolla int64 value on every invocation.

  The implied Python signature must be a valid Python signature (`**kwargs`
  must be last, args after `*` must be keyword-only, etc.)

  This policy *also* applies default Koda value boxing, including to the values
  in the tuple/namedtuple passed to *args/**kwargs. If any markers that support
  the optional `boxing_policy` argument have that argument set, this behavior is
  overridden for the marked parameters.
  """

  def make_python_signature(
      self, signature: arolla.abc.Signature
  ) -> inspect.Signature:
    params = []
    visited_var_positional_param = False
    for param in signature.parameters:
      if param.kind != 'positional-or-keyword':
        raise ValueError(
            'only positional-or-keyword arguments are supported in the '
            'underlying Expr signature'
        )

      marker_type, default_value, unused_boxing_fn = _unpack_param_marker(param)
      python_param_default = (
          default_value
          if default_value is not None
          else inspect.Parameter.empty
      )

      if is_positional_only(marker_type):
        python_param_kind = inspect.Parameter.POSITIONAL_ONLY
      elif is_positional_or_keyword(marker_type):
        python_param_kind = inspect.Parameter.POSITIONAL_OR_KEYWORD
      elif is_var_positional(marker_type):
        if visited_var_positional_param:
          # This is not enforced by inspect.Signature for some reason.
          raise ValueError('multiple variadic positional arguments')
        python_param_kind = inspect.Parameter.VAR_POSITIONAL
        visited_var_positional_param = True
      elif is_keyword_only(marker_type):
        python_param_kind = inspect.Parameter.KEYWORD_ONLY
      elif is_var_keyword(marker_type):
        python_param_kind = inspect.Parameter.VAR_KEYWORD
      elif marker_type is None:
        if visited_var_positional_param:
          # keyword-only (after var-positional)
          python_param_kind = inspect.Parameter.KEYWORD_ONLY
        else:
          # positional-or-keyword (before var-positional)
          python_param_kind = inspect.Parameter.POSITIONAL_OR_KEYWORD
      elif is_hidden_seed(marker_type):
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
    args_queue = collections.deque(args)

    bound_values: list[arolla.QValue | arolla.Expr] = []
    for param in signature.parameters:
      marker_type, default_value, boxing_fn = _unpack_param_marker(param)

      if is_positional_only(marker_type):
        if args_queue:
          bound_values.append(boxing_fn(args_queue.popleft()))
        elif default_value is not None:
          bound_values.append(default_value)
        else:
          raise TypeError(
              f"missing required positional argument: '{param.name}'"
          )
      elif is_positional_or_keyword(marker_type) or marker_type is None:
        if args_queue:
          if param.name in kwargs:
            raise TypeError(f"got multiple values for argument '{param.name}'")
          bound_values.append(boxing_fn(args_queue.popleft()))
        elif param.name in kwargs:
          bound_values.append(boxing_fn(kwargs.pop(param.name)))
        elif default_value is not None:
          bound_values.append(default_value)
        else:
          raise TypeError(
              f"missing required positional argument: '{param.name}'"
          )
      elif is_var_positional(marker_type):
        boxed_args = [boxing_fn(value) for value in args_queue]
        if all(isinstance(value, arolla.QValue) for value in boxed_args):
          bound_values.append(arolla.tuple(*boxed_args))
        else:
          bound_values.append(
              arolla.abc.bind_op('core.make_tuple', *map(as_expr, boxed_args))
          )
        args_queue.clear()
      elif is_keyword_only(marker_type):
        if param.name in kwargs:
          bound_values.append(boxing_fn(kwargs.pop(param.name)))
        elif default_value is not None:
          bound_values.append(default_value)
        else:
          raise TypeError(f"missing required keyword argument: '{param.name}'")
      elif is_var_keyword(marker_type):
        boxed_kwargs = {
            name: boxing_fn(value) for name, value in kwargs.items()
        }
        if all(
            isinstance(value, arolla.QValue) for value in boxed_kwargs.values()
        ):
          bound_values.append(arolla.namedtuple(**boxed_kwargs))
        else:
          bound_values.append(
              arolla.abc.bind_op(
                  'namedtuple.make',
                  arolla.text(','.join(boxed_kwargs.keys())),
                  *map(as_expr, boxed_kwargs.values()),
              )
          )
        kwargs.clear()
      elif is_hidden_seed(marker_type):
        bound_values.append(
            arolla.M.math.add(HIDDEN_SEED_LEAF, _random_int64())
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
          f"got an unexpected keyword argument '{next(iter(kwargs.keys()))}'"
      )

    return tuple(bound_values)


class _FstrBindingPolicy(BasicBindingPolicy):
  """Argument binding policy for Koda fstr operator.

  This policy takes f-string with base64 encoded Expressions and converts
  to the format expression. This single expression is passed as argument to the
  identity kde.strings.fstr operator.

  Example (actual expression may be different):
    kd.fstr(f'Hello {I.x:s}') ->
    kd.fstr(kd.format('Hello {x:s}', x=I.x))
  """

  def make_python_signature(
      self, signature: arolla.abc.Signature
  ) -> inspect.Signature:
    assert len(signature.parameters) == 1
    return inspect.Signature(
        [inspect.Parameter('fstr', inspect.Parameter.POSITIONAL_ONLY)]
    )

  def bind_arguments(
      self, signature: arolla.abc.Signature, *args: Any, **kwargs: Any
  ) -> tuple[arolla.QValue | arolla.Expr, ...]:
    if len(args) != 1:
      raise TypeError(f'expected a single positional argument, got {len(args)}')
    if kwargs:
      raise TypeError(f'no kwargs are allowed, got {kwargs}')
    fstr = args[0]
    if not isinstance(fstr, str):
      raise TypeError(f'expected a string, got {fstr}')
    return (_fstring.fstr_expr(fstr),)


class _SelectBindingPolicy(_FullSignatureBindingPolicy):
  """Argument binding policy for Koda select.* operators.

  It checks if the `fltr` argument is a Python function and invokes it on
  the `ds` argument if yes.

  NOTE: To use the policy, the arguments of the operator must have `ds` and
  `fltr` as their names.
  """

  def __init__(self, compute_on_fn: Callable[[Any], Any]):
    super().__init__()
    self._compute_on_fn = compute_on_fn

  def make_python_signature(
      self, signature: arolla.abc.Signature
  ) -> inspect.Signature:
    if len(signature.parameters) < 2:
      raise ValueError(
          'expected a signature with at least 2 parameters, got '
          f'{len(signature.parameters)}'
      )
    return super().make_python_signature(signature)

  def bind_arguments(
      self, signature: arolla.abc.Signature, *args: Any, **kwargs: Any
  ) -> tuple[arolla.QValue | arolla.Expr, ...]:
    """Returns arguments bound to parameters.

    This method does customization evaluation specific to the select.*
    operators. It checks if the `fltr` argument is a Python function and invokes
    it on the `ds` argument if yes.

    Args:
      signature: The "classic" operator signature.
      *args: The positional arguments.
      **kwargs: The keyword arguments.
    """
    args_queue = collections.deque(args)

    def get_arg(name):
      if args_queue:
        return args_queue.popleft()
      else:
        return kwargs.pop(name)

    ds = get_arg('ds')
    fltr = get_arg('fltr')
    if isinstance(fltr, py_types.FunctionType):
      fltr = fltr(self._compute_on_fn(ds))
    args = [ds, fltr] + list(args_queue)
    return super().bind_arguments(signature, *args, **kwargs)


##### Kola Data policy registrations #####

arolla.abc.register_classic_aux_binding_policy_with_custom_boxing(
    DEFAULT_BOXING_POLICY,
    as_qvalue_or_expr,
    make_literal_fn=literal_operator.literal,
)
arolla.abc.register_classic_aux_binding_policy_with_custom_boxing(
    LIST_TO_SLICE_BOXING_POLICY,
    as_qvalue_or_expr_with_list_to_slice_support,
    make_literal_fn=literal_operator.literal,
)
arolla.abc.register_aux_binding_policy(
    FULL_SIGNATURE_POLICY, _FullSignatureBindingPolicy()
)
arolla.abc.register_aux_binding_policy(FSTR_POLICY, _FstrBindingPolicy())
arolla.abc.register_aux_binding_policy(
    SELECT_POLICY, _SelectBindingPolicy(lambda x: x)
)
arolla.abc.register_aux_binding_policy(
    SELECT_KEYS_POLICY, _SelectBindingPolicy(lambda x: x.get_keys())
)
arolla.abc.register_aux_binding_policy(
    SELECT_VALUES_POLICY, _SelectBindingPolicy(lambda x: x.get_values())
)
arolla.abc.register_aux_binding_policy(
    SELECT_ITEMS_POLICY, _SelectBindingPolicy(lambda x: x[:])
)
