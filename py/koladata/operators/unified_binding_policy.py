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

"""The unified binding policy for Koladata operators."""

import inspect
from typing import Any

from arolla import arolla
from koladata.types import py_boxing

_as_qvalue_or_expr = py_boxing.as_qvalue_or_expr
_as_qvalue = py_boxing.as_qvalue
_as_expr = py_boxing.as_expr
_non_deterministic_leaf = py_boxing.NON_DETERMINISTIC_TOKEN_LEAF


UNIFIED_POLICY = 'koladata_unified_binding_policy'
UNIFIED_POLICY_PREFIX = f'{UNIFIED_POLICY}:'

NON_DETERMINISTIC_PARAM_NAME = '_non_deterministic_token'
NON_DETERMINISTIC_PARAM = arolla.abc.placeholder(NON_DETERMINISTIC_PARAM_NAME)

# Marker for a variadic-positional parameter
_VAR_POSITIONAL = type('VarPositional', (), {})()
_VAR_KEYWORD = type('VarKeyword', (), {})()

# Precomputed values
_EMPTY_TUPLE = arolla.tuple()
_EMPTY_NAMEDTUPLE = arolla.namedtuple()


def var_positional():
  """Returns a marker a variadic-positional parameter.

  This marker should be used as the default value for the last
  positional-or-keyword parameter.
  """
  return _VAR_POSITIONAL


def var_keyword():
  """Returns a marker a variadic-keyword parameter.

  This marker should be used as the default value for the last keyword-only
  parameter.
  """
  return _VAR_KEYWORD


def make_unified_signature(
    signature: inspect.Signature, *, deterministic: bool
) -> arolla.abc.Signature:
  """Returns an operator signature with a unified binding policy.

  Args:
    signature: An `inspect.Signature` object representing the expected python
      signature.
    deterministic: If set to False, a hidden parameter (with the name
      `NON_DETERMINISTIC_PARAM_NAME`) is added to the end of the signature. This
      parameter receives special handling by the binding policy implementation.

  Returns:
    arolla.abc.Signature: An operator signature with the unified binding
    policy applied.
  """
  sig_spec = []
  sig_vals = []
  aux_opts = []
  for param in signature.parameters.values():
    # Perform a sanity check on the special marker values.
    if param.default is _VAR_POSITIONAL:
      if param.kind != param.POSITIONAL_OR_KEYWORD:
        raise ValueError(
            'the marker var_positional() can only be used with'
            ' a keyword-or-positional parameter'
        )
    elif param.default is _VAR_KEYWORD:
      if param.kind != param.KEYWORD_ONLY:
        raise ValueError(
            'the marker var_keyword() can only be used with'
            ' a keyword-only parameter'
        )

    if param.kind == param.POSITIONAL_ONLY:
      if param.default is param.empty:
        sig_spec.append(param.name)
        aux_opts.append('_')
      else:
        sig_spec.append(param.name + '=')
        sig_vals.append(param.default)
        aux_opts.append('_')
    elif param.kind == param.POSITIONAL_OR_KEYWORD:
      if param.default is _VAR_POSITIONAL:
        sig_spec.append(param.name + '=')
        sig_vals.append(_EMPTY_TUPLE)
        aux_opts.append('P')
      elif param.default is param.empty:
        sig_spec.append(param.name)
        aux_opts.append('p')
      else:
        sig_spec.append(param.name + '=')
        sig_vals.append(param.default)
        aux_opts.append('p')
    elif param.kind == param.KEYWORD_ONLY:
      if param.default is _VAR_KEYWORD:
        sig_spec.append(param.name + '=')
        sig_vals.append(_EMPTY_NAMEDTUPLE)
        aux_opts.append('K')
      elif param.default is param.empty:
        sig_spec.append(param.name + '=')
        sig_vals.append(arolla.unspecified())
        aux_opts.append('k')
      else:
        sig_spec.append(param.name + '=')
        sig_vals.append(param.default)
        aux_opts.append('d')
    elif param.kind == param.VAR_POSITIONAL:
      raise ValueError(
          f'a signature with `*{param.name}` is not supported; please use'
          f' `{param.name}=var_positional()` instead.'
      )
    elif param.kind == param.VAR_KEYWORD:
      raise ValueError(
          f'a signature with `**{param.name}` is not supported; please use'
          f' `*, {param.name}=var_keyword()` instead.'
      )
    else:
      raise ValueError(f'unsupported parameter: {param}')

  if not deterministic:
    sig_spec.append(NON_DETERMINISTIC_PARAM_NAME + '=')
    sig_vals.append(arolla.unspecified())
    aux_opts.append('H')
  aux_opts = ''.join(aux_opts)

  # Perform a sanity check on the parameter order.
  if aux_opts.count('P') > 1:
    raise ValueError('only one var_positional() is allowed')
  if 'K' in aux_opts[: deterministic - 2]:
    raise ValueError('arguments cannot follow var-keyword argument')
  if 'Pp' in aux_opts:
    raise ValueError(
        'a keyword-or-positional parameter cannot appear after'
        ' a variadic-positional parameter'
    )
  return arolla.abc.make_operator_signature(  # pytype: disable=bad-return-type
      (
          ','.join(sig_spec) + f'|{UNIFIED_POLICY_PREFIX}{aux_opts}',
          *sig_vals,
      ),
      as_qvalue=_as_qvalue,
  )


def _as_qvalue_or_expr_tuple(
    args: tuple[Any, ...],
) -> arolla.QValue | arolla.Expr:
  if not args:
    return _EMPTY_TUPLE
  args = tuple(map(_as_qvalue_or_expr, args))
  if any(isinstance(arg, arolla.Expr) for arg in args):
    return arolla.abc.make_operator_node(
        'core.make_tuple', tuple(map(_as_expr, args))
    )
  return arolla.tuple(*args)


# TODO: b/381852425 - Consider Koladata's versions of operators
# for make_tuple and make_namedtuple.
def _as_qvalue_or_expr_namedtuple(
    kwargs: dict[str, Any],
) -> arolla.QValue | arolla.Expr:
  if not kwargs:
    return _EMPTY_NAMEDTUPLE
  args = tuple(map(_as_qvalue_or_expr, kwargs.values()))
  if any(isinstance(arg, arolla.Expr) for arg in args):
    return arolla.abc.make_operator_node(
        'namedtuple.make',
        (arolla.text(','.join(kwargs)), *map(_as_expr, args)),
    )
  return arolla.namedtuple(**dict(zip(kwargs, args)))


_MISSING_SENTINEL = object()


class UnifiedBindingPolicy(py_boxing.BasicBindingPolicy):
  """Unified Binding Policy.

  This is a binding policy for Koladata operators, adding support for
  positional-only, keyword-only, and variadic keyword parameters.

  It encodes additional information about parameters in signature.aux_policy
  using the following format:

     koladata_unified_binding_policy:<options>

  Each character in '<options>' represents a parameter and encodes its kind:

    `_`-- positional-only parameter
    `p`-- positional-or-keyword parameter
    `P`-- variadic-positional (*args)
    `k`-- keyword-only, no default
    `d`-- keyword-only, with default value
    `K`-- variadic-keyword (**kwargs)
    `H`-- non-deterministic input
  """

  @staticmethod
  def make_python_signature(
      signature: arolla.abc.Signature,
  ) -> inspect.Signature:
    opts = signature.aux_policy.removeprefix(UNIFIED_POLICY_PREFIX)
    result_params = []
    for i, param in enumerate(signature.parameters):
      opt = opts[i]
      if opt == '_':
        if param.default is None:
          result_params.append(
              inspect.Parameter(param.name, inspect.Parameter.POSITIONAL_ONLY)
          )
        else:
          result_params.append(
              inspect.Parameter(
                  param.name,
                  inspect.Parameter.POSITIONAL_ONLY,
                  default=param.default,
              )
          )
      elif opt == 'p':
        if param.default is None:
          result_params.append(
              inspect.Parameter(
                  param.name, inspect.Parameter.POSITIONAL_OR_KEYWORD
              )
          )
        else:
          result_params.append(
              inspect.Parameter(
                  param.name,
                  inspect.Parameter.POSITIONAL_OR_KEYWORD,
                  default=param.default,
              )
          )
      elif opt == 'P':
        result_params.append(
            inspect.Parameter(param.name, inspect.Parameter.VAR_POSITIONAL)
        )
      elif opt == 'k':
        result_params.append(
            inspect.Parameter(param.name, inspect.Parameter.KEYWORD_ONLY)
        )
      elif opt == 'd':
        result_params.append(
            inspect.Parameter(
                param.name,
                inspect.Parameter.KEYWORD_ONLY,
                default=param.default,
            )
        )
      elif opt == 'K':
        result_params.append(
            inspect.Parameter(param.name, inspect.Parameter.VAR_KEYWORD)
        )
      elif opt == 'H':
        continue
      else:
        raise RuntimeError(f'unexpected option={opt!r}, param={param.name!r}')
    return inspect.Signature(parameters=result_params)

  @staticmethod
  def bind_arguments(
      signature: arolla.abc.Signature, *args: Any, **kwargs: Any
  ) -> list[arolla.QValue | arolla.Expr]:
    params = signature.parameters
    opts = signature.aux_policy.removeprefix(UNIFIED_POLICY_PREFIX)
    opts_len = len(opts)
    args_len = len(args)
    assert len(params) == opts_len
    result = [None] * opts_len
    i = 0

    # Bind the positional parameters using `args`.
    while i < args_len and i < opts_len:
      opt = opts[i]
      if opt == '_':
        result[i] = _as_qvalue_or_expr(args[i])
      elif opt == 'p':
        if params[i].name in kwargs:
          raise TypeError(f'multiple values for argument {params[i].name!r}')
        result[i] = _as_qvalue_or_expr(args[i])
      else:
        break
      i += 1
    if i < opts_len and opts[i] == 'P':
      result[i] = _as_qvalue_or_expr_tuple(args[i:])
      has_unprocessed_args = False
      i += 1
    else:
      has_unprocessed_args = len(args) > i

    # Bind remaining parameters using `kwargs` and the default values.
    missing_positional_params = []
    missing_keyword_only_params = []
    for j in range(i, opts_len):
      opt = opts[j]
      param = params[j]
      if opt == '_':
        if param.default is None:
          missing_positional_params.append(param.name)
        else:
          result[j] = param.default
      elif opt == 'p':
        value = kwargs.pop(param.name, _MISSING_SENTINEL)
        if value is _MISSING_SENTINEL:
          if param.default is None:
            missing_positional_params.append(param.name)
          else:
            result[j] = param.default
        else:
          result[j] = _as_qvalue_or_expr(value)
      elif opt == 'P':
        result[j] = _EMPTY_TUPLE
      elif opt == 'k':
        value = kwargs.pop(param.name, _MISSING_SENTINEL)
        if value is _MISSING_SENTINEL:
          missing_keyword_only_params.append(param.name)
        else:
          result[j] = _as_qvalue_or_expr(value)
      elif opt == 'd':
        value = kwargs.pop(param.name, _MISSING_SENTINEL)
        if value is _MISSING_SENTINEL:
          result[j] = param.default
        else:
          result[j] = _as_qvalue_or_expr(value)
      elif opt == 'K':
        result[j] = _as_qvalue_or_expr_namedtuple(kwargs)
        kwargs.clear()
      elif opt == 'H':
        result[j] = py_boxing.new_non_deterministic_token()
      else:
        raise RuntimeError(f'unexpected option={opt!r}, param={param.name!r}')

    # Report for missing arguments.
    if missing_positional_params:
      if len(missing_positional_params) == 1:
        raise TypeError(
            'missing 1 required positional argument:'
            f' {missing_positional_params[0]!r}'
        )
      raise TypeError(
          f'missing {len(missing_positional_params)} required positional'
          f' arguments: {", ".join(map(repr, missing_positional_params[:-1]))}'
          f' and {missing_positional_params[-1]!r}'
      )
    if missing_keyword_only_params:
      if len(missing_keyword_only_params) == 1:
        raise TypeError(
            'missing 1 required keyword-only argument:'
            f' {missing_keyword_only_params[0]!r}'
        )
      raise TypeError(
          f'missing {len(missing_keyword_only_params)} required keyword-only'
          ' arguments:'
          f' {", ".join(map(repr, missing_keyword_only_params[:-1]))} and'
          f' {missing_keyword_only_params[-1]!r}'
      )

    if has_unprocessed_args:
      count_positionals = opts.count('_') + opts.count('p')
      count_no_defaults = sum(param.default is None for param in params)
      if count_positionals == count_no_defaults:
        if count_positionals == 1:
          raise TypeError(
              f'takes 1 positional argument but {len(args)} were given'
          )
        raise TypeError(
            f'takes {count_positionals} positional arguments but'
            f' {len(args)} were given'
        )
      raise TypeError(
          f'takes from {count_no_defaults} to {count_positionals} positional'
          f' arguments but {len(args)} were given'
      )
    if kwargs:
      raise TypeError(f'an unexpected keyword argument: {next(iter(kwargs))!r}')
    return result  # pytype: disable=bad-return-type


_make_tuple_op = arolla.abc.decay_registered_operator('core.make_tuple')
_make_namedtuple_op = arolla.abc.decay_registered_operator('namedtuple.make')


def _is_make_tuple_op(op: arolla.abc.Operator | None) -> bool:
  return (
      op is not None
      and arolla.abc.decay_registered_operator(op) == _make_tuple_op
  )


def _is_make_namedtuple_op(op: arolla.abc.Operator | None) -> bool:
  return (
      op is not None
      and arolla.abc.decay_registered_operator(op) == _make_namedtuple_op
  )


def _unified_op_repr_var_positional(
    var_positional_node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> list[str]:
  """Repr for varargs. Assumes node is a tuple (op or qvalue)."""
  if _is_make_tuple_op(var_positional_node.op):
    return [tokens[dep].text for dep in var_positional_node.node_deps]
  if isinstance(var_positional_node.qvalue, arolla.types.Tuple):
    return [repr(v) for v in var_positional_node.qvalue]
  token = tokens[var_positional_node]
  if token.precedence.left < 0:
    return [f'*{token.text}']
  return [f'*({token.text})']


def _unified_op_repr_var_keyword(
    var_keyword_node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> list[str]:
  """Repr for varkwargs. Assumes node is a namedtuple (op or qvalue)."""
  if _is_make_namedtuple_op(var_keyword_node.op):
    keys = var_keyword_node.node_deps[0].qvalue.py_value().split(',')
    values = (tokens[dep] for dep in var_keyword_node.node_deps[1:])
    return [f'{k.strip()}={v.text}' for k, v in zip(keys, values)]
  if isinstance(var_keyword_node.qvalue, arolla.types.NamedTuple):
    return [f'{k}={v!r}' for k, v in var_keyword_node.qvalue.as_dict().items()]
  token = tokens[var_keyword_node]
  if token.precedence.left < 0:
    return [f'**{token.text}']
  return [f'**({token.text})']


# Note: We pass `node_op` and `node_op_signature` directly only to avoid
# retrieving them again, saving a few cycles.
def unified_op_repr(
    node: arolla.Expr,
    node_op: arolla.abc.Operator,
    node_op_signature: arolla.abc.Signature,
    tokens: arolla.abc.NodeTokenView,
) -> arolla.abc.ReprToken:
  """Repr function for Koda operators with UNIFIED_BINDING_POLICY aux policy."""
  opts = node_op_signature.aux_policy.removeprefix(UNIFIED_POLICY_PREFIX)
  node_deps = node.node_deps
  node_dep_reprs = []
  for i, param in enumerate(node_op_signature.parameters):
    opt = opts[i]
    dep = node_deps[i]
    if opt == '_' or opt == 'p':
      node_dep_reprs.append(tokens[dep].text)
    elif opt == 'P':
      node_dep_reprs.extend(_unified_op_repr_var_positional(dep, tokens))
    elif opt == 'k' or opt == 'd':
      node_dep_reprs.append(f'{param.name}={tokens[dep].text}')
    elif opt == 'K':
      node_dep_reprs.extend(_unified_op_repr_var_keyword(dep, tokens))
    elif opt == 'H':
      pass
    else:
      raise RuntimeError(f'unexpected option={opt!r}, param={param.name!r}')
  res = arolla.abc.ReprToken()
  res.text = f'{node_op.display_name}({", ".join(node_dep_reprs)})'
  return res


arolla.abc.register_aux_binding_policy(UNIFIED_POLICY, UnifiedBindingPolicy())
