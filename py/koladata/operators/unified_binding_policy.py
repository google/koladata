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

from arolla import arolla
from koladata.operators import py_optools_py_ext
from koladata.types import py_boxing

UNIFIED_POLICY_PREFIX = f'{py_optools_py_ext.UNIFIED_POLICY}:'

# Name of the hidden parameter used to indicate non-deterministic input.
NON_DETERMINISTIC_PARAM_NAME = '_non_deterministic_token'

# Placeholder for the hidden parameter.
NON_DETERMINISTIC_PARAM = arolla.abc.placeholder(NON_DETERMINISTIC_PARAM_NAME)

# Note: The options must match the C++ implementation.
#
_OPT_CHR_POSITIONAL_ONLY = py_optools_py_ext.UNIFIED_POLICY_OPT_POSITIONAL_ONLY
_OPT_CHR_POSITIONAL_OR_KEYWORD = (
    py_optools_py_ext.UNIFIED_POLICY_OPT_POSITIONAL_OR_KEYWORD
)
_OPT_CHR_VAR_POSITIONAL = py_optools_py_ext.UNIFIED_POLICY_OPT_VAR_POSITIONAL
_OPT_CHR_REQUIRED_KEYWORD_ONLY = (
    py_optools_py_ext.UNIFIED_POLICY_OPT_REQUIRED_KEYWORD_ONLY
)
_OPT_CHR_OPTIONAL_KEYWORD_ONLY = (
    py_optools_py_ext.UNIFIED_POLICY_OPT_OPTIONAL_KEYWORD_ONLY
)
_OPT_CHR_VAR_KEYWORD = py_optools_py_ext.UNIFIED_POLICY_OPT_VAR_KEYWORD
_OPT_CHR_NON_DETERMINISTIC = (
    py_optools_py_ext.UNIFIED_POLICY_OPT_NON_DETERMINISTIC
)


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
        aux_opts.append(_OPT_CHR_POSITIONAL_ONLY)
      else:
        sig_spec.append(param.name + '=')
        sig_vals.append(param.default)
        aux_opts.append(_OPT_CHR_POSITIONAL_ONLY)
    elif param.kind == param.POSITIONAL_OR_KEYWORD:
      if param.default is _VAR_POSITIONAL:
        sig_spec.append(param.name + '=')
        sig_vals.append(_EMPTY_TUPLE)
        aux_opts.append(_OPT_CHR_VAR_POSITIONAL)
      elif param.default is param.empty:
        sig_spec.append(param.name)
        aux_opts.append(_OPT_CHR_POSITIONAL_OR_KEYWORD)
      else:
        sig_spec.append(param.name + '=')
        sig_vals.append(param.default)
        aux_opts.append(_OPT_CHR_POSITIONAL_OR_KEYWORD)
    elif param.kind == param.KEYWORD_ONLY:
      if param.default is _VAR_KEYWORD:
        sig_spec.append(param.name + '=')
        sig_vals.append(_EMPTY_NAMEDTUPLE)
        aux_opts.append(_OPT_CHR_VAR_KEYWORD)
      elif param.default is param.empty:
        sig_spec.append(param.name + '=')
        sig_vals.append(arolla.unspecified())
        aux_opts.append(_OPT_CHR_REQUIRED_KEYWORD_ONLY)
      else:
        sig_spec.append(param.name + '=')
        sig_vals.append(param.default)
        aux_opts.append(_OPT_CHR_OPTIONAL_KEYWORD_ONLY)
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
    aux_opts.append(_OPT_CHR_NON_DETERMINISTIC)
  aux_opts = ''.join(aux_opts)

  # Perform a sanity check on the parameter order.
  if aux_opts.count(_OPT_CHR_VAR_POSITIONAL) > 1:
    raise ValueError('only one var_positional() is allowed')
  if _OPT_CHR_VAR_KEYWORD in aux_opts[: deterministic - 2]:
    raise ValueError('arguments cannot follow var-keyword argument')
  if 'Pp' in aux_opts:
    raise ValueError(
        'a keyword-or-positional parameter cannot appear after'
        ' a variadic-positional parameter'
    )
  return arolla.abc.make_operator_signature(  # pytype: disable=bad-return-type
      (','.join(sig_spec) + f'|{UNIFIED_POLICY_PREFIX}{aux_opts}', *sig_vals),
      as_qvalue=py_boxing.as_qvalue,
  )


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
    if opt == _OPT_CHR_POSITIONAL_ONLY or opt == _OPT_CHR_POSITIONAL_OR_KEYWORD:
      node_dep_reprs.append(tokens[dep].text)
    elif opt == _OPT_CHR_VAR_POSITIONAL:
      node_dep_reprs.extend(_unified_op_repr_var_positional(dep, tokens))
    elif (
        opt == _OPT_CHR_REQUIRED_KEYWORD_ONLY
        or opt == _OPT_CHR_OPTIONAL_KEYWORD_ONLY
    ):
      node_dep_reprs.append(f'{param.name}={tokens[dep].text}')
    elif opt == _OPT_CHR_VAR_KEYWORD:
      node_dep_reprs.extend(_unified_op_repr_var_keyword(dep, tokens))
    elif opt == _OPT_CHR_NON_DETERMINISTIC:
      pass
    else:
      raise RuntimeError(f'unexpected option={opt!r}, param={param.name!r}')
  res = arolla.abc.ReprToken()
  res.text = f'{node_op.display_name}({", ".join(node_dep_reprs)})'
  return res
