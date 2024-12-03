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

"""Custom operator representations."""

import typing
from typing import Callable

from arolla import arolla
from koladata.operators import unified_binding_policy
from koladata.types import data_slice
from koladata.types import py_boxing


OperatorReprFn = Callable[
    [arolla.Expr, arolla.abc.NodeTokenView], arolla.abc.ReprToken
]


def _full_signature_varargs_repr(
    node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> str:
  """Repr for varargs. Assumes node is a tuple (op or qvalue)."""
  if node.qvalue is None:
    if arolla.abc.decay_registered_operator(
        node.op
    ) != arolla.abc.decay_registered_operator('core.make_tuple'):
      return tokens[node].text
    return ', '.join(tokens[dep].text for dep in node.node_deps)
  else:
    if not isinstance(node.qvalue, arolla.types.Tuple):
      return tokens[node].text
    return ', '.join(repr(v) for v in node.qvalue)


def _full_signature_varkwargs_repr(
    node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> str:
  """Repr for varkwargs. Assumes node is a namedtuple (op or qvalue)."""
  # pytype: disable=attribute-error
  if node.qvalue is None:
    if arolla.abc.decay_registered_operator(
        node.op
    ) != arolla.abc.decay_registered_operator('namedtuple.make'):
      return tokens[node].text
    keys = node.node_deps[0].qvalue.py_value().split(',')
    values = (tokens[dep] for dep in node.node_deps[1:])
    return ', '.join(f'{k}={v.text}' for k, v in zip(keys, values))
  else:
    if not isinstance(node.qvalue, arolla.types.NamedTuple):
      return tokens[node].text
    return ', '.join(f'{k}={v!r}' for k, v in node.qvalue.as_dict().items())
  # pytype: enable=attribute-error


# Note: We pass `node_op` and `node_op_signature` directly only to avoid
# retrieving them again, saving a few cycles.
def _full_signature_op_repr(
    node: arolla.Expr,
    node_op: arolla.abc.Operator,
    node_op_signature: arolla.abc.Signature,
    tokens: arolla.abc.NodeTokenView,
) -> arolla.abc.ReprToken:
  """Repr function for Koda operators with FULL_SIGNATURE_POLICY aux policy."""
  node_dep_reprs = []
  for dep, param in zip(
      node.node_deps, node_op_signature.parameters, strict=True
  ):
    if not isinstance(
        param.default, arolla.abc.QValue
    ) or not py_boxing.is_param_marker(param.default):
      node_dep_reprs.append(tokens[dep].text)
    elif py_boxing.is_non_deterministic(param.default[1]):
      continue
    elif py_boxing.is_var_positional(param.default[1]):
      if repr_ := _full_signature_varargs_repr(dep, tokens):
        node_dep_reprs.append(repr_)
    elif py_boxing.is_var_keyword(param.default[1]):
      if repr_ := _full_signature_varkwargs_repr(dep, tokens):
        node_dep_reprs.append(repr_)
    elif py_boxing.is_keyword_only(param.default[1]):
      node_dep_reprs.append(f'{param.name}={tokens[dep].text}')
    else:
      node_dep_reprs.append(tokens[dep].text)
  res = arolla.abc.ReprToken()
  dep_txt = ', '.join(node_dep_reprs)
  res.text = f'{node_op.display_name}({dep_txt})'
  return res


def default_op_repr(
    node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> arolla.abc.ReprToken:
  """Default repr function for Koda operators."""
  op = node.op
  assert op is not None
  signature = arolla.abc.get_operator_signature(op)
  if signature.aux_policy == py_boxing.FULL_SIGNATURE_POLICY:
    return _full_signature_op_repr(node, op, signature, tokens)
  if signature.aux_policy.startswith(
      unified_binding_policy.UNIFIED_POLICY_PREFIX
  ):
    return unified_binding_policy.unified_op_repr(node, op, signature, tokens)
  res = arolla.abc.ReprToken()
  dep_txt = ', '.join(tokens[node].text for node in node.node_deps)
  res.text = f'{node.op.display_name}({dep_txt})'
  return res


def _slice_repr(
    node: arolla.Expr, tokens: arolla.abc.NodeTokenView, abbreviation: bool
):
  """Repr for slice."""
  default_arg_repr = '' if abbreviation else 'None'
  fmt = '{start}:{stop}' if abbreviation else 'slice({start}, {stop})'
  if (s := node.qvalue) is not None and isinstance(s, arolla.types.Slice):
    get_field = (
        lambda f: default_arg_repr if f.qtype == arolla.UNSPECIFIED else repr(f)
    )
    # Note step is not supported.
    return fmt.format(start=get_field(s.start), stop=get_field(s.stop))
  else:
    return tokens[node].text


def subslice_repr(
    node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> arolla.abc.ReprToken:
  """Repr for kde.core.subslice."""
  parts = [
      _slice_repr(dep, tokens, abbreviation=False) for dep in node.node_deps
  ]
  res = arolla.abc.ReprToken()
  res.text = f'{node.op.display_name}({", ".join(parts)})'
  return res


def subslicehelper_repr(
    node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> arolla.abc.ReprToken:
  """Repr for expr.S[...]."""
  ds_repr = tokens[node.node_deps[0]].text
  parts = [
      _slice_repr(dep, tokens, abbreviation=True) for dep in node.node_deps[1:]
  ]
  res = arolla.abc.ReprToken()
  res.text = f'{ds_repr}.S[{", ".join(parts)}]'
  return res


def get_item_repr(
    node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> arolla.abc.ReprToken:
  """Repr for kde.core.get_item."""
  deps = node.node_deps
  assert len(deps) == 2, 'get_item expects exact two arguments.'
  x = tokens[deps[0]].text
  key_or_index = _slice_repr(deps[1], tokens, abbreviation=True)
  res = arolla.abc.ReprToken()
  res.text = f'{x}[{key_or_index}]'
  return res


def _is_identifier(s: str) -> bool:
  if not s:
    return False
  if s[0] != '_' and not s[0].isalpha():
    return False
  return all(c.isalnum() or c == '_' for c in s)


def _brackets_if(text: str, condition: bool) -> str:
  return f'({text})' if condition else text


def getattr_repr(
    node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> arolla.abc.ReprToken:
  """Repr for kde.core.get_attr."""
  res = arolla.abc.ReprToken()
  if node.node_deps[2].qtype != arolla.UNSPECIFIED:
    return default_op_repr(node, tokens)
  if node.node_deps[1].qvalue is None or not isinstance(
      node.node_deps[1].qvalue, data_slice.DataSlice
  ):
    return default_op_repr(node, tokens)
  py_attr = typing.cast(
      data_slice.DataSlice, node.node_deps[1].qvalue
  ).internal_as_py()
  if not isinstance(py_attr, str) or not _is_identifier(py_attr):
    return default_op_repr(node, tokens)
  obj = tokens[node.node_deps[0]]
  res.precedence.left = 0
  res.precedence.right = -1
  res.text = (
      f'{_brackets_if(obj.text, obj.precedence.right >= res.precedence.left)}'
      f'.{py_attr}'
  )
  return res


def _make_prefix_repr_fn(
    symbol: str, left_precedence: int, right_precedence: int
) -> OperatorReprFn:
  """Returns a custom repr function for a prefix operator."""

  def repr_fn(
      node: arolla.Expr, tokens: arolla.abc.NodeTokenView
  ) -> arolla.abc.ReprToken:
    res = arolla.abc.ReprToken()
    res.precedence.left = left_precedence
    res.precedence.right = right_precedence
    token = tokens[node.node_deps[0]]
    res.text = symbol + _brackets_if(
        token.text, token.precedence.left >= right_precedence
    )
    return res

  return repr_fn


def _make_infix_repr_fn(
    symbol: str, left_precedence: int, right_precedence: int
) -> OperatorReprFn:
  """Returns a custom repr function for an infix operator."""

  def repr_fn(
      node: arolla.Expr, tokens: arolla.abc.NodeTokenView
  ) -> arolla.abc.ReprToken:
    res = arolla.abc.ReprToken()
    res.precedence.left = left_precedence
    res.precedence.right = right_precedence
    lhs_t, rhs_t = tokens[node.node_deps[0]], tokens[node.node_deps[1]]
    lhs_res = _brackets_if(
        lhs_t.text, lhs_t.precedence.right >= left_precedence
    )
    rhs_res = _brackets_if(
        rhs_t.text, rhs_t.precedence.left >= right_precedence
    )
    res.text = f'{lhs_res} {symbol} {rhs_res}'
    return res

  return repr_fn
#
# Prefix operators.
pos_repr = _make_prefix_repr_fn('+', 1, 1)
neg_repr = _make_prefix_repr_fn('-', 1, 1)
not_repr = _make_prefix_repr_fn('~', 1, 1)

# Infix operators.
pow_repr = _make_infix_repr_fn('**', 1, 2)
multiply_repr = _make_infix_repr_fn('*', 3, 2)
divide_repr = _make_infix_repr_fn('/', 3, 2)
floordiv_repr = _make_infix_repr_fn('//', 3, 2)
mod_repr = _make_infix_repr_fn('%', 3, 2)
add_repr = _make_infix_repr_fn('+', 5, 4)
subtract_repr = _make_infix_repr_fn('-', 5, 4)
apply_mask_repr = _make_infix_repr_fn('&', 7, 6)
coalesce_repr = _make_infix_repr_fn('|', 9, 8)
less_repr = _make_infix_repr_fn('<', 10, 10)
less_equal_repr = _make_infix_repr_fn('<=', 10, 10)
greater_repr = _make_infix_repr_fn('>', 10, 10)
greater_equal_repr = _make_infix_repr_fn('>=', 10, 10)
equal_repr = _make_infix_repr_fn('==', 10, 10)
not_equal_repr = _make_infix_repr_fn('!=', 10, 10)
