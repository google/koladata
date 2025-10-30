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

"""Custom operator representations."""

import typing
from typing import Callable

from arolla import arolla
from koladata.operators import unified_binding_policy
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import qtypes


ReprToken = arolla.abc.ReprToken
OperatorReprFn = Callable[[arolla.Expr, arolla.abc.NodeTokenView], ReprToken]

SRC_PIN = '📍'


def default_op_repr(
    node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> ReprToken:
  """Default repr function for Koda operators."""
  op = node.op
  assert op is not None
  signature = arolla.abc.get_operator_signature(op)
  if signature.aux_policy.startswith(
      unified_binding_policy.UNIFIED_POLICY_PREFIX
  ):
    return unified_binding_policy.unified_op_repr(node, op, signature, tokens)
  res = ReprToken()
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
) -> ReprToken:
  """Repr for kd.slices.subslice."""
  parts = [
      _slice_repr(dep, tokens, abbreviation=False) for dep in node.node_deps
  ]
  res = ReprToken()
  res.text = f'{node.op.display_name}({", ".join(parts)})'
  return res


def subslicehelper_repr(
    node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> ReprToken:
  """Repr for expr.S[...]."""
  ds_repr = tokens[node.node_deps[0]].text
  parts = [
      _slice_repr(dep, tokens, abbreviation=True) for dep in node.node_deps[1:]
  ]
  res = ReprToken()
  res.text = f'{ds_repr}.S[{", ".join(parts)}]'
  return res


def get_item_repr(
    node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> ReprToken:
  """Repr for kd.core.get_item."""
  deps = node.node_deps
  assert len(deps) == 2, 'get_item expects exact two arguments.'
  x = tokens[deps[0]].text
  key_or_index = _slice_repr(deps[1], tokens, abbreviation=True)
  res = ReprToken()
  res.text = f'{x}[{key_or_index}]'
  return res


def call_repr(node: arolla.Expr, tokens: arolla.abc.NodeTokenView) -> ReprToken:
  """Repr for kd.functor.call."""
  res = ReprToken()
  deps = node.node_deps
  assert len(deps) == 5, 'call expects exactly five arguments.'
  func_repr = tokens[deps[0]].text
  args = deps[1]

  if args.op == arolla.M.core.make_tuple:
    # make_tuple node
    args_repr = ', '.join([tokens[arg].text for arg in args.node_deps])
  elif (
      not isinstance(args.op, literal_operator.LiteralOperator)
      or not isinstance(arolla.eval(args), arolla.types.Tuple)
  ):
    # Fall back to the default repr.
    return default_op_repr(node, tokens)
  else:
    # tuple literal
    args_repr = tokens[args].text.removeprefix('(').removesuffix(')')
  kwargs = deps[3]
  if kwargs.op == arolla.M.namedtuple.make:
    # namedtuple.make node
    # The first node_dep is a coma separated string of kwarg names.
    kwarg_names = (
        tokens[kwargs.node_deps[0]].text.lstrip("'").rstrip("'").split(',')
    )
    # The following node deps are the kwarg values.
    kwarg_values = [tokens[n].text for n in kwargs.node_deps[1:]]
    kwargs_repr = ', '.join(
        [f'{k}={v}' for (k, v) in zip(kwarg_names, kwarg_values)]
    )
  elif not isinstance(
      kwargs.op, literal_operator.LiteralOperator
  ) or not isinstance(arolla.eval(kwargs), arolla.types.NamedTuple):
    # Fall back to the default repr.
    return default_op_repr(node, tokens)
  else:
    # Named tuple literal.
    # We can get the kwarg names from the qtype.
    kwarg_names = arolla.types.get_namedtuple_field_names(kwargs.qtype)
    kwargs_repr = ', '.join([
        f'{name}='
        f'{repr(arolla.eval(arolla.M.namedtuple.get_field(kwargs, name)))}'
        for name in kwarg_names
    ])
  return_type_as = deps[2]
  is_default_return_type = isinstance(
      return_type_as.op, literal_operator.LiteralOperator
  ) and (arolla.eval(return_type_as).qtype == qtypes.DATA_SLICE)
  if not is_default_return_type:
    if kwargs_repr:
      pass
      kwargs_repr = (
          f'{kwargs_repr}, return_type_as={tokens[return_type_as].text}'
      )
    else:
      kwargs_repr = f'return_type_as={tokens[return_type_as].text}'
  # deps[4] is the non-deterministic token and does not need to be included into
  # the repr.
  if args_repr and kwargs_repr:
    res.text = f'{func_repr}({args_repr}, {kwargs_repr})'
  else:
    res.text = f'{func_repr}({args_repr}{kwargs_repr})'
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
) -> ReprToken:
  """Repr for kd.core.get_attr."""
  res = ReprToken()
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
  res.precedence = ReprToken.PRECEDENCE_OP_SUBSCRIPTION
  res.text = (
      f'{_brackets_if(obj.text, obj.precedence.right >= res.precedence.left)}'
      f'.{py_attr}'
  )
  return res


def hide_non_deterministic_repr_fn(
    node: arolla.Expr, tokens: arolla.abc.NodeTokenView
) -> ReprToken:
  """Repr for kd.dicts.new."""
  op = node.op
  assert op is not None
  res = ReprToken()
  # Hide the last argument (non-deterministic token) from the repr.
  dep_txt = ', '.join(tokens[node].text for node in node.node_deps[:-1])
  res.text = f'{node.op.display_name}({dep_txt})'
  return res


def _make_prefix_repr_fn(
    symbol: str,
    precedence: ReprToken.Precedence,
) -> OperatorReprFn:
  """Returns a custom repr function for a prefix operator."""

  def repr_fn(node: arolla.Expr, tokens: arolla.abc.NodeTokenView) -> ReprToken:
    res = ReprToken()
    res.precedence = precedence
    token = tokens[node.node_deps[0]]
    res.text = symbol + _brackets_if(
        token.text, token.precedence.left >= precedence.right
    )
    return res

  return repr_fn


def _make_infix_repr_fn(
    symbol: str,
    precedence: ReprToken.Precedence,
) -> OperatorReprFn:
  """Returns a custom repr function for an infix operator."""

  def repr_fn(node: arolla.Expr, tokens: arolla.abc.NodeTokenView) -> ReprToken:
    res = ReprToken()
    res.precedence = precedence
    lhs_t, rhs_t = tokens[node.node_deps[0]], tokens[node.node_deps[1]]
    lhs_res = _brackets_if(
        lhs_t.text, lhs_t.precedence.right >= precedence.left
    )
    rhs_res = _brackets_if(
        rhs_t.text, rhs_t.precedence.left >= precedence.right
    )
    res.text = f'{lhs_res} {symbol} {rhs_res}'
    return res

  return repr_fn


# Prefix operators.
pos_repr = _make_prefix_repr_fn('+', ReprToken.PRECEDENCE_OP_UNARY)
neg_repr = _make_prefix_repr_fn('-', ReprToken.PRECEDENCE_OP_UNARY)
not_repr = _make_prefix_repr_fn('~', ReprToken.PRECEDENCE_OP_UNARY)

# Infix operators.
pow_repr = _make_infix_repr_fn('**', ReprToken.PRECEDENCE_OP_POW)
multiply_repr = _make_infix_repr_fn('*', ReprToken.PRECEDENCE_OP_MUL)
divide_repr = _make_infix_repr_fn('/', ReprToken.PRECEDENCE_OP_MUL)
floordiv_repr = _make_infix_repr_fn('//', ReprToken.PRECEDENCE_OP_MUL)
mod_repr = _make_infix_repr_fn('%', ReprToken.PRECEDENCE_OP_MUL)
add_repr = _make_infix_repr_fn('+', ReprToken.PRECEDENCE_OP_ADD)
subtract_repr = _make_infix_repr_fn('-', ReprToken.PRECEDENCE_OP_ADD)
lshift_repr = _make_infix_repr_fn('<<', ReprToken.PRECEDENCE_OP_SHIFT)
rshift_repr = _make_infix_repr_fn('>>', ReprToken.PRECEDENCE_OP_SHIFT)
apply_mask_repr = _make_infix_repr_fn('&', ReprToken.PRECEDENCE_OP_AND)
coalesce_repr = _make_infix_repr_fn('|', ReprToken.PRECEDENCE_OP_OR)
less_repr = _make_infix_repr_fn('<', ReprToken.PRECEDENCE_OP_COMPARISON)
less_equal_repr = _make_infix_repr_fn('<=', ReprToken.PRECEDENCE_OP_COMPARISON)
greater_repr = _make_infix_repr_fn('>', ReprToken.PRECEDENCE_OP_COMPARISON)
greater_equal_repr = _make_infix_repr_fn(
    '>=', ReprToken.PRECEDENCE_OP_COMPARISON
)
equal_repr = _make_infix_repr_fn('==', ReprToken.PRECEDENCE_OP_COMPARISON)
not_equal_repr = _make_infix_repr_fn('!=', ReprToken.PRECEDENCE_OP_COMPARISON)
