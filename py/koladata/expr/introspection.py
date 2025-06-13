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

"""Tools to introspect and manipulate Exprs."""

from typing import Any

from arolla import arolla
from koladata.expr import input_container
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import mask_constants
from koladata.types import py_boxing
from koladata.types import schema_constants

I = input_container.InputContainer('I')
_KODA_INPUT_OP = arolla.abc.lookup_operator('koda_internal.input')

ExprLike = Any  # Expr-convertible type.


def get_name(expr: arolla.Expr) -> str | None:
  """Returns the name of the given Expr, or None if it does not have one."""
  return arolla.abc.read_name_annotation(expr)


def unwrap_named(expr: arolla.Expr) -> arolla.Expr:
  """Unwraps a named Expr, raising if it is not named."""
  if arolla.abc.read_name_annotation(expr) is None:
    raise ValueError('trying to remove the name from a non-named Expr')
  return expr.node_deps[0]


def pack_expr(expr: arolla.Expr) -> data_slice.DataSlice:
  """Packs the given Expr into a DataItem."""
  return data_slice.DataSlice.from_vals(arolla.quote(expr))


def unpack_expr(ds: data_slice.DataSlice) -> arolla.Expr:
  """Unpacks an Expr stored in a DataItem."""
  if (
      ds.get_ndim() != 0
      or ds.get_schema() != schema_constants.EXPR
      or ds.get_present_count() == 0
  ):
    raise ValueError('only present EXPR DataItems can be unpacked')
  return ds.internal_as_py().unquote()


def is_packed_expr(ds: Any) -> data_slice.DataSlice:
  """Returns kd.present if the argument is a DataItem containing an Expr."""
  if (
      isinstance(ds, data_item.DataItem)
      and ds.get_schema() == schema_constants.EXPR
      and ds.get_present_count() > 0
  ):
    return mask_constants.present
  else:
    return mask_constants.missing


def get_input_names(
    expr: arolla.Expr, container: input_container.InputContainer = I
) -> list[str]:
  """Returns names of `container` inputs used in `expr`."""
  input_names = []
  for node in arolla.abc.post_order(expr):
    if (
        input_name := input_container.get_input_name(node, container)
    ) is not None:
      input_names.append(input_name)
  return sorted(input_names)


def is_input(expr: arolla.Expr) -> bool:
  """Returns True if `expr` is an input `I`."""
  return (
      expr.op == _KODA_INPUT_OP and
      expr.node_deps[0].qvalue.py_value() == 'I'
  )


def is_variable(expr: arolla.Expr) -> bool:
  """Returns True if `expr` is a variable `V`."""
  return (
      expr.op == _KODA_INPUT_OP and
      expr.node_deps[0].qvalue.py_value() == 'V'
  )


def is_literal(expr: arolla.Expr) -> bool:
  """Returns True if `expr` is a Koda Literal."""
  return isinstance(expr.op, literal_operator.LiteralOperator)


def sub_inputs(
    expr: arolla.Expr,
    container: input_container.InputContainer = I,
    /,
    **subs: ExprLike,
) -> arolla.Expr:
  """Returns an expression with `container` inputs replaced with Expr(s)."""
  subs = {
      container[k].fingerprint: py_boxing.as_expr(v) for k, v in subs.items()
  }
  return arolla.sub_by_fingerprint(expr, subs)


def sub_by_name(expr: arolla.Expr, /, **subs: ExprLike) -> arolla.Expr:
  """Returns `expr` with named subexpressions replaced.

  Use `kde.with_name(expr, name)` to create a named subexpression.

  Example:
    foo = kde.with_name(I.x, 'foo')
    bar = kde.with_name(I.y, 'bar')
    expr = foo + bar
    kd.sub_by_name(expr, foo=I.z)
    # -> I.z + kde.with_name(I.y, 'bar')

  Args:
    expr: an expression.
    **subs: mapping from subexpression name to replacement node.
  """
  subs = {k: py_boxing.as_expr(v) for k, v in subs.items()}
  return arolla.sub_by_name(expr, **subs)


def sub(
    expr: arolla.Expr, *subs: ExprLike | tuple[arolla.Expr, ExprLike]
) -> arolla.Expr:
  """Returns `expr` with provided expressions replaced.

  Example usage:
    kd.sub(expr, (from_1, to_1), (from_2, to_2), ...)

  For the special case of a single substitution, you can also do:
    kd.sub(expr, from, to)

  It does the substitution by traversing 'expr' post-order and comparing
  fingerprints of sub-Exprs in the original expression and those in in 'subs'.
  For example,

    kd.sub(I.x + I.y, (I.x, I.z), (I.x + I.y, I.k)) -> I.k

    kd.sub(I.x + I.y, (I.x, I.y), (I.y + I.y, I.z)) -> I.y + I.y

  It does not do deep transformation recursively. For example,

    kd.sub(I.x + I.y, (I.x, I.z), (I.y, I.x)) -> I.z + I.x

  Args:
    expr: Expr which substitutions are applied to
    *subs: Either zero or more (sub_from, sub_to) tuples, or exactly two
      arguments from and to. The keys should be expressions, and the values
      should be possible to convert to expressions using kd.as_expr.

  Returns:
    A new Expr with substitutions.
  """
  if (
      len(subs) == 2
      and isinstance(subs[0], arolla.Expr)
  ):
    subs = (subs,)
  else:
    for tpl in subs:
      if not (
          isinstance(tpl, tuple)
          and len(tpl) == 2
          and isinstance(tpl[0], arolla.Expr)
      ):
        raise ValueError(
            'either all subs must be two-element tuples of Expressions, or'
            ' there must be exactly two non-tuple subs representing a single'
            f' substitution, got: {subs}'
        )
  subs = {f.fingerprint: py_boxing.as_expr(t) for f, t in subs}
  return arolla.sub_by_fingerprint(expr, subs)
