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

"""Arolla expr evaluation using eager Koda operators, used in testing.

Evaluation works by:
  1. Literals are translated to corresponding Koda values.
  2. Operators are replaced by a "covering" Koda operator, which operates on
     Koda values.
  3. The result is translated from a Koda value into an Arolla value.

The Arolla -> Koda operator mapping is supplied through the `kd_op_mapping`
flag.
"""

import functools
import inspect
from typing import Any, Callable, Sequence

from arolla import arolla
from arolla.operator_tests import backend_test_base_flags
from koladata.exceptions import exceptions
from koladata.operators import eager_op_utils
from koladata.operators.tests.util import data_conversion
from koladata.types import data_slice
from koladata.types import jagged_shape

KodaValue = Any
# NOTE: We use eager operators instead of lazy operators since they implement a
# superset of the functionality of lazy operators. This way, we ensure that all
# paths are stress tested.
kd = eager_op_utils.operators_container('kd')


@functools.cache
def _get_kd_op_mapping() -> dict[str, str]:
  """Returns the flag mapping from arolla_op to kd_op."""
  res = {}
  for mapping in backend_test_base_flags.get_extra_flag('kd_op_mapping'):
    parts = mapping.split(':')
    if len(parts) != 2:
      raise ValueError(
          f'invalid `kd_op_mapping` value: {mapping!r}. Expected the form'
          ' `<arolla_op>:<kd_op>`'
      )
    arolla_op, kd_op = parts
    if arolla_op in res:
      raise ValueError(f'duplicate {arolla_op=} found in `kd_op_mapping`')
    res[arolla_op] = kd_op
  return res


def _make_into_fn(
    kd_op: Callable[..., KodaValue],
    agg_index: int,
    arg_indices_for_reshape: Sequence[int],
) -> Callable[..., KodaValue]:
  """Heuristically wraps `kd_op` into an `into` aggregational operator."""

  def fn(*args):
    shape = args[agg_index]
    args = list(args)
    if isinstance(shape, jagged_shape.JaggedShape):
      for arg_index in arg_indices_for_reshape:
        if args[arg_index].qtype != arolla.UNSPECIFIED:
          args[arg_index] = kd.reshape(args[arg_index], shape)
    else:
      assert shape.qtype == arolla.UNSPECIFIED
    args = [x for i, x in enumerate(args) if i != agg_index]
    return kd_op(*args)

  return fn


def _make_over_fn(
    kd_op: Callable[..., KodaValue],
    agg_index: int,
    arg_indices_for_reshape: Sequence[int],
) -> Callable[..., KodaValue]:
  """Heuristically wraps `kd_op` into an `over` aggregational operator."""

  def fn(*args):
    shape = args[agg_index]
    args = list(args)
    if isinstance(shape, jagged_shape.JaggedShape):
      for arg_index in arg_indices_for_reshape:
        if args[arg_index].qtype != arolla.UNSPECIFIED:
          args[arg_index] = kd.reshape(args[arg_index], shape)
    else:
      assert shape.qtype == arolla.UNSPECIFIED
    args = [x for i, x in enumerate(args) if i != agg_index]
    res = kd_op(*args)
    if kd.get_shape(res).rank() != kd.get_shape(args[0]).rank():
      raise ValueError('expected the output to have the same rank as the input')
    return kd.flatten(res)

  return fn


def _adapt_koda_op_for_unspecified_args(
    koda_op: Callable[..., KodaValue],
) -> Callable[..., KodaValue]:
  """Wraps the Koda op so that arolla.UNSPECIFIED args can be replaced with the op's defaults."""

  def get_final_args(*args) -> Sequence[KodaValue]:
    koda_op_signature = inspect.signature(koda_op)
    bound_arguments = koda_op_signature.bind(*args)
    bound_arguments.apply_defaults()
    arguments = bound_arguments.arguments
    result = []
    for param_name, param in koda_op_signature.parameters.items():
      param_default_value = param.default
      arg_value = arguments[param_name]
      if getattr(arg_value, 'qtype', None) == arolla.UNSPECIFIED and isinstance(
          param_default_value, data_slice.DataSlice
      ):
        result.append(param_default_value)
      elif param.kind == inspect.Parameter.VAR_POSITIONAL:
        result.extend(arg_value)
      else:
        result.append(arg_value)
    return result

  def fn(*args):
    final_args = get_final_args(*args)
    return koda_op(*final_args)

  fn.__signature__ = inspect.signature(koda_op)
  return fn


@functools.cache
def _get_eager_koda_op(
    arolla_op: arolla.types.RegisteredOperator,
) -> Callable[..., KodaValue]:
  """Returns an eager Koda operator corresponding to the provided arolla_op."""
  kd_op = _adapt_koda_op_for_unspecified_args(
      kd[_get_kd_op_mapping()[arolla_op.display_name]]
  )
  param_names = list(inspect.signature(arolla_op).parameters.keys())

  arg_indices_for_reshape = [
      param_names.index(arg)
      for arg in backend_test_base_flags.get_extra_flag('args_for_reshape')
  ]
  if not arg_indices_for_reshape:
    arg_indices_for_reshape.append(0)

  if 'into' in param_names:
    return _make_into_fn(
        kd_op, param_names.index('into'), arg_indices_for_reshape
    )
  elif 'over' in param_names:
    return _make_over_fn(
        kd_op, param_names.index('over'), arg_indices_for_reshape
    )
  else:
    return kd_op


def _type_annotate_expr(expr: arolla.Expr, /, **leaf_qvalues: arolla.QValue):
  qtype_annotated_leaves = {
      k: arolla.M.annotation.qtype(arolla.L[k], arolla.as_qvalue(v).qtype)
      for k, v in leaf_qvalues.items()
  }
  return arolla.sub_leaves(expr, **qtype_annotated_leaves)


def _get_output_qtype(
    expr: arolla.Expr, **leaf_qvalues: arolla.QValue
) -> arolla.QType:
  output_qtype = _type_annotate_expr(expr, **leaf_qvalues).qtype
  if output_qtype is None:
    raise ValueError('the output qtype could not be determined')
  return output_qtype


def _strip_annotations(expr: arolla.Expr) -> arolla.Expr:
  """Strips all annotations from the `expr`."""

  def fn(node: arolla.Expr, args: Sequence[arolla.Expr]) -> arolla.Expr:
    if node.op is None:
      return node
    elif arolla.abc.is_annotation_operator(node.op):
      return args[0]
    else:
      return node.op(*args)

  return arolla.abc.post_order_traverse(expr, fn)


def eager_eval(expr: Any, /, **leaf_values: Any) -> arolla.QValue:
  """Evaluates an Arolla expression using Koda backend."""
  leaf_qvalues = {k: arolla.as_qvalue(v) for k, v in leaf_values.items()}
  koda_values = {
      k: data_conversion.koda_from_arolla(v) for k, v in leaf_qvalues.items()
  }

  def fn(node: arolla.Expr, args: Sequence[KodaValue]) -> KodaValue:
    if node.is_leaf:
      try:
        return koda_values[node.leaf_key]
      except KeyError as e:
        raise ValueError(f'missing input for {node}') from e
    elif node.is_literal:
      return data_conversion.koda_from_arolla(node.qvalue)
    elif node.is_placeholder:
      raise ValueError('placeholders are not supported')
    else:
      koda_op = _get_eager_koda_op(node.op)
      try:
        return koda_op(*args)
      # Note that ValueError is expected in the Arolla tests and we need to
      # convert KodaError to ValueError.
      except exceptions.KodaError as e:
        raise ValueError(str(e)) from None
      except Exception as e:
        raise e

  expr = arolla.as_expr(expr)
  output_qtype = _get_output_qtype(expr, **leaf_qvalues)
  res = arolla.abc.post_order_traverse(_strip_annotations(expr), fn)
  return data_conversion.arolla_from_koda(res, output_qtype=output_qtype)
