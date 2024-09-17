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

"""Operator definition and registration tooling."""

import functools
import inspect
from typing import Any, Callable, Collection, Sequence

from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view as view_lib
from koladata.operators import op_repr
from koladata.types import py_boxing


P = input_container.InputContainer('P')


def add_to_registry(
    name: str | None = None,
    *,
    aliases: Collection[str] = (),
    unsafe_override: bool = False,
    view: type[arolla.abc.ExprView] = view_lib.DataSliceView,
    repr_fn: op_repr.OperatorReprFn = op_repr.default_op_repr,
):
  """Wrapper around Arolla's add_to_registry with Koda functionality.

  Args:
    name: Optional name of the operator. Otherwise, inferred from the op.
    aliases: Optional aliases for the operator.
    unsafe_override: Whether to override an existing operator.
    view: Optional view to use for the operator.
    repr_fn: Optional repr function to use for the operator and its aliases.

  Returns:
    Registered operator.
  """

  def impl(op: arolla.types.Operator) -> arolla.types.RegisteredOperator:
    def _register_op(op, name):
      registered_op = arolla.optools.add_to_registry(
          name, unsafe_override=unsafe_override
      )(op)
      arolla.abc.set_expr_view_for_registered_operator(
          registered_op.display_name, view
      )
      arolla.abc.register_op_repr_fn_by_registration_name(
          registered_op.display_name, repr_fn
      )
      return registered_op

    for alias in aliases:
      _register_op(op, alias)
    return _register_op(op, name)

  return impl


def as_backend_operator(
    name: str,
    *,
    qtype_inference_expr: arolla.Expr | arolla.QType,
    qtype_constraints: Sequence[tuple[arolla.Expr, str]] = (),
    aux_policy: str | None = None,
) -> Callable[[Callable[..., Any]], arolla.types.BackendOperator]:
  """Wrapper around Arolla as_backend_operator with Koda default aux policy."""
  if aux_policy is None:
    aux_policy = py_boxing.DEFAULT_BOXING_POLICY
  return arolla.optools.as_backend_operator(
      name=name,
      qtype_inference_expr=qtype_inference_expr,
      qtype_constraints=qtype_constraints,
      experimental_aux_policy=aux_policy,
  )


def as_lambda_operator(
    name: str,
    *,
    qtype_constraints: Sequence[tuple[arolla.Expr, str]] = (),
    aux_policy: str = py_boxing.DEFAULT_BOXING_POLICY,
) -> Callable[[Callable[..., Any]], arolla.types.RestrictedLambdaOperator]:
  """Wrapper around Arolla as_lambda_operator with additional Koda specifics.

  Koda specifics:
    - Adds a DataSliceView to the inputs during tracing, allowing prefix / infix
      notation.

  Args:
    name: Name of the operator.
    qtype_constraints: QType constraints for the operator.
    aux_policy: Aux policy for the operator.

  Returns:
    A decorator that constructs a lambda operator by tracing a python function.
  """

  def impl(fn):

    @arolla.optools.as_lambda_operator(
        name=name,
        qtype_constraints=qtype_constraints,
        experimental_aux_policy=aux_policy,
    )
    @functools.wraps(fn)  # preserves the `fn` signature.
    def fn_wrapper(*args):
      koda_placeholders = [P[arolla_p.placeholder_key] for arolla_p in args]
      placeholder_subs = {
          koda_p.fingerprint: arolla_p
          for koda_p, arolla_p in zip(koda_placeholders, args)
      }

      # If there is a `py_boxing.hidden_seed()`-marked param on the `fn`
      # signature, use its value for the `py_boxing.HIDDEN_SEED_PLACEHOLDER`
      # placeholder.
      if aux_policy == py_boxing.FULL_SIGNATURE_POLICY:
        hidden_seed_param_index = py_boxing.find_hidden_seed_param(
            inspect.signature(fn)
        )
        if hidden_seed_param_index is not None:
          placeholder_subs[py_boxing.HIDDEN_SEED_PLACEHOLDER.fingerprint] = (
              args[hidden_seed_param_index]
          )

      body_expr = fn(*koda_placeholders)
      return arolla.sub_by_fingerprint(body_expr, placeholder_subs)

    return fn_wrapper

  return impl


def equiv_to_op(
    this_op: arolla.types.Operator | str, that_op: arolla.types.Operator | str
) -> bool:
  """Returns true iff the impl of `this_op` equals the impl of `that_op`."""
  this_op = arolla.abc.decay_registered_operator(this_op)
  that_op = arolla.abc.decay_registered_operator(that_op)
  return this_op == that_op
