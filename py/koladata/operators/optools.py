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

import dataclasses
import inspect
import types
from typing import Any, Callable, Collection

from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view as view_lib
from koladata.operators import op_repr
from koladata.types import py_boxing


P = input_container.InputContainer('P')


@dataclasses.dataclass(frozen=True)
class _RegisteredOp:
  op: arolla.types.Operator
  view: type[arolla.abc.ExprView] | None
  repr_fn: op_repr.OperatorReprFn


_REGISTERED_OPS: dict[str, _RegisteredOp] = {}


def _clear_registered_ops():
  _REGISTERED_OPS.clear()


arolla.abc.cache_clear_callbacks.add(_clear_registered_ops)


def add_to_registry(
    name: str | None = None,
    *,
    aliases: Collection[str] = (),
    unsafe_override: bool = False,
    view: type[arolla.abc.ExprView] | None = view_lib.KodaView,
    repr_fn: op_repr.OperatorReprFn = op_repr.default_op_repr,
):
  """Wrapper around Arolla's add_to_registry with Koda functionality.

  Args:
    name: Optional name of the operator. Otherwise, inferred from the op.
    aliases: Optional aliases for the operator.
    unsafe_override: Whether to override an existing operator.
    view: Optional view to use for the operator. If None, the default arolla
      ExprView will be used.
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

    op_name = name or op.display_name
    _REGISTERED_OPS[op_name] = _RegisteredOp(op, view, repr_fn)

    for alias in aliases:
      _register_op(op, alias)
      _REGISTERED_OPS[alias] = _RegisteredOp(op, view, repr_fn)
    return _register_op(op, name)

  return impl


def add_to_registry_as_overloadable(
    name: str,
    *,
    unsafe_override: bool = False,
    view: type[arolla.abc.ExprView] | None = view_lib.KodaView,
    repr_fn: op_repr.OperatorReprFn = op_repr.default_op_repr,
    aux_policy: str = py_boxing.DEFAULT_BOXING_POLICY,
):
  """Koda wrapper around Arolla's add_to_registry_as_overloadable.

  Performs additional Koda-specific registration, such as setting the view and
  repr function.

  Args:
    name: Name of the operator.
    unsafe_override: Whether to override an existing operator.
    view: Optional view to use for the operator.
    repr_fn: Optional repr function to use for the operator and its aliases.
    aux_policy: Aux policy for the operator.

  Returns:
    An overloadable registered operator.
  """

  def impl(fn) -> arolla.types.RegisteredOperator:
    overloadable_op = arolla.optools.add_to_registry_as_overloadable(
        name,
        unsafe_override=unsafe_override,
        experimental_aux_policy=aux_policy,
    )(fn)
    arolla.abc.set_expr_view_for_registered_operator(
        overloadable_op.display_name, view
    )
    arolla.abc.register_op_repr_fn_by_registration_name(
        overloadable_op.display_name, repr_fn
    )
    _REGISTERED_OPS[overloadable_op.display_name] = _RegisteredOp(
        overloadable_op, view, repr_fn
    )
    return overloadable_op

  return impl


def add_to_registry_as_overload(
    name: str | None = None,
    *,
    overload_condition_expr: Any,
    unsafe_override: bool = False,
):
  """Koda wrapper around Arolla's add_to_registry_as_overload.

  Note that for e.g. `name = "foo.bar.baz"`, the wrapped operator will
  be registered as an overload `"baz"` of the overloadable operator `"foo.bar"`.

  Performs no additional Koda-specific registration.

  Args:
    name: Optional name of the operator. Otherwise, inferred from the op.
    overload_condition_expr: Condition for the overload.
    unsafe_override: Whether to override an existing operator.

  Returns:
    A decorator that registers an overload for the operator with the
    corresponding name. Returns the original operator (unlinke the arolla
    equivalent).
  """

  def impl(op: arolla.types.Operator) -> arolla.types.Operator:
    _ = arolla.optools.add_to_registry_as_overload(
        name,
        unsafe_override=unsafe_override,
        overload_condition_expr=overload_condition_expr,
    )(op)
    return op

  return impl


def add_alias(name: str, alias: str):
  registered_op = _REGISTERED_OPS.get(name)
  if registered_op is None:
    raise ValueError(f'Operator {name} is not registered.')
  add_to_registry(
      alias, view=registered_op.view, repr_fn=registered_op.repr_fn
  )(registered_op.op)


def _build_operator_signature_from_fn(
    fn: types.FunctionType, aux_policy: str
) -> arolla.abc.Signature:
  """Builds an operator signature from a python function."""
  signature = arolla.abc.make_operator_signature(
      inspect.signature(fn), as_qvalue=py_boxing.as_qvalue
  )
  return arolla.abc.Signature((signature.parameters, aux_policy))


def as_backend_operator(
    name: str,
    *,
    qtype_inference_expr: arolla.Expr | arolla.QType,
    qtype_constraints: arolla.types.QTypeConstraints = (),
    aux_policy: str = py_boxing.DEFAULT_BOXING_POLICY,
) -> Callable[[types.FunctionType], arolla.types.BackendOperator]:
  """A decorator for defining Koladata-specific backend operators."""

  def impl(fn):
    return arolla.types.BackendOperator(
        name,
        _build_operator_signature_from_fn(fn, aux_policy),
        doc=inspect.getdoc(fn) or '',
        qtype_inference_expr=qtype_inference_expr,
        qtype_constraints=qtype_constraints,
    )

  return impl


def _build_lambda_body_from_fn(fn: types.FunctionType):
  """Builds a lambda body expression from a python function."""
  unmangling = {}

  def gen_tracer(name: str):
    result = P[name]
    unmangling[result.fingerprint] = arolla.abc.placeholder(name)
    return result

  return arolla.abc.sub_by_fingerprint(
      arolla.optools.trace_function(fn, gen_tracer=gen_tracer), unmangling
  )


def as_lambda_operator(
    name: str,
    *,
    qtype_constraints: arolla.types.QTypeConstraints = (),
    aux_policy: str = py_boxing.DEFAULT_BOXING_POLICY,
) -> Callable[[types.FunctionType], arolla.types.RestrictedLambdaOperator]:
  """A decorator for defining Koladata-specific lambda operators.

  Koda specifics:
    - Adds a KodaView to the inputs during tracing, allowing prefix / infix
      notation.

  Args:
    name: Name of the operator.
    qtype_constraints: QType constraints for the operator.
    aux_policy: Aux policy for the operator.

  Returns:
    A decorator that constructs a lambda operator by tracing a python function.
  """

  def impl(fn):
    op_sig = _build_operator_signature_from_fn(fn, aux_policy)
    op_expr = _build_lambda_body_from_fn(fn)

    # If there is a `py_boxing.hidden_seed()`-marked param on the `fn`
    # signature, use its value for the `py_boxing.HIDDEN_SEED_LEAF` leaf.
    if aux_policy == py_boxing.FULL_SIGNATURE_POLICY:
      hidden_seed_param = py_boxing.find_hidden_seed_param(
          inspect.signature(fn)
      )
      if hidden_seed_param is not None:
        op_expr = arolla.abc.sub_by_fingerprint(
            op_expr,
            {
                py_boxing.HIDDEN_SEED_LEAF.fingerprint: arolla.abc.placeholder(
                    hidden_seed_param
                )
            },
        )
    return arolla.optools.make_lambda(
        op_sig,
        op_expr,
        qtype_constraints=qtype_constraints,
        name=name,
        doc=inspect.getdoc(fn) or '',
    )

  return impl


def equiv_to_op(
    this_op: arolla.types.Operator | str, that_op: arolla.types.Operator | str
) -> bool:
  """Returns true iff the impl of `this_op` equals the impl of `that_op`."""
  this_op = arolla.abc.decay_registered_operator(this_op)
  that_op = arolla.abc.decay_registered_operator(that_op)
  return this_op == that_op


def reload_operator_view(view: type[arolla.abc.ExprView]) -> None:
  """Re-registers the view for all registered operators with the same view.

  Uses the fully qualified name (including module) of the view to compare the
  new view with the existing one.

  Note that only operators registered through `optools.add_to_registry` are
  affected.

  Args:
    view: The view to use for the operators.
  """
  for registered_op in _REGISTERED_OPS.values():
    old_view = registered_op.view
    if (
        old_view is not None
        and old_view.__module__ == view.__module__
        and old_view.__qualname__ == view.__qualname__
    ):
      arolla.abc.set_expr_view_for_registered_operator(
          registered_op.op.display_name, view
      )
