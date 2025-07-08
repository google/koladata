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

"""Operator definition and registration tooling."""

import dataclasses
import inspect
import types
from typing import Any, Callable, Collection
import warnings

from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view as view_lib
from koladata.operators import op_repr
from koladata.operators import qtype_utils
from koladata.operators import unified_binding_policy
from koladata.types import py_boxing
from koladata.types import qtypes


@dataclasses.dataclass(frozen=True)
class _RegisteredOp:
  op: arolla.types.Operator
  view: type[arolla.abc.ExprView] | None
  repr_fn: op_repr.OperatorReprFn


_REGISTERED_OPS: dict[str, _RegisteredOp] = {}


def _clear_registered_ops():
  _REGISTERED_OPS.clear()


arolla.abc.cache_clear_callbacks.add(_clear_registered_ops)


def add_alias(name: str, alias: str):
  registered_op = _REGISTERED_OPS.get(name)
  if registered_op is None:
    raise ValueError(f'Operator {name} is not registered.')
  add_to_registry(
      alias, view=registered_op.view, repr_fn=registered_op.repr_fn
  )(registered_op.op)


def add_to_registry(
    name: str | None = None,
    *,
    aliases: Collection[str] = (),
    unsafe_override: bool = False,
    view: type[arolla.abc.ExprView] | None = view_lib.KodaView,
    repr_fn: op_repr.OperatorReprFn | None = None,
):
  """Wrapper around Arolla's add_to_registry with Koda functionality.

  Args:
    name: Optional name of the operator. Otherwise, inferred from the op.
    aliases: Optional aliases for the operator.
    unsafe_override: Whether to override an existing operator.
    view: Optional view to use for the operator. If None, the default arolla
      ExprView will be used.
    repr_fn: Optional repr function to use for the operator and its aliases. In
      case of None, a default repr function will be used.

  Returns:
    Registered operator.
  """
  repr_fn = repr_fn or op_repr.default_op_repr

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

    op = _register_op(op, name)

    op_name = name or op.display_name
    _REGISTERED_OPS[op_name] = _RegisteredOp(op, view, repr_fn)

    for alias in aliases:
      add_alias(op_name, alias)
    return op

  return impl


_KODA_NATIVE_QTYPES_CONDITION_EXPR = arolla.M.seq.all(
    arolla.M.seq.map(
        arolla.LambdaOperator(
            (arolla.P.x == qtypes.DATA_SLICE)
            | (arolla.P.x == qtypes.JAGGED_SHAPE)
            | (arolla.P.x == qtypes.DATA_BAG)
            | (arolla.P.x == arolla.UNSPECIFIED)
        ),
        arolla.M.qtype.get_field_qtypes(arolla.L.input_tuple_qtype),
    )
)


def add_to_registry_as_overloadable_with_default(
    name: str | None = None,
    *,
    aliases: Collection[str] = (),
    unsafe_override: bool = False,
    view: type[arolla.abc.ExprView] | None = view_lib.KodaView,
    repr_fn: op_repr.OperatorReprFn | None = None,
    aux_policy: str = py_boxing.DEFAULT_BOXING_POLICY,
):
  """Wrapper around Arolla's add_to_registry with a default overload.

  The implementation provided in the wrapped function is registered as
  the overload for Koladata native types.

  Args:
    name: Optional name of the operator. Otherwise, inferred from the op.
    aliases: Optional aliases for the operator.
    unsafe_override: Whether to override an existing operator.
    view: Optional view to use for the operator. If None, the default arolla
      ExprView will be used.
    repr_fn: Optional repr function to use for the operator and its aliases. In
      case of None, a default repr function will be used.
    aux_policy: Aux policy for the operator.

  Returns:
    Registered operator.
  """
  repr_fn = repr_fn or op_repr.default_op_repr

  def impl(op: arolla.types.Operator) -> arolla.types.RegisteredOperator:
    op_name = name or op.display_name

    generic_op = add_to_registry_as_overloadable(
        op_name,
        unsafe_override=unsafe_override,
        view=view,
        repr_fn=repr_fn,
        aux_policy=aux_policy,
    )(op)

    _ = add_to_registry_as_overload(
        op_name + '._internal',
        unsafe_override=unsafe_override,
        overload_condition_expr=_KODA_NATIVE_QTYPES_CONDITION_EXPR,
    )(op)

    for alias in aliases:
      add_alias(op_name, alias)

    return generic_op

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
    # To enable registering an overload for an alias, we need to register it
    # under the canonical name of the overloadable operator.
    op_name = name or op.display_name
    name_ns, _, name_suffix = op_name.rpartition('.')
    canonical_name = arolla.abc.decay_registered_operator(name_ns).display_name

    _ = arolla.optools.add_to_registry_as_overload(
        canonical_name + '.' + name_suffix,
        unsafe_override=unsafe_override,
        overload_condition_expr=overload_condition_expr,
    )(op)
    return op

  return impl


def _build_operator_signature_from_fn(
    fn: types.FunctionType, aux_policy: str
) -> arolla.abc.Signature:
  """Builds an operator signature from a python function."""
  signature = arolla.abc.make_operator_signature(
      inspect.signature(fn), as_qvalue=py_boxing.as_qvalue
  )
  return arolla.abc.Signature((signature.parameters, aux_policy))


UNIFIED_NON_DETERMINISTIC_PARAM_NAME = (
    unified_binding_policy.NON_DETERMINISTIC_PARAM_NAME
)

_UNIFIED_NON_DETERMINISTIC_PARAM = arolla.abc.placeholder(
    unified_binding_policy.NON_DETERMINISTIC_PARAM_NAME
)


def unified_non_deterministic_arg() -> arolla.Expr:
  """Returns a non-deterministic token for use with `bind_op(..., arg)`."""
  return py_boxing.new_non_deterministic_token()


def unified_non_deterministic_kwarg() -> dict[str, arolla.Expr]:
  """Returns a non-deterministic token for use with `bind_op(..., **kwarg)`."""
  return {UNIFIED_NON_DETERMINISTIC_PARAM_NAME: unified_non_deterministic_arg()}


def as_backend_operator(
    name: str,
    *,
    qtype_inference_expr: arolla.Expr | arolla.QType = qtypes.DATA_SLICE,
    qtype_constraints: arolla.types.QTypeConstraints = (),
    deterministic: bool = True,
    custom_boxing_fn_name_per_parameter: dict[str, str] | None = None,
) -> Callable[[types.FunctionType], arolla.types.BackendOperator]:
  """Decorator for Koladata backend operators with a unified binding policy.

  Args:
    name: The name of the operator.
    qtype_inference_expr: Expression that computes operator's output type.
      Argument types can be referenced using `arolla.P.arg_name`.
    qtype_constraints: List of `(predicate_expr, error_message)` pairs.
      `predicate_expr` may refer to the argument QType using
      `arolla.P.arg_name`. If a type constraint is not fulfilled, the
      corresponding `error_message` is used. Placeholders, like `{arg_name}`,
      get replaced with the actual type names during the error message
      formatting.
    deterministic: If set to False, a hidden parameter (with the name
      `optools.UNIFIED_NON_DETERMINISTIC_PARAM_NAME`) is added to the end of the
      signature. This parameter receives special handling by the binding policy
      implementation.
    custom_boxing_fn_name_per_parameter: A dictionary specifying a custom boxing
      function per parameter (constants with the boxing functions look like:
      `koladata.types.py_boxing.WITH_*`, e.g. `WITH_PY_FUNCTION_TO_PY_OBJECT`).

  Returns:
    A decorator that constructs a backend operator based on the provided Python
    function signature.
  """

  def impl(fn):
    qtype_constraints_copy = list(qtype_constraints)
    op_sig = unified_binding_policy.make_unified_signature(
        inspect.signature(fn),
        deterministic=deterministic,
        custom_boxing_fn_name_per_parameter=(
            custom_boxing_fn_name_per_parameter or {}
        ),
    )
    if not deterministic:
      qtype_constraints_copy.append(
          qtype_utils.expect_non_deterministic(_UNIFIED_NON_DETERMINISTIC_PARAM)
      )
    return arolla.types.BackendOperator(
        name,
        op_sig,
        doc=inspect.getdoc(fn) or '',
        qtype_inference_expr=py_boxing.as_expr(qtype_inference_expr),
        qtype_constraints=qtype_constraints_copy,
    )

  return impl


_P = input_container.InputContainer('P')


def _build_lambda_body_from_fn(fn: types.FunctionType):
  """Builds a lambda body expression from a python function."""
  unmangling = {}

  def gen_tracer(name: str):
    result = _P[name]
    unmangling[result.fingerprint] = arolla.abc.placeholder(name)
    return result

  return arolla.abc.sub_by_fingerprint(
      arolla.optools.trace_function(fn, gen_tracer=gen_tracer), unmangling
  )


# TOOD: b/383536303 - Consider improving the error messages for "unfixed"
# variadic `*args` and `**kwargs` during tracing.
def as_lambda_operator(
    name: str,
    *,
    qtype_constraints: arolla.types.QTypeConstraints = (),
    deterministic: bool | None = None,
    custom_boxing_fn_name_per_parameter: dict[str, str] | None = None,
    suppress_unused_parameter_warning: bool = False,
) -> Callable[
    [types.FunctionType],
    arolla.types.LambdaOperator | arolla.types.RestrictedLambdaOperator,
]:
  """Decorator for Koladata lambda operators with a unified binding policy.

  Args:
    name: The name of the operator.
    qtype_constraints: List of `(predicate_expr, error_message)` pairs.
      `predicate_expr` may refer to the argument QType using
      `arolla.P.arg_name`. If a type constraint is not fulfilled, the
      corresponding `error_message` is used. Placeholders, like `{arg_name}`,
      get replaced with the actual type names during the error message
      formatting.
    deterministic: If True, the resulting operator will be deterministic and may
      only use deterministic operators. If False, the operator will be declared
      non-deterministic. By default, the decorator attempts to detect the
      operator's determinism.
    custom_boxing_fn_name_per_parameter: A dictionary specifying a custom boxing
      function per parameter (constants with the boxing functions look like:
      `koladata.types.py_boxing.WITH_*`, e.g. `WITH_PY_FUNCTION_TO_PY_OBJECT`).
    suppress_unused_parameter_warning: If True, unused parameters will not cause
      a warning.

  Returns:
    A decorator that constructs a lambda operator by tracing a Python function.
  """

  def impl(fn):
    op_expr = _build_lambda_body_from_fn(fn)

    deterministic_copy = deterministic
    if deterministic_copy is None:
      deterministic_copy = (
          py_boxing.NON_DETERMINISTIC_TOKEN_LEAF.leaf_key
          not in arolla.abc.get_leaf_keys(op_expr)
      )

    op_sig = unified_binding_policy.make_unified_signature(
        inspect.signature(fn),
        deterministic=deterministic_copy,
        custom_boxing_fn_name_per_parameter=(
            custom_boxing_fn_name_per_parameter or {}
        ),
    )

    qtype_constraints_copy = list(qtype_constraints)
    if deterministic_copy:
      if (
          py_boxing.NON_DETERMINISTIC_TOKEN_LEAF.leaf_key
          in arolla.abc.get_leaf_keys(op_expr)
      ):
        raise ValueError(
            'the lambda operator is based on a non-deterministic expression;'
            ' please, specify `deterministic=False` in the declaration'
        )
    else:
      qtype_constraints_copy.append(
          qtype_utils.expect_non_deterministic(_UNIFIED_NON_DETERMINISTIC_PARAM)
      )
      op_expr = arolla.abc.sub_by_fingerprint(
          op_expr,
          {
              py_boxing.NON_DETERMINISTIC_TOKEN_LEAF.fingerprint: (
                  _UNIFIED_NON_DETERMINISTIC_PARAM
              )
          },
      )
    if not suppress_unused_parameter_warning:
      unused_parameters = set(
          param.name
          for param in op_sig.parameters
          if not param.name.startswith('unused')
          and not param.name.startswith('_')
      )
      unused_parameters -= set(arolla.abc.get_placeholder_keys(op_expr))
      if unused_parameters:
        warnings.warn(
            f'kd.optools.as_lambda_operator({name!r}, ...) a lambda operator'
            ' not using some of its parameters: '
            + ', '.join(sorted(unused_parameters)),
            category=arolla.optools.LambdaUnusedParameterWarning,
        )
    return arolla.optools.make_lambda(
        op_sig,
        op_expr,
        qtype_constraints=qtype_constraints_copy,
        name=name,
        doc=inspect.getdoc(fn) or '',
    )

  return impl


def as_py_function_operator(
    name: str,
    *,
    qtype_inference_expr: arolla.Expr | arolla.QType = qtypes.DATA_SLICE,
    qtype_constraints: arolla.types.QTypeConstraints = (),
    codec: bytes | None = None,
    deterministic: bool = True,
    custom_boxing_fn_name_per_parameter: dict[str, str] | None = None,
) -> Callable[[types.FunctionType], arolla.abc.Operator]:
  """Returns a decorator for defining Koladata-specific py-function operators.

  The decorated function should accept QValues as input and returns a single
  QValue. Variadic positional and keyword arguments are passed as tuples and
  dictionaries of QValues, respectively.

  Importantly, it is recommended that the function on which the operator is
  based be pure -- that is, deterministic and without side effects.
  If the function is not pure, please specify deterministic=False.

  Args:
    name: The name of the operator.
    qtype_inference_expr: expression that computes operator's output qtype; an
      argument qtype can be referenced as P.arg_name.
    qtype_constraints: QType constraints for the operator.
    codec: A PyObject serialization codec for the wrapped function, compatible
      with `arolla.types.encode_py_object`. The resulting operator is
      serializable only if the codec is specified.
    deterministic: Set this to `False` if the wrapped function is not pure
      (i.e., non-deterministic or has side effects).
    custom_boxing_fn_name_per_parameter: A dictionary specifying a custom boxing
      function per parameter (constants with the boxing functions look like:
      `koladata.types.py_boxing.WITH_*`, e.g. `WITH_PY_FUNCTION_TO_PY_OBJECT`).
  """

  def impl(fn):
    # Analyse the signature.
    sig = inspect.signature(fn)
    positional_params = []
    var_positional_params = []
    keyword_params = []
    var_keyword_params = []
    for param in sig.parameters.values():
      if (
          param.kind == param.POSITIONAL_ONLY
          or param.kind == param.POSITIONAL_OR_KEYWORD
      ):
        positional_params.append(param.name)
      elif param.kind == param.VAR_POSITIONAL:
        var_positional_params.append(param.name)
      elif param.kind == param.KEYWORD_ONLY:
        keyword_params.append(param.name)
      elif param.kind == param.VAR_KEYWORD:
        var_keyword_params.append(param.name)
      else:
        raise ValueError(f'unsupported parameter: {param}')
    all_params = (
        positional_params
        + var_positional_params
        + keyword_params
        + var_keyword_params
    )
    assert len(var_positional_params) <= 1
    assert len(var_keyword_params) <= 1

    # Prepare an expression for return_type. This expression must be computable
    # at compile time. To achieve this, we declare a backend operator using
    # the provided qtype_inference_expr (without actually defining it in
    # the backend), and we use `M.qtype.qtype_of(...)` to transform
    # the attribute into a value that is accessible at compile time.
    #
    # Importantly, we wrap both `M.qtype.qtype_of` and the backend operator in
    # a lambda to ensure compatibility, regardless of whether literal folding is
    # enabled:
    #  * if literal folding is enabled, the lambda will fold without lowering,
    #    as it infers the qvalue attribute;
    #  * if literal folding is disabled, the node M.qtype.qtype_of replaces
    #    itself with a literal during lowering.
    stub_op_signature = arolla.abc.make_operator_signature(','.join(all_params))
    return_type_expr = arolla.types.LambdaOperator(
        stub_op_signature,
        arolla.M.qtype.qtype_of(
            arolla.types.BackendOperator(
                'koda_internal._undefined_backend_op',
                stub_op_signature,
                qtype_inference_expr=qtype_inference_expr,
            )(*map(arolla.abc.placeholder, all_params))
        ),
        name='return_type_stub_op',
    )(*map(arolla.abc.placeholder, all_params))

    # Prepare an expression for `args`.
    if positional_params and var_positional_params:
      args_expr = arolla.M.core.concat_tuples(
          arolla.M.core.make_tuple(
              *map(arolla.abc.placeholder, positional_params)
          ),
          arolla.abc.placeholder(var_positional_params[0]),
      )
    elif positional_params:
      args_expr = arolla.M.core.make_tuple(
          *map(arolla.abc.placeholder, positional_params)
      )
    elif var_positional_params:
      args_expr = arolla.abc.placeholder(var_positional_params[0])
    else:
      args_expr = arolla.tuple()

    # Prepare an expression for `kwargs`.
    if keyword_params and var_keyword_params:
      # TODO: b/384077837 - Consider detecting cases where a dynamic `**kwargs`
      # shadows `keyword-only` arguments.
      kwargs_expr = arolla.M.namedtuple.union(
          arolla.M.namedtuple.make(
              **{k: arolla.abc.placeholder(k) for k in keyword_params}
          ),
          arolla.abc.placeholder(var_keyword_params[0]),
      )
    elif keyword_params:
      kwargs_expr = arolla.M.namedtuple.make(
          **{k: arolla.abc.placeholder(k) for k in keyword_params}
      )
    elif var_keyword_params:
      kwargs_expr = arolla.abc.placeholder(var_keyword_params[0])
    else:
      kwargs_expr = arolla.namedtuple()

    # Construct a lambda operator.
    fn_expr = arolla.types.PyObject(fn, codec=codec)
    qtype_constraints_copy = list(qtype_constraints)
    if not deterministic:
      fn_expr = arolla.abc.sub_by_fingerprint(
          arolla.abc.aux_bind_op(
              'koda_internal.non_deterministic_identity', fn_expr
          ),
          {
              py_boxing.NON_DETERMINISTIC_TOKEN_LEAF.fingerprint: (
                  _UNIFIED_NON_DETERMINISTIC_PARAM
              )
          },
      )
      qtype_constraints_copy.append(
          qtype_utils.expect_non_deterministic(_UNIFIED_NON_DETERMINISTIC_PARAM)
      )
    op_sig = unified_binding_policy.make_unified_signature(
        sig,
        deterministic=deterministic,
        custom_boxing_fn_name_per_parameter=(
            custom_boxing_fn_name_per_parameter or {}
        ),
    )
    op_expr = arolla.abc.bind_op(
        'py.call', fn_expr, return_type_expr, args_expr, kwargs_expr
    )
    return arolla.optools.make_lambda(
        op_sig,
        op_expr,
        qtype_constraints=qtype_constraints_copy,
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


def make_operators_container(*namespaces: str) -> arolla.OperatorsContainer:
  """Returns an OperatorsContainer for the given namespaces.

  Note that the returned container accesses the global namespace. A common
  pattern is therefore:
    foo = make_operators_container('foo', 'foo.bar', 'foo.baz').foo

  Args:
    *namespaces: Namespaces to make available in the returned container.
  """
  return arolla.OperatorsContainer(unsafe_extra_namespaces=namespaces)
