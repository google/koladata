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

"""Tools to create functors."""

import inspect
import types as py_types
import typing
from typing import Any, Callable
import warnings

from arolla import arolla
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import tracing
from koladata.functions import functions as fns
from koladata.functor import py_functors_py_ext as _py_functors_py_ext
from koladata.functor import signature_utils
from koladata.operators import eager_op_utils as _eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import mask_constants
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants


_kd = _eager_op_utils.operators_container('kde')

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
kde = kde_operators.kde


# We can move this wrapping inside the CPython layer if needed for performance.
def _maybe_wrap_expr(arg: Any) -> arolla.QValue:
  if isinstance(arg, arolla.Expr):
    arg = introspection.pack_expr(arg)
  return py_boxing.as_qvalue(arg)


def _extract_auto_variables(variables: dict[str, Any]) -> dict[str, Any]:
  """Creates additional variables for DataSlices and named nodes."""
  variables = dict(variables)
  aux_variable_fingerprints = set()
  prefix_to_counter = {}

  def create_unique_variable(prefix: str) -> str:
    if prefix not in prefix_to_counter:
      prefix_to_counter[prefix] = 0
    while True:
      var_name = f'{prefix}_{prefix_to_counter[prefix]}'
      prefix_to_counter[prefix] += 1
      if var_name not in variables:
        return var_name

  def transform_node(node: arolla.Expr) -> arolla.Expr:
    if node.is_literal or isinstance(node.op, literal_operator.LiteralOperator):
      val = typing.cast(arolla.QValue, node.qvalue)
      if val.qtype == qtypes.DATA_SLICE:
        val = typing.cast(data_slice.DataSlice, val)
        # Keep simple constants inlined, to avoid too many variables.
        if (
            val.get_bag() is None
            and val.get_ndim() == 0
            and (
                val.get_schema().is_primitive_schema()
                or val.get_schema() == schema_constants.NONE
            )
        ):
          return node
        # We need to implode the DataSlice into lists if it is not a DataItem.
        var_name = create_unique_variable('aux')
        var = V[var_name]
        ndim = val.get_ndim().internal_as_py()
        if ndim:
          val = fns.implode(val, ndim=ndim)
          var = kde.explode(var, ndim=ndim)
        variables[var_name] = val
        aux_variable_fingerprints.add(var.fingerprint)
        return var

    if (expr_name := introspection.get_name(node)) is not None:
      name = (
          create_unique_variable(expr_name)
          if expr_name in variables
          else expr_name
      )
      child = introspection.unwrap_named(node)
      if child.fingerprint in aux_variable_fingerprints:
        # If a literal DataSlice was named, avoid creating a temporary name
        # and use the real name instead. The auxiliary variable might have
        # been wrapped with kde.explode(), so we do a sub_inputs instead of
        # just replacing with V[name].
        var_names = introspection.get_input_names(child, V)
        assert len(var_names) == 1
        variables[name] = variables.pop(var_names[0])
        return introspection.sub_inputs(child, V, **{var_names[0]: V[name]})
      variables[name] = introspection.pack_expr(child)
      return V[name]

    return node

  all_exprs = {}
  for k, v in variables.items():
    if v.get_schema() == schema_constants.EXPR:
      v = introspection.unpack_expr(v)
      all_exprs[k] = v
  combined = arolla.M.core.make_tuple(*all_exprs.values())
  # It is important to transform everything at once if there is some shared
  # named subtree.
  combined = arolla.abc.transform(combined, transform_node)
  assert len(combined.node_deps) == len(all_exprs)
  for k, v in zip(all_exprs, combined.node_deps):
    variables[k] = introspection.pack_expr(v)
  return variables


def expr_fn(
    returns: Any,
    *,
    signature: data_slice.DataSlice | None = None,
    auto_variables: bool = False,
    **variables: Any,
) -> data_slice.DataSlice:
  """Creates a functor.

  Args:
    returns: What should calling a functor return. Will typically be an Expr to
      be evaluated, but can also be a DataItem in which case calling will just
      return this DataItem, or a primitive that will be wrapped as a DataItem.
      When this is an Expr, it either must evaluate to a DataSlice/DataItem, or
      the return_type_as= argument should be specified at kd.call time.
    signature: The signature of the functor. Will be used to map from args/
      kwargs passed at calling time to I.smth inputs of the expressions. When
      None, the default signature will be created based on the inputs from the
      expressions involved.
    auto_variables: When true, we create additional variables automatically
      based on the provided expressions for 'returns' and user-provided
      variables. All non-scalar DataSlice literals become their own variables,
      and all named subexpressions become their own variables. This helps
      readability and manipulation of the resulting functor.
    **variables: The variables of the functor. Each variable can either be an
      expression to be evaluated, or a DataItem, or a primitive that will be
      wrapped as a DataItem. The result of evaluating the variable can be
      accessed as V.smth in other expressions.

  Returns:
    A DataItem representing the functor.
  """
  returns = _maybe_wrap_expr(returns)
  variables = {k: _maybe_wrap_expr(v) for k, v in variables.items()}
  if auto_variables:
    variables['returns'] = returns
    variables = _extract_auto_variables(variables)
    returns = variables.pop('returns')
  return _py_functors_py_ext.create_functor(returns, signature, **variables)


def is_fn(obj: Any) -> data_slice.DataSlice:
  """Checks if `obj` represents a functor.

  Args:
    obj: The value to check.

  Returns:
    kd.present if `obj` is a DataSlice representing a functor, kd.missing
    otherwise (for example if obj has wrong type).
  """
  if isinstance(obj, data_slice.DataSlice) and _py_functors_py_ext.is_fn(obj):
    return mask_constants.present
  else:
    return mask_constants.missing


def trace_py_fn(
    f: Callable[..., Any], *, auto_variables: bool = True, **defaults: Any
) -> data_slice.DataSlice:
  """Returns a Koda functor created by tracing a given Python function.

  When 'f' has variadic positional (*args) or variadic keyword
  (**kwargs) arguments, their name must start with 'unused', and they
  must actually be unused inside 'f'.
  'f' must not use Python control flow operations such as if or for.

  Args:
    f: Python function.
    auto_variables: When true, we create additional variables automatically
      based on the traced expression. All DataSlice literals become their own
      variables, and all named subexpressions become their own variables. This
      helps readability and manipulation of the resulting functor. Note that
      this defaults to True here, while it defaults to False in
      kd.functor.expr_fn.
    **defaults: Keyword defaults to bind to the function. The values in this map
      may be Koda expressions or DataItems (see docstring for kd.bind for more
      details). Defaults can be overridden through kd.call arguments. **defaults
      and inputs to kd.call will be combined and passed through to the function.
      If a parameter that is not passed does not have a default value defined by
      the function then an exception will occur.

  Returns:
    A DataItem representing the functor.
  """
  traced_expr = tracing.trace(f)
  signature = signature_utils.from_py_signature(inspect.signature(f))
  f = expr_fn(traced_expr, signature=signature, auto_variables=auto_variables)
  return bind(f, **defaults) if defaults else f


def py_fn(
    f: Callable[..., Any],
    *,
    return_type_as: Any = data_slice.DataSlice,
    **defaults: Any,
) -> data_slice.DataSlice:
  """Returns a Koda functor wrapping a python function.

  This is the most flexible way to wrap a python function and is recommended
  for large, complex code.

  Functions wrapped with py_fn are not serializable.

  Note that unlike the functors created by kd.functor.expr_fn from an Expr, this
  functor
  will have exactly the same signature as the original function. In particular,
  if the original function does not accept variadic keyword arguments and
  and unknown argument is passed when calling the functor, an exception will
  occur.

  Args:
    f: Python function. It is required that this function returns a
      DataSlice/DataItem or a primitive that will be automatically wrapped into
      a DataItem.
    return_type_as: The return type of the function is expected to be the same
      as the type of this value. This needs to be specified if the function does
      not return a DataSlice/DataItem or a primitive that would be auto-boxed
      into a DataItem. kd.types.DataSlice and kd.types.DataBag can also be
      passed here.
    **defaults: Keyword defaults to bind to the function. The values in this map
      may be Koda expressions or DataItems (see docstring for kd.bind for more
      details). Defaults can be overridden through kd.call arguments. **defaults
      and inputs to kd.call will be combined and passed through to the function.
      If a parameter that is not passed does not have a default value defined by
      the function then an exception will occur.

  Returns:
    A DataItem representing the functor.
  """
  f = expr_fn(
      # Note: we bypass the binding policy of apply_py since we already
      # have the args/kwargs as tuple and namedtuple.
      arolla.abc.bind_op(
          'kde.py.apply_py',
          py_boxing.as_qvalue(f),
          args=I.args,
          return_type_as=py_boxing.as_qvalue(return_type_as),
          kwargs=I.kwargs,
      ),
      signature=signature_utils.ARGS_KWARGS_SIGNATURE,
  )
  return bind(f, **defaults) if defaults else f


def bind(
    fn_def: data_slice.DataSlice,
    /,
    *,
    return_type_as: Any = data_slice.DataSlice,
    **kwargs: Any,
) -> data_slice.DataSlice:
  """Returns a Koda functor that partially binds a function to `kwargs`.

  This function is intended to work the same as functools.partial in Python.
  More specifically, for every "k=something" argument that you pass to this
  function, whenever the resulting functor is called, if the user did not
  provide "k=something_else" at call time, we will add "k=something".

  Note that you can only provide defaults for the arguments passed as keyword
  arguments this way. Positional arguments must still be provided at call time.
  Moreover, if the user provides a value for a positional-or-keyword argument
  positionally, and it was previously bound using this method, an exception
  will occur.

  You can pass expressions with their own inputs as values in `kwargs`. Those
  inputs will become inputs of the resulting functor, will be used to compute
  those expressions, _and_ they will also be passed to the underying functor.
  Use kd.functor.call_fn for a more clear separation of those inputs.

  Example:
    f = kd.bind(kd.fn(I.x + I.y), x=0)
    kd.call(f, y=1)  # 1

  Args:
    fn_def: A Koda functor.
    return_type_as: The return type of the functor is expected to be the same as
      the type of this value. This needs to be specified if the functor does not
      return a DataSlice. kd.types.DataSlice and kd.types.DataBag can also be
      passed here.
    **kwargs: Partial parameter binding. The values in this map may be Koda
      expressions or DataItems. When they are expressions, they must evaluate to
      a DataSlice/DataItem or a primitive that will be automatically wrapped
      into a DataItem. This function creates auxiliary variables with names
      starting with '_aux_fn', so it is not recommended to pass variables with
      such names.

  Returns:
    A new Koda functor with some parameters bound.
  """
  if not is_fn(fn_def):
    raise ValueError(f'bind() expects a functor, got {fn_def}')
  variables = {'_aux_fn': fn_def}
  if any(
      isinstance(v, arolla.Expr) or introspection.is_packed_expr(v)
      for v in kwargs.values()
  ):
    # We create a sub-functor to take care of proper input binding while
    # being able to forward all arguments to the original functor.
    variables['_aux_fn_compute_variables'] = expr_fn(
        arolla.M.namedtuple.make(**{k: V[k] for k in kwargs}),
        **kwargs,
    )
    # Note: we bypass the binding policy of functor.call since we already
    # have the args/kwargs as tuple and namedtuple.
    variables['_aux_fn_variables'] = arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
        'kde.functor.call',
        V['_aux_fn_compute_variables'],
        args=I.args,
        return_type_as=arolla.namedtuple(
            **{k: py_boxing.as_qvalue(data_slice.DataSlice) for k in kwargs}
        ),
        kwargs=I.kwargs,
        **optools.unified_non_deterministic_kwarg(),
    )
    for k in kwargs:
      variables[k] = arolla.M.namedtuple.get_field(V['_aux_fn_variables'], k)
  else:
    variables.update(kwargs)

  return expr_fn(
      # Note: we bypass the binding policy of functor.call since we already
      # have the args/kwargs as tuple and namedtuple.
      arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
          'kde.functor.call',
          V['_aux_fn'],
          args=I.args,
          return_type_as=py_boxing.as_qvalue(return_type_as),
          kwargs=arolla.M.namedtuple.union(
              arolla.M.namedtuple.make(**{k: V[k] for k in kwargs}), I.kwargs
          ),
          **optools.unified_non_deterministic_kwarg(),
      ),
      signature=signature_utils.ARGS_KWARGS_SIGNATURE,
      **variables,
  )


def fstr_fn(returns: str, **kwargs) -> data_slice.DataSlice:
  """Returns a Koda functor from format string.

  Format-string must be created via Python f-string syntax. It must contain at
  least one formatted expression.

  kwargs are used to assign values to the functor variables and can be used in
  the formatted expression using V. syntax.

  Each formatted expression must have custom format specification,
  e.g. `{I.x:s}` or `{V.y:.2f}`.

  Examples:
    kd.call(fstr_fn(f'{I.x:s} {I.y:s}'), x=1, y=2)  # kd.slice('1 2')
    kd.call(fstr_fn(f'{V.x:s} {I.y:s}', x=1), y=2)  # kd.slice('1 2')
    kd.call(fstr_fn(f'{(I.x + I.y):s}'), x=1, y=2)  # kd.slice('3')
    kd.call(fstr_fn('abc'))  # error - no substitutions
    kd.call(fstr_fn('{I.x}'), x=1)  # error - format should be f-string

  Args:
    returns: A format string.
    **kwargs: variable assignments.
  """
  return expr_fn(kde.strings.fstr(returns), **kwargs)


data_item.register_bind_method_implementation(bind)


def fn(
    f: Any, *, use_tracing: bool = True, **kwargs: Any
) -> data_slice.DataSlice:
  """Returns a Koda functor representing `f`.

  This is the most generic version of the functools builder functions.
  It accepts all functools supported function types including python functions,
  Koda Expr.

  Args:
    f: Python function, Koda Expr, Expr packed into a DataItem, or a Koda
      functor (the latter will be just returned unchanged).
    use_tracing: Whether tracing should be used for Python functions.
    **kwargs: Either variables or defaults to pass to the function. See the
      documentation of `expr_fn` and `py_fn` for more details.

  Returns:
    A Koda functor representing `f`.
  """
  if isinstance(f, arolla.Expr) or introspection.is_packed_expr(f):
    return expr_fn(f, **kwargs)
  if isinstance(f, py_types.FunctionType):
    if use_tracing:
      return trace_py_fn(f, **kwargs)
    else:
      return py_fn(f, **kwargs)
  if is_fn(f):
    if kwargs:
      raise ValueError('passed kwargs when calling fn on an existing functor')
    return f
  raise TypeError(f'cannot convert {f} into a functor')


# TODO: Remove this after some time.
def as_fn(
    f: Any, *, use_tracing: bool = True, **kwargs: Any
) -> data_slice.DataSlice:
  """A deprecated alias for kd.fn."""
  warnings.warn(
      'as_fn is deprecated. Use fn instead.',
      RuntimeWarning,
  )
  return fn(f, use_tracing=use_tracing, **kwargs)


def map_py_fn(
    f: Callable[..., Any] | arolla.types.PyObject,
    *,
    schema: Any = None,
    max_threads: Any = 1,
    ndim: Any = 0,
    **defaults: Any,
) -> data_slice.DataSlice:
  """Returns a Koda functor wrapping a python function for kd.map_py.

  See kd.map_py for detailed APIs, and kd.py_fn for details about function
  wrapping. schema, max_threads and ndims cannot be Koda Expr or Koda functor.

  Args:
    f: Python function.
    schema: The schema to use for resulting DataSlice.
    max_threads: maximum number of threads to use.
    ndim: Dimensionality of items to pass to `f`.
    **defaults: Keyword defaults to pass to the function. The values in this map
      may be kde expressions, format strings, or 0-dim DataSlices. See the
      docstring for py_fn for more details.
  """
  f = expr_fn(
      arolla.abc.bind_op(
          'kde.py.map_py',
          py_boxing.as_qvalue(f),
          args=I.args,
          schema=py_boxing.as_qvalue(schema),
          max_threads=py_boxing.as_qvalue(max_threads),
          ndim=py_boxing.as_qvalue(ndim),
          item_completed_callback=py_boxing.as_qvalue(None),
          kwargs=I.kwargs,
      ),
      signature=signature_utils.ARGS_KWARGS_SIGNATURE,
  )
  return bind(f, **defaults) if defaults else f


def get_signature(
    fn_def: data_slice.DataSlice,
) -> data_slice.DataSlice:
  """Retrieves the signature attached to the given functor.

  Args:
    fn_def: The functor to retrieve the signature for, or a slice thereof.

  Returns:
    The signature(s) attached to the functor(s).
  """
  return fn_def.get_attr('__signature__')


def allow_arbitrary_unused_inputs(
    fn_def: data_slice.DataSlice,
) -> data_slice.DataSlice:
  """Returns a functor that allows unused inputs but otherwise behaves the same.

  This is done by adding a `**__extra_inputs__` argument to the signature if
  there is no existing variadic keyword argument there. If there is a variadic
  keyword argument, this function will return the original functor.

  This means that if the functor already accepts arbitrary inputs but fails
  on unknown inputs further down the line (for example, when calling another
  functor), this method will not fix it. In particular, this method has no
  effect on the return values of kd.py_fn or kd.bind. It does however work
  on the output of kd.trace_py_fn.

  Args:
    fn_def: The input functor.

  Returns:
    The input functor if it already has a variadic keyword argument, or its copy
    but with an additional `**__extra_inputs__` variadic keyword argument if
    there is no existing variadic keyword argument.
  """
  sig = get_signature(fn_def)
  if len(sig.parameters) and _kd.any(  # pylint: disable=g-explicit-length-test
      sig.parameters[:].kind == signature_utils.ParameterKind.VAR_KEYWORD
  ):
    return fn_def
  sig = signature_utils.signature(
      _kd.concat(
          sig.parameters[:].extract(),
          signature_utils.parameter(
              '__extra_inputs__', signature_utils.ParameterKind.VAR_KEYWORD
          ).repeat(1),
      )
  )
  return fn_def.clone(__signature__=sig)
