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

"""Runtime type checking for Koda functions."""

from collections.abc import Mapping
import functools
import inspect

from arolla import arolla
from koladata.expr import tracing_mode
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import schema_item

eager = eager_op_utils.operators_container('kd')
lazy = kde_operators.kde


def _verify_input_eager(
    key: str, arg: data_slice.DataSlice, type_constraint: schema_item.SchemaItem
):
  """Verifies the type of input in eager mode."""
  if not isinstance(arg, (data_item.DataItem, data_slice.DataSlice)):
    # Boxing is needed to support Python arguments.
    arg = py_boxing.as_qvalue(arg)
  if arg.get_schema() != type_constraint:
    raise TypeError(
        f'kd.check_inputs: type mismatch for parameter `{key}`.'
        f' Expected type {eager.schema.get_repr(type_constraint)}, got'
        f' {eager.schema.get_repr(arg.get_schema())}'
    )


def _verify_inputs_eager(
    bound_args: inspect.BoundArguments,
    kw_constraints: Mapping[str, schema_item.SchemaItem],
):
  """Verifies the types of inputs in eager mode."""
  for key, type_constraint in kw_constraints.items():
    # KeyError is not possible as parameter presence was checked during
    # decoration.
    _verify_input_eager(key, bound_args.arguments[key], type_constraint)


def _verify_output_eager(
    output: data_slice.DataSlice,
    constraint: schema_item.SchemaItem,
):
  """Verifies the type of output in eager mode."""
  if not isinstance(output, (data_item.DataItem, data_slice.DataSlice)):
    raise TypeError(
        'kd.check_output: expected DataItem/DataSlice output, got'
        f' {type(output)}'
    )
  if output.get_schema() != constraint:
    raise TypeError(
        'kd.check_output: type mismatch for output. Expected type'
        f' {eager.schema.get_repr(constraint)}, got'
        f' {eager.schema.get_repr(output.get_schema())}'
    )


def _with_input_expr_assertion(
    key: str,
    arg: view.KodaView,
    type_constraint: schema_item.SchemaItem,
) -> view.KodaView:
  """Adds an assertion for a single input type in tracing mode."""
  return lazy.assertion.with_assertion(
      arg,
      lazy.get_schema(arg) == type_constraint,
      lambda arg: lazy.strings.join(
          f'kd.check_inputs: type mismatch for parameter `{key}`. Expected'
          f' type {eager.schema.get_repr(type_constraint)}, got ',
          lazy.schema.get_repr(arg.get_schema()),
      ),
      arg,
  )


def _with_input_expr_assertions(
    bound_args: inspect.BoundArguments,
    kw_constraints: Mapping[str, schema_item.SchemaItem],
) -> inspect.BoundArguments:
  """Adds assertions for input types in tracing mode."""
  for key, type_constraint in kw_constraints.items():
    # KeyError is not possible as parameter presence was checked during
    # decoration.
    # Boxing is needed to support Python arguments.
    arg = bound_args.arguments[key]
    if not isinstance(arg, (view.KodaView, arolla.Expr)):
      # We attempt eager verification of a static input.
      _verify_input_eager(key, arg, type_constraint)
    else:
      bound_args.arguments[key] = _with_input_expr_assertion(
          key, arg, type_constraint
      )
  return bound_args


def _with_output_expr_assertion(
    output: data_slice.DataSlice,
    type_constraint: schema_item.SchemaItem,
):
  """Adds an assertion for output type in tracing mode."""
  return lazy.assertion.with_assertion(
      output,
      lazy.get_schema(output) == type_constraint,
      lambda output: lazy.strings.join(
          'kd.check_output: type mismatch for output. Expected type '
          f'{eager.schema.get_repr(type_constraint)}, got ',
          lazy.schema.get_repr(output.get_schema()),
      ),
      output,
  )


def check_inputs(**kw_constraints: schema_item.SchemaItem):
  """Decorator factory for adding runtime input type checking to Koda functions.

  Resulting decorators will check the schemas of DataSlice inputs of
  a function at runtime, and raise TypeError in case of mismatch.

  Decorated functions will preserve the original function's signature and
  docstring.

  Decorated functions can be traced using `kd.fn` and the inputs to the
  resulting functor will be wrapped in kd.assertion.with_assertion nodes that
  match the assertions of the eager version.

  Example for primitive schemas:

    @kd.check_inputs(hours=kd.INT32, minutes=kd.INT32)
    @kd.check_output(kd.STRING)
    def timestamp(hours, minutes):
      return kd.str(hours) + ':' + kd.str(minutes)

    timestamp(ds([10, 10, 10]), kd.ds([15, 30, 45]))  # Does not raise.
    timestamp(ds([10, 10, 10]), kd.ds([15.35, 30.12, 45.1]))  # raises TypeError

  Example for complex schemas:

    Doc = kd.schema.named_schema('Doc', doc_id=kd.INT64, score=kd.FLOAT32)

    Query = kd.schema.named_schema(
        'Query',
        query_text=kd.STRING,
        query_id=kd.INT32,
        docs=kd.list_schema(Doc),
    )

    @kd.check_inputs(query=Query)
    @kd.check_output(Doc)
    def get_docs(query):
      return query.docs[:]

  Args:
    **kw_constraints: mapping of parameter names to DataItem schemas. Names must
      match parameter names in the decorated function. Arguments for the given
      parameters must be DataSlices/DataItems with the corresponding schemas.

  Returns:
    A decorator that can be used to type annotate a function that accepts
    DataSlices/DataItem inputs.
  """
  for key, value in kw_constraints.items():
    if not isinstance(value, schema_item.SchemaItem):
      raise TypeError(
          'kd.check_inputs: invalid constraint: expected constraint for'
          f' parameter `{key}` to be a schema DataItem, got {value}'
      )

  def decorate_f(f):
    signature = inspect.signature(f)
    # TODO: Support variadic parameters constraints?
    if any([
        p.kind is inspect.Parameter.VAR_POSITIONAL
        or p.kind is inspect.Parameter.VAR_KEYWORD
        for p in signature.parameters.values()
    ]):
      raise TypeError(
          'kd.check_inputs does not support variadic parameters in the'
          ' decorated function'
      )
    for key in kw_constraints:
      if key not in signature.parameters:
        raise TypeError(
            f'kd.check_inputs: parameter name `{key}` does not match any'
            ' parameter in function signature'
        )

    def check_inputs_decorator(*args, **kwargs):
      bound_args = signature.bind(*args, **kwargs)
      if not tracing_mode.is_tracing_enabled():
        bound_args.apply_defaults()
        _verify_inputs_eager(bound_args, kw_constraints)
        return f(*args, **kwargs)
      else:
        bound_args = _with_input_expr_assertions(bound_args, kw_constraints)
        return f(*bound_args.args, **bound_args.kwargs)

    return functools.wraps(f)(check_inputs_decorator)

  return decorate_f


def check_output(constraint: schema_item.SchemaItem):
  """Decorator factory for adding runtime output type checking to Koda functions.

  Resulting decorators will check the schema of the DataSlice output of
  a function at runtime, and raise TypeError in case of mismatch.

  Decorated functions will preserve the original function's signature and
  docstring.

  Decorated functions can be traced using `kd.fn` and the output of the
  resulting functor will be wrapped in a kd.assertion.with_assertion node that
  match the assertion of the eager version.

  Example for primitive schemas:

    @kd.check_inputs(hours=kd.INT32, minutes=kd.INT32)
    @kd.check_output(kd.STRING)
    def timestamp(hours, minutes):
      return kd.to_str(hours) + ':' + kd.to_str(minutes)

    timestamp(ds([10, 10, 10]), kd.ds([15, 30, 45]))  # Does not raise.
    timestamp(ds([10, 10, 10]), kd.ds([15.35, 30.12, 45.1]))  # raises TypeError

  Example for complex schemas:

    Doc = kd.schema.named_schema('Doc', doc_id=kd.INT64, score=kd.FLOAT32)

    Query = kd.schema.named_schema(
        'Query',
        query_text=kd.STRING,
        query_id=kd.INT32,
        docs=kd.list_schema(Doc),
    )

    @kd.check_inputs(query=Query)
    @kd.check_output(Doc)
    def get_docs(query):
      return query.docs[:]

  Args:
    constraint: A DataItem schema for the output. Output of the decorated
      function must be a DataSlice/DataItem with the corresponding schema.

  Returns:
    A decorator that can be used to annotate a function returning a
    DataSlice/DataItem.
  """
  if not isinstance(constraint, schema_item.SchemaItem):
    raise TypeError(
        'kd.check_output: invalid constraint: expected constraint for output to'
        f' be a schema DataItem, got {constraint}'
    )

  def decorate_f(f):
    def check_output_decorator(*args, **kwargs):
      res = f(*args, **kwargs)
      if not tracing_mode.is_tracing_enabled():
        _verify_output_eager(res, constraint)
      else:
        res = _with_output_expr_assertion(res, constraint)
      return res

    return functools.wraps(f)(check_output_decorator)

  return decorate_f
