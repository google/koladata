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

"""Runtime type checking for Koda functions."""

from collections.abc import Mapping
import contextlib
import functools
import inspect
from typing import Optional, Self

from arolla import arolla
from koladata.expr import tracing_mode
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import schema_constants
from koladata.types import schema_item

eager = eager_op_utils.operators_container('kd')
lazy = kde_operators.kde

OBJECT_TIP = (
    'It seems you ran a type checking decorator on a Koda Object. Each'
    ' object stores its own schema, so, in a slice of objects, each object'
    ' may have a different type. If the types of the objects in your slice'
    ' are the same, you should be using entities instead of'
    ' objects( go/koda-common-pitfalls#non-objects-vs-objects ). You can'
    ' create entities by using kd.new/kd.uu instead of kd.obj/kd.uuobj.')

_DISABLE_TRACED_TYPE_CHECKING_COUNTER = 0


def _is_traced_type_checking_disabled():
  return _DISABLE_TRACED_TYPE_CHECKING_COUNTER > 0


@contextlib.contextmanager
def disable_traced_type_checking():
  global _DISABLE_TRACED_TYPE_CHECKING_COUNTER
  _DISABLE_TRACED_TYPE_CHECKING_COUNTER += 1

  try:
    yield
  finally:
    _DISABLE_TRACED_TYPE_CHECKING_COUNTER -= 1


class _DuckType(object):
  """Duck type object.

  It is used to specify the constraint that a schema have a superset of the
  specified attributes, as well as recursive constraints for those attributes.

  A duck type is essentially a tree where internal nodes are duck types and
  leaves are concrete schemas.
  """

  def __init__(self, **kwargs: schema_item.SchemaItem | Self):
    self.__dict__['_fields'] = kwargs

  def __iter__(self):
    return iter(self._fields)

  def __getattr__(self, key: str) -> schema_item.SchemaItem | Self:
    try:
      return self._fields[key]
    except KeyError as e:
      raise AttributeError(key) from e

  def __getitem__(self, key: str):
    return self._fields[key]

  def __setattr__(self, key: str, value: schema_item.SchemaItem | Self):
    raise NotImplementedError(
        'DuckType does not support setattr. All attributes must be specified at'
        ' construction.'
    )


TypeConstraint = schema_item.SchemaItem | _DuckType
ExprOrView = view.KodaView | arolla.Expr


def duck_type(**kwargs: TypeConstraint):
  """Creates a duck type constraint to be used in kd.check_inputs/output.

  A duck type constraint will assert that the DataSlice input/output of a
  function has (at least) a certain set of attributes, as well as to specify
  recursive constraints for those attributes.

  Example:
    @kd.check_inputs(query=kd.duck_type(query_text=kd.STRING,
       docs=kd.duck_type()))
    def f(query):
      pass

    Checks that the DataSlice input parameter `query` has a STRING attribute
    `query_text`, and an attribute `docs` of any schema. `query` may also have
    additional unspecified attributes.

  Args:
    **kwargs: mapping of attribute names to constraints. The constraints must be
      either DuckTypes or SchemaItems. To assert only the presence of an
      attribute, without specifying additional constraints on that attribute,
      pass an empty duck type for that attribute.

  Returns:
    A duck type constraint to be used in kd.check_inputs or kd.check_output.
  """
  return _DuckType(**kwargs)


def duck_list(item_constraint: TypeConstraint):
  """Creates a duck list constraint to be used in kd.check_inputs/output.

  A duck_list constraint will assert a DataSlice is a list, checking the
  item_constraint on the items. Use it if you need to nest
  duck type constraints in list constraints.

  Example:
    @kd.check_inputs(query=kd.duck_type(docs=kd.duck_list(
        kd.duck_type(doc_id=kd.INT64, score=kd.FLOAT32)
    )))
    def f(query):
      pass

  Args:
    item_constraint:  DuckType or SchemaItem representing the constraint to be
      checked on the items of the list.

  Returns:
    A duck type constraint to be used in kd.check_inputs or kd.check_output.
  """
  return _DuckType(__items__=item_constraint)


def duck_dict(key_constraint: TypeConstraint, value_constraint: TypeConstraint):
  """Creates a duck dict constraint to be used in kd.check_inputs/output.

  A duck_dict constraint will assert a DataSlice is a dict, checking the
  key_constraint on the keys and value_constraint on the values. Use it if you
  need to nest duck type constraints in dict constraints.

  Example:
    @kd.check_inputs(mapping=kd.duck_dict(kd.STRING,
        kd.duck_type(doc_id=kd.INT64, score=kd.FLOAT32)))
    def f(query):
      pass

  Args:
    key_constraint:  DuckType or SchemaItem representing the constraint to be
      checked on the keys of the dict.
    value_constraint:  DuckType or SchemaItem representing the constraint to be
      checked on the values of the dict.

  Returns:
    A duck type constraint to be used in kd.check_inputs or kd.check_output.
  """
  return _DuckType(
      __keys__=key_constraint,
      __values__=value_constraint,
  )


def _attr_name_repr(attr_name: str) -> str:
  """Replaces certain internal attribute names with user-facing ones."""
  if attr_name == '__items__':
    return 'get_items()'
  elif attr_name == '__keys__':
    return 'get_keys()'
  elif attr_name == '__values__':
    return 'get_values()'
  else:
    return attr_name


def _attribute_missing_error_message(
    arg_key: Optional[str],
    path: str,
    attr_name: str,
    actual_schema: schema_item.SchemaItem,
) -> str:
  """Produces an error message for missing attributes in eager mode.

  Args:
    arg_key: The name of the argument if the error is for kd.check_inputs, or
      None if the error is for kd.check_output.
    path: The path to the missing attribute. Prefixed by `.` if non-empty.
    attr_name: The name of the attribute that is missing.
    actual_schema: The schema of the argument or output.

  Returns:
    An error message.
  """
  if arg_key is not None:
    # kd.check_inputs
    message = (
        f'kd.check_inputs: expected parameter {arg_key}{path} to have'
        f' attribute `{attr_name}`; no attribute `{attr_name}` on'
        f' {arg_key}{path}={eager.schema.get_repr(actual_schema)}'
    )
  else:
    # kd.check_output
    message = (
        f'kd.check_output: expected output{path} to have attribute'
        f' `{attr_name}`; no attribute `{attr_name}` on'
        f' output{path}={eager.schema.get_repr(actual_schema)}'
    )
  if actual_schema == schema_constants.OBJECT:
    message += f'\n\n{OBJECT_TIP}'
  return message


def _type_mismatch_error_message(
    arg_key: Optional[str],
    path: str,
    expected_schema: schema_item.SchemaItem,
    actual_schema: schema_item.SchemaItem,
) -> str:
  """Produces an error message for type mismatches in eager mode.

  Args:
    arg_key: The name of the argument if the error is for kd.check_inputs, or
      None if the error is for kd.check_output.
    path: The path to the argument or output. Prefixed by `.` if non-empty.
    expected_schema: The expected schema.
    actual_schema: The actual schema of the argument or output.

  Returns:
    An error message.
  """
  if arg_key is not None:
    # kd.check_inputs
    message = (
        f'kd.check_inputs: type mismatch for parameter {arg_key}{path};'
        f' expected type {eager.schema.get_repr(expected_schema)}, got'
        f' {eager.schema.get_repr(actual_schema)}'
    )
  else:
    # kd.check_output
    message = (
        f'kd.check_output: type mismatch for output{path}; expected type'
        f' {eager.schema.get_repr(expected_schema)}, got'
        f' {eager.schema.get_repr(actual_schema)}'
    )
  if actual_schema == schema_constants.OBJECT:
    message += f'\n\n{OBJECT_TIP}'
  return message


def _verify_constraint_eager(
    constraint: TypeConstraint,
    actual_schema: schema_item.SchemaItem,
    arg_key: Optional[str],
    path: str = '',
):
  """Processes a type constraint verification in eager mode.

  Args:
    constraint: The constraint to be verified.
    actual_schema: The schema of the argument or output to check against the
      constraint.
    arg_key: The name of the argument if the error is for kd.check_inputs, or
      None if the error is for kd.check_output.
    path: The path from the input/output parameter to the current constraint.

  Raises:
    TypeError: If the constraint is not satisfied by the actual schema.
  """
  if isinstance(constraint, _DuckType):
    for attr_name in constraint:
      if eager.is_primitive(actual_schema) or not eager.has_attr(
          actual_schema, attr_name
      ):
        raise TypeError(
            _attribute_missing_error_message(
                arg_key, path, attr_name, actual_schema
            )
        )
      _verify_constraint_eager(
          constraint[attr_name],
          eager.get_attr(actual_schema, attr_name),
          arg_key,
          path + '.' + _attr_name_repr(attr_name),
      )
  else:
    if actual_schema != constraint:
      raise TypeError(
          _type_mismatch_error_message(arg_key, path, constraint, actual_schema)
      )


def _lazy_type_mismatch_error_message(
    arg_key: str,
    path: str,
    expected_schema: schema_item.SchemaItem,
    actual_schema: ExprOrView,
) -> arolla.Expr:
  """Produces an error message for type mismatches in tracing mode.

  Args:
    arg_key: The name of the argument if the error is for kd.check_inputs, or
      None if the error is for kd.check_output.
    path: The path to the argument or output. Prefixed by `.` if non-empty.
    expected_schema: The expected schema. Known at tracing time.
    actual_schema: The schema of the argument or output. Unknown at tracing
      time.

  Returns:
    An arolla Expr that evaluates to the error message.
  """
  if arg_key is not None:
    # kd.check_inputs
    return lazy.strings.join(
        f'kd.check_inputs: type mismatch for parameter {arg_key}{path};'
        f' expected type {eager.schema.get_repr(expected_schema)}, got ',
        lazy.schema.get_repr(actual_schema),
    )
  else:
    # kd.check_output
    return lazy.strings.join(
        f'kd.check_output: type mismatch for output{path}; expected type'
        f' {eager.schema.get_repr(expected_schema)}, got ',
        lazy.schema.get_repr(actual_schema),
    )


def _with_lazy_attrribute_verification(
    result: ExprOrView,
    actual_schema: ExprOrView,
    arg_key: Optional[str],
    attr_name: str,
    path: str,
) -> arolla.Expr:
  """Verifies that the actual schema has the given attribute in tracing mode.

  Args:
    result: Node to append the verification assertions to.
    actual_schema: The schema of the argument or output to check for attribute
      presence. Unknown at tracing time.
    arg_key: The name of the argument if the error is for kd.check_inputs, or
      None if the error is for kd.check_output. Known at tracing time.
    attr_name: The name of the attribute to check. Known at tracing time.
    path: The path from the input/output parameter to the current constraint.
      Known at tracing time.

  Returns:
    The result node with the verification assertions added.
  """

  def attribute_missing_error_fn(actual_schema):
    if arg_key is not None:
      # kd.check_inputs
      return lazy.strings.join(
          f'kd.check_inputs: expected parameter {arg_key}{path} to have'
          f' attribute `{attr_name}`; no attribute `{attr_name}` on'
          f' {arg_key}{path}=',
          lazy.schema.get_repr(actual_schema),
      )
    else:
      # kd.check_output
      return lazy.strings.join(
          f'kd.check_output: expected output{path} to have attribute'
          f' `{attr_name}`; no attribute `{attr_name}` on'
          f' output{path}=',
          lazy.schema.get_repr(actual_schema),
      )

  # First assert that the value is not a primitive, and only then call
  # has_attr, otherwise has_attr will fail.
  return lazy.assertion.with_assertion(
      result,
      lazy.assertion.with_assertion(
          actual_schema,
          ~actual_schema.is_primitive(),
          attribute_missing_error_fn,
          actual_schema,
      ).has_attr(attr_name),
      attribute_missing_error_fn,
      actual_schema,
  )


def _with_lazy_constraint_verification(
    result: ExprOrView,
    constraint: TypeConstraint,
    actual_schema: ExprOrView,
    arg_key: Optional[str],
    path: str = '',
) -> ExprOrView:
  """Processes a type constraint verification in tracing mode.

  Args:
    result: Node to append the verification assertions to.
    constraint: The constraint to be verified. Known at tracing time.
    actual_schema: The schema of the argument or output to check against the
      constraint. Unknown at tracing time.
    arg_key: The name of the argument if the error is for kd.check_inputs, or
      None if the error is for kd.check_output. Known at tracing time.
    path: The path from the input/output parameter to the current constraint.
      Known at tracing time.

  Returns:
    The result node with the verification assertions added.
  """
  if isinstance(constraint, _DuckType):
    for attr_name in constraint:
      result = _with_lazy_attrribute_verification(
          result, actual_schema, arg_key, attr_name, path
      )
      result = _with_lazy_constraint_verification(
          result,
          constraint[attr_name],
          actual_schema.get_attr(attr_name),
          arg_key,
          path + '.' + _attr_name_repr(attr_name),
      )
  else:
    result = lazy.assertion.with_assertion(
        result,
        actual_schema == constraint,
        lambda actual_schema: _lazy_type_mismatch_error_message(
            arg_key, path, constraint, actual_schema
        ),
        actual_schema,
    )
  return result


def _verify_input_eager(
    key: str, arg: data_slice.DataSlice, constraint: schema_item.SchemaItem
):
  """Verifies the type of input in eager mode."""
  if not isinstance(arg, (data_item.DataItem, data_slice.DataSlice)):
    # Boxing is needed to support Python arguments.
    arg = py_boxing.as_qvalue(arg)
  _verify_constraint_eager(constraint, arg.get_schema(), key)


def _verify_inputs_eager(
    bound_args: inspect.BoundArguments,
    kw_constraints: Mapping[str, TypeConstraint],
):
  """Verifies the types of inputs in eager mode."""
  for key, constraint in kw_constraints.items():
    # KeyError is not possible as parameter presence was checked during
    # decoration.
    _verify_input_eager(key, bound_args.arguments[key], constraint)


def _verify_output_eager(
    output: data_slice.DataSlice,
    constraint: TypeConstraint,
):
  """Verifies the type of output in eager mode."""
  if not isinstance(output, (data_item.DataItem, data_slice.DataSlice)):
    raise TypeError(
        'kd.check_output: expected DataItem/DataSlice output, got'
        f' {type(output)}'
    )
  _verify_constraint_eager(constraint, output.get_schema(), None)


def _with_input_expr_assertions(
    bound_args: inspect.BoundArguments,
    kw_constraints: Mapping[str, schema_item.SchemaItem],
) -> inspect.BoundArguments:
  """Adds assertions for input types in tracing mode."""
  if _is_traced_type_checking_disabled():
    return bound_args
  for key, constraint in kw_constraints.items():
    # KeyError is not possible as parameter presence was checked during
    # decoration.
    # Boxing is needed to support Python arguments.
    arg = bound_args.arguments[key]
    if not isinstance(arg, ExprOrView):
      # We attempt eager verification of a static input.
      _verify_input_eager(key, arg, constraint)
    else:
      # Changes to bound_args will reflect in bound_args.args and
      # bound_args.kwargs.
      bound_args.arguments[key] = _with_lazy_constraint_verification(
          arg, constraint, lazy.schema.get_schema(arg), key
      )
  return bound_args


def _with_output_expr_assertion(
    output: data_slice.DataSlice,
    constraint: schema_item.SchemaItem,
):
  """Adds an assertion for output type in tracing mode."""
  if _is_traced_type_checking_disabled():
    return output
  return _with_lazy_constraint_verification(
      output, constraint, lazy.schema.get_schema(output), None
  )


def check_inputs(**kw_constraints: TypeConstraint):
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
    **kw_constraints: mapping of parameter names to type constraints. Names must
      match parameter names in the decorated function. Arguments for the given
      parameters must be DataSlices/DataItems that match the given type
      constraint(in particular, for SchemaItems, they must have the
      corresponding schema).

  Returns:
    A decorator that can be used to type annotate a function that accepts
    DataSlices/DataItem inputs.
  """
  for key, value in kw_constraints.items():
    if not isinstance(value, TypeConstraint):
      raise TypeError(
          'kd.check_inputs: invalid constraint: expected constraint for'
          f' parameter {key} to be a schema DataItem or a DuckType, got'
          f' {value}'
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


def check_output(constraint: TypeConstraint):
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
    constraint: A type constraint for the output. Output of the decorated
      function must be a DataSlice/DataItem that matches the constraint(in
      particular, for SchemaItems, they must have the corresponding schema).

  Returns:
    A decorator that can be used to annotate a function returning a
    DataSlice/DataItem.
  """
  if not isinstance(constraint, TypeConstraint):
    raise TypeError(
        'kd.check_output: invalid constraint: expected constraint for output to'
        f' be a schema DataItem or a DuckType, got {constraint}'
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
