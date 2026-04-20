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
from __future__ import annotations

from collections.abc import Mapping
import contextlib
import dataclasses
import functools
import inspect
from typing import Any, TypeAlias

from arolla import arolla
from koladata.expr import tracing_mode
from koladata.functor import functor_factories
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import schema_constants
from koladata.types import schema_item
from koladata.types import signature_utils

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

TypeConstraint: TypeAlias = (
    'schema_item.SchemaItem | _DuckType | _StaticWhenTraced | _FunctorType'
)


@dataclasses.dataclass(frozen=True)
class _StaticWhenTraced(object):
  base_type: TypeConstraint | None = None


class _DuckType(object):
  """Duck type object.

  It is used to specify the constraint that a schema have a superset of the
  specified attributes, as well as recursive constraints for those attributes.

  A duck type is essentially a tree where internal nodes are duck types and
  leaves are concrete schemas.
  """

  def __init__(self, **kwargs: TypeConstraint):
    self.__dict__['_fields'] = kwargs

  def __iter__(self):
    return iter(self._fields)

  def __getattr__(self, key: str) -> TypeConstraint:
    try:
      return self._fields[key]
    except KeyError as e:
      raise AttributeError(key) from e

  def __getitem__(self, key: str):
    return self._fields[key]

  def __setattr__(self, key: str, value: TypeConstraint):
    raise NotImplementedError(
        'DuckType does not support setattr. All attributes must be specified at'
        ' construction.'
    )


@dataclasses.dataclass(frozen=True)
class _FunctorType(object):
  signature: inspect.Signature | None = None


def static_when_tracing(
    base_type: TypeConstraint | None = None,
) -> _StaticWhenTraced:
  """A constraint that the argument is static when tracing.

  It is used to check that the argument is not an expression during tracing to
  prevent a common mistake.

  Examples:
  - combined with checking the type:
    @type_checking.check_inputs(value=kd.static_when_tracing(kd.INT32))
  - without checking the type:
    @type_checking.check_inputs(pick_a=kd.static_when_tracing())

  Args:
    base_type: (optional)The base type to check against. If not specified, only
      checks that the argument is a static when tracing.

  Returns:
    A constraint that the argument is a static when tracing.
  """
  return _StaticWhenTraced(base_type)


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
    **kwargs: mapping of attribute names to constraints. They can be any other
      kind of TypeConstraint. To assert only the presence of an
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
    item_constraint:  TypeConstraint representing the constraint to be
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
    key_constraint:  TypeConstraint representing the constraint to be
      checked on the keys of the dict.
    value_constraint:  TypeConstraint representing the constraint to be
      checked on the values of the dict.

  Returns:
    A duck type constraint to be used in kd.check_inputs or kd.check_output.
  """
  return _DuckType(
      __keys__=key_constraint,
      __values__=value_constraint,
  )


def functor(signature: inspect.Signature | None = None) -> _FunctorType:
  """Returns constraint that an argument is a functor.

  Optionally checking signature.

  Args:
    signature: An optional `inspect.Signature` to check against the functor's
      signature. If None, only checks that the argument is a functor.
  """
  return _FunctorType(signature)


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


def _verify_constraint_eager(
    constraint: TypeConstraint,
    actual_value: data_slice.DataSlice,
    arg_key: str | None,
    path: str = '',
):
  """Processes a type constraint verification in eager mode.

  Args:
    constraint: The constraint to be verified.
    actual_value: The argument or output to check against the constraint.
    arg_key: The name of the argument if the error is for kd.check_inputs, or
      None if the error is for kd.check_output.
    path: The path from the input/output parameter to the current constraint.

  Raises:
    TypeError: If the constraint is not satisfied by the actual value.
  """
  error_message_prefix = (
      'kd.check_inputs:' if arg_key is not None else 'kd.check_output:'
  )
  attr_path = (
      f'parameter {arg_key}{path}' if arg_key is not None else f'output{path}'
  )
  if isinstance(constraint, _DuckType):
    actual_schema = actual_value.get_schema()
    for attr_name in constraint:
      if eager.is_primitive(actual_schema) or not eager.has_attr(
          actual_schema, attr_name
      ):
        raise TypeError(
            f'{error_message_prefix} expected {attr_path} '
            f'to have attribute `{attr_name}`; no attribute `{attr_name}` on '
            f'{attr_path}={eager.schema.get_repr(actual_schema)}'
            + (
                f'\n\n{OBJECT_TIP}'
                if actual_schema == schema_constants.OBJECT
                else ''
            )
        )
      _verify_constraint_eager(
          constraint[attr_name],
          eager.get_attr(actual_value, attr_name),
          arg_key,
          path + '.' + _attr_name_repr(attr_name),
      )
  elif isinstance(constraint, _StaticWhenTraced):
    if constraint.base_type is None:
      return
    _verify_constraint_eager(
        constraint.base_type, actual_value, arg_key, path
    )
  elif isinstance(constraint, _FunctorType):
    if not eager.functor.is_fn(actual_value):
      raise TypeError(
          f'{error_message_prefix} expected {attr_path} '
          'to be a functor, got '
          f'{eager.slices.get_repr(actual_value)}'
      )
    if constraint.signature is not None:
      assert isinstance(
          actual_value, data_item.DataItem
      )  # narrow for pytype
      actual_signature = functor_factories.get_inspect_signature(actual_value)
      if actual_signature != constraint.signature:
        raise TypeError(
            f'{error_message_prefix} expected {attr_path} '
            f'to be a functor with signature {constraint.signature}, '
            f'got a functor with signature {actual_signature}'
        )
  else:
    actual_schema = actual_value.get_schema()
    if actual_schema != constraint:
      raise TypeError(
          f'{error_message_prefix} type mismatch for {attr_path}; expected type'
          f' {eager.schema.get_repr(constraint)}, got'
          f' {eager.schema.get_repr(actual_schema)}'
          + (
              f'\n\n{OBJECT_TIP}'
              if actual_schema == schema_constants.OBJECT
              else ''
          )
      )


def _with_lazy_attrribute_verification(
    result: arolla.Expr,
    actual_schema: arolla.Expr,
    arg_key: str | None,
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
  error_message_prefix = (
      'kd.check_inputs:'
      if arg_key is not None
      else 'kd.check_output:'
  )
  attr_path = (
      f'parameter {arg_key}{path}' if arg_key is not None else f'output{path}'
  )

  def attribute_missing_error_fn(actual_schema):
    return lazy.strings.join(
        f'{error_message_prefix} expected {attr_path} to have'
        f' attribute `{attr_name}`; no attribute `{attr_name}` on'
        f' {attr_path}=',
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
    result: arolla.Expr | data_slice.DataSlice,
    constraint: TypeConstraint,
    actual_value: arolla.Expr,
    arg_key: str | None,
    path: str = '',
) -> arolla.Expr | data_slice.DataSlice:
  """Processes a type constraint verification in tracing mode.

  Args:
    result: Node to append the verification assertions to.
    constraint: The constraint to be verified. Known at tracing time.
    actual_value: The argument or output to check against the constraint.
      Unknown at tracing time.
    arg_key: The name of the argument if the error is for kd.check_inputs, or
      None if the error is for kd.check_output. Known at tracing time.
    path: The path from the input/output parameter to the current constraint.
      Known at tracing time.

  Returns:
    The result node with the verification assertions added.
  """
  error_message_prefix = (
      'kd.check_inputs:'
      if arg_key is not None
      else 'kd.check_output:'
  )
  attr_path = (
      f'parameter {arg_key}{path}' if arg_key is not None else f'output{path}'
  )
  if isinstance(constraint, _DuckType):
    actual_schema = lazy.schema.get_schema(actual_value)
    for attr_name in constraint:
      result = _with_lazy_attrribute_verification(
          result, actual_schema, arg_key, attr_name, path
      )
      result = _with_lazy_constraint_verification(
          result,
          constraint[attr_name],
          actual_value.get_attr(attr_name),
          arg_key,
          path + '.' + _attr_name_repr(attr_name),
      )
  elif isinstance(constraint, _StaticWhenTraced):
    raise TypeError((
        f'{error_message_prefix} {attr_path} must be resolved'
        ' statically during tracing and cannot depend on the inputs'
    ))
  elif isinstance(constraint, _FunctorType):
    result = lazy.assertion.with_assertion(
        result,
        lazy.functor.is_fn(actual_value),
        lambda actual_value: lazy.strings.join(
            f'{error_message_prefix} expected {attr_path} to be a'
            ' functor, got ',
            lazy.slices.get_repr(actual_value)
        ),
        actual_value,
    )
    if constraint.signature is not None:
      result = lazy.assertion.with_assertion(
          result,
          lazy.deep_uuid(actual_value.get_attr('__signature__'))
          == lazy.deep_uuid(
              signature_utils.from_py_signature(constraint.signature)
          ),
          lambda actual_value: lazy.strings.join(
              f'{error_message_prefix} expected {attr_path} to be a functor'
              f' with signature {constraint.signature}, got a functor with'
              ' signature ',
              lazy.slices.get_repr(actual_value.get_attr('__signature__')),
          ),
          actual_value,
      )
  else:
    actual_schema = lazy.schema.get_schema(actual_value)
    result = lazy.assertion.with_assertion(
        result,
        actual_schema == constraint,
        lambda actual_schema: lazy.strings.join(
            f'{error_message_prefix} type mismatch for {attr_path};'
            ' expected type ',
            lazy.schema.get_repr(constraint),
            ', got ',
            lazy.schema.get_repr(actual_schema),
        ),
        actual_schema,
    )
  return result


def _verify_input_eager(
    key: str, arg: Any, constraint: TypeConstraint
):
  """Verifies the type of input in eager mode."""
  # Short-circuit boxing for _StaticWhenTraced(None).
  if isinstance(constraint, _StaticWhenTraced) and constraint.base_type is None:
    return

  if not isinstance(arg, (data_item.DataItem, data_slice.DataSlice)):
    # Boxing is needed to support Python arguments.
    arg = py_boxing.as_qvalue(arg)

  _verify_constraint_eager(constraint, arg, key)


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
  if isinstance(constraint, _StaticWhenTraced):
    raise TypeError('_ConstantWhenTraced is not supported for kd.check_output')
  _verify_constraint_eager(constraint, output, None)


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
    arg = bound_args.arguments[key]
    if not isinstance(arg, arolla.Expr):
      # We attempt eager verification of a static input.
      _verify_input_eager(key, arg, constraint)
    else:
      # Changes to bound_args will reflect in bound_args.args and
      # bound_args.kwargs.
      bound_args.arguments[key] = _with_lazy_constraint_verification(
          arg, constraint, arg, key
      )
  return bound_args


def _with_output_expr_assertion(
    output: data_slice.DataSlice | arolla.Expr,
    constraint: schema_item.SchemaItem,
):
  """Adds an assertion for output type in tracing mode."""
  if _is_traced_type_checking_disabled():
    return output
  if not isinstance(output, arolla.Expr):
    # We attempt eager verification of a static output.
    _verify_output_eager(output, constraint)
    return output
  return _with_lazy_constraint_verification(
      output, constraint, output, None
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

  Example for an argument that should not be an Expr at tracing time:
    @kd.check_inputs(x=kd.static_when_tracing(kd.INT32))
    def f(x):
      return x


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
    if not isinstance(
        value,
        (schema_item.SchemaItem, _DuckType, _StaticWhenTraced, _FunctorType),
    ):
      raise TypeError(
          'kd.check_inputs: invalid constraint: expected constraint for'
          f' parameter {key} to be a schema DataItem, a DuckType, a'
          f' FunctorType, or a StaticWhenTraced, got {value}'
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
  if not isinstance(
      constraint,
      (schema_item.SchemaItem, _DuckType, _StaticWhenTraced, _FunctorType),
  ):
    raise TypeError(
        'kd.check_output: invalid constraint: expected constraint for output'
        ' to be a schema DataItem, a DuckType, a FunctorType, or'
        f' a StaticWhenTraced, got {constraint}'
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
