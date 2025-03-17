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

import functools
import inspect

from koladata.expr import tracing_mode
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import schema_constants


def check_inputs(**kw_constraints: data_item.DataItem):
  """Decorator factory for adding runtime input type checking to Koda functions.

  Resulting decorators will check the schemas of DataSlice inputs of
  a function at runtime, and raise TypeError in case of mismatch.

  Decorated functions will preserve the original function's signature and
  docstring.

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
    if value.get_schema() != schema_constants.SCHEMA:
      raise TypeError(
          'kd.check_inputs: invalid constraint: expected constraint for'
          f' parameter `{key}` to be a schema DataItem, got'
          f' {value.get_schema()}'
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
      # TODO: Support traced constraints/assertions.
      if tracing_mode.is_tracing_enabled():
        return f(*args, **kwargs)
      bound_args = signature.bind(*args, **kwargs)
      bound_args.apply_defaults()
      for key, type_constraint in kw_constraints.items():
        # KeyError is not possible as parameter presence was checked during
        # decoration.
        arg = bound_args.arguments[key]
        if not isinstance(arg, (data_item.DataItem, data_slice.DataSlice)):
          raise TypeError(
              'kd.check_inputs: cannot check type on non-DataSlice'
              f' argument for parameter `{key}`'
          )
        if arg.get_schema() != type_constraint:
          raise TypeError(
              f'kd.check_inputs: type mismatch for parameter `{key}`. Expected'
              f' type {type_constraint}, got {arg.get_schema()}'
          )
      return f(*args, **kwargs)

    return functools.wraps(f)(check_inputs_decorator)

  return decorate_f


def check_output(output: data_item.DataItem):
  """Decorator factory for adding runtime output type checking to Koda functions.

  Resulting decorators will check the schema of the DataSlice output of
  a function at runtime, and raise TypeError in case of mismatch.

  Decorated functions will preserve the original function's signature and
  docstring.

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
    output: A DataItem schema for the output. Output of the decorated function
      must be a DataSlice/DataItem with the corresponding schema.

  Returns:
    A decorator that can be used to annotate a function returning a
    DataSlice/DataItem.
  """
  if output.get_schema() != schema_constants.SCHEMA:
    raise TypeError(
        'kd.check_output: invalid constraint: expected constraint for output to'
        f' be a schema DataItem, got {output.get_schema()}'
    )

  def decorate_f(f):
    def check_output_decorator(*args, **kwargs):
      # TODO: Support traced constraints/assertions.
      if tracing_mode.is_tracing_enabled():
        return f(*args, **kwargs)
      res = f(*args, **kwargs)
      if not isinstance(res, (data_item.DataItem, data_slice.DataSlice)):
        raise TypeError(
            'kd.check_output: expected DataItem/DataSlice output, got'
            f' {type(res)}'
        )
      if res.get_schema() != output:
        raise TypeError(
            'kd.check_output: type mismatch for output. Expected'
            f' type {output}, got {res.get_schema()}'
        )
      return res

    return functools.wraps(f)(check_output_decorator)

  return decorate_f
