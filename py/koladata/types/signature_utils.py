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

"""Tools to create and manipulate Koda functor signatures.

TODO: move this to koladata/base when possible.
"""

import inspect
import types
from typing import Any

from koladata.base import py_functors_base_py_ext
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import py_boxing


# Container for parameter kind constants.
ParameterKind = types.SimpleNamespace(
    POSITIONAL_ONLY=py_functors_base_py_ext.positional_only_parameter_kind(),
    POSITIONAL_OR_KEYWORD=py_functors_base_py_ext.positional_or_keyword_parameter_kind(),
    VAR_POSITIONAL=py_functors_base_py_ext.var_positional_parameter_kind(),
    KEYWORD_ONLY=py_functors_base_py_ext.keyword_only_parameter_kind(),
    VAR_KEYWORD=py_functors_base_py_ext.var_keyword_parameter_kind(),
)

# The constant used to represent no default value in stored signatures.
NO_DEFAULT_VALUE = py_functors_base_py_ext.no_default_value_marker()


def parameter(
    name: str,
    kind: data_slice.DataSlice,
    default_value: Any = NO_DEFAULT_VALUE,
) -> data_slice.DataSlice:
  """Creates a functor parameter.

  Args:
    name: The name of the parameter.
    kind: The kind of the parameter, must be one of the constants from the
      ParameterKind namespace.
    default_value: The default value for the parameter. When set to a special
      constant NO_DEFAULT_VALUE, no default value is used when calling a functor
      with this signature.

  Returns:
    A DataSlice with an item representing the parameter.
  """
  default_value = py_boxing.as_qvalue(default_value)
  if not isinstance(default_value, data_item.DataItem):
    raise ValueError(
        'only DataItems can be used as default values for parameters'
    )
  return data_bag.DataBag._obj_no_bag(name=name, kind=kind, default_value=default_value)  # pylint: disable=protected-access


def signature(parameters: list[data_slice.DataSlice]) -> data_slice.DataSlice:
  """Creates a functor signature.

  Note that this method does no validity checks, so the validity of the
  signature will only be checked when you try to create a functor with this
  signature.

  Args:
    parameters: The list of parameters for the signature, in order.

  Returns:
    A DataSlice representing the signature.
  """
  return data_bag.DataBag._obj_no_bag(  # pylint: disable=protected-access
      parameters=data_bag.DataBag.empty()
      .implode(data_slice.DataSlice.from_vals(parameters))
      .freeze_bag()
  )


def _parameter_from_py_parameter(
    param: inspect.Parameter,
) -> data_slice.DataSlice:
  """Converts a Python parameter to a Koda functor signature parameter."""
  match param.kind:
    case inspect.Parameter.POSITIONAL_ONLY:
      kind = ParameterKind.POSITIONAL_ONLY
    case inspect.Parameter.POSITIONAL_OR_KEYWORD:
      kind = ParameterKind.POSITIONAL_OR_KEYWORD
    case inspect.Parameter.VAR_POSITIONAL:
      kind = ParameterKind.VAR_POSITIONAL
    case inspect.Parameter.KEYWORD_ONLY:
      kind = ParameterKind.KEYWORD_ONLY
    case inspect.Parameter.VAR_KEYWORD:
      kind = ParameterKind.VAR_KEYWORD
    case _:
      raise ValueError(f'Unsupported parameter kind: {param.kind}')
  return parameter(
      name=param.name,
      kind=kind,
      default_value=NO_DEFAULT_VALUE
      if param.default is inspect.Parameter.empty
      else param.default,
  )


def from_py_signature(sig: inspect.Signature) -> data_slice.DataSlice:
  """Converts a Python signature to a Koda functor signature."""
  return signature(
      parameters=[
          _parameter_from_py_parameter(param)
          for param in sig.parameters.values()
      ]
  )


ARGS_KWARGS_SIGNATURE = signature([
    parameter('args', ParameterKind.VAR_POSITIONAL),
    parameter('kwargs', ParameterKind.VAR_KEYWORD),
])
