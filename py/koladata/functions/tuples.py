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

"""Koda functions for working with tuples."""

from typing import Any, SupportsIndex

from arolla import arolla
from koladata.types import data_item
from koladata.types import data_slice as _  # pylint: disable=unused-import
from koladata.types import py_boxing


# NOTE: Implemented directly as a function since `n` is required to be a
# literal which does not play well with eager-operator evaluation.
def get_nth(x: Any, n: SupportsIndex) -> arolla.AnyQValue:
  """Returns the nth element of the tuple `x`.

  Args:
    x: a tuple.
    n: the index of the element to return. Must be in the range [0, len(x)).
  """
  try:
    n = n.__index__()
  except (AttributeError, ValueError) as ex:
    raise TypeError(f'expected an index-value, got: {n}') from ex
  x_qv = py_boxing.as_qvalue(x)
  if not isinstance(x_qv, arolla.types.Tuple):
    raise TypeError(f'expected a value convertible to a Tuple, got: {x}')
  if not 0 <= n < len(x_qv):
    raise ValueError(f'expected 0 <= n < len(x), got: {n=}, len(x)={len(x_qv)}')
  return x_qv[n]


# NOTE: Implemented directly as a function since `field_name` is required to be
# a literal which does not play well with eager-operator evaluation.
def get_namedtuple_field(
    namedtuple: arolla.types.NamedTuple, field_name: str | data_item.DataItem
) -> arolla.AnyQValue:
  """Returns the value of the specified `field_name` from the `namedtuple`.

  Args:
    namedtuple: a namedtuple.
    field_name: the name of the field to return.
  """
  field_name_str = (
      field_name.internal_as_py()
      if isinstance(field_name, data_item.DataItem)
      else field_name
  )
  if not isinstance(field_name_str, str):
    raise TypeError(f'expected a value convertible to a str, got: {field_name}')
  if not isinstance(namedtuple, arolla.types.NamedTuple):
    raise TypeError(f'expected a NamedTuple, got: {namedtuple}')
  return namedtuple[field_name_str]
