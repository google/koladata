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

"""Eager operations used by Koda types, but not specific to one of them.

This file is in types/ and not in operators/ because it is not a Expr operator,
and because it is used by the Koda type implementations in this folder.
"""

from typing import Any

from arolla import arolla


def with_name(obj: Any, name: str | arolla.types.Text) -> Any:
  """Checks that the `name` is a string and returns `obj` unchanged.

  This method is useful in tracing workflows: when tracing, we will assign
  the given name to the subexpression computing `obj`. In eager mode, this
  method is effectively a no-op.

  Args:
    obj: Any object.
    name: The name to be used for this sub-expression when tracing this code.
      Must be a string.

  Returns:
    obj unchanged.
  """
  if not isinstance(name, (str, arolla.types.Text)):
    raise ValueError('Name must be a string')
  return obj
