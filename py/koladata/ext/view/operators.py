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

"""Operators that work on views.

The tests are in operator_tests/ folder.
"""

from typing import Any
from koladata.ext.view import view as view_lib


def align(first: Any, *others: Any) -> tuple[view_lib.View, ...]:
  """Aligns the views to a common shape.

  We will also apply auto-boxing if some inputs are not views but can be
  automatically boxed into one.

  Args:
    first: The first argument to align.
    *others: The remaining arguments to align.

  Returns:
    A tuple of aligned views, of size len(others) + 1.
  """
  first = view_lib.box(first)
  if not others:
    return (first,)
  others = tuple(view_lib.box(o) for o in others)
  ref_view = max((first, *others), key=lambda l: l.get_depth())
  return (
      first.expand_to(ref_view),
      *(l.expand_to(ref_view) for l in others),
  )
