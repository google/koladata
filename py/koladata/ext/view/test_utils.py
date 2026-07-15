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

"""Testing utilities for Koda View."""

from koladata import kd
from koladata.ext.view import mask_constants
from koladata.ext.view import view as view_lib


def assert_equal(a: view_lib.View, b: view_lib.View):
  """Asserts that two views are the same."""
  if not isinstance(a, view_lib.View):
    raise AssertionError(f'Expected a View, got {type(a)}')
  if not isinstance(b, view_lib.View):
    raise AssertionError(f'Expected a View, got {type(b)}')
  if a.get_depth() != b.get_depth():
    raise AssertionError(
        f'View depths are not equal: {a.get_depth()} != {b.get_depth()}'
    )
  if a.get() != b.get():
    raise AssertionError(f'View contents are not equal: {a.get()} != {b.get()}')


def from_ds(a: kd.types.DataSlice) -> view_lib.View:
  """Converts a DataSlice to a View for testing.

  It will first convert the DataSlice to Python, so it might lose some
  information such as INT32/INT64 distinction, or the schemas for
  entities/objects.

  Args:
    a: The DataSlice to convert.

  Returns:
    The View with the same data and shape as the DataSlice.
  """
  if not isinstance(a, kd.types.DataSlice):
    raise AssertionError(f'Expected a DataSlice, got {type(a)}')
  res = view_lib.view(a.to_py(max_depth=-1)).explode(a.get_ndim().to_py())
  if kd.get_primitive_schema(a) == kd.MASK:
    res = res.map(lambda x: mask_constants.present if x else None)
  return res
