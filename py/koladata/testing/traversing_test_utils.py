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

"""Testing utilities that traverse DataBags."""

from koladata.testing import traversing_test_utils_py_ext as _traversing_test_utils_py_ext
from koladata.types import data_slice as _data_slice


def assert_data_slice_deep_equivalent(
    actual_value: _data_slice.DataSlice,
    expected_value: _data_slice.DataSlice,
):
  """Koda slices equivalency check.

  * 2 DataSlice(s) are equivalent if their contents and JaggedShape(s) are
    equivalent.
    The content is compared recursively. To be equivalent:
    * objects or entities should have the same set of attributes and the
      attribute values should be equivalent.
    * Dicts should have the same set of keys and the corresponding values should
      be equivalent.
    * Lists should have the same length and elements on corresponding positions
      should be equivalent.

  Args:
    actual_value: DataSlice.
    expected_value: DataSlice.

  Raises:
    AssertionError: If actual_value and expected_value are not equivalent.
  """
  _traversing_test_utils_py_ext.assert_deep_equivalent(
      actual_value, expected_value
  )
