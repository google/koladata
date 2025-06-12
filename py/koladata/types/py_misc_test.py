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

from absl.testing import absltest
from absl.testing import parameterized
# This is needed for jagged_shape.create_shape() to work.
from koladata.operators import kde_operators  # pylint: disable=unused-import
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import py_misc_py_ext

ds = data_slice.DataSlice.from_vals


class PyMiscTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          [[1, 2, 3], [4, 5]],
          [1, 2, 3, 4, 5],
          jagged_shape.create_shape(2, ds([3, 2])),
      ),
      (
          ((1, 2, 3), [4, 5]),
          [1, 2, 3, 4, 5],
          jagged_shape.create_shape(2, ds([3, 2])),
      ),
      (57, [57], jagged_shape.create_shape()),
  )
  def test_flatten_py_list(self, x, expected_flat, expected_shape):
    flat_list, shape = py_misc_py_ext.flatten_py_list(x)
    self.assertEqual(flat_list, expected_flat)
    testing.assert_equal(shape, expected_shape)

  def test_flatten_py_list_errors(self):
    with self.assertRaisesRegex(ValueError, 'has to be a valid nested list'):
      _ = py_misc_py_ext.flatten_py_list([[1, 2], 3])


if __name__ == '__main__':
  absltest.main()
