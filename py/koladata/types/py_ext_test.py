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

"""Tests for bare_python_c."""

from absl.testing import absltest
from koladata.types import data_item_py_ext
from koladata.types import data_slice_py_ext


class DataItemPyExtTest(absltest.TestCase):

  def test_richcompare_not_implemented(self):
    x = data_item_py_ext.DataItem.from_vals(42)
    y = data_item_py_ext.DataItem.from_vals(42)
    with self.assertRaises(TypeError):
      x == y


class DataSlicePyExtTest(absltest.TestCase):

  def test_hash_not_implemented(self):
    x = data_slice_py_ext.DataSlice.from_vals([42])
    with self.assertRaisesRegex(TypeError, 'unhashable type'):
      hash(x)

  def test_richcompare_not_implemented(self):
    x = data_slice_py_ext.DataSlice.from_vals([42])
    y = data_slice_py_ext.DataSlice.from_vals([42])
    with self.assertRaises(TypeError):
      x == y


if __name__ == '__main__':
  absltest.main()
