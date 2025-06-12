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
from koladata.base import to_py_object_testing_clib
from koladata.functions import functions as fns
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals


class ToPyObjectTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(123, schema_constants.INT32), 123),
      (ds(123, schema_constants.INT64), 123),
      (ds(0.5, schema_constants.FLOAT32), 0.5),
      (ds(0.5, schema_constants.FLOAT64), 0.5),
      (ds(True), True),
      (ds("abc"), "abc"),
  )
  def test_object_from_data_item(self, value, expected):
    res = to_py_object_testing_clib.py_object_from_data_item(value)
    self.assertEqual(res, expected)

  def test_object_from_data_item_with_object(self):
    obj = fns.obj(x=123, y=ds(456))
    res = to_py_object_testing_clib.py_object_from_data_item(obj)
    self.assertEqual(res, obj)

  @parameterized.parameters(
      (ds(123, schema_constants.INT32), 123),
      (ds(123, schema_constants.INT64), 123),
      (ds(0.5, schema_constants.FLOAT32), 0.5),
      (ds(0.5, schema_constants.FLOAT64), 0.5),
      (ds(True), True),
      (ds("abc"), "abc"),
      (ds([1, 2, 3]), [1, 2, 3]),
      (ds([[1, 2], [3, 4]]), [[1, 2], [3, 4]]),
  )
  def test_object_from_data_slice(self, value, expected):
    res = to_py_object_testing_clib.py_object_from_data_slice(value)
    self.assertEqual(res, expected)


if __name__ == "__main__":
  absltest.main()
