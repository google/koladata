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

import re
from absl.testing import absltest
from koladata import kd
from koladata.ext.view import test_utils
from koladata.ext.view import view as view_lib


class TestUtilsTest(absltest.TestCase):

  def test_assert_equal_success(self):
    test_utils.assert_equal(view_lib.view(1), view_lib.view(1))
    test_utils.assert_equal(view_lib.view([1, 2])[:], view_lib.view([1, 2])[:])

  def test_assert_equal_not_a_view(self):
    with self.assertRaisesRegex(
        AssertionError, re.escape("Expected a View, got <class 'int'>")
    ):
      test_utils.assert_equal(view_lib.view(1), 1)  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        AssertionError, re.escape("Expected a View, got <class 'int'>")
    ):
      test_utils.assert_equal(1, view_lib.view(1))  # pytype: disable=wrong-arg-types

  def test_assert_equal_different_depths(self):
    with self.assertRaisesRegex(
        AssertionError, "View depths are not equal: 1 != 0"
    ):
      test_utils.assert_equal(view_lib.view([1])[:], view_lib.view([1]))

  def test_assert_equal_different_contents(self):
    with self.assertRaisesRegex(
        AssertionError, "View contents are not equal: 1 != 2"
    ):
      test_utils.assert_equal(view_lib.view(1), view_lib.view(2))

  def test_from_ds(self):
    kd_item = kd.item(None)
    view_item = test_utils.from_ds(kd_item)
    test_utils.assert_equal(view_item, view_lib.view(None))

    kd_slice = kd.slice([1, 2, 3])
    view = test_utils.from_ds(kd_slice)
    test_utils.assert_equal(view, view_lib.view([1, 2, 3])[:])

    kd_slice_2d = kd.slice([[1, 2], [3]])
    view_2d = test_utils.from_ds(kd_slice_2d)
    test_utils.assert_equal(view_2d, view_lib.view([[1, 2], [3]])[:][:])

    kd_slice_of_objects = kd.obj(x=kd.slice([1, 2, 3]))
    view_of_objects = test_utils.from_ds(kd_slice_of_objects)
    test_utils.assert_equal(view_of_objects.x, view_lib.view([1, 2, 3])[:])

  def test_from_ds_not_a_dataslice(self):
    with self.assertRaisesRegex(
        AssertionError, re.escape("Expected a DataSlice, got <class 'int'>")
    ):
      test_utils.from_ds(1)  # pytype: disable=wrong-arg-types


if __name__ == "__main__":
  absltest.main()
