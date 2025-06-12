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
from arolla import arolla
from koladata.base import wrap_utils_testing_clib


class WrapUtilsTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('empty', wrap_utils_testing_clib.make_empty_ds()),
      ('int', wrap_utils_testing_clib.make_int_ds()),
      ('item', wrap_utils_testing_clib.make_text_item_ds()),
  )
  def test_data_slice_roundtrip(self, x):
    y = wrap_utils_testing_clib.unwrap_wrap_data_slice(x)
    arolla.testing.assert_qvalue_equal_by_fingerprint(x, y)

    y = wrap_utils_testing_clib.unsafe_ref_wrap_data_slice(x)
    arolla.testing.assert_qvalue_equal_by_fingerprint(x, y)

    y = wrap_utils_testing_clib.unwrap_optional_wrap_data_slice(x)
    arolla.testing.assert_qvalue_equal_by_fingerprint(x, y)

  def test_data_bag_roundtrip(self):
    db = wrap_utils_testing_clib.make_data_bag()
    y = wrap_utils_testing_clib.unwrap_wrap_data_bag(db)
    arolla.testing.assert_qvalue_equal_by_fingerprint(db, y)

    y = wrap_utils_testing_clib.unwrap_unsafe_wrap_data_bag(db)
    arolla.testing.assert_qvalue_equal_by_fingerprint(db, y)

  def test_jagged_shape_roundtrip(self):
    shape = wrap_utils_testing_clib.make_shape()
    y = wrap_utils_testing_clib.unwrap_wrap_jagged_shape(shape)
    arolla.testing.assert_qvalue_equal_by_fingerprint(shape, y)

  def test_unwrap_data_slice_error(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting input arg to be a DataSlice, got str'
    ):
      _ = wrap_utils_testing_clib.unwrap_wrap_data_slice('abc')

    with self.assertRaisesRegex(
        TypeError,
        'expecting input arg to be a DataSlice, got NoneType',
    ):
      _ = wrap_utils_testing_clib.unwrap_wrap_data_slice(None)

  def test_unwrap_optional_data_slice_error(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting input arg to be a DataSlice, got str'
    ):
      _ = wrap_utils_testing_clib.unwrap_optional_wrap_data_slice('abc')

  def test_unwrap_optional_data_slice_works_with_none(self):
    res = wrap_utils_testing_clib.unwrap_optional_wrap_data_slice(None)
    self.assertIsNone(res)

  def test_unwrap_data_bag_error(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting input arg to be a DataBag, got str'
    ):
      _ = wrap_utils_testing_clib.unwrap_wrap_data_bag('abc')

  def test_wrap_data_bag_roundtrip_works_with_none(self):
    res = wrap_utils_testing_clib.unwrap_wrap_data_bag(None)
    self.assertIsNone(res)

  def test_unwrap_jagged_shape_error(self):
    with self.assertRaisesRegex(
        TypeError,
        'expecting input arg to be a JaggedShape, got str',
    ):
      _ = wrap_utils_testing_clib.unwrap_wrap_jagged_shape('abc')

    with self.assertRaisesRegex(
        TypeError,
        'expecting input arg to be a JaggedShape, got NoneType',
    ):
      _ = wrap_utils_testing_clib.unwrap_wrap_jagged_shape(None)


if __name__ == '__main__':
  absltest.main()
