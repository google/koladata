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
from arolla.derived_qtype import derived_qtype
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import extension_types as ext_types
from koladata.types import qtypes

M = arolla.M | derived_qtype.M


_EXT_TYPE = M.derived_qtype.get_labeled_qtype(qtypes.DATA_SLICE, 'foo').qvalue


class ExtensionTypesTest(parameterized.TestCase):

  def test_is_extension_type(self):
    self.assertFalse(
        ext_types.is_koda_extension(data_slice.DataSlice.from_vals([1, 2, 3]))
    )
    self.assertTrue(
        ext_types.is_koda_extension(
            ext_types.wrap(data_slice.DataSlice.from_vals([1, 2, 3]), _EXT_TYPE)
        )
    )

  def test_wrap_unwrap(self):
    ds = data_slice.DataSlice.from_vals([1, 2, 3])
    wrapped_ds = ext_types.wrap(ds, _EXT_TYPE)
    self.assertEqual(wrapped_ds.qtype, _EXT_TYPE)
    unwrapped_ds = ext_types.unwrap(wrapped_ds)
    self.assertIsInstance(unwrapped_ds, data_slice.DataSlice)
    testing.assert_equal(unwrapped_ds, ds)

  def test_wrap_invalid_input(self):
    with self.assertRaisesRegex(ValueError, 'expected a DataSlice'):
      ext_types.wrap(123, _EXT_TYPE)
    with self.assertRaisesRegex(ValueError, 'expected an extension type'):
      ext_types.wrap(data_slice.DataSlice.from_vals([1, 2, 3]), arolla.INT32)

  def test_unwrap_invalid_input(self):
    with self.assertRaisesRegex(ValueError, 'expected an extension type'):
      ext_types.unwrap(data_slice.DataSlice.from_vals([1, 2, 3]))


if __name__ == '__main__':
  absltest.main()
