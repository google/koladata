# Copyright 2024 Google LLC
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

"""Tests for mask_constants."""

from absl.testing import absltest
from koladata.operators import eager_op_utils
from koladata.testing import testing
from koladata.types import data_item
from koladata.types import mask_constants
from koladata.types import schema_constants


kd = eager_op_utils.operators_container('kde')


class MaskConstantsTest(absltest.TestCase):

  def test_basic(self):
    self.assertEqual(mask_constants.present.get_schema(), schema_constants.MASK)
    self.assertEqual(mask_constants.missing.get_schema(), schema_constants.MASK)
    self.assertEqual(
        kd.has(data_item.DataItem.from_vals(1)), mask_constants.present
    )
    # NOTE: `==` on missing items returns missing and bool(missing) is False.
    testing.assert_equal(
        kd.has(data_item.DataItem.from_vals(None)), mask_constants.missing
    )

  def test_repr(self):
    self.assertEqual(
        repr(mask_constants.present), 'DataItem(present, schema: MASK)'
    )
    self.assertEqual(
        repr(mask_constants.missing), 'DataItem(None, schema: MASK)'
    )


if __name__ == '__main__':
  absltest.main()
