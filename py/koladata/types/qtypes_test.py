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

from absl.testing import absltest
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import ellipsis
from koladata.types import qtypes


class QTypesTest(absltest.TestCase):

  def test_data_slice_qtype(self):
    self.assertEqual(
        data_slice.DataSlice.from_vals([1, 2, 3]).qtype, qtypes.DATA_SLICE
    )

  def test_data_item_qtype(self):
    self.assertEqual(data_item.DataItem.from_vals(1).qtype, qtypes.DATA_SLICE)

  def test_data_bag_qtype(self):
    self.assertEqual(data_bag.DataBag.empty().qtype, qtypes.DATA_BAG)

  def test_jagged_shape_qtype(self):
    self.assertEqual(
        data_slice.DataSlice.from_vals([1, 2, 3]).get_shape().qtype,
        qtypes.JAGGED_SHAPE,
    )

  def test_ellipsis_qtype(self):
    self.assertEqual(ellipsis.ellipsis().qtype, qtypes.ELLIPSIS)

  def test_non_deterministic_qtype(self):
    self.assertEqual(
        qtypes.NON_DETERMINISTIC_TOKEN, qtypes.NON_DETERMINISTIC_TOKEN
    )


if __name__ == '__main__':
  absltest.main()
