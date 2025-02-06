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
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice

kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals


class MergeTest(absltest.TestCase):

  def test_merge_objs_created_via_itemid_multitype(self):
    db1 = bag()
    db2 = bag()
    o1 = db1.obj(
        x=ds([(1 if i % 2 == 0 else None) for i in range(1000)])
    )
    expected = o1.x
    db2.obj(
        x=ds([(i * 2.0 if i % 2 == 1 else None) for i in range(1000)]),
        itemid=o1.get_itemid(),
    )
    db1.merge_inplace(db2, allow_data_conflicts=True, overwrite=False)
    testing.assert_equal(o1.x, expected)

  def test_merge_objs_created_via_itemid_same_type(self):
    db1 = bag()
    db2 = bag()
    o1 = db1.obj(
        x=ds([(1 if i % 2 == 0 else None) for i in range(1000)])
    )
    expected = o1.x
    db2.obj(
        x=ds([(2 if i % 2 == 1 else None) for i in range(1000)]),
        itemid=o1.get_itemid(),
    )
    db1.merge_inplace(db2, allow_data_conflicts=True, overwrite=False)
    testing.assert_equal(o1.x, expected)


if __name__ == '__main__':
  absltest.main()
