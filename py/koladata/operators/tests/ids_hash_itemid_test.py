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
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants


I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE


class IdsHashItemIdTest(parameterized.TestCase):

  @parameterized.parameters(
      (bag().new(a=1),),
      (bag().new(a=ds([1, 2])),),
      (kd.allocation.new_listid(),),
      (kd.allocation.new_dictid(),),
      (kd.allocation.new_listid_like(ds([[1, None], [3]])),),
      (kd.allocation.new_dictid_like(ds([[1, None], [3]])),),
  )
  def test_eval(self, ids):
    hash1 = kd.ids.hash_itemid(ids)
    hash2 = kd.ids.hash_itemid(ids)
    testing.assert_equal(hash1, hash2)
    testing.assert_equal(hash1.get_dtype(), schema_constants.INT64)

  def test_different_hash_values(self):
    hash1 = kd.ids.hash_itemid(bag().new(a=1))
    hash2 = kd.ids.hash_itemid(bag().new(a=1))
    self.assertNotEqual(hash1.fingerprint, hash2.fingerprint)

  def test_error(self):
    with self.assertRaisesRegex(
        ValueError, 'only ItemIds can be encoded, got INT32'
    ):
      kd.ids.hash_itemid(ds([1, 2, 3]))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.ids.hash_itemid,
        [(DATA_SLICE, DATA_SLICE)],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_nonnegative(self):
    ids = kd.allocation.new_itemid_like(ds(list(range(1000))))
    hash_values = kd.ids.hash_itemid(ids)
    predicate = kd.all(hash_values >= 0)
    self.assertTrue(predicate)

  def test_full_range(self):
    ids = kd.allocation.new_itemid_like(ds(list(range(1000))))
    hash_values = kd.ids.hash_itemid(ids)
    # 0.9**1000 < 2e-46, so this will never fail.
    predicate = kd.any(hash_values < 2**63 // 10) & kd.any(
        hash_values > 2**63 - 2**63 // 10
    )
    self.assertTrue(predicate)

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.ids.hash_itemid(I.ds)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.ids.hash_itemid, kde.hash_itemid))


if __name__ == '__main__':
  absltest.main()
