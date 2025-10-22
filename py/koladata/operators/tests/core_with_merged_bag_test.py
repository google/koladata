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
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([(DATA_SLICE, DATA_SLICE)])


class CoreWithMergedBagTest(parameterized.TestCase):

  def test_eval(self):
    db1 = bag()
    x = db1.new(a=1)
    db2 = bag()
    y = x.with_bag(db2)
    y.set_attr('a', 2)
    y.set_attr('b', 2)
    z = x.enriched(db2)

    new_z = kd.core.with_merged_bag(z)
    self.assertIsNot(new_z.get_bag(), db1)
    self.assertIsNot(new_z.get_bag(), db2)
    self.assertIsNot(new_z.get_bag(), z.get_bag())
    self.assertFalse(new_z.get_bag().is_mutable())
    testing.assert_equal(new_z.a.no_bag(), ds(1))
    testing.assert_equal(new_z.b.no_bag(), ds(2))

  def test_no_bag_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'kd.core.with_merged_bag: expect the DataSlice to have a DataBag'
        ' attached',
    ):
      kd.core.with_merged_bag(ds(1))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.with_merged_bag,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.with_merged_bag(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.with_merged_bag, kde.with_merged_bag)
    )


if __name__ == '__main__':
  absltest.main()
