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
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.testdata import masking_coalesce_testdata
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import schema_constants

I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE

QTYPES = [
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
]


class MaskingCoalesceTest(parameterized.TestCase):

  @parameterized.parameters(*masking_coalesce_testdata.TEST_CASES)
  def test_eval(self, x, y, expected):
    testing.assert_equal(kd.masking.coalesce(x, y), expected)

  def test_merging(self):
    mask = ds([arolla.present(), None])
    x = data_bag.DataBag.empty_mutable().new(a=ds([1, 1])) & mask
    x.get_schema().a = schema_constants.OBJECT
    y = data_bag.DataBag.empty_mutable().new(x=ds([1, 1])).with_schema(
        x.get_schema().no_bag()
    ) & (~mask)
    y.set_attr('a', ds(['abc', 'xyz'], schema_constants.OBJECT))
    self.assertNotEqual(x.get_bag().fingerprint, y.get_bag().fingerprint)
    x_extracted = kd.extract(x)
    testing.assert_equivalent(
        kd.masking.coalesce(x, y).a,
        ds([1, 'xyz']).with_bag(x_extracted.get_bag()).enriched(y.get_bag()),
    )

  def test_extraction(self):
    # Regression test for b/408434629.
    db = data_bag.DataBag.empty_mutable()
    lists = ds([db.list([1, 2]), db.list([3, 4])]).freeze_bag()
    l1 = (lists & ds([mask_constants.present, None])).with_list_append_update(8)
    l2 = (lists & ds([None, mask_constants.present])).with_list_append_update(9)
    res = kd.masking.coalesce(l1, l2)
    testing.assert_equal(res[:].no_bag(), ds([[1, 2, 8], [3, 4, 9]]))

  def test_same_bag(self):
    db = data_bag.DataBag.empty_mutable()
    x = db.new()
    y = db.new().with_schema(x.get_schema())
    testing.assert_equal(kd.masking.coalesce(x, y), x)

  def test_incompatible_schema_error(self):
    x = ds([1, None])
    y = data_bag.DataBag.empty_mutable().new()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            r"""kd.masking.coalesce: arguments do not have a common schema.

Schema for `x`: INT32
Schema for `y`: ENTITY()"""
        ),
    ):
      kd.masking.coalesce(x, y)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.masking.coalesce,
        QTYPES,
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_repr(self):
    self.assertEqual(repr(kde.masking.coalesce(I.x, I.y)), 'I.x | I.y')
    self.assertEqual(repr(kde.coalesce(I.x, I.y)), 'I.x | I.y')

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.masking.coalesce(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.masking.coalesce, kde.coalesce))


if __name__ == '__main__':
  absltest.main()
