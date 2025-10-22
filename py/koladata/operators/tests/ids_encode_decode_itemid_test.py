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
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE


class IdsEncodeDecodeItemIdTest(parameterized.TestCase):

  @parameterized.parameters(
      (bag().new(a=1),),
      (bag().new(a=ds([1, 2])),),
      (kd.allocation.new_listid(),),
      (kd.allocation.new_dictid(),),
      (kd.allocation.new_listid_like(ds([[1, None], [3]])),),
      (kd.allocation.new_dictid_like(ds([[1, None], [3]])),),
  )
  def test_encode_decode(self, ids):
    encoded = kd.ids.encode_itemid(ids)
    self.assertFalse(encoded.has_bag())
    if isinstance(encoded, data_item.DataItem):
      self.assertRegex(str(encoded), '[0-9a-zA-Z]{22}')
    else:
      for item in encoded.L:
        self.assertRegex(str(item), '[0-9a-zA-Z]{22}')
    decoded = kd.ids.decode_itemid(encoded)
    self.assertFalse(decoded.has_bag())
    testing.assert_equal(ids.no_bag().get_itemid(), decoded)

  def test_error(self):
    with self.assertRaisesRegex(
        ValueError, 'only ItemIds can be encoded, got INT32'
    ):
      kd.ids.encode_itemid(ds([1, 2, 3]))
    with self.assertRaisesRegex(
        ValueError, 'cannot use encode_itemid on primitives'
    ):
      kd.ids.encode_itemid(ds([1, 2, 3], schema=schema_constants.OBJECT))
    with self.assertRaisesRegex(
        ValueError, 'only STRING can be decoded, got INT32'
    ):
      kd.ids.decode_itemid(ds([1, 2, 3]))
    with self.assertRaisesRegex(
        ValueError, re.escape('only STRING can be decoded, got ENTITY(a=INT32)')
    ):
      kd.ids.decode_itemid(bag().new(a=ds([1, 2, 3])))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.ids.encode_itemid,
        [(DATA_SLICE, DATA_SLICE)],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.ids.encode_itemid(I.ds)))
    self.assertTrue(view.has_koda_view(kde.ids.decode_itemid(I.ds)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.ids.encode_itemid, kde.encode_itemid)
    )
    self.assertTrue(
        optools.equiv_to_op(kde.ids.decode_itemid, kde.decode_itemid)
    )


if __name__ == '__main__':
  absltest.main()
