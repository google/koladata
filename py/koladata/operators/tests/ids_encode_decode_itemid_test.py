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
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class IdsEncodeDecodeItemIdTest(parameterized.TestCase):

  @parameterized.parameters(
      (bag().new(a=1),),
      (bag().new(a=ds([1, 2])),),
      (kde.allocation.new_listid().eval(),),
      (kde.allocation.new_dictid().eval(),),
      (kde.allocation.new_listid_like(ds([[1, None], [3]])).eval(),),
      (kde.allocation.new_dictid_like(ds([[1, None], [3]])).eval(),),
  )
  def test_encode_decode(self, ids):
    encoded = expr_eval.eval(kde.ids.encode_itemid(ids))
    self.assertIsNone(encoded.get_bag())
    if isinstance(encoded, data_item.DataItem):
      self.assertRegex(str(encoded), '[0-9a-zA-Z]{22}')
    else:
      for item in encoded.L:
        self.assertRegex(str(item), '[0-9a-zA-Z]{22}')
    decoded = expr_eval.eval(kde.ids.decode_itemid(encoded))
    self.assertIsNone(decoded.get_bag())
    testing.assert_equal(ids.no_bag().get_itemid(), decoded)

  def test_error(self):
    with self.assertRaisesRegex(
        ValueError, 'only ItemIds can be encoded, got INT32'
    ):
      kde.ids.encode_itemid(ds([1, 2, 3])).eval()
    with self.assertRaisesRegex(
        ValueError, 'cannot use encode_itemid on primitives'
    ):
      kde.ids.encode_itemid(
          ds([1, 2, 3], schema=schema_constants.OBJECT)
      ).eval()
    with self.assertRaisesRegex(
        ValueError, 'only STRING can be decoded, got INT32'
    ):
      kde.ids.decode_itemid(ds([1, 2, 3])).eval()
    with self.assertRaisesRegex(
        ValueError, r'only STRING can be decoded, got ENTITY\(a=INT32\)'
    ):
      kde.ids.decode_itemid(bag().new(a=ds([1, 2, 3]))).eval()

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.ids.encode_itemid,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        frozenset([(DATA_SLICE, DATA_SLICE)]),
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
