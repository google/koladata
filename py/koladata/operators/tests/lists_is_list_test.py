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
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import list_item as _  # pylint: disable=unused-import
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE

present = mask_constants.present
missing = mask_constants.missing


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
])


class ListsIsListTest(parameterized.TestCase):

  @parameterized.parameters(
      # List
      (bag().list(),),
      (bag().list([1, 2, 3]),),
      (ds([bag().list([1, 2]), None, bag().list([3, 4])]),),
      # OBJECT
      (
          ds([
              bag().list([1, 2]).embed_schema(),
              None,
              bag().list([3, 4]).embed_schema(),
          ]),
      ),
      # ANY
      (ds([bag().list([1, 2]), None, bag().list([3, 4])]).as_any(),),
      #
      (bag().list() & None,),
      (ds(None, schema_constants.OBJECT),),
      (ds(None, schema_constants.ANY),),
      (bag().obj(a=1) & None,),
  )
  def test_is_list(self, x):
    self.assertTrue(expr_eval.eval(kde.lists.is_list(x)))

  @parameterized.parameters(
      # Primitive
      (ds(1),),
      (ds([1, 2]),),
      # Dict/Object/Entity
      (bag().obj(a=1),),
      (bag().new(a=1),),
      (bag().dict({1: 2}),),
      # ItemIs
      (bag().list().get_itemid(),),
      # Mixed
      (ds([bag().list([1, 2]).embed_schema(), None, 1]),),
      # Missing
      (ds(None),),
      (ds(None, schema_constants.INT32),),
      (ds([None, None]),),
      (ds([None, None], schema_constants.INT32),),
      (bag().new(a=1) & None,),
      (bag().dict({1: 2}) & None,),
  )
  def test_is_not_list(self, x):
    self.assertFalse(expr_eval.eval(kde.lists.is_list(x)))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.lists.is_list,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.lists.is_list(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.lists.is_list, kde.is_list))


if __name__ == '__main__':
  absltest.main()
