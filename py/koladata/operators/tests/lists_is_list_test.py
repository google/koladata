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
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import list_item as _  # pylint: disable=unused-import
from koladata.types import mask_constants
from koladata.types import schema_constants


I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE

present = mask_constants.present
missing = mask_constants.missing


QTYPES = [(DATA_SLICE, DATA_SLICE)]


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
      # Missing
      (bag().list() & None,),
      (ds(None, schema_constants.OBJECT),),
      (ds(None),),
      (ds([None, None]),),
      (bag().obj(a=1) & None,),
  )
  def test_is_list(self, x):
    self.assertTrue(kd.lists.is_list(x))

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
      (ds(None, schema_constants.INT32),),
      (ds([None, None], schema_constants.INT32),),
      (bag().new(a=1) & None,),
      (bag().dict({1: 2}) & None,),
  )
  def test_is_not_list(self, x):
    self.assertFalse(kd.lists.is_list(x))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.lists.is_list,
        QTYPES,
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.lists.is_list(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.lists.is_list, kde.is_list))


if __name__ == '__main__':
  absltest.main()
