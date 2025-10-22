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
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import dict_item as _  # pylint: disable=unused-import
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

present = mask_constants.present
missing = mask_constants.missing

DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
])


class DictsIsEntityTest(parameterized.TestCase):

  @parameterized.parameters(
      # Entity
      (bag().new(),),
      (bag().new(a=1),),
      (
          ds([
              bag().new(a=1, schema='test'),
              None,
              bag().new(a=2, schema='test'),
          ]),
      ),
      # OBJECT
      (ds([bag().obj(a=1), None, bag().obj(a=2)]),),
      # Missing
      (bag().new() & None,),
      (ds(None, schema_constants.OBJECT),),
      (ds(None),),
      (ds([None, None]),),
      (bag().obj(a=1) & None,),
  )
  def test_is_entity(self, x):
    self.assertTrue(kd.core.is_entity(x))

  @parameterized.parameters(
      # Primitive
      (ds(1),),
      (ds([1, 2]),),
      # List/Object/Dict
      (bag().list([1, 2]).embed_schema(),),
      (bag().list([1, 2]),),
      (bag().dict({1: 2}),),
      # ItemId
      (bag().new().get_itemid(),),
      # Mixed
      (ds([bag().list([1, 2]).embed_schema(), None, 1]),),
      # Missing
      (ds(None, schema_constants.INT32),),
      (ds([None, None], schema_constants.INT32),),
      (bag().dict({1: 2}) & None,),
      (bag().list([1, 2]) & None,),
  )
  def test_is_not_entity(self, x):
    self.assertFalse(kd.core.is_entity(x))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.is_entity,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.is_entity(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.is_entity, kde.is_entity))


if __name__ == '__main__':
  absltest.main()
