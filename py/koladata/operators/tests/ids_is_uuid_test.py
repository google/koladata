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

"""Tests for kd.ids.is_uuid operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
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


class IsUuidTest(parameterized.TestCase):
  """Tests for kd.ids.is_uuid operator."""

  @parameterized.parameters(
      # Uuid
      (bag().uu(a=1),),
      (bag().uu(a=1).get_itemid(),),
      (ds([bag().uu(a=1), None, bag().uu(a=2)]),),
      (bag().dict({1: 2}, itemid=expr_eval.eval(kde.uuid_for_dict())),),
      (bag().list([1, 2], itemid=expr_eval.eval(kde.uuid_for_list())),),
      (bag().uuobj(a=1),),
      (
          ds([
              bag().uuobj(a=1),
              None,
              bag().uuobj(a=2),
          ]),
      ),
      (bag().uu_schema(a=schema_constants.INT32),),
      # Missing
      (ds(None),),
      (ds([]),),
      (bag().uu(a=1) & None,),
      (ds([None, None]),),
      (bag().obj(a=1) & None,),
      (ds(None, schema_constants.OBJECT),),
      (ds(None, schema_constants.SCHEMA),),
      (ds(None, schema_constants.ITEMID),),
  )
  def test_is_uuid(self, x):
    testing.assert_equal(expr_eval.eval(kde.ids.is_uuid(x)), ds(present))

  @parameterized.parameters(
      # Primitive
      (ds(1),),
      (ds([1, 2]),),
      # Dict/Object/Entity
      (bag().obj(a=1),),
      (bag().new(a=1),),
      (bag().dict({1: 2}),),
      # Mixed
      (ds([bag().uu(a=1).embed_schema(), None, 1]),),
      # Missing with wrong schema
      (ds(None, schema_constants.INT32),),
      (ds([None, None], schema_constants.INT32),),
      (bag().new_schema(a=schema_constants.INT32),),
      (
          ds([
              bag().uu_schema(a=schema_constants.INT32),
              bag().new_schema(a=schema_constants.INT32),
          ]),
      ),
  )
  def test_is_not_uuid(self, x):
    testing.assert_equal(expr_eval.eval(kde.ids.is_uuid(x)), ds(missing))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.ids.is_uuid,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.ids.is_uuid(I.x)))


if __name__ == '__main__':
  absltest.main()
