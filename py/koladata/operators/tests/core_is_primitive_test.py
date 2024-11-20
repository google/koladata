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
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
M = arolla.M
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
kde = kde_operators.kde


class KodaIsPrimitiveTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(1),),
      (ds(None, schema_constants.INT32),),
      (ds([1, 2, 3]),),
      (ds('hello'),),
      (ds(['hello', None, 'world']),),
      (ds(arolla.quote(kde.math.subtract(arolla.L.L1, arolla.L.L2))),),
      # Mixed types.
      (ds(['hello', None, 1]),),
      (ds([schema_constants.STRING, None, schema_constants.SCHEMA]),),
  )
  def test_is_primitive(self, param):
    self.assertTrue(expr_eval.eval(kde.core.is_primitive(param)))

  @parameterized.parameters(
      (None,),
      (ds([None, None]),),
      (bag().list([1, 2, 3]),),
      (bag().dict(ds(['hello', 'world']), ds([1, 2])),),
      (bag().obj(a=ds(1), b=ds(2)),),
      (ds([bag().obj(a=ds(1), b=ds(2)), 42, 'abc']),),
      (bag().new_schema(),),
      (ds([schema_constants.INT32, bag().new_schema()]),),
  )
  def test_is_not_primitive(self, param):
    self.assertFalse(expr_eval.eval(kde.core.is_primitive(param)))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.is_primitive(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.is_primitive, kde.is_primitive)
    )


if __name__ == '__main__':
  absltest.main()
