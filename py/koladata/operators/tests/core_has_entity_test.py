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
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
M = arolla.M
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
kde = kde_operators.kde

present = mask_constants.present
missing = mask_constants.missing


class KodaHasEntityTest(parameterized.TestCase):

  @parameterized.parameters(
      # DataItem
      (ds(None), missing),
      (bag().new() & None, missing),
      (bag().new(), present),
      (bag().new(a=1), present),
      (bag().obj(a=1), present),
      (bag().new(a=1).as_any(), present),
      (ds('hello'), missing),
      (bag().dict(), missing),
      (bag().dict().embed_schema(), missing),
      (bag().list(), missing),
      (bag().new_schema(), missing),
      # DataSlice
      (
          ds([
              bag().new(a=1, schema='test'),
              None,
              bag().new(a=2, schema='test'),
          ]),
          ds([present, missing, present]),
      ),
      (ds([None, None]), ds([missing, missing])),
      (ds([None, None], schema_constants.INT32), ds([missing, missing])),
      (ds([None, None], schema_constants.OBJECT), ds([missing, missing])),
      (ds([None, None], schema_constants.ANY), ds([missing, missing])),
      # Mixed types.
      (
          ds([bag().obj(a=1), None, 'world', bag().dict().embed_schema()]),
          ds([present, missing, missing, missing]),
      ),
  )
  def test_eval(self, x, expected):
    testing.assert_equal(expr_eval.eval(kde.core.has_entity(x)), expected)

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.has_entity(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.has_entity, kde.has_entity))


if __name__ == '__main__':
  absltest.main()
