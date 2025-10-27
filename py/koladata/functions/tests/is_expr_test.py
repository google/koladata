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
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class IsExprTest(parameterized.TestCase):

  @parameterized.parameters(
      (kde.math.subtract(arolla.L.L1, arolla.L.L2),),
      (arolla.quote(kde.math.subtract(arolla.L.L1, arolla.L.L2)).unquote(),),
  )
  def test_is_expr(self, param):
    testing.assert_equal(fns.is_expr(param), mask_constants.present)

  @parameterized.parameters(
      (1,),
      ([1, 2, 3],),
      (None,),
      ({1: 2, 2: 3},),
      (arolla.quote(kde.math.subtract(arolla.L.L1, arolla.L.L2)),),
      (ds(1),),
      (fns.obj(),),
      (fns.obj(a=1, b=2),),
      (fns.new(),),
      (fns.new(a=1, b=2),),
      (fns.list(),),
      (fns.dict(),),
      (ds(schema_constants.ITEMID),),
      (ds(arolla.quote(kde.math.subtract(arolla.L.L1, arolla.L.L2))),),
  )
  def test_is_not_expr(self, param):
    testing.assert_equal(fns.is_expr(param), mask_constants.missing)


if __name__ == '__main__':
  absltest.main()
