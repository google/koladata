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
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE

db = data_bag.DataBag.empty()
obj1 = db.obj()
obj2 = db.obj()
present = arolla.present()


QTYPES = frozenset([(DATA_SLICE, DATA_SLICE)])


class MaskingPresentLikeTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('1'), ds(present)),
      (ds(None), ds(None, schema_constants.MASK)),
      (ds(['1', None, '3']), ds([present, None, present])),
      (ds([['1'], [None, '3']]), ds([[present], [None, present]])),
      (ds([[obj1], [None, obj2]]), ds([[present], [None, present]])),
  )
  def test_eval(self, x, expected):
    res = expr_eval.eval(kde.masking.present_like(x))
    testing.assert_equal(res, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.masking.present_like,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.masking.present_like(I.x)))


if __name__ == '__main__':
  absltest.main()
