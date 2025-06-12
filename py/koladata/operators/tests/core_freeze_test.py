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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals


class CoreFreezeTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds([1, 2, 3]),),
      (bag(),),
      (bag().fork(mutable=True),),
      (bag().fork(mutable=False),),
      (bag().new(x=1, y='abc').get_bag(),),
      (bag().new(a=ds([1, 2, 3])),),
      (bag().new(a=ds([1, 2, 3])).freeze_bag(),),
  )
  def test_eval(self, s):
    res = expr_eval.eval(kde.core.freeze(s))
    testing.assert_equivalent(res, s)
    self.assertFalse(res.is_mutable())

  def test_unsupported_input(self):
    with self.assertRaisesRegex(ValueError, 'expected DATA_BAG or DATA_SLICE'):
      kde.core.freeze(arolla.int32(42))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.freeze,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES
        ),
        frozenset([
            (qtypes.DATA_BAG, qtypes.DATA_BAG),
            (qtypes.DATA_SLICE, qtypes.DATA_SLICE),
        ]),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.freeze(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.nofollow, kde.nofollow))


if __name__ == '__main__':
  absltest.main()
