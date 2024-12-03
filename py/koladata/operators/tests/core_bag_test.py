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
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class CoreBagTest(absltest.TestCase):

  def test_eval(self):
    self.assertIsInstance(expr_eval.eval(kde.core.bag()), data_bag.DataBag)
    testing.assert_equivalent(
        expr_eval.eval(kde.core.with_bag(ds(42), kde.core.bag())).get_bag(),
        data_bag.DataBag.empty()
    )

  def test_non_determinism(self):
    res_1 = expr_eval.eval(kde.core.bag())
    res_2 = expr_eval.eval(kde.core.bag())
    self.assertNotEqual(res_1.fingerprint, res_2.fingerprint)
    testing.assert_equivalent(res_1, res_2)

    expr = kde.core.bag()
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(res_1.fingerprint, res_2.fingerprint)
    testing.assert_equivalent(res_1, res_2)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.bag,
        frozenset([(qtypes.NON_DETERMINISTIC_TOKEN, qtypes.DATA_BAG)]),
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.bag()))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.bag, kde.bag))

  def test_repr(self):
    self.assertEqual(repr(kde.core.bag()), 'kde.core.bag()')


if __name__ == '__main__':
  absltest.main()
