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
from koladata.functor import functor_factories
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
kde = kde_operators.kde
DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


present = arolla.present()
db = data_bag.DataBag.empty()
ds = lambda vals: data_slice.DataSlice.from_vals(vals).with_bag(db)
LIST1 = db.list([[1, 2], [3]])


class CoreSelectItemsTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          db.list([1, 2, 3]),
          ds([None, present, present]),
          ds([2, 3]),
      ),
      (
          db.list([1, 2, 3]),
          functor_factories.fn(I.self >= 2),
          ds([2, 3]),
      ),
      (
          LIST1,
          ds([None, present]),
          ds([LIST1[1]]),  # ensure inner list is the same instance.
      ),
      (
          ds([db.list([1, 2, 3]), db.list([2, 3, 4])]),
          ds([None, present]),
          ds([[], [2, 3, 4]]),
      ),
      (
          ds([db.list([1, 2, 3]), db.list([2, 3, 4])]),
          ds([[None, present, present], [present, None, present]]),
          ds([[2, 3], [2, 4]]),
      ),
      (
          db.list([1, 2, 3]),
          lambda x: x >= 2,
          ds([2, 3]),
      ),
      (
          db.list([1, 2, 3]),
          functor_factories.fn(I.self >= 2),
          ds([2, 3]),
      ),
  )
  def test_eval(self, value, fltr, expected):
    result = expr_eval.eval(kde.core.select_items(value, fltr))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      (lambda x: x >= 2),
      (functor_factories.fn(I.self >= 2),),
  )
  def test_eval_with_expr_input(self, fltr):
    result = expr_eval.eval(
        kde.core.select_items(I.x, fltr), x=db.list([1, 2, 3])
    )
    testing.assert_equal(result, ds([2, 3]))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.select_items,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.select(I.x, I.fltr)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.select_items, kde.select_items)
    )


if __name__ == '__main__':
  absltest.main()
