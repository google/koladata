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
from koladata.types import mask_constants
from koladata.types import qtypes

I = input_container.InputContainer('I')
kde = kde_operators.kde


db = data_bag.DataBag.empty()
ds = lambda x: data_slice.DataSlice.from_vals(x).with_db(db)
present = arolla.present()
DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])

DICT = db.dict({1: 1, 2: 2, 3: 3})


class CoreSelectValuesTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([db.dict({1: 1}), db.dict({2: 2}), db.dict({3: 3})]),
          ds([mask_constants.present, None, None]),
          ds([[1], [], []]),
      ),
      (
          ds([db.dict({4: 1}), db.dict({5: 2}), db.dict({6: 3})]),
          functor_factories.fn(I.self == 1),
          ds([[1], [], []]),
      ),
      (
          ds([[db.dict({1: 1})], [db.dict({2: 2}), db.dict({3: 3})]]),
          ds([mask_constants.present, None]),
          ds([[[1]], [[], []]]),
      ),
      (
          ds([[db.dict({1: 1})], [db.dict({2: 2}), db.dict({3: 3})]]),
          ds([[mask_constants.present], [None, mask_constants.present]]),
          ds([[[1]], [[], [3]]]),
      ),
      (
          db.dict({2: DICT}),
          ds([mask_constants.present]),
          ds([DICT]),
      ),
  )
  def test_eval(self, value, fltr, expected):
    result = expr_eval.eval(kde.core.select_values(value, fltr))
    testing.assert_equal(result, expected)

  def test_eval_with_expr(self):
    data = db.dict({1: 1, 2: 2, 3: 3})
    fltr = I.values > 2
    values = expr_eval.eval(kde.core.get_values(data))
    fltr_ds = expr_eval.eval(fltr, values=values)
    expected = expr_eval.eval(kde.core.select(values, fltr_ds))

    result = expr_eval.eval(kde.core.select_keys(data, fltr_ds))
    testing.assert_equal(result, expected)

  def test_eval_with_functor(self):
    data = db.dict({4: 1, 5: 2, 6: 3})
    fltr = functor_factories.fn(I.self >= 2)
    result = expr_eval.eval(kde.core.select_values(data, fltr))
    testing.assert_unordered_equal(result, ds([2, 3]))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.select_values,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_data_slice_view(kde.core.select_values(I.x, I.fltr))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.select_values, kde.select_values)
    )


if __name__ == '__main__':
  absltest.main()
