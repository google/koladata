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
ds = lambda x: data_slice.DataSlice.from_vals(x).with_bag(db)
DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])

DICT = db.dict({1: 1, 2: 2, 3: 3})


class CoreSelectKeysTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([db.dict({1: 1}), db.dict({2: 2}), db.dict({3: 3})]),
          ds([mask_constants.present, None, None]),
          ds([[1], [], []]),
      ),
      (
          ds([db.dict({1: 1}), db.dict({2: 2}), db.dict({3: 3})]),
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
          db.dict({DICT: 2}),
          ds([mask_constants.present]),
          ds([DICT]),
      ),
      (
          db.dict({1: 3, 2: 4, 3: 5}),
          lambda x: x == 2,
          ds([2]),
      ),
      (
          db.dict({1: 3, 2: 4, 3: 5}),
          functor_factories.fn(I.self == 2),
          ds([2]),
      ),
  )
  def test_eval(self, value, fltr, expected):
    result = expr_eval.eval(kde.core.select_keys(value, fltr))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      (lambda x: x >= 2),
      (functor_factories.fn(I.self >= 2),),
  )
  def test_eval_with_expr_input(self, fltr):
    result = expr_eval.eval(
        kde.core.select_keys(I.x, fltr), x=db.dict({1: 4, 2: 5, 3: 6})
    )
    testing.assert_unordered_equal(result, ds([2, 3]))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.select_keys,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_data_slice_view(kde.core.select_keys(I.x, I.fltr))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.select_keys, kde.select_keys)
    )


if __name__ == '__main__':
  absltest.main()
