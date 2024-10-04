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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import functor_factories
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class FunctorMaybeCallTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([1, 2, 3]),
          ds([1]),
          ds([1, 2, 3]),
      ),
      (
          functor_factories.fn(I.self + 1),
          ds([1, 2, 3]),
          ds([2, 3, 4]),
      )
  )
  def test_call_with_functor(self, maybe_fn, arg, expected):
    result = expr_eval.eval(kde.functor._maybe_call(maybe_fn, arg))
    testing.assert_equal(result, expected)

  def test_call_with_error(self):
    f = functor_factories.fn(I.self.db)

    data = ds([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'DataSlice has no associated DataBag; during evaluation of operator'
            ' kde.core.get_db'
        ),
    ):
      expr_eval.eval(kde.functor._maybe_call(f, data))

    entity = data_bag.DataBag.empty().new()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the functor is expected to be evaluated to a DataSlice, but the'
            ' result has type `DATA_BAG` instead'
        ),
    ):
      expr_eval.eval(kde.functor._maybe_call(f, entity))

    db = data_bag.DataBag.empty()
    with self.assertRaisesRegex(
        ValueError, re.escape('expected DATA_SLICE, got arg: DATA_BAG')
    ):
      expr_eval.eval(kde.functor._maybe_call(f, db))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.functor._maybe_call,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_data_slice_view(kde.functor._maybe_call(I.maybe_fn, I.arg))
    )

if __name__ == '__main__':
  absltest.main()
